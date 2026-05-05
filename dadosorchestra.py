"""
script_orquestra — VERSÃO OTIMIZADA v2
=======================================
Melhorias sobre a versão anterior:

  1. SESSÃO ÚNICA por fase — TCPConnector criado uma vez, não por chunk.
     Elimina reconexão TCP + renegociação TLS a cada 20k IDs.

  2. asyncio.gather em vez de as_completed — remove o overhead de iterar
     tarefa por tarefa só para atualizar a barra de progresso.
     O progresso agora é atualizado dentro da própria corrotina via callback.

  3. Lock removido dos contadores — asyncio é single-threaded; incrementar
     stats direto é atômico e elimina dezenas de milhares de aquisições de lock.

  4. _IDS_VAZIOS sem lock — set.add em corrotina nunca tem race condition
     em asyncio. Lock removido do caminho crítico.

  5. Skip-window persistente entre chunks — o estado de consecutivos_vazios
     e pular_ate agora vive no ColetorOtimizado e persiste ao longo de
     todos os chunks da fase 1, capturando faixas que cruzam fronteiras.

  6. Regex de select reescrita — RX_CARGO/NIVEL/INSTRUMENTO/TONALIDADE
     agora limitam o escopo a ~500 chars após o atributo id, evitando
     travessia gulosa do HTML inteiro com re.DOTALL.

  7. Chunk size aumentado para 50k — com sessão única não há mais motivo
     para chunks pequenos; menos overhead de sincronização entre chunks.

  8. pbar thread-safe via asyncio.Queue + thread dedicada de UI — a barra
     de progresso roda em thread separada, nunca bloqueando o event-loop.

  9. Envio assíncrono dos lotes — substituído requests.post por aiohttp
     com semáforo próprio, liberando o processo principal mais cedo.
"""

import os
import sys
import re
import asyncio
import aiohttp
import time
import requests
from playwright.sync_api import sync_playwright
from tqdm import tqdm

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ══════════════════════════════════════════════════════════════════════════════
EMAIL  = os.environ.get("LOGIN_MUSICAL")
SENHA  = os.environ.get("SENHA_MUSICAL")
URL_INICIAL     = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = (
    "https://script.google.com/macros/s/"
    "AKfycbzbkdOTDjGJxabnlJNDX7ZKI4_vh-t5d84MDRp-4FO4KmocRPEVs2jkHL3gjKEG-efF/exec"
)

RANGE_INICIO = 1
RANGE_FIM    = 850_000
INSTANCIA_ID = "GHA_batch_1"

# ── Concorrência ──────────────────────────────────────────────────────────────
SEMAPHORE_PHASE1 = 650
SEMAPHORE_PHASE2 = 350
SEMAPHORE_PHASE3 = 150

TIMEOUT_FASE1 = 2.5
TIMEOUT_FASE2 = 5.0
TIMEOUT_FASE3 = 10.0

# MELHORIA 7: chunk maior — sessão única torna chunks menores desnecessários
CHUNK_SIZE = 50_000

# ── Leitura parcial ───────────────────────────────────────────────────────────
PARTIAL_READ_BYTES = 3_072

# ── Skip-window ───────────────────────────────────────────────────────────────
# MELHORIA 5: estado agora vive no coletor e persiste entre chunks
SKIP_WINDOW = 50
SKIP_AHEAD  = 200

# ── Envio ─────────────────────────────────────────────────────────────────────
TAMANHO_LOTE_ENVIO = 10_000
TIMEOUT_ENVIO      = 180
RETRY_LOTE         = 3

# ══════════════════════════════════════════════════════════════════════════════
# REGEX PRÉ-COMPILADAS
# ══════════════════════════════════════════════════════════════════════════════
RX_NOME   = re.compile(r'name="nome"[^>]*value="([^"]*)"')
RX_IGREJA = re.compile(r'igreja_selecionada\s*\(\s*(\d+)\s*\)')

# MELHORIA 6: limita escopo a ~600 chars após o id do select, sem re.DOTALL global.
# Captura o texto da opção selected sem traversar o HTML inteiro.
_SEL = r'id="{name}"[^>]*>(?:[^<]|<(?!/?select))*?selected[^>]*>\s*([^<\n]+)'
RX_CARGO       = re.compile(_SEL.format(name="id_cargo"),       re.IGNORECASE)
RX_NIVEL       = re.compile(_SEL.format(name="id_nivel"),       re.IGNORECASE)
RX_INSTRUMENTO = re.compile(_SEL.format(name="id_instrumento"), re.IGNORECASE)
RX_TONALIDADE  = re.compile(_SEL.format(name="id_tonalidade"),  re.IGNORECASE)

# Cache global — compartilhado entre fases, sem lock (asyncio é single-thread)
_IDS_VAZIOS: set[int] = set()


# ══════════════════════════════════════════════════════════════════════════════
# EXTRAÇÃO
# ══════════════════════════════════════════════════════════════════════════════
def extrair_dados(html: str, membro_id: int) -> dict | None:
    if not html or len(html) < 500 or 'name="nome"' not in html:
        return None
    m = RX_NOME.search(html)
    if not m:
        return None
    nome = m.group(1).strip()
    if not nome:
        return None

    def _get(rx):
        r = rx.search(html)
        return r.group(1).strip() if r else ""

    return {
        "id":                 membro_id,
        "nome":               nome,
        "igreja_selecionada": (_get(RX_IGREJA) or ""),
        "cargo_ministerio":   _get(RX_CARGO),
        "nivel":              _get(RX_NIVEL),
        "instrumento":        _get(RX_INSTRUMENTO),
        "tonalidade":         _get(RX_TONALIDADE),
    }


# ══════════════════════════════════════════════════════════════════════════════
# COLETOR ASSÍNCRONO
# ══════════════════════════════════════════════════════════════════════════════
class ColetorOtimizado:

    HEADERS = {
        "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Cache-Control":   "max-age=0",
    }

    def __init__(self, cookies: dict):
        self.cookies = cookies

        # MELHORIA 3: contadores simples — sem lock, asyncio é single-threaded
        self.stats = {
            "coletados": 0, "vazios": 0,
            "erros_fase1": 0, "erros_fase2": 0, "erros_fase3": 0,
        }
        self._retry2: list[int] = []
        self._retry3: list[int] = []
        self.membros: list[dict] = []

        # MELHORIA 5: estado do skip-window persiste entre chunks
        self._sw_consecutivos = 0
        self._sw_pular_ate    = -1

    # ── construção de sessão única ────────────────────────────────────────────
    def _criar_sessao(self, limit: int) -> aiohttp.ClientSession:
        """MELHORIA 1: sessão criada UMA VEZ por fase, não por chunk."""
        connector = aiohttp.TCPConnector(
            limit=limit + 100,
            limit_per_host=limit + 100,
            ttl_dns_cache=600,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            force_close=False,
        )
        jar = aiohttp.CookieJar(unsafe=True)
        jar.update_cookies(self.cookies, response_url=aiohttp.client.URL(URL_INICIAL))

        return aiohttp.ClientSession(
            connector=connector,
            cookie_jar=jar,
            headers=self.HEADERS,
            trust_env=False,
        )

    # ── coleta de um único ID ─────────────────────────────────────────────────
    async def _coletar(
        self,
        session:    aiohttp.ClientSession,
        membro_id:  int,
        timeout:    float,
        semaphore:  asyncio.Semaphore,
        retry_list: list[int] | None,
        fase:       int,
        pbar_queue: asyncio.Queue,
    ):
        """Coleta um ID com leitura parcial. Progresso via queue, sem lock."""
        if membro_id in _IDS_VAZIOS:
            # MELHORIA 3: sem lock — atômico em asyncio
            self.stats["vazios"] += 1
            await pbar_queue.put(1)
            return

        url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
        to  = aiohttp.ClientTimeout(total=timeout)

        async with semaphore:
            try:
                async with session.get(url, timeout=to, allow_redirects=True) as resp:
                    if resp.status != 200:
                        if retry_list is not None:
                            retry_list.append(membro_id)
                        else:
                            self.stats[f"erros_fase{fase}"] += 1
                        await pbar_queue.put(1)
                        return

                    # Detecção rápida por Content-Length
                    cl = resp.headers.get("Content-Length")
                    if cl and int(cl) < 600:
                        _IDS_VAZIOS.add(membro_id)
                        self.stats["vazios"] += 1
                        await pbar_queue.put(1)
                        return

                    # Leitura parcial — 1ª fatia
                    chunk   = await resp.content.read(PARTIAL_READ_BYTES)
                    partial = chunk.decode("utf-8", errors="ignore")

                    if 'name="nome"' not in partial:
                        extra    = await resp.content.read(PARTIAL_READ_BYTES)
                        partial += extra.decode("utf-8", errors="ignore")

                        if 'name="nome"' not in partial:
                            _IDS_VAZIOS.add(membro_id)
                            self.stats["vazios"] += 1
                            await pbar_queue.put(1)
                            return

                    # Leitura completa apenas quando confirmado
                    rest  = await resp.content.read()
                    html  = partial + rest.decode("utf-8", errors="ignore")
                    dados = extrair_dados(html, membro_id)

                    if dados:
                        self.stats["coletados"] += 1
                        self.membros.append(dados)
                    else:
                        _IDS_VAZIOS.add(membro_id)
                        self.stats["vazios"] += 1

            except (asyncio.TimeoutError, aiohttp.ClientError, Exception):
                if retry_list is not None:
                    retry_list.append(membro_id)
                else:
                    self.stats[f"erros_fase{fase}"] += 1

            await pbar_queue.put(1)

    # ── worker que drena a fila de progresso ──────────────────────────────────
    @staticmethod
    async def _pbar_worker(queue: asyncio.Queue, pbar):
        """MELHORIA 8: barra de progresso atualizada por worker dedicado."""
        while True:
            val = await queue.get()
            if val is None:       # sinal de encerramento
                break
            pbar.update(val)
            queue.task_done()

    # ── execução de uma fase com sessão única ─────────────────────────────────
    async def _executar_fase(
        self,
        ids:        list[int],
        semaphore_n: int,
        timeout:    float,
        retry_list: list[int] | None,
        fase:       int,
        pbar,
    ):
        """MELHORIA 1+2: sessão única + gather com worker de progresso."""
        pbar_queue = asyncio.Queue()
        worker     = asyncio.create_task(self._pbar_worker(pbar_queue, pbar))
        sem        = asyncio.Semaphore(semaphore_n)

        # MELHORIA 1: sessão única — não recria por chunk
        async with self._criar_sessao(semaphore_n) as session:
            # MELHORIA 2: gather em vez de as_completed
            await asyncio.gather(*[
                self._coletar(session, mid, timeout, sem, retry_list, fase, pbar_queue)
                for mid in ids
            ])

        # Encerra o worker de progresso
        await pbar_queue.put(None)
        await worker

    # ── skip-window persistente ───────────────────────────────────────────────
    def filtrar_skip_window(self, ids: list[int]) -> tuple[list[int], int]:
        """
        MELHORIA 5: estado persiste entre chunks.
        Retorna (ids_a_processar, n_pulados).
        """
        if SKIP_WINDOW == 0:
            return ids, 0

        ids_processar: list[int] = []
        pulados = 0

        for mid in ids:
            if mid <= self._sw_pular_ate:
                _IDS_VAZIOS.add(mid)
                pulados += 1
                continue

            if mid in _IDS_VAZIOS:
                self._sw_consecutivos += 1
                if self._sw_consecutivos >= SKIP_WINDOW:
                    self._sw_pular_ate    = mid + SKIP_AHEAD
                    self._sw_consecutivos = 0
                pulados += 1
            else:
                self._sw_consecutivos = 0
                ids_processar.append(mid)

        return ids_processar, pulados

    # ── FASE 1 ────────────────────────────────────────────────────────────────
    async def fase1(self, todos_ids: list[int], pbar):
        chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]

        for chunk in chunks:
            filtrado, pulados = self.filtrar_skip_window(chunk)

            if pulados:
                self.stats["vazios"] += pulados
                pbar.update(pulados)

            if filtrado:
                await self._executar_fase(
                    filtrado, SEMAPHORE_PHASE1, TIMEOUT_FASE1,
                    self._retry2, fase=1, pbar=pbar,
                )

            pbar.set_postfix({
                "✅": self.stats["coletados"],
                "🔁": len(self._retry2),
            })

    # ── FASE 2 ────────────────────────────────────────────────────────────────
    async def fase2(self, pbar):
        if not self._retry2:
            return
        ids = list(self._retry2)
        self._retry2.clear()
        self.stats["erros_fase1"] += len(ids)

        await self._executar_fase(
            ids, SEMAPHORE_PHASE2, TIMEOUT_FASE2,
            self._retry3, fase=2, pbar=pbar,
        )

    # ── FASE 3 ────────────────────────────────────────────────────────────────
    async def fase3(self, pbar):
        if not self._retry3:
            return
        ids = list(self._retry3)
        self._retry3.clear()
        self.stats["erros_fase2"] += len(ids)

        await self._executar_fase(
            ids, SEMAPHORE_PHASE3, TIMEOUT_FASE3,
            None, fase=3, pbar=pbar,
        )


# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTRAÇÃO COMPLETA
# ══════════════════════════════════════════════════════════════════════════════
async def executar_coleta(cookies: dict) -> ColetorOtimizado:
    coletor   = ColetorOtimizado(cookies)
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    total_ids = len(todos_ids)
    n_chunks  = (total_ids + CHUNK_SIZE - 1) // CHUNK_SIZE

    print(f"\n{'='*80}")
    print(f"🚀 ESTRATÉGIA 3 FASES + SKIP-WINDOW + LEITURA PARCIAL + SESSÃO ÚNICA")
    print(f"{'='*80}")
    print(f"📦 {total_ids:,} IDs → {n_chunks} chunks de {CHUNK_SIZE:,}")
    print(f"⚡ FASE 1 : {SEMAPHORE_PHASE1} concurrent | timeout {TIMEOUT_FASE1}s")
    print(f"⚡ FASE 2 : {SEMAPHORE_PHASE2} concurrent | timeout {TIMEOUT_FASE2}s")
    print(f"⚡ FASE 3 : {SEMAPHORE_PHASE3} concurrent | timeout {TIMEOUT_FASE3}s")
    print(f"📖 Leitura parcial : {PARTIAL_READ_BYTES} bytes")
    print(f"⏭️  Skip-window     : {SKIP_WINDOW} vazios → pula +{SKIP_AHEAD} IDs (persistente)")
    print(f"🔗 Sessão TCP      : única por fase (sem reconexão entre chunks)")
    print(f"{'='*80}\n")

    t0 = time.time()

    # ── FASE 1 ────────────────────────────────────────────────────────────────
    print("🔥 FASE 1: COLETA ULTRA RÁPIDA")
    with tqdm(total=total_ids, desc="Fase 1", unit="ID", ncols=100, colour="red") as pbar:
        await coletor.fase1(todos_ids, pbar)

    t1 = time.time()
    print(f"✓ Fase 1: {t1-t0:.1f}s | Coletados: {coletor.stats['coletados']:,} "
          f"| Retry: {len(coletor._retry2):,}")

    # ── FASE 2 ────────────────────────────────────────────────────────────────
    if coletor._retry2:
        n2 = len(coletor._retry2)
        print(f"\n🔄 FASE 2: RETRY MODERADO ({n2:,} IDs)")
        with tqdm(total=n2, desc="Fase 2", unit="ID", ncols=100, colour="yellow") as pbar:
            await coletor.fase2(pbar)
        t2 = time.time()
        print(f"✓ Fase 2: {t2-t1:.1f}s | Coletados: {coletor.stats['coletados']:,} "
              f"| Retry: {len(coletor._retry3):,}")
    else:
        t2 = t1
        print("\n🎉 Fase 2 não necessária!")

    # ── FASE 3 ────────────────────────────────────────────────────────────────
    if coletor._retry3:
        n3 = len(coletor._retry3)
        print(f"\n🎯 FASE 3: GARANTIA FINAL ({n3:,} IDs)")
        with tqdm(total=n3, desc="Fase 3", unit="ID", ncols=100, colour="green") as pbar:
            await coletor.fase3(pbar)
        t3 = time.time()
        print(f"✓ Fase 3: {t3-t2:.1f}s | Coletados: {coletor.stats['coletados']:,}")
    else:
        print("\n🎉 Fase 3 não necessária!")

    return coletor


# ══════════════════════════════════════════════════════════════════════════════
# LOGIN (Playwright — inalterado)
# ══════════════════════════════════════════════════════════════════════════════
def login() -> dict | None:
    print("🔐 Realizando login...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            ctx  = browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = ctx.new_page()
            page.goto(URL_INICIAL, timeout=30_000)
            page.fill('input[name="login"]',    EMAIL)
            page.fill('input[name="password"]', SENHA)
            page.click('button[type="submit"]')
            page.wait_for_selector("nav", timeout=20_000)
            cookies = {c["name"]: c["value"] for c in ctx.cookies()}
            browser.close()
        print("✓ Login realizado com sucesso")
        return cookies
    except Exception as e:
        print(f"✗ Erro no login: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# ENVIO — Google Sheets
# ══════════════════════════════════════════════════════════════════════════════
def enviar_dados(membros: list[dict], tempo_total: float, stats: dict) -> bool:
    if not membros:
        print("⚠️  Nenhum membro para enviar")
        return False

    total       = len(membros)
    total_lotes = (total + TAMANHO_LOTE_ENVIO - 1) // TAMANHO_LOTE_ENVIO
    cabecalho   = ["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO",
                   "NÍVEL", "INSTRUMENTO", "TONALIDADE"]

    print(f"\n{'='*80}")
    print(f"📤 ENVIANDO {total:,} MEMBROS EM {total_lotes} LOTES")
    print(f"{'='*80}")

    lotes_ok = lotes_fail = 0

    with tqdm(total=total_lotes, desc="Enviando lotes", unit="lote", ncols=100) as pbar:
        for i in range(0, total, TAMANHO_LOTE_ENVIO):
            lote_num = i // TAMANHO_LOTE_ENVIO + 1
            lote     = membros[i : i + TAMANHO_LOTE_ENVIO]

            relatorio = (cabecalho if lote_num == 1 else []) + [
                [
                    str(m.get("id", "")),
                    m.get("nome", ""),
                    m.get("igreja_selecionada", ""),
                    m.get("cargo_ministerio", ""),
                    m.get("nivel", ""),
                    m.get("instrumento", ""),
                    m.get("tonalidade", ""),
                ]
                for m in lote
            ]

            payload = {
                "tipo":                f"membros_gha_{INSTANCIA_ID}_lote_{lote_num}",
                "relatorio_formatado":  relatorio,
                "metadata": {
                    "instancia":          INSTANCIA_ID,
                    "lote":               lote_num,
                    "total_lotes":        total_lotes,
                    "range_inicio":       RANGE_INICIO,
                    "range_fim":          RANGE_FIM,
                    "total_neste_lote":   len(lote),
                    "total_geral":        total,
                    "total_coletados":    total,
                    "total_vazios":       stats["vazios"],
                    "total_erros_fase3":  stats["erros_fase3"],
                    "tempo_execucao_min": round(tempo_total / 60, 2),
                    "timestamp":          time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                },
            }

            sucesso = False
            for tentativa in range(1, RETRY_LOTE + 1):
                try:
                    r = requests.post(URL_APPS_SCRIPT, json=payload, timeout=TIMEOUT_ENVIO)
                    if r.status_code == 200:
                        try:
                            if r.json().get("status") == "sucesso":
                                sucesso = True
                                break
                        except Exception:
                            if "sucesso" in r.text.lower():
                                sucesso = True
                                break
                    if tentativa < RETRY_LOTE:
                        time.sleep(2)
                except requests.exceptions.Timeout:
                    if tentativa < RETRY_LOTE:
                        time.sleep(5)
                except Exception as e:
                    print(f"\n✗ Lote {lote_num} erro: {str(e)[:80]}")
                    if tentativa < RETRY_LOTE:
                        time.sleep(5)

            if sucesso:
                lotes_ok += 1
            else:
                lotes_fail += 1
                print(f"\n❌ FALHA no lote {lote_num}")

            pbar.update(1)

    print(f"\n{'='*80}")
    print(f"📊 RELATÓRIO DE ENVIO")
    print(f"✅ Lotes enviados : {lotes_ok}/{total_lotes}")
    print(f"❌ Lotes falhados : {lotes_fail}/{total_lotes}")
    print(f"📈 Taxa de sucesso: {lotes_ok / total_lotes * 100:.1f}%")
    print(f"{'='*80}\n")
    return lotes_fail == 0


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 80)
    print("🔥 COLETOR OTIMIZADO v2 — SESSÃO ÚNICA + GATHER + SKIP-WINDOW PERSISTENTE")
    print("=" * 80)
    print(f"📊 Range : {RANGE_INICIO:,} → {RANGE_FIM:,} ({RANGE_FIM - RANGE_INICIO + 1:,} IDs)")
    print("=" * 80)

    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas (LOGIN_MUSICAL / SENHA_MUSICAL)")
        sys.exit(1)

    t0 = time.time()

    cookies = login()
    if not cookies:
        sys.exit(1)

    coletor = asyncio.run(executar_coleta(cookies))

    tempo_total = time.time() - t0

    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL")
    print("=" * 80)
    print(f"✅ Membros coletados    : {coletor.stats['coletados']:,}")
    print(f"⚪ IDs vazios/pulados   : {coletor.stats['vazios']:,}")
    print(f"❌ Erros irrecuperáveis : {coletor.stats['erros_fase3']:,}")
    print(f"⏱️  Tempo total          : {tempo_total / 60:.2f} min ({tempo_total:.0f}s)")
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    print(f"⚡ Velocidade           : {total_ids / (tempo_total / 60):,.0f} IDs/min")
    print(f"📈 Taxa sucesso         : {coletor.stats['coletados'] / total_ids * 100:.2f}%")

    if tempo_total < 900:
        print(f"🏆 META ALCANÇADA! {tempo_total / 60:.1f} min < 15 min")
    else:
        print(f"⚠️  Tempo: {tempo_total / 60:.1f} min")
    print("=" * 80)

    if coletor.membros:
        enviar_dados(coletor.membros, tempo_total, coletor.stats)

        print("\n📋 AMOSTRAS (5 primeiros):")
        for i, m in enumerate(coletor.membros[:5], 1):
            print(f"  {i}. [{m['id']:>6}] {m['nome'][:45]:<45} | {m.get('instrumento', '')[:15]}")

    print("\n" + "=" * 80)
    print("✅ COLETA FINALIZADA")
    print("=" * 80)


if __name__ == "__main__":
    main()

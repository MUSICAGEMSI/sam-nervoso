import os
import sys
import re
import json
import asyncio
import aiohttp
import time
import requests
import random
from pathlib import Path
from playwright.sync_api import sync_playwright
from tqdm import tqdm

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ══════════════════════════════════════════════════════════════════════════════
EMAIL           = os.environ.get("LOGIN_MUSICAL")
SENHA           = os.environ.get("SENHA_MUSICAL")
URL_INICIAL     = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = os.environ.get("APPS_SCRIPT_URL", "SUA_URL_AQUI")

RANGE_INICIO = 1
RANGE_FIM    = 850_000

# ── Limites mantidos conforme solicitado ──────────────────────────────────────
SEMAPHORE_PHASE1 = 50
SEMAPHORE_PHASE2 = 30
TIMEOUT_FASE1    = 12.0   # reduzido: respostas lentas vão para retry, não travam
TIMEOUT_FASE2    = 25.0

# ── Chunk maior → menos checkpoints, sessão TCP reutilizada por mais tempo ───
CHUNK_SIZE        = 50_000
SHEETS_BATCH_SIZE = 500

CHECKPOINT_FILE = "/kaggle/working/checkpoint.json"
MEMBROS_FILE    = "/kaggle/working/membros_coletados.jsonl"

_IDS_VAZIOS: set[int] = set()

# ══════════════════════════════════════════════════════════════════════════════
# CHECKPOINT E BACKUP LOCAL
# ══════════════════════════════════════════════════════════════════════════════
def salvar_checkpoint(ultimo_id: int):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"ultimo_id": ultimo_id}, f)

def carregar_checkpoint() -> int:
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
            ultimo = data.get("ultimo_id", RANGE_INICIO)
            print(f"♻️  Checkpoint encontrado — retomando do ID {ultimo}")
            return ultimo
    return RANGE_INICIO

def salvar_membro_local(dados: dict):
    with open(MEMBROS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(dados, ensure_ascii=False) + "\n")

# ══════════════════════════════════════════════════════════════════════════════
# EXTRAÇÃO E REGEX
# ══════════════════════════════════════════════════════════════════════════════
RX_NOME   = re.compile(r'name="nome"[^>]*value="([^"]*)"')
RX_IGREJA = re.compile(r'igreja_selecionada\s*\(\s*(\d+)\s*\)')
_SEL = r'id="{name}"[^>]*>(?:[^<]|<(?!/?select))*?selected[^>]*>\s*([^<\n]+)'
RX_CARGO       = re.compile(_SEL.format(name="id_cargo"),       re.IGNORECASE)
RX_NIVEL       = re.compile(_SEL.format(name="id_nivel"),       re.IGNORECASE)
RX_INSTRUMENTO = re.compile(_SEL.format(name="id_instrumento"), re.IGNORECASE)
RX_TONALIDADE  = re.compile(_SEL.format(name="id_tonalidade"),  re.IGNORECASE)

def extrair_dados(html: str, membro_id: int) -> dict | None:
    if not html or 'name="nome"' not in html:
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
        "igreja_selecionada": _get(RX_IGREJA),
        "cargo_ministerio":   _get(RX_CARGO),
        "nivel":              _get(RX_NIVEL),
        "instrumento":        _get(RX_INSTRUMENTO),
        "tonalidade":         _get(RX_TONALIDADE),
    }

# ══════════════════════════════════════════════════════════════════════════════
# ENVIO PARA O GOOGLE SHEETS
# ══════════════════════════════════════════════════════════════════════════════
def enviar_lote_sheets(membros: list[dict], tentativas: int = 3) -> bool:
    if not membros or URL_APPS_SCRIPT == "SUA_URL_AQUI":
        return False

    payload = {"membros": membros}

    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.post(
                URL_APPS_SCRIPT,
                json=payload,
                timeout=60,
                headers={"Content-Type": "application/json"},
                allow_redirects=True,
            )

            if not resp.text.strip():
                print(f"\n  ⚠️  Resposta vazia (status {resp.status_code})")
                time.sleep(5 * tentativa)
                continue

            data = resp.json()
            if data.get("ok"):
                print(
                    f"\n  📊 Sheets: +{data['gravados']} gravados "
                    f"(total: {data.get('total_na_aba', '?')} | "
                    f"aba: {data.get('aba_atual', '?')})"
                )
                return True
            else:
                print(f"\n  ⚠️  Sheets erro: {data.get('erro')} (tentativa {tentativa})")

        except requests.exceptions.JSONDecodeError:
            print(f"\n  ⚠️  Resposta não-JSON (tentativa {tentativa}): {resp.text[:200]}")
        except Exception as ex:
            print(f"\n  ⚠️  Falha HTTP (tentativa {tentativa}): {ex}")

        if tentativa < tentativas:
            time.sleep(5 * tentativa)

    print(f"\n  ✗ Lote com {len(membros)} membros não gravado após {tentativas} tentativas.")
    return False

# ══════════════════════════════════════════════════════════════════════════════
# COLETOR V6  —  otimizações de desempenho
# ══════════════════════════════════════════════════════════════════════════════
class ColetorV6:
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }

    def __init__(self, cookies: dict):
        self.cookies = cookies
        self.stats   = {"coletados": 0, "vazios": 0, "erros": 0, "enviados_sheets": 0}
        self._retry2, self._retry3 = [], []
        self.membros: list[dict]   = []

        self._buffer_sheets: list[dict] = []
        self._sheets_lock = asyncio.Lock()

        # ── Controle adaptativo de 429 ────────────────────────────────────────
        # Se a taxa de erros 429 subir, um sleep global é adicionado entre
        # tentativas. Isso evita punição desnecessária quando o servidor está ok.
        self._backoff_extra: float = 0.0   # segundos extras de espera global
        self._recent_429: list[float] = []  # timestamps de 429 recentes

    # ── Sessão TCP persistente (reutilizada entre chunks) ─────────────────────
    def _criar_sessao(self, limit: int) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(
            limit=limit,
            ttl_dns_cache=600,
            use_dns_cache=True,
            keepalive_timeout=30,   # mantém conexões TCP vivas entre chunks
        )
        jar = aiohttp.CookieJar(unsafe=True)
        jar.update_cookies(
            self.cookies,
            response_url=aiohttp.client.URL(URL_INICIAL),
        )
        return aiohttp.ClientSession(
            connector=connector,
            cookie_jar=jar,
            headers=self.HEADERS,
        )

    # ── Backoff adaptativo: analisa janela de 10 s ────────────────────────────
    def _atualizar_backoff(self, teve_429: bool):
        agora = time.monotonic()
        if teve_429:
            self._recent_429.append(agora)
        # Mantém apenas os últimos 10 s
        self._recent_429 = [t for t in self._recent_429 if agora - t < 10]
        taxa = len(self._recent_429)           # qtd de 429 nos últimos 10 s
        if taxa >= 10:
            self._backoff_extra = 1.5
        elif taxa >= 5:
            self._backoff_extra = 0.5
        else:
            self._backoff_extra = 0.0

    # ── Flush condicional para Sheets ─────────────────────────────────────────
    async def _flush_sheets_se_cheio(self):
        lote = []
        async with self._sheets_lock:
            if len(self._buffer_sheets) >= SHEETS_BATCH_SIZE:
                lote = self._buffer_sheets[:]
                self._buffer_sheets.clear()
        if lote:
            loop = asyncio.get_event_loop()
            ok = await loop.run_in_executor(None, enviar_lote_sheets, lote)
            if ok:
                self.stats["enviados_sheets"] += len(lote)

    async def _flush_sheets_forcado(self):
        lote = []
        async with self._sheets_lock:
            lote = self._buffer_sheets[:]
            self._buffer_sheets.clear()
        if lote:
            loop = asyncio.get_event_loop()
            ok = await loop.run_in_executor(None, enviar_lote_sheets, lote)
            if ok:
                self.stats["enviados_sheets"] += len(lote)

    # ── Coroutine de coleta — JITTER FORA DO SEMÁFORO (principal otimização) ──
    async def _coletar(
        self,
        session:     aiohttp.ClientSession,
        mid:         int,
        timeout:     float,
        semaphore:   asyncio.Semaphore,
        retry_list:  list | None,
        pbar_queue:  asyncio.Queue,
    ):
        if mid in _IDS_VAZIOS:
            self.stats["vazios"] += 1
            await pbar_queue.put(1)
            return

        # ╔══════════════════════════════════════════════════════════════════╗
        # ║  JITTER FORA DO SEMÁFORO                                        ║
        # ║  Antes: asyncio.sleep ficava DENTRO do "async with semaphore"   ║
        # ║  → O slot ficava bloqueado dormindo (até 0,7 s por requisição)  ║
        # ║  Agora: dormimos ANTES de adquirir o slot                       ║
        # ║  → Semáforo só é ocupado durante a requisição HTTP real         ║
        # ║  Ganho: ~2× no throughput efetivo mantendo 50 conexões          ║
        # ╚══════════════════════════════════════════════════════════════════╝
        jitter = random.uniform(0.05, 0.15) + self._backoff_extra
        await asyncio.sleep(jitter)

        async with semaphore:
            url = f"https://musical.congregacao.org.br/grp_musical/editar/{mid}"
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    allow_redirects=True,
                ) as resp:

                    if resp.status == 200:
                        html  = await resp.text(errors="ignore")
                        dados = extrair_dados(html, mid)
                        if dados:
                            self.membros.append(dados)
                            self.stats["coletados"] += 1
                            salvar_membro_local(dados)
                            async with self._sheets_lock:
                                self._buffer_sheets.append(dados)
                            await self._flush_sheets_se_cheio()
                        else:
                            _IDS_VAZIOS.add(mid)
                            self.stats["vazios"] += 1

                    elif resp.status == 429:
                        self._atualizar_backoff(True)
                        if retry_list is not None:
                            retry_list.append(mid)
                        await asyncio.sleep(random.uniform(3.0, 7.0))

                    elif resp.status in [502, 503, 504, 522]:
                        if retry_list is not None:
                            retry_list.append(mid)
                        await asyncio.sleep(random.uniform(1.0, 3.0))

                    else:
                        _IDS_VAZIOS.add(mid)
                        self.stats["vazios"] += 1

                    self._atualizar_backoff(False)

            except asyncio.TimeoutError:
                # Timeout → retry; não conta como erro permanente
                if retry_list is not None:
                    retry_list.append(mid)
            except Exception:
                if retry_list is not None:
                    retry_list.append(mid)
                self.stats["erros"] += 1

        await pbar_queue.put(1)

    # ── Executa fase com sessão já criada externamente ────────────────────────
    async def _executar_fase_com_sessao(
        self,
        session:    aiohttp.ClientSession,
        ids:        list[int],
        sem:        asyncio.Semaphore,
        timeout:    float,
        retry_list: list | None,
        pbar_queue: asyncio.Queue,
    ):
        await asyncio.gather(*[
            self._coletar(session, mid, timeout, sem, retry_list, pbar_queue)
            for mid in ids
        ])

# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTRAÇÃO
# ══════════════════════════════════════════════════════════════════════════════
async def rodar(cookies: dict) -> ColetorV6:
    coletor   = ColetorV6(cookies)
    inicio    = carregar_checkpoint()
    todos_ids = list(range(inicio, RANGE_FIM + 1))
    chunks    = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]

    print(f"📍 Iniciando do ID {inicio} ({len(todos_ids):,} IDs restantes em {len(chunks)} chunks)")
    print(f"🚀 Semáforo: {SEMAPHORE_PHASE1} conexões | Jitter: 0.05–0.15 s (fora do semáforo)")
    print(f"📦 Chunk: {CHUNK_SIZE:,} IDs | Sheets a cada {SHEETS_BATCH_SIZE} membros")
    print(f"🔗 Apps Script: {URL_APPS_SCRIPT[:70]}{'…' if len(URL_APPS_SCRIPT) > 70 else ''}\n")

    # ── Fase 1 — sessão TCP única para todos os chunks ────────────────────────
    sem1 = asyncio.Semaphore(SEMAPHORE_PHASE1)

    with tqdm(total=len(todos_ids), desc="Fase 1", unit="ID", colour="blue") as pbar:

        # pbar_worker fora do loop de chunks para evitar recriação desnecessária
        pbar_queue: asyncio.Queue = asyncio.Queue()

        async def pbar_worker():
            while True:
                val = await pbar_queue.get()
                if val is None:
                    break
                pbar.update(val)

        worker = asyncio.create_task(pbar_worker())

        # Uma única sessão aiohttp reutilizada por todos os chunks
        # → conexões TCP permanecem vivas (keep-alive), sem overhead de handshake
        async with coletor._criar_sessao(SEMAPHORE_PHASE1) as session:
            for i, chunk in enumerate(chunks):
                await coletor._executar_fase_com_sessao(
                    session, chunk, sem1,
                    TIMEOUT_FASE1, coletor._retry2, pbar_queue,
                )
                salvar_checkpoint(chunk[-1])
                # Pausa mínima entre chunks (flush de buffers, não gargalo)
                if i < len(chunks) - 1:
                    await asyncio.sleep(0.5)

        await pbar_queue.put(None)
        await worker

    # ── Fase 2 — retry ───────────────────────────────────────────────────────
    if coletor._retry2:
        print(f"\n🔄 Retentativa (Fase 2) — {len(coletor._retry2):,} IDs")
        sem2 = asyncio.Semaphore(SEMAPHORE_PHASE2)

        with tqdm(total=len(coletor._retry2), desc="Fase 2", unit="ID", colour="yellow") as pbar:
            pbar_queue2: asyncio.Queue = asyncio.Queue()

            async def pbar_worker2():
                while True:
                    val = await pbar_queue2.get()
                    if val is None:
                        break
                    pbar.update(val)

            worker2 = asyncio.create_task(pbar_worker2())

            async with coletor._criar_sessao(SEMAPHORE_PHASE2) as session2:
                retry_chunks = [
                    coletor._retry2[i:i + CHUNK_SIZE]
                    for i in range(0, len(coletor._retry2), CHUNK_SIZE)
                ]
                for chunk in retry_chunks:
                    await coletor._executar_fase_com_sessao(
                        session2, chunk, sem2,
                        TIMEOUT_FASE2, coletor._retry3, pbar_queue2,
                    )
                    await asyncio.sleep(0.5)

            await pbar_queue2.put(None)
            await worker2

    # ── Flush final ───────────────────────────────────────────────────────────
    print("\n📤 Enviando registros finais ao Sheets…")
    await coletor._flush_sheets_forcado()

    return coletor

# ══════════════════════════════════════════════════════════════════════════════
# LOGIN
# ══════════════════════════════════════════════════════════════════════════════
def login() -> dict:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page    = context.new_page()
        page.goto(URL_INICIAL)
        page.fill('input[name="login"]',    EMAIL)
        page.fill('input[name="password"]', SENHA)
        page.click('button[type="submit"]')
        page.wait_for_selector("nav")
        cookies = {c["name"]: c["value"] for c in context.cookies()}
        browser.close()
        return cookies

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not EMAIL or not SENHA:
        print("✗ Erro: Credenciais não encontradas.")
        sys.exit(1)

    if URL_APPS_SCRIPT == "SUA_URL_AQUI":
        print("⚠️  AVISO: APPS_SCRIPT_URL não configurada. Dados NÃO serão enviados ao Sheets.\n")

    ck  = login()
    res = asyncio.run(rodar(ck))

    print(
        f"\n✅ Concluído!\n"
        f"   Membros coletados : {res.stats['coletados']:,}\n"
        f"   Vazios            : {res.stats['vazios']:,}\n"
        f"   Erros de rede     : {res.stats['erros']:,}\n"
        f"   Enviados ao Sheets: {res.stats['enviados_sheets']:,}\n"
        f"   IDs para retry 3  : {len(res._retry3):,}"
    )

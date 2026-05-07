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

# ── Workers: 300 conexões @ 10s médios = apenas 30 req/s no servidor ─────────
# (antes: 50 workers @ 10s = 5 req/s — literalmente impossível fazer mais rápido)
NUM_WORKERS   = 300
NUM_WORKERS_R = 100
TIMEOUT_FASE1 = 12.0
TIMEOUT_FASE2 = 22.0

CHUNK_SIZE        = 10_000
SHEETS_BATCH_SIZE = 500

CHECKPOINT_FILE = "/kaggle/working/checkpoint.json"
MEMBROS_FILE    = "/kaggle/working/membros_coletados.jsonl"

_IDS_VAZIOS: set[int] = set()

# ══════════════════════════════════════════════════════════════════════════════
# CHECKPOINT
# ══════════════════════════════════════════════════════════════════════════════
def salvar_checkpoint(ultimo_id: int):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"ultimo_id": ultimo_id}, f)

def carregar_checkpoint() -> int:
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE) as f:
            data   = json.load(f)
            ultimo = data.get("ultimo_id", RANGE_INICIO)
            print(f"♻️  Checkpoint — retomando do ID {ultimo:,}")
            return ultimo
    return RANGE_INICIO

def salvar_membro_local(dados: dict):
    with open(MEMBROS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(dados, ensure_ascii=False) + "\n")

# ══════════════════════════════════════════════════════════════════════════════
# EXTRAÇÃO
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
    if not m or not m.group(1).strip():
        return None

    def _get(rx):
        r = rx.search(html)
        return r.group(1).strip() if r else ""

    return {
        "id":                 membro_id,
        "nome":               m.group(1).strip(),
        "igreja_selecionada": _get(RX_IGREJA),
        "cargo_ministerio":   _get(RX_CARGO),
        "nivel":              _get(RX_NIVEL),
        "instrumento":        _get(RX_INSTRUMENTO),
        "tonalidade":         _get(RX_TONALIDADE),
    }

# ══════════════════════════════════════════════════════════════════════════════
# SHEETS
# ══════════════════════════════════════════════════════════════════════════════
def enviar_lote_sheets(membros: list[dict], tentativas: int = 3) -> bool:
    if not membros or URL_APPS_SCRIPT == "SUA_URL_AQUI":
        return False
    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.post(
                URL_APPS_SCRIPT,
                json={"membros": membros},
                timeout=60,
                headers={"Content-Type": "application/json"},
                allow_redirects=True,
            )
            if not resp.text.strip():
                time.sleep(5 * tentativa)
                continue
            data = resp.json()
            if data.get("ok"):
                print(
                    f"\n  📊 Sheets +{data['gravados']} | "
                    f"total: {data.get('total_na_aba','?')} | "
                    f"aba: {data.get('aba_atual','?')}"
                )
                return True
            print(f"\n  ⚠️  Sheets erro: {data.get('erro')} (t{tentativa})")
        except Exception as ex:
            print(f"\n  ⚠️  HTTP (t{tentativa}): {ex}")
        if tentativa < tentativas:
            time.sleep(5 * tentativa)
    return False

# ══════════════════════════════════════════════════════════════════════════════
# DIAGNÓSTICO — roda antes da coleta para medir o servidor
# ══════════════════════════════════════════════════════════════════════════════
async def diagnosticar(cookies: dict):
    """
    Mede o tempo médio de resposta do servidor e detecta se IDs vazios
    retornam redirect (rápido) ou 200 (lento, precisamos baixar HTML).
    Isso determina o throughput máximo possível.
    """
    print("🔬 Diagnóstico do servidor (10 amostras)…")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0",
        "Accept": "text/html,*/*;q=0.8",
    }
    jar = aiohttp.CookieJar(unsafe=True)
    jar.update_cookies(cookies, response_url=aiohttp.client.URL(URL_INICIAL))

    amostras_vazias = []
    amostras_cheias = []

    async with aiohttp.ClientSession(cookie_jar=jar, headers=headers) as session:
        # Testa IDs no início do range (provavelmente vazios)
        for mid in range(1, 11):
            t0 = time.monotonic()
            try:
                async with session.get(
                    f"https://musical.congregacao.org.br/grp_musical/editar/{mid}",
                    timeout=aiohttp.ClientTimeout(total=15),
                    allow_redirects=False,   # detecta redirect sem seguir
                ) as resp:
                    elapsed = time.monotonic() - t0
                    if resp.status in (301, 302, 303, 307, 308):
                        amostras_vazias.append(elapsed)
                        tipo = f"REDIRECT → {resp.headers.get('Location','?')[:50]}"
                    elif resp.status == 200:
                        html = await resp.text(errors="ignore")
                        tem_dado = 'name="nome"' in html
                        amostras_cheias.append(elapsed) if tem_dado else amostras_vazias.append(elapsed)
                        tipo = "200+DADOS" if tem_dado else "200+VAZIO"
                    else:
                        amostras_vazias.append(elapsed)
                        tipo = f"HTTP {resp.status}"
                    print(f"   ID {mid:>6}: {elapsed:.2f}s  [{tipo}]")
            except Exception as ex:
                print(f"   ID {mid:>6}: ERRO — {ex}")

    med_vazio = (sum(amostras_vazias) / len(amostras_vazias)) if amostras_vazias else 0
    med_cheio = (sum(amostras_cheias) / len(amostras_cheias)) if amostras_cheias else 0
    print(f"\n   ⏱  Média IDs vazios : {med_vazio:.2f}s")
    print(f"   ⏱  Média IDs com dado: {med_cheio:.2f}s")
    print(f"   📈 Throughput estimado ({NUM_WORKERS} workers): "
          f"~{NUM_WORKERS / max(med_vazio, 0.05):.0f} ID/s (vazios)\n")
    return med_vazio, med_cheio

# ══════════════════════════════════════════════════════════════════════════════
# COLETOR — Worker Pool + detecção rápida de redirect
# ══════════════════════════════════════════════════════════════════════════════
class ColetorV8:
    """
    Estratégia dupla por requisição:
    1. GET com allow_redirects=False  →  se redirecionar = vazio, PULAMOS o body
                                          custo: apenas o RTT do request inicial (~50-200ms)
    2. Se status 200                  →  baixamos o HTML e extraímos dados
                                          custo: response_time completo (~2-15s)

    Isso separa os IDs vazios (rápidos) dos válidos (lentos), aumentando o
    throughput efetivo mesmo mantendo a carga total sobre o servidor baixa.
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Cache-Control":   "no-cache",
        "Connection":      "keep-alive",
    }

    def __init__(self, cookies: dict):
        self.cookies = cookies
        self.stats   = {
            "coletados": 0, "vazios": 0, "erros": 0,
            "enviados_sheets": 0, "redirects": 0,
        }
        self._retry2: list[int] = []
        self._retry3: list[int] = []
        self.membros: list[dict] = []

        self._buffer_sheets: list[dict] = []
        self._sheets_lock = asyncio.Lock()

        self._backoff_extra: float      = 0.0
        self._recent_429: list[float]   = []

        # Stats de tempo para ajuste dinâmico
        self._tempo_total:  float = 0.0
        self._reqs_ok:      int   = 0

    def _criar_sessao(self, n: int) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(
            limit            = n + 20,
            ttl_dns_cache    = 600,
            use_dns_cache    = True,
            keepalive_timeout= 60,
            enable_cleanup_closed=True,
        )
        jar = aiohttp.CookieJar(unsafe=True)
        jar.update_cookies(self.cookies, response_url=aiohttp.client.URL(URL_INICIAL))
        return aiohttp.ClientSession(connector=connector, cookie_jar=jar, headers=self.HEADERS)

    def _atualizar_backoff(self, teve_429: bool):
        agora = time.monotonic()
        if teve_429:
            self._recent_429.append(agora)
        self._recent_429 = [t for t in self._recent_429 if agora - t < 10]
        n = len(self._recent_429)
        self._backoff_extra = 2.0 if n >= 10 else (0.8 if n >= 5 else 0.0)

    async def _flush_se_cheio(self):
        lote = []
        async with self._sheets_lock:
            if len(self._buffer_sheets) >= SHEETS_BATCH_SIZE:
                lote = self._buffer_sheets[:]
                self._buffer_sheets.clear()
        if lote:
            ok = await asyncio.get_event_loop().run_in_executor(None, enviar_lote_sheets, lote)
            if ok:
                self.stats["enviados_sheets"] += len(lote)

    async def _flush_forcado(self):
        lote = []
        async with self._sheets_lock:
            lote = self._buffer_sheets[:]
            self._buffer_sheets.clear()
        if lote:
            ok = await asyncio.get_event_loop().run_in_executor(None, enviar_lote_sheets, lote)
            if ok:
                self.stats["enviados_sheets"] += len(lote)

    async def _worker(
        self,
        session:    aiohttp.ClientSession,
        fila:       asyncio.Queue,
        timeout:    float,
        retry_list: list | None,
        pbar_queue: asyncio.Queue,
    ):
        while True:
            try:
                mid = fila.get_nowait()
            except asyncio.QueueEmpty:
                return

            if mid in _IDS_VAZIOS:
                self.stats["vazios"] += 1
                await pbar_queue.put(1)
                fila.task_done()
                continue

            if self._backoff_extra > 0:
                await asyncio.sleep(self._backoff_extra + random.uniform(0, 0.5))

            url = f"https://musical.congregacao.org.br/grp_musical/editar/{mid}"
            t0  = time.monotonic()
            try:
                # ── Passo 1: GET sem seguir redirect ─────────────────────────
                # IDs inexistentes costumam redirecionar para lista/login.
                # Detectar isso evita baixar HTML desnecessariamente.
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                    allow_redirects=False,   # ← chave da otimização
                ) as resp:

                    if resp.status in (301, 302, 303, 307, 308):
                        # Redirect = membro não existe (muito rápido, só RTT)
                        _IDS_VAZIOS.add(mid)
                        self.stats["vazios"]    += 1
                        self.stats["redirects"] += 1

                    elif resp.status == 200:
                        # ── Passo 2: só aqui baixamos o body ─────────────────
                        html  = await resp.text(errors="ignore")
                        dados = extrair_dados(html, mid)
                        if dados:
                            self.membros.append(dados)
                            self.stats["coletados"] += 1
                            salvar_membro_local(dados)
                            async with self._sheets_lock:
                                self._buffer_sheets.append(dados)
                            await self._flush_se_cheio()
                        else:
                            _IDS_VAZIOS.add(mid)
                            self.stats["vazios"] += 1

                    elif resp.status == 429:
                        self._atualizar_backoff(True)
                        if retry_list is not None:
                            retry_list.append(mid)
                        await asyncio.sleep(random.uniform(5.0, 10.0))

                    elif resp.status in (502, 503, 504, 522):
                        if retry_list is not None:
                            retry_list.append(mid)
                        await asyncio.sleep(random.uniform(1.0, 3.0))

                    else:
                        _IDS_VAZIOS.add(mid)
                        self.stats["vazios"] += 1

                    self._atualizar_backoff(False)
                    elapsed = time.monotonic() - t0
                    self._tempo_total += elapsed
                    self._reqs_ok     += 1

            except asyncio.TimeoutError:
                if retry_list is not None:
                    retry_list.append(mid)
            except Exception:
                if retry_list is not None:
                    retry_list.append(mid)
                self.stats["erros"] += 1

            await pbar_queue.put(1)
            fila.task_done()

    async def _executar(
        self,
        session:    aiohttp.ClientSession,
        ids:        list[int],
        n_workers:  int,
        timeout:    float,
        retry_list: list | None,
        pbar_queue: asyncio.Queue,
    ):
        fila: asyncio.Queue = asyncio.Queue()
        for mid in ids:
            fila.put_nowait(mid)

        n = min(n_workers, len(ids))
        await asyncio.gather(*[
            asyncio.create_task(
                self._worker(session, fila, timeout, retry_list, pbar_queue)
            )
            for _ in range(n)
        ])

# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTRAÇÃO
# ══════════════════════════════════════════════════════════════════════════════
async def rodar(cookies: dict) -> ColetorV8:
    # Diagnóstico primeiro — entender o servidor antes de bombardeá-lo
    await diagnosticar(cookies)

    coletor   = ColetorV8(cookies)
    inicio    = carregar_checkpoint()
    todos_ids = list(range(inicio, RANGE_FIM + 1))
    chunks    = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]

    print(f"📍 ID inicial  : {inicio:,}  ({len(todos_ids):,} IDs / {len(chunks)} chunks)")
    print(f"🚀 Workers     : {NUM_WORKERS} fase1 / {NUM_WORKERS_R} retry")
    print(f"⚡ Redirect-skip: IDs vazios detectados sem baixar HTML")
    print(f"📦 Chunk       : {CHUNK_SIZE:,}  |  Sheets a cada {SHEETS_BATCH_SIZE}\n")

    pbar_queue: asyncio.Queue = asyncio.Queue()

    async def pbar_worker():
        last_report = time.monotonic()
        while True:
            v = await pbar_queue.get()
            if v is None:
                break
            pbar.update(v)
            # Relatório de velocidade a cada 30s
            agora = time.monotonic()
            if agora - last_report >= 30:
                med = (coletor._tempo_total / coletor._reqs_ok) if coletor._reqs_ok else 0
                print(
                    f"\n  ⚡ {pbar.n:,} IDs | "
                    f"coletados: {coletor.stats['coletados']:,} | "
                    f"redirects: {coletor.stats['redirects']:,} | "
                    f"tempo médio req: {med:.2f}s | "
                    f"backoff: {coletor._backoff_extra:.1f}s"
                )
                last_report = agora

    with tqdm(
        total=len(todos_ids), desc="Fase 1",
        unit="ID", unit_scale=True, colour="blue",
        dynamic_ncols=True,
    ) as pbar:
        pb_task = asyncio.create_task(pbar_worker())
        async with coletor._criar_sessao(NUM_WORKERS) as session:
            for i, chunk in enumerate(chunks):
                await coletor._executar(
                    session, chunk, NUM_WORKERS,
                    TIMEOUT_FASE1, coletor._retry2, pbar_queue,
                )
                salvar_checkpoint(chunk[-1])
                if i < len(chunks) - 1:
                    await asyncio.sleep(0.1)
        await pbar_queue.put(None)
        await pb_task

    if coletor._retry2:
        print(f"\n🔄 Retry (Fase 2) — {len(coletor._retry2):,} IDs")
        pbar_queue2: asyncio.Queue = asyncio.Queue()

        async def pbar_worker2():
            while True:
                v = await pbar_queue2.get()
                if v is None:
                    break
                pbar2.update(v)

        r_chunks = [
            coletor._retry2[i:i + CHUNK_SIZE]
            for i in range(0, len(coletor._retry2), CHUNK_SIZE)
        ]
        with tqdm(
            total=len(coletor._retry2), desc="Fase 2",
            unit="ID", unit_scale=True, colour="yellow",
            dynamic_ncols=True,
        ) as pbar2:
            pb_task2 = asyncio.create_task(pbar_worker2())
            async with coletor._criar_sessao(NUM_WORKERS_R) as session2:
                for chunk in r_chunks:
                    await coletor._executar(
                        session2, chunk, NUM_WORKERS_R,
                        TIMEOUT_FASE2, coletor._retry3, pbar_queue2,
                    )
                    await asyncio.sleep(0.1)
            await pbar_queue2.put(None)
            await pb_task2

    print("\n📤 Enviando registros finais ao Sheets…")
    await coletor._flush_forcado()
    return coletor

# ══════════════════════════════════════════════════════════════════════════════
# LOGIN
# ══════════════════════════════════════════════════════════════════════════════
def login() -> dict:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx     = browser.new_context()
        page    = ctx.new_page()
        page.goto(URL_INICIAL)
        page.fill('input[name="login"]',    EMAIL)
        page.fill('input[name="password"]', SENHA)
        page.click('button[type="submit"]')
        page.wait_for_selector("nav")
        cookies = {c["name"]: c["value"] for c in ctx.cookies()}
        browser.close()
        return cookies

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas.")
        sys.exit(1)
    if URL_APPS_SCRIPT == "SUA_URL_AQUI":
        print("⚠️  APPS_SCRIPT_URL não configurada — sem envio ao Sheets.\n")

    ck  = login()
    res = asyncio.run(rodar(ck))

    med = (res._tempo_total / res._reqs_ok) if res._reqs_ok else 0
    print(
        f"\n✅ Concluído!\n"
        f"   Coletados          : {res.stats['coletados']:,}\n"
        f"   Vazios (200+vazio) : {res.stats['vazios'] - res.stats['redirects']:,}\n"
        f"   Redirects (rápido) : {res.stats['redirects']:,}\n"
        f"   Erros de rede      : {res.stats['erros']:,}\n"
        f"   Enviados ao Sheets : {res.stats['enviados_sheets']:,}\n"
        f"   Tempo médio/req    : {med:.2f}s\n"
        f"   Fila retry 3       : {len(res._retry3):,}"
    )

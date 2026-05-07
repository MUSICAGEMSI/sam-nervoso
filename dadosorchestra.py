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

NUM_WORKERS      = 50    # workers fase 1  — mantido conforme solicitado
NUM_WORKERS_R    = 30    # workers fase 2 (retry)
TIMEOUT_FASE1    = 10.0  # timeout reduzido: respostas lentas → retry, não travam worker
TIMEOUT_FASE2    = 20.0
CHUNK_SIZE       = 10_000   # checkpoint a cada 10 K IDs (~100 s de trabalho)
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
            data  = json.load(f)
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
                    f"\n  📊 Sheets: +{data['gravados']} | "
                    f"total: {data.get('total_na_aba','?')} | "
                    f"aba: {data.get('aba_atual','?')}"
                )
                return True
            print(f"\n  ⚠️  Sheets erro: {data.get('erro')} (t{tentativa})")
        except Exception as ex:
            print(f"\n  ⚠️  Falha HTTP (t{tentativa}): {ex}")
        if tentativa < tentativas:
            time.sleep(5 * tentativa)
    return False

# ══════════════════════════════════════════════════════════════════════════════
# COLETOR — padrão Worker Pool
# ══════════════════════════════════════════════════════════════════════════════
class ColetorV7:
    """
    Arquitetura Worker Pool:
    - N workers fixos consomem de uma asyncio.Queue
    - Sem asyncio.gather() em cima de 50K coroutines
    - Sem semáforo (a fila + workers já controlam a concorrência)
    - Cada worker fica 100% ocupado fazendo I/O, nunca dormindo
    - Backoff ativado apenas quando 429s aparecem
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Cache-Control":   "no-cache",
        "Connection":      "keep-alive",
    }

    def __init__(self, cookies: dict):
        self.cookies  = cookies
        self.stats    = {"coletados": 0, "vazios": 0, "erros": 0, "enviados_sheets": 0}
        self._retry2: list[int] = []
        self._retry3: list[int] = []
        self.membros: list[dict] = []

        self._buffer_sheets: list[dict] = []
        self._sheets_lock = asyncio.Lock()

        # Backoff global: só ativa quando 429s surgem
        self._backoff_extra: float    = 0.0
        self._recent_429:   list[float] = []

    def _criar_sessao(self, n_workers: int) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(
            limit=n_workers + 5,   # leve margem acima do nº de workers
            ttl_dns_cache=600,
            use_dns_cache=True,
            keepalive_timeout=60,
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
        self._backoff_extra = 1.5 if n >= 10 else (0.5 if n >= 5 else 0.0)

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

    # ── Worker: consome IDs da fila até ela esvaziar ──────────────────────────
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
                return   # fila vazia → worker encerra

            if mid in _IDS_VAZIOS:
                self.stats["vazios"] += 1
                await pbar_queue.put(1)
                fila.task_done()
                continue

            # Backoff global (só ativo se houver 429s recentes)
            if self._backoff_extra > 0:
                await asyncio.sleep(self._backoff_extra + random.uniform(0, 0.3))

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
                            await self._flush_se_cheio()
                        else:
                            _IDS_VAZIOS.add(mid)
                            self.stats["vazios"] += 1

                    elif resp.status == 429:
                        self._atualizar_backoff(True)
                        if retry_list is not None:
                            retry_list.append(mid)
                        await asyncio.sleep(random.uniform(4.0, 8.0))

                    elif resp.status in (502, 503, 504, 522):
                        if retry_list is not None:
                            retry_list.append(mid)
                        await asyncio.sleep(random.uniform(1.0, 3.0))

                    else:
                        _IDS_VAZIOS.add(mid)
                        self.stats["vazios"] += 1

                    self._atualizar_backoff(False)

            except asyncio.TimeoutError:
                if retry_list is not None:
                    retry_list.append(mid)
            except Exception:
                if retry_list is not None:
                    retry_list.append(mid)
                self.stats["erros"] += 1

            await pbar_queue.put(1)
            fila.task_done()

    # ── Roda uma lista de IDs com N workers e sessão já criada ───────────────
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

        # Exatamente N workers — sem coroutines ociosas empilhadas
        workers = [
            asyncio.create_task(
                self._worker(session, fila, timeout, retry_list, pbar_queue)
            )
            for _ in range(min(n_workers, len(ids)))
        ]
        await asyncio.gather(*workers)

# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTRAÇÃO
# ══════════════════════════════════════════════════════════════════════════════
async def rodar(cookies: dict) -> ColetorV7:
    coletor   = ColetorV7(cookies)
    inicio    = carregar_checkpoint()
    todos_ids = list(range(inicio, RANGE_FIM + 1))
    chunks    = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]

    print(f"📍 ID inicial : {inicio:,}  ({len(todos_ids):,} IDs em {len(chunks)} chunks)")
    print(f"🚀 Workers    : {NUM_WORKERS} (Fase 1) / {NUM_WORKERS_R} (Retry)")
    print(f"📦 Chunk      : {CHUNK_SIZE:,} IDs  |  Sheets a cada {SHEETS_BATCH_SIZE}")
    print(f"🔗 Apps Script: {URL_APPS_SCRIPT[:70]}{'…' if len(URL_APPS_SCRIPT)>70 else ''}\n")

    # ── pbar_worker único para toda a Fase 1 ─────────────────────────────────
    pbar_queue: asyncio.Queue = asyncio.Queue()

    async def pbar_worker():
        while True:
            v = await pbar_queue.get()
            if v is None:
                break
            pbar.update(v)

    # ── Fase 1 — sessão TCP única, reutilizada entre todos os chunks ──────────
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
                    await asyncio.sleep(0.2)   # pausa mínima entre chunks

        await pbar_queue.put(None)
        await pb_task

    # ── Fase 2 — retry ───────────────────────────────────────────────────────
    if coletor._retry2:
        print(f"\n🔄 Retry (Fase 2) — {len(coletor._retry2):,} IDs")
        pbar_queue2: asyncio.Queue = asyncio.Queue()
        r_chunks = [
            coletor._retry2[i:i + CHUNK_SIZE]
            for i in range(0, len(coletor._retry2), CHUNK_SIZE)
        ]

        async def pbar_worker2():
            while True:
                v = await pbar_queue2.get()
                if v is None:
                    break
                pbar2.update(v)

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
                    await asyncio.sleep(0.2)
            await pbar_queue2.put(None)
            await pb_task2

    # ── Flush final ───────────────────────────────────────────────────────────
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
        print("⚠️  APPS_SCRIPT_URL não configurada — dados não irão ao Sheets.\n")

    ck  = login()
    res = asyncio.run(rodar(ck))

    print(
        f"\n✅ Concluído!\n"
        f"   Coletados         : {res.stats['coletados']:,}\n"
        f"   Vazios/inexistentes: {res.stats['vazios']:,}\n"
        f"   Erros de rede     : {res.stats['erros']:,}\n"
        f"   Enviados ao Sheets: {res.stats['enviados_sheets']:,}\n"
        f"   Fila retry 3      : {len(res._retry3):,}"
    )

import os
import sys
import re
import asyncio
import aiohttp
import time
import requests
import random
from playwright.sync_api import sync_playwright
from tqdm import tqdm

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ══════════════════════════════════════════════════════════════════════════════
EMAIL  = os.environ.get("LOGIN_MUSICAL")
SENHA  = os.environ.get("SENHA_MUSICAL")
URL_INICIAL     = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = os.environ.get("APPS_SCRIPT_URL", "https://script.google.com/macros/library/d/1hBeANkb3D64gY6vWLv-0BsweH2ZdeQid0P95r3yGvkBySpPiQEvkLdQH/2")

RANGE_INICIO = 1
RANGE_FIM    = 850_000

# ── Concorrência ──────────────────────────────────────────────────────────────
SEMAPHORE_PHASE1 = 50
SEMAPHORE_PHASE2 = 30

TIMEOUT_FASE1 = 15.0
TIMEOUT_FASE2 = 25.0

CHUNK_SIZE = 20_000

# Quantos membros acumular antes de enviar um lote ao Sheets
SHEETS_BATCH_SIZE = 500

_IDS_VAZIOS: set[int] = set()

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
    if not html or 'name="nome"' not in html: return None
    m = RX_NOME.search(html)
    if not m: return None
    nome = m.group(1).strip()
    if not nome: return None

    def _get(rx):
        r = rx.search(html)
        return r.group(1).strip() if r else ""

    return {
        "id": membro_id, "nome": nome,
        "igreja_selecionada":  _get(RX_IGREJA),
        "cargo_ministerio":    _get(RX_CARGO),
        "nivel":               _get(RX_NIVEL),
        "instrumento":         _get(RX_INSTRUMENTO),
        "tonalidade":          _get(RX_TONALIDADE),
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
            )
            data = resp.json()
            if data.get("ok"):
                print(
                    f"\n  📊 Sheets: +{data['gravados']} gravados "
                    f"(total: {data.get('total_na_aba', '?')})"
                )
                return True
            else:
                print(f"\n  ⚠️  Sheets erro: {data.get('erro')} (tentativa {tentativa})")
        except Exception as ex:
            print(f"\n  ⚠️  Falha HTTP (tentativa {tentativa}): {ex}")

        if tentativa < tentativas:
            time.sleep(5 * tentativa)

    print(f"\n  ✗ Lote com {len(membros)} membros não foi gravado após {tentativas} tentativas.")
    return False

# ══════════════════════════════════════════════════════════════════════════════
# COLETOR V5
# ══════════════════════════════════════════════════════════════════════════════
class ColetorV5:
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }

    def __init__(self, cookies: dict):
        self.cookies = cookies
        self.stats = {"coletados": 0, "vazios": 0, "erros": 0, "enviados_sheets": 0}
        self._retry2, self._retry3, self.membros = [], [], []
        self._buffer_sheets: list[dict] = []
        self._sheets_lock = asyncio.Lock()

    def _criar_sessao(self, limit: int) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(limit=limit, ttl_dns_cache=600, use_dns_cache=True)
        jar = aiohttp.CookieJar(unsafe=True)
        jar.update_cookies(self.cookies, response_url=aiohttp.client.URL(URL_INICIAL))
        return aiohttp.ClientSession(connector=connector, cookie_jar=jar, headers=self.HEADERS)

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

    async def _coletar(self, session, mid, timeout, semaphore, retry_list, pbar_queue):
        if mid in _IDS_VAZIOS:
            self.stats["vazios"] += 1
            await pbar_queue.put(1)
            return

        async with semaphore:
            await asyncio.sleep(random.uniform(0.2, 0.7))
            url = f"https://musical.congregacao.org.br/grp_musical/editar/{mid}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status == 200:
                        html = await resp.text(errors="ignore")
                        dados = extrair_dados(html, mid)
                        if dados:
                            self.membros.append(dados)
                            self.stats["coletados"] += 1
                            async with self._sheets_lock:
                                self._buffer_sheets.append(dados)
                            await self._flush_sheets_se_cheio()
                        else:
                            _IDS_VAZIOS.add(mid)
                            self.stats["vazios"] += 1
                    elif resp.status in [429, 502, 503, 504, 522]:
                        if retry_list is not None: retry_list.append(mid)
                        await asyncio.sleep(5)
                    else:
                        _IDS_VAZIOS.add(mid)
                        self.stats["vazios"] += 1
            except Exception:
                if retry_list is not None: retry_list.append(mid)
                self.stats["erros"] += 1

            await pbar_queue.put(1)

    async def _executar_fase(self, ids, sem_n, timeout, retry_list, pbar):
        pbar_queue = asyncio.Queue()

        async def pbar_worker():
            while True:
                val = await pbar_queue.get()
                if val is None: break
                pbar.update(val)

        worker = asyncio.create_task(pbar_worker())
        sem = asyncio.Semaphore(sem_n)
        async with self._criar_sessao(sem_n) as session:
            await asyncio.gather(*[
                self._coletar(session, mid, timeout, sem, retry_list, pbar_queue)
                for mid in ids
            ])

        await pbar_queue.put(None)
        await worker

# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTRAÇÃO
# ══════════════════════════════════════════════════════════════════════════════
async def rodar(cookies):
    coletor = ColetorV5(cookies)
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))

    print(f"\n🚀 MODO AMIGÁVEL: {SEMAPHORE_PHASE1} conexões simultâneas")
    print(f"⏳ Jitter ativo (0.2s - 0.7s)")
    print(f"📊 Envio ao Sheets a cada {SHEETS_BATCH_SIZE} membros")
    print(f"🔗 Apps Script: {URL_APPS_SCRIPT[:70]}{'…' if len(URL_APPS_SCRIPT) > 70 else ''}\n")

    # ── Fase 1 ────────────────────────────────────────────────────────────────
    with tqdm(total=len(todos_ids), desc="Fase 1", unit="ID", colour="blue") as pbar:
        chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]
        for c in chunks:
            await coletor._executar_fase(c, SEMAPHORE_PHASE1, TIMEOUT_FASE1, coletor._retry2, pbar)
            await asyncio.sleep(2)

    # ── Fase 2 (retry) ────────────────────────────────────────────────────────
    if coletor._retry2:
        print(f"\n🔄 Retentativa (Fase 2) — {len(coletor._retry2)} IDs")
        with tqdm(total=len(coletor._retry2), desc="Fase 2", unit="ID", colour="yellow") as pbar:
            await coletor._executar_fase(
                coletor._retry2, SEMAPHORE_PHASE2, TIMEOUT_FASE2, coletor._retry3, pbar
            )

    # ── Flush final ───────────────────────────────────────────────────────────
    print("\n📤 Enviando registros finais ao Sheets…")
    await coletor._flush_sheets_forcado()

    return coletor

# ══════════════════════════════════════════════════════════════════════════════
# LOGIN
# ══════════════════════════════════════════════════════════════════════════════
def login():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto(URL_INICIAL)
        page.fill('input[name="login"]', EMAIL)
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

    ck = login()
    res = asyncio.run(rodar(ck))

    print(
        f"\n✅ Concluído!\n"
        f"   Membros coletados : {res.stats['coletados']}\n"
        f"   Vazios            : {res.stats['vazios']}\n"
        f"   Erros de rede     : {res.stats['erros']}\n"
        f"   Enviados ao Sheets: {res.stats['enviados_sheets']}"
    )

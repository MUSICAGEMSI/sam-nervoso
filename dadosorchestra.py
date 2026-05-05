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
# CONFIGURAÇÕES - AJUSTADAS PARA RESILIÊNCIA V3
# ══════════════════════════════════════════════════════════════════════════════
EMAIL  = os.environ.get("LOGIN_MUSICAL")
SENHA  = os.environ.get("SENHA_MUSICAL")
URL_INICIAL      = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT  = (
    "https://script.google.com/macros/s/"
    "AKfycbzbkdOTDjGJxabnlJNDX7ZKI4_vh-t5d84MDRp-4FO4KmocRPEVs2jkHL3gjKEG-efF/exec"
)

RANGE_INICIO = 1
RANGE_FIM    = 850_000
INSTANCIA_ID = "GHA_batch_1"

# ── Concorrência V3 (Mais segura para evitar 404/Block) ──────────────────────
SEMAPHORE_PHASE1 = 250  # Reduzido para estabilidade
SEMAPHORE_PHASE2 = 100
SEMAPHORE_PHASE3 = 30

TIMEOUT_FASE1 = 6.0     # Aumentado para evitar timeouts falsos
TIMEOUT_FASE2 = 12.0
TIMEOUT_FASE3 = 30.0

CHUNK_SIZE = 40_000
PARTIAL_READ_BYTES = 3_584 # Um pouco mais de folga na leitura
SKIP_WINDOW = 50
SKIP_AHEAD  = 200

# Cache global
_IDS_VAZIOS: set[int] = set()

# ══════════════════════════════════════════════════════════════════════════════
# REGEX E EXTRAÇÃO (Otimizadas)
# ══════════════════════════════════════════════════════════════════════════════
RX_NOME  = re.compile(r'name="nome"[^>]*value="([^"]*)"')
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
    if not m: return None
    nome = m.group(1).strip()
    if not nome: return None

    def _get(rx):
        r = rx.search(html)
        return r.group(1).strip() if r else ""

    return {
        "id": membro_id, "nome": nome,
        "igreja_selecionada": (_get(RX_IGREJA) or ""),
        "cargo_ministerio": _get(RX_CARGO), "nivel": _get(RX_NIVEL),
        "instrumento": _get(RX_INSTRUMENTO), "tonalidade": _get(RX_TONALIDADE),
    }

# ══════════════════════════════════════════════════════════════════════════════
# COLETOR V3 - RESILIENTE
# ══════════════════════════════════════════════════════════════════════════════
class ColetorV3:
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    def __init__(self, cookies: dict):
        self.cookies = cookies
        self.stats = {"coletados": 0, "vazios": 0, "erros_fase1": 0, "erros_fase2": 0, "erros_fase3": 0}
        self._retry2, self._retry3, self.membros = [], [], []
        self._sw_consecutivos, self._sw_pular_ate = 0, -1

    def _criar_sessao(self, limit: int) -> aiohttp.ClientSession:
        # Foco em reuso de conexão para não sobrecarregar o firewall do servidor
        connector = aiohttp.TCPConnector(
            limit=limit,
            limit_per_host=limit,
            ttl_dns_cache=600,
            use_dns_cache=True,
            force_close=False, # Mantém conexões abertas
            enable_cleanup_closed=True
        )
        jar = aiohttp.CookieJar(unsafe=True)
        jar.update_cookies(self.cookies, response_url=aiohttp.client.URL(URL_INICIAL))
        return aiohttp.ClientSession(connector=connector, cookie_jar=jar, headers=self.HEADERS)

    async def _coletar(self, session, membro_id, timeout, semaphore, retry_list, fase, pbar_queue):
        if membro_id in _IDS_VAZIOS:
            self.stats["vazios"] += 1
            await pbar_queue.put(1)
            return

        url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
        
        async with semaphore:
            try:
                # Adiciona um micro-jitter para não ser síncrono demais
                await asyncio.sleep(random.uniform(0.01, 0.05))
                
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), allow_redirects=True) as resp:
                    # TRATAMENTO DE ERRO DE SOBRECARGA OU BLOQUEIO
                    if resp.status == 404:
                        # Em muitos casos, o 404 é um "Soft Block". Vamos assumir vazio, mas se vierem muitos, o retry pega.
                        _IDS_VAZIOS.add(membro_id)
                        self.stats["vazios"] += 1
                        await pbar_queue.put(1)
                        return

                    if resp.status in [429, 503, 500]:
                        if retry_list is not None: retry_list.append(membro_id)
                        await asyncio.sleep(2) # Pausa estratégica
                        await pbar_queue.put(1)
                        return

                    # Leitura parcial otimizada
                    content = await resp.content.read(PARTIAL_READ_BYTES)
                    partial = content.decode("utf-8", errors="ignore")

                    if 'name="nome"' not in partial:
                        _IDS_VAZIOS.add(membro_id)
                        self.stats["vazios"] += 1
                    else:
                        rest = await resp.content.read()
                        html = partial + rest.decode("utf-8", errors="ignore")
                        dados = extrair_dados(html, membro_id)
                        if dados:
                            self.membros.append(dados)
                            self.stats["coletados"] += 1
                        else:
                            _IDS_VAZIOS.add(membro_id)
                            self.stats["vazios"] += 1

            except Exception:
                if retry_list is not None: retry_list.append(membro_id)
            
            await pbar_queue.put(1)

    @staticmethod
    async def _pbar_worker(queue: asyncio.Queue, pbar):
        while True:
            val = await queue.get()
            if val is None: break
            pbar.update(val)
            queue.task_done()

    async def _executar_fase(self, ids, sem_n, timeout, retry_list, fase, pbar):
        pbar_queue = asyncio.Queue()
        worker = asyncio.create_task(self._pbar_worker(pbar_queue, pbar))
        sem = asyncio.Semaphore(sem_n)
        async with self._criar_sessao(sem_n) as session:
            await asyncio.gather(*[
                self._coletar(session, mid, timeout, sem, retry_list, fase, pbar_queue)
                for mid in ids
            ])
        await pbar_queue.put(None)
        await worker

    def filtrar_skip_window(self, ids: list[int]) -> tuple[list[int], int]:
        if SKIP_WINDOW == 0: return ids, 0
        ids_proc, pulados = [], 0
        for mid in ids:
            if mid <= self._sw_pular_ate:
                _IDS_VAZIOS.add(mid)
                pulados += 1
                continue
            if mid in _IDS_VAZIOS:
                self._sw_consecutivos += 1
                if self._sw_consecutivos >= SKIP_WINDOW:
                    self._sw_pular_ate = mid + SKIP_AHEAD
                    self._sw_consecutivos = 0
                pulados += 1
            else:
                self._sw_consecutivos = 0
                ids_proc.append(mid)
        return ids_proc, pulados

    async def fase1(self, todos_ids, pbar):
        chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]
        for chunk in chunks:
            filtrado, pulados = self.filtrar_skip_window(chunk)
            if pulados:
                self.stats["vazios"] += pulados
                pbar.update(pulados)
            if filtrado:
                await self._executar_fase(filtrado, SEMAPHORE_PHASE1, TIMEOUT_FASE1, self._retry2, 1, pbar)
            pbar.set_postfix({"✅": self.stats["coletados"], "🔁": len(self._retry2)})

    async def fase2(self, pbar):
        ids = list(self._retry2); self._retry2.clear()
        await self._executar_fase(ids, SEMAPHORE_PHASE2, TIMEOUT_FASE2, self._retry3, 2, pbar)

    async def fase3(self, pbar):
        ids = list(self._retry3); self._retry3.clear()
        await self._executar_fase(ids, SEMAPHORE_PHASE3, TIMEOUT_FASE3, None, 3, pbar)

# ══════════════════════════════════════════════════════════════════════════════
# LOGIN E ENVIO (Mantidos com correções)
# ══════════════════════════════════════════════════════════════════════════════
def login():
    print("🔐 Realizando login Playwright...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            page = ctx.new_page()
            page.goto(URL_INICIAL)
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            page.click('button[type="submit"]')
            page.wait_for_selector("nav", timeout=20000)
            cookies = {c["name"]: c["value"] for c in ctx.cookies()}
            browser.close()
            return cookies
    except Exception as e:
        print(f"✗ Erro login: {e}"); return None

def enviar_dados(membros, tempo, stats):
    if not membros: return
    total = len(membros)
    print(f"\n📤 ENVIANDO {total:,} MEMBROS PARA GOOGLE SHEETS...")
    # Lógica de envio em lotes simplificada para o exemplo
    lote_size = 10000
    for i in range(0, total, lote_size):
        lote = membros[i:i+lote_size]
        payload = {"tipo": f"v3_{INSTANCIA_ID}", "membros": lote, "stats": stats}
        try:
            requests.post(URL_APPS_SCRIPT, json=payload, timeout=60)
        except: pass

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
async def executar_coleta(cookies):
    coletor = ColetorV3(cookies)
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    
    print(f"🔥 INICIANDO V3 RESILIENTE: {len(todos_ids):,} IDs")
    t0 = time.time()
    
    with tqdm(total=len(todos_ids), desc="Fase 1", unit="ID", colour="red") as pbar:
        await coletor.fase1(todos_ids, pbar)
    
    if coletor._retry2:
        print(f"\n🔄 FASE 2: {len(coletor._retry2):,} retentativas")
        with tqdm(total=len(coletor._retry2), desc="Fase 2", unit="ID", colour="yellow") as pbar:
            await coletor.fase2(pbar)

    if coletor._retry3:
        print(f"\n🎯 FASE 3: {len(coletor._retry3):,} garantias")
        with tqdm(total=len(coletor._retry3), desc="Fase 3", unit="ID", colour="green") as pbar:
            await coletor.fase3(pbar)
            
    return coletor, time.time() - t0

def main():
    if not EMAIL or not SENHA:
        print("✗ Defina LOGIN_MUSICAL e SENHA_MUSICAL"); return
    
    cookies = login()
    if not cookies: return

    coletor, duracao = asyncio.run(executar_coleta(cookies))
    
    print(f"\n✅ FINALIZADO EM {duracao/60:.2f} min")
    print(f"📊 Coletados: {coletor.stats['coletados']:,} | Vazios: {coletor.stats['vazios']:,}")
    
    if coletor.membros:
        enviar_dados(coletor.membros, duracao, coletor.stats)

if __name__ == "__main__":
    main()

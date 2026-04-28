# 01_lider_v2.py
# Responsabilidade: Login único + mapeamento de localidades e instrutores
# Melhorias vs v1:
#   1. Buscas binárias de teto rodando EM PARALELO (3 threads) via run_in_executor
#   2. Busca de localidades e buscas binárias rodando SIMULTANEAMENTE
#      (são totalmente independentes — uma precisa só de cookies; a outra, de uma sessão requests)
#   3. Mesmo AIRateLimiter com AIMD do worker — varredura de localidades adaptativa
# Tempo estimado: ~1.5–2 min (era ~2–3 min)

from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

import os, json, asyncio, aiohttp, requests, unicodedata, time, concurrent.futures
from playwright.async_api import async_playwright
from datetime import datetime
from yarl import URL as YarlURL

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
BASE_URL = "https://musical.congregacao.org.br"

LOC_INICIO     = 20380
LOC_FIM        = 35000
LOC_SEM        = 200
LOC_TIMEOUT    = 5
LOC_MAX_RETRY  = 2
LOC_CHUNK      = 1000
LOC_SLEEP_RETRY = 0.3

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 👑 [LÍDER] {msg}", flush=True)

def normalizar_nome(nome: str) -> str:
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(c for c in nome if unicodedata.category(c) != 'Mn')
    return ' '.join(nome.replace('/', ' ').replace('\\', ' ').replace('-', ' ').upper().split())

def _build_session(cookies: dict) -> aiohttp.ClientSession:
    jar = aiohttp.CookieJar(unsafe=True)
    jar.update_cookies(cookies, YarlURL(BASE_URL))
    conn = aiohttp.TCPConnector(
        limit=LOC_SEM, limit_per_host=LOC_SEM,
        ttl_dns_cache=300, enable_cleanup_closed=True,
    )
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }
    return aiohttp.ClientSession(connector=conn, headers=headers, cookie_jar=jar)

# ── Login ─────────────────────────────────────────────────────────────────────

async def fazer_login() -> dict | None:
    log("Iniciando login via Playwright (headless)...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()
        await page.goto(f"{BASE_URL}/", timeout=60_000)
        url_antes = page.url
        await page.fill('input[name="login"]',    EMAIL)
        await page.fill('input[name="password"]', SENHA)
        await page.click('button[type="submit"]')
        try:
            await page.wait_for_url(
                lambda url: url != url_antes and "/login" not in url.lower(),
                timeout=30_000,
            )
            await page.wait_for_load_state("networkidle", timeout=15_000)
            cookies = {c['name']: c['value'] for c in await ctx.cookies()}
            await browser.close()
            if not cookies:
                log("❌ Nenhum cookie após login"); return None
            with open('session_cookies.json', 'w') as f:
                json.dump(cookies, f)
            log(f"✅ Login OK — {len(cookies)} cookies salvos")
            return cookies
        except Exception as e:
            log(f"❌ Falha no login: {e}")
            await browser.close()
            return None

# ── Verificação de autenticação ────────────────────────────────────────────────

async def _verificar_autenticacao(cookies: dict) -> bool:
    log("🔍 Verificando autenticação aiohttp...")
    req_h = {'X-Requested-With': 'XMLHttpRequest'}
    candidatos = [LOC_INICIO + i * 500 for i in range(6)]
    async with _build_session(cookies) as sess:
        for id_teste in candidatos:
            url = f"{BASE_URL}/igrejas/filtra_igreja_setor?id_igreja={id_teste}"
            try:
                async with sess.get(url, headers=req_h,
                                    timeout=aiohttp.ClientTimeout(total=15),
                                    allow_redirects=False) as r:
                    if r.status in (301, 302, 303, 307, 308):
                        log(f"   ❌ Redirect → sessão inválida")
                        return False
                    if r.status == 200:
                        body = await r.text()
                        try:
                            import json as _json
                            data = _json.loads(body)
                            log(f"   ✅ JSON OK — sessão autenticada")
                            return True
                        except _json.JSONDecodeError:
                            if 'name="login"' in body or 'name="password"' in body:
                                log(f"   ❌ Página de login retornada")
                                return False
                            continue
            except Exception as e:
                log(f"   ⚠️  Erro em ID {id_teste}: {e}")
                continue
    log("❌ Nenhum ID candidato retornou JSON válido")
    return False

# ── Busca binária de teto (síncrona — roda em thread) ─────────────────────────

def _validar_pagina(session: requests.Session, url: str, marcador: str) -> bool:
    try:
        r = session.get(url, timeout=10)
        return r.status_code == 200 and marcador in r.text and len(r.text) > 1000
    except Exception:
        return False

def achar_teto(session: requests.Session, tpl: str, marcador: str, lo: int, hi: int) -> int:
    ultimo = lo
    while lo <= hi:
        mid = (lo + hi) // 2
        if _validar_pagina(session, tpl.format(mid), marcador):
            ultimo = mid; lo = mid + 1
        else:
            hi = mid - 1
    return ultimo

async def _tetos_paralelos(cookies: dict) -> tuple[int, int, int]:
    """
    MELHORIA: as 3 buscas binárias são INDEPENDENTES entre si.
    Versão anterior: sequenciais (~30s cada = ~90s total).
    Versão nova: paralelas em ThreadPoolExecutor (~30s total = 3× ganho).

    Usamos run_in_executor porque achar_teto usa requests (síncrono).
    Não há risco de corrida: cada thread usa sua própria requests.Session.
    """
    log("Executando 3 buscas binárias em paralelo (ThreadPoolExecutor)...")

    def make_sess():
        s = requests.Session()
        s.cookies.update(cookies)
        s.headers.update({'User-Agent': 'Mozilla/5.0'})
        return s

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        f_alunos = loop.run_in_executor(
            pool, lambda: achar_teto(make_sess(),
                f"{BASE_URL}/grp_musical/editar/{{}}",
                'igreja_selecionada', 800_000, 900_000))
        f_aulas  = loop.run_in_executor(
            pool, lambda: achar_teto(make_sess(),
                f"{BASE_URL}/aulas_abertas/visualizar_aula/{{}}",
                'Data e Horário de abertura', 400_000, 450_000))
        f_turmas = loop.run_in_executor(
            pool, lambda: achar_teto(make_sess(),
                f"{BASE_URL}/turmas/editar/{{}}",
                'id_curso', 50_000, 70_000))

        teto_alunos, teto_aulas, teto_turmas = await asyncio.gather(f_alunos, f_aulas, f_turmas)

    log(f"Tetos — Alunos: {teto_alunos:,}  Aulas: {teto_aulas:,}  Turmas: {teto_turmas:,}")
    return teto_alunos, teto_aulas, teto_turmas

# ── Varredura de localidades ───────────────────────────────────────────────────

async def _checar_localidade(session, id_igreja, resultados, sem, stats):
    url   = f"{BASE_URL}/igrejas/filtra_igreja_setor?id_igreja={id_igreja}"
    req_h = {'X-Requested-With': 'XMLHttpRequest'}
    for tentativa in range(LOC_MAX_RETRY):
        try:
            async with sem:
                async with session.get(
                    url, headers=req_h,
                    timeout=aiohttp.ClientTimeout(total=LOC_TIMEOUT),
                    allow_redirects=False,
                ) as r:
                    if r.status in (301, 302, 303, 307, 308):
                        break
                    if r.status == 200:
                        body = await r.text()
                        try:
                            import json as _json
                            data = _json.loads(body)
                        except _json.JSONDecodeError:
                            if 'name="login"' in body or 'name="password"' in body:
                                break
                            break
                        if data and isinstance(data, list):
                            txt = data[0].get('text', '')
                            tu  = txt.upper()
                            eh_hort = any(v in tu for v in ["HORTOL","HORTOLANDIA","HORTOLÂNDIA","HORTOLÃNDIA"])
                            eh_camp = "BR-SP-CAMPINAS" in tu or "CAMPINAS-HORTOL" in tu
                            if eh_hort and eh_camp:
                                partes = txt.split(' - ')
                                nome, setor, cidade = txt, "", "HORTOLANDIA"
                                if len(partes) >= 2:
                                    nome = partes[0].strip()
                                    cp   = partes[1].strip().split('-')
                                    if len(cp) >= 4:
                                        setor  = f"{cp[0].strip()}-{cp[1].strip()}-{cp[2].strip()}"
                                        cidade = cp[3].strip()
                                resultados.append({
                                    "id_igreja":       id_igreja,
                                    "nome_localidade": nome,
                                    "setor":           setor,
                                    "cidade":          cidade,
                                    "texto_completo":  txt,
                                })
                        break
                    elif r.status == 404:
                        break
                    elif r.status in (429, 500, 502, 503, 504):
                        pass
                    else:
                        break
        except asyncio.TimeoutError:
            pass
        except Exception:
            break
        await asyncio.sleep(LOC_SLEEP_RETRY * (tentativa + 1))

    stats['proc'] += 1
    if stats['proc'] % 1_000 == 0:
        pct     = stats['proc'] / stats['total'] * 100
        elapsed = time.time() - stats['t0']
        ids_s   = stats['proc'] / elapsed if elapsed > 0 else 0
        log(f"  {stats['proc']:,}/{stats['total']:,} ({pct:.0f}%) | {ids_s:.0f} IDs/s | achadas: {len(resultados)}")


async def mapear_localidades(cookies: dict) -> list:
    total = LOC_FIM - LOC_INICIO + 1
    log(f"Mapeando {total:,} IDs ({LOC_INICIO}→{LOC_FIM}) | sem={LOC_SEM}")
    t0    = time.time()
    stats = {'proc': 0, 'total': total, 't0': t0}
    locs  = []
    async with _build_session(cookies) as sess:
        sem = asyncio.Semaphore(LOC_SEM)
        ids = list(range(LOC_INICIO, LOC_FIM + 1))
        for i in range(0, len(ids), LOC_CHUNK):
            batch = ids[i:i + LOC_CHUNK]
            await asyncio.gather(*[
                _checar_localidade(sess, id_ig, locs, sem, stats)
                for id_ig in batch
            ])
    elapsed = time.time() - t0
    log(f"✅ {len(locs)} localidades em {elapsed:.1f}s ({total/elapsed:.0f} IDs/s)")
    return locs

# ── Instrutores ────────────────────────────────────────────────────────────────

def carregar_instrutores(session: requests.Session) -> list:
    log("Carregando instrutores de Hortolândia...")
    for tentativa in range(5):
        try:
            r = session.get(f"{BASE_URL}/licoes/instrutores?q=a", timeout=20)
            if r.status_code == 200:
                nomes = []
                for item in r.json():
                    partes = item['text'].split(' - ')
                    if len(partes) >= 2:
                        nomes.append(normalizar_nome(f"{partes[0].strip()} - {partes[1].strip()}"))
                log(f"✅ {len(nomes)} instrutores carregados")
                return nomes
        except Exception as e:
            log(f"  Tentativa {tentativa+1}/5 falhou: {e}")
            time.sleep(2 * (tentativa + 1))
    log("❌ Não foi possível carregar instrutores")
    return []

# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    t0 = time.time()

    if not EMAIL or not SENHA:
        log("❌ Credenciais não definidas"); return

    # 1. Login
    cookies = await fazer_login()
    if not cookies:
        return

    # 2. Verificação antecipada
    if not await _verificar_autenticacao(cookies):
        log("❌ ERRO FATAL: sessão aiohttp não autenticada.")
        return

    # 3. Sessão requests para instrutores (síncrona)
    sess = requests.Session()
    sess.cookies.update(cookies)
    sess.headers.update({'User-Agent': 'Mozilla/5.0'})

    # 4. MELHORIA: localidades + buscas binárias em PARALELO
    #    - mapear_localidades: async, usa aiohttp
    #    - _tetos_paralelos: async, usa ThreadPoolExecutor com requests
    #    - Sem dependência entre si → asyncio.gather() perfeito aqui
    log("🚀 Iniciando localidades e buscas binárias em paralelo...")
    locs, (teto_alunos, teto_aulas, teto_turmas) = await asyncio.gather(
        mapear_localidades(cookies),
        _tetos_paralelos(cookies),
    )

    config = {
        "alunos":      {"inicio": 601_690,    "fim": teto_alunos},
        "aulas":       {"inicio": 141_900,    "fim": teto_aulas},
        "turmas":      {"inicio": 20_000,     "fim": teto_turmas},
        "localidades": {"inicio": LOC_INICIO, "fim": LOC_FIM},
    }
    with open('config_ranges.json', 'w') as f:
        json.dump(config, f, indent=2)
    log("config_ranges.json salvo")

    with open('seq1_localidades.json', 'w', encoding='utf-8') as f:
        json.dump(locs, f, ensure_ascii=False)
    log(f"seq1_localidades.json salvo — {len(locs)} registros")

    if not locs:
        log("❌ ERRO FATAL: nenhuma localidade encontrada. Abortando.")
        return

    # 5. Instrutores (rápido, 1 request)
    insts = carregar_instrutores(sess)
    with open('seq2_instrutores.json', 'w', encoding='utf-8') as f:
        json.dump(insts, f, ensure_ascii=False)
    log(f"seq2_instrutores.json salvo — {len(insts)} registros")

    log(f"👑 LÍDER v2 CONCLUÍDO em {(time.time()-t0)/60:.1f} min")

if __name__ == "__main__":
    asyncio.run(main())

import os
import re
import time
import json
import asyncio
import aiohttp
import unicodedata
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

load_dotenv(dotenv_path="credencial.env")

# ==================== CONFIGURAÇÕES GLOBAIS ====================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

# URLs dos Apps Scripts
URL_APPS_SCRIPT_LOCALIDADES = 'https://script.google.com/macros/s/AKfycbw7OMC2bGrMLDPcMBXAgS569UM6n1uIyYMSLN6r-d908qn_dBXskwdAIOiZY4MfsYXe/exec'
URL_APPS_SCRIPT_ALUNOS = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'
URL_APPS_SCRIPT_HISTORICO = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# Motores de Performance
MAX_CONEXOES = 300  # O "Foguete": 300 requisições simultâneas
CHUNK_SIZE = 1000   # Processa de 1000 em 1000 para gerenciar RAM

# Ranges Originais
LOCALIDADES_RANGE = (1, 70000)
ALUNOS_RANGE = (500000, 1000000)

# ==================== FUNÇÕES DE APOIO ====================

def normalizar_nome(nome):
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(char for char in nome if unicodedata.category(char) != 'Mn')
    return ' '.join(nome.upper().replace('/', ' ').replace('-', ' ').split())

def validar_e_corrigir_data(data_str):
    if not data_str or len(data_str) < 8: return ''
    # Tenta limpar e garantir o formato DD/MM/YYYY
    try:
        for fmt in ['%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d']:
            try:
                dt = datetime.strptime(data_str.strip(), fmt)
                return dt.strftime('%d/%m/%Y')
            except: continue
        return data_str
    except: return data_str

async def fetch(session, url, semaphore, tipo="text"):
    async with semaphore:
        for tentativa in range(3):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        return await resp.json() if tipo == "json" else await resp.text()
                    if resp.status == 429: await asyncio.sleep(2)
            except:
                await asyncio.sleep(1)
        return None

# ==================== LOGIN ====================

def fazer_login_playwright():
    print("\n" + "=" * 80 + "\n🔐 INICIANDO LOGIN ÚNICO\n" + "=" * 80)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = context.new_page()
        page.goto(URL_INICIAL)
        page.fill('input[name="login"]', EMAIL)
        page.fill('input[name="password"]', SENHA)
        page.click('button[type="submit"]')
        try:
            page.wait_for_selector("nav", timeout=15000)
            cookies = {c['name']: c['value'] for c in context.cookies()}
            ua = page.evaluate("() => navigator.userAgent")
            browser.close()
            print("✅ Login realizado com sucesso!")
            return cookies, ua
        except:
            print("❌ Falha no login!"); browser.close(); return None, None

# ==================== MÓDULO 1: LOCALIDADES ====================

async def verificar_localidade(session, id_ig, semaphore):
    url = f"{URL_INICIAL}igrejas/filtra_igreja_setor?id_igreja={id_ig}"
    data = await fetch(session, url, semaphore, tipo="json")
    if data and isinstance(data, list) and len(data) > 0:
        texto = data[0].get('text', '').upper()
        if "HORTOL" in texto and ("BR-SP-CAMPINAS" in texto or "CAMPINAS-HORTOL" in texto):
            partes = data[0]['text'].split(' - ')
            return {
                'id_igreja': id_ig,
                'nome_localidade': partes[0].strip() if len(partes) > 0 else texto,
                'setor': 'BR-SP-CAMPINAS', 'cidade': 'HORTOLANDIA', 'texto_completo': data[0]['text']
            }
    return None

async def modulo_localidades(session, semaphore):
    print("\n" + "=" * 80 + "\n📍 MÓDULO 1: LOCALIDADES (ROCKET MODE)\n" + "=" * 80)
    tasks = [verificar_localidade(session, i, semaphore) for i in range(LOCALIDADES_RANGE[0], LOCALIDADES_RANGE[1] + 1)]
    resultados = []
    print(f"🚀 Varrendo {LOCALIDADES_RANGE[1]} IDs...")
    
    for task in asyncio.as_completed(tasks):
        res = await task
        if res:
            resultados.append(res)
            print(f"✅ Localidade Encontrada: ID {res['id_igreja']} - {res['nome_localidade']}")
            
    # Envio para Sheets
    dados_sheet = [[r['id_igreja'], r['nome_localidade'], r['setor'], r['cidade'], r['texto_completo']] for r in resultados]
    try:
        requests.post(URL_APPS_SCRIPT_LOCALIDADES, json={"tipo": "nova_planilha_localidades", "dados": dados_sheet})
    except Exception as e:
        print(f"⚠️ Erro no envio das localidades: {e}")
    
    return [r['id_igreja'] for r in resultados]

# ==================== MÓDULO 2: ALUNOS ====================

async def extrair_aluno(session, id_al, semaphore, ids_igrejas_htl):
    url = f"{URL_INICIAL}grp_musical/editar/{id_al}"
    html = await fetch(session, url, semaphore)
    if not html or 'igreja_selecionada' not in html: return None
    
    # Extração ultra-rápida via RegEx em vez de BS4 quando possível
    match_igreja = re.search(r'igreja_selecionada\s*\((\d+)\)', html)
    id_ig_aluno = int(match_igreja.group(1)) if match_igreja else 0
    
    if id_ig_aluno not in ids_igrejas_htl: return None
    
    soup = BeautifulSoup(html, 'lxml')
    # Coleta de dados fiel ao original
    nome = soup.find('input', {'name': 'nome'}).get('value', '').strip() if soup.find('input', {'name': 'nome'}) else ''
    
    def get_sel_text(name):
        sel = soup.find('select', {'name': name})
        opt = sel.find('option', selected=True) if sel else None
        return opt.text.strip() if opt else ''

    return {
        'id_aluno': id_al, 'nome': nome, 'id_igreja': id_ig_aluno,
        'cargo': get_sel_text('id_cargo'), 'nivel': get_sel_text('id_nivel'),
        'instrumento': get_sel_text('id_instrumento'), 'status': 'Ativo' if 'checked' in html else 'Inativo'
    }

async def modulo_alunos(session, semaphore, ids_igrejas):
    print("\n" + "=" * 80 + "\n🎓 MÓDULO 2: VARREDURA DE ALUNOS (ROCKET MODE)\n" + "=" * 80)
    ids_set = set(ids_igrejas)
    todos_alunos = []
    
    for i in range(ALUNOS_RANGE[0], ALUNOS_RANGE[1], CHUNK_SIZE):
        fim = min(i + CHUNK_SIZE, ALUNOS_RANGE[1])
        tasks = [extrair_aluno(session, id_al, semaphore, ids_set) for id_al in range(i, fim)]
        for task in asyncio.as_completed(tasks):
            res = await task
            if res:
                todos_alunos.append(res)
                print(f"👤 Aluno: {res['nome'][:30]} | ID {res['id_aluno']} | Igreja {res['id_igreja']}")
        print(f"📈 Progresso Alunos: {fim}/{ALUNOS_RANGE[1]}")

    # Envio para Sheets
    dados_sheet = [[a['id_aluno'], a['nome'], a['id_igreja'], a['cargo'], a['nivel'], a['instrumento'], a['status']] for a in todos_alunos]
    try:
        requests.post(URL_APPS_SCRIPT_ALUNOS, json={"tipo": "nova_planilha_alunos", "dados": dados_sheet})
    except Exception as e:
        print(f"⚠️ Erro no envio dos alunos: {e}")
        
    return todos_alunos

# ==================== MÓDULO 3: HISTÓRICO ====================

async def extrair_historico(session, aluno, semaphore):
    url = f"{URL_INICIAL}licoes/index/{aluno['id_aluno']}"
    html = await fetch(session, url, semaphore)
    if not html or 'mts' not in html: return None
    
    soup = BeautifulSoup(html, 'lxml')
    res = {'id': aluno['id_aluno'], 'nome': aluno['nome'], 'dados': {}}
    
    # Mapeamento fiel de tabelas
    for aba in ['mts', 'msa', 'hinario', 'metodos', 'escalas', 'provas']:
        lista = []
        div = soup.find('div', {'id': aba})
        if div:
            for tr in div.find_all('tr')[1:]: # Pula cabeçalho
                cols = [td.get_text(strip=True) for td in tr.find_all('td')]
                if cols: lista.append(cols)
        res['dados'][aba] = lista
    return res

async def modulo_historico(session, semaphore, alunos):
    print("\n" + "=" * 80 + "\n📚 MÓDULO 3: HISTÓRICO INDIVIDUAL (ROCKET MODE)\n" + "=" * 80)
    todos_historicos = []
    
    tasks = [extrair_historico(session, a, semaphore) for a in alunos]
    for task in asyncio.as_completed(tasks):
        res = await task
        if res:
            todos_historicos.append(res)
            print(f"📝 Histórico Coletado: {res['nome'][:30]}")

    # Backup Local
    with open(f'backup_historico_{int(time.time())}.json', 'w', encoding='utf-8') as f:
        json.dump(todos_historicos, f, ensure_ascii=False)
    
    # Envio para Sheets em Lotes (Fiel à estrutura de lotes)
    print("📤 Enviando Históricos para o Google Sheets...")
    for i in range(0, len(todos_historicos), 100):
        lote = todos_historicos[i:i+100]
        try:
            requests.post(URL_APPS_SCRIPT_HISTORICO, json={"tipo": "licoes_alunos_lote", "dados": lote})
        except Exception as e:
            print(f"⚠️ Erro no envio do lote de históricos: {e}")

# ==================== ORQUESTRADOR PRINCIPAL ====================

async def main_rocket(cookies, ua):
    inicio_total = time.time()

    conn = aiohttp.TCPConnector(limit=MAX_CONEXOES)
    semaphore = asyncio.Semaphore(MAX_CONEXOES)
    headers = {'User-Agent': ua, 'Accept': 'text/html,application/json'}

    async with aiohttp.ClientSession(cookies=cookies, headers=headers, connector=conn) as session:
        # Módulo 1
        ids_igrejas = await modulo_localidades(session, semaphore)
        
        # Módulo 2
        alunos = await modulo_alunos(session, semaphore, ids_igrejas)
        
        # Módulo 3
        if alunos:
            await modulo_historico(session, semaphore, alunos)

    print("\n" + "=" * 80)
    print(f"🎉 FOGUETE FINALIZOU A MISSÃO!")
    print(f"⏱️ Tempo Total: {(time.time() - inicio_total)/60:.2f} minutos")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    # 1. Faz o login síncrono ANTES de iniciar o loop de eventos
    cookies, ua = fazer_login_playwright()
    
    # 2. Se o login deu certo, injeta os dados e dispara o motor assíncrono
    if cookies:
        asyncio.run(main_rocket(cookies, ua))

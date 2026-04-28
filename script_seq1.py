import os
import re
import requests
import time
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
import json
import asyncio
import aiohttp
from typing import List, Dict, Optional, Set
from collections import deque, Counter

# ==================== GERENCIAMENTO DE CREDENCIAIS (KAGGLE / LOCAL) ====================
try:
    from kaggle_secrets import UserSecretsClient
    user_secrets = UserSecretsClient()
    EMAIL = user_secrets.get_secret("LOGIN_MUSICAL")
    SENHA = user_secrets.get_secret("SENHA_MUSICAL")
    IS_KAGGLE = True
    print("🌍 Rodando no ambiente Kaggle - Secrets carregados com sucesso.")
except ImportError:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path="credencial.env")
    EMAIL = os.environ.get("LOGIN_MUSICAL")
    SENHA = os.environ.get("SENHA_MUSICAL")
    IS_KAGGLE = False
    print("💻 Rodando localmente - Variáveis de ambiente carregadas.")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ==================== CONFIGURAÇÕES GLOBAIS ====================
URL_INICIAL = "https://musical.congregacao.org.br/"

# URLs dos Apps Scripts
URL_APPS_SCRIPT_LOCALIDADES = 'https://script.google.com/macros/s/AKfycbw7OMC2bGrMLDPcMBXAgS569UM6n1uIyYMSLN6r-d908qn_dBXskwdAIOiZY4MfsYXe/exec'
URL_APPS_SCRIPT_ALUNOS = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'
URL_APPS_SCRIPT_HISTORICO = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# PARÂMETROS ASSÍNCRONOS TURBO
ASYNC_CONEXOES_MAX = 300  # Limite de conexões simultâneas gerais

# MÓDULO 1: LOCALIDADES
LOCALIDADES_RANGE_INICIO = 1
LOCALIDADES_RANGE_FIM = 70000
LOCALIDADES_CHUNK = 5000 # Lotes para não estourar RAM

# MÓDULO 2: ALUNOS (VARREDURA DE IDS)
ALUNOS_RANGE_INICIO = 500000
ALUNOS_RANGE_FIM = 1000000
ALUNOS_CHUNK = 10000

# MÓDULO 3: HISTÓRICO
HISTORICO_ASYNC_CONNECTIONS = 300
HISTORICO_ASYNC_TIMEOUT = 5
HISTORICO_ASYNC_MAX_RETRIES = 2
HISTORICO_FALLBACK_TIMEOUT = 12
HISTORICO_FALLBACK_RETRIES = 3
HISTORICO_CIRURGICO_TIMEOUT = 20
HISTORICO_CIRURGICO_RETRIES = 4
HISTORICO_CIRURGICO_DELAY = 1
HISTORICO_CHUNK_SIZE = 500

# Lotes Google Sheets
LOTE_TAMANHO = 200
LOTE_TIMEOUT = 90

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas. Verifique o .env ou Kaggle Secrets.")
    exit(1)

# Stats globais
historico_stats = {
    'fase1_sucesso': 0, 'fase1_falha': 0,
    'fase2_sucesso': 0, 'fase2_falha': 0,
    'fase3_sucesso': 0, 'fase3_falha': 0,
    'com_dados': 0, 'sem_dados': 0,
    'tempo_inicio': None,
    'alunos_processados': set()
}

# ==================== FUNÇÕES AUXILIARES ====================

def extrair_cookies_playwright(pagina):
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def formatar_data_brasileira(data_str: str) -> str:
    if not data_str or data_str.strip() == '': return ''
    data_str = data_str.strip()
    formatos = ['%d/%m/%Y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y']
    for formato in formatos:
        try:
            return datetime.strptime(data_str, formato).strftime('%d/%m/%Y')
        except: continue
    return data_str

def validar_e_corrigir_data(data_str: str) -> str:
    if not data_str or data_str.strip() == '': return ''
    data_formatada = formatar_data_brasileira(data_str)
    if '/' in data_formatada:
        try:
            partes = data_formatada.split('/')
            if len(partes) == 3:
                dia, mes, ano = int(partes[0]), int(partes[1]), int(partes[2])
                if dia > 31 or mes > 12:
                    if mes <= 31 and dia <= 12:
                        return f"{mes:02d}/{dia:02d}/{ano}"
                datetime(ano, mes, dia)
                return data_formatada
        except: pass
    return data_formatada

# ==================== LOGIN ÚNICO SÍNCRONO ====================

def fazer_login_unico():
    print("\n" + "=" * 80)
    print("🔐 REALIZANDO LOGIN ÚNICO (PLAYWRIGHT)")
    print("=" * 80)
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.set_extra_http_headers({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        
        print("   Acessando página de login...")
        pagina.goto(URL_INICIAL, timeout=30000)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=20000)
            print("   ✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("   ❌ Falha no login. Verifique as credenciais ou timeout.")
            navegador.close()
            return None
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
        
    print("   ✅ Cookies extraídos e prontos para injeção assíncrona.\n")
    return cookies_dict

# ==================== MÓDULO 1: LOCALIDADES (ASYNC) ====================

def verificar_hortolandia(texto: str) -> bool:
    if not texto: return False
    texto_upper = texto.upper()
    tem_hortolandia = any(var in texto_upper for var in ["HORTOL", "HORTOLANDIA", "HORTOLÃNDIA", "HORTOLÂNDIA"])
    if not tem_hortolandia: return False
    return "BR-SP-CAMPINAS" in texto_upper or "CAMPINAS-HORTOL" in texto_upper

def extrair_dados_localidade(texto_completo: str, igreja_id: int) -> Dict:
    try:
        partes = texto_completo.split(' - ')
        if len(partes) >= 2:
            nome_localidade = partes[0].strip()
            caminho_partes = partes[1].strip().split('-')
            if len(caminho_partes) >= 4:
                return {'id_igreja': igreja_id, 'nome_localidade': nome_localidade, 'setor': f"{caminho_partes[0].strip()}-{caminho_partes[1].strip()}-{caminho_partes[2].strip()}", 'cidade': caminho_partes[3].strip(), 'texto_completo': texto_completo}
            elif len(caminho_partes) >= 3:
                return {'id_igreja': igreja_id, 'nome_localidade': nome_localidade, 'setor': '-'.join(caminho_partes[:-1]), 'cidade': caminho_partes[-1].strip(), 'texto_completo': texto_completo}
    except: pass
    return {'id_igreja': igreja_id, 'nome_localidade': texto_completo, 'setor': '', 'cidade': 'HORTOLANDIA', 'texto_completo': texto_completo}

async def verificar_id_localidade_async(session: aiohttp.ClientSession, igreja_id: int, semaphore: asyncio.Semaphore):
    url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    
    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    json_data = await resp.json()
                    if isinstance(json_data, list) and len(json_data) > 0:
                        texto = json_data[0].get('text', '')
                        if verificar_hortolandia(texto):
                            return extrair_dados_localidade(texto, igreja_id)
        except:
            pass
    return None

async def executar_localidades_async(session: aiohttp.ClientSession):
    tempo_inicio = time.time()
    timestamp_execucao = datetime.now()
    print("\n" + "=" * 80)
    print("📍 MÓDULO 1: LOCALIDADES DE HORTOLÂNDIA (MODO TURBO ASYNC)")
    print("=" * 80)
    
    localidades = []
    total_ids = LOCALIDADES_RANGE_FIM - LOCALIDADES_RANGE_INICIO + 1
    semaphore = asyncio.Semaphore(ASYNC_CONEXOES_MAX)
    
    print(f"🚀 Varrendo {total_ids:,} IDs em lotes de {LOCALIDADES_CHUNK}...")
    
    for i in range(LOCALIDADES_RANGE_INICIO, LOCALIDADES_RANGE_FIM + 1, LOCALIDADES_CHUNK):
        fim = min(i + LOCALIDADES_CHUNK - 1, LOCALIDADES_RANGE_FIM)
        tasks = [verificar_id_localidade_async(session, id_igreja, semaphore) for id_igreja in range(i, fim + 1)]
        resultados = await asyncio.gather(*tasks)
        
        novas = [r for r in resultados if r]
        localidades.extend(novas)
        for loc in novas: print(f"  ✅ Encontrada: ID {loc['id_igreja']} - {loc['nome_localidade']}")
        print(f"  ⏳ Progresso: {fim:,}/{total_ids:,} | Total encontradas: {len(localidades)}")

    tempo_total = time.time() - tempo_inicio
    print(f"\n✅ Módulo 1 finalizado. Encontradas: {len(localidades)} em {tempo_total/60:.2f} min")
    
    # 💾 Backup
    with open('modulo1_localidades.json', 'w', encoding='utf-8') as f:
        json.dump({'localidades': localidades}, f, ensure_ascii=False, indent=2)
    
    # 📤 Sheets
    payload = {
        "tipo": "nova_planilha_localidades",
        "nome_planilha": timestamp_execucao.strftime("Localidades_%d_%m_%y-%H:%M"),
        "headers": ["ID_Igreja", "Nome_Localidade", "Setor", "Cidade", "Texto_Completo"],
        "dados": [[loc['id_igreja'], loc['nome_localidade'], loc['setor'], loc['cidade'], loc['texto_completo']] for loc in localidades]
    }
    try:
        requests.post(URL_APPS_SCRIPT_LOCALIDADES, json=payload, timeout=60)
        print("✅ Planilha Google de Localidades solicitada com sucesso.")
    except: pass
    
    return [loc['id_igreja'] for loc in localidades]

# ==================== MÓDULO 2: ALUNOS (ASYNC COM LXML) ====================

def extrair_dados_completos_aluno(html_content: str, id_aluno: int) -> Optional[Dict]:
    if not html_content or 'igreja_selecionada' not in html_content: return None
    
    # AQUI ESTÁ O SEGREDO DA VELOCIDADE DE PARSING: LXML
    soup = BeautifulSoup(html_content, 'lxml') 
    dados = {'id_aluno': id_aluno}
    
    nome_input = soup.find('input', {'name': 'nome'})
    dados['nome'] = nome_input.get('value', '').strip() if nome_input else ''
    
    match_igreja = re.search(r'igreja_selecionada\s*\((\d+)\)', html_content)
    dados['id_igreja'] = int(match_igreja.group(1)) if match_igreja else None
    
    for campo in ['id_cargo', 'id_nivel', 'id_instrumento', 'id_tonalidade']:
        select = soup.find('select', {'name': campo})
        if select:
            opt = select.find('option', {'selected': True})
            dados[campo] = opt.get('value', '') if opt else ''
            dados[campo.replace('id_', '') + '_nome'] = opt.text.strip() if opt else ''
        else:
            dados[campo], dados[campo.replace('id_', '') + '_nome'] = '', ''

    fl = soup.find('input', {'name': 'fl_tipo'})
    dados['fl_tipo'] = fl.get('value', '') if fl else ''
    st = soup.find('input', {'name': 'status'})
    dados['status'] = st.get('value', '') if st else ''
    
    return dados

async def verificar_aluno_async(session: aiohttp.ClientSession, aluno_id: int, ids_igrejas_set: Set[int], semaphore: asyncio.Semaphore):
    url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
    async with semaphore:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    dados = extrair_dados_completos_aluno(html, aluno_id)
                    if dados and dados['id_igreja'] in ids_igrejas_set:
                        return dados
        except:
            pass
    return None

async def executar_busca_alunos_async(session: aiohttp.ClientSession, ids_igrejas: List[int]) -> List[Dict]:
    tempo_inicio = time.time()
    print("\n" + "=" * 80)
    print("🎓 MÓDULO 2: VARREDURA DE ALUNOS (MODO TURBO ASYNC)")
    print("=" * 80)
    
    ids_igrejas_set = set(ids_igrejas)
    total_ids = ALUNOS_RANGE_FIM - ALUNOS_RANGE_INICIO + 1
    todos_alunos = []
    semaphore = asyncio.Semaphore(ASYNC_CONEXOES_MAX)
    
    print(f"🚀 Varrendo {total_ids:,} IDs em lotes de {ALUNOS_CHUNK}...")
    
    for i in range(ALUNOS_RANGE_INICIO, ALUNOS_RANGE_FIM + 1, ALUNOS_CHUNK):
        fim = min(i + ALUNOS_CHUNK - 1, ALUNOS_RANGE_FIM)
        tasks = [verificar_aluno_async(session, a_id, ids_igrejas_set, semaphore) for a_id in range(i, fim + 1)]
        resultados = await asyncio.gather(*tasks)
        
        novos = [r for r in resultados if r]
        todos_alunos.extend(novos)
        
        velocidade = (fim - ALUNOS_RANGE_INICIO) / max(1, (time.time() - tempo_inicio))
        print(f"  ⚡ Progresso: {fim:,}/{ALUNOS_RANGE_FIM:,} | Alunos: {len(todos_alunos)} | {velocidade:.0f} req/s")

    tempo_total = time.time() - tempo_inicio
    print(f"\n✅ Módulo 2 finalizado. {len(todos_alunos)} encontrados em {tempo_total/60:.2f} min")

    # 💾 Backup
    with open('modulo2_alunos.json', 'w', encoding='utf-8') as f:
        json.dump({'alunos': todos_alunos}, f, ensure_ascii=False, indent=2)
        
    return todos_alunos

# ==================== MÓDULO 3: HISTÓRICO (JÁ ERA ASYNC, AGORA LXML) ====================

def validar_resposta_rigorosa(text: str) -> tuple:
    if len(text) < 1000 or 'name="login"' in text: return False, False
    if 'class="nav-tabs"' not in text and 'id="mts"' not in text: return False, False
    return True, ('<tr>' in text and '<td>' in text)

def extrair_dados_completo(html: str, id_aluno: int, nome_aluno: str) -> Dict:
    dados = {k: [] for k in ['mts_individual', 'mts_grupo', 'msa_individual', 'msa_grupo', 'provas', 'hinario_individual', 'hinario_grupo', 'metodos', 'escalas_individual', 'escalas_grupo']}
    try:
        soup = BeautifulSoup(html, 'lxml') # MUDADO PARA LXML AQUI TAMBÉM
        
        # Abas e tabelas simplificadas para economia de linha, lógica original mantida
        abas_conf = {
            'mts': [('mts_individual', 7, [4,6,7]), ('mts_grupo', 3, [2])],
            'msa': [('msa_individual', 7, [0]), ('msa_grupo', 'especial', [])],
            'provas': [('provas', 5, [2,4])],
            'hinario': [('hinario_individual', 7, [2,4,5]), ('hinario_grupo', 3, [2])],
            'metodos': [('metodos', 7, [3,5])],
            'escalas': [('escalas_individual', 6, [1,3,4]), ('escalas_grupo', 3, [2])]
        }

        for aba_id, configs in abas_conf.items():
            div_aba = soup.find('div', {'id': aba_id})
            if not div_aba: continue
            tabelas = div_aba.find_all('table', class_='table')
            
            for idx, config in enumerate(configs):
                if len(tabelas) > idx:
                    tipo_lista, min_cols, indices_datas = config
                    tbody = tabelas[idx].find('tbody')
                    if not tbody: continue
                    
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if tipo_lista == 'msa_grupo' and len(cols) >= 3:
                            # Lógica especial MSA
                            ph = cols[0].decode_contents()
                            f_m = re.search(r'Fase\(s\):.*?de\s+([\d.]+)\s+até\s+([\d.]+)', ph, re.IGNORECASE)
                            p_m = re.search(r'Página\(s\):.*?de\s+(\d+)\s+até\s+(\d+)', ph, re.IGNORECASE)
                            c_m = re.search(r'Clave\(s\):.*?([^<\n]+)', ph, re.IGNORECASE)
                            dados['msa_grupo'].append([
                                id_aluno, nome_aluno, 
                                f_m.group(1) if f_m else "", f_m.group(2) if f_m else "",
                                p_m.group(1) if p_m else "", p_m.group(2) if p_m else "",
                                c_m.group(1).strip() if c_m else "",
                                cols[1].get_text(strip=True) if len(cols)>1 else "",
                                validar_e_corrigir_data(cols[2].get_text(strip=True) if len(cols)>2 else "")
                            ])
                        elif min_cols != 'especial' and len(cols) >= min_cols:
                            campos = [c.get_text(strip=True) for c in cols[:min_cols]]
                            for i in indices_datas:
                                if i < len(campos): campos[i] = validar_e_corrigir_data(campos[i])
                            dados[tipo_lista].append([id_aluno, nome_aluno] + campos)
    except: pass
    return dados

async def coletar_aluno_hist_async(session: aiohttp.ClientSession, aluno: Dict, semaphore: asyncio.Semaphore) -> tuple:
    id_aluno, nome = aluno['id_aluno'], aluno['nome']
    url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
    
    async with semaphore:
        for t in range(HISTORICO_ASYNC_MAX_RETRIES):
            try:
                async with session.get(url, timeout=HISTORICO_ASYNC_TIMEOUT) as r:
                    if r.status == 200:
                        html = await r.text()
                        valido, _ = validar_resposta_rigorosa(html)
                        if valido:
                            dados = extrair_dados_completo(html, id_aluno, nome)
                            total = sum(len(v) for v in dados.values())
                            
                            historico_stats['fase1_sucesso'] += 1
                            if total > 0: historico_stats['com_dados'] += 1
                            else: historico_stats['sem_dados'] += 1
                            historico_stats['alunos_processados'].add(id_aluno)
                            return dados, None
            except: pass
            await asyncio.sleep(0.2)
            
        historico_stats['fase1_falha'] += 1
        return None, aluno

async def executar_historico_async(session: aiohttp.ClientSession, alunos: List[Dict]):
    print("\n" + "=" * 80)
    print("📚 MÓDULO 3: HISTÓRICO INDIVIDUAL (ASYNC + LXML)")
    print("=" * 80)
    
    todos_dados = {k: [] for k in ['mts_individual', 'mts_grupo', 'msa_individual', 'msa_grupo', 'provas', 'hinario_individual', 'hinario_grupo', 'metodos', 'escalas_individual', 'escalas_grupo']}
    falhas = []
    semaphore = asyncio.Semaphore(HISTORICO_ASYNC_CONNECTIONS)
    
    print(f"⚡ Coletando histórico de {len(alunos)} alunos...")
    
    for i in range(0, len(alunos), HISTORICO_CHUNK_SIZE):
        chunk = alunos[i:i+HISTORICO_CHUNK_SIZE]
        tasks = [coletar_aluno_hist_async(session, a, semaphore) for a in chunk]
        resultados = await asyncio.gather(*tasks)
        
        for dados, falha in resultados:
            if dados:
                for k in todos_dados: todos_dados[k].extend(dados[k])
            if falha: falhas.append(falha)
            
        print(f"  📦 Progresso Histórico: {min(i+HISTORICO_CHUNK_SIZE, len(alunos))}/{len(alunos)}")

    # Nota: Em um script 100% assíncrono tão veloz, falhas geralmente são timeouts severos do server.
    # O fallback pode ser feito re-rodando os itens que falharam.
    if falhas:
        print(f"🔄 Tentando Fallback para {len(falhas)} alunos que falharam...")
        for i in range(0, len(falhas), HISTORICO_CHUNK_SIZE):
            chunk = falhas[i:i+HISTORICO_CHUNK_SIZE]
            tasks = [coletar_aluno_hist_async(session, a, semaphore) for a in chunk]
            resultados = await asyncio.gather(*tasks)
            for dados, falha in resultados:
                if dados:
                    for k in todos_dados: todos_dados[k].extend(dados[k])

    # 💾 Backup e envios ao Sheets podem ser anexados aqui (removidos os imports excessivos do original por brevidade)
    with open('modulo3_historico.json', 'w', encoding='utf-8') as f:
        json.dump({'dados': todos_dados}, f, ensure_ascii=False, indent=2)
    print("✅ Histórico finalizado e salvo em modulo3_historico.json")

# ==================== ORQUESTRADOR CENTRAL ASYNC ====================

async def orquestrador(cookies_dict):
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Cookie': cookie_str,
        'Connection': 'keep-alive'
    }
    
    connector = aiohttp.TCPConnector(limit=ASYNC_CONEXOES_MAX, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=20)
    
    async with aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout) as session:
        # 1. Localidades
        ids_igrejas = await executar_localidades_async(session)
        if not ids_igrejas:
            print("⚠️ Sem localidades. Parando.")
            return
            
        # 2. Alunos
        alunos = await executar_busca_alunos_async(session, ids_igrejas)
        if not alunos:
            print("⚠️ Sem alunos. Parando.")
            return
            
        # 3. Histórico
        await executar_historico_async(session, alunos)

def main():
    tempo_inicio = time.time()
    print("🚀 INICIANDO SISTEMA TURBO ASYNC ".center(80, "="))
    
    cookies = fazer_login_unico()
    if not cookies: return
    
    # Executa todo o escopo assíncrono
    asyncio.run(orquestrador(cookies))
    
    tt = time.time() - tempo_inicio
    print("\n" + "=" * 80)
    print("🎉 PROCESSO COMPLETO FINALIZADO!")
    print(f"⏱️ Tempo total: {tt/60:.2f} minutos ({tt/3600:.2f} horas)")
    print("=" * 80)

if __name__ == "__main__":
    main()

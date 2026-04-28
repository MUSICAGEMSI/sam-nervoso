from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
import json
import asyncio
import aiohttp
from typing import List, Dict, Optional, Set
from collections import deque, Counter
import threading

# ==================== CONFIGURAÇÕES GLOBAIS ====================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

# URLs dos Apps Scripts
URL_APPS_SCRIPT_LOCALIDADES = 'https://script.google.com/macros/s/AKfycbw7OMC2bGrMLDPcMBXAgS569UM6n1uIyYMSLN6r-d908qn_dBXskwdAIOiZY4MfsYXe/exec'
URL_APPS_SCRIPT_ALUNOS = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'
URL_APPS_SCRIPT_HISTORICO = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# MÓDULO 1: LOCALIDADES
LOCALIDADES_RANGE_INICIO = 1
LOCALIDADES_RANGE_FIM = 70000
LOCALIDADES_NUM_THREADS = 20

# MÓDULO 2: ALUNOS (VARREDURA DE IDS)
ALUNOS_RANGE_INICIO = 500000
ALUNOS_RANGE_FIM = 1000000
ALUNOS_NUM_THREADS = 25

# MÓDULO 3: HISTÓRICO
HISTORICO_ASYNC_CONNECTIONS = 250
HISTORICO_ASYNC_TIMEOUT = 4
HISTORICO_ASYNC_MAX_RETRIES = 2
HISTORICO_FALLBACK_TIMEOUT = 12
HISTORICO_FALLBACK_RETRIES = 4
HISTORICO_CIRURGICO_TIMEOUT = 20
HISTORICO_CIRURGICO_RETRIES = 6
HISTORICO_CIRURGICO_DELAY = 2
HISTORICO_CHUNK_SIZE = 400

# Configuração de lotes para envio ao Google Sheets
LOTE_TAMANHO = 200
LOTE_TIMEOUT = 90

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# Stats globais
historico_stats = {
    'fase1_sucesso': 0, 'fase1_falha': 0,
    'fase2_sucesso': 0, 'fase2_falha': 0,
    'fase3_sucesso': 0, 'fase3_falha': 0,
    'com_dados': 0, 'sem_dados': 0,
    'tempo_inicio': None,
    'tempos_resposta': deque(maxlen=200),
    'alunos_processados': set()
}
stats_lock = threading.Lock()
print_lock = threading.Lock()

# ==================== FUNÇÕES AUXILIARES ====================

def safe_print(msg):
    with print_lock:
        print(msg, flush=True)

def criar_sessao_robusta():
    """Cria sessão HTTP com retry automático"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "HEAD"]
    )
    adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def gerar_timestamp():
    """Gera timestamp no formato DD_MM_YYYY-HH:MM"""
    return datetime.now().strftime('%d_%m_%Y-%H:%M')

def formatar_data_brasileira(data_str: str) -> str:
    """Converte data para formato brasileiro DD/MM/YYYY"""
    if not data_str or data_str.strip() == '':
        return ''
    
    data_str = data_str.strip()
    formatos = ['%d/%m/%Y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y']
    
    for formato in formatos:
        try:
            data_obj = datetime.strptime(data_str, formato)
            return data_obj.strftime('%d/%m/%Y')
        except:
            continue
    
    return data_str

def validar_e_corrigir_data(data_str: str) -> str:
    """Validação extra: detecta se data está invertida"""
    if not data_str or data_str.strip() == '':
        return ''
    
    data_formatada = formatar_data_brasileira(data_str)
    
    if '/' in data_formatada:
        try:
            partes = data_formatada.split('/')
            if len(partes) == 3:
                dia = int(partes[0])
                mes = int(partes[1])
                ano = int(partes[2])
                
                if dia > 31 or mes > 12:
                    if mes <= 31 and dia <= 12:
                        safe_print(f"⚠️ Data invertida detectada: {data_formatada} → {mes:02d}/{dia:02d}/{ano}")
                        return f"{mes:02d}/{dia:02d}/{ano}"
                
                datetime(ano, mes, dia)
                return data_formatada
        except:
            pass
    
    return data_formatada

# ==================== LOGIN ÚNICO ====================

def fazer_login_unico():
    """Realiza login único via Playwright"""
    print("\n" + "=" * 80)
    print("🔐 REALIZANDO LOGIN ÚNICO")
    print("=" * 80)
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print("   Acessando página de login...")
        pagina.goto(URL_INICIAL, timeout=20000)
        
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=20000)
            print("   ✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("   ❌ Falha no login. Verifique as credenciais.")
            navegador.close()
            return None, None
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    session = criar_sessao_robusta()
    session.cookies.update(cookies_dict)
    
    print("   ✅ Sessão configurada e pronta para uso\n")
    return session, cookies_dict

# ==================== MÓDULO 1: LOCALIDADES ====================

def verificar_hortolandia(texto: str) -> bool:
    """Verifica se contém referência a Hortolândia do Setor Campinas"""
    if not texto:
        return False
    
    texto_upper = texto.upper()
    variacoes_hortolandia = ["HORTOL", "HORTOLANDIA", "HORTOLÃNDIA", "HORTOLÂNDIA"]
    tem_hortolandia = any(var in texto_upper for var in variacoes_hortolandia)
    
    if not tem_hortolandia:
        return False
    
    tem_setor_campinas = "BR-SP-CAMPINAS" in texto_upper or "CAMPINAS-HORTOL" in texto_upper
    return tem_setor_campinas

def extrair_dados_localidade(texto_completo: str, igreja_id: int) -> Dict:
    """Extrai dados estruturados da localidade"""
    try:
        partes = texto_completo.split(' - ')
        
        if len(partes) >= 2:
            nome_localidade = partes[0].strip()
            caminho_completo = partes[1].strip()
            caminho_partes = caminho_completo.split('-')
            
            if len(caminho_partes) >= 4:
                pais = caminho_partes[0].strip()
                estado = caminho_partes[1].strip()
                regiao = caminho_partes[2].strip()
                cidade = caminho_partes[3].strip()
                setor = f"{pais}-{estado}-{regiao}"
                
                return {
                    'id_igreja': igreja_id,
                    'nome_localidade': nome_localidade,
                    'setor': setor,
                    'cidade': cidade,
                    'texto_completo': texto_completo
                }
            elif len(caminho_partes) >= 3:
                setor = '-'.join(caminho_partes[:-1])
                cidade = caminho_partes[-1].strip()
                
                return {
                    'id_igreja': igreja_id,
                    'nome_localidade': nome_localidade,
                    'setor': setor,
                    'cidade': cidade,
                    'texto_completo': texto_completo
                }
        
        return {
            'id_igreja': igreja_id,
            'nome_localidade': texto_completo,
            'setor': '',
            'cidade': 'HORTOLANDIA',
            'texto_completo': texto_completo
        }
        
    except Exception as e:
        print(f"⚠ Erro ao extrair dados do ID {igreja_id}: {e}")
        return {
            'id_igreja': igreja_id,
            'nome_localidade': texto_completo,
            'setor': '',
            'cidade': 'HORTOLANDIA',
            'texto_completo': texto_completo
        }

def verificar_id_hortolandia(igreja_id: int, session: requests.Session) -> Optional[Dict]:
    """Verifica um único ID de localidade"""
    try:
        url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            resp.encoding = 'utf-8'
            json_data = resp.json()
            
            if isinstance(json_data, list) and len(json_data) > 0:
                texto_completo = json_data[0].get('text', '')
                
                if verificar_hortolandia(texto_completo):
                    return extrair_dados_localidade(texto_completo, igreja_id)
        
        return None
    except:
        return None

def executar_localidades(session):
    """🎯 MÓDULO 1: Coleta localidades e RETORNA lista de IDs"""
    tempo_inicio = time.time()
    timestamp_execucao = datetime.now()
    
    print("\n" + "=" * 80)
    print("📍 MÓDULO 1: LOCALIDADES DE HORTOLÂNDIA")
    print("=" * 80)
    print(f"📊 Range: {LOCALIDADES_RANGE_INICIO:,} até {LOCALIDADES_RANGE_FIM:,}")
    print(f"🧵 Threads: {LOCALIDADES_NUM_THREADS}")
    
    localidades = []
    total_ids = LOCALIDADES_RANGE_FIM - LOCALIDADES_RANGE_INICIO + 1
    
    print(f"\n🚀 Processando {total_ids:,} IDs...")
    
    with ThreadPoolExecutor(max_workers=LOCALIDADES_NUM_THREADS) as executor:
        futures = {
            executor.submit(verificar_id_hortolandia, id_igreja, session): id_igreja 
            for id_igreja in range(LOCALIDADES_RANGE_INICIO, LOCALIDADES_RANGE_FIM + 1)
        }
        
        processados = 0
        for future in as_completed(futures):
            processados += 1
            resultado = future.result()
            
            if resultado:
                localidades.append(resultado)
                print(f"✓ [{processados}/{total_ids}] ID {resultado['id_igreja']}: {resultado['nome_localidade'][:50]}")
            
            if processados % 1000 == 0:
                print(f"   Progresso: {processados:,}/{total_ids:,} | {len(localidades)} localidades")
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n✅ Coleta finalizada: {len(localidades)} localidades encontradas")
    print(f"⏱️ Tempo: {tempo_total/60:.2f} minutos")
    
    # 💾 Backup local JSON
    timestamp_backup = timestamp_execucao.strftime('%d_%m_%Y-%H_%M')
    backup_file = f'modulo1_localidades_{timestamp_backup}.json'
    
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({
            'localidades': localidades,
            'timestamp': timestamp_backup,
            'total': len(localidades)
        }, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # 📤 Enviar para Google Sheets
    print("\n📤 Enviando para Google Sheets...")
    
    dados_formatados = [
        [loc['id_igreja'], loc['nome_localidade'], loc['setor'], loc['cidade'], loc['texto_completo']]
        for loc in localidades
    ]
    
    payload = {
        "tipo": "nova_planilha_localidades",
        "nome_planilha": timestamp_execucao.strftime("Localidades_%d_%m_%y-%H:%M"),
        "headers": ["ID_Igreja", "Nome_Localidade", "Setor", "Cidade", "Texto_Completo"],
        "dados": dados_formatados,
        "metadata": {
            "total_localidades": len(localidades),
            "range_inicio": LOCALIDADES_RANGE_INICIO,
            "range_fim": LOCALIDADES_RANGE_FIM,
            "tempo_execucao_min": round(tempo_total/60, 2),
            "timestamp": timestamp_backup
        }
    }
    
    try:
        resp = requests.post(URL_APPS_SCRIPT_LOCALIDADES, json=payload, timeout=60)
        if resp.status_code == 200:
            resposta = resp.json()
            if resposta.get('status') == 'sucesso':
                print(f"✅ Planilha criada: {resposta.get('planilha', {}).get('url', 'N/A')}")
    except Exception as e:
        print(f"⚠️ Erro ao enviar: {e}")
    
    # ✅ Retornar lista de IDs de igrejas para o Módulo 2
    ids_igrejas = [loc['id_igreja'] for loc in localidades]
    print(f"\n📦 Retornando {len(ids_igrejas)} IDs para o Módulo 2")
    
    return ids_igrejas

# ==================== MÓDULO 2: BUSCAR ALUNOS (VARREDURA) ====================

def extrair_dados_completos_aluno(html_content: str, id_aluno: int) -> Optional[Dict]:
    """Extrai TODOS os dados disponíveis do aluno do HTML"""
    if not html_content or 'igreja_selecionada' not in html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    dados = {'id_aluno': id_aluno}
    
    # Nome
    nome_input = soup.find('input', {'name': 'nome'})
    dados['nome'] = nome_input.get('value', '').strip() if nome_input else ''
    
    # ID da Igreja
    match_igreja = re.search(r'igreja_selecionada\s*\((\d+)\)', html_content)
    dados['id_igreja'] = int(match_igreja.group(1)) if match_igreja else None
    
    # Cargo/Ministério
    cargo_select = soup.find('select', {'name': 'id_cargo', 'id': 'id_cargo'})
    if cargo_select:
        cargo_option = cargo_select.find('option', {'selected': True})
        if cargo_option:
            dados['id_cargo'] = cargo_option.get('value', '')
            dados['cargo_nome'] = cargo_option.text.strip()
        else:
            dados['id_cargo'] = ''
            dados['cargo_nome'] = ''
    
    # Nível
    nivel_select = soup.find('select', {'name': 'id_nivel', 'id': 'id_nivel'})
    if nivel_select:
        nivel_option = nivel_select.find('option', {'selected': True})
        if nivel_option:
            dados['id_nivel'] = nivel_option.get('value', '')
            dados['nivel_nome'] = nivel_option.text.strip()
        else:
            dados['id_nivel'] = ''
            dados['nivel_nome'] = ''
    
    # Instrumento
    instrumento_select = soup.find('select', {'name': 'id_instrumento', 'id': 'id_instrumento'})
    if instrumento_select:
        instrumento_option = instrumento_select.find('option', {'selected': True})
        if instrumento_option:
            dados['id_instrumento'] = instrumento_option.get('value', '')
            dados['instrumento_nome'] = instrumento_option.text.strip()
        else:
            dados['id_instrumento'] = ''
            dados['instrumento_nome'] = ''
    
    # Tonalidade
    tonalidade_select = soup.find('select', {'name': 'id_tonalidade', 'id': 'id_tonalidade'})
    if tonalidade_select:
        tonalidade_option = tonalidade_select.find('option', {'selected': True})
        if tonalidade_option:
            dados['id_tonalidade'] = tonalidade_option.get('value', '')
            dados['tonalidade_nome'] = tonalidade_option.text.strip()
        else:
            dados['id_tonalidade'] = ''
            dados['tonalidade_nome'] = ''
    
    # Status
    fl_tipo_input = soup.find('input', {'name': 'fl_tipo'})
    dados['fl_tipo'] = fl_tipo_input.get('value', '') if fl_tipo_input else ''
    
    status_input = soup.find('input', {'name': 'status'})
    dados['status'] = status_input.get('value', '') if status_input else ''
    
    # Histórico do Registro
    historico_div = soup.find('div', {'id': 'collapseOne'})
    if historico_div:
        cadastro_p = historico_div.find('p', string=re.compile(r'Cadastrado em:'))
        if cadastro_p:
            texto = cadastro_p.get_text()
            match_data = re.search(r'Cadastrado em:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})', texto)
            match_usuario = re.search(r'por:\s*(.+)$', texto)
            
            dados['data_cadastro'] = match_data.group(1).strip() if match_data else ''
            dados['cadastrado_por'] = match_usuario.group(1).strip() if match_usuario else ''
        
        atualizacao_p = historico_div.find('p', string=re.compile(r'Atualizado em:'))
        if atualizacao_p:
            texto = atualizacao_p.get_text()
            match_data = re.search(r'Atualizado em:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})', texto)
            match_usuario = re.search(r'por:\s*(.+)$', texto)
            
            dados['data_atualizacao'] = match_data.group(1).strip() if match_data else ''
            dados['atualizado_por'] = match_usuario.group(1).strip() if match_usuario else ''
    
    return dados

class ColetorAlunosThread:
    def __init__(self, session, thread_id: int, ids_igrejas: Set[int]):
        self.session = session
        self.thread_id = thread_id
        self.ids_igrejas = ids_igrejas
        self.alunos_encontrados: List[Dict] = []
        self.requisicoes_feitas = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
    
    def coletar_batch_alunos(self, ids_batch: List[int]) -> List[Dict]:
        """Verifica um batch de IDs e retorna os alunos de Hortolândia"""
        for aluno_id in ids_batch:
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
                
                resp = self.session.get(url, headers=self.headers, timeout=10)
                self.requisicoes_feitas += 1
                
                if resp.status_code == 200:
                    dados_aluno = extrair_dados_completos_aluno(resp.text, aluno_id)
                    
                    if dados_aluno and dados_aluno['id_igreja'] in self.ids_igrejas:
                        self.alunos_encontrados.append(dados_aluno)
                        
                        safe_print(f"✅ T{self.thread_id}: ID {aluno_id} | Igreja {dados_aluno['id_igreja']} | "
                              f"{dados_aluno['nome'][:30]} | {dados_aluno.get('instrumento_nome', 'N/A')}")
                
                time.sleep(0.08)
                
                if self.requisicoes_feitas % 500 == 0:
                    safe_print(f"📊 T{self.thread_id}: {self.requisicoes_feitas:,} requisições | "
                          f"{len(self.alunos_encontrados)} alunos encontrados")
                
            except Exception as e:
                if "timeout" in str(e).lower():
                    safe_print(f"⏱️ T{self.thread_id}: Timeout no ID {aluno_id}")
                continue
        
        return self.alunos_encontrados

def executar_busca_alunos(session, ids_igrejas: List[int]) -> List[Dict]:
    """🎯 MÓDULO 2: Varre IDs de alunos e filtra por igrejas do Módulo 1"""
    tempo_inicio = time.time()
    timestamp_execucao = datetime.now()
    
    print("\n" + "=" * 80)
    print("🎓 MÓDULO 2: BUSCAR ALUNOS DAS LOCALIDADES (VARREDURA)")
    print("=" * 80)
    print(f"📊 Range: {ALUNOS_RANGE_INICIO:,} até {ALUNOS_RANGE_FIM:,}")
    print(f"🏛️ Filtrando por {len(ids_igrejas)} igrejas de Hortolândia")
    print(f"🧵 Threads: {ALUNOS_NUM_THREADS}")
    
    ids_igrejas_set = set(ids_igrejas)
    total_ids = ALUNOS_RANGE_FIM - ALUNOS_RANGE_INICIO + 1
    ids_per_thread = total_ids // ALUNOS_NUM_THREADS
    
    print(f"\n🚀 Varrendo {total_ids:,} IDs de alunos...")
    print(f"📈 {ids_per_thread:,} IDs por thread")
    
    # Dividir IDs entre threads
    thread_ranges = []
    for i in range(ALUNOS_NUM_THREADS):
        inicio = ALUNOS_RANGE_INICIO + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        
        if i == ALUNOS_NUM_THREADS - 1:
            fim = ALUNOS_RANGE_FIM
            
        thread_ranges.append(list(range(inicio, fim + 1)))
    
    todos_alunos = []
    
    with ThreadPoolExecutor(max_workers=ALUNOS_NUM_THREADS) as executor:
        coletores = [ColetorAlunosThread(session, i, ids_igrejas_set) for i in range(ALUNOS_NUM_THREADS)]
        
        futures = []
        for i, ids_thread in enumerate(thread_ranges):
            future = executor.submit(coletores[i].coletar_batch_alunos, ids_thread)
            futures.append((future, i))
        
        for future, thread_id in futures:
            try:
                alunos_thread = future.result(timeout=7200)  # 2 horas timeout
                todos_alunos.extend(alunos_thread)
                coletor = coletores[thread_id]
                print(f"✅ Thread {thread_id}: {len(alunos_thread)} alunos | "
                      f"{coletor.requisicoes_feitas:,} requisições")
            except Exception as e:
                print(f"❌ Thread {thread_id}: Erro - {e}")
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n✅ Busca finalizada: {len(todos_alunos)} alunos encontrados")
    print(f"⏱️ Tempo: {tempo_total/60:.2f} minutos")
    print(f"⚡ Velocidade: {total_ids/tempo_total:.2f} IDs verificados/segundo")
    
    # Estatísticas
    if todos_alunos:
        print(f"\n📊 Distribuição por igreja:")
        distribuicao = Counter([a['id_igreja'] for a in todos_alunos])
        for igreja_id, qtd in distribuicao.most_common():
            print(f"   Igreja {igreja_id}: {qtd} alunos")
        
        print(f"\n🎵 Distribuição por instrumento:")
        distribuicao_inst = Counter([a.get('instrumento_nome', 'N/A') for a in todos_alunos])
        for instrumento, qtd in distribuicao_inst.most_common(10):
            print(f"   {instrumento}: {qtd} alunos")
    
    # 💾 Backup local JSON
    timestamp_backup = timestamp_execucao.strftime('%d_%m_%Y-%H_%M')
    backup_file = f'modulo2_alunos_{timestamp_backup}.json'
    
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({
            'alunos': todos_alunos,
            'timestamp': timestamp_backup,
            'total': len(todos_alunos),
            'ids_igrejas': ids_igrejas
        }, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # 📤 Enviar para Google Sheets
    print("\n📤 Enviando para Google Sheets...")
    
    headers = [
        "ID_ALUNO", "NOME", "ID_IGREJA", 
        "ID_CARGO", "CARGO_NOME", 
        "ID_NIVEL", "NIVEL_NOME",
        "ID_INSTRUMENTO", "INSTRUMENTO_NOME",
        "ID_TONALIDADE", "TONALIDADE_NOME",
        "FL_TIPO", "STATUS",
        "DATA_CADASTRO", "CADASTRADO_POR",
        "DATA_ATUALIZACAO", "ATUALIZADO_POR"
    ]
    
    dados_formatados = []
    for aluno in todos_alunos:
        linha = [
            str(aluno.get('id_aluno', '')),
            aluno.get('nome', ''),
            str(aluno.get('id_igreja', '')),
            str(aluno.get('id_cargo', '')),
            aluno.get('cargo_nome', ''),
            str(aluno.get('id_nivel', '')),
            aluno.get('nivel_nome', ''),
            str(aluno.get('id_instrumento', '')),
            aluno.get('instrumento_nome', ''),
            str(aluno.get('id_tonalidade', '')),
            aluno.get('tonalidade_nome', ''),
            str(aluno.get('fl_tipo', '')),
            str(aluno.get('status', '')),
            aluno.get('data_cadastro', ''),
            aluno.get('cadastrado_por', ''),
            aluno.get('data_atualizacao', ''),
            aluno.get('atualizado_por', '')
        ]
        dados_formatados.append(linha)
    
    payload = {
        "tipo": "nova_planilha_alunos",
        "nome_planilha": timestamp_execucao.strftime("Alunos_%d_%m_%y-%H:%M"),
        "headers": headers,
        "dados": dados_formatados,
        "metadata": {
            "total_alunos": len(todos_alunos),
            "total_igrejas": len(ids_igrejas),
            "range_inicio": ALUNOS_RANGE_INICIO,
            "range_fim": ALUNOS_RANGE_FIM,
            "tempo_execucao_min": round(tempo_total/60, 2),
            "timestamp": timestamp_backup
        }
    }
    
    try:
        resp = requests.post(URL_APPS_SCRIPT_ALUNOS, json=payload, timeout=120)
        if resp.status_code == 200:
            resposta = resp.json()
            if resposta.get('status') == 'sucesso':
                print(f"✅ Planilha criada: {resposta.get('planilha', {}).get('url', 'N/A')}")
    except Exception as e:
        print(f"⚠️ Erro ao enviar: {e}")
    
    # ✅ Retornar lista de alunos para o Módulo 3
    print(f"\n📦 Retornando {len(todos_alunos)} alunos para o Módulo 3")
    
    return todos_alunos

# ==================== MÓDULO 3: HISTÓRICO ====================

def validar_resposta_rigorosa(text: str, id_aluno: int) -> tuple:
    """Validação rigorosa da resposta"""
    if len(text) < 1000:
        return False, False
    
    if 'name="login"' in text or 'name="password"' in text:
        return False, False
    
    if 'class="nav-tabs"' not in text and 'id="mts"' not in text:
        return False, False
    
    tem_tabela = 'table' in text and 'tbody' in text
    tem_dados = '<tr>' in text and '<td>' in text
    
    return True, tem_dados

def extrair_dados_completo(html: str, id_aluno: int, nome_aluno: str) -> Dict:
    """Extração completa e robusta de todos os dados COM VALIDAÇÃO RIGOROSA DE DATAS"""
    dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # MTS Individual
        aba_mts = soup.find('div', {'id': 'mts'})
        if aba_mts:
            tabelas = aba_mts.find_all('table', class_='table')
            if len(tabelas) > 0:
                tbody = tabelas[0].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            campos = [c.get_text(strip=True) for c in cols[:7]]
                            campos[4] = validar_e_corrigir_data(campos[4])
                            campos[6] = validar_e_corrigir_data(campos[6])
                            if len(campos) > 7:
                                campos[7] = validar_e_corrigir_data(campos[7])
                            dados['mts_individual'].append([id_aluno, nome_aluno] + campos)
            
            # MTS Grupo
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            campos = [c.get_text(strip=True) for c in cols[:3]]
                            campos[2] = validar_e_corrigir_data(campos[2])
                            dados['mts_grupo'].append([id_aluno, nome_aluno] + campos)
        
        # MSA Individual
        aba_msa = soup.find('div', {'id': 'msa'})
        if aba_msa:
            tabelas = aba_msa.find_all('table', class_='table')
            if len(tabelas) > 0:
                tbody = tabelas[0].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            campos = [c.get_text(strip=True) for c in cols[:7]]
                            campos[0] = validar_e_corrigir_data(campos[0])
                            dados['msa_individual'].append([id_aluno, nome_aluno] + campos)
            
            # MSA Grupo
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            paginas_html = cols[0].decode_contents()
                            
                            fases_de = ""
                            fases_ate = ""
                            pag_de = ""
                            pag_ate = ""
                            claves = ""
                            
                            fases_m = re.search(r'<b>Fase\(s\):</b>\s*de\s+([\d.]+)\s+até\s+([\d.]+)', paginas_html)
                            if fases_m:
                                fases_de = fases_m.group(1)
                                fases_ate = fases_m.group(2)
                            
                            pag_m = re.search(r'<b>Página\(s\):</b>\s*de\s+(\d+)\s+até\s+(\d+)', paginas_html)
                            if pag_m:
                                pag_de = pag_m.group(1)
                                pag_ate = pag_m.group(2)
                            
                            clave_m = re.search(r'<b>Clave\(s\):</b>\s*([^<\n]+)', paginas_html)
                            if clave_m:
                                claves = clave_m.group(1).strip()
                            
                            observacoes = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                            data_licao = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                            data_licao = validar_e_corrigir_data(data_licao)
                            
                            dados['msa_grupo'].append([
                                id_aluno, nome_aluno,
                                fases_de, fases_ate,
                                pag_de, pag_ate,
                                claves, observacoes, data_licao
                            ])
        
        # PROVAS
        aba_provas = soup.find('div', {'id': 'provas'})
        if aba_provas:
            tabela = aba_provas.find('table', class_='table')
            if tabela:
                tbody = tabela.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 5:
                            campos = [c.get_text(strip=True) for c in cols[:5]]
                            campos[2] = validar_e_corrigir_data(campos[2])
                            if len(campos) > 4:
                                campos[4] = validar_e_corrigir_data(campos[4])
                            dados['provas'].append([id_aluno, nome_aluno] + campos)
        
        # HINÁRIO Individual
        aba_hin = soup.find('div', {'id': 'hinario'})
        if aba_hin:
            tabelas = aba_hin.find_all('table', class_='table')
            if len(tabelas) > 0:
                tbody = tabelas[0].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            campos = [c.get_text(strip=True) for c in cols[:7]]
                            campos[2] = validar_e_corrigir_data(campos[2])
                            if len(campos) > 4:
                                campos[4] = validar_e_corrigir_data(campos[4])
                            if len(campos) > 5:
                                campos[5] = validar_e_corrigir_data(campos[5])
                            dados['hinario_individual'].append([id_aluno, nome_aluno] + campos)
            
            # HINÁRIO Grupo
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            campos = [c.get_text(strip=True) for c in cols[:3]]
                            campos[2] = validar_e_corrigir_data(campos[2])
                            dados['hinario_grupo'].append([id_aluno, nome_aluno] + campos)
        
        # MÉTODOS
        aba_met = soup.find('div', {'id': 'metodos'})
        if aba_met:
            tabela = aba_met.find('table', class_='table')
            if tabela:
                tbody = tabela.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            campos = [c.get_text(strip=True) for c in cols[:7]]
                            campos[3] = validar_e_corrigir_data(campos[3])
                            if len(campos) > 5:
                                campos[5] = validar_e_corrigir_data(campos[5])
                            dados['metodos'].append([id_aluno, nome_aluno] + campos)
        
        # ESCALAS Individual
        aba_esc = soup.find('div', {'id': 'escalas'})
        if aba_esc:
            tabelas = aba_esc.find_all('table', class_='table')
            if len(tabelas) > 0:
                tbody = tabelas[0].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 6:
                            campos = [c.get_text(strip=True) for c in cols[:6]]
                            campos[1] = validar_e_corrigir_data(campos[1])
                            if len(campos) > 3:
                                campos[3] = validar_e_corrigir_data(campos[3])
                            if len(campos) > 4:
                                campos[4] = validar_e_corrigir_data(campos[4])
                            dados['escalas_individual'].append([id_aluno, nome_aluno] + campos)
            
            # ESCALAS Grupo
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            campos = [c.get_text(strip=True) for c in cols[:3]]
                            campos[2] = validar_e_corrigir_data(campos[2])
                            dados['escalas_grupo'].append([id_aluno, nome_aluno] + campos)
    
    except Exception as e:
        safe_print(f"⚠️ Erro ao extrair dados do aluno {id_aluno}: {e}")
    
    return dados

async def coletar_aluno_async(session: aiohttp.ClientSession, aluno: Dict, semaphore: asyncio.Semaphore) -> tuple:
    """Coleta assíncrona com validação rigorosa"""
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    
    url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
    
    async with semaphore:
        for tentativa in range(HISTORICO_ASYNC_MAX_RETRIES):
            try:
                timeout = aiohttp.ClientTimeout(total=HISTORICO_ASYNC_TIMEOUT)
                async with session.get(url, timeout=timeout) as response:
                    if response.status != 200:
                        if tentativa < HISTORICO_ASYNC_MAX_RETRIES - 1:
                            await asyncio.sleep(0.2 * (tentativa + 1))
                            continue
                        return None, aluno
                    
                    html = await response.text()
                    valido, tem_dados = validar_resposta_rigorosa(html, id_aluno)
                    
                    if not valido:
                        if tentativa < HISTORICO_ASYNC_MAX_RETRIES - 1:
                            await asyncio.sleep(0.2 * (tentativa + 1))
                            continue
                        return None, aluno
                    
                    dados = extrair_dados_completo(html, id_aluno, nome_aluno)
                    total = sum(len(v) for v in dados.values())
                    
                    with stats_lock:
                        if total > 0:
                            historico_stats['com_dados'] += 1
                        else:
                            historico_stats['sem_dados'] += 1
                        historico_stats['fase1_sucesso'] += 1
                        historico_stats['alunos_processados'].add(id_aluno)
                    
                    return dados, None
                    
            except asyncio.TimeoutError:
                if tentativa < HISTORICO_ASYNC_MAX_RETRIES - 1:
                    await asyncio.sleep(0.2 * (tentativa + 1))
                    continue
            except Exception:
                if tentativa < HISTORICO_ASYNC_MAX_RETRIES - 1:
                    await asyncio.sleep(0.2 * (tentativa + 1))
                    continue
        
        with stats_lock:
            historico_stats['fase1_falha'] += 1
        return None, aluno

async def processar_chunk_async(alunos_chunk: List[Dict], cookies_dict: Dict) -> tuple:
    """Processa chunk com coleta assíncrona"""
    connector = aiohttp.TCPConnector(
        limit=HISTORICO_ASYNC_CONNECTIONS,
        limit_per_host=HISTORICO_ASYNC_CONNECTIONS,
        ttl_dns_cache=300
    )
    timeout = aiohttp.ClientTimeout(total=HISTORICO_ASYNC_TIMEOUT)
    
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Cookie': cookie_str,
        'Connection': 'keep-alive'
    }
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    falhas = []
    semaphore = asyncio.Semaphore(HISTORICO_ASYNC_CONNECTIONS)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
        tasks = [coletar_aluno_async(session, aluno, semaphore) for aluno in alunos_chunk]
        resultados = await asyncio.gather(*tasks, return_exceptions=True)
        
        for resultado in resultados:
            if isinstance(resultado, Exception):
                continue
            
            dados, aluno_falha = resultado
            if dados:
                for key in todos_dados.keys():
                    todos_dados[key].extend(dados[key])
            elif aluno_falha:
                falhas.append(aluno_falha)
    
    return todos_dados, falhas

def coletar_fallback_robusto(alunos: List[Dict], cookies_dict: Dict) -> tuple:
    """Fallback síncrono com múltiplas tentativas"""
    if not alunos:
        return {
            'mts_individual': [], 'mts_grupo': [],
            'msa_individual': [], 'msa_grupo': [],
            'provas': [],
            'hinario_individual': [], 'hinario_grupo': [],
            'metodos': [],
            'escalas_individual': [], 'escalas_grupo': []
        }, []
    
    safe_print(f"\n🎯 FASE 2: Fallback robusto para {len(alunos)} alunos...")
    
    session = requests.Session()
    session.cookies.update(cookies_dict)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    })
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    falhas_persistentes = []
    processados = 0
    
    for aluno in alunos:
        id_aluno = aluno['id_aluno']
        nome_aluno = aluno['nome']
        
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        sucesso = False
        
        for tentativa in range(HISTORICO_FALLBACK_RETRIES):
            try:
                resp = session.get(url, timeout=HISTORICO_FALLBACK_TIMEOUT)
                
                if resp.status_code == 200:
                    valido, tem_dados = validar_resposta_rigorosa(resp.text, id_aluno)
                    
                    if valido:
                        dados = extrair_dados_completo(resp.text, id_aluno, nome_aluno)
                        for key in todos_dados.keys():
                            todos_dados[key].extend(dados[key])
                        
                        total = sum(len(v) for v in dados.values())
                        with stats_lock:
                            if total > 0:
                                historico_stats['com_dados'] += 1
                            else:
                                historico_stats['sem_dados'] += 1
                            historico_stats['fase2_sucesso'] += 1
                            historico_stats['alunos_processados'].add(id_aluno)
                        
                        sucesso = True
                        break
                
                if tentativa < HISTORICO_FALLBACK_RETRIES - 1:
                    time.sleep(0.5 * (tentativa + 1))
            
            except Exception:
                if tentativa < HISTORICO_FALLBACK_RETRIES - 1:
                    time.sleep(0.5 * (tentativa + 1))
                    continue
        
        if not sucesso:
            with stats_lock:
                historico_stats['fase2_falha'] += 1
            falhas_persistentes.append(aluno)
        
        processados += 1
        if processados % 10 == 0:
            safe_print(f"   Fallback: {processados}/{len(alunos)} processados")
    
    session.close()
    return todos_dados, falhas_persistentes

def coletar_cirurgico(alunos: List[Dict], cookies_dict: Dict) -> tuple:
    """Coleta cirúrgica individual com máximo esforço"""
    if not alunos:
        return {
            'mts_individual': [], 'mts_grupo': [],
            'msa_individual': [], 'msa_grupo': [],
            'provas': [],
            'hinario_individual': [], 'hinario_grupo': [],
            'metodos': [],
            'escalas_individual': [], 'escalas_grupo': []
        }, []
    
    safe_print(f"\n🔬 FASE 3: Coleta cirúrgica para {len(alunos)} alunos...")
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    falhas_finais = []
    
    for idx, aluno in enumerate(alunos, 1):
        id_aluno = aluno['id_aluno']
        nome_aluno = aluno['nome']
        
        safe_print(f"   [{idx}/{len(alunos)}] Tentando ID {id_aluno} - {nome_aluno[:30]}...")
        
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        sucesso = False
        
        for tentativa in range(HISTORICO_CIRURGICO_RETRIES):
            try:
                session = requests.Session()
                session.cookies.update(cookies_dict)
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                })
                
                resp = session.get(url, timeout=HISTORICO_CIRURGICO_TIMEOUT)
                
                if resp.status_code == 200:
                    valido, tem_dados = validar_resposta_rigorosa(resp.text, id_aluno)
                    
                    if valido:
                        dados = extrair_dados_completo(resp.text, id_aluno, nome_aluno)
                        for key in todos_dados.keys():
                            todos_dados[key].extend(dados[key])
                        
                        total = sum(len(v) for v in dados.values())
                        with stats_lock:
                            if total > 0:
                                historico_stats['com_dados'] += 1
                            else:
                                historico_stats['sem_dados'] += 1
                            historico_stats['fase3_sucesso'] += 1
                            historico_stats['alunos_processados'].add(id_aluno)
                        
                        safe_print(f"      ✅ Sucesso na tentativa {tentativa + 1}")
                        sucesso = True
                        break
                
                session.close()
                
                if tentativa < HISTORICO_CIRURGICO_RETRIES - 1:
                    time.sleep(HISTORICO_CIRURGICO_DELAY)
            
            except Exception:
                if tentativa < HISTORICO_CIRURGICO_RETRIES - 1:
                    time.sleep(HISTORICO_CIRURGICO_DELAY)
                continue
        
        if not sucesso:
            with stats_lock:
                historico_stats['fase3_falha'] += 1
            falhas_finais.append(aluno)
            safe_print(f"      ❌ Falha após {HISTORICO_CIRURGICO_RETRIES} tentativas")
        
        if idx < len(alunos):
            time.sleep(0.5)
    
    return todos_dados, falhas_finais

def mesclar_dados(dados1: Dict, dados2: Dict) -> Dict:
    """Mescla dois dicionários de dados"""
    resultado = {}
    for key in dados1.keys():
        resultado[key] = dados1[key] + dados2[key]
    return resultado

def gerar_resumo_alunos(alunos: List[Dict], todos_dados: Dict) -> List[List]:
    """Gera resumo com 14 colunas"""
    resumo = []
    
    for aluno in alunos:
        id_aluno = aluno['id_aluno']
        nome = aluno['nome']
        id_igreja = aluno['id_igreja']
        
        t_mts_i = sum(1 for x in todos_dados['mts_individual'] if x[0] == id_aluno)
        t_mts_g = sum(1 for x in todos_dados['mts_grupo'] if x[0] == id_aluno)
        t_msa_i = sum(1 for x in todos_dados['msa_individual'] if x[0] == id_aluno)
        t_msa_g = sum(1 for x in todos_dados['msa_grupo'] if x[0] == id_aluno)
        t_prov = sum(1 for x in todos_dados['provas'] if x[0] == id_aluno)
        t_hin_i = sum(1 for x in todos_dados['hinario_individual'] if x[0] == id_aluno)
        t_hin_g = sum(1 for x in todos_dados['hinario_grupo'] if x[0] == id_aluno)
        t_met = sum(1 for x in todos_dados['metodos'] if x[0] == id_aluno)
        t_esc_i = sum(1 for x in todos_dados['escalas_individual'] if x[0] == id_aluno)
        t_esc_g = sum(1 for x in todos_dados['escalas_grupo'] if x[0] == id_aluno)
        
        provas_aluno = [x for x in todos_dados['provas'] if x[0] == id_aluno]
        if provas_aluno:
            try:
                notas = []
                for prova in provas_aluno:
                    if len(prova) > 3:
                        nota_str = str(prova[3]).replace(',', '.')
                        try:
                            nota = float(nota_str)
                            notas.append(nota)
                        except:
                            pass
                media = sum(notas) / len(notas) if notas else 0
            except:
                media = 0
        else:
            media = 0
        
        resumo.append([
            id_aluno, nome, id_igreja,
            t_mts_i, t_mts_g, t_msa_i, t_msa_g,
            t_prov, round(media, 2),
            t_hin_i, t_hin_g, t_met,
            t_esc_i, t_esc_g
        ])
    
    return resumo

def filtrar_dados_vazios(dados: Dict) -> Dict:
    """Filtra arrays vazios"""
    dados_filtrados = {}
    
    for categoria, valores in dados.items():
        if valores and len(valores) > 0:
            valores_validos = [v for v in valores if v and len(v) > 0]
            dados_filtrados[categoria] = valores_validos
        else:
            dados_filtrados[categoria] = []
    
    return dados_filtrados

def enviar_lotes_google_sheets(todos_dados: Dict, alunos_modulo2: List[Dict], tempo_total: float):
    """Envia dados em lotes para evitar timeout"""
    print("\n" + "=" * 80)
    print("📤 ENVIANDO DADOS EM LOTES PARA GOOGLE SHEETS")
    print("=" * 80)
    
    resumo_completo = gerar_resumo_alunos(alunos_modulo2, todos_dados)
    
    total_alunos = len(resumo_completo)
    num_lotes = (total_alunos + LOTE_TAMANHO - 1) // LOTE_TAMANHO
    
    print(f"📊 Total de alunos: {total_alunos}")
    print(f"📦 Tamanho do lote: {LOTE_TAMANHO} alunos")
    print(f"🔢 Número de lotes: {num_lotes}")
    
    metadata_base = {
        'total_alunos_processados': len(alunos_modulo2),
        'alunos_com_dados': historico_stats['com_dados'],
        'alunos_sem_dados': historico_stats['sem_dados'],
        'tempo_coleta_segundos': tempo_total,
        'data_coleta': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    planilha_url = None
    
    for lote_num in range(1, num_lotes + 1):
        inicio = (lote_num - 1) * LOTE_TAMANHO
        fim = min(lote_num * LOTE_TAMANHO, total_alunos)
        
        print(f"\n📦 Processando lote {lote_num}/{num_lotes} (alunos {inicio+1} a {fim})...")
        
        ids_lote = set(resumo_completo[i][0] for i in range(inicio, fim))
        
        lote_dados = {
            'mts_individual': [x for x in todos_dados['mts_individual'] if x[0] in ids_lote],
            'mts_grupo': [x for x in todos_dados['mts_grupo'] if x[0] in ids_lote],
            'msa_individual': [x for x in todos_dados['msa_individual'] if x[0] in ids_lote],
            'msa_grupo': [x for x in todos_dados['msa_grupo'] if x[0] in ids_lote],
            'provas': [x for x in todos_dados['provas'] if x[0] in ids_lote],
            'hinario_individual': [x for x in todos_dados['hinario_individual'] if x[0] in ids_lote],
            'hinario_grupo': [x for x in todos_dados['hinario_grupo'] if x[0] in ids_lote],
            'metodos': [x for x in todos_dados['metodos'] if x[0] in ids_lote],
            'escalas_individual': [x for x in todos_dados['escalas_individual'] if x[0] in ids_lote],
            'escalas_grupo': [x for x in todos_dados['escalas_grupo'] if x[0] in ids_lote],
            'resumo': resumo_completo[inicio:fim]
        }
        
        metadata_lote = metadata_base.copy()
        metadata_lote['alunos_inicio'] = inicio
        metadata_lote['alunos_fim'] = fim
        
        payload = {
            'tipo': 'licoes_alunos_lote',
            'lote_numero': lote_num,
            'total_lotes': num_lotes,
            **lote_dados,
            'metadata': metadata_lote
        }
        
        try:
            print(f"   📡 Enviando {fim - inicio} alunos...")
            response = requests.post(
                URL_APPS_SCRIPT_HISTORICO, 
                json=payload, 
                timeout=LOTE_TIMEOUT
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('sucesso'):
                    print(f"   ✅ Lote {lote_num} processado com sucesso")
                    
                    if lote_num == num_lotes and 'planilha' in result:
                        planilha_url = result['planilha'].get('url')
                else:
                    print(f"   ⚠️ Erro no lote {lote_num}: {result.get('erro', 'Desconhecido')}")
            else:
                print(f"   ⚠️ HTTP {response.status_code} no lote {lote_num}")
        
        except Exception as e:
            print(f"   ❌ Erro ao enviar lote {lote_num}: {e}")
        
        if lote_num < num_lotes:
            time.sleep(2)
    
    print("\n" + "=" * 80)
    if planilha_url:
        print(f"✅ ENVIO COMPLETO!")
        print(f"📊 Planilha criada: {planilha_url}")
    else:
        print("⚠️ Envio finalizado, mas URL da planilha não disponível")
    print("=" * 80)

def executar_historico(cookies_dict, alunos_modulo2):
    """🎯 MÓDULO 3: Coleta histórico individual"""
    tempo_inicio = time.time()
    historico_stats['tempo_inicio'] = tempo_inicio
    
    print("\n" + "=" * 80)
    print("📚 MÓDULO 3: HISTÓRICO INDIVIDUAL")
    print("=" * 80)
    
    if not alunos_modulo2:
        print("❌ Nenhum aluno recebido do Módulo 2. Abortando.")
        return
    
    print(f"🎓 Total de alunos a processar: {len(alunos_modulo2)}")
    print(f"⚡ Conexões simultâneas: {HISTORICO_ASYNC_CONNECTIONS}")
    print(f"📦 Tamanho do chunk: {HISTORICO_CHUNK_SIZE}")
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    # FASE 1: COLETA ASSÍNCRONA
    print(f"\n⚡ FASE 1: Coleta assíncrona em alta velocidade...")
    
    falhas_fase1 = []
    total_chunks = (len(alunos_modulo2) + HISTORICO_CHUNK_SIZE - 1) // HISTORICO_CHUNK_SIZE
    
    for i in range(0, len(alunos_modulo2), HISTORICO_CHUNK_SIZE):
        chunk = alunos_modulo2[i:i+HISTORICO_CHUNK_SIZE]
        chunk_num = i // HISTORICO_CHUNK_SIZE + 1
        
        safe_print(f"📦 Chunk {chunk_num}/{total_chunks} ({len(chunk)} alunos)...")
        
        dados_chunk, falhas_chunk = asyncio.run(processar_chunk_async(chunk, cookies_dict))
        todos_dados = mesclar_dados(todos_dados, dados_chunk)
        falhas_fase1.extend(falhas_chunk)
        
        if i + HISTORICO_CHUNK_SIZE < len(alunos_modulo2):
            time.sleep(0.5)
    
    print(f"\n✅ FASE 1 CONCLUÍDA")
    print(f"   Sucesso: {historico_stats['fase1_sucesso']} | Falhas: {len(falhas_fase1)}")
    
    # FASE 2: FALLBACK ROBUSTO
    if falhas_fase1:
        dados_fase2, falhas_fase2 = coletar_fallback_robusto(falhas_fase1, cookies_dict)
        todos_dados = mesclar_dados(todos_dados, dados_fase2)
        
        print(f"✅ FASE 2 CONCLUÍDA")
        print(f"   Recuperados: {historico_stats['fase2_sucesso']} | Falhas: {len(falhas_fase2)}")
    else:
        falhas_fase2 = []
        print("\n🎉 FASE 2 não necessária - todos processados na Fase 1!")
    
    # FASE 3: COLETA CIRÚRGICA
    if falhas_fase2:
        dados_fase3, falhas_finais = coletar_cirurgico(falhas_fase2, cookies_dict)
        todos_dados = mesclar_dados(todos_dados, dados_fase3)
        
        print(f"✅ FASE 3 CONCLUÍDA")
        print(f"   Recuperados: {historico_stats['fase3_sucesso']} | Falhas: {len(falhas_finais)}")
        
        if falhas_finais:
            print("\n⚠️ ALUNOS NÃO COLETADOS:")
            for aluno in falhas_finais:
                print(f"   - ID {aluno['id_aluno']}: {aluno['nome']}")
    else:
        print("\n🎉 FASE 3 não necessária!")
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n✅ Coleta finalizada!")
    print(f"   Alunos processados: {len(historico_stats['alunos_processados'])}")
    print(f"   Com dados: {historico_stats['com_dados']}")
    print(f"   Sem dados: {historico_stats['sem_dados']}")
    print(f"⏱️ Tempo: {tempo_total/60:.2f} minutos")
    
    todos_dados = filtrar_dados_vazios(todos_dados)
    
    # Backup local
    timestamp = gerar_timestamp()
    backup_file = f'modulo3_historico_{timestamp.replace(":", "-")}.json'
    resumo_alunos = gerar_resumo_alunos(alunos_modulo2, todos_dados)
    
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({
            'dados': todos_dados,
            'resumo': resumo_alunos,
            'timestamp': timestamp
        }, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # Enviar em lotes
    enviar_lotes_google_sheets(todos_dados, alunos_modulo2, tempo_total)

# ==================== MAIN - ORQUESTRADOR SEQUENCIAL ====================
def main():
    tempo_inicio_total = time.time()
    
    print("=" * 80)
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("🚀 EXECUTANDO COLETA SEQUENCIAL COMPLETA - VERSÃO CORRIGIDA")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("📜 Script: script_seq1_CORRIGIDO.py")
    print("⏱️  Timeout: 6 HORAS (máximo do GitHub Actions)")
    print("🎯 Estratégia: 3 Módulos Sequenciais com Retry")
    print("💪 Cada módulo cria JSON local + Planilha no Google Sheets")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("=" * 80)
    
    print("\n" + "=" * 80)
    print("🎼 SISTEMA MUSICAL - COLETOR SEQUENCIAL CORRIGIDO")
    print("=" * 80)
    print("📋 Ordem de execução:")
    print("   1️⃣ Localidades de Hortolândia → Salva JSON + Envia Google Sheets")
    print("   2️⃣ Buscar Alunos (VARREDURA usando IDs do Módulo 1) → Salva JSON + Envia Sheets")
    print("   3️⃣ Histórico Individual (usa alunos do Módulo 2) → Salva + Envia")
    print("=" * 80)
    
    # PASSO 1: Login único
    session, cookies = fazer_login_unico()
    
    if not session:
        print("\n❌ Falha no login. Encerrando processo.")
        return
    
    # PASSO 2: Executar Módulo 1 - Localidades
    print("\n🚀 Iniciando Módulo 1...")
    ids_igrejas = executar_localidades(session)
    
    if not ids_igrejas:
        print("\n⚠️ Módulo 1 não encontrou localidades. Interrompendo processo.")
        return
    
    # PASSO 3: Executar Módulo 2 - Alunos (VARREDURA usando IDs do Módulo 1)
    print("\n🚀 Iniciando Módulo 2...")
    alunos = executar_busca_alunos(session, ids_igrejas)
    
    if not alunos:
        print("\n⚠️ Módulo 2 não encontrou alunos. Interrompendo processo.")
        return
    
    # PASSO 4: Executar Módulo 3 - Histórico (usando alunos do Módulo 2)
    print("\n🚀 Iniciando Módulo 3...")
    executar_historico(cookies, alunos)
    
    # RESUMO FINAL
    tempo_total = time.time() - tempo_inicio_total
    
    print("\n" + "=" * 80)
    print("🎉 PROCESSO COMPLETO FINALIZADO!")
    print("=" * 80)
    print(f"⏱️ Tempo total: {tempo_total/60:.2f} minutos ({tempo_total/3600:.2f} horas)")
    print(f"📊 Módulos executados:")
    print(f"   ✅ Módulo 1: {len(ids_igrejas)} localidades")
    print(f"   ✅ Módulo 2: {len(alunos)} alunos encontrados")
    print(f"   ✅ Módulo 3: {len(historico_stats['alunos_processados'])} históricos")
    print(f"💾 Todos os backups salvos localmente:")
    print(f"   - modulo1_localidades_*.json")
    print(f"   - modulo2_alunos_*.json")
    print(f"   - modulo3_historico_*.json")
    print(f"📊 Planilhas criadas no Google Sheets")
    print(f"✅ Todas as datas no formato DD/MM/YYYY")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()

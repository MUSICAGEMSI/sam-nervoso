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

# URLs dos Apps Scripts (Síncronas, via requests no final)
URL_APPS_SCRIPT_AULAS = 'https://script.google.com/macros/s/AKfycbyvEGIUPIvgbSuT_yikqg03nEjqXryd6RfI121A3pRt75v9oJoFNLTdvo3-onNdEsJd/exec'
URL_APPS_SCRIPT_TURMAS = 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec'
URL_APPS_SCRIPT_MATRICULAS = 'https://script.google.com/macros/s/AKfycbxnp24RMIG4zQEsot0KATnFjdeoEHP7nyrr4WXnp-LLLptQTT-Vc_UPYoy__VWipill/exec'

# Cache global
INSTRUTORES_HORTOLANDIA = {}
NOMES_COMPLETOS_NORMALIZADOS = set()

# Controle de Conexões Simultâneas (O "Freio de Mão" do Trator)
MAX_CONEXOES_SIMULTANEAS = 300

# ==================== FUNÇÕES AUXILIARES ====================

def normalizar_nome(nome):
    """Normaliza nome para comparação consistente"""
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(char for char in nome if unicodedata.category(char) != 'Mn')
    nome = nome.replace('/', ' ').replace('\\', ' ').replace('-', ' ')
    return ' '.join(nome.upper().split())

def gerar_timestamp():
    return datetime.now().strftime('%d_%m_%Y-%H:%M')

async def fetch_com_retry(session, url, semaphore, headers=None, max_tentativas=3):
    """Realiza requisição GET assíncrona com controle de semáforo e retentativas"""
    async with semaphore:
        for tentativa in range(max_tentativas):
            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status in [429, 500, 502, 503, 504]:
                        await asyncio.sleep(1 * (tentativa + 1))
                        continue
                    if resp.status != 200:
                        return resp.status, None
                    text = await resp.text()
                    return resp.status, text
            except (aiohttp.ClientError, asyncio.TimeoutError):
                await asyncio.sleep(1 * (tentativa + 1))
        return None, None

def extrair_cookies_playwright(pagina):
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

# ==================== LOGIN ÚNICO ====================

def fazer_login_unico_sync():
    """Realiza login único via Playwright (Síncrono)"""
    print("\n" + "=" * 80)
    print("🔐 REALIZANDO LOGIN ÚNICO")
    print("=" * 80)
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.set_extra_http_headers({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        
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
        user_agent = pagina.evaluate("() => navigator.userAgent")
        navegador.close()
    
    return cookies_dict, user_agent

# ==================== MÓDULO 1: HISTÓRICO DE AULAS ====================

async def carregar_instrutores_hortolandia(session):
    print("\n📋 Carregando instrutores de Hortolândia...")
    url = "https://musical.congregacao.org.br/licoes/instrutores?q=a"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Connection': 'keep-alive'
    }
    
    for tentativa in range(1, 4):
        try:
            async with session.get(url, headers=headers, timeout=20) as resp:
                if resp.status != 200:
                    continue
                try:
                    instrutores = await resp.json()
                except:
                    # Fallback manual caso a descompressão falhe
                    texto = await resp.text()
                    instrutores = json.loads(texto)
                
                ids_dict = {}
                nomes_normalizados = set()
                
                for instrutor in instrutores:
                    id_inst = instrutor['id']
                    partes = instrutor['text'].split(' - ')
                    if len(partes) >= 2:
                        nome_completo = f"{partes[0].strip()} - {partes[1].strip()}"
                        ids_dict[id_inst] = nome_completo
                        nomes_normalizados.add(normalizar_nome(nome_completo))
                        
                print(f"✅ {len(ids_dict)} instrutores carregados!")
                return ids_dict, nomes_normalizados
        except Exception as e:
            await asyncio.sleep(2)
            
    print("❌ Falha ao carregar instrutores\n")
    return {}, set()

async def extrair_data_hora_abertura_rapido(session, aula_id, semaphore):
    url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    status, html = await fetch_com_retry(session, url, semaphore, headers=headers)
    
    if status == 200 and html:
        soup = BeautifulSoup(html, 'lxml') # Mudança para o parser rápido em C
        tbody = soup.find('tbody')
        if tbody:
            for row in tbody.find_all('tr'):
                td_strong = row.find('strong')
                if td_strong and 'Data e Horário de abertura' in td_strong.get_text():
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        valor = tds[1].get_text(strip=True)
                        try:
                            return datetime.strptime(valor, '%d/%m/%Y %H:%M:%S')
                        except:
                            try:
                                return datetime.strptime(valor, '%d/%m/%Y %H:%M')
                            except:
                                pass
    return None

async def buscar_limites_ids_aulas(session, semaphore, data_inicio, data_fim):
    """Realiza a busca binária para encontrar os IDs inicial e final"""
    print(f"\n🔍 BUSCA BINÁRIA: Encontrando IDs de aulas para o período...")
    
    async def busca_binaria(alvo, buscando_primeiro=True):
        esquerda, direita = 1, 1000000
        melhor_id = None
        for _ in range(50):
            if esquerda > direita: break
            meio = (esquerda + direita) // 2
            data_hora = await extrair_data_hora_abertura_rapido(session, meio, semaphore)
            
            if data_hora is None:
                direita = meio - 1 if buscando_primeiro else esquerda + 1
                continue
                
            if buscando_primeiro:
                if data_hora >= alvo:
                    melhor_id = meio
                    direita = meio - 1
                else:
                    esquerda = meio + 1
            else:
                if data_hora <= alvo:
                    melhor_id = meio
                    esquerda = meio + 1
                else:
                    direita = meio - 1
        return melhor_id

    primeiro_id = await busca_binaria(data_inicio, buscando_primeiro=True)
    ultimo_id = await busca_binaria(data_fim, buscando_primeiro=False)
    
    print(f"✅ Limites encontrados: ID Inicial [{primeiro_id}] até ID Final [{ultimo_id}]")
    return primeiro_id, ultimo_id

async def coletar_tudo_de_uma_vez_async(session, aula_id, semaphore):
    """Versão Turbo Async da coleta de aulas"""
    global INSTRUTORES_HORTOLANDIA, NOMES_COMPLETOS_NORMALIZADOS
    
    url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
    headers = {'X-Requested-With': 'XMLHttpRequest', 'Referer': URL_INICIAL}
    
    status, html = await fetch_com_retry(session, url, semaphore, headers)
    if status != 200 or not html: return None
    
    soup = BeautifulSoup(html, 'lxml')
    
    nome_instrutor_html = ""
    tbody = soup.find('tbody')
    if not tbody: return None
    
    for row in tbody.find_all('tr'):
        strg = row.find('strong')
        if strg and 'Instrutor(a) que ministrou a aula' in strg.get_text():
            tds = row.find_all('td')
            if len(tds) >= 2:
                nome_instrutor_html = tds[1].get_text(strip=True)
                break
                
    # Filtro imediato (Fail-fast)
    eh_hortolandia = False
    if nome_instrutor_html:
        if normalizar_nome(nome_instrutor_html) in NOMES_COMPLETOS_NORMALIZADOS:
            eh_hortolandia = True
            
    if not eh_hortolandia: return None
    
    # Se passou, coleta o resto
    data_aula, descricao, comum, hora_inicio, hora_termino, data_hora_abertura = "", "", "", "", "", ""
    id_turma, tem_ata, texto_ata = "", "Não", ""
    
    modal_header = soup.find('div', class_='modal-header')
    if modal_header:
        date_span = modal_header.find('span', class_='pull-right')
        if date_span: data_aula = date_span.get_text(strip=True)

    for row in tbody.find_all('tr'):
        strg = row.find('strong')
        if not strg: continue
        label = strg.get_text(strip=True)
        tds = row.find_all('td')
        if len(tds) < 2: continue
        valor = tds[1].get_text(strip=True)
        
        if 'Comum Congregação' in label: comum = valor.upper()
        elif 'Início' in label and 'Horário' not in label: hora_inicio = valor[:5]
        elif 'Término' in label: hora_termino = valor[:5]
        elif 'Data e Horário de abertura' in label: data_hora_abertura = valor

    table = soup.find('table', class_='table')
    if table and table.find('thead'):
        td_desc = table.find('thead').find('td', class_='bg-blue-gradient')
        if td_desc: descricao = re.sub(r'\s+', ' ', td_desc.get_text(strip=True)).strip()
        
    for tab in soup.find_all('table', class_='table'):
        th = tab.find('thead')
        if th and th.find('tr', class_='bg-green-gradient'):
            tem_ata = "Sim"
            tb = tab.find('tbody')
            if tb and tb.find('td'):
                texto_ata = tb.find('td').get_text(strip=True)
            break

    dia_semana = ""
    if data_aula:
        try:
            dia_semana = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'][datetime.strptime(data_aula, '%d/%m/%Y').weekday()]
        except: pass

    # Requisições adicionais (em paralelo)
    url_editar = f"https://musical.congregacao.org.br/aulas_abertas/editar/{aula_id}"
    status_ed, html_ed = await fetch_com_retry(session, url_editar, semaphore, headers)
    if status_ed == 200 and html_ed:
        t_input = BeautifulSoup(html_ed, 'lxml').find('input', {'name': 'id_turma'})
        if t_input: id_turma = t_input.get('value', '').strip()

    total_alunos, presentes = 0, 0
    lista_presentes, lista_ausentes = "", ""
    relacao_alunos_aula = {}

    if id_turma:
        url_freq = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_frequencias/{aula_id}/{id_turma}"
        status_fq, html_fq = await fetch_com_retry(session, url_freq, semaphore, headers)
        if status_fq == 200 and html_fq:
            tb_freq = BeautifulSoup(html_fq, 'lxml').find('tbody')
            if tb_freq:
                linhas = tb_freq.find_all('tr')
                total_alunos = len(linhas)
                pres_ids, aus_ids = [], []
                
                for linha in linhas:
                    td_nome = linha.find('td')
                    if not td_nome: continue
                    link = linha.find('a', {'data-id-membro': True})
                    if not link: continue
                    
                    id_membro = link.get('data-id-membro', '').strip()
                    nome_completo = td_nome.get_text(strip=True)
                    relacao_alunos_aula[id_membro] = nome_completo
                    
                    if linha.find('i', class_='fa-check'): pres_ids.append(id_membro)
                    else: aus_ids.append(id_membro)
                        
                lista_presentes = "; ".join(pres_ids)
                lista_ausentes = "; ".join(aus_ids)
                presentes = len(pres_ids)

    return {
        'id_aula': aula_id, 'id_turma': id_turma, 'descricao': descricao, 'comum': comum,
        'dia_semana': dia_semana, 'hora_inicio': hora_inicio, 'hora_termino': hora_termino,
        'data_aula': data_aula, 'data_hora_abertura': data_hora_abertura, 'tem_ata': tem_ata,
        'texto_ata': texto_ata, 'instrutor': nome_instrutor_html, 'total_alunos': total_alunos,
        'presentes': presentes, 'lista_presentes': lista_presentes, 'lista_ausentes': lista_ausentes,
        'relacao_alunos_aula': relacao_alunos_aula
    }

async def executar_historico_aulas_async(session, semaphore):
    global INSTRUTORES_HORTOLANDIA, NOMES_COMPLETOS_NORMALIZADOS
    tempo_inicio = time.time()
    
    print("\n" + "=" * 80 + "\n📚 MÓDULO 1: HISTÓRICO DE AULAS (TURBO ASYNC)\n" + "=" * 80)
    INSTRUTORES_HORTOLANDIA, NOMES_COMPLETOS_NORMALIZADOS = await carregar_instrutores_hortolandia(session)
    if not INSTRUTORES_HORTOLANDIA: return None

    dt_inicio = datetime(2024, 1, 1)
    dt_fim = datetime.now()
    primeiro_id, ultimo_id = await buscar_limites_ids_aulas(session, semaphore, dt_inicio, dt_fim)
    if not primeiro_id: return None
    ultimo_id = ultimo_id or (primeiro_id + 50000)

    resultado = []
    dicionario_global_alunos = {}
    aulas_processadas, aulas_hortolandia, aulas_com_ata = 0, 0, 0
    
    print(f"\n🚀 Disparando varredura massiva de {primeiro_id:,} a {ultimo_id:,}...")
    
    # Processamento em Lotes para não estourar a memória RAM do Kaggle
    LOTE = 5000 
    for lote_inicio in range(primeiro_id, ultimo_id + 1, LOTE):
        lote_fim = min(lote_inicio + LOTE - 1, ultimo_id)
        tarefas = [coletar_tudo_de_uma_vez_async(session, i, semaphore) for i in range(lote_inicio, lote_fim + 1)]
        
        for task in asyncio.as_completed(tarefas):
            dados = await task
            aulas_processadas += 1
            if dados:
                aulas_hortolandia += 1
                if dados['tem_ata'] == "Sim": aulas_com_ata += 1
                dicionario_global_alunos.update(dados['relacao_alunos_aula'])
                resultado.append([
                    dados['id_aula'], dados['id_turma'], dados['descricao'], dados['comum'],
                    dados['dia_semana'], dados['hora_inicio'], dados['hora_termino'], dados['data_aula'],
                    dados['data_hora_abertura'], dados['tem_ata'], dados['texto_ata'], dados['instrutor'],
                    dados['total_alunos'], dados['presentes'], dados['lista_presentes'], dados['lista_ausentes']
                ])
                
            if aulas_processadas % 1000 == 0:
                print(f"   [{aulas_processadas:6d}] Lidas | {aulas_hortolandia} HTL | {aulas_com_ata} ATA")

    # Envio Síncrono (Request normal para o Apps Script)
    relacao_lista = [[id_a, nom] for id_a, nom in dicionario_global_alunos.items() if id_a and nom]
    body = {
        "tipo": "historico_aulas_hortolandia", "dados": resultado, "relacao_alunos": relacao_lista,
        "headers": ["ID_Aula", "ID_Turma", "Descrição", "Comum", "Dia_Semana", "Hora_Início", "Hora_Término", "Data_Aula", "Data_Hora_Abertura", "Tem_Ata", "Texto_Ata", "Instrutor", "Total_Alunos", "Presentes", "IDs_Presentes", "IDs_Ausentes"],
        "resumo": {
            "periodo_inicio": dt_inicio.strftime('%d/%m/%Y %H:%M:%S'), "periodo_fim": dt_fim.strftime('%d/%m/%Y %H:%M:%S'),
            "total_aulas": len(resultado), "aulas_processadas": aulas_processadas, "aulas_com_ata": aulas_com_ata,
            "tempo_minutos": round((time.time() - tempo_inicio)/60, 2)
        }
    }
    
    with open(f'backup_aulas_{gerar_timestamp().replace(":", "")}.json', 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
        
    print("\n📤 Enviando para Google Sheets...")
    try:
        req = requests.post(URL_APPS_SCRIPT_AULAS, json=body, timeout=300)
        if req.status_code == 200: print(f"✅ PLANILHA AULAS OK!")
    except Exception as e: print(f"⚠️ Erro no envio: {e}")
    
    return resultado

# ==================== MÓDULO 2: TURMAS ====================

async def coletar_turma_async(session, turma_id, semaphore, ids_instrutores_htl):
    """Unifica verificação e extração num único GET super rápido"""
    url = f"https://musical.congregacao.org.br/turmas/editar/{turma_id}"
    status, html = await fetch_com_retry(session, url, semaphore)
    
    # Se a página for inválida ou não tiver o formulário, descarta em 1 milissegundo.
    if status != 200 or not html or 'form' not in html.lower():
        return False, None
        
    soup = BeautifulSoup(html, 'lxml')
    if not soup.find('form', id='turmas'): return False, None
    
    dados = {'id_turma': turma_id, 'curso': '', 'descricao': '', 'comum': '', 'dia_semana': '', 'data_inicio': '', 'data_encerramento': '', 'hora_inicio': '', 'hora_termino': '', 'responsavel_1_id': '', 'responsavel_1_nome': '', 'responsavel_2_id': '', 'responsavel_2_nome': '', 'destinado_ao': '', 'ativo': 'Não', 'cadastrado_em': '', 'cadastrado_por': '', 'atualizado_em': '', 'atualizado_por': ''}
    
    # Extração de Selects e Inputs padrão
    c_sel = soup.find('select', {'name': 'id_curso'})
    if c_sel and c_sel.find('option', selected=True): dados['curso'] = c_sel.find('option', selected=True).get_text(strip=True)
    
    desc_inp = soup.find('input', {'name': 'descricao'})
    if desc_inp: dados['descricao'] = desc_inp.get('value', '').strip()
    
    cm_sel = soup.find('select', {'name': 'id_igreja'})
    if cm_sel and cm_sel.find('option', selected=True): dados['comum'] = cm_sel.find('option', selected=True).get_text(strip=True).split('|')[0].strip()
    
    dia_sel = soup.find('select', {'name': 'dia_semana'})
    if dia_sel and dia_sel.find('option', selected=True): dados['dia_semana'] = dia_sel.find('option', selected=True).get_text(strip=True)
    
    for campo in ['dt_inicio', 'dt_fim', 'hr_inicio', 'hr_fim']:
        inp = soup.find('input', {'name': campo})
        if inp:
            v = inp.get('value', '').strip()
            if campo.startswith('hr'): v = v[:5]
            dados[campo.replace('dt', 'data').replace('hr', 'hora').replace('fim', 'encerramento')] = v

    # RegEx no Javascript
    for script in soup.find_all('script'):
        txt = script.string
        if txt and 'id_responsavel' in txt:
            m1 = re.search(r"const option = '<option value=\"(\d+)\" selected>(.*?)</option>'", txt)
            if m1: 
                dados['responsavel_1_id'], dados['responsavel_1_nome'] = m1.group(1), m1.group(2).split(' - ')[0].strip()
            m2 = re.search(r"const option2 = '<option value=\"(\d+)\" selected>(.*?)</option>'", txt)
            if m2: 
                dados['responsavel_2_id'], dados['responsavel_2_nome'] = m2.group(1), m2.group(2).split(' - ')[0].strip()

    # Histórico
    hist = soup.find('div', id='collapseOne')
    if hist:
        for p in hist.find_all('p'):
            t = p.get_text(strip=True)
            if 'Cadastrado em:' in t:
                p_ = t.split('por:')
                if len(p_) >= 2: dados['cadastrado_em'], dados['cadastrado_por'] = p_[0].replace('Cadastrado em:', '').strip(), p_[1].strip()
            elif 'Atualizado em:' in t:
                p_ = t.split('por:')
                if len(p_) >= 2: dados['atualizado_em'], dados['atualizado_por'] = p_[0].replace('Atualizado em:', '').strip(), p_[1].strip()

    # FILTRO: Lógica 100% igual ao Seq2 Original
    is_htl = dados['responsavel_1_id'] in ids_instrutores_htl or dados['responsavel_2_id'] in ids_instrutores_htl
    return True, dados if is_htl else None

async def executar_turmas_async(session, semaphore):
    tempo_inicio = time.time()
    print("\n" + "=" * 80 + "\n🎓 MÓDULO 2: TURMAS (VARREDURA 1-GET ASYNC)\n" + "=" * 80)
    
    ids_instrutores_htl = INSTRUTORES_HORTOLANDIA
    if not ids_instrutores_htl: return None, []

    resultado, ids_htl_validos = [], []
    processadas, turmas_htl = 0, 0
    
    print("🚀 Varrendo IDs 20.000 a 70.000 (Isto será muito rápido)...")
    LOTE = 10000
    for lote_inicio in range(20000, 70001, LOTE):
        lote_fim = min(lote_inicio + LOTE - 1, 70000)
        tarefas = [coletar_turma_async(session, i, semaphore, ids_instrutores_htl) for i in range(lote_inicio, lote_fim + 1)]
        
        for task in asyncio.as_completed(tarefas):
            existe, dados = await task
            if existe: processadas += 1
            if dados:
                turmas_htl += 1
                ids_htl_validos.append(dados['id_turma'])
                resultado.append([
                    dados['id_turma'], dados['curso'], dados['descricao'], dados['comum'], dados['dia_semana'],
                    dados['data_inicio'], dados['data_encerramento'], dados['hora_inicio'], dados['hora_termino'],
                    dados['responsavel_1_id'], dados['responsavel_1_nome'], dados['responsavel_2_id'], dados['responsavel_2_nome'],
                    dados['destinado_ao'], dados['ativo'], dados['cadastrado_em'], dados['cadastrado_por'], dados['atualizado_em'], dados['atualizado_por'],
                    'Coletado', datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                ])
                print(f"[{turmas_htl:3d}] ID {dados['id_turma']:5d} ✅ HTL | {dados['curso']} | {dados['responsavel_1_nome']}")

    print(f"\n📊 Turmas Reais: {processadas} | Filtradas Hortolândia: {turmas_htl}")
    
    # Envio Síncrono para o Sheets
    body = {
        "tipo": "dados_turmas", "dados": resultado,
        "headers": ["ID_Turma", "Curso", "Descricao", "Comum", "Dia_Semana", "Data_Inicio", "Data_Encerramento", "Hora_Inicio", "Hora_Termino", "Responsavel_1_ID", "Responsavel_1_Nome", "Responsavel_2_ID", "Responsavel_2_Nome", "Destinado_ao", "Ativo", "Cadastrado_em", "Cadastrado_por", "Atualizado_em", "Atualizado_por", "Status_Coleta", "Data_Coleta"]
    }
    
    try:
        req = requests.post(URL_APPS_SCRIPT_TURMAS, json=body, timeout=120)
        if req.status_code == 200: print(f"✅ PLANILHA TURMAS OK!")
    except Exception as e: print(f"⚠️ Erro no envio: {e}")
    
    return resultado, ids_htl_validos

# ==================== MÓDULO 3: MATRICULADOS ====================

async def extrair_alunos_async(session, turma_id, semaphore):
    url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
    headers = {'X-Requested-With': 'XMLHttpRequest', 'Referer': URL_INICIAL}
    
    status, html = await fetch_com_retry(session, url, semaphore, headers)
    if status != 200 or not html: return turma_id, None
    
    soup = BeautifulSoup(html, 'lxml')
    tbody = soup.find('tbody')
    if not tbody: return turma_id, []
    
    alunos = []
    for row in tbody.find_all('tr'):
        tds = row.find_all('td')
        if len(tds) >= 4:
            nm = tds[0].get_text(strip=True)
            if not nm or 'Nenhum registro' in nm: continue
            alunos.append({
                'ID_Turma': turma_id, 'Nome': nm, 'Comum': tds[1].get_text(strip=True),
                'Instrumento': tds[2].get_text(strip=True), 'Status': tds[3].get_text(strip=True)
            })
    return turma_id, alunos

async def executar_matriculados_async(session, semaphore, ids_turmas):
    print("\n" + "=" * 80 + "\n👥 MÓDULO 3: ALUNOS (ASYNC)\n" + "=" * 80)
    if not ids_turmas: return
    
    resultados_resumo, todos_alunos = [], []
    tarefas = [extrair_alunos_async(session, tid, semaphore) for tid in ids_turmas]
    
    for task in asyncio.as_completed(tarefas):
        tid, alunos = await task
        if alunos is not None:
            todos_alunos.extend(alunos)
            resultados_resumo.append([tid, len(alunos), "Sucesso"])
            print(f"✅ Turma {tid}: {len(alunos)} alunos")
        else:
            resultados_resumo.append([tid, 0, "Erro"])
            
    # Envio síncrono para o Sheets
    dados_alunos_envio = [["ID_Turma", "Nome", "Comum", "Instrumento", "Status"]] + [[a['ID_Turma'], a['Nome'], a['Comum'], a['Instrumento'], a['Status']] for a in todos_alunos]
    body = {"tipo": "contagem_matriculas", "dados": [["ID_Turma", "Quantidade_Matriculados", "Status_Coleta"]] + resultados_resumo}
    
    try:
        req = requests.post(URL_APPS_SCRIPT_MATRICULAS, json=body)
        if req.status_code == 200:
            pid = req.json().get('detalhes', {}).get('planilha_id')
            requests.post(URL_APPS_SCRIPT_MATRICULAS, json={"tipo": "alunos_detalhados", "dados": dados_alunos_envio, "planilha_id": pid})
            print("✅ MATRÍCULAS ENVIADAS OK!")
    except Exception as e: print(f"⚠️ Erro no envio: {e}")

# ==================== MAIN ORQUESTRADOR ASYNC ====================

async def main_async(cookies_dict, user_agent):
    tempo_total_inicio = time.time()
    
    # 2. Abre a Sessão Assíncrona do "Trator" com os cookies já coletados
    headers_globais = {
        'User-Agent': user_agent,
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9',
    }
    
    conn = aiohttp.TCPConnector(limit=MAX_CONEXOES_SIMULTANEAS)
    semaphore = asyncio.Semaphore(MAX_CONEXOES_SIMULTANEAS)
    
    async with aiohttp.ClientSession(cookies=cookies_dict, headers=headers_globais, connector=conn) as session:
        # Roda o Módulo 1
        res_aulas = await executar_historico_aulas_async(session, semaphore)
        if not res_aulas: return
        
        # Roda o Módulo 2
        res_turmas, ids_turmas = await executar_turmas_async(session, semaphore)
        if not ids_turmas: return
        
        # Roda o Módulo 3
        await executar_matriculados_async(session, semaphore, ids_turmas)

    print("\n" + "=" * 80)
    print("🎉 PROCESSO TURBO ASYNC FINALIZADO!")
    print(f"⏱️ Tempo total da extração: {(time.time() - tempo_total_inicio)/60:.2f} minutos")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    # 1. Faz o login síncrono ANTES de iniciar o loop de eventos
    cookies, ua = fazer_login_unico_sync()
    
    # 2. Se o login deu certo, injeta os dados e dispara o motor assíncrono
    if cookies:
        asyncio.run(main_async(cookies, ua))

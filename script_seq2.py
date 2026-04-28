import os
import re
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
import json
import unicodedata
import asyncio
import aiohttp
from typing import List, Dict, Optional, Set, Tuple

# ==================== GERENCIAMENTO DE CREDENCIAIS (KAGGLE / LOCAL) ====================
try:
    from kaggle_secrets import UserSecretsClient
    user_secrets = UserSecretsClient()
    EMAIL = user_secrets.get_secret("LOGIN_MUSICAL")
    SENHA = user_secrets.get_secret("SENHA_MUSICAL")
    IS_KAGGLE = True
    print("🌍 Rodando no ambiente Kaggle - Secrets carregados com sucesso.")
except Exception:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path="credencial.env")
    EMAIL = os.environ.get("LOGIN_MUSICAL")
    SENHA = os.environ.get("SENHA_MUSICAL")
    IS_KAGGLE = False
    print("💻 Rodando localmente - Variáveis de ambiente carregadas.")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ==================== CONFIGURAÇÕES GLOBAIS ====================
URL_INICIAL = "https://musical.congregacao.org.br/"

URL_APPS_SCRIPT_AULAS = 'https://script.google.com/macros/s/AKfycbyvEGIUPIvgbSuT_yikqg03nEjqXryd6RfI121A3pRt75v9oJoFNLTdvo3-onNdEsJd/exec'
URL_APPS_SCRIPT_TURMAS = 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec'
URL_APPS_SCRIPT_MATRICULAS = 'https://script.google.com/macros/s/AKfycbxnp24RMIG4zQEsot0KATnFjdeoEHP7nyrr4WXnp-LLLptQTT-Vc_UPYoy__VWipill/exec'

# Parâmetros Turbo
ASYNC_CONEXOES_MAX = 300
LOTE_CHUNK_SIZE = 5000

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas.")
    exit(1)

# ==================== FUNÇÕES AUXILIARES ====================

def extrair_cookies_playwright(pagina):
    return {cookie['name']: cookie['value'] for cookie in pagina.context.cookies()}

def normalizar_nome(nome):
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(char for char in nome if unicodedata.category(char) != 'Mn')
    nome = nome.replace('/', ' ').replace('\\', ' ').replace('-', ' ')
    return ' '.join(nome.upper().split())

def gerar_timestamp():
    return datetime.now().strftime('%d_%m_%Y-%H:%M')

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
            print("   ❌ Falha no login.")
            navegador.close()
            return None, None
        
        cookies_dict = extrair_cookies_playwright(pagina)
        user_agent = pagina.evaluate("() => navigator.userAgent")
        navegador.close()
    
    return cookies_dict, user_agent

# ==================== MÓDULO 1: HISTÓRICO DE AULAS (ASYNC) ====================

async def carregar_instrutores_hortolandia_async(session: aiohttp.ClientSession):
    print("\n📋 Carregando instrutores de Hortolândia...")
    url = "https://musical.congregacao.org.br/licoes/instrutores?q=a"
    headers = {'Accept': 'application/json, text/plain, */*', 'Accept-Encoding': 'gzip, deflate, br'}
    
    for t in range(3):
        try:
            async with session.get(url, headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    try:
                        instrutores = await resp.json()
                    except:
                        texto = await resp.text()
                        instrutores = json.loads(texto)
                    
                    ids_dict, nomes_norm = {}, set()
                    for inst in instrutores:
                        partes = inst['text'].split(' - ')
                        if len(partes) >= 2:
                            nome_completo = f"{partes[0].strip()} - {partes[1].strip()}"
                            ids_dict[inst['id']] = nome_completo
                            nomes_norm.add(normalizar_nome(nome_completo))
                    
                    print(f"✅ {len(ids_dict)} instrutores carregados!")
                    return ids_dict, nomes_norm
        except Exception as e:
            await asyncio.sleep(1)
            
    print("❌ Falha ao carregar instrutores.")
    return {}, set()

async def extrair_data_hora_abertura_rapido_async(session: aiohttp.ClientSession, aula_id: int):
    url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
    try:
        async with session.get(url, headers={'X-Requested-With': 'XMLHttpRequest'}, timeout=5) as resp:
            if resp.status == 200:
                html = await resp.text()
                if 'Data e Horário de abertura' in html:
                    soup = BeautifulSoup(html, 'lxml')
                    rows = soup.find_all('tr')
                    for row in rows:
                        strong = row.find('strong')
                        if strong and 'Data e Horário de abertura' in strong.get_text():
                            tds = row.find_all('td')
                            if len(tds) >= 2:
                                valor = tds[1].get_text(strip=True)
                                for fmt in ['%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M']:
                                    try: return datetime.strptime(valor, fmt)
                                    except: pass
    except: pass
    return None

async def buscar_primeiro_id_async(session: aiohttp.ClientSession, alvo: datetime):
    print(f"\n🔍 BUSCA BINÁRIA: Primeiro ID >= {alvo.strftime('%d/%m/%Y %H:%M')}")
    esq, dir_idx = 1, 1000000
    melhor_id, melhor_data = None, None
    
    while esq <= dir_idx:
        meio = (esq + dir_idx) // 2
        data_abertura = await extrair_data_hora_abertura_rapido_async(session, meio)
        print(f"   ID {meio:,} -> {data_abertura.strftime('%d/%m/%Y %H:%M') if data_abertura else 'INEXISTENTE'}")
        
        if data_abertura is None:
            dir_idx = meio - 1
        elif data_abertura >= alvo:
            melhor_id, melhor_data = meio, data_abertura
            dir_idx = meio - 1
        else:
            esq = meio + 1
            
    print(f"✅ ID encontrado: {melhor_id:,}")
    return melhor_id

async def buscar_ultimo_id_async(session: aiohttp.ClientSession, alvo: datetime, min_id: int):
    print(f"\n🔍 BUSCA BINÁRIA: Último ID <= {alvo.strftime('%d/%m/%Y %H:%M')}")
    esq, dir_idx = min_id, 1000000
    melhor_id, melhor_data = None, None
    
    while esq <= dir_idx:
        meio = (esq + dir_idx) // 2
        data_abertura = await extrair_data_hora_abertura_rapido_async(session, meio)
        print(f"   ID {meio:,} -> {data_abertura.strftime('%d/%m/%Y %H:%M') if data_abertura else 'INEXISTENTE'}")
        
        if data_abertura is None:
            dir_idx = meio - 1
        elif data_abertura <= alvo:
            melhor_id, melhor_data = meio, data_abertura
            esq = meio + 1
        else:
            dir_idx = meio - 1
            
    print(f"✅ ID encontrado: {melhor_id:,}")
    return melhor_id

async def coletar_aula_async(session: aiohttp.ClientSession, aula_id: int, nomes_norm: set, semaphore: asyncio.Semaphore):
    url_vis = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
    headers = {'X-Requested-With': 'XMLHttpRequest', 'Referer': 'https://musical.congregacao.org.br/painel'}
    
    async with semaphore:
        try:
            async with session.get(url_vis, headers=headers, timeout=10) as resp:
                if resp.status != 200: return None
                html = await resp.text()
                
            soup = BeautifulSoup(html, 'lxml')
            
            nome_instrutor = ""
            for row in soup.find_all('tr'):
                st = row.find('strong')
                if st and 'Instrutor(a) que ministrou a aula' in st.get_text():
                    tds = row.find_all('td')
                    if len(tds) >= 2: nome_instrutor = tds[1].get_text(strip=True)
                    break
                    
            if not nome_instrutor or normalizar_nome(nome_instrutor) not in nomes_norm:
                return None
                
            # É Hortolândia! Agora faz as duas requisições extras SIMULTANEAMENTE.
            url_edit = f"https://musical.congregacao.org.br/aulas_abertas/editar/{aula_id}"
            
            # Pega dados base
            dados = {'id_aula': aula_id, 'instrutor': nome_instrutor, 'comum': '', 'hora_inicio': '', 'hora_termino': '', 'data_hora_abertura': '', 'tem_ata': 'Não', 'texto_ata': '', 'descricao': '', 'data_aula': ''}
            
            mh = soup.find('div', class_='modal-header')
            if mh and mh.find('span', class_='pull-right'): dados['data_aula'] = mh.find('span', class_='pull-right').get_text(strip=True).strip()
            
            for row in soup.find_all('tr'):
                st = row.find('strong')
                if not st: continue
                lbl = st.get_text(strip=True)
                tds = row.find_all('td')
                if len(tds) < 2: continue
                val = tds[1].get_text(strip=True)
                
                if 'Comum Congregação' in lbl: dados['comum'] = val.upper()
                elif 'Início' in lbl and 'Horário' not in lbl: dados['hora_inicio'] = val[:5]
                elif 'Término' in lbl: dados['hora_termino'] = val[:5]
                elif 'Data e Horário de abertura' in lbl: dados['data_hora_abertura'] = val
            
            td_desc = soup.find('td', class_='bg-blue-gradient')
            if td_desc: dados['descricao'] = re.sub(r'\s+', ' ', td_desc.get_text(strip=True)).strip()
            else:
                td_col = soup.find('td', {'colspan': '2'})
                if td_col: dados['descricao'] = td_col.get_text(strip=True)

            for tab in soup.find_all('table', class_='table'):
                th = tab.find('thead')
                if th and th.find('tr', class_='bg-green-gradient') and 'ATA DA AULA' in th.get_text():
                    dados['tem_ata'] = "Sim"
                    tb = tab.find('tbody')
                    if tb and tb.find('td'): dados['texto_ata'] = tb.find('td').get_text(strip=True)
                    break
                    
            try: dados['dia_semana'] = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'][datetime.strptime(dados['data_aula'], '%d/%m/%Y').weekday()]
            except: dados['dia_semana'] = ""

            # Extração paralela Edit/Frequencia
            dados['id_turma'] = ""
            async with session.get(url_edit, headers=headers, timeout=5) as r_edit:
                if r_edit.status == 200:
                    s_edit = BeautifulSoup(await r_edit.text(), 'lxml')
                    ti = s_edit.find('input', {'name': 'id_turma'})
                    if ti: dados['id_turma'] = ti.get('value', '').strip()

            dados.update({'total_alunos': 0, 'presentes': 0, 'lista_presentes': '', 'lista_ausentes': '', 'relacao_alunos_aula': {}})
            
            if dados['id_turma']:
                url_freq = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_frequencias/{aula_id}/{dados['id_turma']}"
                async with session.get(url_freq, headers=headers, timeout=10) as r_freq:
                    if r_freq.status == 200:
                        s_freq = BeautifulSoup(await r_freq.text(), 'lxml')
                        tb_f = s_freq.find('tbody')
                        if tb_f:
                            linhas = tb_f.find_all('tr')
                            dados['total_alunos'] = len(linhas)
                            p_ids, a_ids = [], []
                            for lin in linhas:
                                t_n = lin.find('td')
                                if not t_n: continue
                                nc = t_n.get_text(strip=True)
                                lk = lin.find('a', {'data-id-membro': True})
                                if lk:
                                    id_m = lk.get('data-id-membro', '').strip()
                                    if id_m:
                                        dados['relacao_alunos_aula'][id_m] = nc
                                        if lin.find('i', class_='fa-check'): p_ids.append(id_m)
                                        else: a_ids.append(id_m)
                            dados['lista_presentes'] = "; ".join(p_ids)
                            dados['lista_ausentes'] = "; ".join(a_ids)
                            dados['presentes'] = len(p_ids)
            return dados
        except: return None

async def executar_historico_aulas_async(session: aiohttp.ClientSession):
    t0 = time.time()
    print("\n" + "=" * 80)
    print("📚 MÓDULO 1: HISTÓRICO DE AULAS (TURBO ASYNC)")
    print("=" * 80)
    
    inst_dict, nomes_norm = await carregar_instrutores_hortolandia_async(session)
    if not inst_dict: return None, None
    
    dt_inicio = datetime(2024, 1, 1, 0, 0, 0)
    dt_fim = datetime.now()
    
    primeiro_id = await buscar_primeiro_id_async(session, dt_inicio)
    if not primeiro_id: return None, None
    
    ultimo_id = await buscar_ultimo_id_async(session, dt_fim, primeiro_id)
    if not ultimo_id: ultimo_id = primeiro_id + 50000

    print(f"\n🚀 Processando IDs {primeiro_id:,} até {ultimo_id:,}...")
    
    resultado = []
    dic_alunos = {}
    aulas_htl = 0
    aulas_ata = 0
    
    semaphore = asyncio.Semaphore(ASYNC_CONEXOES_MAX)
    total_ids = ultimo_id - primeiro_id + 1
    
    for i in range(primeiro_id, ultimo_id + 1, LOTE_CHUNK_SIZE):
        fim = min(i + LOTE_CHUNK_SIZE - 1, ultimo_id)
        tasks = [coletar_aula_async(session, a_id, nomes_norm, semaphore) for a_id in range(i, fim + 1)]
        resultados_lote = await asyncio.gather(*tasks)
        
        for d in resultados_lote:
            if d:
                dic_alunos.update(d['relacao_alunos_aula'])
                resultado.append([
                    d['id_aula'], d['id_turma'], d['descricao'], d['comum'], d['dia_semana'],
                    d['hora_inicio'], d['hora_termino'], d['data_aula'], d['data_hora_abertura'],
                    d['tem_ata'], d['texto_ata'], d['instrutor'], d['total_alunos'], d['presentes'],
                    d['lista_presentes'], d['lista_ausentes']
                ])
                aulas_htl += 1
                if d['tem_ata'] == "Sim": aulas_ata += 1
                
        print(f"  ⚡ Progresso: {fim - primeiro_id + 1:,}/{total_ids:,} | Hortolândia: {aulas_htl} | Atas: {aulas_ata}")

    tt = time.time() - t0
    print(f"\n✅ Coleta Módulo 1 finalizada: {aulas_htl:,} aulas HTL em {tt/60:.2f} min")
    
    relacao_lista = [[i, n] for i, n in dic_alunos.items() if i and n]
    body = {
        "tipo": "historico_aulas_hortolandia",
        "dados": resultado,
        "relacao_alunos": relacao_lista,
        "headers": ["ID_Aula", "ID_Turma", "Descrição", "Comum", "Dia_Semana", "Hora_Início", "Hora_Término", "Data_Aula", "Data_Hora_Abertura", "Tem_Ata", "Texto_Ata", "Instrutor", "Total_Alunos", "Presentes", "IDs_Presentes", "IDs_Ausentes"],
        "resumo": {"periodo_inicio": dt_inicio.strftime('%d/%m/%Y %H:%M:%S'), "periodo_fim": dt_fim.strftime('%d/%m/%Y %H:%M:%S'), "total_aulas": len(resultado), "aulas_processadas": total_ids, "aulas_com_ata": aulas_ata, "tempo_minutos": round(tt/60, 2)}
    }
    
    bf = f'backup_aulas_{time.strftime("%Y%m%d_%H%M%S")}.json'
    with open('/kaggle/working/' + bf if IS_KAGGLE else bf, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
        
    try:
        r_post = requests.post(URL_APPS_SCRIPT_AULAS, json=body, timeout=300)
        if r_post.status_code == 200 and r_post.json().get('status') == 'sucesso':
            print("✅ Planilha de Aulas Sincronizada!")
    except Exception as e: print(f"⚠️ Erro webhook Aulas: {e}")
    
    return resultado, inst_dict

# ==================== MÓDULO 2: TURMAS (ASYNC - CHECK & EXTRACT UNIFICADO) ====================

async def coletar_turma_async(session: aiohttp.ClientSession, turma_id: int, ids_htl: dict, semaphore: asyncio.Semaphore):
    """Bate na URL. Se existir form, já extrai e verifica se é de HTL."""
    url = f"https://musical.congregacao.org.br/turmas/editar/{turma_id}"
    async with semaphore:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200: return None
                html = await resp.text()
                if 'form id="turmas"' not in html: return None
                
                soup = BeautifulSoup(html, 'lxml')
                st = "".join(s.string for s in soup.find_all('script') if s.string)
                
                # Pega Responsaveis primeiro pra filtrar cedo
                r1_id, r2_id = '', ''
                m1 = re.search(r"const option = '<option value=\"(\d+)\"", st)
                m2 = re.search(r"const option2 = '<option value=\"(\d+)\"", st)
                if m1: r1_id = int(m1.group(1))
                if m2: r2_id = int(m2.group(1))
                
                # Filtro Hortolândia
                if r1_id not in ids_htl and r2_id not in ids_htl:
                    return None
                
                dados = {'id_turma': turma_id, 'curso': '', 'descricao': '', 'comum': '', 'dia_semana': '', 'data_inicio': '', 'data_encerramento': '', 'hora_inicio': '', 'hora_termino': '', 'responsavel_1_id': r1_id, 'responsavel_1_nome': '', 'responsavel_2_id': r2_id, 'responsavel_2_nome': '', 'destinado_ao': '', 'ativo': 'Não', 'cadastrado_em': '', 'cadastrado_por': '', 'atualizado_em': '', 'atualizado_por': ''}
                
                if m1: dados['responsavel_1_nome'] = re.search(r"const option = '<option value=\"\d+\" selected>(.*?)</option>'", st).group(1).split(' - ')[0].strip()
                if m2: dados['responsavel_2_nome'] = re.search(r"const option2 = '<option value=\"\d+\" selected>(.*?)</option>'", st).group(1).split(' - ')[0].strip()
                
                cs = soup.find('select', {'name': 'id_curso'})
                if cs and cs.find('option', selected=True): dados['curso'] = cs.find('option', selected=True).get_text(strip=True)
                
                di = soup.find('input', {'name': 'descricao'})
                if di: dados['descricao'] = di.get('value', '').strip()
                
                ig = soup.find('select', {'name': 'id_igreja'})
                if ig and ig.find('option', selected=True): dados['comum'] = ig.find('option', selected=True).get_text(strip=True).split('|')[0].strip()
                
                ds = soup.find('select', {'name': 'dia_semana'})
                if ds and ds.find('option', selected=True): dados['dia_semana'] = ds.find('option', selected=True).get_text(strip=True)
                
                for k, html_name in [('data_inicio','dt_inicio'), ('data_encerramento','dt_fim')]:
                    inp = soup.find('input', {'name': html_name})
                    if inp: dados[k] = inp.get('value', '').strip()
                
                for k, html_name in [('hora_inicio','hr_inicio'), ('hora_termino','hr_fim')]:
                    inp = soup.find('input', {'name': html_name})
                    if inp: dados[k] = inp.get('value', '').strip()[:5]
                
                gen = soup.find('select', {'name': 'id_turma_genero'})
                if gen and gen.find('option', selected=True): dados['destinado_ao'] = gen.find('option', selected=True).get_text(strip=True)
                
                chk = soup.find('input', {'name': 'status'})
                if chk and chk.has_attr('checked'): dados['ativo'] = 'Sim'
                
                h_div = soup.find('div', id='collapseOne')
                if h_div:
                    for p in h_div.find_all('p'):
                        txt = p.get_text(strip=True)
                        if 'Cadastrado em:' in txt:
                            pts = txt.split('por:')
                            if len(pts)>=2: dados['cadastrado_em'], dados['cadastrado_por'] = pts[0].replace('Cadastrado em:', '').strip(), pts[1].strip()
                        elif 'Atualizado em:' in txt:
                            pts = txt.split('por:')
                            if len(pts)>=2: dados['atualizado_em'], dados['atualizado_por'] = pts[0].replace('Atualizado em:', '').strip(), pts[1].strip()
                
                return dados
        except: return None

async def executar_turmas_async(session: aiohttp.ClientSession, ids_instrutores_htl: dict):
    t0 = time.time()
    ts = gerar_timestamp()
    print("\n" + "=" * 80)
    print("🎓 MÓDULO 2: TURMAS (VARREDURA + EXTRAÇÃO UNIFICADA TURBO)")
    print("=" * 80)
    
    id_min, id_max = 20000, 70000
    total_ids = id_max - id_min + 1
    resultado, ids_htl_encontradas = [], []
    semaphore = asyncio.Semaphore(ASYNC_CONEXOES_MAX)
    
    print(f"🚀 Varrendo IDs de {id_min:,} até {id_max:,}...")
    
    for i in range(id_min, id_max + 1, LOTE_CHUNK_SIZE):
        fim = min(i + LOTE_CHUNK_SIZE - 1, id_max)
        tasks = [coletar_turma_async(session, tid, ids_instrutores_htl, semaphore) for tid in range(i, fim + 1)]
        resultados_lote = await asyncio.gather(*tasks)
        
        for d in resultados_lote:
            if d:
                ids_htl_encontradas.append(d['id_turma'])
                resultado.append([
                    d['id_turma'], d['curso'], d['descricao'], d['comum'], d['dia_semana'], d['data_inicio'], d['data_encerramento'], d['hora_inicio'], d['hora_termino'], d['responsavel_1_id'], d['responsavel_1_nome'], d['responsavel_2_id'], d['responsavel_2_nome'], d['destinado_ao'], d['ativo'], d['cadastrado_em'], d['cadastrado_por'], d['atualizado_em'], d['atualizado_por'], 'Coletado', time.strftime('%d/%m/%Y %H:%M:%S')
                ])
        
        print(f"  ⚡ Progresso: {fim - id_min + 1:,}/{total_ids:,} | HTL Encontradas: {len(resultado)}")

    tt = time.time() - t0
    print(f"\n✅ Módulo 2 finalizado: {len(resultado)} turmas HTL em {tt/60:.2f} min")
    
    body = {
        "tipo": "dados_turmas", "timestamp": ts, "dados": resultado,
        "headers": ["ID_Turma", "Curso", "Descricao", "Comum", "Dia_Semana", "Data_Inicio", "Data_Encerramento", "Hora_Inicio", "Hora_Termino", "Responsavel_1_ID", "Responsavel_1_Nome", "Responsavel_2_ID", "Responsavel_2_Nome", "Destinado_ao", "Ativo", "Cadastrado_em", "Cadastrado_por", "Atualizado_em", "Atualizado_por", "Status_Coleta", "Data_Coleta"],
        "resumo": {"total_processadas": total_ids, "turmas_hortolandia": len(resultado), "tempo_minutos": round(tt/60, 2)}
    }
    
    bf = f"backup_turmas_{ts.replace(':', '-')}.json"
    with open('/kaggle/working/' + bf if IS_KAGGLE else bf, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
        
    try:
        r_post = requests.post(URL_APPS_SCRIPT_TURMAS, json=body, timeout=120)
        if r_post.status_code == 200 and r_post.json().get('status') == 'sucesso':
            print("✅ Planilha de Turmas Criada/Sincronizada!")
    except Exception as e: print(f"⚠️ Erro webhook Turmas: {e}")
    
    return ids_htl_encontradas

# ==================== MÓDULO 3: MATRICULADOS (ASYNC) ====================

async def extrair_alunos_turma_async(session: aiohttp.ClientSession, turma_id: int, semaphore: asyncio.Semaphore):
    url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
    headers = {'X-Requested-With': 'XMLHttpRequest', 'Referer': 'https://musical.congregacao.org.br/painel'}
    
    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=15) as resp:
                if resp.status != 200: return turma_id, []
                soup = BeautifulSoup(await resp.text(), 'lxml')
                tb = soup.find('tbody')
                if not tb: return turma_id, []
                
                alunos = []
                for row in tb.find_all('tr'):
                    tds = row.find_all('td')
                    if len(tds) >= 4:
                        if not tds[0].get_text(strip=True) or 'Nenhum registro' in tds[0].get_text(strip=True): continue
                        alunos.append({
                            'ID_Turma': turma_id, 'Nome': tds[0].get_text(strip=True),
                            'Comum': tds[1].get_text(strip=True), 'Instrumento': tds[2].get_text(strip=True),
                            'Status': tds[3].get_text(strip=True)
                        })
                return turma_id, alunos
        except: return turma_id, []

async def executar_matriculados_async(session: aiohttp.ClientSession, ids_turmas: list):
    t0 = time.time()
    print("\n" + "=" * 80)
    print("👥 MÓDULO 3: ALUNOS MATRICULADOS (TURBO ASYNC)")
    print("=" * 80)
    
    if not ids_turmas: return
    print(f"🎯 Extraindo matrículas de {len(ids_turmas)} turmas de Hortolândia...")
    
    semaphore = asyncio.Semaphore(ASYNC_CONEXOES_MAX)
    tasks = [extrair_alunos_turma_async(session, tid, semaphore) for tid in ids_turmas]
    resultados = await asyncio.gather(*tasks)
    
    todos_alunos, resultados_resumo = [], []
    for tid, alunos in resultados:
        if alunos:
            todos_alunos.extend(alunos)
            resultados_resumo.append([tid, len(alunos), "Sucesso"])
        else:
            resultados_resumo.append([tid, 0, "Sem Alunos/Erro"])
            
    print(f"✅ Módulo 3 finalizado: {len(todos_alunos)} alunos coletados em {(time.time()-t0)/60:.2f} min")
    
    ts = datetime.now().strftime('%d_%m_%Y-%H_%M')
    data_coleta = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    resumo_envio = [["ID_Turma", "Quantidade_Matriculados", "Status_Coleta"]] + resultados_resumo
    alunos_envio = [["ID_Turma", "Nome", "Comum", "Instrumento", "Status"]] + [[a['ID_Turma'], a['Nome'], a['Comum'], a['Instrumento'], a['Status']] for a in todos_alunos]
    
    bf = f'backup_matriculas_{ts}.json'
    with open('/kaggle/working/' + bf if IS_KAGGLE else bf, 'w', encoding='utf-8') as f:
        json.dump({"resumo": resultados_resumo, "alunos": todos_alunos}, f, indent=2, ensure_ascii=False)
        
    try:
        r1 = requests.post(URL_APPS_SCRIPT_MATRICULAS, json={"tipo": "contagem_matriculas", "dados": resumo_envio, "data_coleta": data_coleta}, timeout=60)
        if r1.status_code == 200 and r1.json().get('status') == 'sucesso':
            pid = r1.json().get('detalhes', {}).get('planilha_id')
            print("📋 Enviando dados detalhados...")
            requests.post(URL_APPS_SCRIPT_MATRICULAS, json={"tipo": "alunos_detalhados", "dados": alunos_envio, "data_coleta": data_coleta, "planilha_id": pid}, timeout=60)
            print("✅ Planilha de Matrículas Sincronizada!")
    except Exception as e: print(f"⚠️ Erro webhook Matrículas: {e}")

# ==================== ORQUESTRADOR CENTRAL ASYNC ====================

async def orquestrador(cookies_dict, user_agent):
    headers = {
        'User-Agent': user_agent,
        'Cookie': "; ".join([f"{k}={v}" for k, v in cookies_dict.items()]),
        'Connection': 'keep-alive'
    }
    
    connector = aiohttp.TCPConnector(limit=ASYNC_CONEXOES_MAX, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=20)
    
    async with aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout) as session:
        # 1. Aulas
        _, inst_dict = await executar_historico_aulas_async(session)
        if not inst_dict: return
        
        # 2. Turmas
        ids_turmas_htl = await executar_turmas_async(session, inst_dict)
        if not ids_turmas_htl: return
        
        # 3. Matriculados
        await executar_matriculados_async(session, ids_turmas_htl)

def main():
    t0 = time.time()
    print("🚀 INICIANDO SISTEMA TURBO ASYNC ".center(80, "="))
    
    cookies, user_agent = fazer_login_unico()
    if not cookies: return
    
    asyncio.run(orquestrador(cookies, user_agent))
    
    print("\n" + "=" * 80)
    print(f"🎉 PROCESSO COMPLETO FINALIZADO em {(time.time()-t0)/60:.2f} min!")
    print("=" * 80)

if __name__ == "__main__":
    main()

from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

import os, json, asyncio, aiohttp, re, unicodedata, time
from contextlib import asynccontextmanager
from bs4 import BeautifulSoup
from lxml import html as lxh
from datetime import datetime

LOTE_ATUAL  = int(os.environ.get('LOTE_ATUAL',  1))
TOTAL_LOTES = int(os.environ.get('TOTAL_LOTES', 1))

HIST_WORKERS     = 30
HIST_QUEUE_SIZE  = 300
MAT_WORKERS      = 15
MAT_QUEUE_SIZE   = 80
SCAN_CHUNK       = 500

AIMD_START    = 30
AIMD_FLOOR    = 15
AIMD_CEILING  = 100
AIMD_AI_AFTER = 5
AIMD_MD_FATOR = 0.8

TIMEOUT_PADRAO = aiohttp.ClientTimeout(total=4, connect=2)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [W{LOTE_ATUAL:02d}/{TOTAL_LOTES}] {msg}", flush=True)

def norm(n: str) -> str:
    n = unicodedata.normalize('NFD', n)
    n = ''.join(c for c in n if unicodedata.category(c) != 'Mn')
    return ' '.join(n.replace('/', ' ').replace('\\', ' ').replace('-', ' ').upper().split())

def fmt_dt(ds: str) -> str:
    if not ds: return ''
    for fmt in ('%d/%m/%Y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y'):
        try: return datetime.strptime(ds.strip(), fmt).strftime('%d/%m/%Y')
        except: continue
    return ds

def validar_corrigir_data(ds: str) -> str:
    ds = fmt_dt(ds)
    if '/' not in ds: return ds
    try:
        p = ds.split('/')
        d, m, a = int(p[0]), int(p[1]), int(p[2])
        if d > 31 or m > 12:
            if m <= 31 and d <= 12:
                return f"{m:02d}/{d:02d}/{a}"
        datetime(a, m, d)
    except: pass
    return ds

def fatia(config: dict, chave: str) -> tuple[int, int]:
    ini = config[chave]['inicio']
    fim = config[chave]['fim']
    total = fim - ini + 1
    tam = total // TOTAL_LOTES
    mi = ini + (LOTE_ATUAL - 1) * tam
    mf = mi + tam - 1 if LOTE_ATUAL < TOTAL_LOTES else fim
    return mi, mf

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

class AIRateLimiter:
    def __init__(self, start=AIMD_START, floor=AIMD_FLOOR, ceiling=AIMD_CEILING, ai_after=AIMD_AI_AFTER, md=AIMD_MD_FATOR):
        self._limit   = start
        self._floor   = floor
        self._ceiling = ceiling
        self._ai_after = ai_after
        self._md      = md
        self._active  = 0
        self._streak  = 0
        self._cond    = asyncio.Condition()

    @property
    def limit(self): return self._limit

    async def acquire(self):
        async with self._cond:
            while self._active >= self._limit:
                await self._cond.wait()
            self._active += 1

    async def release(self, ok: bool):
        async with self._cond:
            self._active -= 1
            if ok:
                self._streak += 1
                if self._streak >= self._ai_after and self._limit < self._ceiling:
                    self._limit += 1
                    self._streak = 0
                    self._cond.notify()
            else:
                self._limit = max(int(self._limit * self._md), self._floor)
                self._streak = 0
            self._cond.notify()

    @asynccontextmanager
    async def slot(self, *, is_retry: bool = False):
        await self.acquire()
        ok = True
        try:
            yield
        except (asyncio.TimeoutError, aiohttp.ServerConnectionError, aiohttp.ServerDisconnectedError, aiohttp.ClientConnectionError):
            ok = False
            raise
        finally:
            await self.release(ok)

async def get_html(session: aiohttp.ClientSession, url: str, limiter: AIRateLimiter, extra_headers: dict | None = None, max_retries: int = 4) -> str | None:
    headers = extra_headers or {}
    for attempt in range(max_retries):
        try:
            async with limiter.slot():
                async with session.get(url, headers=headers, timeout=TIMEOUT_PADRAO, allow_redirects=False) as r:
                    if r.status == 200:
                        return await r.text()
                    if r.status in (301, 302, 303, 307, 308):
                        return None
                    if r.status == 404:
                        return None
                    if r.status in (429, 500, 502, 503, 504):
                        pass
                    else:
                        return None
        except (asyncio.TimeoutError, aiohttp.ServerConnectionError, aiohttp.ServerDisconnectedError, aiohttp.ClientConnectionError):
            pass
        except aiohttp.TooManyRedirects:
            return None
        except Exception:
            return None

        if attempt < max_retries - 1:
            await asyncio.sleep(min(1.5 ** attempt, 6))

    return None

def _extrair_aluno_lxml(html_str: str, aid: int, ids_ig: set) -> dict | None:
    if 'igreja_selecionada' not in html_str:
        return None
    m = re.search(r'igreja_selecionada\s*\((\d+)\)', html_str)
    if not m:
        return None
    igi = int(m.group(1))
    if igi not in ids_ig:
        return None

    tree = lxh.fromstring(html_str.encode('utf-8', errors='replace'))

    def sel_val(name: str) -> tuple[str, str]:
        opts = tree.xpath(f'//select[@id="id_{name}"]/option[@selected]')
        if opts:
            return opts[0].get('value', ''), (opts[0].text_content() or '').strip()
        return '', ''

    def inp_val(name: str) -> str:
        vals = tree.xpath(f'//input[@name="{name}"]/@value')
        return (vals[0] if vals else '').strip()

    d = {
        'id_aluno':  aid,
        'id_igreja': igi,
        'nome':      inp_val('nome'),
        'fl_tipo':   inp_val('fl_tipo'),
        'status':    inp_val('status'),
    }
    for campo in ('cargo', 'nivel', 'instrumento', 'tonalidade'):
        d[f'id_{campo}'], d[f'{campo}_nome'] = sel_val(campo)

    d['data_cadastro'] = d['cadastrado_por'] = ''
    d['data_atualizacao'] = d['atualizado_por'] = ''

    collapse = tree.xpath('//div[@id="collapseOne"]')
    if collapse:
        for p in collapse[0].xpath('.//p'):
            txt = p.text_content()
            if 'Cadastrado em:' in txt:
                mm = re.search(r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})', txt)
                if mm: d['data_cadastro'] = mm.group(1)
                mm = re.search(r'por:\s*(.+)$', txt, re.M)
                if mm: d['cadastrado_por'] = mm.group(1).strip()
            elif 'Atualizado em:' in txt:
                mm = re.search(r'(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})', txt)
                if mm: d['data_atualizacao'] = mm.group(1)
                mm = re.search(r'por:\s*(.+)$', txt, re.M)
                if mm: d['atualizado_por'] = mm.group(1).strip()
    return d

async def _scan_aluno_id(session: aiohttp.ClientSession, aid: int, ids_ig: set, aluno_queue: asyncio.Queue, out_alunos: list, limiter: AIRateLimiter, st: dict):
    html = await get_html(session, f"https://musical.congregacao.org.br/grp_musical/editar/{aid}", limiter)
    if html:
        aluno = _extrair_aluno_lxml(html, aid, ids_ig)
        if aluno:
            out_alunos.append(aluno)
            await aluno_queue.put(aluno)

    st['p'] += 1
    if st['p'] % 500 == 0:
        pct = st['p'] / st['t'] * 100
        elapsed = time.time() - st['t0']
        rps = st['p'] / elapsed if elapsed else 0
        log(f"Alunos: {st['p']:,}/{st['t']:,} ({pct:.1f}%) | {rps:.0f} IDs/s | achados: {len(out_alunos)} | AIMD: {limiter.limit}")

def _extrair_historico_bs4(html: str, aid: int, nome: str) -> dict:
    dados = {k: [] for k in ['mts_individual', 'mts_grupo', 'msa_individual', 'msa_grupo', 'provas', 'hinario_individual', 'hinario_grupo', 'metodos', 'escalas_individual', 'escalas_grupo']}
    soup = BeautifulSoup(html, 'html.parser')

    def linhas_tbody(div, tbl_idx):
        if not div: return []
        tbls = div.find_all('table', class_='table')
        if tbl_idx >= len(tbls): return []
        tb = tbls[tbl_idx].find('tbody')
        return tb.find_all('tr') if tb else []

    def cols(li, n): return [c.text.strip() for c in li.find_all('td')[:n]]

    div = soup.find('div', {'id': 'mts'})
    for li in linhas_tbody(div, 0):
        c = cols(li, 7)
        if len(c) >= 7:
            c[4] = validar_corrigir_data(c[4]); c[6] = validar_corrigir_data(c[6])
            dados['mts_individual'].append([aid, nome] + c)
    for li in linhas_tbody(div, 1):
        c = cols(li, 3)
        if len(c) >= 3:
            c[2] = validar_corrigir_data(c[2])
            dados['mts_grupo'].append([aid, nome] + c)

    div = soup.find('div', {'id': 'msa'})
    for li in linhas_tbody(div, 0):
        c = cols(li, 7)
        if len(c) >= 7:
            c[0] = validar_corrigir_data(c[0])
            dados['msa_individual'].append([aid, nome] + c)
    for li in linhas_tbody(div, 1):
        tds = li.find_all('td')
        if len(tds) < 3: continue
        h0 = tds[0].decode_contents()
        def rx(pattern):
            mm = re.search(pattern, h0); return (mm.group(1), mm.group(2)) if mm else ('', '')
        fd, fa = rx(r'<b>Fase\(s\):</b>\s*de\s+([\d.]+)\s+até\s+([\d.]+)')
        pd, pa = rx(r'<b>Página\(s\):</b>\s*de\s+(\d+)\s+até\s+(\d+)')
        cl_m = re.search(r'<b>Clave\(s\):</b>\s*([^<\n]+)', h0)
        dados['msa_grupo'].append([aid, nome, fd, fa, pd, pa, cl_m.group(1).strip() if cl_m else '', tds[1].text.strip(), validar_corrigir_data(tds[2].text.strip())])

    div = soup.find('div', {'id': 'provas'})
    for li in linhas_tbody(div, 0):
        c = cols(li, 5)
        if len(c) >= 5:
            c[2] = validar_corrigir_data(c[2]); c[4] = validar_corrigir_data(c[4])
            dados['provas'].append([aid, nome] + c)

    div = soup.find('div', {'id': 'hinario'})
    for li in linhas_tbody(div, 0):
        c = cols(li, 7)
        if len(c) >= 7:
            c[2] = validar_corrigir_data(c[2])
            if len(c) > 4: c[4] = validar_corrigir_data(c[4])
            if len(c) > 5: c[5] = validar_corrigir_data(c[5])
            dados['hinario_individual'].append([aid, nome] + c)
    for li in linhas_tbody(div, 1):
        c = cols(li, 3)
        if len(c) >= 3:
            c[2] = validar_corrigir_data(c[2])
            dados['hinario_grupo'].append([aid, nome] + c)

    div = soup.find('div', {'id': 'metodos'})
    for li in linhas_tbody(div, 0):
        c = cols(li, 7)
        if len(c) >= 7:
            c[3] = validar_corrigir_data(c[3])
            if len(c) > 5: c[5] = validar_corrigir_data(c[5])
            dados['metodos'].append([aid, nome] + c)

    div = soup.find('div', {'id': 'escalas'})
    for li in linhas_tbody(div, 0):
        c = cols(li, 6)
        if len(c) >= 6:
            c[1] = validar_corrigir_data(c[1])
            if len(c) > 3: c[3] = validar_corrigir_data(c[3])
            if len(c) > 4: c[4] = validar_corrigir_data(c[4])
            dados['escalas_individual'].append([aid, nome] + c)
    for li in linhas_tbody(div, 1):
        c = cols(li, 3)
        if len(c) >= 3:
            c[2] = validar_corrigir_data(c[2])
            dados['escalas_grupo'].append([aid, nome] + c)

    return dados

async def _worker_historico(session: aiohttp.ClientSession, aluno_queue: asyncio.Queue, out_hist: dict, limiter: AIRateLimiter, st: dict, worker_id: int):
    while True:
        aluno = await aluno_queue.get()
        if aluno is None:
            aluno_queue.task_done()
            await aluno_queue.put(None)
            return

        aid, nome = aluno['id_aluno'], aluno['nome']
        html = await get_html(session, f"https://musical.congregacao.org.br/licoes/index/{aid}", limiter)
        if html and 'id="mts"' in html:
            d = _extrair_historico_bs4(html, aid, nome)
            for k in out_hist:
                out_hist[k].extend(d[k])

        aluno_queue.task_done()
        st['p'] += 1
        if st['p'] % 10 == 0:
            log(f"Histórico: {st['p']:,}/{st['t']:,} | AIMD: {limiter.limit}")

async def run_pipeline_alunos(session: aiohttp.ClientSession, ids: list[int], ids_ig: set, out_alunos: list, out_hist: dict, limiter: AIRateLimiter):
    total = len(ids)
    log(f"🔎 [PIPELINE-1] Alunos: {total:,} IDs | AIMD start={AIMD_START} | hist_workers={HIST_WORKERS} | chunk={SCAN_CHUNK}")

    aluno_queue: asyncio.Queue[dict | None] = asyncio.Queue(maxsize=HIST_QUEUE_SIZE)
    st_scan = {'p': 0, 't': total, 't0': time.time()}
    st_hist = {'p': 0, 't': 0}

    async def producer():
        for batch in chunks(ids, SCAN_CHUNK):
            await asyncio.gather(*[_scan_aluno_id(session, aid, ids_ig, aluno_queue, out_alunos, limiter, st_scan) for aid in batch])
        await aluno_queue.put(None)
        log(f"✅ Scan de alunos concluído: {len(out_alunos)} encontrados")

    async def launch_consumers():
        await asyncio.gather(*[_worker_historico(session, aluno_queue, out_hist, limiter, st_hist, i) for i in range(HIST_WORKERS)])
        log("✅ Histórico pipeline concluído")

    async def wait_and_update():
        await producer()
        st_hist['t'] = len(out_alunos)

    await asyncio.gather(wait_and_update(), launch_consumers())

async def _coletar_aula(session: aiohttp.ClientSession, auid: int, inst_set: set, out_au: list, relacao: dict, limiter: AIRateLimiter, st: dict):
    html = await get_html(session, f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{auid}", limiter, {'X-Requested-With': 'XMLHttpRequest'})
    if not html or len(html) < 500:
        st['p'] += 1; return

    soup = BeautifulSoup(html, 'html.parser')
    ni = ""
    for row in soup.find_all('tr'):
        s = row.find('strong')
        if s and 'Instrutor(a)' in s.text:
            tds = row.find_all('td')
            if len(tds) > 1: ni = tds[1].text.strip()
    if not ni or norm(ni) not in inst_set:
        st['p'] += 1; return

    d = {
        'id_aula': auid, 'instrutor': ni, 'comum': '', 'data_aula': '', 'id_turma': '', 'descricao': '', 'dia_semana': '', 'hora_inicio': '', 'hora_termino': '', 'data_hora_abertura': '', 'tem_ata': 'Não', 'texto_ata': '', 'total_alunos': 0, 'presentes': 0, 'lista_presentes': '', 'lista_ausentes': '',
    }
    mh = soup.find('div', class_='modal-header')
    if mh:
        sp = mh.find('span', class_='pull-right')
        if sp: d['data_aula'] = sp.text.strip()

    for row in soup.find_all('tr'):
        s = row.find('strong')
        if not s: continue
        tds = row.find_all('td')
        if len(tds) < 2: continue
        lbl, val = s.text, tds[1].text.strip()
        if 'Comum'    in lbl: d['comum'] = val.upper()
        elif 'Início' in lbl and 'Horário' not in lbl: d['hora_inicio'] = val[:5]
        elif 'Término' in lbl: d['hora_termino'] = val[:5]
        elif 'abertura' in lbl: d['data_hora_abertura'] = val

    tbl = soup.find('table', class_='table')
    if tbl and tbl.find('thead') and tbl.find('thead').find('td'):
        d['descricao'] = re.sub(r'\s+', ' ', tbl.find('thead').find('td').text).strip()

    for t in soup.find_all('table', class_='table'):
        if t.find('thead') and 'ATA' in t.find('thead').text:
            d['tem_ata'] = 'Sim'
            if t.find('tbody') and t.find('tbody').find('td'):
                d['texto_ata'] = t.find('tbody').find('td').text.strip()

    try:
        d['dia_semana'] = ['Segunda','Terça','Quarta','Quinta','Sexta','Sábado','Domingo'][datetime.strptime(d['data_aula'], '%d/%m/%Y').weekday()]
    except: pass

    html_ed = await get_html(session, f"https://musical.congregacao.org.br/aulas_abertas/editar/{auid}", limiter)
    if html_ed:
        tree_ed = lxh.fromstring(html_ed.encode('utf-8', errors='replace'))
        vals = tree_ed.xpath('//input[@name="id_turma"]/@value')
        if vals: d['id_turma'] = vals[0].strip()

    if d['id_turma']:
        html_f = await get_html(session, f"https://musical.congregacao.org.br/aulas_abertas/visualizar_frequencias/{auid}/{d['id_turma']}", limiter)
        if html_f:
            s3 = BeautifulSoup(html_f, 'html.parser')
            if tb := s3.find('tbody'):
                linhas = tb.find_all('tr')
                d['total_alunos'] = len(linhas)
                lp, la = [], []
                for lin in linhas:
                    lk = lin.find('a', {'data-id-membro': True})
                    if not lk: continue
                    mid = lk.get('data-id-membro', '').strip()
                    if not mid: continue
                    nome_al = lin.find('td')
                    relacao[mid] = nome_al.text.strip() if nome_al else ''
                    if lin.find('i', class_='fa-check'): lp.append(mid)
                    else: la.append(mid)
                d['lista_presentes'] = "; ".join(lp)
                d['lista_ausentes']  = "; ".join(la)
                d['presentes'] = len(lp)

    out_au.append(d)
    st['p'] += 1
    if st['p'] % 100 == 0:
        log(f"Aulas: {st['p']:,}/{st['t']:,} | HTL: {len(out_au)} | AIMD: {limiter.limit}")

async def run_scan_aulas(session: aiohttp.ClientSession, ids: list[int], inst_set: set, out_aulas: list, relacao_aul: dict, limiter: AIRateLimiter):
    total = len(ids)
    log(f"🏫 [SCAN-AULAS] {total:,} IDs")
    st = {'p': 0, 't': total}
    for batch in chunks(ids, SCAN_CHUNK):
        await asyncio.gather(*[_coletar_aula(session, i, inst_set, out_aulas, relacao_aul, limiter, st) for i in batch])
    log(f"✅ Aulas: {len(out_aulas)} HTL")

async def _scan_turma_id(session: aiohttp.ClientSession, tid: int, inst_set: set, out_tu: list, turma_queue: asyncio.Queue, limiter: AIRateLimiter, st: dict):
    html = await get_html(session, f"https://musical.congregacao.org.br/turmas/editar/{tid}", limiter)
    if not html or 'id_curso' not in html:
        st['p'] += 1; return

    soup = BeautifulSoup(html, 'html.parser')
    if not soup.find('form', id='turmas'):
        st['p'] += 1; return

    d = {
        'id_turma': tid, 'ativo': 'Não', 'cadastrado_em': '', 'cadastrado_por': '', 'atualizado_em': '', 'atualizado_por': '', 'responsavel_1_id': '', 'responsavel_1_nome': '', 'responsavel_2_id': '', 'responsavel_2_nome': '',
    }
    for k, n in [('curso','id_curso'),('comum','id_igreja'),('dia_semana','dia_semana'),('destinado_ao','id_turma_genero')]:
        sel = soup.find('select', {'name': n})
        d[k] = sel.find('option', selected=True).text.strip().split('|')[0].strip() if sel and sel.find('option', selected=True) else ''
    for k, n in [('descricao','descricao'),('data_inicio','dt_inicio'),('data_encerramento','dt_fim'),('hora_inicio','hr_inicio'),('hora_termino','hr_fim')]:
        inp = soup.find('input', {'name': n})
        v = inp.get('value', '').strip() if inp else ''
        d[k] = v[:5] if 'hora' in k else v
    for scr in soup.find_all('script'):
        if scr.string and 'id_responsavel' in scr.string:
            if mm := re.search(r"option = '<option value=\"(\d+)\" selected>(.*?)</option>'", scr.string):
                d['responsavel_1_id'] = mm.group(1)
                d['responsavel_1_nome'] = mm.group(2).strip().split(' - ')[0].strip()
            if mm := re.search(r"option2 = '<option value=\"(\d+)\" selected>(.*?)</option>'", scr.string):
                d['responsavel_2_id'] = mm.group(1)
                d['responsavel_2_nome'] = mm.group(2).strip().split(' - ')[0].strip()
    if soup.find('input', {'name': 'status', 'checked': True}):
        d['ativo'] = 'Sim'
    if hd := soup.find('div', id='collapseOne'):
        for p in hd.find_all('p'):
            txt = p.text
            if 'Cadastrado' in txt and 'por:' in txt:
                partes = txt.split('por:')
                d['cadastrado_em'] = partes[0].replace('Cadastrado em:', '').strip()
                d['cadastrado_por'] = partes[1].strip()
            elif 'Atualizado' in txt and 'por:' in txt:
                partes = txt.split('por:')
                d['atualizado_em'] = partes[0].replace('Atualizado em:', '').strip()
                d['atualizado_por'] = partes[1].strip()

    n1 = norm(d['responsavel_1_nome']) if d['responsavel_1_nome'] else ''
    n2 = norm(d['responsavel_2_nome']) if d['responsavel_2_nome'] else ''
    if n1 in inst_set or n2 in inst_set:
        out_tu.append(d)
        await turma_queue.put(tid)

    st['p'] += 1
    if st['p'] % 100 == 0:
        log(f"Turmas: {st['p']:,}/{st['t']:,} | HTL: {len(out_tu)} | AIMD: {limiter.limit}")

async def _worker_matriculas(session: aiohttp.ClientSession, turma_queue: asyncio.Queue, out_mat: list, limiter: AIRateLimiter):
    while True:
        tid = await turma_queue.get()
        if tid is None:
            turma_queue.task_done()
            await turma_queue.put(None)
            return

        html = await get_html(session, f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{tid}", limiter, {'X-Requested-With': 'XMLHttpRequest'})
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            if tb := soup.find('tbody'):
                for rw in tb.find_all('tr'):
                    tds = rw.find_all('td')
                    if len(tds) >= 4 and 'Nenhum' not in tds[0].text:
                        out_mat.append({
                            'ID_Turma':    tid,
                            'Nome':        tds[0].text.strip(),
                            'Comum':       tds[1].text.strip(),
                            'Instrumento': tds[2].text.strip(),
                            'Status':      tds[3].text.strip(),
                        })
        turma_queue.task_done()

async def run_pipeline_turmas(session: aiohttp.ClientSession, ids: list[int], inst_set: set, out_turmas: list, out_mat: list, limiter: AIRateLimiter):
    total = len(ids)
    log(f"👥 [PIPELINE-2] Turmas: {total:,} IDs | mat_workers={MAT_WORKERS}")
    st = {'p': 0, 't': total}
    turma_queue: asyncio.Queue[int | None] = asyncio.Queue(maxsize=MAT_QUEUE_SIZE)

    async def producer():
        for batch in chunks(ids, SCAN_CHUNK):
            await asyncio.gather(*[_scan_turma_id(session, tid, inst_set, out_turmas, turma_queue, limiter, st) for tid in batch])
        await turma_queue.put(None)
        log(f"✅ Scan de turmas concluído: {len(out_turmas)} HTL")

    async def consumers():
        await asyncio.gather(*[_worker_matriculas(session, turma_queue, out_mat, limiter) for _ in range(MAT_WORKERS)])
        log(f"✅ Matrículas pipeline: {len(out_mat)} registros")

    await asyncio.gather(producer(), consumers())

async def main():
    t0 = time.time()
    log("🏁 Worker v2 iniciado — lendo contexto do Líder...")

    for arq in ('config_ranges.json', 'session_cookies.json', 'seq1_localidades.json', 'seq2_instrutores.json'):
        if not os.path.exists(arq):
            log(f"❌ Arquivo obrigatório não encontrado: {arq}"); return

    with open('config_ranges.json',    'r') as f: cfg   = json.load(f)
    with open('session_cookies.json',  'r') as f: ck    = json.load(f)
    with open('seq1_localidades.json', 'r', encoding='utf-8') as f: locs  = json.load(f)
    with open('seq2_instrutores.json', 'r', encoding='utf-8') as f: insts = set(json.load(f))

    ids_ig = set(l['id_igreja'] for l in locs)
    if not ids_ig:
        log("❌ ERRO FATAL: lista de localidades vazia — abortando."); return

    log(f"Contexto OK: {len(ids_ig)} igrejas | {len(insts)} instrutores")

    ia, fa = fatia(cfg, 'alunos')
    iau, fau = fatia(cfg, 'aulas')
    it, ft   = fatia(cfg, 'turmas')

    ids_al = list(range(ia,  fa  + 1))
    ids_au = list(range(iau, fau + 1))
    ids_tu = list(range(it,  ft  + 1))

    log(f"Fatias — Alunos: {ia:,}→{fa:,} ({len(ids_al):,}) | Aulas: {iau:,}→{fau:,} ({len(ids_au):,}) | Turmas: {it:,}→{ft:,} ({len(ids_tu):,})")

    out_alunos  = []
    out_hist    = {k: [] for k in ['mts_individual','mts_grupo','msa_individual','msa_grupo','provas','hinario_individual','hinario_grupo','metodos','escalas_individual','escalas_grupo']}
    out_aulas   = []
    relacao_aul = {}
    out_turmas  = []
    out_mat     = []

    limiter = AIRateLimiter()
    log(f"⚙️  AIMD: start={AIMD_START} | floor={AIMD_FLOOR} | ceiling={AIMD_CEILING} | ai_after={AIMD_AI_AFTER}")

    from yarl import URL as YarlURL
    jar = aiohttp.CookieJar(unsafe=True)
    jar.update_cookies(ck, YarlURL("https://musical.congregacao.org.br"))

    conn = aiohttp.TCPConnector(
        limit=AIMD_CEILING + 20,
        limit_per_host=AIMD_CEILING + 20,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    base_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }

    async def auto_save():
        while True:
            await asyncio.sleep(300)
            log("💾 [AUTO-SAVE] Gravando backup de segurança no disco...")
            backup = {
                "seq1": {"alunos": out_alunos, "historicos": out_hist},
                "seq2": {"aulas": out_aulas, "turmas": out_turmas, "matriculas": out_mat}
            }
            try:
                with open('backup_emergencia.json', 'w', encoding='utf-8') as f:
                    json.dump(backup, f, ensure_ascii=False)
            except Exception:
                pass

    async with aiohttp.ClientSession(connector=conn, headers=base_headers, cookie_jar=jar) as sess:
        log("🚀 Lançando SEQ1 ∥ SEQ2 em paralelo total...")

        tarefa_backup = asyncio.create_task(auto_save())

        await asyncio.gather(
            run_pipeline_alunos(sess, ids_al, ids_ig, out_alunos, out_hist, limiter),
            run_scan_aulas(sess, ids_au, insts, out_aulas, relacao_aul, limiter),
            run_pipeline_turmas(sess, ids_tu, insts, out_turmas, out_mat, limiter),
        )

        tarefa_backup.cancel()

    log(f"AIMD final: limit={limiter.limit} (começou em {AIMD_START})")

    resultado = {
        "lote": LOTE_ATUAL,
        "seq1": {
            "alunos":    out_alunos,
            "historicos": out_hist,
        },
        "seq2": {
            "aulas":         out_aulas,
            "relacao_aulas": relacao_aul,
            "turmas":        out_turmas,
            "matriculas":    out_mat,
        },
    }
    arq_saida = f"output_lote_{LOTE_ATUAL}.json"
    with open(arq_saida, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False)

    t_min = (time.time() - t0) / 60
    log(f"✅ LOTE {LOTE_ATUAL} FINALIZADO em {t_min:.1f} min → {arq_saida}")
    log(f"   Alunos: {len(out_alunos)} | Aulas: {len(out_aulas)} | Turmas: {len(out_turmas)} | Matrículas: {len(out_mat)}")

if __name__ == "__main__":
    asyncio.run(main())

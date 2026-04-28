# 03_consolidador.py
# Responsabilidade: mesclar output_lote_*.json e enviar tudo ao Google Sheets
# Mantém os payloads exatos que os Apps Scripts originais esperam
# Tempo estimado: ~2-5 min (envios paralelos via ThreadPoolExecutor)

from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

import os, json, requests, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── URLs dos Apps Scripts (originais) ────────────────────────────────────────
URLS = {
    "LOC": 'https://script.google.com/macros/s/AKfycbw7OMC2bGrMLDPcMBXAgS569UM6n1uIyYMSLN6r-d908qn_dBXskwdAIOiZY4MfsYXe/exec',
    "ALU": 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec',
    "HIS": 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec',
    "AUL": 'https://script.google.com/macros/s/AKfycbyvEGIUPIvgbSuT_yikqg03nEjqXryd6RfI121A3pRt75v9oJoFNLTdvo3-onNdEsJd/exec',
    "TUR": 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec',
    "MAT": 'https://script.google.com/macros/s/AKfycbxnp24RMIG4zQEsot0KATnFjdeoEHP7nyrr4WXnp-LLLptQTT-Vc_UPYoy__VWipill/exec',
}

LOTE_TAMANHO_HIS = 200   # alunos por lote no envio de histórico
TIMEOUT_POST     = None  # sem limite de tempo

# ── helpers ───────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 📤 {msg}", flush=True)

def post(url_key: str, payload: dict, descricao: str = "") -> dict | None:
    """POST com retry (3x). Retorna JSON ou None."""
    url = URLS[url_key]
    for t in range(3):
        try:
            r = requests.post(url, json=payload, timeout=TIMEOUT_POST)
            if r.status_code == 200:
                try:
                    resp = r.json()
                    # Alguns Apps Scripts encapsulam em {'body': '...'}
                    if 'body' in resp and isinstance(resp['body'], str):
                        resp = json.loads(resp['body'])
                    log(f"  ✅ {descricao} → {resp.get('status','?')}")
                    return resp
                except Exception:
                    log(f"  ✅ {descricao} → HTTP 200 (sem JSON)")
                    return {}
            else:
                log(f"  ⚠️  {descricao} → HTTP {r.status_code} (t={t+1}/3)")
        except Exception as e:
            log(f"  ❌ {descricao} → {e} (t={t+1}/3)")
        time.sleep(3 * (t + 1))
    return None

def gerar_resumo_alunos(alunos: list, historicos: dict) -> list:
    """Gera 14 colunas de resumo por aluno (compatível com seq_01 original)."""
    resumo = []
    for a in alunos:
        aid = a['id_aluno']
        def cnt(k): return sum(1 for x in historicos[k] if x[0] == aid)
        provas_a = [x for x in historicos['provas'] if x[0] == aid]
        notas = []
        for p in provas_a:
            if len(p) > 3:
                try: notas.append(float(str(p[3]).replace(',','.')))
                except: pass
        media = round(sum(notas)/len(notas), 2) if notas else 0
        resumo.append([
            aid, a['nome'], a['id_igreja'],
            cnt('mts_individual'), cnt('mts_grupo'),
            cnt('msa_individual'), cnt('msa_grupo'),
            cnt('provas'), media,
            cnt('hinario_individual'), cnt('hinario_grupo'),
            cnt('metodos'),
            cnt('escalas_individual'), cnt('escalas_grupo'),
        ])
    return resumo

# ── Envios ────────────────────────────────────────────────────────────────────

def enviar_localidades(locs: list, agora: str):
    log(f"Enviando Localidades ({len(locs)} registros)...")
    post("LOC", {
        "tipo":          "nova_planilha_localidades",
        "nome_planilha": f"Localidades_{agora}",
        "headers":       ["ID_Igreja","Nome_Localidade","Setor","Cidade","Texto_Completo"],
        "dados":         [[l['id_igreja'],l['nome_localidade'],l['setor'],l['cidade'],l['texto_completo']] for l in locs],
        "metadata":      {"total_localidades": len(locs), "timestamp": agora},
    }, "Localidades")

def enviar_alunos(alunos: list, agora: str):
    log(f"Enviando Alunos ({len(alunos)} registros)...")
    headers_alu = [
        "ID_ALUNO","NOME","ID_IGREJA",
        "ID_CARGO","CARGO_NOME","ID_NIVEL","NIVEL_NOME",
        "ID_INSTRUMENTO","INSTRUMENTO_NOME","ID_TONALIDADE","TONALIDADE_NOME",
        "FL_TIPO","STATUS","DATA_CADASTRO","CADASTRADO_POR",
        "DATA_ATUALIZACAO","ATUALIZADO_POR",
    ]
    dados_fmt = []
    for a in alunos:
        dados_fmt.append([
            str(a.get('id_aluno','')), a.get('nome',''), str(a.get('id_igreja','')),
            str(a.get('id_cargo','')), a.get('cargo_nome',''),
            str(a.get('id_nivel','')), a.get('nivel_nome',''),
            str(a.get('id_instrumento','')), a.get('instrumento_nome',''),
            str(a.get('id_tonalidade','')), a.get('tonalidade_nome',''),
            str(a.get('fl_tipo','')), str(a.get('status','')),
            a.get('data_cadastro',''), a.get('cadastrado_por',''),
            a.get('data_atualizacao',''), a.get('atualizado_por',''),
        ])
    post("ALU", {
        "tipo":          "nova_planilha_alunos",
        "nome_planilha": f"Alunos_{agora}",
        "headers":       headers_alu,
        "dados":         dados_fmt,
        "metadata":      {"total_alunos": len(alunos), "timestamp": agora},
    }, "Alunos")

def enviar_historico(alunos: list, historicos: dict, agora: str):
    """Envia histórico em lotes de 200 alunos (igual ao seq_01 original).
    Os POSTs internos são sequenciais por design — o Apps Script não
    suporta concorrência nessa rota. O paralelismo acontece ENTRE os
    6 envios principais (esta função roda em sua própria thread)."""
    total = len(alunos)
    num_lotes = (total + LOTE_TAMANHO_HIS - 1) // LOTE_TAMANHO_HIS
    log(f"Enviando Histórico ({total} alunos em {num_lotes} lotes)...")

    resumo_completo = gerar_resumo_alunos(alunos, historicos)

    for n in range(num_lotes):
        ini = n * LOTE_TAMANHO_HIS
        fim = min(ini + LOTE_TAMANHO_HIS, total)
        ids_lote = set(alunos[i]['id_aluno'] for i in range(ini, fim))

        lote_d = {k: [x for x in historicos[k] if x[0] in ids_lote] for k in historicos}
        lote_d['resumo'] = resumo_completo[ini:fim]

        post("HIS", {
            "tipo":         "licoes_alunos_lote",
            "lote_numero":  n + 1,
            "total_lotes":  num_lotes,
            **lote_d,
            "metadata": {
                "alunos_inicio": ini,
                "alunos_fim":    fim,
                "data_coleta":   datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            },
        }, f"Histórico lote {n+1}/{num_lotes}")

        if n < num_lotes - 1:
            time.sleep(2)

def enviar_aulas(aulas: list, relacao: dict, agora: str):
    log(f"Enviando Aulas ({len(aulas)} registros)...")
    dados = []
    for a in aulas:
        dados.append([
            a['id_aula'], a.get('id_turma',''), a.get('descricao',''),
            a.get('comum',''), a.get('dia_semana',''),
            a.get('hora_inicio',''), a.get('hora_termino',''),
            a.get('data_aula',''), a.get('data_hora_abertura',''),
            a.get('tem_ata','Não'), a.get('texto_ata',''),
            a.get('instrutor',''),
            a.get('total_alunos',0), a.get('presentes',0),
            a.get('lista_presentes',''), a.get('lista_ausentes',''),
        ])
    relacao_lista = [[k, v] for k, v in relacao.items() if k and v]
    post("AUL", {
        "tipo":           "historico_aulas_hortolandia",
        "dados":          dados,
        "relacao_alunos": relacao_lista,
        "headers": [
            "ID_Aula","ID_Turma","Descrição","Comum","Dia_Semana",
            "Hora_Início","Hora_Término","Data_Aula","Data_Hora_Abertura",
            "Tem_Ata","Texto_Ata","Instrutor",
            "Total_Alunos","Presentes","IDs_Presentes","IDs_Ausentes",
        ],
        "resumo": {
            "total_aulas": len(aulas),
            "timestamp":   agora,
        },
    }, "Aulas")

def enviar_turmas(turmas: list, agora: str):
    log(f"Enviando Turmas ({len(turmas)} registros)...")
    dados = []
    for t in turmas:
        dados.append([
            t['id_turma'], t.get('curso',''), t.get('descricao',''),
            t.get('comum',''), t.get('dia_semana',''),
            t.get('data_inicio',''), t.get('data_encerramento',''),
            t.get('hora_inicio',''), t.get('hora_termino',''),
            t.get('responsavel_1_id',''), t.get('responsavel_1_nome',''),
            t.get('responsavel_2_id',''), t.get('responsavel_2_nome',''),
            t.get('destinado_ao',''), t.get('ativo',''),
            t.get('cadastrado_em',''), t.get('cadastrado_por',''),
            t.get('atualizado_em',''), t.get('atualizado_por',''),
            'Coletado', datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        ])
    post("TUR", {
        "tipo":      "dados_turmas",
        "timestamp": agora,
        "dados":     dados,
        "headers": [
            "ID_Turma","Curso","Descricao","Comum","Dia_Semana",
            "Data_Inicio","Data_Encerramento","Hora_Inicio","Hora_Termino",
            "Responsavel_1_ID","Responsavel_1_Nome",
            "Responsavel_2_ID","Responsavel_2_Nome",
            "Destinado_ao","Ativo",
            "Cadastrado_em","Cadastrado_por","Atualizado_em","Atualizado_por",
            "Status_Coleta","Data_Coleta",
        ],
        "resumo": {"total_turmas": len(turmas), "timestamp": agora},
    }, "Turmas")

def enviar_matriculas(matriculas: list, agora: str):
    log(f"Enviando Matrículas ({len(matriculas)} registros)...")
    from collections import Counter
    contagem = Counter(m['ID_Turma'] for m in matriculas)
    resumo = [["ID_Turma","Quantidade_Matriculados","Status_Coleta"]] + [
        [tid, qtd, "Sucesso"] for tid, qtd in contagem.items()
    ]
    resp = post("MAT", {
        "tipo":        "contagem_matriculas",
        "dados":       resumo,
        "data_coleta": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }, "Matrículas (resumo)")

    planilha_id = None
    if resp:
        planilha_id = resp.get('detalhes', {}).get('planilha_id') or resp.get('planilha_id')

    detalhado = [["ID_Turma","Nome","Comum","Instrumento","Status"]] + [
        [m['ID_Turma'], m['Nome'], m['Comum'], m['Instrumento'], m['Status']]
        for m in matriculas
    ]
    payload_det = {
        "tipo":        "alunos_detalhados",
        "dados":       detalhado,
        "data_coleta": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }
    if planilha_id:
        payload_det['planilha_id'] = planilha_id
    post("MAT", payload_det, "Matrículas (detalhado)")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    t0    = time.time()
    agora = datetime.now().strftime('%d_%m_%Y-%H:%M')
    log(f"Iniciando Consolidação — {agora}")

    # 1. Verificar localidades
    if not os.path.exists('seq1_localidades.json'):
        log("❌ seq1_localidades.json não encontrado"); return
    with open('seq1_localidades.json', 'r', encoding='utf-8') as f:
        locs = json.load(f)

    # 2. Mesclar todos os lotes do worker
    arqs_lote = sorted(f for f in os.listdir('.') if f.startswith('output_lote_') and f.endswith('.json'))
    if not arqs_lote:
        log("❌ Nenhum output_lote_*.json encontrado"); return
    log(f"Mesclando {len(arqs_lote)} lote(s): {arqs_lote}")

    s1_alunos = []
    s1_hist   = {k: [] for k in [
        'mts_individual','mts_grupo','msa_individual','msa_grupo','provas',
        'hinario_individual','hinario_grupo','metodos','escalas_individual','escalas_grupo',
    ]}
    s2_aulas    = []
    s2_relacao  = {}
    s2_turmas   = []
    s2_mats     = []

    for arq in arqs_lote:
        with open(arq, 'r', encoding='utf-8') as f:
            d = json.load(f)
        s1_alunos.extend(d['seq1']['alunos'])
        for k in s1_hist:
            s1_hist[k].extend(d['seq1']['historicos'].get(k, []))
        s2_aulas.extend(d['seq2']['aulas'])
        s2_relacao.update(d['seq2']['relacao_aulas'])
        s2_turmas.extend(d['seq2']['turmas'])
        s2_mats.extend(d['seq2']['matriculas'])

    # Deduplicar (em caso de lotes sobrepostos por erro de config)
    s1_alunos = list({a['id_aluno']: a for a in s1_alunos}.values())
    s2_aulas  = list({a['id_aula']:  a for a in s2_aulas}.values())
    s2_turmas = list({t['id_turma']: t for t in s2_turmas}.values())

    log(f"Totais mesclados: Locs={len(locs)} | Alunos={len(s1_alunos)} | "
        f"Aulas={len(s2_aulas)} | Turmas={len(s2_turmas)} | Mats={len(s2_mats)}")

    # 3. Backup consolidado local
    ts_bak = datetime.now().strftime('%Y%m%d_%H%M%S')
    bak = {
        'localidades': locs, 'alunos': s1_alunos, 'historicos': s1_hist,
        'aulas': s2_aulas,   'relacao_aulas': s2_relacao,
        'turmas': s2_turmas, 'matriculas': s2_mats,
    }
    bak_file = f"consolidado_{ts_bak}.json"
    with open(bak_file, 'w', encoding='utf-8') as f:
        json.dump(bak, f, ensure_ascii=False)
    log(f"💾 Backup consolidado: {bak_file}")

    # 4. Enviar ao Google Sheets em paralelo
    #    - As 6 funções são completamente independentes (URLs distintas).
    #    - Cada uma roda em sua própria thread via ThreadPoolExecutor.
    #    - Tempo total ≈ max(maior_envio) em vez de soma(todos).
    #    - enviar_historico faz POSTs internos sequenciais por design
    #      (Apps Script não suporta concorrência nessa rota) — isso
    #      é preservado dentro da thread, sem impacto nas outras.
    tarefas = [
        ("Localidades", lambda: enviar_localidades(locs, agora)),
        ("Alunos",      lambda: enviar_alunos(s1_alunos, agora)),
        ("Histórico",   lambda: enviar_historico(s1_alunos, s1_hist, agora)),
        ("Aulas",       lambda: enviar_aulas(s2_aulas, s2_relacao, agora)),
        ("Turmas",      lambda: enviar_turmas(s2_turmas, agora)),
        ("Matrículas",  lambda: enviar_matriculas(s2_mats, agora)),
    ]

    log("🚀 Enviando ao Google Sheets em paralelo (6 threads)...")
    erros = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futuros = {pool.submit(fn): nome for nome, fn in tarefas}
        for fut in as_completed(futuros):
            nome = futuros[fut]
            try:
                fut.result()
            except Exception as e:
                log(f"❌ Erro em {nome}: {e}")
                erros.append(nome)

    t_min = (time.time() - t0) / 60
    log(f"🎉 CONSOLIDAÇÃO CONCLUÍDA em {t_min:.1f} min")
    log(f"   Localidades: {len(locs)}  |  Alunos: {len(s1_alunos)}  |  Aulas: {len(s2_aulas)}")
    log(f"   Turmas: {len(s2_turmas)}  |  Matrículas: {len(s2_mats)}")
    if erros:
        log(f"   ⚠️  Envios com erro: {', '.join(erros)}")

if __name__ == "__main__":
    main()

from flask import Flask, render_template, request, jsonify, redirect, url_for
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

app = Flask(__name__)

# --- CONEXÃO BANCO DE DADOS - NEON (POSTGRESQL) ---
DATABASE_URL =  os.environ.get('DATABASE_URL')  # Variável de ambiente no Render com o link do seu banco na nuvem

def get_db_connection():
    # Agora usamos o link direto para conectar ao banco na nuvem
    return psycopg2.connect(DATABASE_URL)

# FUNÇÃO DE LIMPEZA: Esta é a única parte que mudei para garantir que 14,4 vire 14.4
def limpar_numero(val):
    if val is None or val == '': return 0
    # Remove espaços e troca vírgula por ponto
    temp = str(val).strip().replace(',', '.')
    
    try:
        # Converte para float. Se vier "27.000", vira 27.0
        return float(temp)
    except ValueError:
        return 0

# Rota para a Página Inicial (Dashboard)
@app.route('/')
def index():
    return render_template('index.html') 

# Rota para a Página de Cadastro
@app.route('/cadastro.html')
def cadastro():
    return render_template('cadastro.html')

# ROTA DA API: Salva o novo registro no banco de dados (Lógica Completa)
@app.route('/api/cadastrar', methods=['POST'])
def cadastrar_emissao():
    try:
        data = request.form
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query mantendo todos os campos que você adicionou
        query = """INSERT INTO emissoes 
                   (cadastrante, turno, unidade, estado_gas, diametro_mm, 
                    pressao_barg_inicial, pressao_barg_final, data_inicial, horario_inicial, 
                    data_final, horario_final, volume_estimado, local_evento, 
                    equipamento_fonte, descricao_evento, causa_provavel, 
                    acao_corretiva, classificacao_evento, observacoes) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        # VALUES usando a função limpar_numero para todos os decimais
        values = (
            data.get('cadastrante'), 
            data.get('turno'), 
            data.get('unidade'),
            data.get('estado_gas'), 
            limpar_numero(data.get('diametro')),      # Protegido contra 27000
            limpar_numero(data.get('pressao')),       # Protegido contra 14400
            limpar_numero(data.get('pressao_final')), # Protegido contra erro de escala
            data.get('data_ini'), 
            data.get('hora_ini'), 
            data.get('data_fim'),
            data.get('hora_fim'), 
            limpar_numero(data.get('volume_estimado')), # Valor calculado pelo seu JS
            data.get('local_evento'),
            data.get('equipamento_fonte'),
            data.get('descricao_evento'),
            data.get('causa_provavel'),
            data.get('acao_corretiva'),
            data.get('classificacao_evento'),
            data.get('observacoes')
        )

        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()
        
        return render_template('sucesso.html')
        
    except Exception as e:
        print(f"Erro no cadastro: {e}")
        return jsonify({"status": "erro", "message": str(e)}), 500

# ROTA DA API: Carrega dados reais para KPIs, Gráfico e Tabela (Toda a sua lógica original)
@app.route('/api/dashboard_dados')
def get_dashboard_data():
    try:
        unidade = request.args.get('unidade')
        turno = request.args.get('turno')
        data_ini = request.args.get('data_ini')
        data_fim = request.args.get('data_fim')

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Filtros de Unidade e Turno (Lógica original preservada)
        where_clauses = ["1=1"]
        params = []
        if unidade and "Todas" not in unidade:
            where_clauses.append("unidade = %s")
            params.append(unidade.strip())
        if turno and "Todos" not in turno:
            turno_limpo = turno.replace('☀️ ', '').replace('🌙 ', '').strip()
            where_clauses.append("turno = %s")
            params.append(turno_limpo)

        # --- LOGICA GLOBAL VS FILTRADA ---
        if not data_ini and not data_fim:
            where_sql_kpi = " AND ".join(where_clauses)
            params_kpi = params
        else:
            where_sql_kpi = " AND ".join(where_clauses + ["data_inicial BETWEEN %s AND %s"])
            params_kpi = params + [data_ini, data_fim]

        # 1. Resumo para os Cards Principais (Volume e Eventos)
        cursor.execute(f"SELECT COALESCE(SUM(volume_estimado), 0) as total_vol, COUNT(*) as total_eventos FROM emissoes WHERE {where_sql_kpi}", params_kpi)
        resumo = cursor.fetchone()
        vol_dashboard = float(resumo['total_vol'])
        eventos_dashboard = resumo['total_eventos']

        # 2. Resumo para Variação (MODIFICADO PARA SER DINÂMICO)
        # Se você selecionou uma data no filtro, usamos ela como referência. Se não, usamos hoje.
        if data_ini:
            data_referencia = datetime.strptime(data_ini, '%Y-%m-%d')
        else:
            data_referencia = datetime.now()

        mes_alvo = data_referencia.month
        ano_alvo = data_referencia.year

        # Dados do Mês Alvo (Selecionado ou Atual)
        cursor.execute(f"""SELECT COALESCE(SUM(volume_estimado), 0) as v, COUNT(*) as c 
                          FROM emissoes 
                          WHERE MONTH(data_inicial) = %s AND YEAR(data_inicial) = %s""", (mes_alvo, ano_alvo))
        atual_data = cursor.fetchone()
        
        # Dados do Mês Anterior à referência
        cursor.execute(f"""SELECT COALESCE(SUM(volume_estimado), 0) as v, COUNT(*) as c 
                          FROM emissoes 
                          WHERE MONTH(data_inicial) = MONTH(DATE_SUB(%s, INTERVAL 1 MONTH)) 
                          AND YEAR(data_inicial) = YEAR(DATE_SUB(%s, INTERVAL 1 MONTH))""", (data_referencia, data_referencia))
        passado_data = cursor.fetchone()

        vol_m_atual = float(atual_data['v'])
        ev_m_atual = atual_data['c']
        vol_m_passado = float(passado_data['v'])
        ev_m_passado = passado_data['c']

        # Seus cálculos originais de variação
        var_vol = round(((vol_m_passado - vol_m_atual) / vol_m_passado * 100)) if vol_m_passado > 0 else (0 if vol_m_atual == 0 else -100)
        var_freq = round(((ev_m_passado - ev_m_atual) / ev_m_passado * 100)) if ev_m_passado > 0 else (0 if ev_m_atual == 0 else -100)

        # 3. Tabela e Gráfico (Mantido sua lógica original)
        cursor.execute(f"SELECT TO_CHAR(data_inicial, 'DD/MM/YYYY') as data, unidade, turno, cadastrante, volume_estimado FROM emissoes WHERE {where_sql_kpi} ORDER BY id DESC LIMIT 5", params_kpi)
        ultimas = cursor.fetchall()

        cursor.execute(f"SELECT MONTH(data_inicial) as mes, SUM(volume_estimado) as vol FROM emissoes WHERE YEAR(data_inicial) = 2026 GROUP BY mes ORDER BY mes")
        dados_grafico = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify({
            "ultimas": ultimas,
            "total_vol": vol_dashboard,
            "total_eventos": eventos_dashboard,
            "var_vol": var_vol,
            "var_freq": var_freq,
            "trend_grafico": dados_grafico
        })
    except Exception as e:
        print(f"Erro Dashboard: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/relatorio_consolidado')
def api_relatorio():
    try:
        data_ini = request.args.get('data_ini')
        data_fim = request.args.get('data_fim')
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Indicadores do Período ATUAL (Volume e Frequência) - MANTIDO
        query_stats = "SELECT COALESCE(SUM(volume_estimado), 0) as vol_total, COUNT(*) as total_eventos FROM emissoes WHERE data_inicial BETWEEN %s AND %s"
        cursor.execute(query_stats, (data_ini, data_fim))
        resumo_atual = cursor.fetchone()
        vol_atual = float(resumo_atual['vol_total'])
        atuais = resumo_atual['total_eventos']

       # 2. Indicadores do Período PASSADO (Comparação Mês a Mês)
        d_inicio_filtro = datetime.strptime(data_ini, '%Y-%m-%d')
        
        # Query que busca o mês anterior ao mês da data_ini
        query_passado = """SELECT COALESCE(SUM(volume_estimado), 0) as vol_passado, COUNT(*) as total_passado 
                           FROM emissoes 
                           WHERE MONTH(data_inicial) = MONTH(DATE_SUB(%s, INTERVAL 1 MONTH))
                           AND YEAR(data_inicial) = YEAR(DATE_SUB(%s, INTERVAL 1 MONTH))"""
        
        # Corrigido: Passando apenas as duas datas necessárias para o SQL
        cursor.execute(query_passado, (data_ini, data_ini))
        resumo_passado = cursor.fetchone()
        
        vol_passado = float(resumo_passado['vol_passado'])
        passados = resumo_passado['total_passado']

        # --- AQUI ESTÁ A CORREÇÃO PARA PERMITIR VALORES NEGATIVOS (AUMENTO) ---
        # Se o volume atual for maior que o passado, a conta resultará em um número negativo.
        if vol_passado > 0:
            var_vol = round(((vol_passado - vol_atual) / vol_passado * 100))
        else:
            # Se não houve emissão antes e agora houve, é um aumento de 100% (negativo)
            var_vol = 0 if vol_atual == 0 else -100
        
        if passados > 0:
            var_freq = round(((passados - atuais) / passados * 100))
        else:
            var_freq = 0 if atuais == 0 else -100

        # --- MANTENDO TODA A SUA LÓGICA DE DETALHAMENTO POR UNIDADE (Bloco 3) - INTEGRAL ---
        unidades_lista = ['SDGN1', 'SDGN2', 'SDGN3']
        detalhes_unidades = {}
        volumes_map = {}

        for unid in unidades_lista:
            cursor.execute("SELECT COUNT(*) as total, COALESCE(SUM(volume_estimado), 0) as vol FROM emissoes WHERE unidade = %s AND data_inicial BETWEEN %s AND %s", (unid, data_ini, data_fim))
            basico = cursor.fetchone()
            volumes_map[unid] = float(basico['vol'])
            
            detalhes_unidades[unid] = {
                "total_eventos": basico['total'],
                "volume": float(basico['vol']),
                "local_comum": "Sem registros",
                "fonte_comum": "Sem registros",
                "causa_comum": "Sem registros"
            }

            if basico['total'] > 0:
                for campo, chave in [('local_evento', 'local_comum'), ('equipamento_fonte', 'fonte_comum'), ('causa_provavel', 'causa_comum')]:
                    cursor.execute(f"SELECT {campo} FROM emissoes WHERE unidade = %s AND data_inicial BETWEEN %s AND %s GROUP BY {campo} ORDER BY COUNT(*) DESC LIMIT 1", (unid, data_ini, data_fim))
                    res = cursor.fetchone()
                    if res: detalhes_unidades[unid][chave] = res[campo]

        # --- MANTENDO SUA LISTA DETALHADA COM TO_CHAR (Bloco 4) - INTEGRAL ---
        query_lista = """SELECT TO_CHAR(data_inicial, 'DD/MM/YYYY') as data, 
                                TO_CHAR(horario_inicial, 'HH24:MI') as hora, 
                                unidade, equipamento_fonte, local_evento,
                                volume_estimado, causa_provavel, acao_corretiva 
                         FROM emissoes 
                         WHERE data_inicial BETWEEN %s AND %s 
                         ORDER BY data_inicial DESC, horario_inicial DESC"""
        cursor.execute(query_lista, (data_ini, data_fim))
        lista_eventos = cursor.fetchall()

        cursor.close()
        conn.close()

        # Retornando o JSON com os nomes que o seu HTML espera - MANTIDO
        print(f"DEBUG: Vol Atual: {vol_atual}, Vol Passado: {vol_passado}, Var: {var_vol}")
        return jsonify({
            "vol_total": vol_atual,
            "total_eventos": atuais,
            "var_vol": var_vol,
            "var_freq": var_freq,
            "volumes_unidades": volumes_map,
            "detalhes_unidades": detalhes_unidades,
            "lista_eventos": lista_eventos 
        })
    except Exception as e:
        print(f"Erro na API: {e}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/relatorio.html')
def relatorio_page():
    return render_template('relatorio.html')

import os

if __name__ == '__main__':
    # O Render usa uma variável de ambiente chamada PORT
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
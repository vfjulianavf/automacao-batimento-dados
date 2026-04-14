import streamlit as st
import pandas as pd
import pyodbc
from concurrent.futures import ThreadPoolExecutor
import time
import contextlib
import warnings
import io
from datetime import date, timedelta

# --- CONFIGURAÇÃO DE WARNINGS E VARIÁVEIS ---
warnings.filterwarnings('ignore', category=UserWarning, module='pandas.io.sql')

MODULOS_DISPONIVEIS = {
    'Consumidor.gov (13)': 13,
    'Bacen (12)': 12,
    'Participa DF (15)': 15,
    'Procon (3)': 3,
    'Módulo 20': 20,
    'Módulo 16': 16,
    'Módulo 17': 17,
    'Módulo 18': 18,
    'Módulo 22': 22,
    'Módulo 19': 19,
    'Módulo 21': 21,
    'Módulo 14': 14,
    'Módulo 34': 34,
    'Módulo 44': 44,
}

# --- FUNÇÕES AUXILIARES DE EXCEL ---

@st.cache_data
def convert_df_to_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Resultados')
    processed_data = output.getvalue()
    return processed_data

# --- FUNÇÕES DE CONEXÃO E QUERY ---

def get_sql_batimento(data_inicio: str, data_fim: str, sq_modulos: str, debug_external_protocol: str = None, debug_internal_protocol: str = None) -> str:
    """
    Gera a query SQL corrigindo a incompatibilidade de tipos (STRING vs TINYINT)
    e garantindo a união correta para o Impala.
    """
    debug_filter_geral = ""
    debug_filter_bacen = ""
    date_filter = ""
    
    # 🧩 O segredo aqui é garantir que a comparação no WHERE use o mesmo tipo de dado
    # Transformamos o sq_modulos (que vem do Streamlit) em uma lista de inteiros para o SQL
    
    if debug_external_protocol:
        debug_filter_geral = f"AND n.ds_resposta_padrao = '{debug_external_protocol}'"
        debug_filter_bacen = f"AND CAST(a.nr_demanda_bacen AS STRING) = '{debug_external_protocol}'"
    elif debug_internal_protocol:
        debug_filter_geral = f"AND c.dn_protocolo = '{debug_internal_protocol}'"
        debug_filter_bacen = f"AND a.dn_protocolo = '{debug_internal_protocol}'"

    if not (debug_external_protocol or debug_internal_protocol):
        date_filter = f"""
            AND CAST(a.dt_abertura AS DATE) >= '{data_inicio}'
            AND CAST(a.dt_abertura AS DATE) < date_add(CAST('{data_fim}' AS DATE), 1)
        """

    sql_query = f"""
WITH base_unificada AS (
    -- 1. BUSCA NA ESTRUTURA GERAL (Tabelas GTA)
    SELECT
        a.dt_abertura,
        c.dn_protocolo AS protocolo_interno,
        CAST(b.no_modulo AS STRING) AS no_modulo,
        MAX(h.no_formulario) AS no_formulario,
        d.no_status_ocorrencia,
        CAST(n.ds_resposta_padrao AS STRING) AS protocolo_externo
    FROM database_oracle_gta.tb_ocorrencia a
    LEFT JOIN database_oracle_gta.tb_modulo b ON b.sq_modulo = a.sq_modulo
    LEFT JOIN database_oracle_gta.tb_protocolo c ON c.sq_protocolo = a.sq_protocolo
    LEFT JOIN database_oracle_gta.tb_status_ocorrencia d ON d.cd_status_ocorrencia = a.cd_status_ocorrencia
    LEFT JOIN database_oracle_gta.tb_trilha_cadastro e ON e.sq_ocorrencia = a.sq_ocorrencia
    LEFT JOIN database_oracle_gta.tb_formulario h ON h.sq_formulario = e.sq_formulario
    LEFT JOIN database_oracle_gta.tb_trilha_classificacao j ON j.sq_trilha_cadastro = e.sq_trilha_cadastro
    LEFT JOIN database_oracle_gta.tb_campo_padrao m ON m.sq_modulo = a.sq_modulo
    LEFT JOIN database_oracle_gta.tb_resposta_padrao n ON n.sq_ocorrencia = a.sq_ocorrencia
    WHERE 1=1
        {date_filter}
        AND a.sq_modulo IN ({sq_modulos})
        AND m.no_campo_padrao = 'Protocolo de origem'
        AND n.sq_campo_padrao = m.sq_campo_padrao
        AND (j.ds_classificacao IN ('Improcedente', 'Procedente não solucionada', 'Procedente solucionada') OR j.ds_classificacao IS NULL)
        {debug_filter_geral}
    GROUP BY 1, 2, 3, 5, 6

    UNION ALL

    -- 2. BUSCA NA TABELA ESPECÍFICA BACEN API
    SELECT
        a.dt_abertura,
        a.dn_protocolo AS protocolo_interno,
        CAST('Bacen (12)' AS STRING) AS no_modulo,
        MAX(b.no_formulario) AS no_formulario,
        b.no_status_ocorrencia,
        CAST(a.nr_demanda_bacen AS STRING) AS protocolo_externo
    FROM database.tb_gta_bacen_api a
    LEFT JOIN database.tb_gta_protocolo b ON b.sq_ocorrencia = a.sq_ocorrencia
    WHERE 1=1
        {date_filter}
        -- Corrigido: Comparamos número com número
        AND 12 IN ({sq_modulos})
        AND (b.ds_classificacao IN ('Improcedente', 'Procedente não solucionada', 'Procedente solucionada') OR b.ds_classificacao IS NULL)
        {debug_filter_bacen}
    GROUP BY 1, 2, 3, 5, 6
)
SELECT 
    dt_abertura,
    protocolo_interno,
    protocolo_externo,
    no_modulo,
    no_formulario,
    no_status_ocorrencia
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY protocolo_interno ORDER BY dt_abertura DESC) as rn
    FROM base_unificada
) AS subquery_final 
WHERE rn = 1
"""
    return sql_query

def get_sql_quick_search(protocolos_internos: str) -> str:
    return f"""
SELECT
    c.dn_protocolo AS protocolo_interno,
    DATE_FORMAT(a.dt_abertura, 'dd/MM/yyyy') AS dt_abertura,
    d.no_status_ocorrencia
FROM database_oracle_gta.tb_ocorrencia a
LEFT JOIN database_oracle_gta.tb_protocolo c ON c.sq_protocolo = a.sq_protocolo
LEFT JOIN database_oracle_gta.tb_status_ocorrencia d ON d.cd_status_ocorrencia = a.cd_status_ocorrencia
WHERE c.dn_protocolo IN ({protocolos_internos})
"""

@st.cache_data(ttl=600, show_spinner=False)
def run_query_odbc(conn_str, sql_query: str) -> pd.DataFrame:
    conn_str_with_timeout = conn_str + (';QUERYTIMEOUT=0;TIMEOUT=0' if 'TIMEOUT' not in conn_str.upper() else '')
    try:
        with contextlib.closing(pyodbc.connect(conn_str_with_timeout, autocommit=True)) as cn:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                return pd.read_sql(sql_query, cn)
    except Exception as e:
        raise Exception(f"Falha na consulta: {e}")

# --- INICIALIZAÇÃO DO ESTADO DA SESSÃO ---
if 'df_final' not in st.session_state: st.session_state['df_final'] = pd.DataFrame()
if 'df_faltou' not in st.session_state: st.session_state['df_faltou'] = pd.DataFrame()
if 'coluna_protocolo' not in st.session_state: st.session_state['coluna_protocolo'] = 'Selecione...'
if 'coluna_situacao_upload' not in st.session_state: st.session_state['coluna_situacao_upload'] = 'Não Usar'
if 'df_upload_original' not in st.session_state: st.session_state['df_upload_original'] = pd.DataFrame()

hoje = date.today()
data_30_dias_atras = hoje - timedelta(days=30)

st.set_page_config(layout="wide", page_title="Batimento de Protocolos")
st.title("🤝 Batimento Protocolo Externo (Upload) vs. Interno")

# --- BARRA LATERAL (CONEXÃO) ---
st.sidebar.header('🔌 Configuração do Datalake')
dsn = st.sidebar.text_input('DSN (Fonte de Dados)', value='')
uid = st.sidebar.text_input('Usuário (UID):', value='')
pwd = st.sidebar.text_input('Senha (PWD):', type='password', value='')
conn_str = f'DSN={dsn}' + (f';UID={uid};PWD={pwd}' if uid and pwd else '')

if st.sidebar.button('Testar conexão'):
    try:
        with contextlib.closing(pyodbc.connect(conn_str + ';TIMEOUT=5', autocommit=True)) as cn:
            st.sidebar.success('✅ Conexão DSN OK')
    except Exception as e:
        st.sidebar.error(f'❌ Falha na conexão: {e}')

# --- 1. CARREGAR E MAPEAR ---
st.subheader("1. Carregar, Mapear e Filtrar Protocolos Externos")
uploaded_file = st.file_uploader("Selecione o arquivo CSV ou Excel", type=['csv', 'xlsx', 'xls'])

if uploaded_file is not None:
    if st.session_state.get('uploaded_file_name') != uploaded_file.name:
        file_ext = uploaded_file.name.split('.')[-1].lower()
        if file_ext == 'csv':
            try:
                df_temp = pd.read_csv(uploaded_file, encoding='utf-8', sep=None, engine='python', dtype=str)
            except:
                uploaded_file.seek(0)
                df_temp = pd.read_csv(uploaded_file, encoding='latin-1', sep=None, engine='python', dtype=str)
        else:
            df_temp = pd.read_excel(uploaded_file, dtype=str)
        st.session_state['df_upload_original'] = df_temp
        st.session_state['uploaded_file_name'] = uploaded_file.name

    df_upload = st.session_state['df_upload_original']
    opcoes_colunas = ['Selecione...'] + df_upload.columns.tolist()
    
    # Prioriza sugerir a coluna "Número" se ela existir
    idx_sugerido = opcoes_colunas.index('Número') if 'Número' in opcoes_colunas else 0
    
    coluna_protocolo = st.selectbox("Qual coluna contém o número do Protocolo Externo?", options=opcoes_colunas, index=idx_sugerido)
    st.session_state['coluna_protocolo'] = coluna_protocolo

    col_sit_sel, col_sit_fil = st.columns([1, 2])
    with col_sit_sel:
        coluna_situacao_upload = st.selectbox("Coluna de Situação para Filtrar?", options=['Não Usar'] + df_upload.columns.tolist())
        st.session_state['coluna_situacao_upload'] = coluna_situacao_upload

    df_filtrado_upload = df_upload.copy()
    if coluna_situacao_upload != 'Não Usar':
        opcoes_unicas = ['(Selecionar Todos)'] + sorted(df_upload[coluna_situacao_upload].dropna().unique().tolist())
        with col_sit_fil:
            situacoes_selecionadas = st.multiselect("Quais valores incluir?", options=opcoes_unicas, default=['(Selecionar Todos)'])
        if situacoes_selecionadas and '(Selecionar Todos)' not in situacoes_selecionadas:
            df_filtrado_upload = df_upload[df_upload[coluna_situacao_upload].isin(situacoes_selecionadas)]

    if coluna_protocolo != 'Selecione...':
        df_filtrado_upload['Protocolo_Externo_Upload'] = df_filtrado_upload[coluna_protocolo].astype(str).str.strip().str.upper()
        st.session_state['df_upload'] = df_filtrado_upload
        st.session_state['df_upload_unique_count'] = len(df_filtrado_upload['Protocolo_Externo_Upload'].unique())
        st.dataframe(df_filtrado_upload.head(10))

# --- 2. FILTROS BUSCA ---
st.subheader("2. Filtros da Busca no Datalake")
modulos_selecionados_nomes = st.multiselect("Selecione os Módulos:", options=list(MODULOS_DISPONIVEIS.keys()), default=['Bacen (12)', 'Consumidor.gov (13)'])
sq_modulos_str = ", ".join([str(MODULOS_DISPONIVEIS[n]) for n in modulos_selecionados_nomes])
col_ini, col_fim = st.columns(2)
data_inicio_sel = col_ini.date_input("Data Inicial", value=data_30_dias_atras)
data_fim_sel = col_fim.date_input("Data Final", value=hoje)

# --- 3. EXECUÇÃO ---
st.subheader("3. Executar Batimento e Análise")
if st.button('🚀 Iniciar Batimento', type='primary'):
    if not dsn or st.session_state['coluna_protocolo'] == 'Selecione...':
        st.error("Configure a conexão e selecione a coluna de protocolo.")
    else:
        sql = get_sql_batimento(data_inicio_sel.strftime('%Y-%m-%d'), data_fim_sel.strftime('%Y-%m-%d'), sq_modulos_str)
        with st.spinner("Buscando dados no Datalake..."):
            df_interna = run_query_odbc(conn_str, sql)
        
        if not df_interna.empty:
            df_interna['Protocolo_Externo_Interno'] = df_interna['protocolo_externo'].astype(str).str.strip().str.upper()
            df_final = st.session_state['df_upload'].merge(df_interna, left_on='Protocolo_Externo_Upload', right_on='Protocolo_Externo_Interno', how='left')
            df_final['Status_Batimento'] = df_final['protocolo_interno'].apply(lambda x: '✅ Encontrado' if pd.notnull(x) else '❌ Faltando')
            
            st.session_state['df_final'] = df_final
            st.session_state['df_faltou'] = df_final[df_final['Status_Batimento'] == '❌ Faltando']
            st.rerun()

# --- 4. RESULTADOS ---
if not st.session_state['df_final'].empty:
    st.header("4. Resultados do Batimento")
    res = st.session_state['df_final']
    st.metric("Protocolos Faltantes", len(st.session_state['df_faltou']))
    st.dataframe(res[['Protocolo_Externo_Upload', 'Status_Batimento', 'protocolo_interno', 'no_modulo', 'no_status_ocorrencia']])
    
    st.download_button("📥 Baixar Resultado (Excel)", data=convert_df_to_excel(res), file_name="batimento.xlsx")

# --- 5. BUSCA RÁPIDA ---
st.markdown("---")
st.header("5. 🔍 Busca Rápida")
prot_input = st.text_area("Protocolos Internos:")
if st.button("Buscar Detalhes"):
    if prot_input and dsn:
        list_sql = ", ".join([f"'{p.strip()}'" for p in prot_input.replace('\n', ',').split(',') if p.strip()])
        df_quick = run_query_odbc(conn_str, get_sql_quick_search(list_sql))
        st.dataframe(df_quick)

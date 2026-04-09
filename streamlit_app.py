import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import os
import re
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from fpdf import FPDF

# ==========================================
# 1. CONFIGURACIÓN Y ESTILOS
# ==========================================
st.set_page_config(page_title="Control de Golpes de Matrices - Fumiscor", layout="wide", page_icon="⚙️")

st.markdown("""
<style>
    .header-style { font-size: 26px; font-weight: bold; margin-bottom: 5px; color: #1F2937; text-align: center; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="header-style">⚙️ Reporte Inteligente: Control de Golpes de Matrices</div>', unsafe_allow_html=True)
st.write("<p style='text-align: center;'>Cruce de Catálogo (RH) + Google Forms + SQL Server.</p>", unsafe_allow_html=True)
st.divider()

# ==========================================
# 2. ENLACES DE DATOS (Google Sheets & Forms)
# ==========================================
# Catálogo Maestro
URL_CATALOGO = "https://docs.google.com/spreadsheets/d/198KjQWZwfvvWwq1q1N1zv1cgzkot2hhGbwQvbi9_zFQ/export?format=csv&gid=818188145"

# Hojas de respuestas de Google Forms (Preventivos y Correctivos)
URL_FORMS_PREV = "https://docs.google.com/spreadsheets/d/1MptnOuRfyOAr1EgzNJVygTtNziOSdzXJn-PZDX0pNzc/export?format=csv&gid=324842888"
URL_FORMS_CORR = "https://docs.google.com/spreadsheets/d/1A-0mngZdgvZGbqzWjA_awhrwfvca0K4aGqp5NBAoFAY/export?format=csv&gid=238711679"

# ==========================================
# 3. FUNCIONES DE EXTRACCIÓN Y LIMPIEZA
# ==========================================
def clean_str(val):
    if pd.isna(val): return ""
    v = str(val).strip().upper()
    if v.endswith('.0'): v = v[:-2]
    return v

def get_match_key(texto):
    """Extrae la secuencia numérica más larga para cruzar datos entre plataformas."""
    if pd.isna(texto): return ""
    val = str(texto).upper()
    matches = re.findall(r'\d{5,}', val)
    return max(matches, key=len) if matches else re.sub(r'[^A-Z0-9]', '', val)

@st.cache_data(ttl=300)
def load_all_sources():
    # --- 1. CARGAR CATÁLOGO (FILA 3 encabezados) ---
    df_cat = pd.read_csv(URL_CATALOGO, skiprows=2).dropna(how='all')
    df_cat.columns = df_cat.columns.astype(str).str.upper().str.strip()

    # --- 2. CARGAR RESPUESTAS DE GOOGLE FORMS ---
    def fetch_forms(url):
        try:
            df = pd.read_csv(url)
            df.columns = df.columns.astype(str).str.upper()
            # Buscar columna de fecha
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MARCA TEMPORAL' in c), None)
            # Buscar columna de pieza (RH)
            col_p = next((c for c in df.columns if 'PIEZA' in c or 'NUMERO' in c or 'RH' in c), None)
            
            if col_f and col_p:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['PIEZA_KEY'] = df[col_p].apply(get_match_key)
                return df[['FECHA_DT', 'PIEZA_KEY']].dropna()
        except: return pd.DataFrame()
        return pd.DataFrame()

    df_forms_mants = pd.concat([fetch_forms(URL_FORMS_PREV), fetch_forms(URL_FORMS_CORR)])

    # --- 3. SQL SERVER (PRODUCCIÓN Y EVENTOS) ---
    try:
        conn = st.connection("wii_bi", type="sql")
        
        # Producción desde 2023
        q_prod = "SELECT pr.Code as PIEZA, CAST(p.Date as DATE) as FECHA, SUM(p.Good + p.Rework) as GOLPES FROM PROD_D_01 p JOIN PRODUCT pr ON p.ProductId = pr.ProductId WHERE p.Date >= '2023-01-01' GROUP BY pr.Code, CAST(p.Date as DATE)"
        df_sql_prod = conn.query(q_prod)
        df_sql_prod['PIEZA_KEY'] = df_sql_prod['PIEZA'].apply(get_match_key)
        df_sql_prod['FECHA'] = pd.to_datetime(df_sql_prod['FECHA'])

        # Mantenimientos registrados en SQL
        q_event = "SELECT CAST(e.Date as DATE) as FECHA, t4.Name as DETALLE FROM EVENT_01 e LEFT JOIN EVENTTYPE t4 ON e.EventTypeLevel4 = t4.EventTypeId WHERE UPPER(t4.Name) LIKE '%MATRI%' AND e.Date >= '2023-01-01'"
        df_sql_mants = conn.query(q_event)
        df_sql_mants['PIEZA_KEY'] = df_sql_mants['DETALLE'].apply(get_match_key)
        df_sql_mants['FECHA_DT'] = pd.to_datetime(df_sql_mants['FECHA'])
        
    except Exception as e:
        st.error(f"Error SQL: {e}")
        df_sql_prod, df_sql_mants = pd.DataFrame(), pd.DataFrame()

    return df_cat, df_sql_prod, df_forms_mants, df_sql_mants

# ==========================================
# 4. MOTOR DE CRUCE Y CÁLCULO
# ==========================================
def procesar_logica_golpes(df_cat, df_prod, df_forms, df_sql_mants):
    resultados = []
    anio_actual = pd.to_datetime("today").year
    inicio_anio = pd.to_datetime(f"{anio_actual}-01-01")

    # Unificamos todos los mantenimientos externos (Forms + SQL)
    mants_externos = pd.concat([df_forms, df_sql_mants[['FECHA_DT', 'PIEZA_KEY']]]).sort_values('FECHA_DT', ascending=False)

    for _, row in df_cat.iterrows():
        pieza_rh = clean_str(row.get('RH', ''))
        if not pieza_rh or pieza_rh in ['NAN', '-']: continue
        
        pieza_key = get_match_key(pieza_rh)
        golpes_excel = pd.to_numeric(row.get('GOLPES', 0), errors='coerce') or 0
        fecha_excel = pd.to_datetime(row.get('ULTIMO MANTENIMIENTO'), dayfirst=True, errors='coerce')

        # Buscar mantenimiento más reciente fuera del excel
        match_externo = mants_externos[mants_externos['PIEZA_KEY'] == pieza_key]
        fecha_externa = match_externo['FECHA_DT'].max() if not match_externo.empty else pd.NaT

        # LÓGICA DE DECISIÓN
        golpes_finales = 0
        if pd.notna(fecha_externa) and (pd.isna(fecha_excel) or fecha_externa > fecha_excel):
            # CASO 1: Hay un mantenimiento nuevo. Reset y contar desde esa fecha.
            fecha_final = fecha_externa
            prod = df_prod[(df_prod['PIEZA_KEY'] == pieza_key) & (df_prod['FECHA'] >= fecha_final)]
            golpes_finales = int(prod['GOLPES'].sum())
        else:
            # CASO 2: No hay nada nuevo. Base Excel + Prod desde el 1 de Enero.
            fecha_final = fecha_excel
            prod = df_prod[(df_prod['PIEZA_KEY'] == pieza_key) & (df_prod['FECHA'] >= inicio_anio)]
            golpes_finales = int(golpes_excel) + int(prod['GOLPES'].sum())

        limite = 20000
        color = "VERDE"
        if golpes_finales >= limite: color = "ROJO"
        elif golpes_finales >= (limite * 0.8): color = "AMARILLO"

        resultados.append({
            'CLIENTE': clean_str(row.get('CLIENTE', '-')),
            'PIEZA': pieza_rh,
            'ULT_MANT': fecha_final.strftime('%d/%m/%Y') if pd.notna(fecha_final) else "-",
            'GOLPES': golpes_finales,
            'LIMITE': limite,
            'COLOR': color,
            'ESTADO': "MANT. REQUERIDO" if color == "ROJO" else "ALERTA" if color == "AMARILLO" else "OK"
        })
    return pd.DataFrame(resultados)

# ==========================================
# 5. GENERACIÓN DE PDF Y EXPORTACIÓN
# ==========================================
# (Se mantiene la lógica de las clases PDF anteriores...)
class PDFGolpes(FPDF):
    def header(self):
        self.set_font("Arial", 'B', 15)
        self.set_text_color(31, 73, 125)
        self.cell(0, 10, "Control de Golpes de Matrices (Validacion Forms + SQL)", border=0, ln=True, align='C')
        self.ln(5)
    def footer(self):
        self.set_y(-15); self.set_font("Arial", "I", 8); self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, "C")

def build_pdf_data(df):
    pdf = PDFGolpes(orientation='L', unit='mm', format='A4')
    pdf.add_page()
    pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(31, 73, 125); pdf.set_text_color(255, 255, 255)
    
    cols = [("Cliente", 25), ("Pieza RH", 90), ("Ult. Mant.", 30), ("Golpes", 30), ("Limite", 30), ("Estado", 60)]
    for c in cols: pdf.cell(c[1], 8, c[0], 1, 0, 'C', True)
    pdf.ln()

    pdf.set_font("Arial", '', 8); pdf.set_text_color(0, 0, 0)
    for _, r in df.iterrows():
        bg = (255, 180, 180) if r['COLOR'] == "ROJO" else (255, 240, 180) if r['COLOR'] == "AMARILLO" else (255, 255, 255)
        pdf.set_fill_color(*bg)
        pdf.cell(25, 7, r['CLIENTE'], 1, 0, 'C', True)
        pdf.cell(90, 7, r['PIEZA'][:50], 1, 0, 'L', True)
        pdf.cell(30, 7, r['ULT_MANT'], 1, 0, 'C', True)
        pdf.cell(30, 7, f"{r['GOLPES']:,}", 1, 0, 'C', True)
        pdf.cell(30, 7, f"{r['LIMITE']:,}", 1, 0, 'C', True)
        pdf.cell(60, 7, r['ESTADO'], 1, 1, 'C', True)

    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name); b = open(buf.name, "rb").read(); os.remove(buf.name)
    return b

# ==========================================
# 6. INTERFAZ STREAMLIT
# ==========================================
if st.button("🔄 Actualizar Todo (Limpiar Caché)"):
    st.cache_data.clear(); st.rerun()

with st.spinner("Sincronizando Excel, Google Forms y SQL Server..."):
    df_cat, df_prod, df_forms, df_sql_m = load_all_sources()

if not df_cat.empty:
    st.success("Sincronización Exitosa.")
    if st.button("⚙️ Procesar Estado de Matrices", type="primary", use_container_width=True):
        res = procesar_logica_golpes(df_cat, df_prod, df_forms, df_sql_m)
        st.session_state['res_final'] = res

if 'res_final' in st.session_state:
    df = st.session_state['res_final']
    st.write("---")
    c1, c2, c3 = st.columns(3)
    c1.metric("Matrices Críticas", len(df[df['COLOR']=="ROJO"]))
    c2.metric("En Alerta", len(df[df['COLOR']=="AMARILLO"]))
    c3.metric("Total Analizadas", len(df))
    
    pdf_bytes = build_pdf_data(df)
    st.download_button("📥 Descargar Reporte PDF Completo", pdf_bytes, "Reporte_Golpes_Matrices.pdf", "application/pdf", use_container_width=True)

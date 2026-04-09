import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import os
import re
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
st.write("<p style='text-align: center;'>Validación de Mantenimientos (Multicolumna) + SQL Server.</p>", unsafe_allow_html=True)
st.divider()

# ==========================================
# 2. ENLACES DE DATOS
# ==========================================
URL_CATALOGO = "https://docs.google.com/spreadsheets/d/198KjQWZwfvvWwq1q1N1zv1cgzkot2hhGbwQvbi9_zFQ/export?format=csv&gid=818188145"
URL_FORMS_PREV = "https://docs.google.com/spreadsheets/d/1VqsPNhAlT1kPCltbMWsbkZNFBKdwZRFM5RAmnRV0v3c/export?format=csv&gid=1603203990"
URL_FORMS_CORR = "https://docs.google.com/spreadsheets/d/1bL_tnlSXGO_t9tKnhIHT5pZ3DAxivbiq2tFETVxBaVI/export?format=csv&gid=1507213893"

# ==========================================
# 3. FUNCIONES DE EXTRACCIÓN Y LIMPIEZA
# ==========================================
def clean_str(val):
    if pd.isna(val): return ""
    v = str(val).strip().upper()
    if v.endswith('.0'): v = v[:-2]
    return v

def get_match_key(texto):
    if pd.isna(texto): return ""
    val = str(texto).upper()
    val = re.sub(r'-?OP\d+', '', val) 
    matches = re.findall(r'\d{5,}', val) 
    return max(matches, key=len) if matches else re.sub(r'[^A-Z0-9]', '', val)

@st.cache_data(ttl=300)
def load_all_sources():
    try:
        df_cat = pd.read_csv(URL_CATALOGO, skiprows=2).dropna(how='all')
        df_cat.columns = df_cat.columns.astype(str).str.upper().str.strip()
        df_cat['PIEZA_KEY'] = df_cat['RH'].apply(lambda x: get_match_key(clean_str(x)))
    except Exception as e:
        st.error(f"Error cargando Catálogo: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def fetch_forms(url, tipo_mant):
        try:
            df = pd.read_csv(url)
            if df.empty: return pd.DataFrame()
            df.columns = [str(c).upper().strip() for c in df.columns]
            
            # --- LÓGICA DE BÚSQUEDA ---
            col_f = next((c for c in df.columns if c == 'FECHA'), 'MARCA TEMPORAL')
            cols_pieza = [c for c in df.columns if c.startswith('PIEZAS') or 'NUMERO DE PIEZA' in c]
            
            # Buscamos TODAS las columnas que pregunten si el mantenimiento terminó (Sección 1.7, 2.7, etc.)
            cols_terminado = [c for c in df.columns if 'TERMINO' in c or 'TERMINADO' in c]
            
            registros = []
            for _, row in df.iterrows():
                fecha = pd.to_datetime(row.get(col_f), dayfirst=True, errors='coerce')
                if pd.isna(fecha): continue
                
                pieza_raw = ""
                for cp in cols_pieza:
                    val = clean_str(row.get(cp))
                    if val and val not in ['NAN', 'NONE', '-', '0', '']:
                        pieza_raw = val
                        break 
                
                if not pieza_raw: continue
                
                # REVISIÓN MULTICOLUMNA: Si en alguna de las columnas dice "No", se considera ABIERTO
                abierto = 'NO'
                for ct in cols_terminado:
                    val_term = clean_str(row.get(ct))
                    if val_term == 'NO' or val_term.startswith('NO'):
                        abierto = 'SI'
                        break # Con un solo "No" ya es suficiente para identificarlo como pendiente
                
                registros.append({
                    'FECHA_DT': fecha,
                    'TIPO_MANT': tipo_mant,
                    'ABIERTO': abierto,
                    'PIEZA_RAW': pieza_raw,
                    'PIEZA_KEY': get_match_key(pieza_raw)
                })
            return pd.DataFrame(registros)
        except: return pd.DataFrame()

    df_forms_all = pd.concat([fetch_forms(URL_FORMS_PREV, "PREV"), fetch_forms(URL_FORMS_CORR, "CORR")])

    try:
        conn = st.connection("wii_bi", type="sql")
        q_prod = "SELECT pr.Code as PIEZA, CAST(p.Date as DATE) as FECHA, SUM(p.Good + p.Rework) as GOLPES FROM PROD_D_01 p JOIN PRODUCT pr ON p.ProductId = pr.ProductId WHERE p.Date >= '2023-01-01' GROUP BY pr.Code, CAST(p.Date as DATE)"
        df_sql_prod = conn.query(q_prod)
        df_sql_prod['PIEZA_KEY'] = df_sql_prod['PIEZA'].apply(get_match_key)
        df_sql_prod['FECHA'] = pd.to_datetime(df_sql_prod['FECHA'])
    except: df_sql_prod = pd.DataFrame()

    return df_cat, df_sql_prod, df_forms_all

# ==========================================
# 4. MOTOR DE CRUCE Y CÁLCULO
# ==========================================
def procesar_logica_golpes(df_cat, df_prod, df_forms):
    resultados = []
    abiertos = []
    inicio_anio = pd.to_datetime(f"{datetime.now().year}-01-01")

    # 1. Identificar mantenimientos ABIERTOS
    if not df_forms.empty:
        df_ab = df_forms[df_forms['ABIERTO'] == 'SI']
        for _, r in df_ab.iterrows():
            match_cat = df_cat[df_cat['PIEZA_KEY'] == r['PIEZA_KEY']]
            cliente = clean_str(match_cat.iloc[0].get('CLIENTE', 'Externo')) if not match_cat.empty else "Externo"
            abiertos.append({
                'CLIENTE': cliente,
                'PIEZA_REPORTE': str(r['PIEZA_RAW']),
                'TIPO': r['TIPO_MANT'],
                'FECHA_APERTURA': r['FECHA_DT'].strftime('%d/%m/%Y')
            })

    # 2. Calcular semáforo para Catálogo
    for _, row in df_cat.iterrows():
        pieza_rh = clean_str(row.get('RH', ''))
        if not pieza_rh or pieza_rh in ['NAN', '-']: continue
        
        p_key = row['PIEZA_KEY']
        g_excel = pd.to_numeric(row.get('GOLPES', 0), errors='coerce') or 0
        f_excel = pd.to_datetime(row.get('ULTIMO MANTENIMIENTO'), dayfirst=True, errors='coerce')

        # Buscar último mantenimiento CERRADO (donde todas las columnas fueron 'Si')
        f_form = pd.NaT
        if not df_forms.empty:
            match_f = df_forms[(df_forms['PIEZA_KEY'] == p_key) & (df_forms['ABIERTO'] == 'NO')]
            if not match_f.empty: f_form = match_f['FECHA_DT'].max()

        if pd.notna(f_form) and (pd.isna(f_excel) or f_form > f_excel):
            f_final = f_form
            prod = df_prod[(df_prod['PIEZA_KEY'] == p_key) & (df_prod['FECHA'] >= f_final)]
            g_final = int(prod['GOLPES'].sum())
        else:
            f_final = f_excel
            prod = df_prod[(df_prod['PIEZA_KEY'] == p_key) & (df_prod['FECHA'] >= inicio_anio)]
            g_final = int(g_excel) + int(prod['GOLPES'].sum())

        limite = 20000
        color = "ROJO" if g_final >= limite else "AMARILLO" if g_final >= (limite*0.8) else "VERDE"

        resultados.append({
            'CLIENTE': clean_str(row.get('CLIENTE', '-')),
            'PIEZA': pieza_rh,
            'ULT_MANT': f_final.strftime('%d/%m/%Y') if pd.notna(f_final) else "-",
            'GOLPES': g_final, 'LIMITE': limite, 'COLOR': color,
            'ESTADO': "MANT. REQUERIDO" if color == "ROJO" else "ALERTA" if color == "AMARILLO" else "OK"
        })
        
    return pd.DataFrame(resultados), pd.DataFrame(abiertos)

# ==========================================
# 5. GENERACIÓN DE PDF
# ==========================================
class PDFGolpes(FPDF):
    def header(self):
        self.set_font("Arial", 'B', 15)
        self.set_text_color(31, 73, 125)
        self.cell(0, 10, "Control de Golpes de Matrices (Validacion Forms + SQL)", border=0, ln=True, align='C')
        self.ln(3)
    def footer(self):
        self.set_y(-15); self.set_font("Arial", "I", 8); self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, "C")

def build_pdf_data(df, df_ab):
    pdf = PDFGolpes(orientation='L', unit='mm', format='A4')
    pdf.add_page()
    pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(31, 73, 125); pdf.set_text_color(255, 255, 255)
    cols = [("Cliente", 25), ("Pieza RH", 90), ("Ult. Mant.", 25), ("Golpes Acum.", 30), ("Limite", 30), ("Estado Actual", 70)]
    for c in cols: pdf.cell(c[1], 8, c[0], 1, 0, 'C', True)
    pdf.ln()

    for _, r in df.iterrows():
        pdf.set_fill_color(255, 255, 255); pdf.set_text_color(0, 0, 0); pdf.set_font("Arial", '', 8)
        pdf.cell(25, 7, r['CLIENTE'], 1, 0, 'C'); pdf.cell(90, 7, r['PIEZA'][:50], 1, 0, 'L')
        pdf.cell(25, 7, r['ULT_MANT'], 1, 0, 'C'); pdf.cell(30, 7, f"{r['GOLPES']:,}", 1, 0, 'C')
        pdf.cell(30, 7, f"{r['LIMITE']:,}", 1, 0, 'C')
        bg = (220, 53, 69) if r['COLOR'] == "ROJO" else (255, 193, 7) if r['COLOR'] == "AMARILLO" else (40, 167, 69)
        pdf.set_fill_color(*bg); pdf.set_text_color(255 if r['COLOR'] != "AMARILLO" else 0); pdf.set_font("Arial", 'B', 8)
        pdf.cell(70, 7, r['ESTADO'], 1, 1, 'C', True)

    if not df_ab.empty:
        pdf.add_page()
        pdf.set_font("Arial", 'B', 14); pdf.set_text_color(192, 0, 0); pdf.cell(0, 8, "MANTENIMIENTOS ABIERTOS (Pendientes de cierre)", ln=True)
        pdf.ln(3); pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(192, 0, 0); pdf.set_text_color(255, 255, 255)
        pdf.cell(30, 8, "Cliente", 1, 0, 'C', True); pdf.cell(120, 8, "Pieza Reportada", 1, 0, 'C', True)
        pdf.cell(30, 8, "Tipo", 1, 0, 'C', True); pdf.cell(40, 8, "Apertura", 1, 1, 'C', True)
        pdf.set_font("Arial", '', 8); pdf.set_text_color(0, 0, 0); pdf.set_fill_color(255, 240, 240)
        for _, r in df_ab.iterrows():
            pdf.cell(30, 7, str(r['CLIENTE']), 1, 0, 'C', True); pdf.cell(120, 7, str(r['PIEZA_REPORTE']), 1, 0, 'L', True)
            pdf.cell(30, 7, str(r['TIPO']), 1, 0, 'C', True); pdf.cell(40, 7, str(r['FECHA_APERTURA']), 1, 1, 'C', True)

    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name); b = open(buf.name, "rb").read(); os.remove(buf.name)
    return b

# ==========================================
# 6. INTERFAZ STREAMLIT
# ==========================================
if st.button("🔄 Actualizar Todo (Limpiar Caché)", use_container_width=True):
    st.cache_data.clear(); st.rerun()

with st.spinner("Sincronizando fuentes de datos..."):
    df_cat, df_prod, df_forms = load_all_sources()

if not df_cat.empty:
    if st.button("⚙️ Procesar Estado de Matrices", type="primary", use_container_width=True):
        df_res, df_ab = procesar_logica_golpes(df_cat, df_prod, df_forms)
        st.session_state['res_final'], st.session_state['ab_final'] = df_res, df_ab

if 'res_final' in st.session_state:
    df, df_ab = st.session_state['res_final'], st.session_state['ab_final']
    st.write("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Críticas 🔴", len(df[df['COLOR']=="ROJO"]))
    c2.metric("Alerta 🟡", len(df[df['COLOR']=="AMARILLO"]))
    c3.metric("Analizadas ⚙️", len(df))
    c4.metric("Abiertos ⚠️", len(df_ab))
    
    if not df_ab.empty:
        st.error("⚠️ Se detectaron Mantenimientos ABIERTOS:")
        st.dataframe(df_ab, use_container_width=True)
    
    pdf_bytes = build_pdf_data(df, df_ab)
    st.download_button("📥 Descargar Reporte PDF Completo", pdf_bytes, "Reporte_Golpes.pdf", "application/pdf", use_container_width=True)

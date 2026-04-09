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
st.write("<p style='text-align: center;'>Cruce de Catálogo (RH) + Producción SQL + Últimos Mantenimientos (Forms).</p>", unsafe_allow_html=True)
st.divider()

# ==========================================
# 2. ENLACES DE DATOS (Google Sheets & Forms)
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
    """Extrae la secuencia numérica más larga para cruzar datos entre plataformas."""
    if pd.isna(texto): return ""
    val = str(texto).upper()
    # Ignorar operaciones que confunden la búsqueda
    val = re.sub(r'-?OP\d+', '', val)
    # Buscar secuencias de al menos 5 números seguidos
    matches = re.findall(r'\d{5,}', val)
    return max(matches, key=len) if matches else re.sub(r'[^A-Z0-9]', '', val)

@st.cache_data(ttl=300)
def load_all_sources():
    # --- 1. CARGAR CATÁLOGO (Saltando 2 filas) ---
    try:
        df_cat = pd.read_csv(URL_CATALOGO, skiprows=2).dropna(how='all')
        df_cat.columns = df_cat.columns.astype(str).str.upper().str.strip()
        df_cat.columns = df_cat.columns.str.replace(r'\s+', ' ', regex=True)
    except Exception as e:
        st.error(f"Error cargando Catálogo: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # --- 2. CARGAR RESPUESTAS DE GOOGLE FORMS (FILTRO ESTRICTO) ---
    def fetch_forms(url, tipo_mant):
        try:
            df = pd.read_csv(url)
            cols = [str(c).upper().strip() for c in df.columns]
            df.columns = cols
            
            # Buscar FECHA (Priorizar la manual del operador)
            col_f = next((c for c in cols if c == 'FECHA'), None)
            if not col_f:
                col_f = next((c for c in cols if 'MARCA TEMPORAL' in c), None)
            if not col_f: return pd.DataFrame()
            
            # COLUMNAS DE PIEZAS: Solo buscamos las listas desplegables reales
            cols_pieza = [c for c in cols if c.startswith('PIEZAS') or 'NUMERO DE PIEZA' in c or 'NÚMERO DE PIEZA' in c]
            
            # COLUMNAS DE TERMINACIÓN
            cols_term = [c for c in cols if 'TERMINADO' in c or 'TERMINO' in c]
            
            registros = []
            for _, row in df.iterrows():
                fecha = pd.to_datetime(row[col_f], dayfirst=True, errors='coerce')
                if pd.isna(fecha): continue
                
                # Encontrar la pieza en las columnas correctas
                pieza_raw = ""
                for cp in cols_pieza:
                    val = clean_str(row[cp])
                    if val and val not in ['NAN', 'NONE', '-', '0', 'N/A', 'NO APLICA', '']:
                        pieza_raw = val
                        break 
                
                if not pieza_raw: continue
                
                pieza_key = get_match_key(pieza_raw)
                if not pieza_key: continue
                
                # Verificar si el trabajo está abierto
                abierto = 'NO'
                if cols_term:
                    for ct in cols_term:
                        val_term = clean_str(row[ct])
                        if val_term in ['NO', 'FALSO', 'PENDIENTE'] or 'NO' in val_term.split():
                            abierto = 'SI'
                            break
                
                registros.append({
                    'FECHA_DT': fecha,
                    'PIEZA_KEY': pieza_key,
                    'TIPO_MANT': tipo_mant,
                    'ABIERTO': abierto,
                    'PIEZA_RAW': pieza_raw
                })
                
            return pd.DataFrame(registros)
        except Exception as e: 
            return pd.DataFrame()

    df_forms_mants = pd.concat([fetch_forms(URL_FORMS_PREV, "PREV"), fetch_forms(URL_FORMS_CORR, "CORR")])

    # --- 3. SQL SERVER (SOLO PARA PRODUCCIÓN) ---
    try:
        conn = st.connection("wii_bi", type="sql")
        q_prod = """
            SELECT pr.Code as PIEZA, CAST(p.Date as DATE) as FECHA, SUM(p.Good + p.Rework) as GOLPES 
            FROM PROD_D_01 p JOIN PRODUCT pr ON p.ProductId = pr.ProductId 
            WHERE p.Date >= '2023-01-01' GROUP BY pr.Code, CAST(p.Date as DATE)
        """
        df_sql_prod = conn.query(q_prod)
        df_sql_prod['PIEZA_KEY'] = df_sql_prod['PIEZA'].apply(get_match_key)
        df_sql_prod['FECHA'] = pd.to_datetime(df_sql_prod['FECHA'])
    except Exception as e:
        st.error(f"Error SQL: {e}")
        df_sql_prod = pd.DataFrame()

    return df_cat, df_sql_prod, df_forms_mants

# ==========================================
# 4. MOTOR DE CRUCE Y CÁLCULO
# ==========================================
def procesar_logica_golpes(df_cat, df_prod, df_forms):
    resultados = []
    abiertos = []
    
    anio_actual = pd.to_datetime("today").year
    inicio_anio = pd.to_datetime(f"{anio_actual}-01-01")

    # 1. RECOPILAR TODO LO ABIERTO DE LOS FORMS
    if not df_forms.empty:
        df_solo_abiertos = df_forms[df_forms['ABIERTO'] == 'SI']
        for _, ab_row in df_solo_abiertos.iterrows():
            match_cat = df_cat[df_cat['RH'].apply(lambda x: get_match_key(clean_str(x))) == ab_row['PIEZA_KEY']]
            cliente_txt = clean_str(match_cat.iloc[0]['CLIENTE']) if not match_cat.empty and 'CLIENTE' in match_cat.columns else "Externo/LH"
            pieza_txt = clean_str(match_cat.iloc[0]['RH']) if not match_cat.empty and 'RH' in match_cat.columns else "-"
            
            abiertos.append({
                'CLIENTE': cliente_txt,
                'PIEZA_CATALOGO': pieza_txt,
                'PIEZA_REPORTE': str(ab_row['PIEZA_RAW']),
                'TIPO_MANT': str(ab_row['TIPO_MANT']),
                'FECHA_APERTURA': ab_row['FECHA_DT'].strftime('%d/%m/%Y')
            })

    # 2. CALCULAR GOLPES PARA EL LISTADO RH
    for _, row in df_cat.iterrows():
        pieza_rh = clean_str(row.get('RH', ''))
        if not pieza_rh or pieza_rh in ['NAN', '-']: continue
        
        pieza_key = get_match_key(pieza_rh)
        cliente = clean_str(row.get('CLIENTE', '-'))
        
        golpes_excel = pd.to_numeric(row.get('GOLPES', 0), errors='coerce')
        golpes_excel = 0 if pd.isna(golpes_excel) else int(golpes_excel)
        
        fecha_excel = pd.to_datetime(row.get('ULTIMO MANTENIMIENTO'), dayfirst=True, errors='coerce')

        fecha_form = pd.NaT
        if not df_forms.empty:
            # Solo consideramos mantenimientos CERRADOS para resetear el contador de golpes
            match_form = df_forms[(df_forms['PIEZA_KEY'] == pieza_key) & (df_forms['ABIERTO'] == 'NO')]
            if not match_form.empty:
                fecha_form = match_form['FECHA_DT'].max()

        golpes_finales = 0
        if pd.notna(fecha_form) and (pd.isna(fecha_excel) or fecha_form > fecha_excel):
            fecha_final = fecha_form
            prod = df_prod[(df_prod['PIEZA_KEY'] == pieza_key) & (df_prod['FECHA'] >= fecha_final)]
            golpes_finales = int(prod['GOLPES'].sum())
        else:
            fecha_final = fecha_excel
            prod = df_prod[(df_prod['PIEZA_KEY'] == pieza_key) & (df_prod['FECHA'] >= inicio_anio)]
            golpes_finales = golpes_excel + int(prod['GOLPES'].sum())

        limite = 20000
        color = "VERDE"
        if golpes_finales >= limite: color = "ROJO"
        elif golpes_finales >= (limite * 0.8): color = "AMARILLO"

        resultados.append({
            'CLIENTE': cliente,
            'PIEZA': pieza_rh,
            'ULT_MANT': fecha_final.strftime('%d/%m/%Y') if pd.notna(fecha_final) else "-",
            'GOLPES': golpes_finales,
            'LIMITE': limite,
            'COLOR': color,
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
        self.set_font("Arial", 'I', 9)
        self.set_text_color(100, 100, 100)
        hora_arg = datetime.utcnow() - timedelta(hours=3)
        self.cell(0, 5, f"Reporte generado el: {hora_arg.strftime('%d/%m/%Y %H:%M')}", border=0, ln=True, align='C')
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, "C")

def build_pdf_data(df, df_abiertos):
    pdf = PDFGolpes(orientation='L', unit='mm', format='A4')
    
    # --- HOJA 1: MATRICES Y GOLPES ---
    pdf.add_page()
    pdf.set_font("Arial", 'B', 9)
    pdf.set_fill_color(31, 73, 125)
    pdf.set_text_color(255, 255, 255)
    
    cols = [("Cliente", 25), ("Pieza RH (Catálogo)", 90), ("Ult. Mant.", 25), ("Golpes Acum.", 30), ("Limite", 30), ("Estado Actual", 70)]
    for c in cols: pdf.cell(c[1], 8, c[0], 1, 0, 'C', True)
    pdf.ln()

    for _, r in df.iterrows():
        pdf.set_fill_color(255, 255, 255); pdf.set_text_color(0, 0, 0); pdf.set_font("Arial", '', 8)
        
        pdf.cell(25, 7, r['CLIENTE'], 1, 0, 'C')
        pdf.cell(90, 7, r['PIEZA'][:50], 1, 0, 'L')
        pdf.cell(25, 7, r['ULT_MANT'], 1, 0, 'C')
        pdf.set_font("Arial", 'B', 8)
        pdf.cell(30, 7, f"{r['GOLPES']:,}", 1, 0, 'C')
        pdf.set_font("Arial", '', 8)
        pdf.cell(30, 7, f"{r['LIMITE']:,}", 1, 0, 'C')
        
        # RESALTAR SOLO ESTA CELDA
        bg = (220, 53, 69) if r['COLOR'] == "ROJO" else (255, 193, 7) if r['COLOR'] == "AMARILLO" else (40, 167, 69)
        txt = (255, 255, 255) if r['COLOR'] in ["ROJO", "VERDE"] else (0, 0, 0)
        
        pdf.set_fill_color(*bg); pdf.set_text_color(*txt); pdf.set_font("Arial", 'B', 8)
        pdf.cell(70, 7, r['ESTADO'], 1, 1, 'C', True)

    # --- HOJA 2: ABIERTOS ---
    if not df_abiertos.empty:
        pdf.add_page()
        pdf.set_font("Arial", 'B', 14); pdf.set_text_color(192, 0, 0)
        pdf.cell(0, 8, "MANTENIMIENTOS ABIERTOS (Pendientes en Google Forms)", ln=True)
        pdf.ln(3)
        
        pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(192, 0, 0); pdf.set_text_color(255, 255, 255)
        
        pdf.cell(25, 8, "Cliente", 1, 0, 'C', True)
        pdf.cell(80, 8, "Pieza (Estimada)", 1, 0, 'C', True)
        pdf.cell(100, 8, "Reportado como", 1, 0, 'C', True)
        pdf.cell(30, 8, "Tipo", 1, 0, 'C', True)
        pdf.cell(35, 8, "Apertura", 1, 1, 'C', True)
        
        pdf.set_font("Arial", '', 8); pdf.set_text_color(0, 0, 0); pdf.set_fill_color(255, 240, 240)
        
        for _, r in df_abiertos.iterrows():
            pdf.cell(25, 7, str(r['CLIENTE']), 1, 0, 'C', True)
            pdf.cell(80, 7, str(r['PIEZA_CATALOGO'])[:45], 1, 0, 'L', True)
            pdf.cell(100, 7, str(r['PIEZA_REPORTE'])[:55], 1, 0, 'L', True)
            pdf.cell(30, 7, str(r['TIPO_MANT']), 1, 0, 'C', True)
            pdf.cell(35, 7, str(r['FECHA_APERTURA']), 1, 1, 'C', True)

    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name); b = open(buf.name, "rb").read(); os.remove(buf.name)
    return b

# ==========================================
# 6. INTERFAZ STREAMLIT
# ==========================================
if st.button("🔄 Actualizar Todo (Limpiar Caché)", use_container_width=True):
    st.cache_data.clear(); st.rerun()

with st.spinner("Sincronizando Excel, Google Forms y SQL Server..."):
    try:
        df_cat, df_prod, df_forms = load_all_sources()
        datos_listos = not df_cat.empty
    except Exception as e:
        st.error(f"🚨 Error: {e}"); datos_listos = False

if datos_listos:
    st.success("Sincronización Exitosa.")
    if st.button("⚙️ Procesar Estado de Matrices", type="primary", use_container_width=True):
        with st.spinner("Calculando golpes y revisando pendientes..."):
            df_res, df_ab = procesar_logica_golpes(df_cat, df_prod, df_forms)
            st.session_state['res_final'] = df_res
            st.session_state['ab_final'] = df_ab

if 'res_final' in st.session_state:
    df, df_ab = st.session_state['res_final'], st.session_state['ab_final']
    st.write("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Críticas 🔴", len(df[df['COLOR']=="ROJO"]))
    c2.metric("Alerta 🟡", len(df[df['COLOR']=="AMARILLO"]))
    c3.metric("Total ⚙️", len(df))
    c4.metric("Abiertos ⚠️", len(df_ab))
    
    pdf_bytes = build_pdf_data(df, df_ab)
    st.download_button("📥 Descargar Reporte PDF Completo", pdf_bytes, "Reporte_Golpes_Matrices.pdf", "application/pdf", use_container_width=True)

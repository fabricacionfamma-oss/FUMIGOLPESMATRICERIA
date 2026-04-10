import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import os
import re
from difflib import SequenceMatcher
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

st.markdown('<div class="header-style">⚙️ Sistema de Diagnóstico y Control - Fumiscor</div>', unsafe_allow_html=True)
st.write("<p style='text-align: center;'>Cruce automático de Catálogo, Base SQL de Producción y Formularios de Mantenimiento.</p>", unsafe_allow_html=True)
st.divider()

# ==========================================
# 2. ENLACES DE DATOS (FUMISCOR)
# ==========================================
URL_CATALOGO = "https://docs.google.com/spreadsheets/d/198KjQWZwfvvWwq1q1N1zv1cgzkot2hhGbwQvbi9_zFQ/export?format=csv&gid=818188145"
URL_FORMS_PREV = "https://docs.google.com/spreadsheets/d/1VqsPNhAlT1kPCltbMWsbkZNFBKdwZRFM5RAmnRV0v3c/export?format=csv&gid=1603203990"
URL_FORMS_CORR = "https://docs.google.com/spreadsheets/d/1bL_tnlSXGO_t9tKnhIHT5pZ3DAxivbiq2tFETVxBaVI/export?format=csv&gid=1507213893"

# ==========================================
# 3. FUNCIONES DE LIMPIEZA Y COINCIDENCIA
# ==========================================
def clean_str(val):
    if pd.isna(val): return ""
    return str(val).strip().upper()

def get_best_match(texto, lista_candidatos, umbral=0.5):
    """Encuentra la mejor coincidencia basada en caracteres compartidos."""
    if pd.isna(texto) or not str(texto).strip(): return ""
    val = clean_str(texto)
    
    mejor_coincidencia = val
    mejor_puntaje = 0.0
    
    for candidato in lista_candidatos:
        cand_str = clean_str(candidato)
        if not cand_str: continue
        puntaje = SequenceMatcher(None, val, cand_str).ratio()
        if puntaje > mejor_puntaje:
            mejor_puntaje = puntaje
            mejor_coincidencia = cand_str
            
    if mejor_puntaje >= umbral:
        return mejor_coincidencia
    return val

@st.cache_data(ttl=60)
def load_all_sources():
    # --- A. CARGAR CATÁLOGO ---
    try:
        df_cat_raw = pd.read_csv(URL_CATALOGO)
        header_idx = -1
        for i, row in df_cat_raw.iterrows():
            row_vals = " ".join([str(x).upper() for x in row.values])
            if 'RH' in row_vals and 'CLIENTE' in row_vals:
                header_idx = i
                break
                
        if header_idx != -1:
            df_cat = pd.read_csv(URL_CATALOGO, skiprows=header_idx + 1).dropna(how='all')
        else:
            df_cat = pd.read_csv(URL_CATALOGO, skiprows=2).dropna(how='all')

        df_cat.columns = [str(c).upper().strip() for c in df_cat.columns]
        
        if 'RH' in df_cat.columns:
            df_cat = df_cat.dropna(subset=['RH'])
            df_cat = df_cat[df_cat['RH'].astype(str).str.strip() != '']
            df_cat['PIEZA_KEY'] = df_cat['RH'].apply(clean_str)
        else:
            st.error("❌ No se encontró la columna 'RH' en el Catálogo.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error cargando Catálogo: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    catalogo_piezas = df_cat['PIEZA_KEY'].unique().tolist()

    # --- B. CARGAR FORMS ---
    def fetch_forms(url, tipo_mant):
        try:
            df_raw = pd.read_csv(url)
            if df_raw.empty: return pd.DataFrame()

            header_idx = -1
            for i, row in df_raw.iterrows():
                row_vals = " ".join([str(x).upper() for x in row.values])
                if 'MARCA TEMPORAL' in row_vals or 'FECHA' in row_vals:
                    header_idx = i
                    break
            
            if header_idx != -1:
                df_raw = pd.read_csv(url, skiprows=header_idx + 1)
            
            df_raw.columns = [str(c).upper().strip() for c in df_raw.columns]
            
            col_f = next((c for c in df_raw.columns if 'FECHA' in c or 'MARCA TEMPORAL' in c), None)
            palabras_clave_pieza = ['PIEZA', 'RH', 'LH', 'MATRIZ', 'CÓDIGO']
            cols_pieza = [c for c in df_raw.columns if any(k in c for k in palabras_clave_pieza)]
            cols_term = [c for c in df_raw.columns if 'TERMINADO' in c or 'TERMINO' in c or 'ESTADO' in c]
            
            if not col_f or not cols_pieza:
                return pd.DataFrame()

            registros = []
            for _, row in df_raw.iterrows():
                fecha = pd.to_datetime(row.get(col_f), dayfirst=True, errors='coerce') if col_f else pd.NaT
                if pd.isna(fecha): continue
                
                pieza_raw = ""
                for cp in cols_pieza:
                    val = clean_str(row.get(cp))
                    if val and val not in ['NAN', 'NONE', '-', '0', 'N/A', '']:
                        pieza_raw = val
                        break 
                
                if not pieza_raw: continue
                
                # REGLA: Si alguna columna dice SI, todo el registro es SI (cerrado). Sino, NO (abierto)
                terminado = 'NO'
                for ct in cols_term:
                    val_t = clean_str(row.get(ct))
                    if val_t in ['SI', 'SÍ', 'VERDADERO']:
                        terminado = 'SI'
                        break
                
                pieza_key = get_best_match(pieza_raw, catalogo_piezas)

                registros.append({
                    'FECHA_DT': fecha, 'TIPO_MANT': tipo_mant, 'TERMINADO': terminado,
                    'PIEZA_RAW': pieza_raw, 'PIEZA_KEY': pieza_key
                })
            return pd.DataFrame(registros)
        except Exception as e:
            return pd.DataFrame()

    df_prev = fetch_forms(URL_FORMS_PREV, "PREV")
    df_corr = fetch_forms(URL_FORMS_CORR, "CORR")
    df_forms_all = pd.concat([df_prev, df_corr], ignore_index=True) if not df_prev.empty or not df_corr.empty else pd.DataFrame()

    # --- C. SQL (PRODUCCIÓN) ---
    try:
        conn = st.connection("wii_bi", type="sql")
        q = "SELECT pr.Code as PIEZA, CAST(p.Date as DATE) as FECHA, SUM(p.Good + p.Rework) as GOLPES FROM PROD_D_01 p JOIN PRODUCT pr ON p.ProductId = pr.ProductId WHERE p.Date >= '2023-01-01' GROUP BY pr.Code, CAST(p.Date as DATE)"
        df_sql = conn.query(q)
        df_sql['FECHA'] = pd.to_datetime(df_sql['FECHA'])
        
        piezas_unicas_sql = df_sql['PIEZA'].unique()
        mapeo_piezas = {p: get_best_match(p, catalogo_piezas) for p in piezas_unicas_sql}
        df_sql['PIEZA_KEY'] = df_sql['PIEZA'].map(mapeo_piezas)
    except: 
        df_sql = pd.DataFrame()

    return df_cat, df_sql, df_forms_all

# ==========================================
# 4. LÓGICA DE PROCESAMIENTO
# ==========================================
def procesar_datos(df_cat, df_sql, df_forms):
    res_semaforo = []
    res_abiertos = []
    hoy = datetime.now()
    inicio_anio = pd.to_datetime(f"{hoy.year}-01-01")

    for _, row in df_cat.iterrows():
        p_key = row['PIEZA_KEY']
        if not row['RH'] or row['RH'] == '-': continue
        
        f_excel = pd.to_datetime(row.get('ULTIMO MANTENIMIENTO'), dayfirst=True, errors='coerce')
        g_base = pd.to_numeric(row.get('GOLPES'), errors='coerce')
        g_base = g_base if pd.notna(g_base) else 0

        # Lógica de Mantenimientos por Pieza
        f_prev = pd.NaT
        f_corr = pd.NaT
        tiene_abierto = False
        fecha_abierto = pd.NaT
        tipo_abierto = ""

        if not df_forms.empty:
            match_f = df_forms[df_forms['PIEZA_KEY'] == p_key].copy()
            if not match_f.empty:
                match_f = match_f.sort_values('FECHA_DT')
                
                # REGLA: Comprobar el ÚLTIMO registro cronológico
                last_record = match_f.iloc[-1]
                if last_record['TERMINADO'] == 'NO':
                    tiene_abierto = True
                    fecha_abierto = last_record['FECHA_DT']
                    tipo_abierto = last_record['TIPO_MANT']

                # Obtener la fecha de los cerrados
                cerrados = match_f[match_f['TERMINADO'] == 'SI']
                if not cerrados.empty:
                    max_p = cerrados[cerrados['TIPO_MANT'] == 'PREV']['FECHA_DT'].max()
                    max_c = cerrados[cerrados['TIPO_MANT'] == 'CORR']['FECHA_DT'].max()
                    if pd.notna(max_p): f_prev = max_p
                    if pd.notna(max_c): f_corr = max_c

        # Establecer la fecha de corte para sumar golpes
        fechas_validas = [f for f in [f_prev, f_corr, f_excel] if pd.notna(f)]
        f_final = max(fechas_validas) if fechas_validas else pd.NaT

        if pd.notna(f_final):
            if pd.notna(f_excel) and f_final == f_excel:
                prod = df_sql[(df_sql['PIEZA_KEY'] == p_key) & (df_sql['FECHA'] >= inicio_anio)] if not df_sql.empty else pd.DataFrame()
                g_total = int(g_base) + (int(prod['GOLPES'].sum()) if not prod.empty else 0)
            else:
                prod = df_sql[(df_sql['PIEZA_KEY'] == p_key) & (df_sql['FECHA'] >= f_final)] if not df_sql.empty else pd.DataFrame()
                g_total = int(prod['GOLPES'].sum()) if not prod.empty else 0
        else:
            prod = df_sql[(df_sql['PIEZA_KEY'] == p_key) & (df_sql['FECHA'] >= inicio_anio)] if not df_sql.empty else pd.DataFrame()
            g_total = int(g_base) + (int(prod['GOLPES'].sum()) if not prod.empty else 0)

        limite = 20000
        color = "ROJO" if g_total >= limite else "AMARILLO" if g_total >= (limite*0.8) else "VERDE"
        estado = "MANT. REQUERIDO" if color == "ROJO" else "ALERTA PREVENTIVO" if color == "AMARILLO" else "OK"
        
        # Guardamos en formato compatible para la generación PDF
        res_semaforo.append({
            'CLIENTE': row.get('CLIENTE', '-'), 'PIEZA': row['RH'], 'OP': '-', 'TIPO': '-',
            'ULT_PREV': f_prev.strftime('%d/%m/%y') if pd.notna(f_prev) else "-",
            'ULT_CORR': f_corr.strftime('%d/%m/%y') if pd.notna(f_corr) else "-",
            'GOLPES': g_total, 'LIMITE': limite, 'ESTADO': estado, 'COLOR': color
        })

        if tiene_abierto:
            res_abiertos.append({
                'CLIENTE': row.get('CLIENTE', '-'), 'PIEZA': row['RH'], 'OP': '-', 'TIPO': '-',
                'TIPO_MANT_ABIERTO': tipo_abierto, 'FECHA_APERTURA': fecha_abierto.strftime('%d/%m/%Y')
            })

    return pd.DataFrame(res_semaforo), pd.DataFrame(res_abiertos)

# ==========================================
# 5. GENERACIÓN DEL PDF (FPDF Y PLOTLY)
# ==========================================
class PDFGolpes(FPDF):
    def header(self):
        self.set_font("Arial", 'B', 15)
        self.set_text_color(31, 73, 125)
        self.cell(0, 10, "Resumen de Golpes", border=0, ln=True, align='C')
        self.set_font("Arial", 'I', 9)
        self.set_text_color(100, 100, 100)
        hora_arg = datetime.utcnow() - timedelta(hours=3)
        self.cell(0, 5, f"Calculo generado el: {hora_arg.strftime('%d/%m/%Y %H:%M')}", border=0, ln=True, align='C')
        self.ln(3)
        
    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, "C")

class PDFResumen(FPDF):
    def header(self):
        self.set_font("Arial", 'B', 15)
        self.set_text_color(31, 73, 125)
        self.cell(0, 10, "Estado de los Mantenimientos de Matrices Fumiscor", border=0, ln=True, align='C')
        self.set_font("Arial", 'I', 9)
        self.set_text_color(100, 100, 100)
        hora_arg = datetime.utcnow() - timedelta(hours=3)
        self.cell(0, 5, f"Generado el: {hora_arg.strftime('%d/%m/%Y %H:%M')}", border=0, ln=True, align='C')
        self.ln(3)
        
    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, "C")

def build_pdf_main(df_resultados, df_abiertos):
    pdf = PDFGolpes(orientation='L', unit='mm', format='A4')
    
    # --- HOJA 1: DETALLE ---
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    pdf.set_font("Arial", 'B', 9)
    pdf.set_fill_color(31, 73, 125)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(15, 8, "Cliente", 1, 0, 'C', fill=True)
    pdf.cell(70, 8, "Codigo Pieza", 1, 0, 'C', fill=True)
    pdf.cell(12, 8, "OP", 1, 0, 'C', fill=True)
    pdf.cell(12, 8, "Tipo", 1, 0, 'C', fill=True)
    pdf.cell(22, 8, "Ult. Prev.", 1, 0, 'C', fill=True)
    pdf.cell(22, 8, "Ult. Corr.", 1, 0, 'C', fill=True)
    pdf.cell(26, 8, "Golpes Ac.", 1, 0, 'C', fill=True)
    pdf.cell(26, 8, "Limite M.", 1, 0, 'C', fill=True)
    pdf.cell(72, 8, "Estado / Accion", 1, 1, 'C', fill=True)
    
    pdf.set_font("Arial", '', 8)
    for _, row in df_resultados.iterrows():
        bg = (255, 180, 180) if row['COLOR'] == "ROJO" else (255, 240, 180) if row['COLOR'] == "AMARILLO" else (198, 239, 206)
        txt = (180, 0, 0) if row['COLOR'] == "ROJO" else (150, 100, 0) if row['COLOR'] == "AMARILLO" else (0, 100, 0)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(15, 7, str(row['CLIENTE']), 1, 0, 'C')
        pdf.cell(70, 7, str(row['PIEZA'])[:45], 1, 0, 'L')
        pdf.cell(12, 7, str(row['OP']), 1, 0, 'C')
        pdf.cell(12, 7, str(row['TIPO']), 1, 0, 'C')
        pdf.cell(22, 7, str(row['ULT_PREV']), 1, 0, 'C')
        pdf.cell(22, 7, str(row['ULT_CORR']), 1, 0, 'C')
        pdf.set_fill_color(*bg); pdf.set_text_color(*txt); pdf.set_font("Arial", 'B', 8)
        pdf.cell(26, 7, f"{row['GOLPES']:,}", 1, 0, 'C', fill=True)
        pdf.set_text_color(0, 0, 0); pdf.set_font("Arial", '', 8)
        pdf.cell(26, 7, f"{row['LIMITE']:,}", 1, 0, 'C')
        pdf.set_fill_color(*bg); pdf.set_text_color(*txt); pdf.set_font("Arial", 'B', 8)
        pdf.cell(72, 7, str(row['ESTADO']), 1, 1, 'C', fill=True)

    # --- HOJA 2: ABIERTOS ---
    if not df_abiertos.empty:
        pdf.add_page()
        pdf.set_font("Arial", 'B', 12); pdf.set_text_color(192, 0, 0)
        pdf.cell(0, 8, "MANTENIMIENTOS ABIERTOS (Pendientes de Cierre)", ln=True)
        pdf.ln(3)
        pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(192, 0, 0); pdf.set_text_color(255, 255, 255)
        pdf.cell(25, 8, "Cliente", 1, 0, 'C', fill=True)
        pdf.cell(90, 8, "Pieza", 1, 0, 'C', fill=True)
        pdf.cell(15, 8, "OP", 1, 0, 'C', fill=True)
        pdf.cell(35, 8, "Tipo Mant.", 1, 0, 'C', fill=True)
        pdf.cell(35, 8, "Fecha Apertura", 1, 1, 'C', fill=True)
        pdf.set_font("Arial", '', 8); pdf.set_text_color(0, 0, 0)
        for _, r in df_abiertos.iterrows():
            pdf.cell(25, 7, str(r['CLIENTE']), 1, 0, 'C')
            pdf.cell(90, 7, str(r['PIEZA']), 1, 0, 'L')
            pdf.cell(15, 7, str(r['OP']), 1, 0, 'C')
            pdf.cell(35, 7, str(r['TIPO_MANT_ABIERTO']), 1, 0, 'C')
            pdf.cell(35, 7, str(r['FECHA_APERTURA']), 1, 1, 'C')

    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name)
    b = open(buf.name, "rb").read()
    os.remove(buf.name)
    return b

def build_pdf_resumen(df_resultados):
    pdf = PDFResumen(orientation='L', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.ln(2)
    
    resumen_data = []
    total_gen = len(df_resultados)
    total_ok = len(df_resultados[df_resultados['COLOR'] == 'VERDE'])
    total_nok = total_gen - total_ok
    
    for c in sorted([x for x in df_resultados['CLIENTE'].unique() if x != "-"]):
        df_c = df_resultados[df_resultados['CLIENTE'] == c]
        tot = len(df_c)
        ok = len(df_c[df_c['COLOR'] == 'VERDE'])
        nok = tot - ok
        if tot > 0:
            resumen_data.append({
                'CLIENTE': c, 'TOT': tot, 'OK': ok, 'NOK': nok, 
                'POK': f"{int(round(ok/tot*100))}%", 
                'PNOK': f"{int(round(nok/tot*100))}%"
            })

    # --- TABLA RESUMEN ---
    pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(31, 73, 125); pdf.set_text_color(255, 255, 255)
    mx = 43.5; pdf.set_x(mx)
    pdf.cell(35, 6, "CLIENTE", 1, 0, 'C', fill=True)
    pdf.cell(25, 6, "TOTAL PIEZAS", 1, 0, 'C', fill=True)
    pdf.cell(35, 6, "OK / CON PREV.", 1, 0, 'C', fill=True)
    pdf.cell(35, 6, "ALERTA / VENCIDO", 1, 0, 'C', fill=True)
    pdf.cell(40, 6, "% OK", 1, 0, 'C', fill=True)
    pdf.cell(40, 6, "% NO OK", 1, 1, 'C', fill=True)
    
    pdf.set_font("Arial", '', 9); pdf.set_text_color(0, 0, 0)
    for r in resumen_data:
        pdf.set_x(mx)
        pdf.cell(35, 6, r['CLIENTE'], 1, 0, 'C')
        pdf.cell(25, 6, str(r['TOT']), 1, 0, 'C')
        pdf.cell(35, 6, str(r['OK']), 1, 0, 'C')
        pdf.cell(35, 6, str(r['NOK']), 1, 0, 'C')
        pdf.cell(40, 6, r['POK'], 1, 0, 'C')
        pdf.cell(40, 6, r['PNOK'], 1, 1, 'C')
        
    pdf.set_x(mx); pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(220, 220, 220)
    pdf.cell(35, 6, "TOTAL", 1, 0, 'C', fill=True)
    pdf.cell(25, 6, str(total_gen), 1, 0, 'C', fill=True)
    pdf.cell(35, 6, str(total_ok), 1, 0, 'C', fill=True)
    pdf.cell(35, 6, str(total_nok), 1, 0, 'C', fill=True)
    pdf.cell(40, 6, f"{int(round(total_ok/total_gen*100))}%" if total_gen > 0 else "0%", 1, 0, 'C', fill=True)
    pdf.cell(40, 6, f"{int(round(total_nok/total_gen*100))}%" if total_gen > 0 else "0%", 1, 1, 'C', fill=True)
    
    # --- GRÁFICOS (PLOTLY) ---
    if len(resumen_data) > 0:
        pdf.ln(5)
        y_charts = pdf.get_y()
        
        fig_gen = go.Figure(data=[go.Pie(
            labels=['EN REGLA', 'VENCIDO/ALERTA'], 
            values=[total_ok, total_nok], 
            marker_colors=['#2ca02c', '#d62728']
        )])
        fig_gen.update_traces(textposition='inside', textinfo='percent+label', showlegend=False)
        fig_gen.update_layout(title_text="Estado General (Total)", title_x=0.5, margin=dict(t=40, b=10, l=10, r=10), height=300, width=300)

        fig_cli = make_subplots(
            rows=1, cols=len(resumen_data), 
            specs=[[{'type':'domain'}] * len(resumen_data)], 
            subplot_titles=[r['CLIENTE'] for r in resumen_data]
        )
        for i, r in enumerate(resumen_data):
            fig_cli.add_trace(go.Pie(
                labels=['EN REGLA', 'VENCIDO/ALERTA'], 
                values=[r['OK'], r['NOK']], 
                marker_colors=['#2ca02c', '#d62728']
            ), 1, i + 1)
        
        fig_cli.update_traces(textposition='inside', textinfo='percent')
        fig_cli.update_layout(
            title_text="Desglose por Cliente", title_x=0.5, 
            showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5), 
            margin=dict(t=40, b=40, l=10, r=10), height=300, width=700
        )
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_gen:
            fig_gen.write_image(tmp_gen.name, engine="kaleido")
            pdf.image(tmp_gen.name, x=15, y=y_charts, w=70) 
            os.remove(tmp_gen.name)
            
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_cli:
            fig_cli.write_image(tmp_cli.name, engine="kaleido")
            pdf.image(tmp_cli.name, x=90, y=y_charts, w=190) 
            os.remove(tmp_cli.name)
    
    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name)
    b = open(buf.name, "rb").read()
    os.remove(buf.name)
    return b

# ==========================================
# 6. INTERFAZ Y BOTONES DE DESCARGA
# ==========================================
if st.button("🔄 Sincronizar Bases y Limpiar Caché", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

with st.spinner("Conectando con Google Sheets y SQL..."):
    df_cat, df_sql, df_forms = load_all_sources()

if not df_cat.empty:
    with st.expander("🛠️ PANEL DE DIAGNÓSTICO INTERNO"):
        st.write(f"Total filas en Catálogo: {len(df_cat)} | Total filas SQL: {len(df_sql)} | Total forms: {len(df_forms)}")
        
    if st.button("⚙️ Procesar Datos de Matrices y Generar PDFs", use_container_width=True, type="primary"):
        with st.spinner("Calculando estado de matrices y renderizando documentos..."):
            df_res, df_abiertos = procesar_datos(df_cat, df_sql, df_forms)
            st.session_state['df_res'] = df_res
            st.session_state['df_abiertos'] = df_abiertos

    if 'df_res' in st.session_state and not st.session_state['df_res'].empty:
        df_res = st.session_state['df_res']
        df_abiertos = st.session_state['df_abiertos']
        
        rojos = len(df_res[df_res['COLOR']=='ROJO'])
        amarillos = len(df_res[df_res['COLOR']=='AMARILLO'])
        verdes = len(df_res[df_res['COLOR']=='VERDE'])
        
        st.write("---")
        st.write(f"**Resumen de la corrida:** 🔴 {rojos} Críticas | 🟡 {amarillos} Alerta | 🟢 {verdes} OK")
        
        col_desc1, col_desc2 = st.columns(2)
        h = datetime.utcnow() - timedelta(hours=3)
        fecha_str = h.strftime('%d%m%Y')
        
        with col_desc1:
            pdf_main_data = build_pdf_main(df_res, df_abiertos)
            st.download_button(
                label="📥 Descargar Resumen de Golpes", 
                data=pdf_main_data, 
                file_name=f"Resumen_de_Golpes_{fecha_str}.pdf", 
                mime="application/pdf", 
                use_container_width=True
            )
            
        with col_desc2:
            pdf_resumen_data = build_pdf_resumen(df_res)
            st.download_button(
                label="📊 Descargar Estado de los Mantenimientos de Matrices Fumiscor", 
                data=pdf_resumen_data, 
                file_name=f"Estado_de_los_mantenimientos_de_matrices_fumiscor_{fecha_str}.pdf", 
                mime="application/pdf", 
                use_container_width=True
            )

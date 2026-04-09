import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import os
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

st.markdown('<div class="header-style">⚙️ Reporte: Control de Golpes de Matrices (Fumiscor)</div>', unsafe_allow_html=True)
st.write("<p style='text-align: center;'>Cruce automático de Catálogo de Google Sheets con Producción y Mantenimiento de SQL Server.</p>", unsafe_allow_html=True)
st.divider()

# ==========================================
# 2. ENLACES Y CONFIGURACIÓN
# ==========================================
# URL del Catálogo Maestro proporcionado (formato export=csv)
URL_CATALOGO = "https://docs.google.com/spreadsheets/d/198KjQWZwfvvWwq1q1N1zv1cgzkot2hhGbwQvbi9_zFQ/export?format=csv&gid=818188145"

# ==========================================
# 3. FUNCIONES DE LIMPIEZA Y EXTRACCIÓN
# ==========================================
def clean_str(val):
    if pd.isna(val): return ""
    v = str(val).strip().upper()
    if v.endswith('.0'): v = v[:-2]
    return v

def get_match_key(pieza_str):
    pieza_str = str(pieza_str).strip()
    p = pieza_str.split('/')[0].strip()
    if ' - ' in p:
        p = p.split(' - ')[0].strip()
    elif '-' in p:
        p = p.split('-')[0].strip()
    return p

@st.cache_data(ttl=300)
def load_all_data():
    # 1. CARGAR CATÁLOGO DE GOOGLE SHEETS
    try:
        df_cat = pd.read_csv(URL_CATALOGO)
        df_cat.columns = df_cat.columns.astype(str).str.replace('\n', ' ').str.replace('\r', '').str.strip()
        df_cat.columns = df_cat.columns.str.replace(r'\s+', ' ', regex=True)
        col_activo = next((c for c in df_cat.columns if 'ACTIVO' in c.upper()), None)
        if col_activo:
            df_cat = df_cat[df_cat[col_activo].astype(str).str.strip().str.upper() == 'SI']
    except Exception as e:
        st.error(f"Error al cargar el Catálogo de Matrices: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 2. CONECTAR A SQL SERVER PARA PRODUCCIÓN Y EVENTOS
    try:
        conn = st.connection("wii_bi", type="sql")
        
        # Extraemos la producción acumulada (Golpes = Buenas + Retrabajo) desde el 2023
        q_prod = """
            SELECT 
                pr.Code as Codigo_Pieza, 
                CAST(p.Date as DATE) as Fecha, 
                SUM(p.Good + p.Rework) as Golpes_Totales
            FROM PROD_D_01 p 
            JOIN PRODUCT pr ON p.ProductId = pr.ProductId 
            WHERE p.Date >= '2023-01-01'
            GROUP BY pr.Code, CAST(p.Date as DATE)
        """
        df_prod_raw = conn.query(q_prod)
        
        # Extraemos los mantenimientos de Matrices en EVENT_01
        q_event = """
            SELECT 
                CAST(e.Date as DATE) as Fecha, 
                t3.Name as Nivel3, 
                t4.Name as Nivel4
            FROM EVENT_01 e
            LEFT JOIN EVENTTYPE t1 ON e.EventTypeLevel1 = t1.EventTypeId
            LEFT JOIN EVENTTYPE t2 ON e.EventTypeLevel2 = t2.EventTypeId
            LEFT JOIN EVENTTYPE t3 ON e.EventTypeLevel3 = t3.EventTypeId
            LEFT JOIN EVENTTYPE t4 ON e.EventTypeLevel4 = t4.EventTypeId
            WHERE (UPPER(t1.Name) LIKE '%MATRI%' OR UPPER(t2.Name) LIKE '%MATRI%' OR UPPER(t3.Name) LIKE '%MATRI%')
              AND e.Date >= '2023-01-01'
        """
        df_event_raw = conn.query(q_event)
        
    except Exception as e:
        st.error(f"Error al conectar con SQL Server wii_bi: {e}")
        return df_cat, pd.DataFrame(), pd.DataFrame()

    # 3. PROCESAR PRODUCCIÓN (SQL -> Formato Pandas)
    if not df_prod_raw.empty:
        df_prod_raw['Fecha'] = pd.to_datetime(df_prod_raw['Fecha'], errors='coerce')
        df_prod_raw['Pieza_Match'] = df_prod_raw['Codigo_Pieza'].apply(lambda x: get_match_key(clean_str(x)))
        df_prod_raw['Golpes_Totales'] = pd.to_numeric(df_prod_raw['Golpes_Totales'], errors='coerce').fillna(0)
    else:
        df_prod_raw = pd.DataFrame(columns=['Fecha', 'Pieza_Match', 'Golpes_Totales'])

    # 4. PROCESAR MANTENIMIENTOS (SQL -> Formato Pandas)
    registros_mant = []
    if not df_event_raw.empty:
        for _, row in df_event_raw.iterrows():
            texto_evento = f"{row['Nivel3']} {row['Nivel4']}".upper()
            
            # Asumimos que la pieza o código lo suelen anotar en el Nivel 4
            pieza_match = get_match_key(clean_str(row['Nivel4'])) 
            
            tipo = "PREV" if "PREV" in texto_evento else "CORR" if "CORR" in texto_evento else "OTRO"
            
            # Solo guardamos los que tienen una clasificación clara
            if tipo in ["PREV", "CORR"]:
                registros_mant.append({
                    'Fecha': pd.to_datetime(row['Fecha']), 
                    'Pieza_Match': pieza_match, 
                    'OP': '', # SQL Event no suele tener la OP separada
                    'Tipo_Mant': tipo, 
                    'Terminado': 'SI' # Si está en EVENT_01, consideramos que se ejecutó
                })
                
    df_mant_raw = pd.DataFrame(registros_mant) if registros_mant else pd.DataFrame(columns=['Fecha', 'Pieza_Match', 'OP', 'Tipo_Mant', 'Terminado'])

    return df_cat, df_prod_raw, df_mant_raw

# ==========================================
# 4. MOTOR DE CRUCE Y CÁLCULO
# ==========================================
def procesar_estado_matrices(df_cat, df_prod, df_mant):
    resultados = []
    abiertos = []
    
    col_pieza = next((c for c in df_cat.columns if c.upper() == 'PIEZA' or 'NÚMERO DE PIEZA' in c.upper() or 'CODIGO' in c.upper()), 'PIEZA')
    col_op = next((c for c in df_cat.columns if c.upper() == 'OP' or 'OPERACION' in c.upper()), 'OP')
    col_cliente = next((c for c in df_cat.columns if 'CLIENTE' in c.upper()), 'CLIENTE')
    col_tipo = next((c for c in df_cat.columns if 'TIPO' in c.upper()), 'TIPO')
    col_limite = next((c for c in df_cat.columns if 'GOLPES PARA MANTENIMIENTO' in c.upper() or 'LIMITE' in c.upper()), 'GOLPES PARA MANTENIMIENTO')
    col_alerta = next((c for c in df_cat.columns if 'ALERTA' in c.upper()), 'ALERTA')
    col_prev = next((c for c in df_cat.columns if 'ULTIMO PREVENTIVO' in c.upper()), 'ULTIMO PREVENTIVO')
    col_corr = next((c for c in df_cat.columns if 'ULTIMO CORRECTIVO' in c.upper()), 'ULTIMO CORRECTIVO')

    for _, row in df_cat.iterrows():
        pieza_completa = clean_str(row.get(col_pieza, ''))
        op = clean_str(row.get(col_op, ''))
        if not pieza_completa or pieza_completa == 'NAN': continue
        pieza_match = get_match_key(pieza_completa)
        
        limite_mant = pd.to_numeric(row.get(col_limite, 0), errors='coerce')
        if pd.isna(limite_mant) or limite_mant == 0: limite_mant = 20000
        
        limite_alerta = pd.to_numeric(row.get(col_alerta, 0), errors='coerce') 
        if pd.isna(limite_alerta) or limite_alerta == 0: limite_alerta = (limite_mant * 0.8)
        
        fecha_prev, fecha_corr, fecha_abierto = pd.NaT, pd.NaT, pd.NaT
        tiene_abierto, tipo_abierto = False, ""
        
        # 1. Leer fechas desde el Catálogo (Google Sheets)
        if col_prev: fecha_prev = pd.to_datetime(row.get(col_prev), dayfirst=True, errors='coerce')
        if col_corr: fecha_corr = pd.to_datetime(row.get(col_corr), dayfirst=True, errors='coerce')

        # 2. Leer fechas más recientes desde SQL (Si existieran y coincidieran)
        if not df_mant.empty:
            match = df_mant[(df_mant['Pieza_Match'] == pieza_match)]
            
            term = match[match['Terminado'] == 'SI']
            max_fecha_cerrado = pd.NaT
            if not term.empty:
                max_fecha_cerrado = term['Fecha'].max()
                max_p = term[term['Tipo_Mant'] == 'PREV']['Fecha'].max()
                max_c = term[term['Tipo_Mant'] == 'CORR']['Fecha'].max()
                if pd.notna(max_p) and (pd.isna(fecha_prev) or max_p > fecha_prev): fecha_prev = max_p
                if pd.notna(max_c) and (pd.isna(fecha_corr) or max_c > fecha_corr): fecha_corr = max_c
                
            ab = match[match['Terminado'] == 'NO']
            if not ab.empty:
                max_fecha_abierto = ab['Fecha'].max()
                if pd.isna(max_fecha_cerrado) or max_fecha_abierto > max_fecha_cerrado:
                    tiene_abierto = True
                    fecha_abierto = max_fecha_abierto
                    tipo_abierto = ab.loc[ab['Fecha'].idxmax(), 'Tipo_Mant']

        # 3. Definir la Fecha Base para contar los golpes
        fecha_base = pd.NaT
        if pd.notna(fecha_prev) and pd.notna(fecha_corr): fecha_base = max(fecha_prev, fecha_corr)
        elif pd.notna(fecha_prev): fecha_base = fecha_prev
        elif pd.notna(fecha_corr): fecha_base = fecha_corr

        # 4. Filtrar y sumar Producción desde SQL
        prod_match = df_prod[df_prod['Pieza_Match'] == pieza_match]
        if pd.notna(fecha_base):
            prod_match = prod_match[prod_match['Fecha'] >= fecha_base]
        
        golpes_totales = int(prod_match['Golpes_Totales'].sum())
        
        # 5. Estado de la Matriz
        color, estado = "VERDE", "OK"
        if golpes_totales >= limite_mant: color, estado = "ROJO", "MANT. REQUERIDO"
        elif golpes_totales >= limite_alerta: color, estado = "AMARILLO", "ALERTA PREVENTIVO"
            
        resultados.append({
            'CLIENTE': clean_str(row.get(col_cliente, '-')), 'PIEZA': pieza_completa, 'OP': op,
            'TIPO': clean_str(row.get(col_tipo, '-')), 'ULT_PREV': fecha_prev.strftime('%d/%m/%y') if pd.notna(fecha_prev) else "-",
            'ULT_CORR': fecha_corr.strftime('%d/%m/%y') if pd.notna(fecha_corr) else "-",
            'GOLPES': golpes_totales, 'LIMITE': int(limite_mant), 'ESTADO': estado, 'COLOR': color
        })
        if tiene_abierto:
            abiertos.append({'CLIENTE': clean_str(row.get(col_cliente, '-')), 'PIEZA': pieza_completa, 'OP': op,
                             'TIPO': clean_str(row.get(col_tipo, '-')), 'TIPO_MANT_ABIERTO': tipo_abierto, 'FECHA_APERTURA': fecha_abierto.strftime('%d/%m/%Y')})
            
    return pd.DataFrame(resultados), pd.DataFrame(abiertos)

# ==========================================
# 5. GENERACIÓN DEL PDF (FPDF)
# ==========================================
class PDFGolpes(FPDF):
    def header(self):
        self.set_font("Arial", 'B', 15)
        self.set_text_color(31, 73, 125)
        self.cell(0, 10, "Control de Golpes de Matrices (Detalle Principal)", border=0, ln=True, align='C')
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
        self.cell(0, 10, "Estado General del Mantenimiento Preventivo", border=0, ln=True, align='C')
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
    """Genera el reporte principal: Detalle de piezas y Mantenimientos Abiertos (Hojas separadas)."""
    pdf = PDFGolpes(orientation='L', unit='mm', format='A4')
    
    # --- HOJA 1: DETALLE DE GOLPES ---
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

    # --- HOJA 2: MANTENIMIENTOS ABIERTOS ---
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
            pdf.cell(25, 7, r['CLIENTE'], 1, 0, 'C')
            pdf.cell(90, 7, r['PIEZA'], 1, 0, 'L')
            pdf.cell(15, 7, r['OP'], 1, 0, 'C')
            pdf.cell(35, 7, r['TIPO_MANT_ABIERTO'], 1, 0, 'C')
            pdf.cell(35, 7, r['FECHA_APERTURA'], 1, 1, 'C')

    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name)
    b = open(buf.name, "rb").read()
    os.remove(buf.name)
    return b

def build_pdf_resumen(df_resultados):
    """Genera exclusivamente el reporte de Estado General concentrado en UNA SOLA HOJA."""
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

    # --- 1. TABLA RESUMEN CON FORMATO MÁS COMPACTO ---
    pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(31, 73, 125); pdf.set_text_color(255, 255, 255)
    mx = 43.5; pdf.set_x(mx)
    pdf.cell(35, 6, "CLIENTE", 1, 0, 'C', fill=True)
    pdf.cell(25, 6, "TOTAL OP", 1, 0, 'C', fill=True)
    pdf.cell(35, 6, "CON PREV.", 1, 0, 'C', fill=True)
    pdf.cell(35, 6, "SIN MANT.", 1, 0, 'C', fill=True)
    pdf.cell(40, 6, "% PREV", 1, 0, 'C', fill=True)
    pdf.cell(40, 6, "% SIN MANT", 1, 1, 'C', fill=True)
    
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
    
    # --- 2. GRÁFICOS DE TORTA (GENERAL + CLIENTES) EN LA MISMA HOJA ---
    if len(resumen_data) > 0:
        pdf.ln(5)
        y_charts = pdf.get_y()
        
        fig_gen = go.Figure(data=[go.Pie(
            labels=['CON PREVENTIVO', 'SIN MANT.'], 
            values=[total_ok, total_nok], 
            marker_colors=['#2ca02c', '#d62728']
        )])
        fig_gen.update_traces(textposition='inside', textinfo='percent+label', showlegend=False)
        fig_gen.update_layout(title_text="Matrices Totales", title_x=0.5, margin=dict(t=40, b=10, l=10, r=10), height=300, width=300)

        fig_cli = make_subplots(
            rows=1, cols=len(resumen_data), 
            specs=[[{'type':'domain'}] * len(resumen_data)], 
            subplot_titles=[r['CLIENTE'] for r in resumen_data]
        )
        for i, r in enumerate(resumen_data):
            fig_cli.add_trace(go.Pie(
                labels=['CON PREVENTIVO', 'SIN MANT.'], 
                values=[r['OK'], r['NOK']], 
                marker_colors=['#2ca02c', '#d62728']
            ), 1, i + 1)
        
        fig_cli.update_traces(textposition='inside', textinfo='percent')
        fig_cli.update_layout(
            title_text="Desglose por Cliente", title_x=0.5, 
            showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5), 
            margin=dict(t=40, b=40, l=10, r=10), height=300, width=700
        )
        fig_cli.update_annotations(font_size=12)
        
        # Eliminado engine="kaleido" para evitar warnings en la nube
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_gen:
            fig_gen.write_image(tmp_gen.name)
            pdf.image(tmp_gen.name, x=15, y=y_charts, w=70)
            os.remove(tmp_gen.name)
            
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_cli:
            fig_cli.write_image(tmp_cli.name)
            pdf.image(tmp_cli.name, x=90, y=y_charts, w=190)
            os.remove(tmp_cli.name)
    
    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name)
    b = open(buf.name, "rb").read()
    os.remove(buf.name)
    return b

# ==========================================
# 6. INTERFAZ DE STREAMLIT
# ==========================================

if st.button("🔄 Forzar Actualización de Datos (Borrar Caché)", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

with st.spinner("Conectando con base de datos SQL y Google Sheets..."):
    try:
        df_cat_raw, df_prod_raw, df_mant_raw = load_all_data()
        datos_listos = not df_cat_raw.empty
    except Exception as e:
        st.error(f"Error crítico durante la extracción: {e}")
        datos_listos = False

if datos_listos:
    st.success("Bases de datos sincronizadas exitosamente.")
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.info("El sistema está cruzando el listado maestro de matrices con el historial acumulado de piezas buenas y retrabajos desde la base de datos oficial (wii_bi).")
        
    with col2:
        if st.button("⚙️ Procesar Datos de Matrices", use_container_width=True, type="primary"):
            with st.spinner("Calculando estado de matrices..."):
                df_res, df_abiertos = procesar_estado_matrices(df_cat_raw, df_prod_raw, df_mant_raw)
                
                st.session_state['df_res'] = df_res
                st.session_state['df_abiertos'] = df_abiertos

    # Si los datos ya fueron procesados, mostramos botones de descarga
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
                label="📥 Descargar Reporte Principal (Detalles y Pendientes)", 
                data=pdf_main_data, 
                file_name=f"Reporte_Golpes_Detalle_{fecha_str}.pdf", 
                mime="application/pdf", 
                use_container_width=True
            )
            
        with col_desc2:
            pdf_resumen_data = build_pdf_resumen(df_res)
            st.download_button(
                label="📊 Descargar Resumen General (Tabla y Gráficos)", 
                data=pdf_resumen_data, 
                file_name=f"Reporte_Golpes_Resumen_{fecha_str}.pdf", 
                mime="application/pdf", 
                use_container_width=True
            )
    elif 'df_res' in st.session_state and st.session_state['df_res'].empty:
        st.warning("No hay datos activos en el catálogo de matrices.")

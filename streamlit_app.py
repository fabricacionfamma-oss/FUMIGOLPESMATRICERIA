import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import os
import io
import difflib
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from fpdf import FPDF

# ==========================================
# 0. MAPEO DE MÁQUINAS (EXCLUSIVO ESTAMPADO)
# ==========================================
MAQUINAS_ESTAMPADO = [
    "P-023", "P-024", "P-025", "P-026", "P-027", "P-028", "P-029", "P-030",
    "BAL-002", "BAL-003", "BAL-005", "BAL-006", "BAL-007", "BAL-008", "BAL-009", 
    "BAL-010", "BAL-011", "BAL-012", "BAL-013", "BAL-014", "BAL-015",
    "P-011", "P-012", "P-013", "P-014", "P-016", "P-017", "P-018", 
    "P-015", "P-019", "P-020", "P-021", "P-022", "GOF01"
]

# ==========================================
# 1. CONFIGURACIÓN Y ESTILOS
# ==========================================
st.set_page_config(page_title="Control de Golpes - Fumiscor", layout="wide", page_icon="⚙️")

st.markdown("""
<style>
    .header-style { font-size: 26px; font-weight: bold; margin-bottom: 5px; color: #1F2937; text-align: center; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="header-style">⚙️ Sistema de Diagnóstico y Control - Fumiscor</div>', unsafe_allow_html=True)
st.divider()

# ==========================================
# 2. ENLACES DE DATOS (FUMISCOR)
# ==========================================
URL_CATALOGO = "https://docs.google.com/spreadsheets/d/198KjQWZwfvvWwq1q1N1zv1cgzkot2hhGbwQvbi9_zFQ/export?format=csv&gid=1766766947"
URL_FORMS_PREV = "https://docs.google.com/spreadsheets/d/1VqsPNhAlT1kPCltbMWsbkZNFBKdwZRFM5RAmnRV0v3c/export?format=csv&gid=1603203990"
URL_FORMS_CORR = "https://docs.google.com/spreadsheets/d/1bL_tnlSXGO_t9tKnhIHT5pZ3DAxivbiq2tFETVxBaVI/export?format=csv&gid=1507213893"

# ==========================================
# 3. FUNCIONES DE LIMPIEZA Y MATCHING AVANZADO
# ==========================================
def clean_str(val):
    if pd.isna(val): return ""
    return str(val).strip().upper().replace(" ", "")

def find_op_col_for_pieza(pieza_col_name, df_cols):
    mapping = {
        'PIEZAS RENAULT': 'OPERACION',
        'PIEZAS FAURECIA': 'OPERACION 2',
        'PIEZAS FIAT': 'OPERACION 3',
        'PIEZAS DENSO': 'OPERACION 4',
        'PIEZAS PEUGEOT': 'OPERACION 5',
        'NUMERO DE PIEZA': 'OPERACION 6',
        'MATRIZ': 'OPERACION'
    }
    target = next((v for k, v in mapping.items() if k in pieza_col_name.upper()), None)
    if target:
        for c in df_cols:
            if clean_str(c) == clean_str(target): return c
            if clean_str(c).replace('Ó','O') == clean_str(target): return c
    return None

def get_best_match_hybrid(pieza_raw, operacion_raw, cat_matrices):
    p_clean = clean_str(pieza_raw)
    op_clean = clean_str(operacion_raw)
    
    if not p_clean: return ""
    
    for m in cat_matrices:
        if clean_str(m) == p_clean: return m
            
    candidates = [m for m in cat_matrices if (p_clean in clean_str(m) or clean_str(m) in p_clean) and len(p_clean)>6]
    
    if not candidates:
        matches = difflib.get_close_matches(p_clean, [clean_str(m) for m in cat_matrices], n=5, cutoff=0.75)
        if matches:
            candidates = [m for m in cat_matrices if clean_str(m) in matches]
            
    if not candidates: return pieza_raw 
    if len(candidates) == 1: return candidates[0]
        
    best_score = -999
    best_cand = candidates[0]
    
    for cand in candidates:
        cand_clean = clean_str(cand)
        score = 0
        
        if op_clean in ['20', '30', '40', '50', '60']:
            if f"OP{op_clean}" in cand_clean: score += 10
            if op_clean == '20' and 'MP2' in cand_clean: score += 10
            if op_clean == '30' and 'MP3' in cand_clean: score += 10
            if op_clean == '40' and 'MP4' in cand_clean: score += 10
            if op_clean == '20' and ('MP1' in cand_clean or 'OP30' in cand_clean): score -= 5
            
        elif op_clean in ['MULTIPUESTO', 'MP', '10', 'PROGRESIVA', 'PROG', '']:
            if 'MP1' in cand_clean or ('MP' not in cand_clean and 'OP' not in cand_clean): score += 5
            if 'MP2' in cand_clean or 'OP20' in cand_clean or 'MP3' in cand_clean or 'OP30' in cand_clean: score -= 5
            
        if score > best_score:
            best_score = score
            best_cand = cand
            
    return best_cand

def get_best_match_sql(texto, lista_candidatos):
    if pd.isna(texto) or not str(texto).strip(): return ""
    val = clean_str(texto)
    
    for cand in lista_candidatos:
        if clean_str(cand) == val: return cand
        
    valid_candidates = []
    for cand in lista_candidatos:
        c_clean = clean_str(cand)
        if len(cand) > 6 and (c_clean in val or val in c_clean):
            if "OP" in val and "OP" not in c_clean:
                continue
            valid_candidates.append(cand)

    if valid_candidates:
        valid_candidates.sort(key=len, reverse=True)
        return valid_candidates[0]
        
    matches = difflib.get_close_matches(val, [clean_str(c) for c in lista_candidatos], n=1, cutoff=0.82)
    if matches: 
        for cand in lista_candidatos:
            if clean_str(cand) == matches[0]: return cand
            
    return texto

@st.cache_data(ttl=60)
def load_all_sources():
    try:
        df_cat = pd.read_csv(URL_CATALOGO).dropna(how='all')
        df_cat.columns = [str(c).strip().upper() for c in df_cat.columns]
        
        col_matriz = next((c for c in df_cat.columns if c == 'MATRIZ'), None)
        col_prod = next((c for c in df_cat.columns if c in ['PRODUCTO 1', 'PRODUCTO', 'CODIGO']), None)
        col_tipo = next((c for c in df_cat.columns if c == 'TIPO'), None)
        col_op = next((c for c in df_cat.columns if c == 'OP'), None)

        if not col_matriz:
            st.error("❌ No se encontró la columna 'MATRIZ' en el Catálogo.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        df_cat = df_cat.dropna(subset=[col_matriz])
        df_cat['FORM_KEY'] = df_cat[col_matriz].astype(str).str.strip()
        df_cat['SQL_KEY'] = df_cat[col_prod].astype(str).str.strip() if col_prod else df_cat['FORM_KEY']
        df_cat['OP_MOSTRAR'] = df_cat[col_op].fillna('-').astype(str) if col_op else '-'
        df_cat['PIEZA_MOSTRAR'] = df_cat[col_matriz].fillna('-').astype(str)
        df_cat['TIPO_MOSTRAR'] = df_cat[col_tipo].fillna('-').astype(str) if col_tipo else '-'

        lista_forms_keys = df_cat['FORM_KEY'].unique().tolist()
        lista_sql_keys = df_cat['SQL_KEY'].unique().tolist()
            
    except Exception as e:
        st.error(f"Error cargando Catálogo: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def fetch_forms(url, tipo_mant):
        try:
            df_raw = pd.read_csv(url)
            if df_raw.empty: return pd.DataFrame()

            header_idx = -1
            for i, row in df_raw.head(15).iterrows():
                row_vals = " ".join([str(x).upper() for x in row.values])
                if 'MARCA TEMPORAL' in row_vals or 'FECHA' in row_vals:
                    header_idx = i; break
            
            if header_idx != -1: df_raw = pd.read_csv(url, skiprows=header_idx + 1)
            df_raw.columns = [str(c).upper().strip() for c in df_raw.columns]
            df_raw = df_raw.loc[:, ~df_raw.columns.duplicated()]
            
            col_f_manual = 'FECHA' if 'FECHA' in df_raw.columns else None
            col_f_auto = next((c for c in df_raw.columns if 'MARCA TEMPORAL' in c), None)
            
            nombres_buscados = ['PIEZAS RENAULT', 'PIEZAS FIAT', 'PIEZAS PEUGEOT', 'PIEZAS FAURECIA', 'PIEZAS DENSO', 'MATRIZ']
            cols_pieza = [c for c in df_raw.columns if any(n in c for n in nombres_buscados) and 'TIPO' not in c and '[' not in c]
            if not cols_pieza:
                cols_pieza = [c for c in df_raw.columns if 'PIEZA' in c and 'TIPO' not in c and 'NUMERO' not in c and '[' not in c]
                          
            cols_term = [c for c in df_raw.columns if 'TERMINADO' in c or 'TERMINO' in c or 'ESTADO DEL MANTENIMIENTO' in c]
            
            if not col_f_auto and not col_f_manual: return pd.DataFrame()

            registros = []
            for _, row in df_raw.iterrows():
                fecha = pd.NaT
                if col_f_manual and pd.notna(row.get(col_f_manual)):
                    fecha = pd.to_datetime(row.get(col_f_manual), format='mixed', dayfirst=True, errors='coerce')
                if pd.isna(fecha) and col_f_auto and pd.notna(row.get(col_f_auto)):
                    fecha = pd.to_datetime(row.get(col_f_auto), format='mixed', dayfirst=True, errors='coerce')
                if pd.isna(fecha): continue
                
                pieza_raw, op_raw = "", ""
                for cp in cols_pieza:
                    val = clean_str(row.get(cp))
                    if val and val not in ['NAN', 'NONE', '-', '0', 'N/A', '']:
                        pieza_raw = str(row.get(cp)) 
                        col_op_name = find_op_col_for_pieza(cp, df_raw.columns)
                        if col_op_name and pd.notna(row.get(col_op_name)):
                            op_raw = str(row.get(col_op_name))
                        break 
                
                if not pieza_raw: continue
                
                terminado = 'NO'
                for ct in cols_term:
                    val_t = clean_str(row.get(ct))
                    if val_t in ['SI', 'SÍ', 'VERDADERO', 'FINALIZADO', 'OK']:
                        terminado = 'SI'
                        break
                
                f_key = get_best_match_hybrid(pieza_raw, op_raw, lista_forms_keys)
                registros.append({'FECHA_DT': fecha, 'TIPO_MANT': tipo_mant, 'TERMINADO': terminado, 'FORM_KEY': f_key})
                    
            return pd.DataFrame(registros)
        except Exception as e: 
            st.error(f"Error procesando formulario {tipo_mant}: {e}")
            return pd.DataFrame()

    df_prev = fetch_forms(URL_FORMS_PREV, "PREV")
    df_corr = fetch_forms(URL_FORMS_CORR, "CORR")
    df_forms_all = pd.concat([df_prev, df_corr], ignore_index=True) if not df_prev.empty or not df_corr.empty else pd.DataFrame()

    try:
        conn = st.connection("wii_bi", type="sql")
        q = """
        SELECT pr.Code as PIEZA, CAST(p.Date as DATE) as FECHA, c.Name as MAQUINA, SUM(p.Good + p.Rework) as GOLPES 
        FROM PROD_D_01 p 
        JOIN PRODUCT pr ON p.ProductId = pr.ProductId 
        JOIN CELL c ON p.CellId = c.CellId
        WHERE p.Date >= '2025-01-01' 
        GROUP BY pr.Code, CAST(p.Date as DATE), c.Name
        """
        df_sql = conn.query(q)
        df_sql = df_sql[df_sql['MAQUINA'].isin(MAQUINAS_ESTAMPADO)]
        df_sql = df_sql.groupby(['PIEZA', 'FECHA'])['GOLPES'].sum().reset_index()
        df_sql['FECHA'] = pd.to_datetime(df_sql['FECHA'], errors='coerce')
        df_sql['GOLPES'] = pd.to_numeric(df_sql['GOLPES'], errors='coerce').fillna(0)
        
        mapeo_piezas = {p: get_best_match_sql(p, lista_sql_keys) for p in df_sql['PIEZA'].unique()}
        df_sql['SQL_KEY'] = df_sql['PIEZA'].map(mapeo_piezas)
    except Exception as e: 
        st.error(f"Error SQL: {e}"); df_sql = pd.DataFrame()

    return df_cat, df_sql, df_forms_all

# ==========================================
# 4. LÓGICA DE PROCESAMIENTO
# ==========================================
def procesar_datos(df_cat, df_sql, df_forms):
    res_semaforo = []
    fecha_corte_default = pd.to_datetime("2026-01-01")

    for _, row in df_cat.iterrows():
        f_key = row.get('FORM_KEY')
        s_key = row.get('SQL_KEY')
        
        if pd.isna(f_key) or not f_key: continue
        
        cliente = str(row.get('CLIENTE', '-')).strip().upper()
        if cliente in ['NAN', 'NONE', '']: cliente = '-'

        pieza_mostrar = str(row.get('PIEZA_MOSTRAR')).strip()
        tipo_matriz = str(row.get('TIPO_MOSTRAR', '-')).strip().upper()
        
        op_mostrar = str(row.get('OP_MOSTRAR', '-')).strip()
        if op_mostrar in ['NAN', 'NONE', '']: op_mostrar = '-'

        if 'PROG' in tipo_matriz: limite = 40000; tipo_impreso = "PROGRESIVA"
        elif 'MEC' in tipo_matriz: limite = 20000; tipo_impreso = "MECANICA"
        elif 'BAL' in tipo_matriz: limite = 30000; tipo_impreso = "BALANCIN"
        else: limite = 30000; tipo_impreso = tipo_matriz if tipo_matriz != '-' else '-'

        f_prev, f_corr = pd.NaT, pd.NaT

        if not df_forms.empty:
            match_f = df_forms[df_forms['FORM_KEY'] == f_key].copy()
            if not match_f.empty:
                cerrados = match_f[match_f['TERMINADO'] == 'SI']
                if not cerrados.empty:
                    mp = cerrados[cerrados['TIPO_MANT'] == 'PREV']['FECHA_DT'].max()
                    mc = cerrados[cerrados['TIPO_MANT'] == 'CORR']['FECHA_DT'].max()
                    if pd.notna(mp): f_prev = mp
                    if pd.notna(mc): f_corr = mc

        fechas_validas = [f for f in [f_prev, f_corr] if pd.notna(f)]
        fecha_inicio_calculo = max(fechas_validas) if fechas_validas else fecha_corte_default

        prod = df_sql[(df_sql['SQL_KEY'] == s_key) & (df_sql['FECHA'] >= fecha_inicio_calculo)] if not df_sql.empty else pd.DataFrame()
        g_total = int(prod['GOLPES'].sum()) if not prod.empty else 0

        color = "ROJO" if g_total >= limite else "AMARILLO" if g_total >= (limite*0.8) else "VERDE"
        estado = "MANT. REQUERIDO" if color == "ROJO" else "ALERTA PREVENTIVO" if color == "AMARILLO" else "OK"
        
        res_semaforo.append({
            'CLIENTE': cliente, 
            'PIEZA': pieza_mostrar, 
            'OP': op_mostrar, 
            'TIPO': str(tipo_impreso).encode('latin-1', 'replace').decode('latin-1'),
            'ULT_PREV': f_prev.strftime('%d/%m/%Y') if pd.notna(f_prev) else "-",
            'ULT_CORR': f_corr.strftime('%d/%m/%Y') if pd.notna(f_corr) else "-",
            'GOLPES': g_total, 
            'LIMITE': limite, 
            'ESTADO': estado, 
            'COLOR': color
        })

    return pd.DataFrame(res_semaforo)

# ==========================================
# 5. GENERACIÓN DEL PDF Y EXCEL
# ==========================================
class PDFGolpes(FPDF):
    def header(self):
        self.set_font("Arial", 'B', 15); self.set_text_color(31, 73, 125)
        self.cell(0, 10, "Resumen de Golpes", border=0, ln=True, align='C')
        self.set_font("Arial", 'I', 9); self.set_text_color(100, 100, 100)
        hora_arg = datetime.utcnow() - timedelta(hours=3)
        self.cell(0, 5, f"Calculo generado el: {hora_arg.strftime('%d/%m/%Y %H:%M')}", border=0, ln=True, align='C'); self.ln(3)
        
    def footer(self):
        self.set_y(-15); self.set_font("Arial", "I", 8); self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, "C")

class PDFResumen(FPDF):
    def header(self):
        self.set_font("Arial", 'B', 15); self.set_text_color(31, 73, 125)
        self.cell(0, 10, "Estado de los Mantenimientos de Matrices Fumiscor", border=0, ln=True, align='C')
        self.set_font("Arial", 'I', 9); self.set_text_color(100, 100, 100)
        hora_arg = datetime.utcnow() - timedelta(hours=3)
        self.cell(0, 5, f"Generado el: {hora_arg.strftime('%d/%m/%Y %H:%M')}", border=0, ln=True, align='C'); self.ln(3)
        
    def footer(self):
        self.set_y(-15); self.set_font("Arial", "I", 8); self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, "C")

def build_pdf_main(df_resultados):
    pdf = PDFGolpes(orientation='L', unit='mm', format='A4')
    pdf.add_page(); pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(31, 73, 125); pdf.set_text_color(255, 255, 255)
    
    pdf.cell(15, 8, "Cliente", 1, 0, 'C', fill=True)
    pdf.cell(76, 8, "Pieza / Matriz", 1, 0, 'C', fill=True)
    pdf.cell(10, 8, "OP", 1, 0, 'C', fill=True)
    pdf.cell(28, 8, "Tipo Matriz", 1, 0, 'C', fill=True)
    pdf.cell(22, 8, "Ult. Prev.", 1, 0, 'C', fill=True)
    pdf.cell(22, 8, "Ult. Corr.", 1, 0, 'C', fill=True)
    pdf.cell(26, 8, "Golpes Ac.", 1, 0, 'C', fill=True)
    pdf.cell(26, 8, "Limite M.", 1, 0, 'C', fill=True)
    pdf.cell(52, 8, "Estado / Accion", 1, 1, 'C', fill=True)
    
    pdf.set_font("Arial", '', 8)
    for _, row in df_resultados.iterrows():
        bg = (255, 180, 180) if row['COLOR'] == "ROJO" else (255, 240, 180) if row['COLOR'] == "AMARILLO" else (198, 239, 206)
        txt = (180, 0, 0) if row['COLOR'] == "ROJO" else (150, 100, 0) if row['COLOR'] == "AMARILLO" else (0, 100, 0)
        tipo_str = str(row['TIPO']) if str(row['TIPO']).upper() not in ['NAN', 'NONE', ''] else '-'
        
        pdf.set_text_color(0, 0, 0)
        pdf.cell(15, 7, str(row['CLIENTE'])[:10], 1, 0, 'C')
        pdf.cell(76, 7, str(row['PIEZA'])[:65], 1, 0, 'L')
        pdf.cell(10, 7, str(row['OP'])[:8], 1, 0, 'C')
        pdf.cell(28, 7, tipo_str[:15], 1, 0, 'C') 
        pdf.cell(22, 7, str(row['ULT_PREV']), 1, 0, 'C')
        pdf.cell(22, 7, str(row['ULT_CORR']), 1, 0, 'C')
        
        pdf.set_fill_color(*bg); pdf.set_text_color(*txt); pdf.set_font("Arial", 'B', 8)
        pdf.cell(26, 7, f"{row['GOLPES']:,}", 1, 0, 'C', fill=True)
        pdf.set_text_color(0, 0, 0); pdf.set_font("Arial", '', 8)
        pdf.cell(26, 7, f"{row['LIMITE']:,}", 1, 0, 'C')
        pdf.set_fill_color(*bg); pdf.set_text_color(*txt); pdf.set_font("Arial", 'B', 8)
        pdf.cell(52, 7, str(row['ESTADO']), 1, 1, 'C', fill=True)

    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name)
    b = open(buf.name, "rb").read()
    os.remove(buf.name)
    return b

def build_pdf_resumen(df_resultados):
    pdf = PDFResumen(orientation='L', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=15); pdf.add_page(); pdf.ln(2)
    
    resumen_data = []
    total_gen = len(df_resultados); total_ok = len(df_resultados[df_resultados['COLOR'] == 'VERDE']); total_nok = total_gen - total_ok
    for c in sorted([x for x in df_resultados['CLIENTE'].unique() if x != "-"]):
        df_c = df_resultados[df_resultados['CLIENTE'] == c]
        tot = len(df_c); ok = len(df_c[df_c['COLOR'] == 'VERDE']); nok = tot - ok
        if tot > 0: resumen_data.append({'CLIENTE': c, 'TOT': tot, 'OK': ok, 'NOK': nok, 'POK': f"{int(round(ok/tot*100))}%", 'PNOK': f"{int(round(nok/tot*100))}%"})

    pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(31, 73, 125); pdf.set_text_color(255, 255, 255); mx = 43.5; pdf.set_x(mx)
    pdf.cell(35, 6, "CLIENTE", 1, 0, 'C', fill=True); pdf.cell(25, 6, "TOTAL PIEZAS", 1, 0, 'C', fill=True)
    pdf.cell(35, 6, "OK / CON PREV.", 1, 0, 'C', fill=True); pdf.cell(35, 6, "ALERTA / VENCIDO", 1, 0, 'C', fill=True)
    pdf.cell(40, 6, "% OK", 1, 0, 'C', fill=True); pdf.cell(40, 6, "% NO OK", 1, 1, 'C', fill=True)
    
    pdf.set_font("Arial", '', 9); pdf.set_text_color(0, 0, 0)
    for r in resumen_data:
        pdf.set_x(mx); pdf.cell(35, 6, r['CLIENTE'], 1, 0, 'C'); pdf.cell(25, 6, str(r['TOT']), 1, 0, 'C')
        pdf.cell(35, 6, str(r['OK']), 1, 0, 'C'); pdf.cell(35, 6, str(r['NOK']), 1, 0, 'C')
        pdf.cell(40, 6, r['POK'], 1, 0, 'C'); pdf.cell(40, 6, r['PNOK'], 1, 1, 'C')
        
    pdf.set_x(mx); pdf.set_font("Arial", 'B', 9); pdf.set_fill_color(220, 220, 220)
    pdf.cell(35, 6, "TOTAL", 1, 0, 'C', fill=True); pdf.cell(25, 6, str(total_gen), 1, 0, 'C', fill=True)
    pdf.cell(35, 6, str(total_ok), 1, 0, 'C', fill=True); pdf.cell(35, 6, str(total_nok), 1, 0, 'C', fill=True)
    pdf.cell(40, 6, f"{int(round(total_ok/total_gen*100))}%" if total_gen > 0 else "0%", 1, 0, 'C', fill=True)
    pdf.cell(40, 6, f"{int(round(total_nok/total_gen*100))}%" if total_gen > 0 else "0%", 1, 1, 'C', fill=True)
    
    if len(resumen_data) > 0:
        pdf.ln(5); y_charts = pdf.get_y()
        fig_gen = go.Figure(data=[go.Pie(labels=['EN REGLA', 'VENCIDO/ALERTA'], values=[total_ok, total_nok], marker_colors=['#2ca02c', '#d62728'])])
        fig_gen.update_traces(textposition='inside', textinfo='percent+label', showlegend=False)
        fig_gen.update_layout(title_text="Estado General (Total)", title_x=0.5, margin=dict(t=40, b=10, l=10, r=10), height=300, width=300)

        fig_cli = make_subplots(rows=1, cols=len(resumen_data), specs=[[{'type':'domain'}] * len(resumen_data)], subplot_titles=[r['CLIENTE'] for r in resumen_data])
        for i, r in enumerate(resumen_data):
            fig_cli.add_trace(go.Pie(labels=['EN REGLA', 'VENCIDO/ALERTA'], values=[r['OK'], r['NOK']], marker_colors=['#2ca02c', '#d62728']), 1, i + 1)
        
        fig_cli.update_traces(textposition='inside', textinfo='percent')
        fig_cli.update_layout(title_text="Desglose por Cliente", title_x=0.5, showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5), margin=dict(t=40, b=40, l=10, r=10), height=300, width=700)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_gen:
            fig_gen.write_image(tmp_gen.name, engine="kaleido"); pdf.image(tmp_gen.name, x=15, y=y_charts, w=70); os.remove(tmp_gen.name)
            
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_cli:
            fig_cli.write_image(tmp_cli.name, engine="kaleido"); pdf.image(tmp_cli.name, x=90, y=y_charts, w=190); os.remove(tmp_cli.name)
    
    buf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    pdf.output(buf.name)
    b = open(buf.name, "rb").read()
    os.remove(buf.name)
    return b

def build_excel_main(df_resultados):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_resultados.to_excel(writer, index=False, sheet_name='Estado de Matrices')
    return output.getvalue()

# ==========================================
# 6. INTERFAZ PRINCIPAL
# ==========================================
if st.button("🔄 Sincronizar Bases de Datos", use_container_width=True):
    st.cache_data.clear(); st.rerun()

with st.spinner("Conectando y procesando..."):
    df_cat, df_sql, df_forms = load_all_sources()

if not df_cat.empty:
    if st.button("⚙️ Procesar Datos de Matrices y Generar Informes", use_container_width=True, type="primary"):
        with st.spinner("Calculando estado de matrices y renderizando documentos..."):
            df_res = procesar_datos(df_cat, df_sql, df_forms)
            st.session_state['df_res'] = df_res

    if 'df_res' in st.session_state and not st.session_state['df_res'].empty:
        df_res = st.session_state['df_res']
        
        rojos = len(df_res[df_res['COLOR']=='ROJO'])
        amarillos = len(df_res[df_res['COLOR']=='AMARILLO'])
        verdes = len(df_res[df_res['COLOR']=='VERDE'])
        
        st.write("---")
        st.write(f"**Resumen de la corrida:** 🔴 {rojos} Críticas | 🟡 {amarillos} Alerta | 🟢 {verdes} OK")
        
        st.dataframe(df_res[['CLIENTE', 'PIEZA', 'OP', 'TIPO', 'ULT_PREV', 'ULT_CORR', 'GOLPES', 'ESTADO']].style.apply(lambda x: ['background-color: lightcoral' if v == 'ROJO' else 'background-color: lightgoldenrodyellow' if v == 'AMARILLO' else 'background-color: lightgreen' for v in x], subset=['ESTADO']))

        col_desc1, col_desc2, col_desc3 = st.columns(3)
        fecha_str = (datetime.utcnow() - timedelta(hours=3)).strftime('%d%m%Y')
        
        with col_desc1:
            pdf_main_data = build_pdf_main(df_res)
            st.download_button(label="📥 PDF: Resumen de Golpes", data=pdf_main_data, file_name=f"Resumen_de_Golpes_{fecha_str}.pdf", mime="application/pdf", use_container_width=True)
            
        with col_desc2:
            excel_main_data = build_excel_main(df_res)
            st.download_button(label="📥 EXCEL: Resumen de Golpes", data=excel_main_data, file_name=f"Resumen_de_Golpes_{fecha_str}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
            
        with col_desc3:
            pdf_resumen_data = build_pdf_resumen(df_res)
            st.download_button(label="📊 PDF: Gráficos y Estado General", data=pdf_resumen_data, file_name=f"Estado_Mantenimientos_{fecha_str}.pdf", mime="application/pdf", use_container_width=True)

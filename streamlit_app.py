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

st.markdown('<h2 style="text-align: center;">⚙️ Sistema de Diagnóstico y Control - Fumiscor</h2>', unsafe_allow_html=True)
st.divider()

# ==========================================
# 2. ENLACES DE DATOS
# ==========================================
URL_CATALOGO = "https://docs.google.com/spreadsheets/d/198KjQWZwfvvWwq1q1N1zv1cgzkot2hhGbwQvbi9_zFQ/export?format=csv&gid=818188145"
URL_FORMS_PREV = "https://docs.google.com/spreadsheets/d/1VqsPNhAlT1kPCltbMWsbkZNFBKdwZRFM5RAmnRV0v3c/export?format=csv&gid=1603203990"
URL_FORMS_CORR = "https://docs.google.com/spreadsheets/d/1bL_tnlSXGO_t9tKnhIHT5pZ3DAxivbiq2tFETVxBaVI/export?format=csv&gid=1507213893"

# ==========================================
# 3. FUNCIONES DE LIMPIEZA
# ==========================================
def clean_str(val):
    if pd.isna(val): return ""
    return str(val).strip().upper()

def get_match_key(texto):
    if pd.isna(texto): return ""
    val = str(texto).upper()
    val = re.sub(r'-?OP\d+', '', val) 
    matches = re.findall(r'\d{4,}', val) 
    return max(matches, key=len) if matches else re.sub(r'[^A-Z0-9]', '', val)

@st.cache_data(ttl=60)
def load_all_sources():
    # 1. CARGAR CATÁLOGO
    try:
        df_cat = pd.read_csv(URL_CATALOGO, skiprows=2).dropna(how='all')
        df_cat.columns = [c.upper().strip() for c in df_cat.columns]
        df_cat['PIEZA_KEY'] = df_cat['RH'].apply(lambda x: get_match_key(clean_str(x)))
    except Exception as e:
        st.error(f"Error Catálogo: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 2. CARGAR FORMS CON BÚSQUEDA FLEXIBLE
    def fetch_forms(url, tipo_mant):
        try:
            df_raw = pd.read_csv(url)
            if df_raw.empty: return pd.DataFrame()
            
            # Limpiar nombres de columnas
            orig_cols = df_raw.columns
            df_raw.columns = [str(c).upper().strip() for c in df_raw.columns]
            
            # --- BUSCAR COLUMNA DE FECHA ---
            col_f = next((c for c in df_raw.columns if 'FECHA' in c), None)
            if not col_f: col_f = next((c for c in df_raw.columns if 'MARCA TEMPORAL' in c), None)
            
            # --- BUSCAR COLUMNAS DE PIEZAS (Múltiples secciones) ---
            cols_pieza = [c for c in df_raw.columns if any(k in c for k in ['PIEZA', 'RH', 'LH', 'MATRIZ'])]
            
            # --- BUSCAR COLUMNAS DE TERMINADO ---
            cols_term = [c for c in df_raw.columns if 'TERMINADO' in c or 'TERMINO' in c]
            
            registros = []
            for _, row in df_raw.iterrows():
                # Fecha
                fecha = pd.to_datetime(row.get(col_f), dayfirst=True, errors='coerce') if col_f else pd.NaT
                if pd.isna(fecha): continue
                
                # Pieza (Escaneo de todas las columnas candidatas)
                pieza_raw = ""
                for cp in cols_pieza:
                    val = clean_str(row.get(cp))
                    if val and val not in ['NAN', 'NONE', '-', '0', 'N/A']:
                        pieza_raw = val
                        break
                
                if not pieza_raw: continue
                
                # Estado Abierto/Cerrado
                abierto = 'NO'
                for ct in cols_term:
                    val_t = clean_str(row.get(ct))
                    if val_t == 'NO' or val_t.startswith('NO') or 'FALSO' in val_t:
                        abierto = 'SI'
                        break
                
                registros.append({
                    'FECHA_DT': fecha, 'TIPO_MANT': tipo_mant, 'ABIERTO': abierto,
                    'PIEZA_RAW': pieza_raw, 'PIEZA_KEY': get_match_key(pieza_raw)
                })
            return pd.DataFrame(registros)
        except Exception as e:
            st.error(f"Error crítico en {tipo_mant}: {e}")
            return pd.DataFrame()

    df_prev = fetch_forms(URL_FORMS_PREV, "PREV")
    df_corr = fetch_forms(URL_FORMS_CORR, "CORR")
    df_forms_all = pd.concat([df_prev, df_corr], ignore_index=True)

    # 3. SQL
    try:
        conn = st.connection("wii_bi", type="sql")
        q = "SELECT pr.Code as PIEZA, CAST(p.Date as DATE) as FECHA, SUM(p.Good + p.Rework) as GOLPES FROM PROD_D_01 p JOIN PRODUCT pr ON p.ProductId = pr.ProductId WHERE p.Date >= '2023-01-01' GROUP BY pr.Code, CAST(p.Date as DATE)"
        df_sql = conn.query(q)
        df_sql['PIEZA_KEY'] = df_sql['PIEZA'].apply(get_match_key)
        df_sql['FECHA'] = pd.to_datetime(df_sql['FECHA'])
    except: df_sql = pd.DataFrame()

    return df_cat, df_sql, df_forms_all

# ==========================================
# 4. LÓGICA DE PROCESAMIENTO
# ==========================================
def procesar_datos(df_cat, df_sql, df_forms):
    res_semaforo = []
    res_abiertos = []
    hoy = datetime.now()
    inicio_anio = pd.to_datetime(f"{hoy.year}-01-01")

    # Mantenimientos Abiertos
    if not df_forms.empty:
        df_solo_ab = df_forms[df_forms['ABIERTO'] == 'SI']
        for _, r in df_solo_ab.iterrows():
            match = df_cat[df_cat['PIEZA_KEY'] == r['PIEZA_KEY']]
            cliente = match.iloc[0]['CLIENTE'] if not match.empty else "No Catálogo"
            res_abiertos.append({
                'CLIENTE': cliente, 'PIEZA_REPORTADA': r['PIEZA_RAW'],
                'TIPO': r['TIPO_MANT'], 'FECHA': r['FECHA_DT'].strftime('%d/%m/%Y')
            })

    # Semáforo
    for _, row in df_cat.iterrows():
        p_key = row['PIEZA_KEY']
        if not row['RH'] or row['RH'] == '-': continue
        
        # Fecha de Excel
        f_excel = pd.to_datetime(row.get('ULTIMO MANTENIMIENTO'), dayfirst=True, errors='coerce')
        # Fecha de Forms (Solo cerrados para resetear)
        f_form = pd.NaT
        if not df_forms.empty:
            match_f = df_forms[(df_forms['PIEZA_KEY'] == p_key) & (df_forms['ABIERTO'] == 'NO')]
            if not match_f.empty: f_form = match_f['FECHA_DT'].max()

        # Determinar fecha de corte y golpes base
        g_base = pd.to_numeric(row.get('GOLPES'), errors='coerce') or 0
        if pd.notna(f_form) and (pd.isna(f_excel) or f_form > f_excel):
            f_final, g_acum = f_form, 0 # Reset
            prod = df_sql[(df_sql['PIEZA_KEY'] == p_key) & (df_sql['FECHA'] >= f_final)]
            g_total = int(prod['GOLPES'].sum())
        else:
            f_final = f_excel
            prod = df_sql[(df_sql['PIEZA_KEY'] == p_key) & (df_sql['FECHA'] >= inicio_anio)]
            g_total = int(g_base) + int(prod['GOLPES'].sum())

        limite = 20000
        color = "ROJO" if g_total >= limite else "AMARILLO" if g_total >= (limite*0.8) else "VERDE"
        
        res_semaforo.append({
            'CLIENTE': row['CLIENTE'], 'PIEZA': row['RH'], 
            'ULT_MANT': f_final.strftime('%d/%m/%Y') if pd.notna(f_final) else "-",
            'GOLPES': g_total, 'COLOR': color
        })

    return pd.DataFrame(res_semaforo), pd.DataFrame(res_abiertos)

# ==========================================
# 5. INTERFAZ
# ==========================================
if st.button("🔄 Sincronizar y Limpiar Caché"):
    st.cache_data.clear(); st.rerun()

df_cat, df_sql, df_forms = load_all_sources()

if not df_cat.empty:
    # --- SECCIÓN DE DIAGNÓSTICO ---
    with st.expander("🛠️ PANEL DE DIAGNÓSTICO (Ver si los Forms cargan)"):
        st.write(f"Total filas en Forms: {len(df_forms)}")
        if not df_forms.empty:
            st.write("Últimas 5 filas procesadas de los Formularios:")
            st.dataframe(df_forms.tail())
        else:
            st.warning("No se procesaron registros. Revisa si las columnas del Formulario contienen las palabras 'PIEZA' o 'FECHA'.")

    if st.button("⚙️ Generar Informe Actualizado", type="primary", use_container_width=True):
        df_res, df_ab = procesar_datos(df_cat, df_sql, df_forms)
        
        st.write("---")
        col1, col2, col3 = st.columns(3)
        col1.metric("Análisis Catálogo", len(df_res))
        col2.metric("Mantenimientos Abiertos", len(df_ab))
        col3.metric("Datos Forms", len(df_forms))

        if not df_ab.empty:
            st.error("⚠️ MANTENIMIENTOS ABIERTOS DETECTADOS")
            st.table(df_ab)
        
        st.success("Informe generado. El PDF incluiría estas tablas actualizadas.")

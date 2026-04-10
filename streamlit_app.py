import streamlit as st
import pandas as pd
from datetime import datetime
import tempfile
import os
import re
from difflib import SequenceMatcher
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
# 3. FUNCIONES DE LIMPIEZA Y COINCIDENCIA
# ==========================================
def clean_str(val):
    if pd.isna(val): return ""
    return str(val).strip().upper()

def get_best_match(texto, lista_candidatos, umbral=0.5):
    """
    Encuentra la mejor coincidencia basada en la cantidad de caracteres compartidos.
    """
    if pd.isna(texto) or not str(texto).strip(): return ""
    val = clean_str(texto)
    
    mejor_coincidencia = val
    mejor_puntaje = 0.0
    
    for candidato in lista_candidatos:
        cand_str = clean_str(candidato)
        if not cand_str: continue
        
        # Evalúa el ratio de caracteres compartidos entre el texto y el candidato
        puntaje = SequenceMatcher(None, val, cand_str).ratio()
        
        if puntaje > mejor_puntaje:
            mejor_puntaje = puntaje
            mejor_coincidencia = cand_str
            
    # Si la similitud supera el umbral, devolvemos el RH oficial del catálogo
    if mejor_puntaje >= umbral:
        return mejor_coincidencia
    return val # Si no alcanza el umbral, dejamos el texto original

@st.cache_data(ttl=60)
def load_all_sources():
    # ==========================================
    # A. CARGAR CATÁLOGO (Listado de Matrices)
    # ==========================================
    try:
        df_cat_raw = pd.read_csv(URL_CATALOGO)
        
        # Buscar la fila de encabezados dinámicamente
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

    # Lista oficial de piezas para buscar coincidencias
    catalogo_piezas = df_cat['PIEZA_KEY'].unique().tolist()

    # ==========================================
    # B. CARGAR FORMS CON BÚSQUEDA DINÁMICA
    # ==========================================
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
            
            # Columnas clave
            col_f = next((c for c in df_raw.columns if 'FECHA' in c or 'MARCA TEMPORAL' in c), None)
            palabras_clave_pieza = ['PIEZA', 'RH', 'LH', 'MATRIZ', 'CÓDIGO']
            cols_pieza = [c for c in df_raw.columns if any(k in c for k in palabras_clave_pieza)]
            cols_term = [c for c in df_raw.columns if 'TERMINADO' in c or 'TERMINO' in c or 'ESTADO' in c]
            
            if not col_f or not cols_pieza:
                st.warning(f"⚠️ En {tipo_mant} no detecté las columnas. Leídas: {', '.join(df_raw.columns[:5])}...")
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
                
                abierto = 'NO'
                for ct in cols_term:
                    val_t = clean_str(row.get(ct))
                    if val_t == 'NO' or val_t.startswith('NO') or 'FALSO' in val_t:
                        abierto = 'SI'
                        break
                
                # Asignamos la pieza comparando similitud con el catálogo
                pieza_key = get_best_match(pieza_raw, catalogo_piezas)

                registros.append({
                    'FECHA_DT': fecha, 'TIPO_MANT': tipo_mant, 'ABIERTO': abierto,
                    'PIEZA_RAW': pieza_raw, 'PIEZA_KEY': pieza_key
                })
            return pd.DataFrame(registros)
        except Exception as e:
            st.error(f"Error crítico en {tipo_mant}: {e}")
            return pd.DataFrame()

    df_prev = fetch_forms(URL_FORMS_PREV, "PREV")
    df_corr = fetch_forms(URL_FORMS_CORR, "CORR")
    df_forms_all = pd.concat([df_prev, df_corr], ignore_index=True) if not df_prev.empty or not df_corr.empty else pd.DataFrame()

    # ==========================================
    # C. SQL (Base de Datos de Producción)
    # ==========================================
    try:
        conn = st.connection("wii_bi", type="sql")
        q = "SELECT pr.Code as PIEZA, CAST(p.Date as DATE) as FECHA, SUM(p.Good + p.Rework) as GOLPES FROM PROD_D_01 p JOIN PRODUCT pr ON p.ProductId = pr.ProductId WHERE p.Date >= '2023-01-01' GROUP BY pr.Code, CAST(p.Date as DATE)"
        df_sql = conn.query(q)
        df_sql['FECHA'] = pd.to_datetime(df_sql['FECHA'])
        
        # Mapeo optimizado: Buscamos coincidencias solo para los valores únicos de SQL y luego asignamos
        piezas_unicas_sql = df_sql['PIEZA'].unique()
        mapeo_piezas = {p: get_best_match(p, catalogo_piezas) for p in piezas_unicas_sql}
        df_sql['PIEZA_KEY'] = df_sql['PIEZA'].map(mapeo_piezas)
        
    except Exception as e: 
        st.warning(f"⚠️ No se pudo conectar a la base de datos SQL. Verifica las credenciales. Error: {e}")
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

    # Semáforo de Golpes
    for _, row in df_cat.iterrows():
        p_key = row['PIEZA_KEY']
        if not row['RH'] or row['RH'] == '-': continue
        
        # Fecha base de Excel
        f_excel = pd.to_datetime(row.get('ULTIMO MANTENIMIENTO'), dayfirst=True, errors='coerce')
        
        # Fecha de Forms (Solo cerrados)
        f_form = pd.NaT
        if not df_forms.empty:
            match_f = df_forms[(df_forms['PIEZA_KEY'] == p_key) & (df_forms['ABIERTO'] == 'NO')]
            if not match_f.empty: f_form = match_f['FECHA_DT'].max()

        # Determinar fecha de corte y calcular golpes
        g_base = pd.to_numeric(row.get('GOLPES'), errors='coerce')
        g_base = g_base if pd.notna(g_base) else 0

        if pd.notna(f_form) and (pd.isna(f_excel) or f_form > f_excel):
            f_final = f_form
            # Sumar producción desde la fecha del último mantenimiento
            if not df_sql.empty:
                prod = df_sql[(df_sql['PIEZA_KEY'] == p_key) & (df_sql['FECHA'] >= f_final)]
                g_total = int(prod['GOLPES'].sum())
            else:
                g_total = 0
        else:
            f_final = f_excel
            if not df_sql.empty:
                prod = df_sql[(df_sql['PIEZA_KEY'] == p_key) & (df_sql['FECHA'] >= inicio_anio)]
                g_total = int(g_base) + int(prod['GOLPES'].sum())
            else:
                g_total = int(g_base)

        limite = 20000
        color = "ROJO" if g_total >= limite else "AMARILLO" if g_total >= (limite*0.8) else "VERDE"
        
        res_semaforo.append({
            'CLIENTE': row.get('CLIENTE', '-'), 'PIEZA': row['RH'], 
            'ULT_MANT': f_final.strftime('%d/%m/%Y') if pd.notna(f_final) else "-",
            'GOLPES': g_total, 'COLOR': color
        })

    return pd.DataFrame(res_semaforo), pd.DataFrame(res_abiertos)

# ==========================================
# 5. INTERFAZ
# ==========================================
if st.button("🔄 Sincronizar y Limpiar Caché"):
    st.cache_data.clear()
    st.rerun()

df_cat, df_sql, df_forms = load_all_sources()

if not df_cat.empty:
    with st.expander("🛠️ PANEL DE DIAGNÓSTICO (Ver si los datos cargan)"):
        st.write(f"Total filas en Catálogo procesadas: {len(df_cat)}")
        st.write(f"Total filas en Forms procesadas: {len(df_forms)}")
        st.write(f"Total filas en SQL procesadas: {len(df_sql)}")
        if not df_forms.empty:
            st.write("Últimas 5 filas procesadas de los Formularios:")
            st.dataframe(df_forms[['FECHA_DT', 'PIEZA_RAW', 'PIEZA_KEY', 'ABIERTO']].tail())

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
        
        st.success("Informe generado con éxito.")
        st.dataframe(df_res)

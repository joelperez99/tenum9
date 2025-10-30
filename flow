# streamlit_app.py
# 🎾 API Tennis → Snowflake sin External Access (Streamlit hace la llamada HTTP)
# - Elige DB/Schema/Tabla desde la UI (por defecto: TENNIS_DB.RAW.RAW_TENNIS_MATCH_KEYS)
# - Trae fixtures por fecha y timezone desde https://api.api-tennis.com
# - Si el entorno bloquea internet, sube un JSON (payload del API) y guarda igual
# - Inserta/actualiza por (source_date, timezone_used): borra y carga

import io
import json
import datetime as dt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# -------------------------------
# Configuración básica de página
# -------------------------------
st.set_page_config(page_title="Tennis Match Keys → Snowflake", layout="wide")
st.title("🎾 Tennis Match Keys → Snowflake (sin External Access)")
st.caption("Streamlit hace la llamada HTTP. Si la red está bloqueada, usa la carga por JSON.")

# -------------------------------
# Conexión Snowflake
# -------------------------------
session = get_active_session()

def run_sql(sql: str):
    return session.sql(sql).collect()

def set_context(warehouse: str, db: str, schema: str):
    run_sql(f"use warehouse {warehouse}")
    run_sql(f"create database if not exists {db}")
    run_sql(f"use database {db}")
    run_sql(f"create schema if not exists {db}.{schema}")
    run_sql(f"use schema {schema}")

def table_fqn(db: str, schema: str, table: str) -> str:
    return f'{db}.{schema}.{table}'

def ensure_table(db: str, schema: str, table: str):
    tbl = table_fqn(db, schema, table)
    run_sql(f"""
        create table if not exists {tbl} (
          event_key         string,
          event_date        string,
          event_time        string,
          first_player      string,
          second_player     string,
          tournament_name   string,
          event_type_type   string,
          event_status      string,
          source_date       date,
          timezone_used     string,
          _ingested_at      timestamp_ntz default current_timestamp()
        )
    """)

def df_to_csv_download(df: pd.DataFrame, filename: str, label: str = "⬇️ Descargar CSV"):
    if df is None or df.empty:
        return
    st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        use_container_width=True
    )

# -------------------------------
# Parámetros UI
# -------------------------------
with st.sidebar:
    st.header("⚙️ Contexto Snowflake")
    warehouse = st.text_input("Warehouse", value="COMPUTE_WH")
    db = st.text_input("Database", value="TENNIS_DB")
    schema = st.text_input("Schema", value="RAW")
    table = st.text_input("Tabla", value="RAW_TENNIS_MATCH_KEYS")
    st.caption("Se crearán DB/Schema/Tabla si no existen (si tu rol lo permite).")

colA, colB, colC, colD = st.columns([1, 1, 1, 2])
with colA:
    fecha = st.date_input("Fecha", value=dt.date.today(), format="YYYY-MM-DD")
with colB:
    timezone = st.text_input("Timezone", value="America/Monterrey")
with colC:
    api_key = st.text_input("API Key", type="password", help="Tu API key de api-tennis.com")
with colD:
    st.write("")

date_str = fecha.strftime("%Y-%m-%d")
tbl_fqn = table_fqn(db, schema, table)

st.markdown("---")

# -------------------------------
# Funciones de negocio
# -------------------------------
import requests

BASE_URL = "https://api.api-tennis.com/tennis/"
HEADERS = {"Accept": "application/json"}

def fetch_from_api(api_key: str, date_str: str, tz: str):
    params = {
        "method": "get_fixtures",
        "APIkey": api_key,
        "date_start": date_str,
        "date_stop":  date_str,
        "timezone":   tz
    }
    r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=40)
    r.raise_for_status()
    return r.json()

def normalize_result(result_list):
    rows = []
    for item in result_list or []:
        rows.append({
            "event_key":       str(item.get("event_key") or item.get("match_key") or ""),
            "event_date":      item.get("event_date", ""),
            "event_time":      item.get("event_time", ""),
            "first_player":    item.get("event_first_player", ""),
            "second_player":   item.get("event_second_player", ""),
            "tournament_name": item.get("tournament_name", ""),
            "event_type_type": item.get("event_type_type", ""),
            "event_status":    item.get("event_status", "")
        })
    return rows

def upsert_rows(df: pd.DataFrame, date_str: str, tz: str):
    # Simple: delete + insert por (source_date, timezone_used)
    run_sql(f"""
        delete from {tbl_fqn}
        where source_date = to_date('{date_str}')
          and timezone_used = '{tz}'
    """)
    if not df.empty:
        # Agrega columnas control
        df = df.copy()
        df["source_date"] = date_str
        df["timezone_used"] = tz
        session.write_pandas(df, tbl_fqn, auto_create_table=False)

# -------------------------------
# Acciones
# -------------------------------
b1, b2, b3 = st.columns([1.4, 1.4, 2])
with b1:
    do_fetch = st.button("📥 Traer desde API", use_container_width=True)
with b2:
    do_save = st.button("💾 Guardar en Snowflake", use_container_width=True)
with b3:
    st.write("")

st.info(f"Destino: **{tbl_fqn}**")

# Asegurar contexto y tabla
try:
    set_context(warehouse, db, schema)
    ensure_table(db, schema, table)
except Exception as e:
    st.error(f"Error de contexto/DDL: {e}")

session_state_key = "df_preview"
if session_state_key not in st.session_state:
    st.session_state[session_state_key] = pd.DataFrame()

df_preview: pd.DataFrame = st.session_state[session_state_key]

# 1) Traer datos desde API
if do_fetch:
    if not api_key.strip():
        st.warning("Ingresa tu **API Key**.")
    else:
        with st.spinner("Llamando API…"):
            try:
                payload = fetch_from_api(api_key.strip(), date_str, timezone.strip())
                ok = bool(payload.get("success") == 1)
                if not ok:
                    st.error(f"API success!=1. Respuesta: {payload}")
                else:
                    rows = normalize_result(payload.get("result") or [])
                    df_preview = pd.DataFrame(rows)
                    st.session_state[session_state_key] = df_preview
                    st.success(f"OK. {len(df_preview)} partidos para {date_str} ({timezone}).")
            except Exception as e:
                st.error(f"No se pudo llamar al API desde Streamlit.\n"
                         f"Posible bloqueo de red en tu cuenta Snowflake.\n\nDetalle: {e}")

# 2) Plan B: subir JSON si no hay internet
st.markdown("#### 📄 Plan B: Subir JSON del API (payload)")
st.caption("Si tu cuenta bloquea internet, pega el JSON del endpoint o súbelo como archivo.")
uploaded = st.file_uploader("Sube un archivo .json", type=["json"])
raw_json_text = st.text_area("…o pega el JSON aquí")

if st.button("➡️ Procesar JSON", use_container_width=True):
    try:
        data = None
        if uploaded is not None:
            data = json.load(uploaded)
        elif raw_json_text.strip():
            data = json.loads(raw_json_text)
        else:
            st.warning("Sube un archivo o pega JSON.")
            data = None

        if data is not None:
            ok = bool(data.get("success") == 1)
            if not ok:
                st.error(f"JSON no tiene success==1. Respuesta: {data}")
            else:
                rows = normalize_result(data.get("result") or [])
                df_preview = pd.DataFrame(rows)
                st.session_state[session_state_key] = df_preview
                st.success(f"JSON procesado. {len(df_preview)} partidos.")
    except Exception as e:
        st.error(f"JSON inválido: {e}")

st.markdown("---")

# Vista previa + descarga
st.subheader("📊 Vista previa")
if df_preview is not None and not df_preview.empty:
    st.dataframe(df_preview, use_container_width=True, height=420)
    df_to_csv_download(df_preview, f"match_keys_{date_str}.csv")
else:
    st.info("No hay datos todavía. Usa **Traer desde API** o **Procesar JSON**.")

# Guardar en Snowflake
if do_save:
    if df_preview is None or df_preview.empty:
        st.warning("Primero trae o sube datos.")
    else:
        with st.spinner("Guardando en Snowflake…"):
            try:
                upsert_rows(df_preview, date_str, timezone.strip())
                st.success(f"Guardado en {tbl_fqn}.")
            except Exception as e:
                st.error(f"Error al guardar: {e}")

st.markdown("---")

# Consulta rápida
st.subheader("🔎 Consultar en Snowflake")
lim = st.number_input("Límite", min_value=1, max_value=10000, value=200, step=50)
query = f"""
select
  event_key, event_date, event_time, first_player, second_player,
  tournament_name, event_type_type, event_status,
  source_date, timezone_used, _ingested_at
from {tbl_fqn}
where source_date = to_date('{date_str}')
  and timezone_used = '{timezone}'
order by tournament_name, event_time, event_key
limit {int(lim)}
"""
st.code(query.strip(), language="sql")

try:
    df_db = session.sql(query).to_pandas()
    st.dataframe(df_db, use_container_width=True, height=360)
except Exception as e:
    st.warning(f"No se pudo consultar: {e}")

st.caption("Tip: si el botón 'Traer desde API' falla por red, usa el Plan B (sube JSON) y luego 'Guardar en Snowflake'.")

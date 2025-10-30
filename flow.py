# streamlit_app.py
# ✅ Funciona en Snowflake Streamlit
# 🎾 Tennis API → Snowflake Table (sin external access)
# Plan A: Llama API desde Streamlit (si tu Snowflake lo permite)
# Plan B: Subes JSON con los partidos → se guardan igual

import streamlit as st
import pandas as pd
import requests
import datetime as dt
import json
from snowflake.snowpark.context import get_active_session

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
st.set_page_config(page_title="🎾 Tennis Loader", layout="wide")
st.title("🎾 Cargar Match Keys de Tennis a Snowflake")

session = get_active_session()

# ----------------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------------
def run_sql(sql):
    return session.sql(sql).collect()

def ensure_table(db, schema, table):
    run_sql(f"create database if not exists {db}")
    run_sql(f"use database {db}")
    run_sql(f"create schema if not exists {db}.{schema}")
    run_sql(f"use schema {schema}")
    run_sql(f"""
        create table if not exists {db}.{schema}.{table} (
            event_key string,
            event_date string,
            event_time string,
            first_player string,
            second_player string,
            tournament_name string,
            event_type_type string,
            event_status string,
            source_date date,
            timezone_used string,
            _ingested_at timestamp_ntz default current_timestamp()
        )
    """)

def delete_existing(db, schema, table, date, tz):
    run_sql(f"""
        delete from {db}.{schema}.{table}
        where source_date = to_date('{date}')
          and timezone_used = '{tz}'
    """)

def insert_df(df, db, schema, table, date, tz):
    df2 = df.copy()
    df2["source_date"] = date
    df2["timezone_used"] = tz
    session.write_pandas(df2, f"{db}.{schema}.{table}", auto_create_table=False)

def fetch_api(api_key, date, tz):
    URL = "https://api.api-tennis.com/tennis/"
    params = {
        "method":"get_fixtures",
        "APIkey":api_key,
        "date_start":date,
        "date_stop":date,
        "timezone":tz
    }
    r = requests.get(URL, params=params, timeout=40)
    r.raise_for_status()
    return r.json()

def normalize(data):
    rows=[]
    for x in data:
        rows.append({
            "event_key": str(x.get("event_key") or x.get("match_key") or ""),
            "event_date": x.get("event_date",""),
            "event_time": x.get("event_time",""),
            "first_player": x.get("event_first_player",""),
            "second_player": x.get("event_second_player",""),
            "tournament_name": x.get("tournament_name",""),
            "event_type_type": x.get("event_type_type",""),
            "event_status": x.get("event_status","")
        })
    return pd.DataFrame(rows)

# ----------------------------------------------------------------------------
# UI
# ----------------------------------------------------------------------------
with st.sidebar:
    st.header("⚙️ Configuración Snowflake")
    warehouse = st.text_input("Warehouse", "COMPUTE_WH")
    db = st.text_input("Database", "TENNIS_DB")
    schema = st.text_input("Schema", "RAW")
    table = st.text_input("Table", "RAW_TENNIS_MATCH_KEYS")

    st.header("🌎 API Tennis")
    api_key = st.text_input("API Key", type="password")

col1,col2,col3,_ = st.columns([1,1,1,2])
with col1:
    fecha = st.date_input("Fecha", dt.date.today(), format="YYYY-MM-DD")
with col2:
    timezone = st.text_input("Timezone", "America/Monterrey")
with col3:
    st.write("")

date_str = fecha.strftime("%Y-%m-%d")

# ----------------------------------------------------------------------------
# Setup DB + Table
# ----------------------------------------------------------------------------
try:
    run_sql(f"use warehouse {warehouse}")
    ensure_table(db, schema, table)
    st.success(f"Conectado a ➜ `{db}.{schema}.{table}`")
except Exception as e:
    st.error(f"❌ Error conectando/creando tabla: {e}")

# ----------------------------------------------------------------------------
# Actions
# ----------------------------------------------------------------------------
btn_api  = st.button("📥 Traer desde API")
btn_save = st.button("💾 Guardar en Snowflake")

st.subheader("📄 Plan B: Subir JSON del API (si no hay internet)")
upload = st.file_uploader("Sube archivo JSON", type=["json"])

# Memory
if "df_buf" not in st.session_state:
    st.session_state.df_buf = pd.DataFrame()

# ----------------------------------------------------------------------------
# API FETCH
# ----------------------------------------------------------------------------
if btn_api:
    if not api_key.strip():
        st.warning("Ingresa API Key primero.")
    else:
        try:
            st.info("Llamando API...")
            data = fetch_api(api_key.strip(), date_str, timezone.strip())
            if data.get("success") != 1:
                st.error(f"API devolvió error: {data}")
            else:
                st.session_state.df_buf = normalize(data.get("result", []))
                st.success(f"✅ {len(st.session_state.df_buf)} partidos obtenidos")
        except Exception as e:
            st.error(f"❌ Error llamando API desde Snowflake Streamlit.\n"
                     f"Tu cuenta puede no tener salida de red.\n\n{e}")

# ----------------------------------------------------------------------------
# JSON Upload
# ----------------------------------------------------------------------------
if upload:
    try:
        raw = json.load(upload)
        if raw.get("success") != 1:
            st.error("JSON no contiene success=1")
        else:
            st.session_state.df_buf = normalize(raw.get("result", []))
            st.success(f"✅ {len(st.session_state.df_buf)} partidos cargados desde JSON")
    except Exception as e:
        st.error(f"❌ JSON inválido: {e}")

# ----------------------------------------------------------------------------
# Preview
# ----------------------------------------------------------------------------
st.subheader("📊 Vista previa")
df = st.session_state.df_buf

if df.empty:
    st.info("Sin datos aún.")
else:
    st.dataframe(df, use_container_width=True, height=400)
    st.download_button(
        "⬇️ Descargar CSV",
        df.to_csv(index=False).encode("utf-8"),
        f"match_keys_{date_str}.csv"
    )

# ----------------------------------------------------------------------------
# Save
# ----------------------------------------------------------------------------
if btn_save:
    if df.empty:
        st.warning("No hay datos para guardar")
    else:
        try:
            delete_existing(db, schema, table, date_str, timezone.strip())
            insert_df(df, db, schema, table, date_str, timezone.strip())
            st.success("✅ Datos guardados en Snowflake")
        except Exception as e:
            st.error(f"❌ Error guardando: {e}")

# ----------------------------------------------------------------------------
# Query
# ----------------------------------------------------------------------------
st.subheader("🔎 Ver datos en Snowflake")
limit = st.number_input("Límite", 1, 10000, 100)

q = f"""
select *
from {db}.{schema}.{table}
where source_date = to_date('{date_str}')
  and timezone_used = '{timezone}'
order by tournament_name, event_time
limit {limit}
"""
st.code(q, language="sql")

try:
    df_db = session.sql(q).to_pandas()
    st.dataframe(df_db, use_container_width=True, height=350)
except:
    st.warning("Aún sin datos o error en consulta.")


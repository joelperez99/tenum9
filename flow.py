# streamlit_app.py
# ✅ Funciona en Snowsight (Snowflake Streamlit)
# ❌ No usar en Streamlit Cloud / local
# 🎾 Tennis match keys → Snowflake con opción API / JSON fallback

import streamlit as st
import pandas as pd
import requests
import datetime as dt
import json

# ----------------------------------------------------------------------------
# ✅ Intento obtener la sesión activa de Snowflake (solo existe en Snowsight)
# ----------------------------------------------------------------------------
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    st.error(
        "❌ Error: Esta app debe ejecutarse dentro de Snowflake Snowsight.\n\n"
        "**Ir a:** Snowflake → Snowsight → Projects & Apps → Streamlit → Create App"
    )
    st.stop()

# ----------------------------------------------------------------------------
# Funciones SQL helper
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

# ----------------------------------------------------------------------------
# API Tennis helpers
# ----------------------------------------------------------------------------
def fetch_api(api_key, date, tz):
    url = "https://api.api-tennis.com/tennis/"
    params = {
        "method":"get_fixtures",
        "APIkey":api_key,
        "date_start":date,
        "date_stop":date,
        "timezone":tz
    }
    r = requests.get(url, params=params, timeout=40)
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
# UI Layout
# ----------------------------------------------------------------------------
st.set_page_config(page_title="🎾 Tennis Loader", layout="wide")
st.title("🎾 Tennis Match Keys → Snowflake")

with st.sidebar:
    st.header("⚙️ Snowflake Config")
    warehouse = st.text_input("Warehouse", "COMPUTE_WH")
    db = st.text_input("Database", "TENNIS_DB")
    schema = st.text_input("Schema", "RAW")
    table = st.text_input("Table", "RAW_TENNIS_MATCH_KEYS")

    st.header("🌍 API Tennis")
    api_key = st.text_input("API Key", type="password", help="Tu API key de api-tennis.com")

colA,colB,_ = st.columns([1,1,2])
with colA:
    fecha = st.date_input("Fecha", dt.date.today())
with colB:
    timezone = st.text_input("Timezone", "America/Monterrey")

date_str = fecha.strftime("%Y-%m-%d")

# ----------------------------------------------------------------------------
# Ensure DB Objects
# ----------------------------------------------------------------------------
try:
    run_sql(f"use warehouse {warehouse}")
    ensure_table(db, schema, table)
    st.success(f"✅ Conectado a {db}.{schema}.{table}")
except Exception as e:
    st.error(f"❌ Error conectando a Snowflake:\n{e}")
    st.stop()

# ----------------------------------------------------------------------------
# Buttons
# ----------------------------------------------------------------------------
st.markdown("### 📥 Obtener datos")
btn_api = st.button("📡 Traer desde API (si Snowflake tiene internet)")
btn_save = st.button("💾 Guardar en Snowflake")

st.markdown("---")
st.markdown("### 📄 Plan B: Subir archivo JSON (si Snowflake NO tiene internet)")
upload = st.file_uploader("Subir JSON del API", type=["json"])

# ----------------------------------------------------------------------------
# State store
# ----------------------------------------------------------------------------
if "df_buf" not in st.session_state:
    st.session_state.df_buf = pd.DataFrame()

# ----------------------------------------------------------------------------
# Fetch API
# ----------------------------------------------------------------------------
if btn_api:
    if not api_key:
        st.warning("Ingresa tu API key.")
    else:
        try:
            st.info("Llamando API…")
            payload = fetch_api(api_key.strip(), date_str, timezone.strip())
            if payload.get("success") != 1:
                st.error(f"API devolvió error:\n{payload}")
            else:
                st.session_state.df_buf = normalize(payload.get("result", []))
                st.success(f"✅ {len(st.session_state.df_buf)} partidos recibidos")
        except Exception as e:
            st.error(f"❌ Snowflake no tiene salida a internet.\nUsa JSON upload.\n\n{e}")

# ----------------------------------------------------------------------------
# JSON upload fallback
# ----------------------------------------------------------------------------
if upload:
    try:
        raw = json.load(upload)
        if raw.get("success") != 1:
            st.error("JSON no contiene success==1")
        else:
            st.session_state.df_buf = normalize(raw.get("result", []))
            st.success(f"✅ {len(st.session_state.df_buf)} partidos cargados desde JSON")
    except Exception as e:
        st.error(f"❌ Error leyendo JSON:\n{e}")

# ----------------------------------------------------------------------------
# Preview
# ----------------------------------------------------------------------------
st.subheader("📊 Vista previa")
df = st.session_state.df_buf

if df.empty:
    st.info("Sin datos todavía. Trae desde API o sube JSON.")
else:
    st.dataframe(df, use_container_width=True, height=400)
    st.download_button(
        "⬇️ Descargar CSV", df.to_csv(index=False).encode(),
        file_name=f"match_keys_{date_str}.csv"
    )

# ----------------------------------------------------------------------------
# Save to Snowflake
# ----------------------------------------------------------------------------
if btn_save:
    if df.empty:
        st.warning("No hay datos para guardar.")
    else:
        try:
            delete_existing(db, schema, table, date_str, timezone.strip())
            insert_df(df, db, schema, table, date_str, timezone.strip())
            st.success("✅ Datos guardados correctamente")
        except Exception as e:
            st.error(f"❌ Error guardando en Snowflake:\n{e}")

# ----------------------------------------------------------------------------
# Query results
# ----------------------------------------------------------------------------
st.markdown("---")
st.subheader("🔎 Consultar datos guardados")
limit = st.number_input("Límite", min_value=1, value=200)

query = f"""
select *
from {db}.{schema}.{table}
where source_date = to_date('{date_str}')
  and timezone_used = '{timezone}'
order by tournament_name, event_time
limit {limit}
"""

st.code(query, language="sql")

try:
    st.dataframe(session.sql(query).to_pandas(), use_container_width=True, height=300)
except:
    st.info("Aún no hay datos para esta fecha.")

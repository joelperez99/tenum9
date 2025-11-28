import os
import json
import requests
import datetime as dt
import pandas as pd
import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

st.set_page_config(page_title="🎾 Tennis → Snowflake", layout="wide")
st.title("🎾 Cargar Match Keys (API Tennis) → Snowflake")

# -----------------------------
# Helpers credenciales
# -----------------------------
def _get_secret(name, default=""):
    try:
        return st.secrets[name]
    except Exception:
        return os.getenv(name, default)

SF_ACCOUNT   = _get_secret("SF_ACCOUNT")
SF_USER      = _get_secret("SF_USER")
SF_PASSWORD  = _get_secret("SF_PASSWORD")
SF_ROLE      = _get_secret("SF_ROLE", "ACCOUNTADMIN")
SF_WAREHOUSE = _get_secret("SF_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE  = _get_secret("SF_DATABASE", "TENNIS_DB")
SF_SCHEMA    = _get_secret("SF_SCHEMA", "RAW")
SF_TABLE     = _get_secret("SF_TABLE", "RAW_TENNIS_MATCH_KEYS")

# -----------------------------
# Conexión Snowflake
# -----------------------------
@st.cache_resource(show_spinner=False)
def get_sf_conn():
    if not (SF_ACCOUNT and SF_USER and SF_PASSWORD):
        raise RuntimeError("Faltan credenciales SF_ACCOUNT/SF_USER/SF_PASSWORD en Secrets.")
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )

def sf_exec(cnx, sql):
    cur = cnx.cursor()
    try:
        cur.execute(sql)
        try:
            return cur.fetchall()
        except Exception:
            return []
    finally:
        cur.close()

def ensure_objects(cnx):
    sf_exec(cnx, f"create database if not exists {SF_DATABASE}")
    sf_exec(cnx, f"create schema if not exists {SF_DATABASE}.{SF_SCHEMA}")
    sf_exec(cnx, f"use database {SF_DATABASE}")
    sf_exec(cnx, f"use schema {SF_SCHEMA}")
    sf_exec(cnx, f"""
        create table if not exists {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (
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

def delete_partition(cnx, date_str, timezone):
    sf_exec(cnx, f"""
        delete from {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
        where source_date = to_date('{date_str}')
          and timezone_used = '{timezone}'
    """)

def insert_df(cnx, df):
    write_pandas(
        conn=cnx,
        df=df,
        table_name=SF_TABLE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )

# -----------------------------
# API Tennis
# -----------------------------
BASE_URL = "https://api.api-tennis.com/tennis/"

def fetch_api(api_key: str, date_str: str, timezone: str) -> dict:
    r = requests.get(
        BASE_URL,
        params={
            "method": "get_fixtures",
            "APIkey": api_key,
            "date_start": date_str,
            "date_stop": date_str,
            "timezone": timezone
        },
        timeout=40
    )
    r.raise_for_status()
    return r.json()

def normalize_result(result_list):
    rows = []
    for it in (result_list or []):
        rows.append({
            "event_key":       str(it.get("event_key") or it.get("match_key") or ""),
            "event_date":      it.get("event_date", ""),
            "event_time":      it.get("event_time", ""),
            "first_player":    it.get("event_first_player", ""),
            "second_player":   it.get("event_second_player", ""),
            "tournament_name": it.get("tournament_name", ""),
            "event_type_type": it.get("event_type_type", ""),
            "event_status":    it.get("event_status", "")
        })
    return pd.DataFrame(rows)

# -----------------------------
# UI
# -----------------------------
with st.sidebar:
    st.header("🌍 API Tennis")
    api_key = st.text_input("API Key", type="password", help="Tu API key de api-tennis.com")
    fecha = st.date_input("Fecha", value=dt.date.today(), format="YYYY-MM-DD")
    timezone = st.text_input("Timezone", value="America/Monterrey")

date_str = fecha.strftime("%Y-%m-%d")

col1, col2, col3 = st.columns([1.2, 1.2, 2])
with col1:
    do_fetch = st.button("📡 Traer desde API")
with col2:
    do_save = st.button("💾 Guardar en Snowflake")

st.markdown("#### 📄 Plan B: subir JSON del API")
upl = st.file_uploader("Archivo .json", type=["json"])

if "df_buf" not in st.session_state:
    st.session_state.df_buf = pd.DataFrame()

# -----------------------------
# Acciones
# -----------------------------
if do_fetch:
    if not api_key.strip():
        st.warning("Ingresa tu API Key.")
    else:
        try:
            payload = fetch_api(api_key.strip(), date_str, timezone.strip())
            if payload.get("success") != 1:
                st.error(f"Error de API: {payload}")
            else:
                st.session_state.df_buf = normalize_result(payload.get("result"))
                st.success(f"OK. {len(st.session_state.df_buf)} partidos.")
        except Exception as e:
            st.error(f"Error llamando API: {e}")

if upl is not None:
    try:
        data = json.load(upl)
        if data.get("success") != 1:
            st.error("JSON no contiene success=1")
        else:
            st.session_state.df_buf = normalize_result(data.get("result"))
            st.success(f"JSON cargado. {len(st.session_state.df_buf)} partidos.")
    except Exception as e:
        st.error(f"JSON inválido: {e}")

st.markdown("---")
st.subheader("📊 Vista previa")

df = st.session_state.df_buf

if df.empty:
    st.info("Sin datos aún.")
else:
    st.dataframe(df, use_container_width=True, height=420)

    # ================================
    # 🔵 NUEVO BOTÓN — Copiar Match Keys
    # ================================
    matchkeys_str = "\n".join(df["event_key"].astype(str).tolist())

    if st.button("📋 Copiar Match Keys"):
        st.copy_to_clipboard(matchkeys_str)
        st.success("Match Keys copiados al portapapeles ✔️")

    st.download_button(
        "⬇️ Descargar CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"match_keys_{date_str}.csv",
        mime="text/csv",
        use_container_width=True
    )

if do_save:
    if df.empty:
        st.warning("No hay datos para guardar.")
    else:
        try:
            cnx = get_sf_conn()
            ensure_objects(cnx)
            delete_partition(cnx, date_str, timezone.strip())
            df2 = df.copy()
            df2["source_date"] = date_str
            df2["timezone_used"] = timezone.strip()
            insert_df(cnx, df2)
            st.success(f"Guardado en {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}")
        except Exception as e:
            st.error(f"Error guardando en Snowflake: {e}")
        finally:
            try:
                cnx.close()
            except:
                pass

st.markdown("---")
st.subheader("🔎 Consulta rápida en Snowflake")

lim = st.number_input("Límite", 1, 10000, 200, 50)

q = f"""
select event_key,event_date,event_time,first_player,second_player,
       tournament_name,event_type_type,event_status,
       source_date,timezone_used,_ingested_at
from {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
where source_date = to_date('{date_str}')
  and timezone_used = '{timezone}'
order by tournament_name, event_time, event_key
limit {int(lim)}
"""

st.code(q, language="sql")

try:
    cnx2 = get_sf_conn()
    df_db = pd.read_sql(q, cnx2)
    cnx2.close()
    st.dataframe(df_db, use_container_width=True, height=360)
except Exception as e:
    st.info(f"No se pudo consultar Snowflake: {e}")

import streamlit as st
import pandas as pd
import requests
import datetime as dt
import json
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ------------------------------
# Config Streamlit
# ------------------------------
st.set_page_config(page_title="🎾 Tennis Loader", layout="wide")
st.title("🎾 Tennis Match Keys → Snowflake")

# ------------------------------
# Credenciales desde secrets/env
# ------------------------------
def get_sf_conn():
    return snowflake.connector.connect(
        account=os.getenv("SF_ACCOUNT", st.secrets.get("SF_ACCOUNT")),
        user=os.getenv("SF_USER", st.secrets.get("SF_USER")),
        password=os.getenv("SF_PASSWORD", st.secrets.get("SF_PASSWORD")),
        role=os.getenv("SF_ROLE", st.secrets.get("SF_ROLE")),
        warehouse=os.getenv("SF_WAREHOUSE", st.secrets.get("SF_WAREHOUSE")),
        database=os.getenv("SF_DATABASE", st.secrets.get("SF_DATABASE")),
        schema=os.getenv("SF_SCHEMA", st.secrets.get("SF_SCHEMA")),
    )

DB = os.getenv("SF_DATABASE", st.secrets.get("SF_DATABASE"))
SCHEMA = os.getenv("SF_SCHEMA", st.secrets.get("SF_SCHEMA"))
TABLE = os.getenv("SF_TABLE", st.secrets.get("SF_TABLE"))

# ------------------------------
# DB helpers
# ------------------------------
def ensure_table(cnx):
    cnx.cursor().execute(f"""
        create table if not exists {DB}.{SCHEMA}.{TABLE} (
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
            _ingested_at timestamp default current_timestamp()
        )
    """)

def delete_existing(cnx, date, tz):
    cnx.cursor().execute(f"""
        delete from {DB}.{SCHEMA}.{TABLE}
        where source_date = to_date('{date}')
          and timezone_used = '{tz}'
    """)

def write_df(cnx, df):
    write_pandas(cnx, df, TABLE, database=DB, schema=SCHEMA)

# ------------------------------
# API Tennis functions
# ------------------------------
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

# ------------------------------
# UI
# ------------------------------
with st.sidebar:
    api_key = st.text_input("API Key", type="password")
    fecha = st.date_input("Fecha", dt.date.today())
    timezone = st.text_input("Timezone", "America/Monterrey")

date_str = fecha.strftime("%Y-%m-%d")

btn_fetch = st.button("📡 Traer desde API")
btn_save = st.button("💾 Guardar en Snowflake")

st.subheader("📂 O subir JSON manual")
upload = st.file_uploader("JSON API", type=["json"])

if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()

# ------------------------------
# Fetch API
# ------------------------------
if btn_fetch:
    if not api_key:
        st.warning("Ingresa la API key")
    else:
        try:
            data = fetch_api(api_key, date_str, timezone)
            if data.get("success") != 1:
                st.error(data)
            else:
                st.session_state.df = normalize(data["result"])
                st.success(f"{len(st.session_state.df)} partidos obtenidos")
        except Exception as e:
            st.error(e)

# ------------------------------
# JSON upload
# ------------------------------
if upload:
    raw = json.load(upload)
    if raw.get("success") != 1:
        st.error("JSON incorrecto")
    else:
        st.session_state.df = normalize(raw["result"])
        st.success(f"{len(st.session_state.df)} partidos cargados")

# ------------------------------
# Preview
# ------------------------------
st.write("### Vista previa")
df = st.session_state.df
if df.empty:
    st.info("No hay datos aún")
else:
    st.dataframe(df, height=350)
    st.download_button("⬇️ CSV", df.to_csv(index=False), f"matches_{date_str}.csv")

# ------------------------------
# Save to Snowflake
# ------------------------------
if btn_save:
    if df.empty:
        st.warning("No hay datos para guardar")
    else:
        try:
            cnx = get_sf_conn()
            ensure_table(cnx)
            delete_existing(cnx, date_str, timezone)
            df2 = df.copy()
            df2["source_date"] = date_str
            df2["timezone_used"] = timezone
            write_df(cnx, df2)
            st.success("✅ Guardado en Snowflake")
        except Exception as e:
            st.error(e)
        finally:
            try: cnx.close()
            except: pass

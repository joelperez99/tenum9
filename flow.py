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
    # Usa secrets de Streamlit Cloud primero, luego variables de entorno
    try:
        return st.secrets[name]
    except Exception:
        return os.getenv(name, default)

# Lee credenciales Snowflake (desde Secrets o ENV)
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
    sf_exec(cnx, f"create database if not exists { SF_DATABASE }")
    sf_exec(cnx, f"create schema if not exists { SF_DATABASE }.{ SF_SCHEMA }")
    sf_exec(cnx, f"use database { SF_DATABASE }")
    sf_exec(cnx, f"use schema { SF_SCHEMA }")
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

def delete_partition_range(cnx, start_str, stop_str, timezone):
    """
    Borra todas las filas del rango [start_str, stop_str]
    para el timezone indicado.
    """
    sf_exec(cnx, f"""
        delete from {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
        where source_date between to_date('{start_str}') and to_date('{stop_str}')
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

def fetch_api_day(api_key: str, date_str: str, timezone: str) -> dict:
    """
    Llama a la API SOLO para un día concreto (date_start = date_stop = date_str).
    Esto evita pedir rangos grandes que saturen la API.
    """
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
    fecha_desde = st.date_input("Fecha desde", value=dt.date.today(), format="YYYY-MM-DD")
    fecha_hasta = st.date_input("Fecha hasta", value=dt.date.today(), format="YYYY-MM-DD")
    timezone = st.text_input("Timezone", value="America/Monterrey")

# Strings de fechas
start_str = fecha_desde.strftime("%Y-%m-%d")
stop_str  = fecha_hasta.strftime("%Y-%m-%d")

# Validación rápida del rango
if fecha_hasta < fecha_desde:
    st.sidebar.error("⚠️ La 'Fecha hasta' no puede ser menor que la 'Fecha desde'.")

col1, col2, col3 = st.columns([1.2, 1.2, 2])
with col1:
    do_fetch = st.button("📡 Traer desde API")
with col2:
    do_save = st.button("💾 Guardar en Snowflake")

st.markdown("#### 📄 Plan B: subir JSON del API (si prefieres pegar el payload)")
upl = st.file_uploader("Archivo .json", type=["json"])

# buffer de datos
if "df_buf" not in st.session_state:
    st.session_state.df_buf = pd.DataFrame()

# -----------------------------
# Acciones
# -----------------------------
if do_fetch:
    if not api_key.strip():
        st.warning("Ingresa tu API Key.")
    elif fecha_hasta < fecha_desde:
        st.error("Rango de fechas inválido. Corrige 'Fecha desde' y 'Fecha hasta'.")
    else:
        try:
            total_dias = (fecha_hasta - fecha_desde).days + 1
            barra = st.progress(0.0, text="Consultando API día por día...")
            dfs = []
            errores = []

            for i in range(total_dias):
                dia = fecha_desde + dt.timedelta(days=i)
                dia_str = dia.strftime("%Y-%m-%d")
                try:
                    payload = fetch_api_day(api_key.strip(), dia_str, timezone.strip())
                    if payload.get("success") != 1:
                        errores.append(f"{dia_str}: success != 1 ({payload.get('message', payload)})")
                    else:
                        df_dia = normalize_result(payload.get("result"))
                        if not df_dia.empty:
                            dfs.append(df_dia)
                except Exception as e:
                    errores.append(f"{dia_str}: {e}")

                barra.progress((i + 1) / total_dias, text=f"Consultando {dia_str} ({i+1}/{total_dias})")

            if not dfs:
                st.error("No se obtuvieron partidos en el rango seleccionado (o todos los días dieron error).")
                if errores:
                    st.warning("Detalle de errores:\n" + "\n".join(errores))
            else:
                df_all = pd.concat(dfs, ignore_index=True)

                # Opcional: eliminar duplicados por event_key
                if "event_key" in df_all.columns:
                    df_all = df_all.drop_duplicates(subset=["event_key"])

                st.session_state.df_buf = df_all

                msg = f"OK. {len(st.session_state.df_buf)} partidos entre {start_str} y {stop_str}, consultando día por día."
                if errores:
                    msg += f" Se encontraron algunos errores en ciertos días ({len(errores)}); revisa los detalles abajo."
                st.success(msg)

                if errores:
                    with st.expander("Ver detalles de errores por día"):
                        for e in errores:
                            st.text(e)

        except Exception as e:
            st.error(f"Error general llamando API: {e}")

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
    st.info("Sin datos aún. Usa 'Traer desde API' o sube un JSON.")
else:
    st.dataframe(df, use_container_width=True, height=420)

    # ================================
    # 🔵 Botón: Copiar Match Keys
    # ================================
    matchkeys_str = "\n".join(df["event_key"].astype(str).tolist())
    matchkeys_json = json.dumps(matchkeys_str)

    st.markdown(
        f"""
        <button
            style="
                margin-top: 0.5rem;
                padding: 0.4rem 0.8rem;
                border-radius: 0.3rem;
                border: 1px solid #ccc;
                cursor: pointer;
                background-color: #f5f5f5;
            "
            onclick='navigator.clipboard.writeText({matchkeys_json}); alert("Match Keys copiados al portapapeles");'>
            📋 Copiar Match Keys
        </button>
        """,
        unsafe_allow_html=True,
    )

    st.download_button(
        "⬇️ Descargar CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"match_keys_{start_str}_a_{stop_str}.csv",
        mime="text/csv",
        use_container_width=True
    )

# -----------------------------
# Guardar en Snowflake
# -----------------------------
if do_save:
    if df.empty:
        st.warning("No hay datos para guardar.")
    elif fecha_hasta < fecha_desde:
        st.error("Rango de fechas inválido. Corrige 'Fecha desde' y 'Fecha hasta'.")
    else:
        try:
            cnx = get_sf_conn()
            ensure_objects(cnx)

            # Borra partición del rango
            delete_partition_range(cnx, start_str, stop_str, timezone.strip())

            # Prepara DF para Snowflake
            df2 = df.copy()
            # Usa event_date como source_date; si falla, cae en start_str
            try:
                df2["source_date"] = pd.to_datetime(df2["event_date"], errors="coerce").dt.date
                default_date = pd.to_datetime(start_str).date()
                df2["source_date"] = df2["source_date"].fillna(default_date)
            except Exception:
                df2["source_date"] = pd.to_datetime(start_str).date()

            df2["timezone_used"] = timezone.strip()

            insert_df(cnx, df2)
            st.success(f"Guardado en {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (rango {start_str} a {stop_str}).")
        except Exception as e:
            st.error(f"Error guardando en Snowflake: {e}")
        finally:
            try:
                cnx.close()
            except Exception:
                pass

st.markdown("---")
st.subheader("🔎 Consulta rápida en Snowflake")

lim = st.number_input("Límite", 1, 10000, 200, 50)

q = f"""
select event_key,event_date,event_time,first_player,second_player,
       tournament_name,event_type_type,event_status,
       source_date,timezone_used,_ingested_at
from {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
where source_date between to_date('{start_str}') and to_date('{stop_str}')
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
    st.info(f"No se pudo consultar (¿tabla aún vacía?). Detalle: {e}")

"""Dashboard Streamlit: alertas de emergencia en vivo sobre Bucaramanga."""
import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from html import escape as html_escape

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st
from confluent_kafka import Consumer, TopicPartition
from psycopg2 import pool as pg_pool

# ------------------------------ CONFIG -------------------------------------
BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALERT_TOPIC   = os.getenv("ALERT_TOPIC", "alertas-emergencia")
SENSOR_TOPIC  = os.getenv("SENSOR_TOPIC", "sensor-vital-data")
HISTORY       = 50

DB_HOST       = os.getenv("DB_HOST", "localhost")
DB_PORT       = os.getenv("DB_PORT", "5432")
DB_NAME       = os.getenv("DB_NAME", "health")
DB_USER       = os.getenv("DB_USER", "health")
DB_PASSWORD   = os.getenv("DB_PASSWORD", "health")

BOGOTA        = timezone(timedelta(hours=-5))

SEVERITY_ORDER = ["CRÍTICA", "SEVERA", "MODERADA"]
SEVERITY_COLOR = {"CRÍTICA": "#dc2626", "SEVERA": "#ea580c", "MODERADA": "#d97706"}

st.set_page_config(
    page_title="Alertas de Salud · Bucaramanga",
    page_icon="🚑",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------ ESTILOS ------------------------------------
CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'Inter', system-ui, -apple-system, sans-serif;
}
#MainMenu, footer { visibility: hidden; }
.block-container { padding-top: 1.25rem; padding-bottom: 2rem; max-width: 1450px; }

/* --------------------- HERO --------------------- */
.hero {
    background: linear-gradient(120deg, #0b1220 0%, #111e35 55%, #1e293b 100%);
    color: #f1f5f9;
    padding: 1.6rem 2rem;
    border-radius: 18px;
    margin-bottom: 1rem;
    display: flex; align-items: center; justify-content: space-between;
    box-shadow: 0 12px 30px rgba(15, 23, 42, 0.18);
}
.hero-left h1 { margin: 0; font-size: 1.6rem; font-weight: 800; letter-spacing: -0.02em; }
.hero-left p  { margin: 0.3rem 0 0; color: #94a3b8; font-size: 0.9rem; }
.hero-right   { display: flex; flex-direction: column; align-items: flex-end; gap: 0.45rem; }
.live-badge {
    display: inline-flex; align-items: center; gap: 0.55rem;
    padding: 0.4rem 0.9rem; border-radius: 999px;
    font-weight: 600; font-size: 0.82rem; letter-spacing: 0.02em;
    background: rgba(34, 197, 94, 0.15); color: #4ade80;
    border: 1px solid rgba(34, 197, 94, 0.35);
}
.live-badge.offline { background: rgba(239, 68, 68, 0.15); color: #f87171;
                      border-color: rgba(239, 68, 68, 0.35); }
.live-dot { width: 8px; height: 8px; border-radius: 50%; background: #22c55e;
            box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.6); animation: pulse 1.6s infinite; }
.live-badge.offline .live-dot { background: #ef4444; animation: none; box-shadow: none; }
@keyframes pulse {
    0%   { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.6); }
    70%  { box-shadow: 0 0 0 9px rgba(34, 197, 94, 0); }
    100% { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0); }
}
.hero-time { color: #64748b; font-size: 0.78rem; font-variant-numeric: tabular-nums; }

/* --------------------- KPI CARDS --------------------- */
.kpi {
    background: #ffffff; border: 1px solid #e2e8f0; border-radius: 14px;
    padding: 1.1rem 1.3rem; height: 100%;
    box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04), 0 8px 20px rgba(15, 23, 42, 0.03);
    transition: transform 0.15s ease, box-shadow 0.15s ease;
}
.kpi:hover { transform: translateY(-1px);
             box-shadow: 0 4px 10px rgba(15, 23, 42, 0.07), 0 12px 28px rgba(15, 23, 42, 0.05); }
.kpi-label { font-size: 0.72rem; font-weight: 600; text-transform: uppercase;
             letter-spacing: 0.06em; color: #64748b; margin-bottom: 0.45rem; }
.kpi-value { font-size: 1.9rem; font-weight: 800; color: #0f172a;
             line-height: 1.05; font-variant-numeric: tabular-nums; }
.kpi-value.danger { color: #dc2626; }
.kpi-value.ok     { color: #16a34a; }
.kpi-value.warn   { color: #d97706; }
.kpi-hint { font-size: 0.78rem; color: #94a3b8; margin-top: 0.4rem; min-height: 1em; }

/* --------------------- SECTION HEADINGS --------------------- */
.section-head {
    display: flex; align-items: center; gap: 0.5rem;
    font-size: 1rem; font-weight: 700; color: #0f172a;
    margin: 1.5rem 0 0.6rem; padding-bottom: 0.5rem; border-bottom: 2px solid #e2e8f0;
}
.section-head .dot { width: 8px; height: 8px; border-radius: 50%; background: #dc2626; }

/* --------------------- SIDEBAR --------------------- */
[data-testid="stSidebar"] { background: #f8fafc; border-right: 1px solid #e2e8f0; }
[data-testid="stSidebar"] h3 { font-size: 0.95rem; text-transform: uppercase;
                               letter-spacing: 0.06em; color: #475569; }
.note { color: #94a3b8; font-size: 0.82rem; line-height: 1.5; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# ============================ CONSUMIDOR KAFKA ==============================
class AlertStore:
    """Consume Kafka en un hilo de fondo; la UI solo lee instantáneas."""

    def __init__(self):
        self._lock = threading.Lock()
        self._alerts = deque(maxlen=HISTORY)
        self._total_alerts = 0
        self._total_events = 0
        self._throughput = 0.0
        self._connected = False
        self._start_time = time.time()
        threading.Thread(target=self._run, daemon=True, name="alert-consumer").start()

    def snapshot(self):
        with self._lock:
            return (
                list(self._alerts),
                self._total_alerts,
                self._total_events,
                self._throughput,
                self._connected,
                self._start_time,
            )

    def _count_events(self, consumer):
        meta = consumer.list_topics(SENSOR_TOPIC, timeout=5).topics.get(SENSOR_TOPIC)
        if meta is None or not meta.partitions:
            return 0
        total = 0
        for partition in meta.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(SENSOR_TOPIC, partition), timeout=3
            )
            total += high - low
        return total

    def _run(self):
        while True:
            consumer = None
            try:
                consumer = Consumer({
                    "bootstrap.servers": BOOTSTRAP,
                    "group.id": "streamlit-dashboard",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                })
                consumer.subscribe([ALERT_TOPIC])
                last_metrics = 0.0
                prev_events = None
                prev_ts = None
                while True:
                    msg = consumer.poll(0.5)
                    with self._lock:
                        self._connected = True
                    if msg is not None and not msg.error():
                        try:
                            alert = json.loads(msg.value())
                        except ValueError:
                            continue
                        with self._lock:
                            self._alerts.append(alert)
                            self._total_alerts += 1
                    if time.time() - last_metrics > 2:
                        events = self._count_events(consumer)
                        now = time.time()
                        with self._lock:
                            self._total_events = events
                            if prev_events is not None and prev_ts is not None and now > prev_ts:
                                self._throughput = max(0.0, (events - prev_events) / (now - prev_ts))
                            prev_events, prev_ts = events, now
                        last_metrics = now
            except Exception:
                with self._lock:
                    self._connected = False
                    self._throughput = 0.0
                time.sleep(3)
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass


@st.cache_resource(show_spinner=False)
def get_store():
    return AlertStore()


@st.cache_resource(show_spinner=False)
def get_pg_pool():
    return pg_pool.ThreadedConnectionPool(
        minconn=1, maxconn=8,
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


# ============================ HELPERS ======================================
def fmt_time(epoch_seconds):
    try:
        return datetime.fromtimestamp(int(epoch_seconds), tz=BOGOTA).strftime("%H:%M:%S")
    except (TypeError, ValueError):
        return "—"


def fmt_uptime(seconds):
    seconds = max(0, int(seconds))
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def severity_of(hr, spo2):
    if hr >= 160 or spo2 <= 82:
        return "CRÍTICA"
    if hr >= 145 or spo2 <= 85:
        return "SEVERA"
    return "MODERADA"


def kpi(label, value, hint="", variant=""):
    return (
        f'<div class="kpi">'
        f'  <div class="kpi-label">{label}</div>'
        f'  <div class="kpi-value {variant}">{value}</div>'
        f'  <div class="kpi-hint">{hint}</div>'
        f'</div>'
    )


@st.cache_data(ttl=2, show_spinner=False)
def query_history(limit=500, patient_q=""):
    """Consulta el histórico persistido en Postgres."""
    sql = """
        SELECT alert_time, patient_id, max_heart_rate, min_oxygen_saturation,
               latitude, longitude, window_start, window_end
        FROM alertas_emergencia
        WHERE 1=1
    """
    params = []
    if patient_q:
        sql += " AND patient_id ILIKE %s"
        params.append(f"%{patient_q}%")
    sql += " ORDER BY alert_time DESC LIMIT %s"
    params.append(limit)

    p = get_pg_pool()
    conn = p.getconn()
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        p.putconn(conn)
    return df


@st.cache_data(ttl=2, show_spinner=False)
def query_history_stats():
    p = get_pg_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)                                AS total,
                       COUNT(DISTINCT patient_id)              AS pacientes,
                       COALESCE(AVG(max_heart_rate), 0)        AS avg_hr,
                       COALESCE(AVG(min_oxygen_saturation), 0) AS avg_spo2,
                       MIN(inserted_at)                        AS primera,
                       MAX(inserted_at)                        AS ultima
                FROM alertas_emergencia
            """)
            row = cur.fetchone()
    finally:
        p.putconn(conn)
    return dict(zip(["total", "pacientes", "avg_hr", "avg_spo2", "primera", "ultima"], row))


# ============================ UI ===========================================
def render_header(connected, last_alert_ts):
    status_class = "" if connected else "offline"
    status_text = "En vivo" if connected else "Sin conexión"
    last = fmt_time(last_alert_ts) if last_alert_ts else "—"
    st.markdown(
        f'<div class="hero">'
        f'  <div class="hero-left">'
        f'    <h1>🚑 Sistema de Alertas de Salud en Tiempo Real</h1>'
        f'    <p>Detección de emergencias cardiopulmonares · Bucaramanga, Colombia</p>'
        f'  </div>'
        f'  <div class="hero-right">'
        f'    <div class="live-badge {status_class}"><span class="live-dot"></span>{status_text}</div>'
        f'    <div class="hero-time">Última alerta: {last} (COT)</div>'
        f'  </div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_sidebar():
    with st.sidebar:
        st.markdown("### Filtros (en vivo)")
        severity_filter = st.multiselect(
            "Severidad",
            options=SEVERITY_ORDER,
            default=SEVERITY_ORDER,
            key="severity_filter",
        )
        patient_query = st.text_input(
            "Buscar paciente",
            placeholder="patient-0042",
            key="patient_query",
        ).strip()

        st.markdown("---")
        st.markdown("### Regla de detección")
        st.markdown(
            '<div class="note">'
            "Frecuencia cardíaca máx. <b>&gt; 130 lpm</b><br>"
            "Saturación de O₂ mín. <b>&lt; 90 %</b><br>"
            "Ventanas <i>tumbling</i> de 15 s · Flink SQL"
            "</div>",
            unsafe_allow_html=True,
        )

        st.markdown("---")
        st.markdown("### Pipeline")
        st.markdown(
            '<div class="note">'
            "Wearables → <b>Kafka</b> (sensor-vital-data)<br>"
            "→ <b>Flink SQL</b> (ventaneo + agregación)<br>"
            "→ <b>Kafka</b> (alertas-emergencia)<br>"
            "→ <b>PostgreSQL</b> (persistencia)<br>"
            "→ <b>Streamlit</b> (este tablero)"
            "</div>",
            unsafe_allow_html=True,
        )

        st.markdown("---")
        st.caption(f"Bootstrap: `{html_escape(BOOTSTRAP)}`")
    return severity_filter, patient_query


# ---------------------------- TAB: EN VIVO ---------------------------------
def render_live(store, severity_filter, patient_query):
    alerts, total_alerts, total_events, throughput, connected, start_time = store.snapshot()
    df = pd.DataFrame(alerts) if alerts else pd.DataFrame()

    if not df.empty:
        df["severity"] = [severity_of(h, s) for h, s in
                          zip(df["max_heart_rate"], df["min_oxygen_saturation"])]
        df["alert_dt"] = pd.to_datetime(df["alert_time"], unit="s", utc=True).dt.tz_convert(BOGOTA)

    last_alert = int(df["alert_time"].iloc[-1]) if not df.empty else None
    render_header(connected, last_alert)

    uptime = time.time() - start_time

    # ---------- KPI fila 1 ----------
    row1 = st.columns(4)
    with row1[0]:
        st.markdown(kpi("Eventos procesados",
                        f"{total_events:,}".replace(",", "."),
                        "Mensajes acumulados en el tópico de sensores"),
                    unsafe_allow_html=True)
    with row1[1]:
        st.markdown(kpi("Alertas generadas",
                        f"{total_alerts:,}".replace(",", "."),
                        "Detecciones emitidas por Flink",
                        "danger" if total_alerts else ""),
                    unsafe_allow_html=True)
    with row1[2]:
        st.markdown(kpi("Tasa de eventos",
                        f"{throughput:,.0f}/s".replace(",", "."),
                        "Throughput del tópico de sensores"),
                    unsafe_allow_html=True)
    with row1[3]:
        st.markdown(kpi("Última alerta",
                        fmt_time(last_alert) if last_alert else "—",
                        "Hora local (COT)"),
                    unsafe_allow_html=True)

    # ---------- KPI fila 2 ----------
    n_patients = df["patient_id"].nunique() if not df.empty else 0
    n_crit     = int((df["severity"] == "CRÍTICA").sum()) if not df.empty else 0
    pct_crit   = (n_crit / len(df) * 100) if not df.empty else 0.0

    row2 = st.columns(4)
    with row2[0]:
        st.markdown(kpi("Pacientes en alerta", f"{n_patients}",
                        "Pacientes únicos en las últimas alertas"), unsafe_allow_html=True)
    with row2[1]:
        st.markdown(kpi("Alertas críticas", f"{n_crit}",
                        f"{pct_crit:.0f}% del registro reciente",
                        "danger" if n_crit else ""), unsafe_allow_html=True)
    with row2[2]:
        st.markdown(kpi("Estado del consumidor",
                        "En línea" if connected else "Sin conexión",
                        "Kafka · grupo streamlit-dashboard",
                        "ok" if connected else "danger"), unsafe_allow_html=True)
    with row2[3]:
        st.markdown(kpi("Tiempo activo", fmt_uptime(uptime),
                        "Desde el arranque del tablero"), unsafe_allow_html=True)

    # ---------- Filtros ----------
    view = df.copy()
    if not view.empty:
        if severity_filter:
            view = view[view["severity"].isin(severity_filter)]
        if patient_query:
            view = view[view["patient_id"].str.contains(patient_query, case=False, na=False)]

    if df.empty:
        st.markdown('<div class="section-head"><span class="dot"></span>Esperando datos…</div>',
                    unsafe_allow_html=True)
        st.info("El tablero se poblará en cuanto Flink emita la primera alerta. "
                "Esto suele ocurrir pocos segundos después de iniciar el job SQL.")
        return

    # ---------- Mapa + distribución ----------
    st.markdown('<div class="section-head"><span class="dot"></span>Geolocalización de alertas · Bucaramanga</div>',
                unsafe_allow_html=True)
    map_col, sev_col = st.columns([2, 1])
    with map_col:
        if not view.empty:
            st.map(view, latitude="latitude", longitude="longitude",
                   size=60, color="#dc2626", use_container_width=True, height=380)
        else:
            st.info("Ninguna alerta coincide con los filtros.")
    with sev_col:
        st.markdown("**Distribución por severidad**")
        if not view.empty:
            counts = view["severity"].value_counts().reindex(SEVERITY_ORDER).fillna(0).astype(int)
            sev_df = pd.DataFrame({"Severidad": counts.index, "Alertas": counts.values})
            bar = alt.Chart(sev_df).mark_bar(
                cornerRadiusTopRight=6, cornerRadiusBottomRight=6
            ).encode(
                y=alt.Y("Severidad:N", sort=SEVERITY_ORDER, title=None,
                        axis=alt.Axis(labelAngle=0)),
                x=alt.X("Alertas:Q", title=None, axis=alt.Axis(grid=False)),
                color=alt.Color(
                    "Severidad:N",
                    scale=alt.Scale(domain=SEVERITY_ORDER,
                                    range=[SEVERITY_COLOR[s] for s in SEVERITY_ORDER]),
                    legend=None,
                ),
                tooltip=["Severidad", "Alertas"],
            ).properties(height=340)
            st.altair_chart(bar, use_container_width=True)
        else:
            st.caption("Sin datos para graficar.")

    # ---------- Actividad reciente ----------
    st.markdown('<div class="section-head"><span class="dot"></span>Actividad reciente</div>',
                unsafe_allow_html=True)
    act_left, act_right = st.columns(2)

    with act_left:
        st.markdown("**Alertas por minuto (últimos 15 min)**")
        if not view.empty:
            tl = view[["alert_dt"]].copy()
            tl["Alertas"] = 1
            tl = tl.set_index("alert_dt").sort_index().resample("1min").sum().reset_index()
            cutoff = pd.Timestamp.now(tz=BOGOTA) - pd.Timedelta(minutes=15)
            tl = tl[tl["alert_dt"] >= cutoff]
            if not tl.empty:
                line = alt.Chart(tl).mark_area(
                    line={"color": "#dc2626", "strokeWidth": 2},
                    color="rgba(220,38,38,0.18)",
                ).encode(
                    x=alt.X("alert_dt:T", title=None, axis=alt.Axis(format="%H:%M")),
                    y=alt.Y("Alertas:Q", title=None),
                    tooltip=[alt.Tooltip("alert_dt:T", title="Hora", format="%H:%M"), "Alertas"],
                ).properties(height=260)
                st.altair_chart(line, use_container_width=True)
            else:
                st.caption("Sin alertas en los últimos 15 minutos.")
        else:
            st.caption("Sin datos.")

    with act_right:
        st.markdown("**FC vs. SpO₂ por alerta**")
        if not view.empty:
            base = alt.Chart(view).mark_circle(
                size=110, opacity=0.85, stroke="white", strokeWidth=1.2
            ).encode(
                x=alt.X("max_heart_rate:Q", title="FC máxima (lpm)",
                        scale=alt.Scale(zero=False)),
                y=alt.Y("min_oxygen_saturation:Q", title="SpO₂ mínima (%)",
                        scale=alt.Scale(zero=False)),
                color=alt.Color(
                    "severity:N",
                    scale=alt.Scale(domain=SEVERITY_ORDER,
                                    range=[SEVERITY_COLOR[s] for s in SEVERITY_ORDER]),
                    legend=alt.Legend(title=None, orient="top"),
                ),
                tooltip=[
                    "patient_id", "severity",
                    alt.Tooltip("max_heart_rate:Q", title="FC máx"),
                    alt.Tooltip("min_oxygen_saturation:Q", title="SpO₂ mín"),
                    alt.Tooltip("alert_dt:T", title="Hora", format="%H:%M:%S"),
                ],
            )
            hr_rule  = alt.Chart(pd.DataFrame({"x": [130]})).mark_rule(
                strokeDash=[5, 5], color="#94a3b8").encode(x="x:Q")
            spo2_rule = alt.Chart(pd.DataFrame({"y": [90]})).mark_rule(
                strokeDash=[5, 5], color="#94a3b8").encode(y="y:Q")
            scatter = (base + hr_rule + spo2_rule).properties(height=260).interactive()
            st.altair_chart(scatter, use_container_width=True)
            st.caption("Umbrales: FC > 130 lpm y SpO₂ < 90 % (líneas punteadas).")
        else:
            st.caption("Sin datos.")

    # ---------- Tabla ----------
    st.markdown('<div class="section-head"><span class="dot"></span>Registro de alertas</div>',
                unsafe_allow_html=True)
    if view.empty:
        st.info("No hay alertas que mostrar con los filtros aplicados.")
        return

    st.caption(f"Mostrando **{len(view)}** de **{len(df)}** alertas recientes · ordenadas de más nueva a más antigua")

    rev = view.iloc[::-1].reset_index(drop=True)
    table = pd.DataFrame({
        "Hora":          rev["alert_dt"].dt.strftime("%H:%M:%S"),
        "Paciente":      rev["patient_id"],
        "Severidad":     rev["severity"],
        "FC máx (lpm)":  rev["max_heart_rate"],
        "SpO₂ mín (%)":  rev["min_oxygen_saturation"],
        "Latitud":       rev["latitude"].round(5),
        "Longitud":      rev["longitude"].round(5),
    })

    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        height=380,
        column_config={
            "Hora":         st.column_config.TextColumn("Hora", width="small"),
            "Paciente":     st.column_config.TextColumn("Paciente", width="medium"),
            "Severidad":    st.column_config.TextColumn("Severidad", width="small"),
            "FC máx (lpm)": st.column_config.ProgressColumn(
                "FC máx (lpm)", min_value=130, max_value=180, format="%d"),
            "SpO₂ mín (%)": st.column_config.ProgressColumn(
                "SpO₂ mín (%)", min_value=75, max_value=95, format="%d"),
            "Latitud":      st.column_config.NumberColumn("Lat", format="%.5f"),
            "Longitud":     st.column_config.NumberColumn("Lon", format="%.5f"),
        },
    )

    csv = view[["alert_time", "patient_id", "severity",
                "max_heart_rate", "min_oxygen_saturation",
                "latitude", "longitude"]].to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Descargar alertas (CSV)",
        data=csv,
        file_name=f"alertas_bucaramanga_{datetime.now(BOGOTA):%Y%m%d_%H%M%S}.csv",
        mime="text/csv",
        key="download_csv",
    )


# ---------------------------- TAB: HISTORIAL -------------------------------
def render_history():
    st.markdown('<div class="section-head"><span class="dot"></span>Historial persistente · PostgreSQL</div>',
                unsafe_allow_html=True)
    st.caption("Estas alertas **sobreviven a reinicios** del cluster porque Flink las "
               "escribió a PostgreSQL vía JDBC en modo UPSERT (idempotente).")

    try:
        stats = query_history_stats()
    except Exception as e:
        st.error(f"No se pudo consultar Postgres: {e}")
        st.info("Verifica que el servicio `postgres` esté healthy: "
                "`docker compose ps postgres` y `docker compose logs postgres`.")
        return

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi("Alertas totales", f"{stats['total']:,}".replace(",", "."),
                        "Persistidas en PostgreSQL"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("Pacientes afectados", f"{stats['pacientes']:,}".replace(",", "."),
                        "Con al menos una alerta"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("FC media (histórico)", f"{stats['avg_hr']:.0f} lpm",
                        "De todas las alertas"), unsafe_allow_html=True)
    with c4:
        ultima = stats["ultima"].strftime("%H:%M:%S") if stats["ultima"] else "—"
        st.markdown(kpi("Última alerta persistida", ultima, "COT"), unsafe_allow_html=True)

    f1, f2 = st.columns([1, 2])
    with f1:
        limit = st.selectbox("Últimas N alertas", [100, 500, 1000, 5000], index=1)
    with f2:
        paciente = st.text_input("Filtrar por paciente (histórico)",
                                 placeholder="patient-0042",
                                 key="history_patient")

    try:
        df = query_history(limit=limit, patient_q=paciente.strip())
    except Exception as e:
        st.error(f"Error al consultar Postgres: {e}")
        return

    if df.empty:
        st.info("Aún no hay alertas persistidas. Espera ~15 s tras el arranque del job Flink "
                "(o haz clic en 'reset' en la pestaña en vivo).")
        return

    df["alert_dt"] = pd.to_datetime(df["alert_time"], unit="s", utc=True).dt.tz_convert(BOGOTA)
    df["severity"] = [severity_of(h, s) for h, s in
                      zip(df["max_heart_rate"], df["min_oxygen_saturation"])]

    g1, g2 = st.columns([2, 1])
    with g1:
        st.markdown("**Alertas por minuto (histórico)**")
        tl = df.set_index("alert_dt").sort_index().resample("1min").size().reset_index(name="Alertas")
        tl = tl[tl["Alertas"] > 0]
        if not tl.empty:
            line = alt.Chart(tl).mark_area(
                line={"color": "#dc2626", "strokeWidth": 2},
                color="rgba(220,38,38,0.18)",
            ).encode(
                x=alt.X("alert_dt:T", title=None, axis=alt.Axis(format="%H:%M")),
                y=alt.Y("Alertas:Q", title=None),
                tooltip=[alt.Tooltip("alert_dt:T", title="Minuto", format="%H:%M"), "Alertas"],
            ).properties(height=280)
            st.altair_chart(line, use_container_width=True)
    with g2:
        st.markdown("**Proporción por severidad**")
        counts = df["severity"].value_counts().reindex(SEVERITY_ORDER).fillna(0).astype(int)
        sev_df = pd.DataFrame({"Severidad": counts.index, "Alertas": counts.values})
        chart = alt.Chart(sev_df).mark_arc(innerRadius=55, outerRadius=95).encode(
            theta="Alertas:Q",
            color=alt.Color("Severidad:N",
                            scale=alt.Scale(domain=SEVERITY_ORDER,
                                            range=[SEVERITY_COLOR[s] for s in SEVERITY_ORDER]),
                            legend=alt.Legend(title=None, orient="bottom")),
            tooltip=["Severidad", "Alertas"],
        ).properties(height=280)
        st.altair_chart(chart, use_container_width=True)

    st.markdown("**Registro completo**")
    table = pd.DataFrame({
        "Fecha/Hora":   df["alert_dt"].dt.strftime("%Y-%m-%d %H:%M:%S"),
        "Paciente":     df["patient_id"],
        "Severidad":    df["severity"],
        "FC máx (lpm)": df["max_heart_rate"],
        "SpO₂ mín (%)": df["min_oxygen_saturation"],
        "Lat":          df["latitude"].round(5),
        "Lon":          df["longitude"].round(5),
    })
    st.dataframe(
        table, use_container_width=True, hide_index=True, height=420,
        column_config={
            "FC máx (lpm)": st.column_config.ProgressColumn(
                "FC máx (lpm)", min_value=130, max_value=180, format="%d"),
            "SpO₂ mín (%)": st.column_config.ProgressColumn(
                "SpO₂ mín (%)", min_value=75, max_value=95, format="%d"),
        },
    )
    st.download_button(
        "⬇️ Descargar histórico (CSV)",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"historico_alertas_{datetime.now(BOGOTA):%Y%m%d_%H%M%S}.csv",
        mime="text/csv",
        key="download_history_csv",
    )


# ============================ ENTRYPOINT ====================================
store = get_store()
severity_filter, patient_query = render_sidebar()

tab_live, tab_hist = st.tabs(["🔴 En vivo (Kafka)", "📚 Historial (PostgreSQL)"])
with tab_live:
    render_live(store, severity_filter, patient_query)
with tab_hist:
    render_history()

time.sleep(1)
st.rerun()

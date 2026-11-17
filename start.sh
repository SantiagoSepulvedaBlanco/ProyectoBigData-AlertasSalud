#!/usr/bin/env bash
# =============================================================================
# start.sh - Arranque ordenado del Sistema de Alertas de Salud en Tiempo Real
#
# Orden: infraestructura (Kafka + Postgres + Flink) -> tópicos -> generador
#        (con datos fluyendo) -> job Flink -> dashboard -> verificación alerta
#
# Uso:  ./start.sh            arranque normal (idempotente)
#       ./start.sh --reset    borra contenedores/volúmenes y arranca limpio
#                             (⚠ borra también los datos persistidos en Postgres)
# =============================================================================
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

SENSOR_TOPIC="sensor-vital-data"
ALERT_TOPIC="alertas-emergencia"
FLINK_URL="http://localhost:8081"
JAR_DIR="flink-libs"
MAVEN="https://repo1.maven.org/maven2/org/apache/flink"
MAVEN_PG="https://repo1.maven.org/maven2/org/postgresql/postgresql"

KAFKA_JAR="flink-sql-connector-kafka-3.1.0-1.18.jar"
JSON_JAR="flink-json-1.18.1.jar"
JDBC_JAR="flink-connector-jdbc-3.1.2-1.18.jar"
PG_JAR="postgresql-42.7.3.jar"

TOTAL=7

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RED=$'\033[31m'; BLUE=$'\033[34m'; RESET=$'\033[0m'
else
  BOLD=""; GREEN=""; YELLOW=""; RED=""; BLUE=""; RESET=""
fi
step() { printf "\n%s[%s/%s]%s %s\n" "${BLUE}${BOLD}" "$1" "$TOTAL" "${RESET}" "$2"; }
ok()   { printf "  %s✔%s %s\n" "$GREEN" "$RESET" "$1"; }
info() { printf "  • %s\n" "$1"; }
warn() { printf "  %s!%s %s\n" "$YELLOW" "$RESET" "$1"; }
fail() { printf "\n%s✘ %s%s\n" "$RED" "$1" "$RESET" >&2; exit 1; }

trap 'printf "\n%s✘ Falló en la línea %s.%s Revisa: docker compose logs --tail=50\n" "$RED" "$LINENO" "$RESET" >&2' ERR

DC="docker compose"

wait_for() {
  local desc=$1 timeout=$2 elapsed=0
  shift 2
  until "$@" > /dev/null 2>&1; do
    if (( elapsed >= timeout )); then
      fail "Tiempo agotado esperando: $desc (${timeout}s)"
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
}

flink_has_taskmanager() { curl -sf "$FLINK_URL/overview" | grep -Eq '"taskmanagers":[1-9]'; }
flink_job_running()     { curl -sf "$FLINK_URL/jobs/overview" | grep -q '"state":"RUNNING"'; }

topic_has_data() {
  $DC exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:29092 --topic "$SENSOR_TOPIC" --time -1 2>/dev/null \
    | awk -F: '{ s += $3 } END { exit !(s > 0) }'
}

download_jar() {
  local name=$1 url=$2
  if [[ -s "$JAR_DIR/$name" ]]; then ok "$name"; return 0; fi
  info "Descargando $name ..."
  curl -fL --retry 3 -o "$JAR_DIR/$name" "$url"
  ok "$name descargado"
}

# ---------------------------------------------------------------------------
step 1 "Verificando requisitos"
command -v docker >/dev/null || fail "Docker no está instalado."
docker info >/dev/null 2>&1 || fail "No se puede usar Docker: el servicio no corre o tu usuario no tiene permisos (ver GUIA_LINUX.md)."
$DC version >/dev/null 2>&1 || fail "Se requiere Docker Compose v2 ('docker compose')."
command -v curl >/dev/null || fail "curl no está instalado."
ok "Docker y Docker Compose disponibles"

if [[ "${1:-}" == "--reset" ]]; then
  warn "Modo --reset: eliminando contenedores y volúmenes (incluye datos de Postgres)"
  $DC down -v --remove-orphans
fi

mkdir -p "$JAR_DIR"
download_jar "$KAFKA_JAR" "$MAVEN/flink-sql-connector-kafka/3.1.0-1.18/$KAFKA_JAR"
download_jar "$JSON_JAR"  "$MAVEN/flink-json/1.18.1/$JSON_JAR"
download_jar "$JDBC_JAR"  "$MAVEN/flink-connector-jdbc/3.1.2-1.18/$JDBC_JAR"
download_jar "$PG_JAR"    "$MAVEN_PG/42.7.3/$PG_JAR"

# ---------------------------------------------------------------------------
step 2 "Levantando Kafka, Postgres y Flink (esperando healthchecks)"
$DC up -d --build --wait kafka postgres jobmanager taskmanager
wait_for "TaskManager registrado en Flink" 90 flink_has_taskmanager
ok "Kafka, Postgres y Flink listos (Flink UI: $FLINK_URL)"

# ---------------------------------------------------------------------------
step 3 "Creando tópicos de Kafka"
for spec in "$SENSOR_TOPIC:3" "$ALERT_TOPIC:1"; do
  topic="${spec%%:*}"; partitions="${spec##*:}"
  $DC exec -T kafka kafka-topics --bootstrap-server kafka:29092 \
    --create --if-not-exists --topic "$topic" \
    --partitions "$partitions" --replication-factor 1 >/dev/null
  ok "Tópico '$topic' ($partitions partición/es)"
done

# ---------------------------------------------------------------------------
step 4 "Iniciando el generador de datos"
$DC up -d --build generator
wait_for "primeros datos en '$SENSOR_TOPIC'" 90 topic_has_data
ok "El generador ya está enviando datos"

# ---------------------------------------------------------------------------
step 5 "Enviando el job SQL a Flink"
if flink_job_running; then
  ok "Ya hay un job en ejecución; no se envía un duplicado (usa --reset para reiniciar)"
else
  $DC exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/job.sql
  wait_for "job de Flink en estado RUNNING" 90 flink_job_running
  ok "Job en estado RUNNING"
fi

# ---------------------------------------------------------------------------
step 6 "Iniciando el dashboard"
$DC up -d --build dashboard
wait_for "dashboard Streamlit" 60 curl -sf http://localhost:8501/_stcore/health
ok "Dashboard disponible"

# ---------------------------------------------------------------------------
step 7 "Verificando el flujo completo (primera alerta, hasta 60 s)"
alert=$($DC exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic "$ALERT_TOPIC" --from-beginning --max-messages 1 --timeout-ms 60000 2>/dev/null || true)
if [[ -n "$alert" ]]; then
  ok "Alerta detectada por Flink:"
  printf "    %s\n" "$alert"
else
  warn "Aún no hay alertas. Revisa el job en $FLINK_URL o ejecuta: docker compose logs generator"
fi

# Verificación rápida de persistencia (opcional, no bloqueante)
pg_count=$($DC exec -T postgres psql -U health -d health -tAc \
  "SELECT COUNT(*) FROM alertas_emergencia;" 2>/dev/null | tr -d '[:space:]' || echo "")
if [[ -n "$pg_count" ]]; then
  ok "Alertas persistidas en PostgreSQL: $pg_count"
fi

cat <<MSG

${GREEN}${BOLD}Sistema en ejecución${RESET}
  Dashboard : http://localhost:8501  (pestañas: En vivo / Historial)
  Flink UI  : $FLINK_URL
  Postgres  : localhost:5432  (user/pass/db: health/health/health)
  Logs      : docker compose logs -f generator
  Detener   : ./stop.sh       (conserva datos)
              ./stop.sh --purge  (borra volúmenes e imágenes)
MSG

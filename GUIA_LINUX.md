# Guía de ejecución — Linux

Probado en la lógica para Ubuntu/Debian; en Fedora, Arch y derivados los pasos son equivalentes.

## 1. Requisitos
| Recurso        | Mínimo recomendado                                           |
| -------------- | ------------------------------------------------------------ |
| RAM libre      | 6 GB                                                         |
| Disco libre    | 6 GB (imágenes de Kafka, Flink, PostgreSQL/TimescaleDB y Python) |
| Puertos libres | 9092, 8081, 8501, **5432**                                   |
| Software       | Docker Engine, Docker Compose v2 (`docker compose`), `curl`  |

> Si ya tienes un PostgreSQL local corriendo en el puerto 5432, cambia el mapeo
> `5432:5432` del servicio `postgres` en `docker-compose.yml` por `"5433:5432"`.

## 2. Instalar Docker (una sola vez)
```bash
curl -fsSL https://get.docker.com | sh
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker
```
Verifica:
```bash
docker --version
docker compose version    # v2.x
curl --version
```

## 3. Preparar el proyecto
```bash
unzip ProjectBigData.zip
cd ProjectBigData
chmod +x start.sh stop.sh
```

## 4. Ejecutar
```bash
./start.sh            # arranque normal (idempotente)
./start.sh --reset    # desde cero (borra contenedores, volúmenes y datos de Postgres)
```
La primera vez tarda unos 4–7 minutos (descarga de imágenes, `.jar` y `psycopg2-binary`); después, ~1 minuto.
Al terminar verás la primera alerta detectada y las direcciones:

- Dashboard: http://localhost:8501 → pestañas **"🔴 En vivo"** y **"📚 Historial"**
- Flink UI: http://localhost:8081
- PostgreSQL: `localhost:5432` (user/pass/db: `health`/`health`/`health`)

## 5. Operación diaria
```bash
docker compose ps                       # estado de los servicios
docker compose logs -f generator       # logs del generador (también: dashboard, jobmanager, postgres, kafka)
./stop.sh                               # detener (CONSERVA datos de Postgres)
./stop.sh --purge                       # detener y borrar imágenes/volúmenes locales
```
Para cambiar la cantidad de sensores edita `NUM_SENSORS` del servicio `generator` en `docker-compose.yml` y ejecuta `./start.sh`.

## 6. Verificar y consultar la persistencia

**Alertas crudas en el dashboard:** pestaña **📚 Historial (PostgreSQL)**.

**Consultar Postgres directamente:**
```bash
docker compose exec postgres psql -U health -d health -c \
  "SELECT COUNT(*) AS total, COUNT(DISTINCT patient_id) AS pacientes FROM alertas_emergencia;"

docker compose exec postgres psql -U health -d health -c \
  "SELECT patient_id, max_heart_rate, min_oxygen_saturation,
          to_timestamp(alert_time) AS hora
     FROM alertas_emergencia
     ORDER BY alert_time DESC
     LIMIT 10;"
```

**Los datos sobreviven a reinicios:** `./stop.sh && ./start.sh` conserva el histórico.
Solo `./start.sh --reset` o `./stop.sh --purge` lo eliminan (borran el volumen `pgdata`).

## 7. Problemas frecuentes
| Síntoma                                  | Solución                                                     |
| ---------------------------------------- | ------------------------------------------------------------ |
| `permission denied ... docker.sock`      | `sudo usermod -aG docker $USER` y reinicia sesión (o usa `sudo ./start.sh`). |
| `Cannot connect to the Docker daemon`    | `sudo systemctl start docker`.                               |
| `docker: 'compose' is not a command`     | Instala el plugin: `sudo apt install docker-compose-plugin`. |
| **Puerto 5432 ocupado** (Postgres local) | Cambia el mapeo a `"5433:5432"` en el servicio `postgres` de `docker-compose.yml` y ejecuta `./start.sh --reset`. |
| Puerto 8081/8501 ocupado                 | `sudo ss -ltnp \| grep -E ':(9092\|8081\|8501)'` y detén el proceso, o cambia el mapeo. |
| `bad interpreter: /usr/bin/env bash^M`   | `sed -i 's/\r$//' start.sh stop.sh`.                         |
| SELinux (Fedora/RHEL) bloquea volúmenes  | Agrega `:z` al final de cada volumen en `docker-compose.yml` (`:ro,z`). |
| **Sin alertas persistidas** tras 2 min   | `docker compose logs postgres` y `docker compose logs jobmanager`. Verifica el job en http://localhost:8081. |
| **Job Flink falla al enviar a Postgres** | Los JAR `flink-connector-jdbc` y `postgresql` deben estar en `flink-libs/`; ejecuta de nuevo `./start.sh`. |
| Estado raro tras cambios                 | `./start.sh --reset` (borra datos persistidos, tarda más la primera vez). |

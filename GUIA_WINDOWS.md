# Guía de ejecución — Windows 10/11

## 1. Requisitos
| Recurso        | Mínimo recomendado                                    |
| -------------- | ----------------------------------------------------- |
| Sistema        | Windows 10 (2004 o superior) o Windows 11, 64 bits    |
| Virtualización | Activada en BIOS/UEFI (Intel VT-x / AMD-V)            |
| RAM libre      | 6 GB (8 GB en total recomendado)                      |
| Disco libre    | 6 GB                                                  |
| Puertos libres | 9092, 8081, 8501, **5432**                            |
| Software       | Docker Desktop con backend WSL 2 (incluye Compose v2) |

No necesitas instalar Python, Java, Kafka, Flink ni PostgreSQL: todo corre en contenedores.

> Si ya tienes un PostgreSQL local en el puerto 5432, cambia el mapeo
> `5432:5432` del servicio `postgres` en `docker-compose.yml` por `"5433:5432"`.

## 2. Instalar Docker Desktop (una sola vez)
1. **PowerShell como administrador:** `wsl --install`, luego reinicia.
2. Instala **Docker Desktop** desde docker.com, dejando *Use WSL 2 instead of Hyper-V*.
3. Abre Docker Desktop y espera **Engine running**.
4. Verifica:
```powershell
docker --version
docker compose version
```

## 3. Preparar el proyecto
Descomprime `ProjectBigData.zip` en una ruta corta y local, p. ej. `C:\Proyectos\ProjectBigData`.
Evita OneDrive y rutas profundas.

## 4. Ejecutar
**Opción A — doble clic:** abre la carpeta y haz doble clic en `start.bat`.

**Opción B — PowerShell o Windows Terminal:**
```powershell
cd C:\Proyectos\ProjectBigData
powershell -ExecutionPolicy Bypass -File .\start.ps1           # normal
powershell -ExecutionPolicy Bypass -File .\start.ps1 -Reset    # desde cero
```
La primera vez tarda ~4–7 minutos; después ~1 minuto.
Al terminar verás la primera alerta y las direcciones:

- Dashboard: http://localhost:8501 → pestañas **"🔴 En vivo"** y **"📚 Historial"**
- Flink UI: http://localhost:8081
- PostgreSQL: `localhost:5432` (user/pass/db: `health`/`health`/`health`)

## 5. Operación diaria
```powershell
docker compose ps                       # estado
docker compose logs -f generator       # logs
.\stop.ps1                              # detener (CONSERVA datos de Postgres)
.\stop.ps1 -Purge                       # detener y borrar volúmenes e imágenes
```

## 6. Verificar la persistencia

**Dashboard:** pestaña **📚 Historial (PostgreSQL)**.

**Consultar Postgres directamente:**
```powershell
docker compose exec postgres psql -U health -d health -c "SELECT COUNT(*) FROM alertas_emergencia;"

docker compose exec postgres psql -U health -d health -c `
  "SELECT patient_id, max_heart_rate, min_oxygen_saturation, `
  `to_timestamp(alert_time) AS hora FROM alertas_emergencia `
  `ORDER BY alert_time DESC LIMIT 10;"
```

**Los datos sobreviven a reinicios:** `stop.bat` + `start.bat` conserva el histórico.
Solo `start.ps1 -Reset` o `stop.ps1 -Purge` lo eliminan.

## 7. Alternativa: ejecutar dentro de WSL 2
Abre tu distro Ubuntu, copia el proyecto a `~/ProjectBigData` (**no** en `/mnt/c`) y sigue `GUIA_LINUX.md`. Requiere integración WSL en Docker Desktop (*Settings → Resources → WSL integration*).

## 8. Problemas frecuentes
| Síntoma                                        | Solución                                                     |
| ---------------------------------------------- | ------------------------------------------------------------ |
| *Ejecución de scripts deshabilitada*           | Usa `-ExecutionPolicy Bypass` o el `start.bat`.              |
| *Docker no está corriendo*                     | Abre Docker Desktop y espera **Engine running**.             |
| `wsl --install` pide virtualización            | Actívala en BIOS/UEFI (SVM / Intel VT-x) y reinicia.         |
| **Puerto 5432 ocupado**                        | Cambia el mapeo a `"5433:5432"` en el servicio `postgres` del `docker-compose.yml` y ejecuta `.\start.ps1 -Reset`. |
| Puerto 8081/8501 ocupado                       | `netstat -ano \| findstr :8081` → `taskkill /PID <pid> /F`.  |
| Lentitud / contenedores cerrándose por memoria | Crea `%UserProfile%\.wslconfig` con `[wsl2]` y `memory=6GB`, y ejecuta `wsl --shutdown`. |
| Error con montajes de `job.sql` o `.jar`       | Borra `flink-libs` si quedó vacía y ejecuta de nuevo el script. |
| Acentos raros en consola                       | Usa Windows Terminal o PowerShell 7 (no afecta el funcionamiento). |
| Error `^M` en `start.sh` bajo WSL              | `sed -i 's/\r$//' start.sh stop.sh`.                         |
| **Sin alertas persistidas** tras 2 min         | `docker compose logs postgres` y `docker compose logs jobmanager`. Verifica el job en http://localhost:8081. |
| **Job Flink falla hacia Postgres**             | Verifica que `flink-libs/` contenga `flink-connector-jdbc-3.1.2-1.18.jar` y `postgresql-42.7.3.jar`; ejecuta de nuevo `.\start.ps1`. |
| Estado raro tras cambios                       | `.\start.ps1 -Reset`.                                        |

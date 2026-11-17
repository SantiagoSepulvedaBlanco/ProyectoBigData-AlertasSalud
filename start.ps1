<#
.SYNOPSIS
  Arranque ordenado del Sistema de Alertas de Salud en Tiempo Real (Windows).
.DESCRIPTION
  Equivalente a start.sh. Orden: infraestructura (Kafka + Postgres + Flink)
  -> topicos -> generador (con datos fluyendo) -> job Flink -> dashboard
  -> verificacion de la primera alerta y de la persistencia.
.PARAMETER Reset
  Elimina contenedores y volumenes previos antes de arrancar
  (incluye los datos persistidos en Postgres).
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\start.ps1
  powershell -ExecutionPolicy Bypass -File .\start.ps1 -Reset
#>
[CmdletBinding()]
param([switch]$Reset)

$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Set-Location -Path $PSScriptRoot

$SensorTopic = 'sensor-vital-data'
$AlertTopic  = 'alertas-emergencia'
$FlinkUrl    = 'http://localhost:8081'
$JarDir      = 'flink-libs'
$Maven       = 'https://repo1.maven.org/maven2/org/apache/flink'
$MavenPg     = 'https://repo1.maven.org/maven2/org/postgresql/postgresql'

$KafkaJar = 'flink-sql-connector-kafka-3.1.0-1.18.jar'
$JsonJar  = 'flink-json-1.18.1.jar'
$JdbcJar  = 'flink-connector-jdbc-3.1.2-1.18.jar'
$PgJar    = 'postgresql-42.7.3.jar'
$Total    = 7

function Step([int]$N, [string]$Msg) { Write-Host ""; Write-Host "[$N/$Total] $Msg" -ForegroundColor Cyan }
function Ok([string]$Msg)   { Write-Host "  [OK] $Msg" -ForegroundColor Green }
function Info([string]$Msg) { Write-Host "  - $Msg" }
function Warn([string]$Msg) { Write-Host "  [!] $Msg" -ForegroundColor Yellow }
function Fail([string]$Msg) { Write-Host ""; Write-Host "[X] $Msg" -ForegroundColor Red; exit 1 }

function Dc {
    & docker compose @args
    if ($LASTEXITCODE -ne 0) {
        Fail "Fallo: docker compose $($args -join ' ') (codigo $LASTEXITCODE). Revisa: docker compose logs --tail=50"
    }
}

function Wait-For([string]$Desc, [int]$Timeout, [scriptblock]$Test) {
    $elapsed = 0
    while ($true) {
        $ready = $false
        try { $ready = [bool](& $Test) } catch { $ready = $false }
        if ($ready) { return }
        if ($elapsed -ge $Timeout) { Fail "Tiempo agotado esperando: $Desc ($Timeout s)" }
        Start-Sleep -Seconds 2
        $elapsed += 2
    }
}

function Test-TaskManager {
    try { return ((Invoke-RestMethod "$FlinkUrl/overview" -TimeoutSec 5).taskmanagers -ge 1) } catch { return $false }
}
function Test-JobRunning {
    try {
        $jobs = (Invoke-RestMethod "$FlinkUrl/jobs/overview" -TimeoutSec 5).jobs
        return (@($jobs | Where-Object { $_.state -eq 'RUNNING' }).Count -gt 0)
    } catch { return $false }
}
function Test-TopicHasData {
    $out = & docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 --topic $SensorTopic --time -1 2>$null
    $sum = 0
    foreach ($line in $out) {
        $p = "$line" -split ':'
        if ($p.Count -ge 3 -and $p[2] -match '^\d+$') { $sum += [long]$p[2] }
    }
    return ($sum -gt 0)
}
function Test-Dashboard {
    try { return ((Invoke-WebRequest 'http://localhost:8501/_stcore/health' -UseBasicParsing -TimeoutSec 5).StatusCode -eq 200) } catch { return $false }
}

function Get-Jar([string]$Name, [string]$Url) {
    $path = Join-Path $JarDir $Name
    if ((Test-Path $path) -and ((Get-Item $path).Length -gt 0)) { Ok $Name; return }
    Info "Descargando $Name ..."
    try { Invoke-WebRequest -Uri $Url -OutFile $path -UseBasicParsing }
    catch { Remove-Item $path -ErrorAction SilentlyContinue; Fail "No se pudo descargar $Name ($($_.Exception.Message))" }
    Ok "$Name descargado"
}

# ---------------------------------------------------------------------------
Step 1 "Verificando requisitos"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Fail "Docker no esta instalado. Instala Docker Desktop (ver GUIA_WINDOWS.md)."
}
& docker info *> $null
if ($LASTEXITCODE -ne 0) { Fail "Docker no esta corriendo. Abre Docker Desktop, espera a que diga 'Engine running' y reintenta." }
& docker compose version *> $null
if ($LASTEXITCODE -ne 0) { Fail "Se requiere Docker Compose v2 ('docker compose'). Actualiza Docker Desktop." }
Ok "Docker y Docker Compose disponibles"

if ($Reset) {
    Warn "Modo -Reset: eliminando contenedores y volumenes (incluye datos de Postgres)"
    Dc down -v --remove-orphans
}

New-Item -ItemType Directory -Force -Path $JarDir | Out-Null
Get-Jar $KafkaJar "$Maven/flink-sql-connector-kafka/3.1.0-1.18/$KafkaJar"
Get-Jar $JsonJar  "$Maven/flink-json/1.18.1/$JsonJar"
Get-Jar $JdbcJar  "$Maven/flink-connector-jdbc/3.1.2-1.18/$JdbcJar"
Get-Jar $PgJar    "$MavenPg/42.7.3/$PgJar"

# ---------------------------------------------------------------------------
Step 2 "Levantando Kafka, Postgres y Flink (esperando healthchecks)"
Dc up -d --build --wait kafka postgres jobmanager taskmanager
Wait-For "TaskManager registrado en Flink" 90 { Test-TaskManager }
Ok "Kafka, Postgres y Flink listos (Flink UI: $FlinkUrl)"

# ---------------------------------------------------------------------------
Step 3 "Creando topicos de Kafka"
$topics = @(@{ Name = $SensorTopic; Partitions = 3 }, @{ Name = $AlertTopic; Partitions = 1 })
foreach ($t in $topics) {
    $name = $t.Name
    $parts = [string]$t.Partitions
    Dc exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic $name --partitions $parts --replication-factor 1 | Out-Null
    Ok "Topico '$name' ($parts particion/es)"
}

# ---------------------------------------------------------------------------
Step 4 "Iniciando el generador de datos"
Dc up -d --build generator
Wait-For "primeros datos en '$SensorTopic'" 90 { Test-TopicHasData }
Ok "El generador ya esta enviando datos"

# ---------------------------------------------------------------------------
Step 5 "Enviando el job SQL a Flink"
if (Test-JobRunning) {
    Ok "Ya hay un job en ejecucion; no se envia un duplicado (usa -Reset para reiniciar)"
} else {
    Dc exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/job.sql
    Wait-For "job de Flink en estado RUNNING" 90 { Test-JobRunning }
    Ok "Job en estado RUNNING"
}

# ---------------------------------------------------------------------------
Step 6 "Iniciando el dashboard"
Dc up -d --build dashboard
Wait-For "dashboard Streamlit" 60 { Test-Dashboard }
Ok "Dashboard disponible"

# ---------------------------------------------------------------------------
Step 7 "Verificando el flujo completo (primera alerta, hasta 60 s)"
$alert = (& docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic $AlertTopic --from-beginning --max-messages 1 --timeout-ms 60000 2>$null) | Out-String
if ($alert.Trim()) {
    Ok "Alerta detectada por Flink:"
    Write-Host "    $($alert.Trim())"
} else {
    Warn "Aun no hay alertas. Revisa el job en $FlinkUrl o ejecuta: docker compose logs generator"
}

# Verificacion de persistencia en Postgres
$pgCount = (& docker compose exec -T postgres psql -U health -d health -tAc "SELECT COUNT(*) FROM alertas_emergencia;" 2>$null) | Out-String
if ($pgCount.Trim()) {
    Ok "Alertas persistidas en PostgreSQL: $($pgCount.Trim())"
}

Write-Host ""
Write-Host "Sistema en ejecucion" -ForegroundColor Green
Write-Host "  Dashboard : http://localhost:8501  (pestanas: En vivo / Historial)"
Write-Host "  Flink UI  : $FlinkUrl"
Write-Host "  Postgres  : localhost:5432  (user/pass/db: health/health/health)"
Write-Host "  Logs      : docker compose logs -f generator"
Write-Host "  Detener   : .\stop.ps1          (conserva datos)"
Write-Host "              .\stop.ps1 -Purge   (borra volumenes e imagenes)"

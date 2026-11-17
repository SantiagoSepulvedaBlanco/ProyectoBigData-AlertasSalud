<#
.SYNOPSIS
  Detiene el sistema. Con -Purge elimina tambien volumenes e imagenes locales.
#>
[CmdletBinding()]
param([switch]$Purge)
Set-Location -Path $PSScriptRoot
if ($Purge) { & docker compose down -v --remove-orphans --rmi local }
else        { & docker compose down --remove-orphans }
if ($LASTEXITCODE -ne 0) { Write-Host "[X] No se pudo detener el sistema." -ForegroundColor Red; exit 1 }
Write-Host "Sistema detenido." -ForegroundColor Green

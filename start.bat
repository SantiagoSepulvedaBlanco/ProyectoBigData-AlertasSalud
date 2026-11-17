@echo off
REM Doble clic para iniciar el sistema (evita el bloqueo de scripts de PowerShell)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
pause

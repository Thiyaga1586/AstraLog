@echo off
setlocal
set "ROOT=%~dp0.."
cd /d "%ROOT%"

where docker >nul 2>&1
if errorlevel 1 (
  echo ERROR: Docker CLI not found. Install and start Docker Desktop.
  exit /b 1
)

docker info >nul 2>&1
if errorlevel 1 (
  echo ERROR: Docker Desktop engine is not running.
  echo Start Docker Desktop, wait for its engine to be ready, then retry.
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%\scripts\secure-monitoring.ps1"
if errorlevel 1 (
  echo ERROR: Grafana authentication startup or verification failed.
  exit /b 1
)

echo.
echo Prometheus: http://127.0.0.1:9090/targets
echo Grafana:    http://127.0.0.1:3000
echo Grafana login: admin
echo The strong password is saved locally in observability\secrets\grafana_admin_password.txt
echo To display it on your own CMD screen: type observability\secrets\grafana_admin_password.txt
echo This stack only collects metrics when the instrumented AstraLog demo is running.
endlocal

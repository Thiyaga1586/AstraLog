@echo off
setlocal
set "ROOT=%~dp0.."

if not exist "%ROOT%\Cargo.toml" (
  echo ERROR: AstraLog Cargo.toml not found next to scripts.
  exit /b 1
)
if not exist "%ROOT%\dashboard\web\package-lock.json" (
  echo ERROR: Control Center frontend package-lock.json not found.
  exit /b 1
)

echo AstraLog Control Center will start three local Command Prompt windows.
echo Ports: broker 7001-7003, Raft 7101-7103, adapter 8787, web 5173.
echo The RF3 demo is disposable. It is not a production deployment.
echo Close each new window or press Ctrl+C there to stop its process.

start "AstraLog RF3 Demo" /D "%ROOT%" cmd /k "cargo run -p astralog-broker --example control_center_demo -- --traffic"
start "AstraLog Read Only Adapter" /D "%ROOT%" cmd /k "cargo run -p astralog-dashboard -- --bootstrap 127.0.0.1:7001 --bind 127.0.0.1:8787 --poll-timeout-ms 5000"
start "AstraLog React UI" /D "%ROOT%\dashboard\web" cmd /k "npm ci && npm run dev"

echo.
echo Open http://127.0.0.1:5173 once the frontend is ready.
echo The browser switches to Live mode after actual broker metadata arrives.
endlocal

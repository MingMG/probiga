@echo off
chcp 65001 >nul
cd /d "%~dp0.."
set PORT=8010
if not defined MYSQL_URL if not defined DATABASE_URL (
    echo ERROR: MYSQL_URL or DATABASE_URL must be configured.
    exit /b 2
)
if not defined API_EMBEDDED_SCHEDULER_ENABLED set "API_EMBEDDED_SCHEDULER_ENABLED=true"
echo ============================================
echo   ProBigA 热门数据看板
echo   http://localhost:%PORT%
echo ============================================
if exist ".venv\Scripts\python.exe" (
    .venv\Scripts\python.exe -m uvicorn server.api.main:app --host 0.0.0.0 --port %PORT%
) else (
    python -m uvicorn server.api.main:app --host 0.0.0.0 --port %PORT%
)
pause

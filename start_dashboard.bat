@echo off
chcp 65001 >nul
cd /d "%~dp0"
set PORT=8010
if not defined MYSQL_URL if not defined DATABASE_URL (
    echo ERROR: MYSQL_URL or DATABASE_URL must be configured.
    exit /b 2
)
echo ============================================
echo   ProBigA 看板启动中...
echo ============================================

echo 1. 杀掉旧进程...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":%PORT%.*LISTENING"') do (
    echo    杀掉 PID: %%a
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul

echo 2. 启动新服务...
echo    地址: http://localhost:%PORT%
echo ============================================
set PYTHON_EXE=%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe
if exist ".venv\Scripts\python.exe" (
    .venv\Scripts\python.exe -m uvicorn server.api.main:app --host 0.0.0.0 --port %PORT%
) else if exist "%PYTHON_EXE%" (
    "%PYTHON_EXE%" -m uvicorn server.api.main:app --host 0.0.0.0 --port %PORT%
) else (
    python -m uvicorn server.api.main:app --host 0.0.0.0 --port %PORT%
)
pause

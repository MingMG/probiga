@echo off
chcp 65001 >nul
if not "%PROBIGA_ALLOW_LEGACY_DEPLOY%"=="I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES" (
    echo Legacy deploy blocked. Set PROBIGA_ALLOW_LEGACY_DEPLOY=I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES to override. 1>&2
    exit /b 64
)
if "%PROBIGA_REMOTE_SSH_HOST%"=="" (echo Set PROBIGA_REMOTE_SSH_HOST first. & exit /b 1)
if "%PROBIGA_REMOTE_SSH_USER%"=="" (echo Set PROBIGA_REMOTE_SSH_USER to a named deploy account first. & exit /b 1)
if /I "%PROBIGA_REMOTE_SSH_USER%"=="root" (echo Root production deploy is forbidden. & exit /b 1)
if "%PROBIGA_SSH_KNOWN_HOSTS%"=="" (echo Set PROBIGA_SSH_KNOWN_HOSTS first. & exit /b 1)
if not exist "%PROBIGA_SSH_KNOWN_HOSTS%" (echo Pinned known-hosts file does not exist. & exit /b 1)
if "%PROBIGA_REMOTE_SSH_KEY_FILE%"=="" (echo Set PROBIGA_REMOTE_SSH_KEY_FILE first. & exit /b 1)
if not exist "%PROBIGA_REMOTE_SSH_KEY_FILE%" (echo Deploy key file does not exist. & exit /b 1)
set SSH_OPTIONS=-o BatchMode=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=yes -o UserKnownHostsFile="%PROBIGA_SSH_KNOWN_HOSTS%" -i "%PROBIGA_REMOTE_SSH_KEY_FILE%"
if "%PROBIGA_REMOTE_ROOT%"=="" (echo Set PROBIGA_REMOTE_ROOT first. & exit /b 1)
set "SERVER=%PROBIGA_REMOTE_SSH_USER%@%PROBIGA_REMOTE_SSH_HOST%"
set "REMOTE=%PROBIGA_REMOTE_ROOT%"
cd /d "%~dp0.."

echo Prepare remote directories ...
ssh %SSH_OPTIONS% %SERVER% "mkdir -p %REMOTE%/tools %REMOTE%/server/api/routers %REMOTE%/server/static %REMOTE%/server/static/js %REMOTE%/server/static/css"
if errorlevel 1 goto fail

echo Upload hot_data.py ...
scp %SSH_OPTIONS% server\api\routers\hot_data.py %SERVER%:%REMOTE%/server/api/routers/hot_data.py
if errorlevel 1 goto fail

echo Upload portfolio_math.py ...
scp %SSH_OPTIONS% server\api\routers\portfolio_math.py %SERVER%:%REMOTE%/server/api/routers/portfolio_math.py
if errorlevel 1 goto fail

echo Upload fetch_sector_heat_east_daily.py ...
scp %SSH_OPTIONS% tools\fetch_sector_heat_east_daily.py %SERVER%:%REMOTE%/tools/fetch_sector_heat_east_daily.py
if errorlevel 1 goto fail

echo Upload index.html ...
scp %SSH_OPTIONS% server\static\index.html %SERVER%:%REMOTE%/server/static/index.html
if errorlevel 1 goto fail

echo Upload app.js ...
scp %SSH_OPTIONS% server\static\js\app.js %SERVER%:%REMOTE%/server/static/js/app.js
if errorlevel 1 goto fail

echo Upload style.css ...
scp %SSH_OPTIONS% server\static\css\style.css %SERVER%:%REMOTE%/server/static/css/style.css
if errorlevel 1 goto fail

echo Restart probiga ...
ssh %SSH_OPTIONS% %SERVER% "systemctl restart probiga && sleep 2 && systemctl status probiga --no-pager | head -10"
if errorlevel 1 goto fail

echo.
echo Done. Ctrl+F5 in browser, then reload the portfolio tab.
pause
exit /b 0

:fail
echo Failed. Check pinned SSH identity / network.
pause
exit /b 1

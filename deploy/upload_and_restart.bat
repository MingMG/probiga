@echo off
chcp 65001 >nul
cd /d "E:\My Code\ProBigA"

echo Prepare remote directories ...
ssh root@47.113.123.190 "mkdir -p /opt/ProBigA/tools /opt/ProBigA/server/api/routers /opt/ProBigA/server/static/js /opt/ProBigA/server/static/css"
if errorlevel 1 goto fail

echo Upload hot_data.py ...
scp server\api\routers\hot_data.py root@47.113.123.190:/opt/ProBigA/server/api/routers/hot_data.py
if errorlevel 1 goto fail

echo Upload fetch_sector_heat_east_daily.py ...
scp tools\fetch_sector_heat_east_daily.py root@47.113.123.190:/opt/ProBigA/tools/fetch_sector_heat_east_daily.py
if errorlevel 1 goto fail

echo Upload index.html ...
scp server\static\index.html root@47.113.123.190:/opt/ProBigA/server/static/index.html
if errorlevel 1 goto fail

echo Upload app.js ...
scp server\static\js\app.js root@47.113.123.190:/opt/ProBigA/server/static/js/app.js
if errorlevel 1 goto fail

echo Upload style.css ...
scp server\static\css\style.css root@47.113.123.190:/opt/ProBigA/server/static/css/style.css
if errorlevel 1 goto fail

echo Restart probiga ...
ssh root@47.113.123.190 "systemctl restart probiga && sleep 2 && systemctl status probiga --no-pager | head -10"
if errorlevel 1 goto fail

echo.
echo Done. Ctrl+F5 in browser, then reload the page.
pause
exit /b 0

:fail
echo Failed. Check SSH password / network.
pause
exit /b 1

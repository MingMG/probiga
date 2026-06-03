@echo off
REM 绕过 .ps1 执行策略，在项目根目录执行 AkShare 全市场 K 线同步
cd /d "%~dp0.."
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0sync_stock_kline_akshare.ps1" %*
exit /b %ERRORLEVEL%

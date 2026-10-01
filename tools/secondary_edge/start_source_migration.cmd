@echo off
setlocal
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%~dp0prepare_cold_package.ps1"
set "MigrationExit=%ERRORLEVEL%"
if not "%MigrationExit%"=="0" echo Preparation did not complete. Source will NOT be resumed automatically.
pause
exit /b %MigrationExit%

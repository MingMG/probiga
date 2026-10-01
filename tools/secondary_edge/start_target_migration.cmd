@echo off
setlocal
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%~dp0target_entry.ps1" -PackageRoot "%~dp0."
set "migration_exit=%errorlevel%"
if "%migration_exit%"=="3010" echo Restart Windows. Migration resumes after you sign in; do not start another script.
if not "%migration_exit%"=="0" if not "%migration_exit%"=="3010" echo Migration remains paused and needs attention.
exit /b %migration_exit%

@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py st_a_list_daily
if errorlevel 1 pause

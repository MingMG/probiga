@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py st_a_list_info
if errorlevel 1 pause

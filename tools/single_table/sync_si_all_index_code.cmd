@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py si_all_index_code
if errorlevel 1 pause

@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py si_index_constituent
if errorlevel 1 pause

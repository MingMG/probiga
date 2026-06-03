@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py sm_index_kline
if errorlevel 1 pause

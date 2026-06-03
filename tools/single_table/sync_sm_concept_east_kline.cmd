@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py sm_concept_east_kline
if errorlevel 1 pause

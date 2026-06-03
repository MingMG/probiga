@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py sm_concept_ths_minute
if errorlevel 1 pause

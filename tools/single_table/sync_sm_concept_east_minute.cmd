@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py sm_concept_east_minute
if errorlevel 1 pause

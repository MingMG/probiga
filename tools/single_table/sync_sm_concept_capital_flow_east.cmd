@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py sm_concept_capital_flow_east
if errorlevel 1 pause

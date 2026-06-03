@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py sm_stock_capital_flow_min
if errorlevel 1 pause

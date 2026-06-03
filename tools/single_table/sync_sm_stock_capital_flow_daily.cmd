@echo off
cd /d "%~dp0..\.."
python tools\sync_capital_flow_ths.py
if errorlevel 1 pause

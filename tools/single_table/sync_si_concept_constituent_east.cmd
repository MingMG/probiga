@echo off
cd /d "%~dp0..\.."
python tools\run_single_table.py si_concept_constituent_east
if errorlevel 1 pause

@echo off
REM 仓库根执行: tools\run_all_single_tables.cmd
cd /d "%~dp0\.."

echo [1/2] 新浪指数 si_all_index_code ...
python tools\fetch_si_all_index_code_sina.py

echo.
echo [2/2] 其余表 run_single_table --run-all ...
python tools\run_single_table.py --run-all

if errorlevel 1 (
  echo 有步骤失败，见上方日志。
  pause
  exit /b 1
)
echo 全部完成。
pause

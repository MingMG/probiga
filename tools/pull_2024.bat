@echo off
chcp 65001 >nul
echo ============================================
echo  ProBigA 历史数据拉取（2024-01-01 起）
echo  请确认 SSH 隧道已打开！
echo ============================================
echo.
if not defined MYSQL_URL if not defined DATABASE_URL (
    echo ERROR: MYSQL_URL or DATABASE_URL must be configured.
    exit /b 2
)
set SM_MAX_STOCKS=200
set SM_HTTP_RETRIES=3
set SM_REQUEST_SLEEP=0.5
set SE_SKIP_GLOBAL_TRUNCATE=1

cd /d "E:\My Code\ProBigA"

echo [1/6] 基础数据...
python tools\run_single_table.py si_all_index_code
python tools\run_single_table.py si_index_constituent
python tools\run_single_table.py si_concept_constituent_east
echo [1/6] 完成

echo [2/6] 逐日热门数据...
python -c "from datetime import datetime,timedelta;s=datetime(2024,1,1);e=datetime.now();d=s;from subprocess import run;from sys import executable as py;print(f'开始拉取 {int((e-s).days)+1} 天数据');exec(open('tools/pull_loop_fast.py').read())"

echo [3/6] 逐日龙虎榜...
python -c "from datetime import datetime,timedelta;s=datetime(2024,1,1);e=datetime.now();d=s;from subprocess import run;from sys import executable as py;from os import environ;print('龙虎榜逐日...');exec(open('tools/pull_alist_loop.py').read())"

echo [4/6] 资金流向...
python -c "from datetime import datetime,timedelta;s=datetime.now()-timedelta(days=120);e=datetime.now();d=s;from subprocess import run;from sys import executable as py;print(f'资金流向 {int((e-s).days)} 天');exec(open('tools/pull_flow_loop.py').read())"

echo [5/6] 分红+快照...
python tools\run_single_table.py sm_dividend

echo [6/6] 当天快照...
python tools\fetch_sector_heat_east_daily.py %date:~0,4%-%date:~5,2%-%date:~8,2%
python tools\fetch_hot_rank_ths.py %date:~0,4%-%date:~5,2%-%date:~8,2%
python tools\fetch_hot_concept_ths_daily.py %date:~0,4%-%date:~5,2%-%date:~8,2%
python tools\run_single_table.py sm_stock_current
python tools\run_single_table.py sm_index_current

echo.
echo ============================================
echo  全部完成！
echo ============================================
pause

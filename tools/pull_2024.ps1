#!/usr/bin/env pwsh
# ============================================================================
# ProBigA 历史数据拉取（2024-01-01 起，不含K线）
# 使用方法：
#   1. 先开隧道：ssh -L 3307:127.0.0.1:3306 $env:PROBIGA_REMOTE_SSH_USER@$env:PROBIGA_REMOTE_SSH_HOST
#   2. 在本窗口执行（必须管理员权限）：Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
#   3. 执行本脚本：.\tools\pull_2024.ps1
# ============================================================================
$ErrorActionPreference = "Continue"

if (-not $ENV:MYSQL_URL) {
    Write-Error "MYSQL_URL is not set. Point it at your local tunnel before running this script."
    exit 1
}
$ENV:SM_MAX_STOCKS = "200"
$ENV:SM_HTTP_RETRIES = "3"
$ENV:SM_REQUEST_SLEEP = "0.5"
$ENV:SE_A_LIST_DATE = ""
$ENV:SE_SKIP_GLOBAL_TRUNCATE = "1"

$START_DATE = [datetime]"2024-01-01"
$END_DATE = Get-Date
$TOTAL_DAYS = [math]::Ceiling(($END_DATE - $START_DATE).TotalDays)
$today = $END_DATE.ToString("yyyy-MM-dd")

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  ProBigA 历史数据拉取" -ForegroundColor Cyan
Write-Host "  起始日期: 2024-01-01" -ForegroundColor Cyan
Write-Host "  截止日期: $today" -ForegroundColor Cyan
Write-Host "  总天数: $TOTAL_DAYS" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# ====== 第一步：基础数据（一次性）======
Write-Host "`n----- 1/6 基础数据 -----" -ForegroundColor Green
python tools/run_single_table.py si_all_index_code
Write-Host "✅ 指数代码完成" -ForegroundColor Green
python tools/run_single_table.py si_index_constituent
Write-Host "✅ 指数成分股完成" -ForegroundColor Green
python tools/run_single_table.py si_concept_constituent_east
Write-Host "✅ 东财概念代码完成" -ForegroundColor Green

# ====== 第二步：逐日热门数据（同花顺热股+概念+东财人气+融合）======
Write-Host "`n----- 2/6 逐日热门数据（2024-01-01 ~ 今天）-----" -ForegroundColor Green
$d = $START_DATE
$day = 0
while ($d -le $END_DATE) {
    $dateStr = $d.ToString("yyyy-MM-dd")
    $day++
    if ($day % 100 -eq 0) { Write-Host "  进度: $day/$TOTAL_DAYS ($dateStr)" -ForegroundColor Yellow }
    
    # 同花顺热股（有历史）
    python tools/fetch_hot_rank_ths.py $dateStr 2>$null
    
    # 同花顺概念（有历史）
    python tools/fetch_hot_concept_ths_daily.py $dateStr 2>$null
    
    # 东财人气榜（仅当天有数据，历史日期跳过空）
    python tools/fetch_hot_pop_rank_east.py $dateStr 2>$null
    
    # 融合榜单（有历史）
    python tools/merge_hot_rank.py $dateStr --top 100 2>$null
    python tools/merge_hot_rank.py $dateStr --top 100 --days 3 2>$null
    python tools/merge_hot_rank.py $dateStr --top 100 --days 5 2>$null
    
    $d = $d.AddDays(1)
}
Write-Host "✅ 逐日热门数据完成" -ForegroundColor Green

# ====== 第三步：逐日龙虎榜（2024-01-01 起）======
Write-Host "`n----- 3/6 逐日龙虎榜（2024-01-01 ~ 今天）-----" -ForegroundColor Green
$d = $START_DATE
$day = 0
while ($d -le $END_DATE) {
    $dateStr = $d.ToString("yyyy-MM-dd")
    $day++
    if ($day % 100 -eq 0) { Write-Host "  进度: $day/$TOTAL_DAYS ($dateStr)" -ForegroundColor Yellow }
    
    $ENV:SE_A_LIST_DATE = $dateStr
    python tools/run_single_table.py st_a_list_daily 2>$null
    
    $d = $d.AddDays(1)
}
Write-Host "✅ 龙虎榜列表完成" -ForegroundColor Green

# 明细只拉最近30天（数据量较大）
Write-Host "  龙虎榜明细（最近30天）..." -ForegroundColor Yellow
$d = $END_DATE.AddDays(-30)
while ($d -le $END_DATE) {
    $dateStr = $d.ToString("yyyy-MM-dd")
    $ENV:SE_A_LIST_DATE = $dateStr
    python tools/run_single_table.py st_a_list_info 2>$null
    $d = $d.AddDays(1)
}
Write-Host "✅ 龙虎榜明细完成" -ForegroundColor Green

# ====== 第四步：逐日资金流向（120天 或 2024-01-01 起，取近的）======
Write-Host "`n----- 4/6 个股资金流向（2024-01-01 起）-----" -ForegroundColor Green
$flowStart = $START_DATE
# 资金流向只能拉120天，如果2024-01-01到现在超过120天，只拉最近120天
$maxFlowDays = 120
$flowActualStart = (Get-Date).AddDays(-$maxFlowDays)
if ($flowActualStart -gt $flowStart) { $flowStart = $flowActualStart }

$d = $flowStart
$day = 0
$flowDays = [math]::Ceiling(($END_DATE - $flowStart).TotalDays)
while ($d -le $END_DATE) {
    $dateStr = $d.ToString("yyyy-MM-dd")
    $day++
    if ($day % 30 -eq 0) { Write-Host "  进度: $day/$flowDays ($dateStr)" -ForegroundColor Yellow }
    
    python tools/fetch_sm_stock_capital_flow_daily.py $dateStr 2>$null
    
    $d = $d.AddDays(1)
}
Write-Host "✅ 资金流向完成" -ForegroundColor Green

# ====== 第五步：个股分红（一次性）======
Write-Host "`n----- 5/6 个股分红 -----" -ForegroundColor Green
python tools/run_single_table.py sm_dividend
Write-Host "✅ 分红完成" -ForegroundColor Green

# ====== 第六步：当天行情快照======
Write-Host "`n----- 6/6 当天行情快照 -----" -ForegroundColor Green
python tools/fetch_sector_heat_east_daily.py $today
python tools/fetch_hot_rank_ths.py $today
python tools/fetch_hot_concept_ths_daily.py $today
python tools/fetch_hot_pop_rank_east.py $today
python tools/merge_hot_rank.py $today --top 100
python tools/merge_hot_rank.py $today --top 100 --days 3
python tools/merge_hot_rank.py $today --top 100 --days 5
Write-Host "✅ 当天热门数据完成" -ForegroundColor Green

python tools/run_single_table.py sm_stock_current
python tools/run_single_table.py sm_concept_ths_current
python tools/run_single_table.py sm_concept_east_current
python tools/run_single_table.py sm_index_current
Write-Host "✅ 行情快照完成" -ForegroundColor Green

# ====== 完成 ======
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  🎉 全部数据拉取完成！" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  查看数据量："
Write-Host "  python -c 'from sqlalchemy import text; from server.common.batch_db import create_batch_engine; e=create_batch_engine(); c=e.connect(); [print(f\"{t}: {c.execute(text(\"SELECT COUNT(*) FROM \"+t)).scalar()} rows\") for t in [\"st_hot_rank_ths\",\"st_hot_concept_ths_daily\",\"st_hot_rank_fused\",\"st_hot_rank_multi_day\",\"st_a_list_daily\",\"sm_stock_capital_flow_daily\"]]; c.close()'"
Write-Host ""
pause

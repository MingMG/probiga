#!/usr/bin/env bash
# ============================================================================
# ProBigA 全量历史数据拉取脚本
# 说明：
#   - 在服务器上执行：nohup bash /root/pull_history.sh > /root/pull_history.log 2>&1 &
#   - 查看进度：tail -f /root/pull_history.log
#   - 依赖顺序已排好，不会错
# ============================================================================
set -e
cd /opt/ProBigA
source venv/bin/activate
if [[ -z "${MYSQL_URL:-}" && -z "${DATABASE_URL:-}" ]]; then
    echo "MYSQL_URL or DATABASE_URL must be configured." >&2
    exit 2
fi

NOW=$(date '+%Y-%m-%d %H:%M:%S')
echo "========================================"
echo "  ProBigA 全量拉取开始: $NOW"
echo "========================================"

# ---- 环境变量（控制内存 & 请求频率） ----
export SM_MAX_STOCKS="500"
export SM_MAX_INDEXES="200"
export SM_MAX_CONCEPTS="100"
export SM_HTTP_RETRIES="5"
export SM_HTTP_BACKOFF="3"
export SM_REQUEST_SLEEP="0.5"
export SI_REQUEST_SLEEP="0.5"
export SE_REQUEST_SLEEP="0.5"
export SM_MARKET_START="2020-01-01"

# ---- 第1步：清理旧 SQL 文件 ----
echo ""
echo "========================================"
echo " [1] 清理旧文件..."
echo "========================================"
rm -f /root/probiga_dump.sql /root/probiga_deploy.tar /root/03_scheduled_tasks.sql /root/probiga_dump_part*.sql 2>/dev/null || true
rm -f /opt/ProBigA/tools/03_scheduled_tasks.sql 2>/dev/null || true
echo "  ✅  旧 SQL/tar 文件已删除"
du -sh /var/lib/mysql/probiga/ 2>/dev/null || echo "  (mysql数据目录)"

# ---- 第2步：基础数据（必须最先拉） ----
echo ""
echo "========================================"
echo " [2] 基础数据：股票代码、指数代码、概念成分..."
echo "========================================"

echo "  --- 2-a: 指数代码列表 ---"
python tools/run_single_table.py si_all_index_code
echo "  ✅  si_all_index_code 完成"

echo "  --- 2-b: 指数成分股 ---"
python tools/run_single_table.py si_index_constituent
echo "  ✅  si_index_constituent 完成"

echo "  --- 2-c: 东财概念代码 ---"
python tools/run_single_table.py si_concept_constituent_east
echo "  ✅  si_concept_constituent_east 完成"

echo "  --- 2-d: (完成，2a已包含全量股票代码)---"

# ---- 第3步：热门数据（独立，可并行） ----
echo ""
echo "========================================"
echo " [3] 热门数据..."
echo "========================================"

echo "  --- 3-a: 同花顺热门概念 (近7天) ---"
for d in $(python -c "from datetime import datetime,timedelta; n=datetime.now(); [print((n-timedelta(days=i)).strftime('%Y-%m-%d')) for i in range(7,0,-1)]"); do
    echo "    日期: $d"
    python tools/fetch_hot_concept_ths_daily.py "$d"
done
echo "  ✅  hot_concept 完成"

echo "  --- 3-b: 同花顺热股 (近7天) ---"
for d in $(python -c "from datetime import datetime,timedelta; n=datetime.now(); [print((n-timedelta(days=i)).strftime('%Y-%m-%d')) for i in range(7,0,-1)]"); do
    echo "    日期: $d"
    python tools/fetch_hot_rank_ths.py "$d"
done
echo "  ✅  hot_rank_ths 完成"

echo "  --- 3-c: 东财人气榜 (近7天) ---"
for d in $(python -c "from datetime import datetime,timedelta; n=datetime.now(); [print((n-timedelta(days=i)).strftime('%Y-%m-%d')) for i in range(7,0,-1)]"); do
    echo "    日期: $d"
    python tools/fetch_hot_pop_rank_east.py "$d"
done
echo "  ✅  hot_pop_east 完成"

echo "  --- 3-d: 融合榜单 ---"
python tools/merge_hot_rank.py $(date '+%Y-%m-%d') --top 100
echo "  ✅  fused 完成"
python tools/merge_hot_rank.py $(date '+%Y-%m-%d') --top 100 --days 3
echo "  ✅  fused(3天) 完成"
python tools/merge_hot_rank.py $(date '+%Y-%m-%d') --top 100 --days 5
echo "  ✅  fused(5天) 完成"

# ---- 第4步：个股行情数据（依赖 si_all_code）----
echo ""
echo "========================================"
echo " [4] 个股行情数据..."
echo "========================================"

echo "  --- 4-a: 个股行情快照 ---"
python tools/run_single_table.py sm_stock_current
echo "  ✅  sm_stock_current 完成"

echo "  --- 4-b: 个股分钟行情 ---"
python tools/run_single_table.py sm_stock_minute
echo "  ✅  sm_stock_minute 完成"

echo "  --- 4-c: 个股五档行情 ---"
python tools/run_single_table.py sm_stock_five_level
echo "  ✅  sm_stock_five_level 完成"

echo "  --- 4-d: 个股分红 ---"
python tools/run_single_table.py sm_dividend
echo "  ✅  sm_dividend 完成"

echo "  --- 4-e: 个股资金流向(分钟) ---"
python tools/run_single_table.py sm_stock_capital_flow_min
echo "  ✅  sm_stock_capital_flow_min 完成"

# ---- 第5步：K线数据（2020年起，分批防OOM）----
echo ""
echo "========================================"
echo " [5] K线数据..."
echo "========================================"

echo "  --- 5-a: 个股K线（2020-01-01 起，每100只写一次库）---"
export SM_MARKET_START="2020-01-01"
python -m biz.stock_market.sync_stock_market --only stock_kline --limit -1
echo "  ✅  sm_stock_kline 完成"

echo "  --- 5-b: 个股分笔成交 ---"
python tools/run_single_table.py sm_stock_bar
echo "  ✅  sm_stock_bar 完成"

echo "  --- 5-c: 指数K线 ---"
python tools/run_single_table.py sm_index_kline
echo "  ✅  index_kline 完成"

# ---- 第6步：个股日度资金流向（2026-01-01起，逐日拉取）----
echo ""
echo "========================================"
echo " [6] 个股日度资金流向（2026-01-01起）..."
echo "========================================"
python -c "
from datetime import datetime, timedelta
start = datetime(2026, 1, 1)
end = datetime.now()
d = start
while d <= end:
    date_str = d.strftime('%Y-%m-%d')
    print(f'  {date_str}', flush=True)
    import subprocess, sys
    subprocess.run([sys.executable, 'tools/fetch_sm_stock_capital_flow_daily.py', date_str], capture_output=False)
    d += timedelta(days=1)
"
echo "  ✅  capital_flow_daily 完成"

# ---- 第7步：概念行情 ----
echo ""
echo "========================================"
echo " [7] 概念行情..."
echo "========================================"

echo "  --- 7-a: 东财概念行情 ---"
python tools/run_single_table.py sm_concept_east_current
echo "  ✅  concept_east_current 完成"

echo "  --- 7-b: 东财概念分钟 ---"
python tools/run_single_table.py sm_concept_east_minute
echo "  ✅  concept_east_minute 完成"

echo "  --- 7-c: 同花顺概念行情 ---"
python tools/run_single_table.py sm_concept_ths_current
echo "  ✅  concept_ths_current 完成"

echo "  --- 7-d: 同花顺概念分钟 ---"
python tools/run_single_table.py sm_concept_ths_minute
echo "  ✅  concept_ths_minute 完成"

echo "  --- 7-e: 概念资金流向 ---"
python tools/run_single_table.py sm_concept_capital_flow_east
echo "  ✅  concept_capital_flow 完成"

# ---- 第8步：指数行情 ----
echo ""
echo "========================================"
echo " [8] 指数行情..."
echo "========================================"

echo "  --- 8-a: 指数行情 ---"
python tools/run_single_table.py sm_index_current
echo "  ✅  index_current 完成"

echo "  --- 8-b: 指数分钟 ---"
python tools/run_single_table.py sm_index_minute
echo "  ✅  index_minute 完成"

# ---- 第9步：龙虎榜 ----
echo ""
echo "========================================"
echo " [9] 龙虎榜..."
echo "========================================"
python tools/run_single_table.py st_a_list_daily
echo "  ✅  a_list_daily 完成"
python tools/run_single_table.py st_a_list_info
echo "  ✅  a_list_info 完成"

# ---- 完成 ----
END=$(date '+%Y-%m-%d %H:%M:%S')
echo ""
echo "========================================"
echo "  🎉 全量拉取完成！"
echo "  开始时间: $NOW"
echo "  结束时间: $END"
echo "========================================"
echo ""
echo "  查看数据量:"
echo "    python tools/_db_size.py"
echo ""

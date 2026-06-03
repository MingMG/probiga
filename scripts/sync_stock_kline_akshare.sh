#!/usr/bin/env bash
# 类 a_share_daily_import：YYYYMMDD、offset/limit、可选第 5 参 --skip-progress
# 用法: ./scripts/sync_stock_kline_akshare.sh 20200101 20260417 0 0
#       ./scripts/sync_stock_kline_akshare.sh 20200101 20260417 0 0 --skip-progress
#       PROGRESS_FILE=/path/file.txt ./scripts/sync_stock_kline_akshare.sh 20200101 20260417 0 200

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

START="${1:-20200101}"
END="${2:-$(date +%Y%m%d)}"
OFF="${3:-0}"
LIM="${4:-0}"
DEFAULT_PF="${PROGRESS_FILE:-$ROOT/stock_kline_akshare_progress.txt}"

ARGS=(python -m biz.stock_market.sync_stock_market --only stock_kline --kline-source akshare
  --start-date "$START" --end-date "$END" --offset "$OFF" --limit "$LIM")
if [[ "${5:-}" == "--skip-progress" ]]; then
  ARGS+=(--skip-progress)
else
  ARGS+=(--progress-file "$DEFAULT_PF")
fi
if [[ -n "${KLINE_ADJUST:-}" ]]; then
  ARGS+=(--kline-adjust "$KLINE_ADJUST")
fi
exec "${ARGS[@]}"

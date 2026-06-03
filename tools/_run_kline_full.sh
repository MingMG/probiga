#!/bin/bash
cd /opt/ProBigA
export SM_STOCK_KLINE_AKSHARE_TRUNCATE=0
export SM_MAX_STOCKS=0
export SM_STOCK_KLINE_PROGRESS_LOG_EVERY=200
export SM_STOCK_KLINE_AKSHARE_SLEEP=0.5
nohup /opt/ProBigA/venv/bin/python -m biz.stock_market.sync_stock_market --only stock_kline --kline-source akshare >> /tmp/kline_sync.log 2>&1 &
echo "Kline sync started, PID=$!"

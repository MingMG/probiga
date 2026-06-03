#!/usr/bin/env python3
import sys
import os

sys.path.insert(0, "/opt/ProBigA")
os.environ["MYSQL_URL"] = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"

import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

engine = create_engine(os.environ["MYSQL_URL"])
trade_date = "2026-06-02"
min_score = 70
top_per_mode = 30

from tools.screen_stocks import run_trend_strong, run_low_start, run_trend, run_flow

logger.info("Starting screening...")

all_dfs = []
screeners = [
    ("trend_strong", lambda: run_trend_strong(engine, trade_date, top_per_mode, 1, 0, 10, 0.5, 0.8, 2.5, 150.0, 0.95)),
    ("low_start", lambda: run_low_start(engine, trade_date, top_per_mode, 1, 0, 60, 0.28, 1.25, 2.0, 10.5)),
    ("trend", lambda: run_trend(engine, trade_date, top_per_mode, 1, 0, 0)),
    ("flow", lambda: run_flow(engine, trade_date, top_per_mode, 5_000_000)),
]

for name, fn in screeners:
    logger.info(f"Running {name}...")
    try:
        df = fn()
        if df is not None and not df.empty:
            df["_source"] = name
            all_dfs.append(df)
            logger.info(f"  {name}: {len(df)} stocks")
        else:
            logger.info(f"  {name}: empty")
    except Exception as e:
        logger.error(f"  {name} failed: {e}")

logger.info(f"Total all_dfs: {len(all_dfs)}")

if not all_dfs:
    logger.warning("No screening results, exiting")
    sys.exit(0)

combined = pd.concat(all_dfs, ignore_index=True)
combined["stock_code"] = combined["stock_code"].astype(str).str.strip().str.zfill(6)
if "short_name" in combined.columns:
    combined = combined[~combined["short_name"].fillna("").str.contains("ST", case=False)]
combined = combined[combined["stock_code"].str.match(r"^(0|6)")]
dedup = combined.drop_duplicates(subset=["stock_code"])
logger.info(f"After dedup: {len(dedup)} stocks")

# Test analysis engine
from server.engine.stock_analysis_engine import StockAnalysisEngine
analysis_engine = StockAnalysisEngine()

results = []
for _, row in dedup.head(3).iterrows():
    code = str(row["stock_code"]).zfill(6)
    name = row.get("short_name", code)
    logger.info(f"Analyzing {code} {name}...")
    try:
        result = analysis_engine.analyze(code, full_data=True)
        logger.info(f"  score={result.short_term_score}, recommend={result.recommend.status}")
        if result.recommend.status == "ALLOW" and result.short_term_score >= min_score:
            results.append({
                "stock_code": code,
                "short_name": name,
                "ai_score": result.short_term_score,
                "pick_date": trade_date,
            })
    except Exception as e:
        logger.error(f"  Failed: {e}")

logger.info(f"Results: {len(results)} stocks passed filter")

#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, "/opt/ProBigA")
os.environ["MYSQL_URL"] = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"

from server.engine.stock_analysis_engine import StockAnalysisEngine

engine = StockAnalysisEngine()

print("=== full_data=False ===")
result1 = engine.analyze("000001", full_data=False)
print(f"strengths: {result1.strengths}")
print(f"risks: {result1.risks}")
print(f"summary: {result1.summary}")
print(f"scores: fundamental={result1.scores.fundamental}, capital={result1.scores.capital}, technical={result1.scores.technical}")

print("\n=== full_data=True ===")
result2 = engine.analyze("000001", full_data=True)
print(f"strengths: {result2.strengths}")
print(f"risks: {result2.risks}")
print(f"summary: {result2.summary}")
print(f"scores: fundamental={result2.scores.fundamental}, capital={result2.scores.capital}, technical={result2.scores.technical}")

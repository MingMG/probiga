#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from env_config import resolve_tool_mysql_url

from server.common.process_env import temporary_env
from server.engine.stock_analysis_engine import StockAnalysisEngine


def main() -> None:
    with temporary_env({"MYSQL_URL": resolve_tool_mysql_url()}, overwrite=False):
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


if __name__ == "__main__":
    main()

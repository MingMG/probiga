#!/usr/bin/env python3
"""Produce the canonical post-close research artifact for the daily report flow."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.evening_review.generate import published_report, resolve_review_trade_date
from biz.review.quant_digest import generate_quant_digest
from biz.review.research_flow import load_digest
from server.common.batch_db import create_batch_engine


def get_engine():
    return create_batch_engine(connect_args={"connect_timeout": 8, "read_timeout": 45, "write_timeout": 30})


def main():
    parser = argparse.ArgumentParser(description="生成统一盘后复盘")
    dates = parser.add_mutually_exclusive_group(required=True)
    dates.add_argument("date", nargs="?", help="已闭市交易日 YYYY-MM-DD")
    dates.add_argument("--today", action="store_true", help="使用权威最近闭市交易日")
    parser.add_argument("--force", action="store_true", help="重新生成同日研究；默认复用当前规则下的已验证产物")
    args = parser.parse_args()
    engine = get_engine()
    target = resolve_review_trade_date(engine, args.date or "")
    existing = load_digest(engine, target, required=False) if not args.force else None
    if existing is not None:
        try:
            content = published_report(existing, target)
        except RuntimeError:
            # Only the producer rebuilds outdated or incomplete artifacts from
            # verified sources. Consumers never substitute or regenerate them.
            pass
        else:
            print(content)
            return 0
    result = generate_quant_digest(engine, target, persist=True)
    if result.get("publish_status") != "ready":
        print(json.dumps(result.get("quality_json") or {}, ensure_ascii=False, indent=2))
        return 2
    print(published_report(result, target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

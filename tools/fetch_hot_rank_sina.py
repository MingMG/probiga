#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Fail closed for Sina's unverifiable pseudo-attention ranking."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from server.common.hot_rank_schema import validate_hot_rank_runtime_schema
from server.common.hot_rank_source_contract import (
    HOT_RANK_SINA_TASK_TYPE,
    HotRankDataBlocked,
    SINA_ATTENTION_DATA_BLOCK_REASON,
    build_blocked_receipt,
    shanghai_now,
)

# ``Market_Center.getHQNodeData`` silently ignores ``sort=attention``: the
# response has no attention/heat field and falls back to a security-code
# ordering.  Shape checks alone would therefore certify a fabricated "hot"
# Top100.  Keep the collector fail-closed until Sina exposes a provider field
# whose ranking semantics can be independently verified.
def _run_ddl(engine) -> None:
    validate_hot_rank_runtime_schema(engine, tables={"st_hot_rank_sina"})


def _fetch_sina_rows() -> list[dict]:
    """Retired unsafe helper retained as an explicit fail-closed boundary."""

    raise HotRankDataBlocked(SINA_ATTENTION_DATA_BLOCK_REASON)


def fetch_hot_rank_sina(
    snapshot_date: str,
    top: int = 100,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    if int(top) != 100:
        raise ValueError("Sina formal publisher requires exactly top=100")
    # This must stay before schema inspection, provider requests, and writes.
    # It is also deliberately independent of the requested date: Sina has no
    # valid success path, so every invocation has the same auditable block.
    # The endpoint currently returns a quote inventory ordered by stock code.
    raise HotRankDataBlocked(SINA_ATTENTION_DATA_BLOCK_REASON)


def main() -> int:
    parser = argparse.ArgumentParser(description="新浪当前交易日热股Top100同步")
    parser.add_argument(
        "snapshot_date",
        nargs="?",
        default=shanghai_now().date().isoformat(),
        help="快照日期 YYYY-MM-DD",
    )
    parser.add_argument("--top", type=int, default=100, help="固定为100")
    args = parser.parse_args()
    try:
        datetime.strptime(args.snapshot_date, "%Y-%m-%d")
    except ValueError:
        print("日期格式错误，应为 YYYY-MM-DD", file=sys.stderr)
        return 1

    started_at = shanghai_now()
    try:
        result = fetch_hot_rank_sina(
            args.snapshot_date,
            args.top,
            now=started_at,
        )
    except HotRankDataBlocked as exc:
        result = build_blocked_receipt(
            task_type=HOT_RANK_SINA_TASK_TYPE,
            requested_date=args.snapshot_date,
            started_at=started_at,
            reason=str(exc),
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        print(f"Sina hot rank sync blocked: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Sina hot rank sync failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

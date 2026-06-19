#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Probe MyQuant bridge connectivity without printing secrets."""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.myquant.bridge import current, history, is_configured


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe MyQuant/Goldminer SDK bridge")
    parser.add_argument("--symbol", default="600519", help="A-share stock code, default 600519")
    parser.add_argument("--start", default="", help="Start date/time, default 14 days ago")
    parser.add_argument("--end", default="", help="End date/time, default today")
    parser.add_argument("--frequency", default="1d", help="Frequency such as 1d, 60s, 300s")
    args = parser.parse_args()

    if not is_configured():
        print("MyQuant bridge is not configured. Check GM_TOKEN and runtime/emquant-py36/python.exe.")
        return 2

    end = args.end or date.today().isoformat()
    start = args.start or (date.today() - timedelta(days=14)).isoformat()
    bars = history(
        [args.symbol],
        frequency=args.frequency,
        start_time=start,
        end_time=end,
        fields="symbol,eob,open,high,low,close,volume,amount",
    )
    print(f"history rows: {len(bars)}")
    if not bars.empty:
        print(bars.tail(3).to_string(index=False))

    snap = current([args.symbol])
    print(f"current rows: {len(snap)}")
    if not snap.empty:
        cols = [c for c in ["symbol", "price", "open", "high", "low", "cum_volume", "cum_amount", "created_at"] if c in snap.columns]
        print(snap[cols].head(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

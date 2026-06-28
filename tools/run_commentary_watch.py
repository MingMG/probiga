#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.routers.commentary import _run_profile_assessment


def main() -> int:
    parser = argparse.ArgumentParser(description="运行股评监控配置，并可推送企业微信")
    parser.add_argument("--profile-id", type=int, required=True, help="监控配置 ID")
    parser.add_argument("--push", action="store_true", help="评估完成后立即推送企业微信")
    parser.add_argument("--as-of-date", default="", help="评估日期 YYYY-MM-DD")
    args = parser.parse_args()

    result = _run_profile_assessment(
        args.profile_id,
        push=args.push,
        as_of_date=args.as_of_date or None,
    )
    profile = result.get("profile") or {}
    summary = {
        "profile_id": args.profile_id,
        "profile_name": profile.get("profile_name"),
        "total": (result.get("result") or {}).get("total"),
        "push": result.get("push"),
    }
    print(summary)
    push_result = result.get("push")
    if push_result and not push_result.get("success"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

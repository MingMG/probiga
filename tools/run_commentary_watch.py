#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.routers.commentary import _run_due_profiles, _run_profile_assessment


def main() -> int:
    parser = argparse.ArgumentParser(description="运行股评监控配置，并可推送企业微信")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--profile-id", type=int, help="监控配置 ID")
    mode.add_argument("--run-due", action="store_true", help="运行当前分钟到点的所有启用配置")
    parser.add_argument("--push", action="store_true", help="评估完成后立即推送企业微信")
    parser.add_argument("--as-of-date", default="", help="评估日期 YYYY-MM-DD")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.run_due:
        summary = _run_due_profiles(push=args.push)
        failures = [row for row in summary.get("profiles") or [] if row.get("status") != "success"]
        exit_code = 1 if failures else 0
    else:
        result = _run_profile_assessment(
            int(args.profile_id),
            push=args.push,
            as_of_date=args.as_of_date or None,
        )
        profile = result.get("profile") or {}
        summary = {
            "status": "success",
            "profile_id": args.profile_id,
            "profile_name": profile.get("profile_name"),
            "total": (result.get("result") or {}).get("total"),
            "push": result.get("push"),
        }
        push_result = result.get("push")
        exit_code = 1 if push_result and not push_result.get("success") else 0
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, default=str))
    else:
        print(summary)
    if exit_code:
        return exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

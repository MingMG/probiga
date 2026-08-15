# -*- coding: utf-8 -*-
"""Freeze and optionally deliver the independent 09:20 auction confirmation."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.premarket.theme_forecast import (
    format_auction_confirmation_markdown,
    run_auction_confirmation,
)
from integrations.wecom.webhook import send_markdown
from server.common.batch_db import create_batch_engine
from server.common.config import get_wecom_webhook


def main() -> int:
    parser = argparse.ArgumentParser(description="09:20 frozen-candidate auction confirmation")
    parser.add_argument("--date", default="", help="Session date, default today")
    parser.add_argument("--cutoff", default="", help="Cutoff datetime, default today 09:20:59")
    parser.add_argument("--push", action="store_true", help="Push result to briefing WeCom bot")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    now = datetime.now().replace(microsecond=0)
    session_date = (args.date or now.date().isoformat())[:10]
    cutoff = datetime.fromisoformat(args.cutoff) if args.cutoff else datetime.combine(
        datetime.fromisoformat(session_date).date(), time(9, 20, 59)
    )
    if cutoff.date() == now.date():
        cutoff = min(cutoff, now)

    engine = create_batch_engine()
    result = run_auction_confirmation(
        engine,
        session_date=session_date,
        cutoff_at=cutoff,
        persist=True,
    )
    if args.push:
        webhook = get_wecom_webhook("briefing", required=False)
        if not str(webhook or "").strip():
            raise RuntimeError("未配置企业微信早报机器人 webhook")
        response = send_markdown(webhook, format_auction_confirmation_markdown(result))
        if not isinstance(response, dict) or response.get("errcode") not in (None, 0):
            raise RuntimeError("企业微信09:20竞价确认未获得成功回执")
        result["delivery"] = {"success": True}

    if args.json:
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        print(format_auction_confirmation_markdown(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Update a frozen post-close review with newly timestamped overnight facts."""
from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path
ensure_adata_import_path(ROOT)
from server.common.batch_db import create_batch_engine
from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.config import get_wecom_webhook
from integrations.wecom.delivery import deliver_markdown
from biz.review.daily_reports import publish_briefing as generate_briefing
log = logging.getLogger("briefing")


def get_engine():
    return create_batch_engine(connect_args={"connect_timeout": 8, "read_timeout": 45, "write_timeout": 30})


def push_to_wecom(content: str, engine=None) -> bool:
    """可靠投递早报；任何缺配置或部分失败都会抛错。"""

    result = deliver_markdown(
        get_wecom_webhook("briefing", required=False),
        content,
        engine=engine or get_engine(),
        delivery_kind="early_briefing",
        webhook_kind="briefing",
        title="## 📰 A股早报",
    )
    log.info(
        "企微推送完成: delivery_id=%s, %d/%d 段",
        result.delivery_id,
        result.delivered_count,
        result.segment_count,
    )
    return result.success


def _safe_print(text_value: str) -> None:
    try:
        print(text_value)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((text_value + "\n").encode("utf-8", errors="replace"))


def main():
    parser = argparse.ArgumentParser(description="A股统一次日早报")
    delivery = parser.add_mutually_exclusive_group()
    delivery.add_argument("--test", action="store_true", help="真实研究预览，不写数据库、不推送")
    delivery.add_argument("--push", action="store_true", help="显式推送企业微信；默认仅发布或核验网站正式报告")
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    engine = get_engine()
    target = authoritative_closed_trade_date(engine)
    if not target:
        raise RuntimeError("DATA_BLOCKED stage=briefing_calendar: closed session unavailable")
    research = generate_briefing(engine, target, persist=not args.test)
    content = research["content"]
    if args.output:
        Path(args.output).write_text(content, encoding="utf-8")
    if args.test:
        _safe_print(content)
    elif args.push:
        push_to_wecom(content, engine=engine)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

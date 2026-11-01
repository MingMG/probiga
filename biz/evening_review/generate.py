#!/usr/bin/env python3
"""Publish the same unified post-close research artifact used by the website."""
from __future__ import annotations
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from sqlalchemy import text
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path
ensure_adata_import_path(ROOT)
from server.common.batch_db import create_batch_engine, read_records
from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.config import get_wecom_webhook
from integrations.wecom.delivery import deliver_markdown
from biz.review.research_flow import artifact_hash, assert_digest, load_digest
from biz.review.market_decision import rules_identity, source_digest_identity
log = logging.getLogger("evening")


def get_engine():
    return create_batch_engine(connect_args={"connect_timeout": 8, "read_timeout": 45, "write_timeout": 30})


def _iso_trade_date(value: object, *, field: str) -> str:
    raw = str(value or "")[:10]
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"DATA_BLOCKED: {field} is not a valid trade date") from exc
    if parsed.isoformat() != raw:
        raise RuntimeError(f"DATA_BLOCKED: {field} is not a valid trade date")
    return raw


def resolve_review_trade_date(
    engine, explicit_date: str = "", *, now: datetime | None = None
) -> str:
    """Resolve a closed exchange session; never infer readiness from K-line MAX."""

    authoritative = authoritative_closed_trade_date(engine, now=now)
    if not authoritative:
        raise RuntimeError(
            "DATA_BLOCKED: authoritative closed trade date is unavailable"
        )
    authoritative = _iso_trade_date(
        authoritative, field="authoritative closed trade date"
    )
    if not explicit_date:
        return authoritative
    target = _iso_trade_date(explicit_date, field="requested review date")
    if target > authoritative:
        raise RuntimeError(
            "DATA_BLOCKED: requested review date is not a closed session: "
            f"requested={target}, authoritative={authoritative}"
        )
    rows = _query(
        engine,
        "SELECT COUNT(*) AS n FROM si_trade_calendar "
        "WHERE trade_status=1 AND trade_date=:d",
        {"d": target},
    )
    if not rows or int(rows[0].get("n") or 0) != 1:
        raise RuntimeError(
            f"DATA_BLOCKED: requested review date is not an open session: {target}"
        )
    return target


def push_to_wecom(content: str, engine=None) -> bool:
    """可靠投递晚报；任何缺配置或部分失败都会抛错。"""

    result = deliver_markdown(
        get_wecom_webhook("briefing", required=False),
        content,
        engine=engine or get_engine(),
        delivery_kind="evening_review",
        webhook_kind="briefing",
        title="## 📊 盘后复盘",
    )
    log.info(
        "企微推送完成: delivery_id=%s, %d/%d 段",
        result.delivery_id,
        result.delivered_count,
        result.segment_count,
    )
    return result.success


def _query(engine, sql: str, params: dict = None):
    # SQLAlchemy 2.x does not accept a plain SQL string through every
    # execution path. Bind explicitly so named parameters such as ``:d`` are
    # preserved when pandas and SQLAlchemy versions differ.
    return read_records(text(sql), engine, params=params)


def _safe_print(text_value: str) -> None:
    try:
        print(text_value)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((text_value + "\n").encode("utf-8", errors="replace"))


def published_report(digest: dict, target: str) -> str:
    """Validate a frozen producer artifact before any delivery or reuse."""
    assert_digest(digest, target)
    research = (digest.get("factor_validation_json") or {}).get("market_decision") or {}
    content = str(digest.get("compact_review") or "").strip()
    expected_hash = artifact_hash({key: value for key, value in research.items()
                                   if key != "research_sha256"})
    if (research.get("status") != "COMPLETE" or research.get("report_kind") != "review"
            or research.get("review_date") != target
            or research.get("rules_sha256") != rules_identity()
            or research.get("source_digest_sha256") != source_digest_identity(digest)
            or research.get("research_sha256") != expected_hash
            or not content or research.get("content") != content):
        raise RuntimeError("DATA_BLOCKED stage=review_publication: canonical research is missing, stale or inconsistent")
    return content


def main():
    parser = argparse.ArgumentParser(description="A股统一盘后复盘")
    parser.add_argument("date", nargs="?", help="已闭市交易日 YYYY-MM-DD")
    parser.add_argument("--test", action="store_true", help="预览已保存的正式复盘，不重新生成、不推送")
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    engine = get_engine()
    target = resolve_review_trade_date(engine, args.date or "")
    digest = load_digest(engine, target)
    report = published_report(digest, target)
    if args.output:
        Path(args.output).write_text(report, encoding="utf-8")
    if args.test:
        _safe_print(report)
    else:
        push_to_wecom(report, engine=engine)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

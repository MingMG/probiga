#!/usr/bin/env python3
"""Publish the latest completed session's research observations for the UI."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time
import gzip
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import (
    DAILY_CLOSE_READY_TIME,
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.kline_data import get_kline_engine
from server.trading_v3.decision_worker import run_retrospective_research_v3
from server.trading_v3.research_pool import (
    MAX_RESEARCH_PAYLOAD_BYTES,
    publish_research_pool,
    read_research_pool,
    validate_research_payload,
)
from server.trading_v3.versioning import code_version
from tools.env_config import create_tool_engine, load_project_env


PACKAGED_RESEARCH_POOL_ROOT = ROOT / "tools" / "research_pool_seeds"
PACKAGED_RESEARCH_POOL_SEEDS = {
    date(2026, 9, 4): {
        "filename": "2026-09-04.json.gz",
        "gzip_sha256": "e71d4cffd822b249cea8aca8f30bfd4b5cef1ec04fe723441c9da7375767e1e5",
        "payload_file_sha256": "ebf5089f1dfaf69edd5526db96ef5c715c2cd47b350741ef2bbde2d02798a3da",
    },
}


def _current_time(now: datetime | None = None) -> datetime:
    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE)
    return current


def _load_packaged_seed(target: date) -> tuple[dict, bytes]:
    seed = PACKAGED_RESEARCH_POOL_SEEDS.get(target)
    if not seed:
        raise ValueError("No packaged research seed exists for the requested date")
    root = PACKAGED_RESEARCH_POOL_ROOT.resolve(strict=True)
    path = (root / seed["filename"]).resolve(strict=True)
    if path.parent != root or path.is_symlink() or not path.is_file():
        raise ValueError("Packaged research seed path is invalid")
    size = path.stat().st_size
    if size <= 0 or size > MAX_RESEARCH_PAYLOAD_BYTES:
        raise ValueError("Packaged research seed compressed size is invalid")
    compressed = path.read_bytes()
    if len(compressed) != size or hashlib.sha256(compressed).hexdigest() != seed["gzip_sha256"]:
        raise ValueError("Packaged research seed compressed hash differs")
    source_bytes = gzip.decompress(compressed)
    if not source_bytes or len(source_bytes) > MAX_RESEARCH_PAYLOAD_BYTES:
        raise ValueError("Packaged research seed expanded size is invalid")
    if hashlib.sha256(source_bytes).hexdigest() != seed["payload_file_sha256"]:
        raise ValueError("Packaged research seed payload hash differs")
    try:
        payload = json.loads(source_bytes.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Packaged research seed is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("Packaged research seed must be an object")
    return payload, source_bytes


def publish_packaged_research_pool(
    engine,
    *,
    target: date,
    now: datetime | None = None,
) -> dict:
    current = _current_time(now)
    closed = date.fromisoformat(authoritative_closed_trade_date(engine, now=current))
    if target != closed:
        raise ValueError("Packaged research seed date is not the authoritative closed session")
    payload, source_bytes = _load_packaged_seed(target)
    verified = validate_research_payload(payload, expected_date=target, now=current)
    build_sha = code_version()[0]
    publication = publish_research_pool(
        payload,
        publisher_build_sha=build_sha,
        published_at=current,
        source_bytes=source_bytes,
        require_observations=True,
    )
    published_pool = read_research_pool(target, now=current)
    if (
        published_pool.get("pool_readable") is not True
        or published_pool.get("status") not in {"READY", "EMPTY"}
        or published_pool.get("trade_date") != target.isoformat()
        or published_pool.get("artifact_sha256") != verified["artifact_sha256"]
        or published_pool.get("artifact_sha256")
        != publication.get("artifact_sha256")
        or published_pool.get("payload_file_sha256")
        != publication.get("payload_file_sha256")
        or published_pool.get("publisher_build_sha") != build_sha
    ):
        raise RuntimeError("Published packaged research pool failed exact readback")
    summary = dict(published_pool.get("summary") or {})
    observation_count = summary.get("observation_stock_count")
    if (
        not isinstance(observation_count, int)
        or isinstance(observation_count, bool)
        or observation_count <= 0
    ):
        raise RuntimeError("Packaged research pool has no observation candidates")
    return {
        "schema": "probiga.trading-v3-research-pool-task.v1",
        "status": "completed",
        "source": "PACKAGED_VERIFIED_RESEARCH_SEED",
        "trade_date": target.isoformat(),
        "research_known_at": verified["research_known_at"].isoformat(sep=" "),
        "database_writes": False,
        "order_authority": False,
        "notification_eligible": False,
        "publication": publication,
        "readback": {
            "status": published_pool["status"],
            "artifact_sha256": published_pool["artifact_sha256"],
            "payload_file_sha256": published_pool["payload_file_sha256"],
            "summary": summary,
        },
    }


def generate_research_pool(engine, *, kline_engine, now: datetime | None = None) -> dict:
    current = _current_time(now)
    known_at = current.replace(tzinfo=None, microsecond=0)
    target = date.fromisoformat(authoritative_closed_trade_date(engine, now=current))
    cutoff = datetime.combine(
        target, DAILY_CLOSE_READY_TIME if target == known_at.date() else time.max
    )
    if cutoff >= known_at:
        raise ValueError("Research observations require a completed session")
    result = run_retrospective_research_v3(
        engine,
        as_of=target,
        decision_at=cutoff,
        research_known_at=known_at,
        mode="close",
        kline_engine=kline_engine,
        universe_limit=1200,
        per_sleeve_limit=300,
        resolve_fact_cutoff_from_evidence=True,
    )
    result["notification"] = {"status": "suppressed", "reason": "RETROSPECTIVE_RESEARCH"}
    publication = publish_research_pool(
        result,
        publisher_build_sha=code_version()[0],
        require_observations=True,
    )
    published_pool = read_research_pool(target)
    if (
        published_pool.get("pool_readable") is not True
        or published_pool.get("status") not in {"READY", "EMPTY"}
        or published_pool.get("trade_date") != target.isoformat()
        or published_pool.get("artifact_sha256")
        != publication.get("artifact_sha256")
        or published_pool.get("payload_file_sha256")
        != publication.get("payload_file_sha256")
    ):
        raise RuntimeError("Published research pool failed exact readback")
    summary = dict(published_pool.get("summary") or {})
    observation_count = summary.get("observation_stock_count")
    if (
        not isinstance(observation_count, int)
        or isinstance(observation_count, bool)
        or observation_count < 0
    ):
        raise RuntimeError("Published research pool readback summary is invalid")
    if observation_count == 0:
        raise RuntimeError(
            "NO_RESEARCH_OBSERVATION_CANDIDATES: "
            f"total_forecast_count={summary.get('total_forecast_count')!r}, "
            f"excluded_forecast_count={summary.get('excluded_forecast_count')!r}"
        )
    return {
        "schema": "probiga.trading-v3-research-pool-task.v1",
        "status": "completed",
        "trade_date": target.isoformat(),
        "research_known_at": known_at.isoformat(sep=" "),
        "database_writes": False,
        "order_authority": False,
        "notification_eligible": False,
        "publication": publication,
        "readback": {
            "status": published_pool["status"],
            "artifact_sha256": published_pool["artifact_sha256"],
            "payload_file_sha256": published_pool["payload_file_sha256"],
            "summary": summary,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-packaged-seed",
        metavar="YYYY-MM-DD",
        help="publish the fixed verified research seed for this exact closed session",
    )
    args = parser.parse_args()
    load_project_env()
    primary = create_tool_engine()
    kline = None
    try:
        if args.from_packaged_seed:
            target = date.fromisoformat(args.from_packaged_seed)
            result = publish_packaged_research_pool(primary, target=target)
        else:
            kline = get_kline_engine()
            result = generate_research_pool(primary, kline_engine=kline)
    finally:
        primary.dispose()
        if kline is not None and kline is not primary:
            kline.dispose()
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

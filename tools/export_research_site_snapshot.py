#!/usr/bin/env python3
"""Capture a dated research snapshot from the local production API.

The default emits JSON without changing either service. To publish, configure
PROBIGA_RESEARCH_SITE_ORIGIN and PROBIGA_RESEARCH_SITE_TOKEN in the runner's
protected environment and pass --publish.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from http.client import HTTPException
import json
import os
import re
from time import monotonic
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo


SCHEMA = "probiga.research-site-snapshot.v1"
SITE_PATH = "/api/probiga-snapshot"
SHANGHAI = ZoneInfo("Asia/Shanghai")
SOURCES = {
    "market_board": ("/api/monitor/board", "date"),
    "sector_rotation": ("/api/hot-data/sector-rotation", "date"),
    "market_stage": ("/api/market-stage/current", "trade_date"),
    "premarket_forecast": ("/api/hot-data/premarket-theme-forecast", "session_date"),
    "auction": ("/api/v3/premarket/auction-gate", "execution_session_date"),
}


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def request_json(url, *, stage, deadline, body=None, token=None):
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise TimeoutError(f"{stage}: deadline exceeded")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["OAI-Sites-Authorization"] = f"Bearer {token}"
    request = Request(url, data=body, headers=headers)
    try:
        with build_opener(ProxyHandler({}), _NoRedirects()).open(
            request, timeout=min(8, remaining),
        ) as response:
            result = bytearray()
            while chunk := response.read1(65536):
                result.extend(chunk)
                if len(result) > 2 * 1024 * 1024 or monotonic() >= deadline:
                    raise TimeoutError(f"{stage}: response exceeded time or size bound")
        data = json.loads(result)
        if not isinstance(data, dict):
            raise ValueError("expected JSON object")
        return data
    except (OSError, HTTPException, ValueError) as exc:
        status = getattr(exc, "code", type(exc).__name__)
        raise RuntimeError(f"{stage}: {status}") from None


def _fields(value, keys):
    if not isinstance(value, dict):
        return {}
    return {key: value[key] for key in keys.split()
            if key in value and isinstance(value[key], (str, int, float, bool, type(None)))}


def _rows(value, keys, limit):
    return [_fields(row, keys) for row in value[:limit] if isinstance(row, dict)] if isinstance(value, list) else []


def _time(value, captured_at):
    if not isinstance(value, str) or len(value) < 16:
        return None
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=SHANGHAI)
    return timestamp.isoformat() if timestamp <= captured_at else None


def _market_board(raw):
    ladder = raw.get("limit_ladder") or {}
    tiers = [
        {**_fields(row, "streak count"),
         "stocks": _rows(row.get("stocks"), "stock_code short_name", 50)}
        for row in (ladder.get("rows") or [])[:12] if isinstance(row, dict)
    ]
    return {
        **_fields(raw, "sample_size price_sample_size"),
        "distribution": _rows(raw.get("distribution"), "key label lower upper count", 15),
        "indices": _rows(raw.get("indices"), "index_code index_name price change_pct amount as_of", 15),
        "limit_ladder": {**_fields(ladder, "count limit_up_count max_streak available_at"), "rows": tiers},
        "events": _rows(raw.get("events"), "event_type event_family stock_code snapshot_at available_at data_source", 20),
        "unavailable_fields": [field for field in (raw.get("unavailable_fields") or [])[:20]
                               if isinstance(field, str)],
    }


def _rotation(raw):
    return {
        **_fields(raw, "data_source lookback_days"),
        **{key: _rows(raw.get(key), "name avg_change_pct momentum rank_change main_net_inflow", 12)
           for key in ("rising_sectors", "falling_sectors", "rising_concepts", "flow_in_top", "flow_out_top")},
    }


def project_section(name, raw, trade_date, captured_at):
    section = {"status": "DATA_BLOCKED", "source_path": SOURCES[name][0],
               "source_trade_date": None, "source_as_of": None, "reason": "没有可核验的所选日期记录", "data": {}}
    selected = raw.get("trade_date")
    status = str(raw.get("status") or "").upper()
    as_of = raw.get("as_of")
    data = {}
    if name == "market_board" and status in {"AVAILABLE", "PARTIAL"}:
        data = _market_board(raw)
    elif name == "sector_rotation" and not raw.get("error"):
        data = _rotation(raw)
        status = "AVAILABLE" if any(data[key] for key in ("rising_sectors", "rising_concepts", "flow_in_top")) else "DATA_BLOCKED"
        as_of = raw.get("flow_snapshot_at")
    elif name == "market_stage" and status in {"READY", "INSUFFICIENT"}:
        snap = raw.get("snapshot") or {}
        data = {**_fields(raw, "model_version evidence_mode"), **_fields(snap, "state"),
                "periods": {key: _fields(value, "state end_date observation_count")
                            for key, value in (snap.get("periods") or {}).items() if key in {"daily", "weekly", "monthly"}},
                "evidence": [item for item in (snap.get("evidence") or [])[:10] if isinstance(item, str)]}
        as_of = snap.get("as_of")
        status = "AVAILABLE" if status == "READY" and raw.get("evidence_mode") == "VERIFIED_PUBLICATION" else "PARTIAL"
        if raw.get("evidence_mode") == "CURRENT_OBSERVATION":
            section["reason"] = "同一交易日的当前观察，历史决策时点未验证"
    elif name == "premarket_forecast":
        selected, as_of = raw.get("session_date"), raw.get("cutoff_at")
        if (raw.get("run_uid") and raw.get("fallback") is False and raw.get("stage") == "PREMARKET_0908"
                and raw.get("decision_scope") == "RESEARCH_DISPLAY_ONLY" and raw.get("order_authority") is False):
            status = "AVAILABLE"
            data = {**_fields(raw, "run_uid news_evidence_status membership_evidence_status industry_evidence_status"),
                    "themes": _rows(raw.get("themes"), "theme_key theme_name rank score reason", 20),
                    "stock_candidates": _rows(raw.get("stock_candidates"), "stock_code stock_name rank global_rank score reason", 40)}
        else:
            section["reason"] = "没有所选交易日的 09:08 冻结批次"
    elif name == "auction":
        auction = raw.get("data") or {}
        selected, as_of = auction.get("execution_session_date"), auction.get("cutoff_at")
        if auction.get("reason"):
            section["reason"] = str(auction["reason"])[:300]
        if auction.get("status") in {"COMPLETED", "VALID_EMPTY"} and auction.get("source_run_uid"):
            status = "AVAILABLE"
            data = {**_fields(auction, "reason_code evidence_mode"),
                    "summary": _fields(auction.get("summary"), "candidate_count reviewed_count"),
                    "assessments": _rows(auction.get("assessments"), "stock_code stock_name gate_status gap_pct quote_at reason", 40)}
    if selected == trade_date:
        section["source_trade_date"] = selected
        section["source_as_of"] = _time(as_of, captured_at)
        if status in {"AVAILABLE", "PARTIAL"} and data:
            section.update(status=status, data=data, reason=None if status == "AVAILABLE" else section["reason"])
        elif raw.get("reason"):
            section["reason"] = str(raw["reason"])[:300]
    return section


def capture(trade_date, *, source="http://127.0.0.1"):
    deadline = monotonic() + 28

    def release():
        health = request_json(source + "/api/health", stage="source_health", deadline=deadline)
        revision = health.get("release_revision") or {}
        sha = revision.get("actual_git_sha")
        if (health.get("status") != "ok" or revision.get("deployment_mode") != "production"
                or revision.get("matches_expected") is not True or not re.fullmatch(r"[0-9a-f]{40}", str(sha))):
            raise RuntimeError("source_health: production release identity unavailable")
        return sha

    sha = release()

    def read(item):
        name, (path, key) = item
        raw = request_json(source + path + "?" + urlencode({key: trade_date}), stage=name, deadline=deadline)
        if name == "market_stage" and raw.get("status") != "READY":
            current = request_json(source + path, stage="market_stage_current", deadline=deadline)
            if current.get("trade_date") == trade_date and current.get("status") == "READY":
                raw = current
        return name, raw

    with ThreadPoolExecutor(max_workers=len(SOURCES)) as executor:
        observations = dict(executor.map(read, SOURCES.items()))
    if release() != sha:
        raise RuntimeError("source_health: release changed during capture")
    now = datetime.now(timezone.utc)
    sections = {name: project_section(name, raw, trade_date, now) for name, raw in observations.items()}
    if not any(section["status"] in {"AVAILABLE", "PARTIAL"} for section in sections.values()):
        raise RuntimeError("source_coverage: no verified dated research sections")
    return {"schema": SCHEMA, "trade_date": trade_date,
            "captured_at": now.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "source": "probiga_production", "source_build": sha, "sections": sections}


def publish(snapshot):
    site = os.environ.get("PROBIGA_RESEARCH_SITE_ORIGIN", "").rstrip("/")
    token = os.environ.get("PROBIGA_RESEARCH_SITE_TOKEN")
    if not token or not re.fullmatch(r"https://[a-z0-9.-]+\.chatgpt\.site", site):
        raise ValueError("site_config: private Sites origin or service credential missing")
    payload = json.dumps(snapshot, ensure_ascii=False, allow_nan=False).encode()
    if len(payload) > 256 * 1024:
        raise ValueError("site_write: snapshot exceeds 256 KiB")
    deadline = monotonic() + 20
    path = site + SITE_PATH
    try:
        request_json(path, stage="site_write", deadline=deadline, body=payload, token=token)
    except RuntimeError:
        # A timed-out write may have reached D1. Reconcile this exact capture once.
        deadline = monotonic() + 10
    returned = request_json(path + "?" + urlencode({"trade_date": snapshot["trade_date"]}),
                            stage="site_readback", deadline=deadline, token=token)
    recorded_at = _time(returned.get("recorded_at"), datetime.now(timezone.utc))
    if (returned.get("snapshot") != snapshot or not recorded_at
            or datetime.fromisoformat(recorded_at) < datetime.fromisoformat(snapshot["captured_at"])):
        raise RuntimeError("site_readback: persisted snapshot differs from capture")
    return {"status": "COMPLETE", "scope": "site_snapshot_publication",
            "trade_date": snapshot["trade_date"], "captured_at": snapshot["captured_at"],
            "recorded_at": returned["recorded_at"], "sections": {key: item["status"] for key, item in snapshot["sections"].items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--source", default="http://127.0.0.1")
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    try:
        if date.fromisoformat(args.trade_date).isoformat() != args.trade_date:
            raise ValueError("trade_date: expected YYYY-MM-DD")
        source = urlsplit(args.source)
        if (source.scheme != "http" or source.hostname != "127.0.0.1" or source.username or source.password
                or source.path or source.query or source.fragment):
            raise ValueError("source_config: expected an HTTP loopback origin")
        snapshot = capture(args.trade_date, source=args.source)
        print(json.dumps(publish(snapshot) if args.publish else snapshot, ensure_ascii=False, allow_nan=False))
    except (RuntimeError, ValueError) as exc:
        parser.exit(2, f"{exc}\n")


if __name__ == "__main__":
    main()

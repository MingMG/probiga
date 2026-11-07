"""Corroboration for exceptional prices in the post-close review only."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256
import json
import logging
import re
from time import monotonic
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError

from biz.review.amount_integrity import assess_amount_integrity
from server.common.daily_delivery_control import load_completed_market_capture_receipt
from server.common.official_listing_evidence import _fetch
from server.common.turnover_snapshot import (
    TURNOVER_CAPTURE_KIND,
    TURNOVER_PROVIDER,
    TURNOVER_SNAPSHOT_ROW_TABLE,
    TURNOVER_SNAPSHOT_RUN_TABLE,
    TURNOVER_SOURCE_FIELD,
    TURNOVER_UNIT,
    _reconstruct_persisted_capture,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
BSE_PAGE = "https://www.bse.cn/disclosure/agreement_transfer.html"
BSE_ENDPOINT = "https://www.bse.cn/tnqfgkcjxxController/btcjxxList.do"
_MAX_EXCEPTION_ROWS = 16
# QMT stock DAT stores whole 100-share lots and whole yuan. Keep these
# resolution bounds local to corroborated DAT rows.
_SHARE_RESOLUTION = Decimal(100)
_AMOUNT_RESOLUTION = Decimal(1)
_BSE_PAGE_SIZE = 20
_BSE_MAX_PAGES = 4
logger = logging.getLogger(__name__)


def _local(value: datetime) -> datetime:
    return value.astimezone(SHANGHAI).replace(tzinfo=None) if value.tzinfo else value


def _decimal(value) -> Decimal:
    return Decimal(str(value))


def _bse_blocks(day: str, *, deadline: float) -> tuple[dict, datetime]:
    trades = defaultdict(list)
    digest = sha256()
    identities = set()
    with requests.Session() as session:
        session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": BSE_PAGE,
                                "X-Requested-With": "XMLHttpRequest"})
        for page in range(_BSE_MAX_PAGES):
            url = BSE_ENDPOINT + "?" + urlencode({
                "page": page, "startDate": day, "endDate": day, "btzqdm": "",
                "keywords": "", "xxfcbj": "2", "xxfcbjs[]": "2", "position": "now",
                "sortfield": "hqjsrq", "sorttype": "", "callback": "bseRead",
            })
            raw = _fetch(session, "GET", url, deadline=deadline,
                         stage="review_bse_blocks", cap=128 * 1024)
            digest.update(sha256(raw).digest())
            body = raw.decode("utf-8-sig").strip()
            prefix, suffix = "bseRead([", ", ''])"
            if not body.startswith(prefix) or not body.endswith(suffix):
                raise ValueError("BSE block disclosure response wrapper differs")
            document, = json.loads(body[len(prefix):-len(suffix)], parse_float=Decimal)
            rows, total = document["content"], document["totalElements"]
            if (not isinstance(rows, list) or type(total) is not int or total < 0
                    or total > _BSE_PAGE_SIZE * _BSE_MAX_PAGES or document["number"] != page
                    or document["size"] != _BSE_PAGE_SIZE
                    or document["numberOfElements"] != len(rows)
                    or len(rows) != min(_BSE_PAGE_SIZE, max(0, total - page * _BSE_PAGE_SIZE))
                    or (page and total != expected_total)):
                raise ValueError("BSE block disclosure page coverage differs")
            expected_total = total
            for row in rows:
                code = row["hqzqdm"]
                stamp = time.fromisoformat(row["hqcjsj"])
                shares, price = row["hqcjsl"], _decimal(row["hqcjjg"])
                identity = (row["hqbcjhth"], row["hqscjhth"])
                if (not re.fullmatch(r"[489][0-9]{5}", code)
                        or row["hqjsrq"] != day.replace("-", "")
                        or row["hqfcbj"] != "2" or row["xxzrlx"] != "B"
                        or not time(15) <= stamp <= time(15, 30)
                        or type(shares) is not int or shares <= 0
                        or not price.is_finite() or price <= 0
                        or not all(identity) or identity in identities):
                    raise ValueError("BSE block disclosure row identity differs")
                identities.add(identity)
                trades[code].append({"time": stamp.isoformat(), "shares": shares, "price": str(price)})
            if document["lastPage"] is True and len(identities) == total:
                return {"source": BSE_PAGE, "response_root": digest.hexdigest(),
                        "trades": trades}, datetime.now(SHANGHAI).replace(tzinfo=None)
    raise ValueError("BSE block disclosure page bound exhausted")


def _turnover_rows(engine, day: str, codes: list[str], cutoff: datetime, deadline: float) -> tuple[dict, dict]:
    with engine.connect() as connection:
        run = connection.execute(text(f"""
            SELECT /*+ MAX_EXECUTION_TIME(2000) */ run_id, collector_build_sha, decision_at,
                   published_at, expected_count, semantic_sha256, field_value_root_sha256
            FROM {TURNOVER_SNAPSHOT_RUN_TABLE}
            WHERE target_date=:day AND decision_at<=:cutoff AND published_at<=:cutoff
              AND status='COMPLETED' AND capture_kind=:kind AND provider=:provider
              AND source_field=:field AND unit=:unit AND expected_count=fetched_count
              AND expected_count=valid_count AND expected_count=matched_count
              AND expected_count=promoted_count
            ORDER BY expected_count DESC, decision_at DESC, published_at DESC, run_id DESC LIMIT 1
        """), {"day": day, "cutoff": cutoff, "kind": TURNOVER_CAPTURE_KIND,
               "provider": TURNOVER_PROVIDER, "field": TURNOVER_SOURCE_FIELD, "unit": TURNOVER_UNIT}
        ).mappings().one_or_none()
        if run is None or monotonic() >= deadline:
            raise RuntimeError("review turnover completion lookup unavailable")
        completion = load_completed_market_capture_receipt(
            connection, raw_run_id=run["run_id"], stage_name="target_turnover_snapshot",
            target_date=day, build_sha=run["collector_build_sha"], decision_at=cutoff,
        )
        known_at, receipt = completion["known_at"], completion["receipt"]
        if (not run["decision_at"] <= run["published_at"] <= known_at <= cutoff
                or any(receipt.get(key) != run[key] for key in
                       ("expected_count", "semantic_sha256", "field_value_root_sha256"))
                or receipt.get("promoted_count") != run["expected_count"]
                or monotonic() >= deadline):
            raise ValueError("review turnover completion identity differs")
        rows = connection.execute(text(f"""
            SELECT /*+ MAX_EXECUTION_TIME(2000) */ * FROM {TURNOVER_SNAPSHOT_ROW_TABLE}
            WHERE run_id=:run AND trade_date=:day AND k_type=1 AND adjust_type=0
              AND stock_code IN :codes AND LENGTH(raw_payload)<=16384
            LIMIT :limit
        """).bindparams(bindparam("codes", expanding=True)),
            {"run": run["run_id"], "day": day, "codes": codes, "limit": len(codes) + 1}
        ).mappings().all()
        if (len(rows) > len(codes) or len({row["stock_code"] for row in rows}) != len(rows)
                or monotonic() >= deadline):
            raise ValueError("review turnover row coverage differs")
    captures = {}
    for row in rows:
        try:
            captures[row["stock_code"]] = _reconstruct_persisted_capture(
                row, decision_at=run["decision_at"], published_at=run["published_at"],
            )
        except (RuntimeError, ValueError) as exc:
            logger.warning("review turnover row %s %s unavailable: %s", day, row["stock_code"], type(exc).__name__)
    return captures, {"turnover_run_id": run["run_id"], "turnover_known_at": known_at.isoformat()}


def _verified_row(row: dict, capture, capture_evidence: dict, blocks: dict | None,
                  block_known_at: datetime | None) -> dict | None:
    amount, volume, low, high = (_decimal(row[key]) for key in ("amount", "volume", "low", "high"))
    if (volume % _SHARE_RESOLUTION != 0 or amount % _AMOUNT_RESOLUTION != 0
            or capture.target.target_row_id != row["id"]
            or (_decimal(row["open"]), _decimal(row["close"]), low, high, volume)
               != (capture.source_open, capture.source_close, capture.source_low,
                   capture.source_high, capture.source_volume_shares)
            or abs(amount - capture.source_amount) > _AMOUNT_RESOLUTION):
        return None
    auction_amount, auction_volume = capture.source_amount, volume
    trades = (blocks or {}).get("trades", {}).get(row["stock_code"], [])
    if trades:
        auction_amount -= sum(Decimal(trade["price"]) * trade["shares"] for trade in trades)
        auction_volume -= sum(trade["shares"] for trade in trades)
    elif low != high:
        return None
    if auction_amount <= 0 or auction_volume < _SHARE_RESOLUTION:
        return None
    min_shares = max(auction_volume - _SHARE_RESOLUTION + 1,
                     ((auction_amount - _AMOUNT_RESOLUTION) / high).to_integral_value(rounding=ROUND_CEILING))
    max_shares = min(auction_volume + _SHARE_RESOLUTION - 1,
                     ((auction_amount + _AMOUNT_RESOLUTION) / low).to_integral_value(rounding=ROUND_FLOOR))
    if min_shares > max_shares:
        return None
    proof = {**capture_evidence, "kind": "BSE_AUCTION_SCOPE" if trades else "QMT_DAT_LOT_RESOLUTION",
             "source_payload_sha256": capture.raw_payload_sha256, "source_amount": str(capture.source_amount),
             "stock_code": row["stock_code"], "trade_date": str(row["trade_date"])[:10],
             "daily_row_known_at": _local(row["etl_sync_at"]).isoformat(),
             "amount": float(amount), "volume": float(volume), "low": float(low), "high": float(high)}
    known_at = max(_local(row["etl_sync_at"]), datetime.fromisoformat(capture_evidence["turnover_known_at"]))
    if trades:
        known_at = max(known_at, block_known_at)
        proof["blocks"] = {"source": blocks["source"], "response_root": blocks["response_root"],
                           "observed_at": block_known_at.isoformat(), "trades": trades}
    proof["available_at"] = known_at.isoformat()
    return proof


def attach_review_amount_evidence(engine, bars, *, cutoff: datetime | None = None):
    """Only consult extra sources for a small set of real DAT range exceptions."""
    required = {"id", "data_source", "quality_status", "permission_status", "etl_sync_at"}
    if bars.empty or not required <= set(bars.columns):
        return bars
    baseline = assess_amount_integrity(bars, max_samples=_MAX_EXCEPTION_ROWS + 1)
    if not 0 < baseline["inconsistent_count"] <= _MAX_EXCEPTION_ROWS:
        return bars
    codes = [row["stock_code"] for row in baseline["samples"]
             if row["reason"] == "implied_price_outside_daily_range"]
    known_cutoff = _local(cutoff or datetime.now(SHANGHAI))
    candidates = {
        row["stock_code"]: row for row in bars.loc[bars["stock_code"].isin(codes)].to_dict("records")
        if row["data_source"] == "qmt_datadir" and row["quality_status"] == "VERIFIED"
        and row["permission_status"] == "SUPPORTED"
        and isinstance(row["etl_sync_at"], datetime) and _local(row["etl_sync_at"]) <= known_cutoff
    }
    day = baseline["trade_date"]
    if not day or not candidates:
        return bars
    # Historical callers must use only evidence that was already known. The
    # exchange response has no verifiable original publication timestamp.
    deadline = monotonic() + 15
    try:
        captures, capture_evidence = _turnover_rows(engine, day, sorted(candidates), known_cutoff, deadline)
    except (SQLAlchemyError, TimeoutError, RuntimeError, ValueError) as exc:
        logger.warning("review turnover corroboration unavailable for %s: %s", day, type(exc).__name__)
        return bars
    blocks, block_known_at = None, None
    if cutoff is None and any(re.fullmatch(r"[489][0-9]{5}", code) for code in captures):
        try:
            blocks, block_known_at = _bse_blocks(day, deadline=deadline)
        except (ArithmeticError, TimeoutError, RuntimeError, ValueError, KeyError, TypeError) as exc:
            logger.warning("review BSE block disclosure unavailable for %s: %s", day, type(exc).__name__)
    proofs = {code: proof for code, capture in captures.items()
              if (proof := _verified_row(candidates[code], capture, capture_evidence, blocks, block_known_at)) is not None}
    return bars.assign(_review_amount_proof=bars["stock_code"].map(proofs)) if proofs else bars

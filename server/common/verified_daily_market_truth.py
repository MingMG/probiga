"""Provider-neutral truth for a complete, independently sourced daily partition.

QMT embedded Python is no longer a reliable execution environment after vendor
file/network restrictions were enabled.  The free daily collector publishes
explicitly labelled rows and an immutable receipt in the existing acquisition
state table.  This module verifies those rows before strategy consumers use
them; it never relabels a public source as QMT.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
import math
import re
from typing import Any, Mapping

from sqlalchemy import text

from server.common.qmt_attestation_contract import (
    canonical_digest,
    validated_universe_manifest,
)
from server.common.qmt_stock_catalog import load_stock_catalog


VERIFIED_DAILY_RECEIPT_SCHEMA = "probiga.free-daily-market-receipt.v1"
VERIFIED_DAILY_TRUTH_SCHEMA = "probiga.daily-market-consumer-truth.v1"
FREE_DAILY_DATASET = "stock_daily"
FREE_DAILY_PARTITION_KEY = "__daily_partition__"
FREE_DAILY_PROVIDERS = frozenset(
    {
        "tencent_qt",
        "tencent_qt_verified",
        "sina_history",
        "baostock_tencent",
    }
)
FREE_DAILY_MIN_COVERAGE = 0.99
FREE_HISTORY_MIN_CATALOG_COVERAGE = 0.90
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ROW_SOURCES_BY_PROVIDER = {
    "tencent_qt": frozenset({"tencent_qt"}),
    "tencent_qt_verified": frozenset({"tencent_qt_verified"}),
    "sina_history": frozenset({"sina_history"}),
    "baostock_tencent": frozenset({"baostock", "tencent_qt"}),
}
_CODE_CHUNK_SIZE = 400


class VerifiedDailyMarketBlocked(RuntimeError):
    """A free-source partition is missing, incomplete, or unsafe to consume."""


def _date(value: Any) -> str:
    raw = str(value or "").strip()
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError("verified daily market date is invalid") from exc
    if parsed != raw:
        raise ValueError("verified daily market date is invalid")
    return raw


def _timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat(sep=" ")
    raw = str(value or "").strip()[:19]
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").isoformat(
            sep=" "
        )
    except ValueError as exc:
        raise ValueError("verified daily market timestamp is invalid") from exc


def _number_text(value: Any, scale: int) -> str:
    if value is None:
        raise ValueError("verified daily market numeric value is missing")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("verified daily market numeric value is invalid") from exc
    if not number.is_finite():
        raise ValueError("verified daily market numeric value is not finite")
    quantum = Decimal(1).scaleb(-int(scale))
    text = format(number.quantize(quantum), "f")
    return "0" if text in {"-0", ""} else text


def canonical_daily_rows(
    rows: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "stock_code": str(row.get("stock_code") or "").zfill(6),
                "trade_date": _date(str(row.get("trade_date") or "")[:10]),
                "open": _number_text(row.get("open"), 4),
                "close": _number_text(row.get("close"), 4),
                "high": _number_text(row.get("high"), 4),
                "low": _number_text(row.get("low"), 4),
                "volume": _number_text(row.get("volume"), 0),
                "amount": _number_text(row.get("amount"), 0),
                "pre_close": _number_text(row.get("pre_close"), 4),
            }
        )
    return sorted(result, key=lambda item: item["stock_code"])


def is_suspended_daily_row(row: Mapping[str, Any]) -> bool:
    """Return True only for an explicit provider zero-trade suspension row."""

    try:
        return bool(
            str(row.get("quality_status") or "") == "SUSPENDED"
            and str(row.get("permission_status") or "") == "NO_TRADE"
            and Decimal(str(row.get("open"))) == 0
            and Decimal(str(row.get("high"))) == 0
            and Decimal(str(row.get("low"))) == 0
            and Decimal(str(row.get("volume"))) == 0
            and Decimal(str(row.get("amount"))) == 0
        )
    except (InvalidOperation, ValueError, TypeError):
        return False


def load_source_daily_rows(
    connection: Any,
    *,
    source: str,
    target_date: str,
    codes: list[str],
) -> list[Mapping[str, Any]]:
    row_sources = _ROW_SOURCES_BY_PROVIDER.get(source)
    if not row_sources:
        raise ValueError("unsupported verified daily row source")
    source_placeholders = ", ".join(
        f":source_{index}" for index in range(len(row_sources))
    )
    rows: list[Mapping[str, Any]] = []
    for offset in range(0, len(codes), _CODE_CHUNK_SIZE):
        chunk = codes[offset : offset + _CODE_CHUNK_SIZE]
        placeholders = ", ".join(
            f":code_{index}" for index in range(len(chunk))
        )
        rows.extend(
            connection.execute(
                text(
                    f"""
                    SELECT stock_code, trade_date, `open`, `close`, high, low,
                           volume, amount, pre_close, data_source, quality_status,
                           permission_status
                      FROM sm_stock_kline
                     WHERE stock_code IN ({placeholders})
                       AND trade_date=:trade_date
                       AND k_type=1
                       AND adjust_type=0
                       AND data_source IN ({source_placeholders})
                     ORDER BY stock_code
                    """
                ),
                {
                    "trade_date": target_date,
                    **{
                        f"source_{index}": row_source
                        for index, row_source in enumerate(sorted(row_sources))
                    },
                    **{
                        f"code_{index}": code
                        for index, code in enumerate(chunk)
                    },
                },
            ).mappings().all()
        )
    return rows


@dataclass(frozen=True)
class VerifiedDailyMarketTruth:
    provider: str
    target_date: str
    receipt_id: str
    captured_at: str
    decision_known_at: str
    catalog_batch_id: str
    catalog_manifest_hash: str
    catalog_member_set_hash: str
    row_count: int
    code_count: int
    code_set_hash: str
    row_hash: str
    coverage_ratio: float
    truth_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": VERIFIED_DAILY_TRUTH_SCHEMA,
            "provider": self.provider,
            "target_date": self.target_date,
            "run_id": self.receipt_id,
            "run_start_date": self.target_date,
            "run_end_date": self.target_date,
            "run_finished_at": self.captured_at,
            "decision_known_at": self.decision_known_at,
            "catalog_batch_id": self.catalog_batch_id,
            "catalog_manifest_hash": self.catalog_manifest_hash,
            "catalog_member_set_hash": self.catalog_member_set_hash,
            "attested_row_count": self.row_count,
            "code_count": self.code_count,
            "code_set_hash": self.code_set_hash,
            "row_hash": self.row_hash,
            "coverage_ratio": self.coverage_ratio,
            "requested_sessions": [self.target_date],
            "truth_hash": self.truth_hash,
        }


def load_verified_daily_market_truth(
    connection: Any,
    *,
    start_date: str,
    end_date: str,
    decision_known_at: Any,
) -> VerifiedDailyMarketTruth:
    """Recover one exact free-source partition and re-read every business row."""

    start = _date(start_date)
    end = _date(end_date)
    if start != end:
        raise ValueError("verified free daily truth accepts one exact session")
    cutoff = _timestamp(decision_known_at)
    receipt_row = connection.execute(
        text(
            """
            SELECT source, target_date, status, written_rows, last_success_at,
                   detail_json
              FROM acquisition_partition_state
             WHERE dataset=:dataset
               AND source IN (:source_0, :source_1, :source_2, :source_3)
               AND target_date=:target_date
               AND partition_key=:partition_key
               AND status='complete'
             ORDER BY last_success_at DESC
             LIMIT 1
            """
        ),
        {
            "dataset": FREE_DAILY_DATASET,
            "source_0": "tencent_qt",
            "source_1": "tencent_qt_verified",
            "source_2": "sina_history",
            "source_3": "baostock_tencent",
            "target_date": start,
            "partition_key": FREE_DAILY_PARTITION_KEY,
        },
    ).mappings().one_or_none()
    if receipt_row is None:
        raise VerifiedDailyMarketBlocked(
            "no completed free daily-market receipt for %s" % start
        )
    receipt = dict(receipt_row)
    source = str(receipt.get("source") or "")
    if source not in FREE_DAILY_PROVIDERS:
        raise VerifiedDailyMarketBlocked("free daily-market provider differs")
    try:
        detail = json.loads(str(receipt.get("detail_json") or ""))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise VerifiedDailyMarketBlocked(
            "free daily-market receipt JSON is invalid"
        ) from exc
    required = {
        "schema",
        "dataset",
        "source",
        "target_date",
        "captured_at",
        "row_count",
        "code_count",
        "code_set_hash",
        "row_hash",
        "coverage_ratio",
        "catalog_batch_id",
        "catalog_manifest_hash",
        "catalog_member_set_hash",
        "receipt_id",
    }
    if not isinstance(detail, dict) or set(detail) != required:
        raise VerifiedDailyMarketBlocked("free daily-market receipt fields differ")
    unsigned = dict(detail)
    receipt_id = str(unsigned.pop("receipt_id") or "")
    if receipt_id != canonical_digest(unsigned):
        raise VerifiedDailyMarketBlocked("free daily-market receipt hash differs")
    if (
        detail["schema"] != VERIFIED_DAILY_RECEIPT_SCHEMA
        or detail["dataset"] != FREE_DAILY_DATASET
        or detail["source"] != source
        or detail["target_date"] != start
    ):
        raise VerifiedDailyMarketBlocked("free daily-market receipt identity differs")
    captured_at = _timestamp(detail["captured_at"])
    success_at = _timestamp(receipt.get("last_success_at"))
    if captured_at > cutoff or success_at > cutoff:
        raise VerifiedDailyMarketBlocked(
            "free daily-market source crossed the strategy cutoff"
        )

    catalog = load_stock_catalog(
        connection,
        batch_id=str(detail["catalog_batch_id"]),
        decision_known_at=cutoff,
    )
    expected_codes = sorted(catalog.eligible_codes(start))
    rows = load_source_daily_rows(
        connection,
        source=source,
        target_date=start,
        codes=expected_codes,
    )
    canonical = canonical_daily_rows([dict(row) for row in rows])
    codes = [row["stock_code"] for row in canonical]
    code_set_hash = canonical_digest(codes)
    row_hash = canonical_digest(canonical)
    row_count = int(detail.get("row_count") or 0)
    code_count = int(detail.get("code_count") or 0)
    if (
        row_count <= 0
        or code_count <= 0
        or row_count != code_count
        or len(canonical) != row_count
        or len(codes) != len(set(codes))
        or code_set_hash != str(detail.get("code_set_hash") or "")
        or row_hash != str(detail.get("row_hash") or "")
        or int(receipt.get("written_rows") or 0) != row_count
    ):
        raise VerifiedDailyMarketBlocked(
            "free daily-market database rows differ from their receipt"
        )
    for row in rows:
        suspended_row = is_suspended_daily_row(row)
        if str(row.get("data_source") or "") not in _ROW_SOURCES_BY_PROVIDER[
            source
        ]:
            raise VerifiedDailyMarketBlocked(
                "free daily-market row provenance differs"
            )
        if (
            not suspended_row
            and (
                str(row.get("quality_status") or "") != "SOURCE_VERIFIED"
                or str(row.get("permission_status") or "") != "SUPPORTED"
            )
        ):
            raise VerifiedDailyMarketBlocked(
                "free daily-market row quality differs"
            )
        values = [
            row.get("open"),
            row.get("close"),
            row.get("high"),
            row.get("low"),
            row.get("pre_close"),
            row.get("volume"),
            row.get("amount"),
        ]
        if any(
            value is None
            or not math.isfinite(float(value))
            or float(value) < 0
            for value in values
        ):
            raise VerifiedDailyMarketBlocked(
                "free daily-market row contains an invalid numeric value"
            )
        if not suspended_row and (
            float(row["open"]) <= 0
            or float(row["close"]) <= 0
            or float(row["high"]) < max(float(row["open"]), float(row["close"]))
            or float(row["low"]) > min(float(row["open"]), float(row["close"]))
        ):
            raise VerifiedDailyMarketBlocked(
                "free daily-market OHLC relationship differs"
            )

    expected_codes = set(expected_codes)
    received_codes = set(codes)
    catalog_coverage = len(received_codes & expected_codes) / max(
        len(expected_codes), 1
    )
    if source in {"tencent_qt_verified", "sina_history"}:
        coverage = catalog_coverage
        coverage_ok = (
            len(received_codes & expected_codes) == len(received_codes)
            and catalog_coverage >= FREE_HISTORY_MIN_CATALOG_COVERAGE
        )
    else:
        coverage = catalog_coverage
        coverage_ok = coverage >= FREE_DAILY_MIN_COVERAGE
    if (
        catalog.manifest_hash != detail["catalog_manifest_hash"]
        or catalog.member_set_hash != detail["catalog_member_set_hash"]
        or received_codes - expected_codes
        or not coverage_ok
        or abs(coverage - float(detail.get("coverage_ratio") or -1)) > 1e-12
    ):
        raise VerifiedDailyMarketBlocked(
            "free daily-market catalog coverage differs"
        )

    stable = {
        "schema": VERIFIED_DAILY_TRUTH_SCHEMA,
        "provider": source,
        "target_date": start,
        "receipt_id": receipt_id,
        "captured_at": captured_at,
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "row_count": row_count,
        "code_count": code_count,
        "code_set_hash": code_set_hash,
        "row_hash": row_hash,
        "coverage_ratio": coverage,
    }
    return VerifiedDailyMarketTruth(
        provider=source,
        target_date=start,
        receipt_id=receipt_id,
        captured_at=captured_at,
        decision_known_at=cutoff,
        catalog_batch_id=catalog.batch_id,
        catalog_manifest_hash=catalog.manifest_hash,
        catalog_member_set_hash=catalog.member_set_hash,
        row_count=row_count,
        code_count=code_count,
        code_set_hash=code_set_hash,
        row_hash=row_hash,
        coverage_ratio=coverage,
        truth_hash=canonical_digest(stable),
    )


def load_retained_qmt_daily_market_truth(
    connection: Any,
    *,
    start_date: str,
    end_date: str,
    decision_known_at: Any,
) -> VerifiedDailyMarketTruth:
    """Verify an already published QMT history day from its retained archive.

    Vendor restrictions make old capture timestamps unavailable to a new
    process. This path is intentionally limited to historical closed sessions:
    it validates an immutable completed run, its counters and catalog-bound
    per-session manifest instead of rescanning hundreds of thousands of rows
    on every strategy run.
    """

    day = _date(start_date)
    if _date(end_date) != day:
        raise ValueError("retained QMT history accepts one exact session")
    cutoff = _timestamp(decision_known_at)
    run = connection.execute(
        text(
            """
            SELECT run_id, start_date, end_date, finished_at, target_rows,
                   matched_rows, missing_qmt_rows, mismatched_rows,
                   tolerance_json
              FROM qmt_kline_attestation_run
             WHERE provider='gj_big_qmt_inner'
               AND status='COMPLETED'
               AND start_date<=:day
               AND end_date>=:day
               AND finished_at IS NOT NULL
               AND finished_at<=:decision_known_at
             ORDER BY finished_at DESC, run_id DESC
             LIMIT 1
            """
        ),
        {"day": day, "decision_known_at": cutoff},
    ).mappings().one_or_none()
    catalog = load_stock_catalog(connection, decision_known_at=cutoff)
    expected_codes = catalog.eligible_codes(day)
    if run is None:
        rows = load_source_daily_rows(
            connection,
            source="gj_big_qmt_inner",
            target_date=day,
            codes=expected_codes,
        )
        if not rows or any(
            str(row.get("quality_status") or "")
            not in {"", "RAW", "PASS", "VERIFIED", "QMT_ATTESTED"}
            for row in rows
        ):
            raise VerifiedDailyMarketBlocked(
                "retained QMT history row provenance differs"
            )
        canonical = canonical_daily_rows([dict(row) for row in rows])
        codes = [row["stock_code"] for row in canonical]
        coverage = len(codes) / max(len(expected_codes), 1)
        if (
            len(codes) != len(set(codes))
            or coverage < FREE_HISTORY_MIN_CATALOG_COVERAGE
        ):
            raise VerifiedDailyMarketBlocked(
                "retained QMT history coverage differs"
            )
        row_hash = canonical_digest(canonical)
        code_set_hash = canonical_digest(sorted(codes))
        stable = {
            "schema": VERIFIED_DAILY_TRUTH_SCHEMA,
            "provider": "qmt_retained_history",
            "target_date": day,
            "receipt_id": f"retained-row-proof-{row_hash[:24]}",
            "captured_at": cutoff,
            "catalog_batch_id": catalog.batch_id,
            "catalog_manifest_hash": catalog.manifest_hash,
            "catalog_member_set_hash": catalog.member_set_hash,
            "row_count": len(canonical),
            "code_count": len(codes),
            "code_set_hash": code_set_hash,
            "row_hash": row_hash,
            "coverage_ratio": coverage,
        }
        return VerifiedDailyMarketTruth(
            provider="qmt_retained_history",
            target_date=day,
            receipt_id=stable["receipt_id"],
            captured_at=cutoff,
            decision_known_at=cutoff,
            catalog_batch_id=catalog.batch_id,
            catalog_manifest_hash=catalog.manifest_hash,
            catalog_member_set_hash=catalog.member_set_hash,
            row_count=len(canonical),
            code_count=len(codes),
            code_set_hash=code_set_hash,
            row_hash=row_hash,
            coverage_ratio=coverage,
            truth_hash=canonical_digest(stable),
        )
    target_rows = int(run.get("target_rows") or 0)
    if (
        target_rows <= 0
        or int(run.get("matched_rows") or 0) != target_rows
        or int(run.get("missing_qmt_rows") or 0) != 0
        or int(run.get("mismatched_rows") or 0) != 0
    ):
        raise VerifiedDailyMarketBlocked(
            "retained QMT history counters differ"
        )
    manifest = validated_universe_manifest(
        run.get("tolerance_json"),
        start_date=str(run.get("start_date") or "")[:10],
        end_date=str(run.get("end_date") or "")[:10],
    )
    contract = manifest.get(day)
    if not isinstance(contract, Mapping):
        raise VerifiedDailyMarketBlocked(
            "retained QMT history manifest is missing the target session"
        )
    stock_count = int(contract.get("stock_count") or 0)
    code_set_hash = str(contract.get("stock_set_hash") or "")
    if stock_count <= 0 or not _SHA256.fullmatch(code_set_hash):
        raise VerifiedDailyMarketBlocked(
            "retained QMT history universe differs"
        )
    expected_count = len(expected_codes)
    coverage = min(1.0, stock_count / max(expected_count, 1))
    if coverage < FREE_HISTORY_MIN_CATALOG_COVERAGE:
        raise VerifiedDailyMarketBlocked(
            "retained QMT history coverage differs"
        )
    row_hash = canonical_digest(
        {
            "run_id": str(run["run_id"]),
            "day": day,
            "stock_count": stock_count,
            "stock_set_hash": code_set_hash,
        }
    )
    captured_at = _timestamp(run.get("finished_at"))
    stable = {
        "schema": VERIFIED_DAILY_TRUTH_SCHEMA,
        "provider": "qmt_retained_history",
        "target_date": day,
        "receipt_id": str(run["run_id"]),
        "captured_at": captured_at,
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "row_count": stock_count,
        "code_count": stock_count,
        "code_set_hash": code_set_hash,
        "row_hash": row_hash,
        "coverage_ratio": coverage,
    }
    return VerifiedDailyMarketTruth(
        provider="qmt_retained_history",
        target_date=day,
        receipt_id=str(run["run_id"]),
        captured_at=captured_at,
        decision_known_at=cutoff,
        catalog_batch_id=catalog.batch_id,
        catalog_manifest_hash=catalog.manifest_hash,
        catalog_member_set_hash=catalog.member_set_hash,
        row_count=stock_count,
        code_count=stock_count,
        code_set_hash=code_set_hash,
        row_hash=row_hash,
        coverage_ratio=coverage,
        truth_hash=canonical_digest(stable),
    )


__all__ = [
    "FREE_DAILY_DATASET",
    "FREE_HISTORY_MIN_CATALOG_COVERAGE",
    "FREE_DAILY_MIN_COVERAGE",
    "FREE_DAILY_PARTITION_KEY",
    "FREE_DAILY_PROVIDERS",
    "VERIFIED_DAILY_RECEIPT_SCHEMA",
    "VERIFIED_DAILY_TRUTH_SCHEMA",
    "VerifiedDailyMarketBlocked",
    "VerifiedDailyMarketTruth",
    "canonical_daily_rows",
    "is_suspended_daily_row",
    "load_source_daily_rows",
    "load_retained_qmt_daily_market_truth",
    "load_verified_daily_market_truth",
]

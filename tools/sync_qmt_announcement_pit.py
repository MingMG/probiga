#!/usr/bin/env python3
"""Synchronize one full-market official QMT announcement PIT batch."""
from __future__ import annotations

import argparse
from collections.abc import Mapping
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.qmt_announcement_pit import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_WINDOW_DAYS,
    MAX_CAPTURE_DELAY,
    QMT_ANNOUNCEMENT_SOURCE,
    QMT_ANNOUNCEMENT_TASK_SCHEMA,
    QMTAnnouncementBlocked,
    synchronize_qmt_announcements,
    validate_complete_qmt_announcement_batch,
    validate_task_result,
)
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_trade_calendar import load_trade_calendar_receipt
from tools.qmt_announcement_task_contract import (
    QMT_ANNOUNCEMENT_CHECKPOINT_DIR,
)


_BIGQMT_IDENTITY_FIELDS = (
    "strategy_release_protocol",
    "strategy_identity_protocol",
    "strategy_identity_frozen",
    "strategy_identity_status",
    "strategy_build_sha",
    "strategy_git_blob",
    "strategy_source_sha256",
    "strategy_artifact_sha256",
    "strategy_loaded_identity_sha256",
)
_ANNOUNCEMENT_RECEIPT_FIELDS = (
    "frames", "source_method", "period", "count", "dividend_type",
    "fill_data", "subscribe", "download_history", "requested_start_time",
    "requested_end_time", "requested_stock_count",
    "requested_stock_set_sha256", "observed_stock_count",
    "observed_stock_set_sha256", "observed_row_count",
    "estimated_uncompressed_bytes",
)
_MAX_ANNOUNCEMENT_BATCH_ROWS = 200000
_MAX_ANNOUNCEMENT_BATCH_JSON_BYTES = 64 * 1024 * 1024


class BigQmtAnnouncementAdapter:
    """Expose the built-in BigQMT spool as the existing xtdata read contract."""

    force_fresh_capture = True

    def __init__(
        self,
        *,
        bridge=None,
        timeout: int = 600,
        expected_build_sha: str = "",
        release_validator=None,
    ) -> None:
        if bridge is None:
            from integrations.bigqmt import bridge as bigqmt_bridge

            bridge = bigqmt_bridge
        self._bridge = bridge
        self._timeout = max(1, int(timeout))
        self._expected_build_sha = str(expected_build_sha or "").strip().lower()
        if release_validator is None:
            from integrations.bigqmt.release_identity import (
                validate_strategy_release_payload,
            )

            release_validator = validate_strategy_release_payload
        self._release_validator = release_validator
        self._release_proof: dict | None = None
        self._pending: dict[str, tuple[str, str]] = {}
        self._deadline_monotonic: float | None = None

    def bind_capture_deadline(
        self,
        *,
        fact_cutoff_at: datetime,
        max_capture_delay: timedelta,
    ) -> None:
        cutoff = _shanghai_naive(fact_cutoff_at)
        now = datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None)
        remaining = (cutoff + max_capture_delay - now).total_seconds()
        if remaining <= 0:
            raise RuntimeError("BigQMT announcement capture deadline expired")
        self._deadline_monotonic = time.monotonic() + remaining

    def _remaining_timeout(self, cap: int | float) -> float:
        if self._deadline_monotonic is None:
            raise RuntimeError("BigQMT announcement capture deadline is not bound")
        remaining = self._deadline_monotonic - time.monotonic()
        if remaining < 1.0:
            raise RuntimeError("BigQMT announcement capture deadline expired")
        return min(float(cap), remaining)

    def connect(self, *, port: int, remember_if_success: bool) -> None:
        del port
        if remember_if_success is not False:
            raise RuntimeError("BigQMT announcement adapter connection must be ephemeral")
        capabilities = self._bridge.capabilities(
            timeout=self._remaining_timeout(min(180, self._timeout))
        )
        actions = capabilities.get("actions") if isinstance(capabilities, dict) else None
        if (
            not isinstance(capabilities, dict)
            or capabilities.get("status") != "ok"
            or capabilities.get("source") != "gj_big_qmt_inner"
            or capabilities.get("bridge_version") != "bigqmt_inner_v2"
            or capabilities.get("strategy_identity_frozen") is not True
            or capabilities.get("strategy_identity_status") != "BOUND"
            or not isinstance(actions, list)
            or "announcement" not in actions
        ):
            raise RuntimeError("BigQMT announcement capability is unavailable")
        expected_build_sha = (
            self._expected_build_sha
            or os.environ.get("PROBIGA_BUILD_COMMIT_SHA", "").strip().lower()
        )
        if re.fullmatch(r"[0-9a-f]{40}", expected_build_sha) is None:
            raise RuntimeError("BigQMT expected main build SHA is unavailable")
        proof = self._release_validator(
            capabilities,
            expected_build_sha=expected_build_sha,
            root=ROOT,
            source_path=(
                ROOT
                / "integrations"
                / "bigqmt"
                / "qmt_strategy"
                / "probiga_big_qmt_bridge.py"
            ),
        )
        self._release_proof = dict(proof)

    def download_history_data(
        self,
        stock_code: str,
        *,
        period: str,
        start_time: str,
        end_time: str,
        **_kwargs,
    ) -> None:
        code = str(stock_code or "").strip().upper()
        start = str(start_time or "").strip()
        end = str(end_time or "").strip()
        if period != "announcement" or not code or len(start) != 14 or len(end) != 14:
            raise RuntimeError("BigQMT announcement download scope differs")
        self._pending[code] = (start, end)

    def get_market_data_ex(
        self,
        *,
        field_list,
        stock_list,
        period: str,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: str,
        fill_data: bool,
    ) -> dict:
        if self._release_proof is None:
            raise RuntimeError("BigQMT announcement adapter is not connected")
        codes = [str(code or "").strip().upper() for code in stock_list]
        start = str(start_time or "").strip()
        end = str(end_time or "").strip()
        if (
            field_list != []
            or not codes
            or len(codes) != len(set(codes))
            or period != "announcement"
            or count != -1
            or dividend_type != "none"
            or fill_data is not False
            or any(self._pending.get(code) != (start, end) for code in codes)
        ):
            raise RuntimeError("BigQMT announcement read contract differs")
        capture = self._bridge.announcement_capture(
            codes,
            start_date=start,
            end_date=end,
            download_history=True,
            timeout=self._remaining_timeout(self._timeout),
        )
        if (
            not isinstance(capture, Mapping)
            or capture.get("status") != "ok"
            or capture.get("action") != "announcement"
            or capture.get("source") != "gj_big_qmt_inner"
            or capture.get("bridge_version") != "bigqmt_inner_v2"
            or capture.get("source_method")
            not in {
                "ContextInfo.get_market_data_ex_ori",
                "ContextInfo.get_market_data_ex",
            }
            or capture.get("requested_start_time") != start
            or capture.get("requested_end_time") != end
            or capture.get("requested_stock_count") != len(codes)
            or capture.get("period") != "announcement"
            or capture.get("count") != -1
            or capture.get("dividend_type") != "none"
            or capture.get("fill_data") is not False
            or capture.get("subscribe") is not False
            or capture.get("download_history") is not True
            or capture.get("requested_stock_set_sha256")
            != _announcement_stock_set_sha256(codes)
            or capture.get("observed_stock_count") != len(codes)
            or capture.get("observed_stock_set_sha256")
            != _announcement_stock_set_sha256(codes)
            or type(capture.get("observed_row_count")) is not int
            or not 0 <= capture["observed_row_count"] <= (
                _MAX_ANNOUNCEMENT_BATCH_ROWS
            )
            or type(capture.get("estimated_uncompressed_bytes")) is not int
            or not 0 <= capture["estimated_uncompressed_bytes"] <= (
                _MAX_ANNOUNCEMENT_BATCH_JSON_BYTES
            )
        ):
            raise RuntimeError("BigQMT announcement response provenance differs")
        for field in _BIGQMT_IDENTITY_FIELDS:
            if capture.get(field) != self._release_proof.get(field):
                raise RuntimeError(
                    f"BigQMT announcement response release identity differs: {field}"
                )
        receipt_payload = {
            field: capture.get(field) for field in _ANNOUNCEMENT_RECEIPT_FIELDS
        }
        expected_receipt = hashlib.sha256(json.dumps(
            receipt_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")).hexdigest()
        if capture.get("capture_receipt_sha256") != expected_receipt:
            raise RuntimeError("BigQMT announcement capture receipt differs")
        frames = self._bridge.announcement_frames(dict(capture))
        if set(frames) != set(codes):
            raise RuntimeError("BigQMT announcement response stock scope differs")
        for code in codes:
            self._pending.pop(code, None)
        return frames


def _announcement_stock_set_sha256(codes) -> str:
    ordered = sorted(set(str(code) for code in codes))
    encoded = json.dumps(
        ordered, ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _announcement_data_adapter(mode: str, *, platform_name: str | None = None):
    selected = str(mode or "auto").strip().lower()
    platform = os.name if platform_name is None else str(platform_name)
    if selected == "auto":
        selected = "bigqmt" if platform == "nt" else "xtdata"
    if selected == "bigqmt":
        return BigQmtAnnouncementAdapter()
    if selected == "xtdata":
        from integrations.qmt.runtime import import_xtdata

        return import_xtdata()
    raise ValueError("QMT announcement data adapter is invalid")


def _announcement_capture_options(adapter, *, engine, no_resume: bool) -> dict:
    force_fresh_capture = bool(
        getattr(adapter, "force_fresh_capture", False)
    )
    coverage_target_date = None
    if force_fresh_capture:
        coverage_target_date = authoritative_closed_trade_date(engine)
        if not coverage_target_date:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_TRADE_DATE_UNAVAILABLE"
            )
    return {
        "resume": not bool(no_resume) and not force_fresh_capture,
        "coverage_target_date": coverage_target_date,
    }


def _is_production() -> bool:
    return os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == (
        "production"
    )


def _checkpoint_root(value: str) -> Path:
    """Return one writable, non-link checkpoint root without code-tree escape."""

    raw = str(value or "").strip()
    requested = (
        raw
        or os.environ.get("QMT_ANNOUNCEMENT_CHECKPOINT_DIR", "").strip()
        or QMT_ANNOUNCEMENT_CHECKPOINT_DIR
    )
    if os.name == "nt" and requested == QMT_ANNOUNCEMENT_CHECKPOINT_DIR:
        program_data = os.environ.get("PROGRAMDATA", r"C:\ProgramData").strip()
        candidate = (
            Path(program_data)
            / "ProBigA"
            / "scheduler"
            / "qmt-announcement-checkpoints"
        )
    else:
        candidate = Path(requested)
    if not candidate.is_absolute():
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "not-absolute"
        )
    absolute = Path(os.path.abspath(str(candidate)))
    if _is_production() and requested != QMT_ANNOUNCEMENT_CHECKPOINT_DIR:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
            "production-root-differs",
        )

    try:
        if absolute.exists() or absolute.is_symlink():
            if absolute.is_symlink() or not absolute.is_dir():
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                    "root-not-directory-or-is-symlink",
                )
        elif _is_production() and os.name != "nt":
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                "production-root-missing",
            )
        else:
            absolute.mkdir(parents=True, mode=0o700)
            if os.name == "posix":
                absolute.chmod(0o700)

        resolved = absolute.resolve(strict=True)
    except QMTAnnouncementBlocked:
        raise
    except OSError as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", type(exc).__name__
        ) from exc
    if resolved != absolute:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "resolved-root-differs"
        )

    root_stat = absolute.stat(follow_symlinks=False)
    if not stat.S_ISDIR(root_stat.st_mode):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "root-is-not-directory"
        )
    if _is_production() and os.name == "posix":
        if root_stat.st_uid != os.geteuid():
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                "service-user-does-not-own-root",
            )
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "root-mode-not-0700"
            )

    # State is service-owned and therefore untrusted input on the next run.
    # Reject every link before any checkpoint manifest/result is read.  With a
    # resolved exact root and no descendant links, all reads/writes stay under
    # the persistent state directory while the release tree remains sealed.
    def walk_error(exc: OSError) -> None:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", type(exc).__name__
        ) from exc

    for current, directories, files in os.walk(
        absolute, followlinks=False, onerror=walk_error
    ):
        current_path = Path(current)
        try:
            current_path.resolve(strict=True).relative_to(absolute)
        except (OSError, ValueError) as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                "checkpoint-tree-resolve-escape",
            ) from exc
        for name in [*directories, *files]:
            entry = current_path / name
            if entry.is_symlink():
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                    "checkpoint-tree-contains-symlink",
                )
    if not os.access(absolute, os.R_OK | os.W_OK | os.X_OK):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "root-not-rwx"
        )
    return absolute


def _shanghai_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=None)
    return value.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)


def _iso_date(value: object) -> date:
    raw = str(value or "")[:10]
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_BATCH_ENVELOPE_INVALID", "date"
        ) from exc
    if parsed.isoformat() != raw:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_BATCH_ENVELOPE_INVALID", "date"
        )
    return parsed


def _exact_datetime(value: object, *, field: str) -> datetime:
    raw = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_BATCH_ENVELOPE_INVALID", field
        ) from exc
    return _shanghai_naive(parsed)


def _nonnegative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_BATCH_ENVELOPE_INVALID", field
        )
    try:
        normalized = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_BATCH_ENVELOPE_INVALID", field
        ) from exc
    if normalized < 0 or normalized != value:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_BATCH_ENVELOPE_INVALID", field
        )
    return normalized


def validate_existing_complete_qmt_announcement_batch(
    engine,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    now: datetime | None = None,
    expected_trade_date: str = "",
) -> dict:
    """Read-only proof of the existing official batch for the closed session.

    QMT capture is owned by the Windows edge.  A Linux deployment may only
    select the unique newest batch for the authoritative closed trading day
    and revalidate its immutable calendar, catalog, full-market coverage and
    global content root.  This function executes SELECT statements only.
    """

    if not 20 <= int(window_days) <= 3660:
        raise ValueError("QMT announcement window_days must be 20..3660")
    observed_at = now or datetime.now(PRODUCTION_TIMEZONE)
    decision_at = _shanghai_naive(observed_at).replace(microsecond=0)
    target_text = str(
        authoritative_closed_trade_date(engine, now=observed_at) or ""
    )
    target = _iso_date(target_text)
    if expected_trade_date:
        expected = _iso_date(expected_trade_date)
        if target != expected:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_EXPECTED_TRADE_DATE_DIFFERS",
                f"expected={expected.isoformat()},authoritative={target.isoformat()}",
            )
    if target > decision_at.date():
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_TRADE_DATE_UNAVAILABLE", "future"
        )
    required_start = target - timedelta(days=int(window_days))

    with engine.connect() as connection:
        try:
            calendar = load_trade_calendar_receipt(
                connection,
                start_date=required_start.isoformat(),
                end_date=target.isoformat(),
                decision_known_at=decision_at,
            )
            sessions = calendar.sessions_between(
                required_start.isoformat(), target.isoformat()
            )
        except Exception as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_AUTHORITATIVE_CALENDAR_UNAVAILABLE",
                type(exc).__name__,
            ) from exc
        if not sessions or sessions[-1] != target.isoformat():
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_AUTHORITATIVE_CALENDAR_DIFFERS"
            )

        batches = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT batch_id, MIN(stock_code) AS sample_stock_code, "
                    "MIN(known_at) AS min_known_at, "
                    "MAX(known_at) AS max_known_at, "
                    "MIN(received_at) AS min_received_at, "
                    "MAX(received_at) AS max_received_at, "
                    "MIN(covered_through_at) AS min_fact_cutoff_at, "
                    "MAX(covered_through_at) AS max_fact_cutoff_at, "
                    "MIN(window_start) AS min_window_start, "
                    "MAX(window_start) AS max_window_start, "
                    "MIN(window_end) AS min_window_end, "
                    "MAX(window_end) AS max_window_end, "
                    "COUNT(*) AS coverage_row_count, "
                    "COUNT(DISTINCT stock_code) AS distinct_stock_count, "
                    "SUM(CASE WHEN coverage_status='COMPLETE' THEN 0 "
                    "ELSE 1 END) AS invalid_coverage_count, "
                    "SUM(CASE WHEN result_count<0 THEN 1 ELSE 0 END) "
                    "AS invalid_result_count, "
                    "SUM(result_count) AS event_count, "
                    "SUM(CASE WHEN result_count=0 THEN 1 ELSE 0 END) "
                    "AS empty_stock_count "
                    "FROM st_pit_source_coverage "
                    "WHERE fact_kind='event' AND source=:source "
                    "AND known_at<=:decision_at "
                    "AND DATE(known_at)>=:target_trade_date "
                    "AND DATE(known_at)<=:decision_date "
                    "GROUP BY batch_id "
                    "HAVING SUM(CASE WHEN coverage_status='COMPLETE' THEN 0 "
                    "ELSE 1 END)=0 "
                    "ORDER BY max_known_at DESC, batch_id DESC LIMIT 2"
                ),
                {
                    "source": QMT_ANNOUNCEMENT_SOURCE,
                    "decision_at": decision_at,
                    "target_trade_date": target.isoformat(),
                    "decision_date": decision_at.date().isoformat(),
                },
            ).mappings()
        ]

        if not batches:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND",
                target.isoformat(),
            )
        latest = batches[0]
        if len(batches) > 1 and _exact_datetime(
            batches[1].get("max_known_at"), field="max_known_at"
        ) == _exact_datetime(latest.get("max_known_at"), field="max_known_at"):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_LATEST_BATCH_AMBIGUOUS",
                target.isoformat(),
            )

        batch_id = str(latest.get("batch_id") or "")
        known_at = _exact_datetime(
            latest.get("max_known_at"), field="known_at"
        )
        received_at = _exact_datetime(
            latest.get("max_received_at"), field="received_at"
        )
        fact_cutoff_at = _exact_datetime(
            latest.get("max_fact_cutoff_at"), field="fact_cutoff_at"
        )
        window_start = _iso_date(latest.get("max_window_start"))
        window_end = _iso_date(latest.get("max_window_end"))
        coverage_count = _nonnegative_integer(
            latest.get("coverage_row_count"), field="coverage_row_count"
        )
        distinct_count = _nonnegative_integer(
            latest.get("distinct_stock_count"), field="distinct_stock_count"
        )
        invalid_coverage = _nonnegative_integer(
            latest.get("invalid_coverage_count"),
            field="invalid_coverage_count",
        )
        invalid_results = _nonnegative_integer(
            latest.get("invalid_result_count"), field="invalid_result_count"
        )
        event_count = _nonnegative_integer(
            latest.get("event_count"), field="event_count"
        )
        empty_stock_count = _nonnegative_integer(
            latest.get("empty_stock_count"), field="empty_stock_count"
        )
        uniform_envelope = (
            bool(batch_id)
            and bool(str(latest.get("sample_stock_code") or ""))
            and _exact_datetime(
                latest.get("min_known_at"), field="known_at"
            )
            == known_at
            and _exact_datetime(
                latest.get("min_received_at"), field="received_at"
            )
            == received_at
            and _exact_datetime(
                latest.get("min_fact_cutoff_at"), field="fact_cutoff_at"
            )
            == fact_cutoff_at
            and _iso_date(latest.get("min_window_start")) == window_start
            and _iso_date(latest.get("min_window_end")) == window_end
            and known_at == received_at
            and datetime.combine(target, datetime.min.time()) <= fact_cutoff_at
            and fact_cutoff_at <= known_at <= decision_at
            and target <= window_end <= decision_at.date()
            and fact_cutoff_at.date() == window_end
            and window_start == required_start
            and window_end >= target
            and coverage_count == distinct_count
            and invalid_coverage == 0
            and invalid_results == 0
            and event_count > 0
            and empty_stock_count < coverage_count
        )
        if not uniform_envelope:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_BATCH_ENVELOPE_INVALID", batch_id
            )

        try:
            catalog = load_stock_catalog(
                connection, decision_known_at=fact_cutoff_at
            )
            catalog_codes = catalog.eligible_codes(target.isoformat())
        except Exception as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_AUTHORITATIVE_CATALOG_UNAVAILABLE",
                type(exc).__name__,
            ) from exc
        if not catalog_codes or len(catalog_codes) != coverage_count:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CATALOG_COVERAGE_DIFFERS", batch_id
            )

    proof = validate_complete_qmt_announcement_batch(
        engine,
        codes=catalog_codes,
        decision_at=known_at,
        fact_cutoff_at=fact_cutoff_at,
        window_start=window_start,
        window_end=window_end,
    )
    capture_seconds = int((received_at - fact_cutoff_at).total_seconds())
    if (
        proof.get("status") != "COMPLETE"
        or str(proof.get("batch_id") or "") != batch_id
        or str(proof.get("catalog_batch_id") or "") != catalog.batch_id
        or str(proof.get("catalog_manifest_hash") or "")
        != catalog.manifest_hash
        or str(proof.get("catalog_member_set_hash") or "")
        != catalog.member_set_hash
        or int(proof.get("catalog_member_count") or 0) != len(catalog_codes)
        or _iso_date(proof.get("window_start")) != window_start
        or _iso_date(proof.get("window_end")) != window_end
        or _exact_datetime(proof.get("fact_cutoff_at"), field="fact_cutoff_at")
        != fact_cutoff_at
        or _exact_datetime(proof.get("received_at"), field="received_at")
        != received_at
        or capture_seconds < 0
        or capture_seconds > int(MAX_CAPTURE_DELAY.total_seconds())
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_COMPLETE_BATCH_PROOF_DIFFERS", batch_id
        )
    return {
        "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
        "status": "COMPLETE",
        "reason_code": "QMT_ANNOUNCEMENT_EXISTING_FULL_MARKET_COMPLETE",
        "detail": "",
        "mode": "validate-existing-complete-batch",
        "trade_date": target.isoformat(),
        "source": QMT_ANNOUNCEMENT_SOURCE,
        "funding_eligible": True,
        "calendar_batch_id": calendar.batch_id,
        "calendar_manifest_hash": calendar.manifest_hash,
        "batch_id": batch_id,
        "batch_root_hash": str(proof.get("batch_root_hash") or ""),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "stock_count": len(catalog_codes),
        "coverage_count": coverage_count,
        "event_count": event_count,
        "empty_stock_count": empty_stock_count,
        "fact_cutoff_at": str(proof.get("fact_cutoff_at") or ""),
        "decision_at": str(proof.get("decision_at") or ""),
        "received_at": str(proof.get("received_at") or ""),
        "capture_seconds": capture_seconds,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "database_writes": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def validate_existing_task_result(
    payload: object,
    process_exit: int,
    *,
    expected_trade_date: str,
) -> str:
    """Strict deploy-only envelope for a read-only existing-batch proof."""

    expected = _iso_date(expected_trade_date).isoformat()
    disposition = validate_task_result(payload, process_exit)
    if (
        not isinstance(payload, dict)
        or payload.get("mode") != "validate-existing-complete-batch"
        or payload.get("database_writes") is not False
    ):
        raise ValueError("QMT announcement read-only result mode differs")
    if payload.get("status") == "COMPLETE":
        complete_fields = {
            "schema", "status", "reason_code", "detail", "mode",
            "trade_date", "source", "funding_eligible",
            "calendar_batch_id", "calendar_manifest_hash",
            "batch_id", "batch_root_hash", "catalog_batch_id",
            "catalog_manifest_hash", "catalog_member_set_hash",
            "stock_count", "coverage_count", "event_count",
            "empty_stock_count", "fact_cutoff_at", "decision_at",
            "received_at", "capture_seconds", "window_start", "window_end",
            "database_writes", "automatic_real_order_submission",
            "real_order_authority",
        }
        expected_date = date.fromisoformat(expected)
        window_end = _iso_date(payload.get("window_end"))
        required_start = expected_date - timedelta(days=DEFAULT_WINDOW_DAYS)
        fact_cutoff_at = _exact_datetime(
            payload.get("fact_cutoff_at"), field="fact_cutoff_at"
        )
        received_at = _exact_datetime(
            payload.get("received_at"), field="received_at"
        )
        counter_fields = (
            "stock_count", "coverage_count", "capture_seconds",
            "event_count", "empty_stock_count",
        )
        identifier_fields = (
            "calendar_batch_id", "batch_id", "catalog_batch_id",
        )
        hash_fields = (
            "calendar_manifest_hash", "batch_root_hash",
            "catalog_manifest_hash", "catalog_member_set_hash",
        )
        if (
            set(payload) != complete_fields
            or any(
                type(payload.get(field)) is not int
                for field in counter_fields
            )
            or any(
                type(payload.get(field)) is not str
                for field in (*identifier_fields, *hash_fields)
            )
            or payload.get("reason_code")
            != "QMT_ANNOUNCEMENT_EXISTING_FULL_MARKET_COMPLETE"
            or payload.get("detail") != ""
            or payload.get("trade_date") != expected
            or payload.get("source") != QMT_ANNOUNCEMENT_SOURCE
            or payload.get("funding_eligible") is not True
            or _iso_date(payload.get("window_start")) != required_start
            or window_end < expected_date
            or fact_cutoff_at.date() != window_end
            or not window_end <= received_at.date()
            or payload["event_count"] <= 0
            or not 0 <= payload["empty_stock_count"] < payload["stock_count"]
            or payload.get("stock_count") != payload.get("coverage_count")
            or payload["stock_count"] <= 0
            or not payload["calendar_batch_id"]
            or re.fullmatch(
                r"[0-9a-f]{64}",
                payload["calendar_manifest_hash"],
            )
            is None
            or not payload["batch_id"].startswith("qmt-ann-")
            or re.fullmatch(r"[0-9a-f]{64}", payload["batch_root_hash"])
            is None
            or not payload["catalog_batch_id"]
            or re.fullmatch(
                r"[0-9a-f]{64}",
                payload["catalog_manifest_hash"],
            )
            is None
            or re.fullmatch(
                r"[0-9a-f]{64}",
                payload["catalog_member_set_hash"],
            )
            is None
        ):
            raise ValueError(
                "QMT announcement read-only COMPLETE result differs"
            )
    return disposition


def _blocked(reason_code: str, detail: str = "") -> dict:
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    return {
        "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
        "status": "DATA_BLOCKED",
        "reason_code": str(reason_code or "QMT_ANNOUNCEMENT_DATA_BLOCKED"),
        "detail": str(detail or "")[:1000],
        "batch_id": "",
        "batch_root_hash": "",
        "catalog_batch_id": "",
        "catalog_manifest_hash": "",
        "catalog_member_set_hash": "",
        "stock_count": 0,
        "coverage_count": 0,
        "event_count": 0,
        "empty_stock_count": 0,
        "fact_cutoff_at": now,
        "decision_at": now,
        "received_at": now,
        "capture_seconds": 0,
        "window_start": "",
        "window_end": "",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument(
        "--data-adapter",
        choices=("auto", "bigqmt", "xtdata"),
        default="auto",
        help="announcement source transport; auto uses BigQMT on Windows",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="",
    )
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument(
        "--validate-existing-complete-batch",
        action="store_true",
        help=(
            "read-only validation of the existing Windows-QMT full-market "
            "batch for the authoritative closed trading day"
        ),
    )
    parser.add_argument("--expected-trade-date", default="")
    parser.add_argument(
        "--validate-result-exit", type=int, default=-1,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--validate-existing-result-exit", type=int, default=-1,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)
    if args.validate_result_exit >= 0 and (
        args.validate_existing_result_exit >= 0
    ):
        parser.error("result validators are mutually exclusive")
    if args.validate_existing_result_exit >= 0:
        if not args.expected_trade_date:
            parser.error("--expected-trade-date is required")
        try:
            payload = json.load(sys.stdin)
            print(validate_existing_task_result(
                payload,
                args.validate_existing_result_exit,
                expected_trade_date=args.expected_trade_date,
            ))
            return 0
        except Exception as exc:
            print(f"invalid:{type(exc).__name__}", file=sys.stderr)
            return 2
    if args.validate_result_exit >= 0:
        try:
            payload = json.load(sys.stdin)
            print(validate_task_result(payload, args.validate_result_exit))
            return 0
        except Exception as exc:
            print(f"invalid:{type(exc).__name__}", file=sys.stderr)
            return 2

    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    engine = None
    try:
        engine = create_tool_engine()
        if args.validate_existing_complete_batch:
            if (
                not args.expected_trade_date
                or args.checkpoint_dir
                or args.no_resume
                or args.window_days != DEFAULT_WINDOW_DAYS
                or args.batch_size != DEFAULT_BATCH_SIZE
                or args.data_adapter != "auto"
            ):
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_READ_ONLY_ARGUMENTS_INVALID"
                )
            payload = validate_existing_complete_qmt_announcement_batch(
                engine,
                window_days=args.window_days,
                expected_trade_date=args.expected_trade_date,
            )
        else:
            if args.expected_trade_date:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CAPTURE_ARGUMENTS_INVALID"
                )
            checkpoint_dir = _checkpoint_root(args.checkpoint_dir)

            # Some QMT builds print connection diagnostics.  Keep stdout a
            # single machine JSON record so scheduler validation cannot accept
            # ambiguity.
            with redirect_stdout(sys.stderr):
                xtdata = _announcement_data_adapter(args.data_adapter)
                capture_options = _announcement_capture_options(
                    xtdata, engine=engine, no_resume=args.no_resume
                )
                payload = synchronize_qmt_announcements(
                    engine,
                    xtdata=xtdata,
                    checkpoint_root=checkpoint_dir,
                    window_days=args.window_days,
                    batch_size=args.batch_size,
                    **capture_options,
                )
    except Exception as exc:
        reason = str(getattr(exc, "reason_code", "") or "")
        if not reason:
            message = str(exc).lower()
            reason = (
                "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE"
                if "xtquant" in message or "qmt" in message and "import" in message
                else "QMT_ANNOUNCEMENT_RUNTIME_DATA_BLOCKED"
            )
        payload = _blocked(reason, type(exc).__name__)
        if args.validate_existing_complete_batch:
            payload.update({
                "mode": "validate-existing-complete-batch",
                "database_writes": False,
            })
    finally:
        if engine is not None:
            engine.dispose()
    process_exit = 0 if payload.get("status") == "COMPLETE" else 2
    try:
        validate_task_result(payload, process_exit)
    except Exception as exc:
        payload = _blocked(
            "QMT_ANNOUNCEMENT_INVALID_RESULT_CONTRACT", type(exc).__name__
        )
        process_exit = 2
        validate_task_result(payload, process_exit)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return process_exit


if __name__ == "__main__":
    raise SystemExit(main())

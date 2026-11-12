#!/usr/bin/env python3
"""Synchronize one full-market official QMT announcement PIT batch."""
from __future__ import annotations

import argparse
from collections.abc import Mapping
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
import re
from socket import gethostname
import stat
import subprocess
import sys
import time
import uuid

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.qmt_announcement_pit import (
    ANNOUNCEMENT_FALLBACK_REASON_CODES,
    AUTHORITATIVE_ANNOUNCEMENT_SOURCES,
    CNINFO_ANNOUNCEMENT_SOURCE,
    DEFAULT_BATCH_SIZE,
    DEFAULT_WINDOW_DAYS,
    EASTMONEY_ANNOUNCEMENT_SOURCE,
    HISTORICAL_CAPTURE_DEADLINE,
    HISTORICAL_PUBLISH_DEADLINE,
    PUBLISH_COMMIT_UNKNOWN,
    QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN,
    QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V3_SCHEMA,
    QMT_ANNOUNCEMENT_RECONSTRUCTION_STOCK_SCOPE,
    QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN,
    QMT_ANNOUNCEMENT_SOURCE,
    QMT_ANNOUNCEMENT_TASK_SCHEMA,
    QMTAnnouncementBlocked,
    AnnouncementCatalog,
    HistoricalReconstructionContext,
    _explicit_qmt_unavailability_reason,
    _reconstruction_authority_matches,
    synchronize_qmt_announcements,
    synchronize_historical_cninfo_announcements,
    canonical_hash,
    validate_complete_qmt_announcement_batch,
    validate_complete_historical_reconstruction_batch,
    validate_task_result,
)
from server.common.authoritative_market_clock import (
    DAILY_CLOSE_READY_TIME,
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
    resolve_strategy_pool_decision_slot,
)
from server.common.component_release import runtime_contract_build_sha
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_datadir_daily_coverage import (
    PROVIDER as QMT_DATADIR_PROVIDER,
    load_daily_manifest,
    retained_listing_evidence,
)
from server.common.official_listing_evidence import load_official_listing_evidence, validate_official_listing_evidence
from server.common.qmt_datadir_result import load_receipt, partition_from_receipt
from server.common.qmt_datadir_progress import MinuteImportProgress
from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
from server.common.qmt_attestation_contract import (
    validated_no_row_exception_contract,
)
from server.common.qmt_trade_calendar import load_trade_calendar_receipt
from tools.qmt_announcement_task_contract import (
    PREMARKET_ANNOUNCEMENT_DECISION_TIME,
    PREMARKET_ANNOUNCEMENT_SOURCE_TIME,
    PREMARKET_ANNOUNCEMENT_TASK_TYPE,
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
_FULL_MARKET_BATCH_PREFIX = {
    QMT_ANNOUNCEMENT_SOURCE: "qmt-ann-",
    CNINFO_ANNOUNCEMENT_SOURCE: "cninfo-ann-",
    EASTMONEY_ANNOUNCEMENT_SOURCE: "em-ann-",
}


class BigQmtAnnouncementAdapter:
    """Expose the built-in BigQMT spool as the existing xtdata read contract."""

    force_fresh_capture = True
    # The core checkpoint is already keyed by the frozen cutoff, catalog and
    # per-code result hash.  BigQMT can safely reuse completed shards from the
    # same capture just like the provider-backed fallback adapter.
    resumable_capture = True

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

    def _remaining_timeout(self, cap: int | float) -> float:
        return max(1.0, float(cap))

    def connect_announcement_transport(self) -> None:
        timeout = self._remaining_timeout(min(180, self._timeout))
        try:
            capabilities = self._bridge.capabilities(timeout=timeout)
        except TimeoutError as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_PROVIDER_TIMEOUT", "capabilities"
            ) from exc
        actions = capabilities.get("actions") if isinstance(capabilities, dict) else None
        if (
            not isinstance(capabilities, dict)
            or capabilities.get("status") != "ok"
            or capabilities.get("source") != "gj_big_qmt_inner"
            or capabilities.get("bridge_version") != "bigqmt_inner_v2"
            or capabilities.get("strategy_identity_frozen") is not True
            or capabilities.get("strategy_identity_status") != "BOUND"
            or not isinstance(actions, list)
        ):
            raise RuntimeError("BigQMT announcement capability identity differs")
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
        if "announcement" not in actions:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_API_UNAVAILABLE", "announcement"
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
        timeout = self._remaining_timeout(self._timeout)
        try:
            capture = self._bridge.announcement_capture(
                codes,
                start_date=start,
                end_date=end,
                download_history=True,
                timeout=timeout,
            )
        except TimeoutError as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_PROVIDER_TIMEOUT", "announcement_capture"
            ) from exc
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


def _fallback_announcement_adapter(
    provider_name: str,
    *,
    provider_factory=None,
):
    """Build one audited fallback transport without touching a browser UI."""

    selected = str(provider_name or "").strip().lower()
    from server.common.announcement_provider import (
        CninfoMarketAnnouncementProvider,
        EastmoneyAnnouncementProvider,
        ProviderBackedAnnouncementAdapter,
    )

    if provider_factory is not None:
        provider = provider_factory(selected)
    elif selected == "cninfo":
        provider = CninfoMarketAnnouncementProvider()
    elif selected == "eastmoney":
        provider = EastmoneyAnnouncementProvider()
    else:
        raise ValueError("announcement fallback provider is invalid")
    expected_source = {
        "cninfo": CNINFO_ANNOUNCEMENT_SOURCE,
        "eastmoney": EASTMONEY_ANNOUNCEMENT_SOURCE,
    }[selected]
    if str(getattr(provider, "source", "") or "") != expected_source:
        raise ValueError("announcement fallback source identity differs")
    return ProviderBackedAnnouncementAdapter(
        provider, workers=3
    )


def _announcement_capture_options(
    adapter, *, engine, no_resume: bool, coverage_target_date: str | None = None,
) -> dict:
    force_fresh_capture = bool(
        getattr(adapter, "force_fresh_capture", False)
    )
    resumable_capture = bool(
        getattr(adapter, "resumable_capture", False)
    )
    if force_fresh_capture and coverage_target_date is None:
        coverage_target_date = authoritative_closed_trade_date(engine)
        if not coverage_target_date:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_TRADE_DATE_UNAVAILABLE"
            )
    return {
        "resume": (
            not bool(no_resume)
            and (not force_fresh_capture or resumable_capture)
        ),
        "coverage_target_date": coverage_target_date,
    }


def _is_production() -> bool:
    return os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == (
        "production"
    )


def _git_revision(ref: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", str(ref)],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=30,
    )
    return completed.stdout.strip().lower()


def _git_branch() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--abbrev-ref", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=30,
    )
    return completed.stdout.strip()


def _git_tracked_status() -> str:
    completed = subprocess.run(
        [
            "git", "-C", str(ROOT), "status", "--porcelain",
            "--untracked-files=no",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=30,
    )
    return completed.stdout.strip()


def _manual_research_build_sha(expected_build_sha: str) -> str:
    expected = str(expected_build_sha or "").strip().lower()
    environment_build = str(
        os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or ""
    ).strip().lower()
    configured_root = str(os.environ.get("PROBIGA_CODE_ROOT") or "").strip()
    normalized_root = os.path.normcase(os.path.realpath(str(ROOT)))
    normalized_configured = os.path.normcase(os.path.realpath(configured_root))
    try:
        head = _git_revision("HEAD")
        main = _git_revision("origin/main")
        branch = _git_branch()
        tracked_status = _git_tracked_status()
    except Exception as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RESEARCH_RELEASE_IDENTITY_INVALID",
            type(exc).__name__,
        ) from exc
    if (
        re.fullmatch(r"[0-9a-f]{40}", expected) is None
        or expected == "0" * 40
        or not _is_production()
        or environment_build != expected
        or not configured_root
        or normalized_configured != normalized_root
        or head != expected
        or main != expected
        or branch != "main"
        or tracked_status
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RESEARCH_RELEASE_IDENTITY_INVALID"
        )
    return expected


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


def _build_historical_catalog_reconciliation(
    *,
    target_trade_date: date,
    authority_catalog,
    prior_catalog,
    attested_daily_count: int,
    no_trade_codes: list[str],
    added_evidence: Mapping | None = None,
) -> tuple[list[str], dict]:
    """Explain every catalog difference against the target-day authority."""

    target = target_trade_date.isoformat()
    authority_codes = authority_catalog.eligible_codes(target)
    prior_codes = prior_catalog.eligible_codes(target)
    normalized_no_trade = sorted(set(no_trade_codes))
    if (
        not authority_codes
        or attested_daily_count + len(normalized_no_trade)
        != len(authority_codes)
        or set(normalized_no_trade) - set(authority_codes)
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_DAILY_AUTHORITY_DIFFERS"
        )
    authority_members = {
        str(item["stock_code"]).zfill(6): dict(item)
        for item in authority_catalog.members
    }
    exclusions: list[dict] = []
    for code in sorted(set(prior_codes) - set(authority_codes)):
        member = authority_members.get(code)
        if member is None:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_RECONSTRUCTION_CATALOG_DIFF_UNEXPLAINED",
                code,
            )
        list_date = str(member.get("list_date") or "")[:10]
        expire_date = str(member.get("expire_date") or "")[:10]
        reason = ""
        if list_date > target:
            reason = "LISTED_AFTER_TARGET"
        elif expire_date and expire_date < target:
            reason = "EXPIRED_BEFORE_TARGET"
        if not reason:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_RECONSTRUCTION_CATALOG_DIFF_UNEXPLAINED",
                code,
            )
        exclusions.append({
            "stock_code": code,
            "reason": reason,
            "list_date": list_date,
            "expire_date": expire_date,
            "authority_instrument_batch_id": str(
                member.get("instrument_batch_id") or ""
            ),
        })
    additions = []
    evidence_by_code = added_evidence or {}
    for code in sorted(set(authority_codes) - set(prior_codes)):
        member, proof = authority_members.get(code), evidence_by_code.get(code)
        if (member is None or not isinstance(proof, Mapping) or code in normalized_no_trade
                or str(member.get("list_date") or "")[:10] != target):
            raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_CATALOG_DIFF_UNEXPLAINED", code)
        additions.append({"stock_code": code, "qmt_code": str(member["qmt_code"]).upper(),
                          "reason": "OFFICIAL_TARGET_DAY_LISTING_WITH_NATIVE_TRADE",
                          "list_date": target, "listing_disclosure": proof["listing_disclosure"],
                          "native_daily_entity": proof["native_daily_entity"]})
    return authority_codes, {
        "schema": "probiga.qmt-announcement-catalog-reconciliation.v2",
        "target_trade_date": target,
        "prior_catalog_batch_id": prior_catalog.batch_id,
        "prior_catalog_manifest_hash": prior_catalog.manifest_hash,
        "prior_eligible_count": len(prior_codes),
        "authority_catalog_batch_id": authority_catalog.batch_id,
        "authority_catalog_manifest_hash": authority_catalog.manifest_hash,
        "authority_eligible_count": len(authority_codes),
        "attested_daily_count": attested_daily_count,
        "native_no_trade_count": len(normalized_no_trade),
        "native_no_trade_codes": normalized_no_trade,
        "native_no_trade_codes_sha256": canonical_hash(normalized_no_trade),
        "excluded_from_prior": exclusions,
        "added_to_prior": additions,
    }


def _load_datadir_reconstruction_scope(
    connection, *, target_date: str, decision: datetime, truth, added_codes: set[str],
) -> tuple[list[str], dict]:
    # The daily truth loader already verified the business rows. Reopen its
    # exact immutable manifest for the evidence behind each missing bar.
    manifest = load_daily_manifest(
        connection, target_date, decision_known_at=decision,
        manifest_hash=truth.truth_hash, verify_rows=False,
    )
    receipt = load_receipt(connection, dataset="stock_daily", trade_date=target_date)
    if manifest is None or receipt is None:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_ATTESTATION_MISSING"
        )
    partition = partition_from_receipt(receipt)
    captured_at = _exact_datetime(manifest["captured_at"], field="captured_at")
    started_at = _exact_datetime(receipt["started_at"], field="started_at")
    finished_at = _exact_datetime(receipt["finished_at"], field="finished_at")
    if (
        manifest["run_id"] != truth.run_id
        or manifest["manifest_hash"] != truth.truth_hash
        or partition["status"] != "COMPLETE"
        or partition["trade_date"] != target_date
        or partition["run_id"] != truth.run_id
        or partition["full_market_manifest_hash"] != truth.truth_hash
        or partition["coverage"] != 1.0
        or any(partition[key] != truth.attested_row_count for key in (
            "row_count", "code_count", "readback_rows", "readback_codes",
        ))
        or not started_at <= finished_at <= decision
        or not captured_at <= finished_at
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_DATADIR_PUBLICATION_DIFFERS"
        )
    entities = manifest["entities"]
    if manifest.get("historical_reconstruction") is True:
        # Official lifecycle evidence can prove a missing bar but does not
        # supply the native suspension observations required by this seal.
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_NATIVE_SUSPENSION_SCOPE_UNSUPPORTED"
        )
    if any(entity["classification"] == "NOT_LISTED" for entity in entities):
        # The v3 reconciliation seal accounts for native no-trade stocks
        # separately. A listing exclusion cannot be represented as suspension.
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_LISTING_SCOPE_UNSUPPORTED"
        )
    no_trade = [
        entity["stock_code"] for entity in entities
        if entity["classification"] == "NO_TRADE"
    ]
    additions = {entity["stock_code"]: entity for entity in entities
                 if entity["stock_code"] in added_codes and entity["classification"] == "TRADED"}
    return no_trade, additions


def _load_historical_reconstruction_authority(
    engine,
    *,
    target_trade_date: date,
    decision_known_at: datetime,
    execution_origin: str = QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN,
    existing_batch_only: bool = False,
) -> tuple[AnnouncementCatalog, dict]:
    """Bind reconstruction to validated daily truth and stock-catalog scope."""

    target = target_trade_date
    decision = _shanghai_naive(decision_known_at).replace(microsecond=0)
    if decision <= datetime.combine(target, datetime.max.time()):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_TARGET_NOT_HISTORICAL"
        )
    try:
        with engine.connect() as connection:
            truth = load_qmt_daily_market_truth(
                connection,
                start_date=target.isoformat(),
                end_date=target.isoformat(),
                decision_known_at=decision,
            )
            authority_catalog = load_stock_catalog(
                connection,
                batch_id=truth.catalog_batch_id,
                decision_known_at=decision,
            )
            prior_catalog = load_stock_catalog(
                connection,
                decision_known_at=datetime.combine(
                    target, datetime.max.time()
                ),
            )
            authority_codes = authority_catalog.eligible_codes(target.isoformat())
            added_codes = set(authority_codes) - set(prior_catalog.eligible_codes(target.isoformat()))
            traded_additions = {}
            if truth.provider == QMT_DATADIR_PROVIDER:
                no_trade_codes, traded_additions = _load_datadir_reconstruction_scope(
                    connection, target_date=target.isoformat(), decision=decision,
                    truth=truth, added_codes=added_codes,
                )
            else:
                tolerance = connection.execute(
                    text(
                        "SELECT start_date,end_date,tolerance_json "
                        "FROM qmt_kline_attestation_run "
                        "WHERE run_id=:run_id AND status='COMPLETED'"
                    ),
                    {"run_id": truth.run_id},
                ).mappings().one_or_none()
                if tolerance is None:
                    raise QMTAnnouncementBlocked(
                        "QMT_ANNOUNCEMENT_RECONSTRUCTION_ATTESTATION_MISSING"
                    )
                no_row = validated_no_row_exception_contract(
                    tolerance["tolerance_json"],
                    start_date=str(tolerance["start_date"])[:10],
                    end_date=str(tolerance["end_date"])[:10],
                )
                no_trade_codes = [
                    str(item.get("stock_code") or "").zfill(6)
                    for item in ((no_row or {}).get("entities") or [])
                    if isinstance(item, Mapping)
                    and target.isoformat() in list(item.get("affected_trade_dates") or [])
                ]
    except QMTAnnouncementBlocked:
        raise
    except Exception as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_UNAVAILABLE",
            type(exc).__name__,
        ) from exc
    if (
        truth.requested_sessions != (target.isoformat(),)
        or truth.catalog_manifest_hash != authority_catalog.manifest_hash
        or truth.catalog_member_set_hash != authority_catalog.member_set_hash
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_RECONSTRUCTION_DAILY_AUTHORITY_DIFFERS"
        )
    authority_code_set = set(authority_codes)
    if (truth.attested_row_count + len(set(no_trade_codes)) != len(authority_codes)
            or set(no_trade_codes) - authority_code_set):
        raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_DAILY_AUTHORITY_DIFFERS")
    qmt_by_code = {
        str(item["stock_code"]).zfill(6): str(item["qmt_code"]).upper()
        for item in authority_catalog.members if str(item["stock_code"]).zfill(6) in authority_code_set
    }
    if set(qmt_by_code) != set(authority_codes) or not authority_codes:
        raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_CATALOG_MAPPING_INCOMPLETE")
    catalog = AnnouncementCatalog(batch_id=authority_catalog.batch_id, manifest_hash=authority_catalog.manifest_hash,
                                  member_set_hash=authority_catalog.member_set_hash, codes=tuple(authority_codes),
                                  qmt_by_code=qmt_by_code)
    if existing_batch_only:
        # The retained batch revalidates its own complete authority and must
        # cover this exact current stock scope. No issuer call occurs here.
        return catalog, {}
    added_evidence = {}
    if added_codes:
        if len(added_codes) > 8:
            raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_CATALOG_DIFF_UNEXPLAINED", "addition_scope_exceeds_bound")
        if set(traded_additions) != added_codes:
            raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_CATALOG_DIFF_UNEXPLAINED",
                                         sorted(added_codes - set(traded_additions))[0])
        prelisting = (target - timedelta(days=1)).isoformat()
        official = retained_listing_evidence(engine, sorted(added_codes), target_date=prelisting, decision_known_at=decision)
        missing = sorted(added_codes - set(official))
        if missing:
            official.update(load_official_listing_evidence(missing, target_date=prelisting))
        for code in sorted(added_codes):
            try:
                document = validate_official_listing_evidence(official[code], stock_code=code, target_date=prelisting)
                captured = _exact_datetime(document["captured_at"], field="listing_captured_at")
                if (document["listing_date"] != target.isoformat()
                        or captured > _shanghai_naive(datetime.now(PRODUCTION_TIMEZONE))):
                    raise ValueError("target-day listing identity differs")
            except (KeyError, TypeError, ValueError) as exc:
                raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_CATALOG_DIFF_UNEXPLAINED", code) from exc
            added_evidence[code] = {"listing_disclosure": document, "native_daily_entity": traded_additions[code]}
    authority_codes, reconciled = _build_historical_catalog_reconciliation(
        target_trade_date=target,
        authority_catalog=authority_catalog,
        prior_catalog=prior_catalog,
        attested_daily_count=truth.attested_row_count,
        no_trade_codes=no_trade_codes,
        added_evidence=added_evidence,
    )
    reconciliation_sha = canonical_hash(reconciled)
    truth_payload = truth.as_dict()
    authority = {
        "schema": QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V3_SCHEMA,
        "execution_origin": str(execution_origin or "").strip().upper(),
        "stock_scope_authority": (
            QMT_ANNOUNCEMENT_RECONSTRUCTION_STOCK_SCOPE
        ),
        "target_trade_date": target.isoformat(),
        "catalog_batch_id": authority_catalog.batch_id,
        "catalog_manifest_hash": authority_catalog.manifest_hash,
        "catalog_member_set_hash": authority_catalog.member_set_hash,
        "catalog_member_count": len(authority_codes),
        "catalog_codes_sha256": canonical_hash(authority_codes),
        "qmt_daily_truth": truth_payload,
        "qmt_daily_truth_sha256": canonical_hash(truth_payload),
        "reconciliation": reconciled,
        "reconciliation_sha256": reconciliation_sha,
    }
    if added_codes and not _reconstruction_authority_matches(
        authority, target_trade_date=target, catalog=catalog,
        known_at=_shanghai_naive(datetime.now(PRODUCTION_TIMEZONE)),
    ):
        raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_RECONSTRUCTION_CATALOG_DIFF_UNEXPLAINED", sorted(added_codes)[0])
    return catalog, authority


def _existing_live_batch_inventory(
    connection, *, source: str, target: date, decision_at: datetime,
    capture_deadline: datetime | None,
) -> list[dict]:
    """Read candidate metadata for the exact acceptance window."""
    from server.common.pit_source_coverage_index import PIT_SOURCE_COVERAGE_DATE_INDEX_NAME

    mysql = connection.dialect.name == "mysql"
    query_budget = "/*+ MAX_EXECUTION_TIME(10000) */ " if mysql else ""
    index_hint = f" FORCE INDEX (`{PIT_SOURCE_COVERAGE_DATE_INDEX_NAME}`)" if mysql else ""
    # The indexed date comes from the observed timestamp, so legacy batch IDs
    # and existing ambiguity/envelope checks retain their original meaning.
    statement = text(
        f"SELECT {query_budget}batch_id, MIN(stock_code) AS sample_stock_code, "
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
        f"FROM st_pit_source_coverage{index_hint} "
        "WHERE fact_kind='event' AND source=:source "
        "AND watermark_kind='QUERY_CUTOFF' "
        "AND batch_id LIKE :batch_pattern "
        "AND known_at>=:known_from AND known_at<=:decision_at "
        "AND (:capture_deadline IS NULL "
        "OR known_at<:capture_deadline) "
        "GROUP BY batch_id "
        "HAVING SUM(CASE WHEN coverage_status='COMPLETE' THEN 0 "
        "ELSE 1 END)=0 "
        "ORDER BY max_known_at DESC, batch_id DESC LIMIT 2"
    )
    try:
        return [
            dict(row)
            for row in connection.execute(statement, {
                "source": source,
                "batch_pattern": _FULL_MARKET_BATCH_PREFIX[source] + "%",
                "known_from": datetime.combine(target, datetime.min.time()),
                "decision_at": decision_at,
                "capture_deadline": capture_deadline,
            }).mappings()
        ]
    except SQLAlchemyError as exc:
        driver_args = getattr(getattr(exc, "orig", None), "args", ())
        error_number = driver_args[0] if driver_args and type(driver_args[0]) is int else None
        error_type = type(exc).__name__
        error_detail = f":errno={error_number}" if error_number is not None else ""
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_EXISTING_BATCH_INVENTORY_FAILED",
            f"{source}:{error_type}{error_detail}",
        ) from None


def validate_existing_complete_qmt_announcement_batch(
    engine,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    now: datetime | None = None,
    expected_trade_date: str = "",
    validation_run_uid: str = "",
    validation_build_sha: str = "",
) -> dict:
    """Read-only proof of the existing official batch for the closed session.

    QMT capture is owned by the Windows edge.  A release replay may only
    select the unique newest batch whose immutable capture window still maps
    to the scheduler-bound closed trading day, then revalidate its calendar,
    catalog, full-market coverage and global content root.  This function
    executes SELECT statements only.
    """

    if not 20 <= int(window_days) <= 3660:
        raise ValueError("QMT announcement window_days must be 20..3660")
    exact_validation_run_uid = str(validation_run_uid).strip().lower()
    exact_validation_build_sha = str(validation_build_sha).strip().lower()
    validation_identity_declared = bool(
        exact_validation_run_uid or exact_validation_build_sha
    )
    if validation_identity_declared and (
        re.fullmatch(r"[0-9a-f]{32}", exact_validation_run_uid) is None
        or re.fullmatch(r"[0-9a-f]{40}", exact_validation_build_sha) is None
        or exact_validation_build_sha == "0" * 40
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_VALIDATION_IDENTITY_INVALID"
        )
    validation_identity = (
        {
            "validation_run_uid": exact_validation_run_uid,
            "validation_build_sha": exact_validation_build_sha,
        }
        if validation_identity_declared else {}
    )
    observed_at = now or datetime.now(PRODUCTION_TIMEZONE)
    decision_at = _shanghai_naive(observed_at).replace(microsecond=0)
    latest_target_text = str(
        authoritative_closed_trade_date(engine, now=observed_at) or ""
    )
    latest_target = _iso_date(latest_target_text)
    if expected_trade_date:
        target = _iso_date(expected_trade_date)
        if target > latest_target:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_EXPECTED_TRADE_DATE_DIFFERS",
                "expected="
                f"{target.isoformat()},latest_authoritative="
                f"{latest_target.isoformat()}",
            )
    else:
        target = latest_target
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
                end_date=latest_target.isoformat(),
                decision_known_at=decision_at,
            )
            sessions = calendar.sessions_between(
                required_start.isoformat(), latest_target.isoformat()
            )
        except Exception as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_AUTHORITATIVE_CALENDAR_UNAVAILABLE",
                type(exc).__name__,
            ) from exc
        if (
            not sessions
            or sessions[-1] != latest_target.isoformat()
            or target.isoformat() not in sessions
        ):
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_AUTHORITATIVE_CALENDAR_DIFFERS"
            )
        later_sessions = [
            date.fromisoformat(session)
            for session in sessions
            if session > target.isoformat()
        ]
        capture_deadline = (
            datetime.combine(later_sessions[0], DAILY_CLOSE_READY_TIME)
            if later_sessions
            else None
        )

        # Provider identity is shared with narrower consumers such as the
        # holdings risk watch. Only full-market producer batches may win this
        # selection. Historical reconstructions use a separate watermark.
        selected_source = ""
        batches: list[dict] = []
        for candidate_source in AUTHORITATIVE_ANNOUNCEMENT_SOURCES:
            batches = _existing_live_batch_inventory(
                connection, source=candidate_source, target=target, decision_at=decision_at,
                capture_deadline=capture_deadline,
            )
            if batches:
                if candidate_source == QMT_ANNOUNCEMENT_SOURCE:
                    latest_qmt = batches[0]
                    try:
                        qmt_coverage_count = _nonnegative_integer(
                            latest_qmt.get("coverage_row_count"),
                            field="coverage_row_count",
                        )
                        qmt_distinct_count = _nonnegative_integer(
                            latest_qmt.get("distinct_stock_count"),
                            field="distinct_stock_count",
                        )
                        qmt_invalid_coverage = _nonnegative_integer(
                            latest_qmt.get("invalid_coverage_count"),
                            field="invalid_coverage_count",
                        )
                        qmt_invalid_results = _nonnegative_integer(
                            latest_qmt.get("invalid_result_count"),
                            field="invalid_result_count",
                        )
                        qmt_event_count = _nonnegative_integer(
                            latest_qmt.get("event_count"), field="event_count",
                        )
                        qmt_empty_count = _nonnegative_integer(
                            latest_qmt.get("empty_stock_count"),
                            field="empty_stock_count",
                        )
                        qmt_known_at = _exact_datetime(
                            latest_qmt.get("max_known_at"), field="known_at",
                        )
                        qmt_received_at = _exact_datetime(
                            latest_qmt.get("max_received_at"),
                            field="received_at",
                        )
                        qmt_fact_cutoff_at = _exact_datetime(
                            latest_qmt.get("max_fact_cutoff_at"),
                            field="fact_cutoff_at",
                        )
                        qmt_window_start = _iso_date(
                            latest_qmt.get("max_window_start")
                        )
                        qmt_window_end = _iso_date(
                            latest_qmt.get("max_window_end")
                        )
                        qmt_uniform_empty_envelope = (
                            qmt_coverage_count >= 100
                            and qmt_coverage_count == qmt_distinct_count
                            and qmt_invalid_coverage == 0
                            and qmt_invalid_results == 0
                            and qmt_event_count == 0
                            and qmt_empty_count == qmt_coverage_count
                            and _exact_datetime(
                                latest_qmt.get("min_known_at"), field="known_at",
                            ) == qmt_known_at
                            and _exact_datetime(
                                latest_qmt.get("min_received_at"),
                                field="received_at",
                            ) == qmt_received_at
                            and _exact_datetime(
                                latest_qmt.get("min_fact_cutoff_at"),
                                field="fact_cutoff_at",
                            ) == qmt_fact_cutoff_at
                            and _iso_date(latest_qmt.get("min_window_start"))
                            == qmt_window_start
                            and _iso_date(latest_qmt.get("min_window_end"))
                            == qmt_window_end
                            and qmt_known_at == qmt_received_at
                            and datetime.combine(
                                target, datetime.min.time()
                            ) <= qmt_fact_cutoff_at
                            and qmt_fact_cutoff_at
                            <= qmt_known_at <= decision_at
                            and target <= qmt_window_end <= decision_at.date()
                            and qmt_fact_cutoff_at.date() == qmt_window_end
                            and qmt_window_start == required_start
                            and not (
                                len(batches) > 1
                                and _exact_datetime(
                                    batches[1].get("max_known_at"),
                                    field="max_known_at",
                                ) == qmt_known_at
                            )
                        )
                    except (QMTAnnouncementBlocked, TypeError, ValueError):
                        qmt_uniform_empty_envelope = False

                    if qmt_uniform_empty_envelope:
                        try:
                            qmt_catalog = load_stock_catalog(
                                connection,
                                decision_known_at=qmt_fact_cutoff_at,
                            )
                            qmt_catalog_codes = qmt_catalog.eligible_codes(
                                target.isoformat()
                            )
                        except Exception:
                            # A malformed envelope/catalog must remain selected
                            # and fail below; it may never authorize fallback.
                            qmt_catalog_codes = []
                        if len(qmt_catalog_codes) == qmt_coverage_count:
                            try:
                                validate_complete_qmt_announcement_batch(
                                    engine,
                                    codes=qmt_catalog_codes,
                                    decision_at=qmt_known_at,
                                    fact_cutoff_at=qmt_fact_cutoff_at,
                                    window_start=qmt_window_start,
                                    window_end=qmt_window_end,
                                    source=QMT_ANNOUNCEMENT_SOURCE,
                                )
                            except QMTAnnouncementBlocked as exc:
                                if (
                                    exc.reason_code
                                    == "QMT_ANNOUNCEMENT_FULL_MARKET_ALL_EMPTY_UNPROVEN"
                                    and exc.detail == "legacy-complete-batch"
                                ):
                                    # Only this fully hash-validated legacy
                                    # permission-failure shape may yield to a
                                    # separately proven fallback provider.
                                    batches = []
                                    continue
                                raise
                selected_source = candidate_source
                break

        if not batches:
            if target > latest_target or (
                target == latest_target and decision_at.date() <= target
            ):
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND",
                    target.isoformat(),
                )
            catalog, _authority = _load_historical_reconstruction_authority(
                engine,
                target_trade_date=target,
                decision_known_at=decision_at,
                existing_batch_only=True,
            )
            historical = validate_complete_historical_reconstruction_batch(
                engine,
                codes=catalog.codes,
                decision_at=decision_at,
                window_start=required_start,
                window_end=target,
                expected_trade_date=target,
            )
            provenance = dict(
                historical.get("reconstruction_provenance") or {}
            )
            reconstructed_at = _exact_datetime(
                provenance.get("reconstructed_at"),
                field="reconstructed_at",
            )
            reconstruction_started_at = _exact_datetime(
                provenance.get("reconstruction_started_at"),
                field="reconstruction_started_at",
            )
            persisted_origin = str(
                provenance.get("execution_origin") or ""
            ).strip().upper()
            return {
                "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
                "status": "COMPLETE",
                "reason_code": (
                    "QMT_ANNOUNCEMENT_EXISTING_HISTORICAL_"
                    "RECONSTRUCTION_COMPLETE"
                ),
                "detail": "",
                "mode": "HISTORICAL_RECONSTRUCTION_EXISTING",
                "trade_date": target.isoformat(),
                "source": CNINFO_ANNOUNCEMENT_SOURCE,
                "primary_source": QMT_ANNOUNCEMENT_SOURCE,
                "fallback_reason": (
                    "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED"
                ),
                "funding_eligible": True,
                "calendar_batch_id": calendar.batch_id,
                "calendar_manifest_hash": calendar.manifest_hash,
                "batch_id": historical["batch_id"],
                "batch_root_hash": historical["batch_root_hash"],
                "catalog_batch_id": catalog.batch_id,
                "catalog_manifest_hash": catalog.manifest_hash,
                "catalog_member_set_hash": catalog.member_set_hash,
                "stock_count": len(catalog.codes),
                "coverage_count": historical["coverage_count"],
                "event_count": historical["event_count"],
                "empty_stock_count": historical["empty_stock_count"],
                "fact_cutoff_at": historical["fact_cutoff_at"],
                "source_query_cutoff_at": historical[
                    "source_query_cutoff_at"
                ],
                "decision_at": historical["decision_at"],
                "received_at": historical["received_at"],
                "reconstructed_at": historical["reconstructed_at"],
                "capture_seconds": max(
                    0,
                    int(
                        (reconstructed_at - reconstruction_started_at)
                        .total_seconds()
                    ),
                ),
                "window_start": historical["window_start"],
                "window_end": historical["window_end"],
                "reconstruction_provenance": provenance,
                "reconstruction_sha256": historical[
                    "reconstruction_sha256"
                ],
                **(
                    {"execution_origin": persisted_origin}
                    if persisted_origin
                    else {}
                ),
                **(
                    {
                        "research_run_uid": str(
                            provenance.get("scheduler_run_uid") or ""
                        ).strip().lower()
                    }
                    if persisted_origin
                    == QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN
                    else {}
                ),
                **validation_identity,
                "database_writes": False,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }
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
        source=selected_source,
    )
    capture_seconds = int((received_at - fact_cutoff_at).total_seconds())
    closed_sessions_at_cutoff = [
        session
        for session in sessions
        if session < fact_cutoff_at.date().isoformat()
        or (
            session == fact_cutoff_at.date().isoformat()
            and fact_cutoff_at.time() >= DAILY_CLOSE_READY_TIME
        )
    ]
    if (
        proof.get("status") != "COMPLETE"
        or proof.get("source") != selected_source
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
        or not closed_sessions_at_cutoff
        or closed_sessions_at_cutoff[-1] != target.isoformat()
        or capture_seconds < 0
    ):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_COMPLETE_BATCH_PROOF_DIFFERS", batch_id
        )
    return {
        "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
        "status": "COMPLETE",
        "reason_code": (
            "QMT_ANNOUNCEMENT_EXISTING_FULL_MARKET_COMPLETE"
            if selected_source == QMT_ANNOUNCEMENT_SOURCE
            else "ANNOUNCEMENT_FALLBACK_EXISTING_FULL_MARKET_COMPLETE"
        ),
        "detail": "",
        "mode": "validate-existing-complete-batch",
        "trade_date": target.isoformat(),
        "source": selected_source,
        **({
            "primary_source": str(proof.get("primary_source") or ""),
            "fallback_reason": str(proof.get("fallback_reason") or ""),
        } if selected_source != QMT_ANNOUNCEMENT_SOURCE else {}),
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
        **validation_identity,
        "database_writes": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def validate_existing_task_result(
    payload: object,
    process_exit: int,
    *,
    expected_trade_date: str,
    expected_scheduler_run_uid: str = "",
    expected_build_sha: str = "",
) -> str:
    """Validate an exact release target's lookup or historical recovery result."""

    expected = _iso_date(expected_trade_date).isoformat()
    disposition = validate_task_result(payload, process_exit)
    if (
        isinstance(payload, dict)
        and payload.get("status") == "DATA_BLOCKED"
        and payload.get("mode") == "HISTORICAL_RECONSTRUCTION_RECOVERY"
        and payload.get("trade_date") == expected
    ):
        expected_writes = None if payload.get("reason_code") == PUBLISH_COMMIT_UNKNOWN else False
        if "database_writes" not in payload or payload["database_writes"] is not expected_writes:
            raise ValueError("QMT announcement historical recovery write outcome differs")
        return disposition
    if isinstance(payload, dict) and payload.get("mode") in {
        "HISTORICAL_RECONSTRUCTION",
        "HISTORICAL_RECONSTRUCTION_EXISTING",
    }:
        empty_stock_count = payload.get("empty_stock_count")
        provenance = payload.get("reconstruction_provenance")
        if not isinstance(provenance, dict):
            raise ValueError("QMT announcement reconstruction proof is absent")
        scheduler_run_uid = str(
            provenance.get("scheduler_run_uid") or ""
        ).strip().lower()
        build_sha = str(provenance.get("build_sha") or "").strip().lower()
        validation_run_uid = str(
            payload.get("validation_run_uid") or ""
        ).strip().lower()
        validation_build_sha = str(
            payload.get("validation_build_sha") or ""
        ).strip().lower()
        validation_identity_required = bool(
            expected_scheduler_run_uid or expected_build_sha
        )
        validation_identity_declared = bool(
            validation_run_uid or validation_build_sha
        )
        if (
            payload.get("status") != "COMPLETE"
            or payload.get("trade_date") != expected
            or provenance.get("target_trade_date") != expected
            or payload.get("source") != CNINFO_ANNOUNCEMENT_SOURCE
            or payload.get("primary_source") != QMT_ANNOUNCEMENT_SOURCE
            or payload.get("fallback_reason")
            != "QMT_ANNOUNCEMENT_HISTORICAL_RECONSTRUCTION_REQUIRED"
            or payload.get("funding_eligible") is not True
            or provenance.get("provider") != CNINFO_ANNOUNCEMENT_SOURCE
            or provenance.get("source") != CNINFO_ANNOUNCEMENT_SOURCE
            or re.fullmatch(r"[0-9a-f]{32}", scheduler_run_uid) is None
            or re.fullmatch(r"[0-9a-f]{40}", build_sha) is None
            or build_sha == "0" * 40
            or (
                (validation_identity_required or validation_identity_declared)
                and (
                    re.fullmatch(
                        r"[0-9a-f]{32}", validation_run_uid
                    ) is None
                    or re.fullmatch(
                        r"[0-9a-f]{40}", validation_build_sha
                    ) is None
                    or validation_build_sha == "0" * 40
                )
            )
            or (
                expected_scheduler_run_uid
                and validation_run_uid
                != str(expected_scheduler_run_uid).strip().lower()
            )
            or (
                expected_build_sha
                and validation_build_sha
                != str(expected_build_sha).strip().lower()
            )
            or (
                payload.get("mode") == "HISTORICAL_RECONSTRUCTION"
                and (
                    scheduler_run_uid != validation_run_uid
                    or build_sha != validation_build_sha
                )
            )
            or provenance.get("automatic_real_order_submission") is not False
            or provenance.get("real_order_authority") is not False
            or payload.get("database_writes")
            is not (payload.get("mode") == "HISTORICAL_RECONSTRUCTION")
            or payload.get("stock_count") != payload.get("coverage_count")
            or int(payload.get("stock_count") or 0) <= 0
            or type(empty_stock_count) is not int
            or not 0 <= empty_stock_count <= int(payload.get("stock_count") or 0)
            or _iso_date(payload.get("window_end")) != _iso_date(expected)
            or _iso_date(payload.get("window_start"))
            != _iso_date(expected) - timedelta(days=DEFAULT_WINDOW_DAYS)
        ):
            raise ValueError(
                "QMT announcement historical recovery result differs"
            )
        return disposition
    if (
        not isinstance(payload, dict)
        or payload.get("mode") != "validate-existing-complete-batch"
        or payload.get("database_writes") is not False
        or payload.get("reason_code") == PUBLISH_COMMIT_UNKNOWN
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
        source_name = str(payload.get("source") or "")
        validation_identity_required = bool(
            expected_scheduler_run_uid or expected_build_sha
        )
        validation_identity_declared = bool(
            payload.get("validation_run_uid")
            or payload.get("validation_build_sha")
        )
        if validation_identity_declared:
            complete_fields.update({
                "validation_run_uid", "validation_build_sha",
            })
        if source_name != QMT_ANNOUNCEMENT_SOURCE:
            complete_fields.update({"primary_source", "fallback_reason"})
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
            or payload.get("reason_code") not in {
                "QMT_ANNOUNCEMENT_EXISTING_FULL_MARKET_COMPLETE",
                "ANNOUNCEMENT_FALLBACK_EXISTING_FULL_MARKET_COMPLETE",
            }
            or payload.get("detail") != ""
            or payload.get("trade_date") != expected
            or source_name not in AUTHORITATIVE_ANNOUNCEMENT_SOURCES
            or (
                source_name != QMT_ANNOUNCEMENT_SOURCE
                and (
                    payload.get("primary_source") != QMT_ANNOUNCEMENT_SOURCE
                    or str(payload.get("fallback_reason") or "")
                    not in ANNOUNCEMENT_FALLBACK_REASON_CODES
                )
            )
            or payload.get("funding_eligible") is not True
            or (
                (validation_identity_required or validation_identity_declared)
                and (
                    re.fullmatch(
                        r"[0-9a-f]{32}",
                        str(payload.get("validation_run_uid") or ""),
                    ) is None
                    or re.fullmatch(
                        r"[0-9a-f]{40}",
                        str(payload.get("validation_build_sha") or ""),
                    ) is None
                    or payload.get("validation_build_sha") == "0" * 40
                )
            )
            or (
                expected_scheduler_run_uid
                and payload.get("validation_run_uid")
                != str(expected_scheduler_run_uid).strip().lower()
            )
            or (
                expected_build_sha
                and payload.get("validation_build_sha")
                != str(expected_build_sha).strip().lower()
            )
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
            or not payload["batch_id"].startswith(
                _FULL_MARKET_BATCH_PREFIX[source_name]
            )
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


def _close_announcement_fallback(fallback) -> None:
    try:
        closer = getattr(fallback, "close", None)
        if callable(closer):
            closer()
    except Exception as exc:
        logger.warning("Announcement provider cleanup failed: %s", type(exc).__name__)


def _blocked(reason_code: str, detail: str = "") -> dict:
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    return {
        "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
        "status": "DATA_BLOCKED",
        "reason_code": str(reason_code or "QMT_ANNOUNCEMENT_DATA_BLOCKED"),
        "detail": str(detail or "")[:1000],
        **({"database_writes": None} if reason_code == PUBLISH_COMMIT_UNKNOWN else {}),
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


_PREMARKET_SOURCE_RESULT_SCHEMA = "probiga.qmt-announcement-premarket-source.v1"


def _premarket_slot_fields(
    *, trade_date: str, source_cutoff: str, decision_cutoff: str,
) -> dict[str, str]:
    data_session = date.fromisoformat(trade_date)
    source = _shanghai_naive(datetime.fromisoformat(source_cutoff))
    decision = _shanghai_naive(datetime.fromisoformat(decision_cutoff))
    if (
        data_session.isoformat() != trade_date
        or data_session >= decision.date()
        or source != datetime.fromisoformat(
            f"{decision.date()}T{PREMARKET_ANNOUNCEMENT_SOURCE_TIME}"
        )
        or decision != datetime.fromisoformat(
            f"{decision.date()}T{PREMARKET_ANNOUNCEMENT_DECISION_TIME}"
        )
    ):
        raise ValueError("premarket announcement source slot differs")
    return {
        "schema": _PREMARKET_SOURCE_RESULT_SCHEMA,
        "data_trade_date": trade_date,
        "execution_session_date": decision.date().isoformat(),
        "source_cutoff_at": source.isoformat(timespec="microseconds"),
        "decision_cutoff_at": decision.isoformat(timespec="microseconds"),
    }


def _premarket_source_claim(
    engine, *, fields: Mapping[str, str], now: datetime,
) -> dict[str, str]:
    source = datetime.fromisoformat(fields["source_cutoff_at"])
    decision = datetime.fromisoformat(fields["decision_cutoff_at"])
    current = _shanghai_naive(now)
    if not source <= current < decision:
        raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_PREMARKET_WINDOW_CLOSED")
    run_uid = os.environ.get("PROBIGA_SCHEDULER_HISTORY_RUN_UID", "").strip()
    build_sha = os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA", "").strip()
    task_id = os.environ.get("PROBIGA_SCHEDULER_TASK_ID", "").strip()
    if (
        os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE") != PREMARKET_ANNOUNCEMENT_TASK_TYPE
        or os.environ.get("PROBIGA_SCHEDULER_EXECUTOR_ROLE") != "qmt_windows_edge"
        or not re.fullmatch(r"[0-9a-f]{32}", run_uid)
        or not re.fullmatch(r"[0-9a-f]{40}", build_sha)
        or not re.fullmatch(r"[1-9][0-9]*", task_id)
    ):
        raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_PREMARKET_OWNER_UNAVAILABLE")
    slot = resolve_strategy_pool_decision_slot(
        engine, data_trade_date=fields["data_trade_date"],
        phase="PREMARKET_REVISION", evaluated_at=decision,
    )
    if (
        slot.execution_session_date != fields["execution_session_date"]
        or _shanghai_naive(slot.scheduled_at) != decision
    ):
        raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_PREMARKET_SLOT_DIFFERS")
    with engine.connect() as connection:
        claim_time = connection.execute(text(
            "SELECT run_at FROM st_scheduled_task_history "
            "WHERE run_uid=:run_uid AND task_id=:task_id AND task_type=:task_type "
            "AND build_sha=:build_sha AND host_name=:host_name "
            "AND status='running' AND trigger_source='scheduled'"
        ), {
            "run_uid": run_uid, "task_id": int(task_id), "build_sha": build_sha,
            "task_type": PREMARKET_ANNOUNCEMENT_TASK_TYPE, "host_name": gethostname()[:128],
        }).scalar_one_or_none()
    if claim_time is None or not source <= _shanghai_naive(
        datetime.fromisoformat(str(claim_time))
    ) <= current:
        raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_PREMARKET_OWNER_UNAVAILABLE")
    return {
        **fields,
        "scheduler_run_uid": run_uid,
        "scheduler_build_sha": build_sha,
    }


def validate_premarket_source_task_result(
    payload: dict, process_exit: int, *, expected_trade_date: str,
    expected_source_cutoff: str, expected_decision_cutoff: str,
    expected_scheduler_run_uid: str, expected_build_sha: str,
    started_at: datetime | None = None, finished_at: datetime | None = None,
    expected_parent: Mapping[str, str] | None = None,
) -> str:
    """Replay the exact producer contract under its Windows scheduler identity."""
    disposition = validate_task_result(payload, process_exit)
    if disposition != "complete":
        return disposition
    fields = _premarket_slot_fields(
        trade_date=expected_trade_date, source_cutoff=expected_source_cutoff,
        decision_cutoff=expected_decision_cutoff,
    )
    source = datetime.fromisoformat(fields["source_cutoff_at"])
    decision = datetime.fromisoformat(fields["decision_cutoff_at"])
    cutoff = _shanghai_naive(datetime.fromisoformat(payload["fact_cutoff_at"]))
    received = _shanghai_naive(datetime.fromisoformat(payload["received_at"]))
    published = _shanghai_naive(datetime.fromisoformat(str(payload.get("publish_completed_at") or "")))
    receipt = payload.get("premarket_source")
    if not isinstance(receipt, Mapping):
        raise ValueError("premarket announcement source receipt is missing")
    parent_batch = str(receipt.get("parent_batch_id") or "")
    parent_root = str(receipt.get("parent_batch_root_hash") or "")
    parent_source = str(receipt.get("parent_source") or "")
    if (
        not re.fullmatch(r"[0-9a-f]{32}", expected_scheduler_run_uid)
        or not re.fullmatch(r"[0-9a-f]{40}", expected_build_sha)
        or receipt != {
            **fields, "scheduler_run_uid": expected_scheduler_run_uid,
            "scheduler_build_sha": expected_build_sha,
            "parent_batch_id": parent_batch,
            "parent_batch_root_hash": parent_root,
            "parent_source": parent_source,
        }
        or not parent_batch or parent_batch == payload.get("batch_id")
        or not re.fullmatch(r"[0-9a-f]{64}", parent_root)
        or payload.get("parent_batch_id") != parent_batch
        or payload.get("parent_batch_root_hash") != parent_root
        or not re.fullmatch(r"[0-9a-f]{64}", str(payload.get("incremental_chain_root_hash") or ""))
        or parent_source != payload.get("source")
        or (expected_parent is not None and (
            parent_batch != expected_parent.get("batch_id")
            or parent_root != expected_parent.get("batch_root_hash")
            or parent_source != expected_parent.get("source")
            or str(payload.get("fallback_reason") or "")
            != str(expected_parent.get("fallback_reason") or "")
        ))
        or payload.get("mode")
        or payload.get("source") not in {
            QMT_ANNOUNCEMENT_SOURCE, CNINFO_ANNOUNCEMENT_SOURCE,
        }
        or payload.get("coverage_target_date") != expected_trade_date
        or payload.get("window_start") != (
            date.fromisoformat(expected_trade_date) - timedelta(days=DEFAULT_WINDOW_DAYS)
        ).isoformat()
        or payload.get("window_end") != fields["execution_session_date"]
        or cutoff != source
        or not source <= received <= published <= decision
        or (started_at is not None and not source <= _shanghai_naive(started_at) <= received)
        or (finished_at is not None and not published <= _shanghai_naive(finished_at) <= decision)
    ):
        raise ValueError("premarket announcement source receipt identity or time differs")
    return disposition


def main(argv: list[str] | None = None, *, premarket_source: bool = False) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument(
        "--overlap-days",
        type=int,
        default=3,
        help=(
            "normal daily capture overlap; the remaining window is reused "
            "from the last validated immutable batch"
        ),
    )
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument(
        "--data-adapter",
        choices=("auto", "bigqmt", "xtdata"),
        default="auto",
        help="announcement source transport; auto uses BigQMT on Windows",
    )
    parser.add_argument(
        "--fallback-provider",
        choices=("none", "cninfo", "eastmoney"),
        default="cninfo",
        help=(
            "QMT明确不可用后的审计源；默认巨潮全市场分页，"
            "未穷尽全目录时仍DATA_BLOCKED"
        ),
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
    parser.add_argument(
        "--recover-missing-historical",
        action="store_true",
        help=(
            "for a scheduler-bound historical session, validate an exact "
            "batch first and reconstruct a missing batch from CNINFO only"
        ),
    )
    parser.add_argument(
        "--research-recover-date",
        default="",
        help=(
            "manual research-only historical material recovery for one exact "
            "trade date; never grants decision or order authority"
        ),
    )
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--expected-trade-date", default="")
    if premarket_source:
        parser.add_argument("--premarket-source-cutoff", required=True)
        parser.add_argument("--premarket-decision-cutoff", required=True)
    parser.add_argument(
        "--validate-result-exit", type=int, default=-1,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--validate-existing-result-exit", type=int, default=-1,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)
    historical_modes = sum(bool(value) for value in (
        args.validate_existing_complete_batch,
        args.recover_missing_historical,
        args.research_recover_date,
    ))
    if premarket_source and (
        historical_modes or not args.expected_trade_date or args.no_resume
        or args.window_days != DEFAULT_WINDOW_DAYS or args.overlap_days != 3
        or args.batch_size != DEFAULT_BATCH_SIZE or args.data_adapter != "auto"
        or args.fallback_provider != "cninfo"
        or args.checkpoint_dir != QMT_ANNOUNCEMENT_CHECKPOINT_DIR
        or args.validate_existing_result_exit >= 0 or args.validate_result_exit >= 0
    ):
        parser.error("premarket source requires the frozen Windows scheduler arguments")
    if historical_modes > 1:
        parser.error("historical batch modes are mutually exclusive")
    if args.research_recover_date:
        if args.expected_trade_date or not args.expected_build_sha:
            parser.error(
                "--research-recover-date requires --expected-build-sha and "
                "does not accept --expected-trade-date"
            )
    elif args.expected_build_sha:
        parser.error("--expected-build-sha requires --research-recover-date")
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
    progress = None
    manual_research = bool(args.research_recover_date)
    target_text = (
        args.research_recover_date
        if manual_research
        else args.expected_trade_date
    )
    validation_run_uid = ""
    validation_build_sha = ""
    execution_origin = ""
    premarket_claim: dict[str, str] = {}
    premarket_parent = None
    try:
        if historical_modes:
            if (
                not target_text
                or args.checkpoint_dir
                or args.no_resume
                or args.window_days != DEFAULT_WINDOW_DAYS
                or args.overlap_days != 3
                or args.batch_size != DEFAULT_BATCH_SIZE
                or args.data_adapter != "auto"
                or args.fallback_provider != "cninfo"
            ):
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_READ_ONLY_ARGUMENTS_INVALID"
                )
            if manual_research:
                validation_run_uid = uuid.uuid4().hex
                validation_build_sha = _manual_research_build_sha(
                    args.expected_build_sha
                )
                execution_origin = QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN
            else:
                validation_run_uid = os.environ.get(
                    "PROBIGA_SCHEDULER_HISTORY_RUN_UID", ""
                ).strip().lower()
                validation_build_sha = os.environ.get(
                    "PROBIGA_SCHEDULER_BUILD_SHA", ""
                ).strip().lower()
                execution_origin = QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN
        if args.recover_missing_historical or manual_research or premarket_source:
            engine = create_tool_engine(connect_args={"connect_timeout": 8, "read_timeout": 45, "write_timeout": 45})
        else:
            engine = create_tool_engine()
        if historical_modes:
            try:
                payload = validate_existing_complete_qmt_announcement_batch(
                    engine,
                    window_days=args.window_days,
                    expected_trade_date=target_text,
                    validation_run_uid=validation_run_uid,
                    validation_build_sha=validation_build_sha,
                )
                if manual_research and payload.get("mode") == (
                    "HISTORICAL_RECONSTRUCTION_EXISTING"
                ):
                    persisted = payload.get("reconstruction_provenance")
                    if isinstance(persisted, Mapping) and persisted.get(
                        "execution_origin"
                    ) == QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN:
                        payload["execution_origin"] = (
                            QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN
                        )
                        payload["research_run_uid"] = str(
                            persisted.get("scheduler_run_uid") or ""
                        ).strip().lower()
            except QMTAnnouncementBlocked as existing_exc:
                if (
                    not (args.recover_missing_historical or manual_research)
                    or existing_exc.reason_code
                    != "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND"
                    or existing_exc.detail != "no-common-batch"
                ):
                    raise
                observed = datetime.now(PRODUCTION_TIMEZONE)
                target = _iso_date(target_text)
                catalog, authority = _load_historical_reconstruction_authority(
                    engine,
                    target_trade_date=target,
                    decision_known_at=observed,
                    execution_origin=execution_origin,
                )
                checkpoint_dir = _checkpoint_root(args.checkpoint_dir)
                fallback = _fallback_announcement_adapter("cninfo")
                if os.name == "nt" and _is_production() and not manual_research:
                    progress = MinuteImportProgress(
                        run_id=validation_run_uid, total_units=len(catalog.codes), operation="historical_reconstruction",
                        days=[target], dataset="qmt_announcement_pit", checkpoint={},
                    )
                try:
                    payload = synchronize_historical_cninfo_announcements(
                        engine,
                        adapter=fallback,
                        checkpoint_root=checkpoint_dir,
                        catalog=catalog,
                        context=HistoricalReconstructionContext(
                            target_trade_date=target,
                            source_query_cutoff_at=datetime.combine(
                                target, datetime.max.time()
                            ),
                            reconstruction_started_at=_shanghai_naive(
                                observed
                            ),
                            scheduler_run_uid=validation_run_uid,
                            build_sha=validation_build_sha,
                            authority=authority,
                            execution_origin=execution_origin,
                        ),
                        window_days=args.window_days,
                        batch_size=args.batch_size,
                        progress=progress,
                    )
                    payload["validation_run_uid"] = validation_run_uid
                    payload["validation_build_sha"] = validation_build_sha
                    payload["execution_origin"] = execution_origin
                    if manual_research:
                        payload["research_run_uid"] = validation_run_uid
                finally:
                    _close_announcement_fallback(fallback)
        else:
            frozen_capture_options = {}
            if premarket_source:
                fields = _premarket_slot_fields(
                    trade_date=target_text, source_cutoff=args.premarket_source_cutoff,
                    decision_cutoff=args.premarket_decision_cutoff,
                )
                premarket_claim = _premarket_source_claim(
                    engine, fields=fields, now=datetime.now(PRODUCTION_TIMEZONE),
                )
                from server.common.daily_delivery_control import load_frozen_initial_announcement_parent

                premarket_parent = load_frozen_initial_announcement_parent(
                    engine, trade_date=target_text,
                    release_id=runtime_contract_build_sha(
                        expected_build_sha=premarket_claim["scheduler_build_sha"],
                    ),
                )
                if not premarket_parent:
                    raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_PREMARKET_PARENT_UNAVAILABLE")
                parent_source = premarket_parent.get("source")
                parent_fallback_reason = str(premarket_parent.get("fallback_reason") or "")
                if not (
                    parent_source == QMT_ANNOUNCEMENT_SOURCE and not parent_fallback_reason
                    or parent_source == CNINFO_ANNOUNCEMENT_SOURCE
                    and parent_fallback_reason in ANNOUNCEMENT_FALLBACK_REASON_CODES
                ):
                    raise QMTAnnouncementBlocked("QMT_ANNOUNCEMENT_PREMARKET_PARENT_SOURCE_DIFFERS")
                premarket_claim.update({
                    "parent_batch_id": premarket_parent["batch_id"],
                    "parent_batch_root_hash": premarket_parent["batch_root_hash"],
                    "parent_source": parent_source,
                })
                frozen_capture_options = {
                    "require_exact_capture_cutoff": True,
                    "capture_fact_cutoff_at": fields["source_cutoff_at"],
                    "completion_deadline_at": fields["decision_cutoff_at"],
                    "required_incremental_parent": premarket_parent,
                }
            elif args.expected_trade_date:
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CAPTURE_ARGUMENTS_INVALID"
                )
            checkpoint_dir = _checkpoint_root(args.checkpoint_dir)

            # Some QMT builds print connection diagnostics.  Keep stdout a
            # single machine JSON record so scheduler validation cannot accept
            # ambiguity.
            with redirect_stdout(sys.stderr):
                inherited_fallback = (
                    premarket_parent is not None
                    and premarket_parent["source"] == CNINFO_ANNOUNCEMENT_SOURCE
                )
                if inherited_fallback:
                    primary_payload = _blocked(
                        premarket_parent["fallback_reason"], "signed-initial-source",
                    )
                else:
                    try:
                        xtdata = _announcement_data_adapter(args.data_adapter)
                        capture_options = _announcement_capture_options(
                            xtdata, engine=engine, no_resume=args.no_resume,
                            coverage_target_date=target_text if premarket_source else None,
                        )
                        primary_payload = synchronize_qmt_announcements(
                            engine,
                            xtdata=xtdata,
                            checkpoint_root=checkpoint_dir,
                            window_days=args.window_days,
                            overlap_days=args.overlap_days,
                            batch_size=args.batch_size,
                            **capture_options,
                            **frozen_capture_options,
                        )
                    except Exception as exc:
                        primary_reason = _explicit_qmt_unavailability_reason(exc)
                        if not primary_reason:
                            raise
                        primary_payload = _blocked(primary_reason, type(exc).__name__)
                payload = primary_payload
                primary_reason = str(
                    primary_payload.get("reason_code") or ""
                )
                if (
                    primary_payload.get("status") == "DATA_BLOCKED"
                    and primary_reason in ANNOUNCEMENT_FALLBACK_REASON_CODES
                    and args.fallback_provider != "none"
                    and (not premarket_source or inherited_fallback)
                ):
                    fallback = None
                    try:
                        fallback = _fallback_announcement_adapter(
                            args.fallback_provider
                        )
                        fallback_options = _announcement_capture_options(
                            fallback, engine=engine, no_resume=args.no_resume,
                            coverage_target_date=target_text if premarket_source else None,
                        )
                        fallback_options["capture_fact_cutoff_at"] = primary_payload.get(
                            "fact_cutoff_at"
                        )
                        fallback_options.update(frozen_capture_options)
                        payload = synchronize_qmt_announcements(
                            engine,
                            xtdata=fallback,
                            checkpoint_root=checkpoint_dir,
                            window_days=args.window_days,
                            overlap_days=args.overlap_days,
                            batch_size=args.batch_size,
                            source=str(fallback.source),
                            fallback_reason=primary_reason,
                            **fallback_options,
                        )
                        if not inherited_fallback:
                            payload["primary_attempt_status"] = "DATA_BLOCKED"
                            payload["primary_attempt_reason_code"] = primary_reason
                    except Exception as exc:
                        reason = str(
                            getattr(exc, "reason_code", "") or ""
                        ) or "ANNOUNCEMENT_FALLBACK_RUNTIME_DATA_BLOCKED"
                        payload = _blocked(
                            reason,
                            exc.detail
                            if isinstance(exc, QMTAnnouncementBlocked)
                            else type(exc).__name__,
                        )
                        payload.update({
                            "source": str(
                                getattr(fallback, "source", "") or ""
                            ),
                            "primary_source": QMT_ANNOUNCEMENT_SOURCE,
                            "fallback_reason": primary_reason,
                        })
                        if not inherited_fallback:
                            payload["primary_attempt_status"] = "DATA_BLOCKED"
                            payload["primary_attempt_reason_code"] = primary_reason
                    finally:
                        _close_announcement_fallback(fallback)
    except Exception as exc:
        reason = str(getattr(exc, "reason_code", "") or "")
        if not reason:
            message = str(exc).lower()
            reason = (
                "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE"
                if "xtquant" in message or "qmt" in message and "import" in message
                else "QMT_ANNOUNCEMENT_RUNTIME_DATA_BLOCKED"
            )
        payload = _blocked(
            reason,
            exc.detail
            if isinstance(exc, QMTAnnouncementBlocked)
            else type(exc).__name__,
        )
        if historical_modes:
            payload.update({
                "mode": (
                    "HISTORICAL_RECONSTRUCTION_RECOVERY"
                    if args.recover_missing_historical
                    or args.research_recover_date
                    else "validate-existing-complete-batch"
                ),
                "trade_date": (
                    args.research_recover_date or args.expected_trade_date
                ),
            })
            payload.setdefault("database_writes", False)
            if reason in {HISTORICAL_CAPTURE_DEADLINE, HISTORICAL_PUBLISH_DEADLINE,
                          "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"}:
                payload["retryable"] = True
            if args.research_recover_date:
                payload.update({
                    "execution_origin": (
                        QMT_ANNOUNCEMENT_MANUAL_RESEARCH_ORIGIN
                    ),
                    "research_run_uid": locals().get(
                        "validation_run_uid", ""
                    ),
                })
    finally:
        if engine is not None:
            engine.dispose()
    if premarket_claim:
        payload["premarket_source"] = premarket_claim
    process_exit = 0 if payload.get("status") == "COMPLETE" else 2
    try:
        if premarket_source and process_exit == 0:
            validate_premarket_source_task_result(
                payload, process_exit, expected_trade_date=target_text,
                expected_source_cutoff=args.premarket_source_cutoff,
                expected_decision_cutoff=args.premarket_decision_cutoff,
                expected_scheduler_run_uid=premarket_claim["scheduler_run_uid"],
                expected_build_sha=premarket_claim["scheduler_build_sha"],
                finished_at=datetime.now(PRODUCTION_TIMEZONE),
                expected_parent=premarket_parent,
            )
        else:
            validate_task_result(payload, process_exit)
    except Exception as exc:
        payload = _blocked(
            "QMT_ANNOUNCEMENT_INVALID_RESULT_CONTRACT", type(exc).__name__
        )
        process_exit = 2
        validate_task_result(payload, process_exit)
    if progress is not None:
        progress.emit("terminal", status="COMPLETE" if process_exit == 0 else "FAILED",
                      reason_code=payload.get("reason_code"))
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return process_exit


if __name__ == "__main__":
    raise SystemExit(main())

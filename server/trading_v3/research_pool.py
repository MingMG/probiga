from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections import Counter
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


RESEARCH_SCHEMA = "probiga.trading-v3-retrospective-research.v1"
RESEARCH_POOL_SCHEMA = "probiga.trading-v3-research-pool.v1"
RESEARCH_POOL_MANIFEST_SCHEMA = "probiga.trading-v3-research-pool-manifest.v1"
RESEARCH_POOL_OBJECT_PREFIX = "research-pool-object-"
RESEARCH_POOL_MANIFEST_PREFIX = "research-pool-manifest-"
MAX_RESEARCH_PAYLOAD_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024
MAX_MANIFEST_SCAN_BYTES = 4 * 1024 * 1024
MAX_RESEARCH_READ_BYTES = 128 * 1024 * 1024
OBSERVATION_FORECAST_STATUSES = frozenset({
    "VALIDATED_POSITIVE",
    "PAPER_DISCOVERY_CANDIDATE",
    "LEFT_SIDE_PREPARE",
    "RESEARCH_ONLY_UNCALIBRATED",
})

_STATUS_PRIORITY = {
    "VALIDATED_POSITIVE": 0,
    "PAPER_DISCOVERY_CANDIDATE": 1,
    "LEFT_SIDE_PREPARE": 2,
    "RESEARCH_ONLY_UNCALIBRATED": 3,
}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_STOCK_CODE_RE = re.compile(r"^[0-9]{6}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class ResearchPoolValidationError(ValueError):
    pass


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ResearchPoolValidationError(f"{field} must be an object")
    return dict(value)


def _parse_date(value: Any, field: str) -> date:
    try:
        return date.fromisoformat(str(value or ""))
    except (TypeError, ValueError) as exc:
        raise ResearchPoolValidationError(f"{field} is invalid") from exc


def _parse_shanghai_datetime(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ResearchPoolValidationError(f"{field} is invalid") from exc
    if parsed.tzinfo is not None and parsed.utcoffset() is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    return parsed


def _published_at(value: datetime | None) -> datetime:
    resolved = value or datetime.now(_SHANGHAI)
    if resolved.tzinfo is None or resolved.utcoffset() is None:
        resolved = resolved.replace(tzinfo=_SHANGHAI)
    return resolved.astimezone(_SHANGHAI)


def _require_false(mapping: Mapping[str, Any], field: str) -> None:
    if mapping.get(field) is not False:
        raise ResearchPoolValidationError(f"{field} must be false")


def _require_empty_list(mapping: Mapping[str, Any], field: str) -> None:
    if mapping.get(field) != []:
        raise ResearchPoolValidationError(f"{field} must be empty")


def validate_research_payload(
    payload: Mapping[str, Any],
    *,
    expected_date: date | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    outer = _mapping(payload, "payload")
    if outer.get("schema") != RESEARCH_SCHEMA:
        raise ResearchPoolValidationError("research payload schema differs")
    if outer.get("status") != "ok" or outer.get("run_status") != "COMPLETED":
        raise ResearchPoolValidationError("research payload is not complete")
    if outer.get("result_scope") != "RETROSPECTIVE_RESEARCH":
        raise ResearchPoolValidationError("research payload scope differs")
    if outer.get("actionable_status") != "REPLAY_ONLY":
        raise ResearchPoolValidationError("research payload is not replay-only")
    for field in (
        "persisted",
        "canonical_eligible",
        "competition_eligible",
        "order_authority",
        "notification_eligible",
        "execution_enabled",
        "real_trading_enabled",
    ):
        _require_false(outer, field)
    if int(outer.get("paper_order_count") or 0) != 0:
        raise ResearchPoolValidationError("research payload contains paper orders")
    if int(outer.get("position_state_updates") or 0) != 0:
        raise ResearchPoolValidationError("research payload contains position writes")
    if int(outer.get("real_order_count") or 0) != 0:
        raise ResearchPoolValidationError("research payload contains real orders")
    if int(outer.get("target_count") or 0) != 0:
        raise ResearchPoolValidationError("research payload contains portfolio targets")
    if outer.get("portfolio_status") != "RESEARCH_ONLY":
        raise ResearchPoolValidationError("research portfolio status differs")
    for field in (
        "paper_orders",
        "superseded_paper_orders",
        "superseded_partial_paper_orders",
        "superseded_execution_plans",
        "premarket_frozen_paper_orders",
        "premarket_frozen_execution_plans",
    ):
        _require_empty_list(outer, field)
    notification = _mapping(outer.get("notification"), "notification")
    if (
        notification.get("status") != "suppressed"
        or notification.get("reason") != "RETROSPECTIVE_RESEARCH"
    ):
        raise ResearchPoolValidationError("research notification was not suppressed")

    artifact = _mapping(outer.get("research_artifact"), "research_artifact")
    if artifact.get("schema") != RESEARCH_SCHEMA:
        raise ResearchPoolValidationError("research artifact schema differs")
    for field in (
        "persisted",
        "canonical_eligible",
        "competition_eligible",
        "order_authority",
        "notification_eligible",
        "historical_production_decision",
    ):
        _require_false(artifact, field)
    if artifact.get("interpretation") != (
        "CURRENT_CODE_AND_MODEL_APPLIED_TO_HISTORICAL_FACTS"
    ):
        raise ResearchPoolValidationError("research interpretation differs")
    assumptions = _mapping(
        artifact.get("research_assumptions"),
        "research_assumptions",
    )
    for field in (
        "account_snapshot_consumed",
        "position_state_consumed",
        "open_order_state_consumed",
        "paper_learning_consumed",
        "portfolio_allocation_computed",
    ):
        _require_false(assumptions, field)

    target_date = _parse_date(artifact.get("trade_date"), "artifact.trade_date")
    if _parse_date(artifact.get("requested_as_of"), "artifact.requested_as_of") != target_date:
        raise ResearchPoolValidationError("research requested date differs")
    if _parse_date(outer.get("trade_date"), "trade_date") != target_date:
        raise ResearchPoolValidationError("outer research date differs")
    if expected_date is not None and target_date != expected_date:
        raise ResearchPoolValidationError("research target date differs")

    fact_cutoff_at = _parse_shanghai_datetime(
        artifact.get("historical_fact_cutoff_at"),
        "historical_fact_cutoff_at",
    )
    if fact_cutoff_at.date() != target_date:
        raise ResearchPoolValidationError("research cutoff date differs")
    if _parse_shanghai_datetime(outer.get("decision_at"), "decision_at") != fact_cutoff_at:
        raise ResearchPoolValidationError("outer research cutoff differs")
    research_known_at = _parse_shanghai_datetime(
        artifact.get("research_known_at"),
        "research_known_at",
    )
    if target_date < research_known_at.date():
        if fact_cutoff_at.time() != time.max or research_known_at <= fact_cutoff_at:
            raise ResearchPoolValidationError("historical research cutoff is not exact day end")
    elif target_date == research_known_at.date():
        if fact_cutoff_at.time() < time(18, 0):
            raise ResearchPoolValidationError("same-day research requires a closed session")
        if research_known_at <= fact_cutoff_at:
            raise ResearchPoolValidationError("research knowledge time must follow cutoff")
    else:
        raise ResearchPoolValidationError("research target date is in the future")
    check_now = _published_at(now).replace(tzinfo=None)
    if research_known_at > check_now + timedelta(minutes=5):
        raise ResearchPoolValidationError("research knowledge time is in the future")

    pit_evidence = _mapping(artifact.get("pit_evidence"), "pit_evidence")
    if _parse_shanghai_datetime(
        pit_evidence.get("fact_cutoff_at"),
        "pit_evidence.fact_cutoff_at",
    ) != fact_cutoff_at:
        raise ResearchPoolValidationError("PIT fact cutoff differs")
    if _parse_shanghai_datetime(
        pit_evidence.get("decision_known_at"),
        "pit_evidence.decision_known_at",
    ) != research_known_at:
        raise ResearchPoolValidationError("PIT knowledge time differs")

    artifact_sha256 = str(artifact.get("artifact_sha256") or "").lower()
    if not _SHA256_RE.fullmatch(artifact_sha256):
        raise ResearchPoolValidationError("research artifact hash is invalid")
    unhashed = dict(artifact)
    unhashed.pop("artifact_sha256", None)
    if _sha256_bytes(_canonical_json_bytes(unhashed)) != artifact_sha256:
        raise ResearchPoolValidationError("research artifact hash differs")

    run_uid = str(artifact.get("research_run_uid") or "").strip()
    if not run_uid or str(outer.get("research_run_uid") or "").strip() != run_uid:
        raise ResearchPoolValidationError("research run identity differs")
    forecasts = artifact.get("forecasts")
    if not isinstance(forecasts, list):
        raise ResearchPoolValidationError("research forecasts must be a list")
    if int(outer.get("forecast_count") or 0) != len(forecasts):
        raise ResearchPoolValidationError("research forecast count differs")
    validated_count = sum(
        isinstance(item, Mapping)
        and str(item.get("status") or "") == "VALIDATED_POSITIVE"
        for item in forecasts
    )
    if int(outer.get("validated_count") or 0) != validated_count:
        raise ResearchPoolValidationError("validated forecast count differs")
    for item in forecasts:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("status") or "").strip().upper() not in (
            OBSERVATION_FORECAST_STATUSES
        ):
            continue
        if not str(item.get("strategy_key") or "").strip():
            raise ResearchPoolValidationError(
                "matching research strategy key is unavailable"
            )

    return {
        "target_date": target_date,
        "fact_cutoff_at": fact_cutoff_at,
        "research_known_at": research_known_at,
        "artifact_sha256": artifact_sha256,
        "research_run_uid": run_uid,
        "forecast_count": len(forecasts),
        "artifact": artifact,
        "forecasts": forecasts,
    }


def _is_reparse_point(path: Path) -> bool:
    try:
        attributes = int(getattr(path.lstat(), "st_file_attributes", 0) or 0)
    except OSError:
        return False
    return bool(attributes & int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)))


def _reject_existing_link_components(path: Path) -> None:
    current = path
    while True:
        if os.path.lexists(current) and (current.is_symlink() or _is_reparse_point(current)):
            raise ResearchPoolValidationError("research pool path contains a link")
        if current.parent == current:
            break
        current = current.parent


def _resolved_absolute(path: Path, field: str) -> Path:
    if not path.is_absolute():
        raise ResearchPoolValidationError(f"{field} must be absolute")
    _reject_existing_link_components(path)
    resolved = path.resolve(strict=False)
    if resolved == Path(resolved.anchor):
        raise ResearchPoolValidationError(f"{field} is too broad")
    return resolved


def research_pool_store_root(store_root: Path | None = None) -> Path:
    if store_root is not None:
        return _resolved_absolute(Path(store_root), "research pool store root")
    configured = str(os.environ.get("PROBIGA_JOB_LOG_ROOT") or "").strip()
    if configured:
        job_root = Path(configured)
    elif os.name == "nt":
        program_data = str(os.environ.get("PROGRAMDATA") or "").strip()
        if not program_data:
            raise ResearchPoolValidationError(
                "PROBIGA_JOB_LOG_ROOT or PROGRAMDATA is required"
            )
        job_root = Path(program_data) / "ProBigA" / "jobs"
    else:
        job_root = Path("/var/lib/probiga/jobs")
    return _resolved_absolute(job_root, "research pool store root")


def _object_filename(payload_file_sha256: str) -> str:
    return f"{RESEARCH_POOL_OBJECT_PREFIX}{payload_file_sha256}.json"


def _manifest_filename(
    target_date: date,
    research_known_at: datetime,
    published_at: datetime,
    payload_file_sha256: str,
) -> str:
    return (
        f"{RESEARCH_POOL_MANIFEST_PREFIX}{target_date.isoformat()}-"
        f"{research_known_at.strftime('%Y%m%dT%H%M%S%f')}-"
        f"{_published_at(published_at).strftime('%Y%m%dT%H%M%S%f%z')}-"
        f"{payload_file_sha256}.json"
    )


def _ensure_store_root(store_root: Path) -> Path:
    resolved = _resolved_absolute(store_root, "research pool store root")
    resolved.mkdir(parents=True, exist_ok=True, mode=0o700)
    _reject_existing_link_components(resolved)
    resolved = resolved.resolve(strict=True)
    if os.name != "nt":
        os.chmod(resolved, 0o700)
    return resolved


def _write_immutable(path: Path, content: bytes) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _reject_existing_link_components(path.parent)
    if path.exists():
        if path.is_symlink() or _is_reparse_point(path):
            raise ResearchPoolValidationError("research pool object is a link")
        if path.read_bytes() != content:
            raise ResearchPoolValidationError("immutable research pool object differs")
        return False
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name != "nt":
            os.chmod(temporary, 0o600)
        created = True
        try:
            os.link(temporary, path)
        except FileExistsError:
            created = False
            if path.read_bytes() != content:
                raise ResearchPoolValidationError(
                    "immutable research pool object differs"
                )
        return created
    finally:
        temporary.unlink(missing_ok=True)


def _payload_bytes(
    payload: Mapping[str, Any],
    source_bytes: bytes | None,
) -> tuple[bytes, str, str]:
    canonical = _canonical_json_bytes(payload)
    canonical_sha256 = _sha256_bytes(canonical)
    if source_bytes is None:
        return canonical, canonical_sha256, canonical_sha256
    if len(source_bytes) > MAX_RESEARCH_PAYLOAD_BYTES:
        raise ResearchPoolValidationError("research payload is too large")
    try:
        decoded = json.loads(source_bytes.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResearchPoolValidationError("research source file is not valid JSON") from exc
    if decoded != dict(payload):
        raise ResearchPoolValidationError("research source bytes differ from payload")
    return source_bytes, canonical_sha256, _sha256_bytes(source_bytes)


def publish_research_pool(
    payload: Mapping[str, Any],
    *,
    publisher_build_sha: str,
    store_root: Path | None = None,
    published_at: datetime | None = None,
    source_bytes: bytes | None = None,
    require_observations: bool = False,
) -> dict[str, Any]:
    build_sha = str(publisher_build_sha or "").strip().lower()
    if not _GIT_SHA_RE.fullmatch(build_sha):
        raise ResearchPoolValidationError("publisher build SHA is invalid")
    publication_time = _published_at(published_at)
    verified = validate_research_payload(
        payload,
        now=publication_time,
    )
    object_bytes, payload_sha256, payload_file_sha256 = _payload_bytes(
        payload,
        source_bytes,
    )
    if len(object_bytes) > MAX_RESEARCH_PAYLOAD_BYTES:
        raise ResearchPoolValidationError("research payload is too large")

    target_text = verified["target_date"].isoformat()
    manifest = {
        "schema": RESEARCH_POOL_MANIFEST_SCHEMA,
        "target_date": target_text,
        "data_date": target_text,
        "historical_fact_cutoff_at": verified["fact_cutoff_at"].isoformat(
            sep=" "
        ),
        "research_known_at": verified["research_known_at"].isoformat(sep=" "),
        "published_at": publication_time.isoformat(),
        "publisher_build_sha": build_sha,
        "research_run_uid": verified["research_run_uid"],
        "artifact_sha256": verified["artifact_sha256"],
        "payload_sha256": payload_sha256,
        "payload_file_sha256": payload_file_sha256,
        "forecast_count": verified["forecast_count"],
    }
    projected_pool = _project_pool(payload, manifest, verified)
    if require_observations and projected_pool["status"] != "READY":
        summary = projected_pool["summary"]
        raise ResearchPoolValidationError(
            "NO_RESEARCH_OBSERVATION_CANDIDATES: "
            f"total_forecast_count={summary['total_forecast_count']!r}, "
            f"excluded_forecast_count={summary['excluded_forecast_count']!r}"
        )

    root = _ensure_store_root(research_pool_store_root(store_root))
    object_path = root / _object_filename(payload_file_sha256)
    object_created = _write_immutable(object_path, object_bytes)
    manifest_bytes = _canonical_json_bytes(manifest)
    manifest_path = root / _manifest_filename(
        verified["target_date"],
        verified["research_known_at"],
        publication_time,
        payload_file_sha256,
    )
    manifest_created = _write_immutable(manifest_path, manifest_bytes)
    return {
        "schema": "probiga.trading-v3-research-pool-publication.v1",
        "status": "ok",
        "publication_status": "PASS",
        "target_date": target_text,
        "research_known_at": manifest["research_known_at"],
        "published_at": manifest["published_at"],
        "publisher_build_sha": build_sha,
        "artifact_sha256": verified["artifact_sha256"],
        "payload_sha256": payload_sha256,
        "payload_file_sha256": payload_file_sha256,
        "forecast_count": verified["forecast_count"],
        "object_created": object_created,
        "manifest_created": manifest_created,
        "database_writes": False,
        "notifications_sent": False,
        "decision_scope": "RESEARCH_ONLY",
        "new_buy_eligible": False,
        "order_eligible": False,
    }


def _read_bounded_json(path: Path, limit: int, field: str) -> tuple[dict[str, Any], bytes]:
    if path.is_symlink() or _is_reparse_point(path) or not path.is_file():
        raise ResearchPoolValidationError(f"{field} is not a regular file")
    size = path.stat().st_size
    if size <= 0 or size > limit:
        raise ResearchPoolValidationError(f"{field} size is invalid")
    raw = path.read_bytes()
    if len(raw) != size:
        raise ResearchPoolValidationError(f"{field} changed while reading")
    try:
        decoded = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResearchPoolValidationError(f"{field} is not valid JSON") from exc
    return _mapping(decoded, field), raw


def load_research_payload_file(
    path: Path,
    *,
    allowed_roots: tuple[Path, ...],
) -> tuple[dict[str, Any], bytes]:
    candidate = _resolved_absolute(Path(path), "research payload path")
    allowed = tuple(_resolved_absolute(Path(root), "allowed input root") for root in allowed_roots)
    if not any(candidate == root or candidate.is_relative_to(root) for root in allowed):
        raise ResearchPoolValidationError("research payload path is outside allowed roots")
    return _read_bounded_json(
        candidate,
        MAX_RESEARCH_PAYLOAD_BYTES,
        "research payload file",
    )


def _empty_pool(target_date: date, reason: str) -> dict[str, Any]:
    return {
        "schema": RESEARCH_POOL_SCHEMA,
        "status": "UNAVAILABLE",
        "pool_kind": "RETROSPECTIVE_RESEARCH_OBSERVATION",
        "decision_scope": "RESEARCH_ONLY",
        "trade_date": target_date.isoformat(),
        "data_date": target_date.isoformat(),
        "historical_fact_cutoff_at": None,
        "research_known_at": None,
        "generated_at": None,
        "published_at": None,
        "publisher_build_sha": None,
        "artifact_sha256": None,
        "payload_sha256": None,
        "payload_file_sha256": None,
        "pool_readable": False,
        "reason_codes": [reason],
        "permissions": {
            "new_buy_eligible": False,
            "order_eligible": False,
            "notification_eligible": False,
        },
        "summary": {
            "observation_stock_count": 0,
            "matching_forecast_count": 0,
            "total_forecast_count": 0,
            "excluded_forecast_count": 0,
            "excluded_stock_count": 0,
            "status_forecast_counts": {},
        },
        "items": [],
    }


def _finite_number(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def _project_pool(
    payload: Mapping[str, Any],
    manifest: Mapping[str, Any],
    verified: Mapping[str, Any],
) -> dict[str, Any]:
    forecasts = list(verified["forecasts"])
    status_counts: Counter[str] = Counter()
    all_codes: set[str] = set()
    grouped: dict[str, dict[str, Any]] = {}
    matching_forecast_count = 0
    for raw in forecasts:
        if not isinstance(raw, Mapping):
            status_counts["INVALID"] += 1
            continue
        forecast = dict(raw)
        status_text = str(forecast.get("status") or "").strip().upper()
        status_counts[status_text or "UNKNOWN"] += 1
        code = str(forecast.get("stock_code") or "").strip()
        if _STOCK_CODE_RE.fullmatch(code):
            all_codes.add(code)
        if status_text not in OBSERVATION_FORECAST_STATUSES:
            continue
        if not _STOCK_CODE_RE.fullmatch(code):
            raise ResearchPoolValidationError("matching research stock code is invalid")
        matching_forecast_count += 1
        item = grouped.setdefault(
            code,
            {
                "stock_code": code,
                "stock_name": str(forecast.get("stock_name") or code).strip() or code,
                "strategy_keys": [],
                "statuses": [],
                "reasons": [],
                "reference_price": None,
                "raw_score": None,
                "confidence": None,
                "source_forecast_count": 0,
            },
        )
        item["source_forecast_count"] += 1
        strategy_key = str(forecast.get("strategy_key") or "").strip()
        if strategy_key and strategy_key not in item["strategy_keys"]:
            item["strategy_keys"].append(strategy_key)
        if status_text not in item["statuses"]:
            item["statuses"].append(status_text)
        reasons = forecast.get("reasons")
        if isinstance(reasons, list):
            for reason in reasons:
                normalized = str(reason or "").strip()
                if normalized and normalized not in item["reasons"]:
                    item["reasons"].append(normalized)
        features = forecast.get("features")
        reference_price = (
            _finite_number(features.get("price"))
            if isinstance(features, Mapping)
            else None
        )
        if reference_price is not None and reference_price > 0:
            item["reference_price"] = reference_price
        for field in ("raw_score", "confidence"):
            numeric = _finite_number(forecast.get(field))
            if numeric is not None and (
                item[field] is None or numeric > float(item[field])
            ):
                item[field] = numeric

    items = list(grouped.values())
    for item in items:
        item["strategy_keys"].sort()
        item["statuses"].sort(key=lambda value: (_STATUS_PRIORITY[value], value))
        item["status"] = item["statuses"][0]
        item["reasons"] = item["reasons"][:8]
        item.update({
            "data_date": manifest["data_date"],
            "research_known_at": manifest["research_known_at"],
            "publisher_build_sha": manifest["publisher_build_sha"],
            "artifact_sha256": manifest["artifact_sha256"],
            "decision_scope": "RESEARCH_ONLY",
            "display_action": "WATCH",
            "actionability": "RESEARCH_ONLY",
            "new_buy_eligible": False,
            "order_eligible": False,
        })
    items.sort(key=lambda item: (
        _STATUS_PRIORITY[item["status"]],
        -(float(item["raw_score"]) if item["raw_score"] is not None else -1e99),
        item["stock_code"],
    ))
    for rank, item in enumerate(items, 1):
        item["rank_no"] = rank

    status = "READY" if items else "EMPTY"
    return {
        "schema": RESEARCH_POOL_SCHEMA,
        "status": status,
        "pool_kind": "RETROSPECTIVE_RESEARCH_OBSERVATION",
        "decision_scope": "RESEARCH_ONLY",
        "trade_date": manifest["target_date"],
        "data_date": manifest["data_date"],
        "historical_fact_cutoff_at": manifest["historical_fact_cutoff_at"],
        "research_known_at": manifest["research_known_at"],
        "generated_at": manifest["research_known_at"],
        "published_at": manifest["published_at"],
        "publisher_build_sha": manifest["publisher_build_sha"],
        "artifact_sha256": manifest["artifact_sha256"],
        "payload_sha256": manifest["payload_sha256"],
        "payload_file_sha256": manifest["payload_file_sha256"],
        "pool_readable": True,
        "reason_codes": ([] if items else ["NO_MATCHING_RESEARCH_OBSERVATIONS"]),
        "permissions": {
            "new_buy_eligible": False,
            "order_eligible": False,
            "notification_eligible": False,
        },
        "summary": {
            "observation_stock_count": len(items),
            "matching_forecast_count": matching_forecast_count,
            "total_forecast_count": len(forecasts),
            "excluded_forecast_count": len(forecasts) - matching_forecast_count,
            "excluded_stock_count": len(all_codes - set(grouped)),
            "status_forecast_counts": dict(sorted(status_counts.items())),
        },
        "items": items,
    }


def _validated_manifest_payload(
    *,
    root: Path,
    manifest_path: Path,
    target_date: date,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest, _ = _read_bounded_json(
        manifest_path,
        MAX_MANIFEST_BYTES,
        "research pool manifest",
    )
    if manifest.get("schema") != RESEARCH_POOL_MANIFEST_SCHEMA:
        raise ResearchPoolValidationError("research pool manifest schema differs")
    if _parse_date(manifest.get("target_date"), "manifest.target_date") != target_date:
        raise ResearchPoolValidationError("research pool manifest target differs")
    if str(manifest.get("data_date") or "") != target_date.isoformat():
        raise ResearchPoolValidationError("research pool manifest data date differs")
    build_sha = str(manifest.get("publisher_build_sha") or "").lower()
    if not _GIT_SHA_RE.fullmatch(build_sha):
        raise ResearchPoolValidationError("research pool manifest build differs")
    payload_file_sha256 = str(manifest.get("payload_file_sha256") or "").lower()
    payload_sha256 = str(manifest.get("payload_sha256") or "").lower()
    artifact_sha256 = str(manifest.get("artifact_sha256") or "").lower()
    if not all(
        _SHA256_RE.fullmatch(value)
        for value in (payload_file_sha256, payload_sha256, artifact_sha256)
    ):
        raise ResearchPoolValidationError("research pool manifest hash is invalid")
    manifest_known_at = _parse_shanghai_datetime(
        manifest.get("research_known_at"),
        "manifest.research_known_at",
    )
    manifest_published_at = _parse_shanghai_datetime(
        manifest.get("published_at"),
        "manifest.published_at",
    )
    expected_name = _manifest_filename(
        target_date,
        manifest_known_at,
        datetime.fromisoformat(str(manifest["published_at"])),
        payload_file_sha256,
    )
    if manifest_path.name != expected_name:
        raise ResearchPoolValidationError("research pool manifest filename differs")
    object_path = root / _object_filename(payload_file_sha256)
    payload, object_bytes = _read_bounded_json(
        object_path,
        MAX_RESEARCH_PAYLOAD_BYTES,
        "research pool object",
    )
    if _sha256_bytes(object_bytes) != payload_file_sha256:
        raise ResearchPoolValidationError("research pool file hash differs")
    if _sha256_bytes(_canonical_json_bytes(payload)) != payload_sha256:
        raise ResearchPoolValidationError("research pool payload hash differs")
    verified = validate_research_payload(
        payload,
        expected_date=target_date,
        now=now,
    )
    if verified["artifact_sha256"] != artifact_sha256:
        raise ResearchPoolValidationError("research pool artifact hash differs")
    if int(manifest.get("forecast_count") or -1) != verified["forecast_count"]:
        raise ResearchPoolValidationError("research pool manifest count differs")
    if str(manifest.get("research_run_uid") or "") != verified["research_run_uid"]:
        raise ResearchPoolValidationError("research pool manifest run differs")
    if manifest_known_at != verified["research_known_at"]:
        raise ResearchPoolValidationError("research pool manifest knowledge differs")
    if _parse_shanghai_datetime(
        manifest.get("historical_fact_cutoff_at"),
        "manifest.historical_fact_cutoff_at",
    ) != verified["fact_cutoff_at"]:
        raise ResearchPoolValidationError("research pool manifest cutoff differs")
    if manifest_published_at < verified["research_known_at"]:
        raise ResearchPoolValidationError("research pool was published before generation")
    if manifest_published_at > _published_at(now).replace(tzinfo=None) + timedelta(minutes=5):
        raise ResearchPoolValidationError("research pool publication time is in the future")
    return manifest, payload, verified


def read_research_pool(
    target_date: date,
    *,
    store_root: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if type(target_date) is not date:
        raise ResearchPoolValidationError("target_date must be a date")
    root = research_pool_store_root(store_root)
    if not root.exists():
        return _empty_pool(target_date, "NO_EXACT_RESEARCH_POOL")
    _reject_existing_link_components(root)
    if not root.is_dir():
        return _empty_pool(target_date, "NO_VALID_EXACT_RESEARCH_POOL")
    manifest_paths = sorted(
        root.glob(
            f"{RESEARCH_POOL_MANIFEST_PREFIX}{target_date.isoformat()}-*.json"
        ),
        key=lambda item: item.name,
        reverse=True,
    )
    if not manifest_paths:
        return _empty_pool(target_date, "NO_EXACT_RESEARCH_POOL")
    check_now = _published_at(now)
    manifest_bytes_seen = 0
    payload_bytes_seen = 0
    for manifest_path in manifest_paths:
        try:
            manifest_size = manifest_path.stat().st_size
            manifest_bytes_seen += manifest_size
            if manifest_bytes_seen > MAX_MANIFEST_SCAN_BYTES:
                break
            preview, _ = _read_bounded_json(
                manifest_path,
                MAX_MANIFEST_BYTES,
                "research pool manifest",
            )
            preview_hash = str(preview.get("payload_file_sha256") or "").lower()
            if not _SHA256_RE.fullmatch(preview_hash):
                continue
            preview_object = root / _object_filename(preview_hash)
            payload_size = preview_object.stat().st_size
            payload_bytes_seen += payload_size
            if payload_bytes_seen > MAX_RESEARCH_READ_BYTES:
                break
            manifest, payload, verified = _validated_manifest_payload(
                root=root,
                manifest_path=manifest_path,
                target_date=target_date,
                now=check_now,
            )
            return _project_pool(payload, manifest, verified)
        except (OSError, ResearchPoolValidationError):
            continue
    return _empty_pool(target_date, "NO_VALID_EXACT_RESEARCH_POOL")

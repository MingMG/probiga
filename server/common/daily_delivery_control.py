"""Durable control plane for one authoritative daily stock-pool delivery.

The scheduler task table remains configuration and a compact status view.  The
tables in this module are the durable runtime truth: one session per trade date
and release, append-only stage attempts with leases/fencing tokens, and an
append-only terminal receipt for both successful and blocked deliveries.

Runtime callers are DML-only.  DDL belongs to the privileged production schema
bundle.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text


SESSION_TABLE = "st_daily_delivery_session"
ATTEMPT_TABLE = "st_daily_stage_attempt"
RECEIPT_TABLE = "st_daily_delivery_receipt"
DELIVERY_RECEIPT_SCHEMA = "probiga.daily-delivery.v1"
SESSION_SCHEMA = "probiga.daily-run.v1"
STRATEGY_RELEASE_SCHEMA = "probiga.strategy-release-binding.v1"
SCORE_SNAPSHOT_SCHEMA = "probiga.score-snapshot-identity.v1"
SCHEDULER_VALIDATION_EVIDENCE_SCHEMA = (
    "probiga.scheduler-validation-evidence.v1"
)
DAILY_STAGE_IDEMPOTENT_REPLAY_SCHEMA = (
    "probiga.daily-stage-idempotent-replay.v1"
)

TERMINAL_STATUSES = frozenset({"PASS", "DEGRADED", "BLOCKED"})
ATTEMPT_TERMINAL_STATUSES = frozenset(
    {"SUCCESS", "DEGRADED", "BLOCKED", "FAILED", "TIMEOUT", "STOPPED", "SUPERSEDED"}
)
SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
SHA64_RE = re.compile(r"^[0-9a-f]{64}$")
RUN_UID_RE = re.compile(r"^[0-9a-f]{32,64}$")
CONTROL_TIMEZONE = ZoneInfo("Asia/Shanghai")

_REQUIRED_COLUMNS = {
    SESSION_TABLE: frozenset(
        {
            "id",
            "session_uid",
            "run_id",
            "trade_date",
            "release_id",
            "strategy_release_id",
            "status",
            "latest_generation",
            "latest_fencing_token",
            "canonical_receipt_uid",
            "started_at",
            "terminal_at",
            "updated_at",
        }
    ),
    ATTEMPT_TABLE: frozenset(
        {
            "id",
            "attempt_uid",
            "session_uid",
            "scheduler_run_uid",
            "stage_name",
            "shard_id",
            "attempt_no",
            "status",
            "input_root_sha256",
            "output_dataset_id",
            "lease_owner",
            "lease_until",
            "fencing_token",
            "checkpoint_json",
            "error_code",
            "error_detail",
            "started_at",
            "finished_at",
        }
    ),
    RECEIPT_TABLE: frozenset(
        {
            "id",
            "receipt_uid",
            "session_uid",
            "generation",
            "status",
            "scheduler_run_uid",
            "stage_name",
            "release_id",
            "strategy_release_id",
            "analysis_run_uid",
            "governance_run_uid",
            "score_snapshot_id",
            "canonical_pool_sha256",
            "recommendation_count",
            "retryable",
            "blocking_stage",
            "error_code",
            "error_detail",
            "receipt_json",
            "receipt_sha256",
            "created_at",
        }
    ),
}


class DailyDeliveryFenceLost(RuntimeError):
    """The caller no longer owns the newest live attempt for one stage."""


class DailyDeliveryLeaseHeld(RuntimeError):
    """A different owner still has a live lease for the requested stage."""


def canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _control_now() -> datetime:
    """Return the shared naive database wall clock used by Linux and Windows."""

    return datetime.now(CONTROL_TIMEZONE).replace(tzinfo=None)


def _normalized_trade_date(value: object) -> str:
    raw = str(value or "").strip()[:10]
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("daily delivery trade date is invalid") from exc
    if parsed.isoformat() != raw:
        raise ValueError("daily delivery trade date is invalid")
    return raw


def _normalized_release_id(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if SHA40_RE.fullmatch(normalized) is None or normalized == "0" * 40:
        raise ValueError("daily delivery release id is invalid")
    return normalized


def strategy_release_identity() -> str:
    """Hash the immutable built-in strategy/config manifests.

    Dynamic strategy versions remain independently hash-bound inside the
    governance result.  This identity prevents a silent built-in parameter
    change from sharing a daily session with the prior configuration.
    """

    from server.common.versioned_strategy_config import (
        load_market_state_config,
        load_stock_manifest,
        market_state_config_hash,
        stock_manifest_hash,
    )

    stock = load_stock_manifest()
    market = load_market_state_config()
    return canonical_sha256(
        {
            "schema": STRATEGY_RELEASE_SCHEMA,
            "stock_config_version": str(stock.get("config_version") or ""),
            "stock_manifest_sha256": stock_manifest_hash(),
            "market_config_version": str(market.get("config_version") or ""),
            "market_state_config_sha256": market_state_config_hash(),
        }
    )


def daily_session_identity(trade_date: object, release_id: object) -> dict[str, str]:
    target = _normalized_trade_date(trade_date)
    release = _normalized_release_id(release_id)
    session_uid = canonical_sha256(
        {"schema": SESSION_SCHEMA, "trade_date": target, "release_id": release}
    )
    return {
        "session_uid": session_uid,
        "run_id": f"{target.replace('-', '')}-{release[:12]}",
        "trade_date": target,
        "release_id": release,
    }


def score_snapshot_identity(delivery_receipt: Mapping[str, object]) -> str:
    """Create a stable ID for the exact full-market scoring publication."""

    analysis_run_uid = str(delivery_receipt.get("analysis_run_uid") or "").lower()
    pool_root = str(delivery_receipt.get("canonical_pool_sha256") or "").lower()
    if RUN_UID_RE.fullmatch(analysis_run_uid) is None:
        raise ValueError("analysis run identity is invalid")
    if SHA64_RE.fullmatch(pool_root) is None:
        raise ValueError("canonical pool root is invalid")
    return canonical_sha256(
        {
            "schema": SCORE_SNAPSHOT_SCHEMA,
            "trade_date": _normalized_trade_date(
                delivery_receipt.get("target_trade_date")
            ),
            "analysis_run_uid": analysis_run_uid,
            "analysis_count": int(delivery_receipt.get("analysis_count") or 0),
            "recommendation_count": int(
                delivery_receipt.get("recommendation_count") or 0
            ),
            "canonical_pool_sha256": pool_root,
        }
    )


def _core_input_bindings(
    delivery_receipt: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    aliases = {
        "qmt_stock_daily_canonical": "kline",
        "target_turnover_snapshot": "turnover",
        "stock_finance": "financial",
        "qmt_announcement_pit": "announcement",
    }
    result: dict[str, dict[str, object]] = {}
    rows = delivery_receipt.get("base_data_receipts")
    for raw in rows if isinstance(rows, list) else []:
        row = dict(raw) if isinstance(raw, Mapping) else {}
        stage = str(row.get("task_type") or "")
        key = aliases.get(stage)
        dataset_id = str(row.get("run_uid") or "").lower()
        root = str(row.get("evidence_sha256") or "").lower()
        if (
            key
            and RUN_UID_RE.fullmatch(dataset_id) is not None
            and SHA64_RE.fullmatch(root) is not None
        ):
            result[key] = {
                "dataset_id": dataset_id,
                "root": root,
                "stage": stage,
                "input_root": (
                    str(row.get("input_receipt_root_sha256") or "").lower()
                    or None
                ),
            }
    return result


def _dialect_name(connection) -> str:
    return str(getattr(getattr(connection, "dialect", None), "name", "")).lower()


def _column_names(connection, table_name: str) -> set[str]:
    if _dialect_name(connection) == "sqlite":
        return {
            str(row[1])
            for row in connection.execute(
                text(f"PRAGMA table_info({table_name})")
            ).fetchall()
        }
    return {
        str(row[0])
        for row in connection.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
    }


def validate_daily_delivery_runtime_schema(engine) -> dict[str, object]:
    """Read-only validation for the complete delivery control-plane schema."""

    observed: dict[str, list[str]] = {}
    with engine.connect() as connection:
        for table_name, required in _REQUIRED_COLUMNS.items():
            columns = _column_names(connection, table_name)
            missing = sorted(required - columns)
            if missing:
                raise RuntimeError(
                    f"daily delivery table {table_name} differs: missing_columns={missing}"
                )
            observed[table_name] = sorted(columns)
    return {
        "tables": observed,
        "table_count": len(observed),
        "physical_contract_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def privileged_migrate_daily_delivery_schema(engine) -> dict[str, object]:
    """Create the control-plane tables using the privileged release engine."""

    with engine.begin() as connection:
        sqlite = _dialect_name(connection) == "sqlite"
        if sqlite:
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {SESSION_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_uid TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    trade_date DATE NOT NULL,
                    release_id TEXT NOT NULL,
                    strategy_release_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'RUNNING',
                    latest_generation INTEGER NOT NULL DEFAULT 0,
                    latest_fencing_token INTEGER NOT NULL DEFAULT 0,
                    canonical_receipt_uid TEXT NULL,
                    started_at DATETIME NOT NULL,
                    terminal_at DATETIME NULL,
                    updated_at DATETIME NOT NULL,
                    UNIQUE (trade_date, release_id)
                )
            """))
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {ATTEMPT_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    attempt_uid TEXT NOT NULL UNIQUE,
                    session_uid TEXT NOT NULL,
                    scheduler_run_uid TEXT NOT NULL UNIQUE,
                    stage_name TEXT NOT NULL,
                    shard_id TEXT NOT NULL DEFAULT 'main',
                    attempt_no INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    input_root_sha256 TEXT NULL,
                    output_dataset_id TEXT NULL,
                    lease_owner TEXT NOT NULL,
                    lease_until DATETIME NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    checkpoint_json TEXT NULL,
                    error_code TEXT NULL,
                    error_detail TEXT NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME NULL,
                    UNIQUE (session_uid, stage_name, shard_id, attempt_no)
                )
            """))
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {RECEIPT_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    receipt_uid TEXT NOT NULL UNIQUE,
                    session_uid TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    scheduler_run_uid TEXT NOT NULL UNIQUE,
                    stage_name TEXT NOT NULL,
                    release_id TEXT NOT NULL,
                    strategy_release_id TEXT NOT NULL,
                    analysis_run_uid TEXT NULL,
                    governance_run_uid TEXT NULL,
                    score_snapshot_id TEXT NULL,
                    canonical_pool_sha256 TEXT NULL,
                    recommendation_count INTEGER NULL,
                    retryable INTEGER NOT NULL DEFAULT 0,
                    blocking_stage TEXT NULL,
                    error_code TEXT NULL,
                    error_detail TEXT NULL,
                    receipt_json TEXT NOT NULL,
                    receipt_sha256 TEXT NOT NULL,
                    created_at DATETIME NOT NULL,
                    UNIQUE (session_uid, generation)
                )
            """))
        else:
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {SESSION_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    session_uid CHAR(64) NOT NULL,
                    run_id VARCHAR(96) NOT NULL,
                    trade_date DATE NOT NULL,
                    release_id CHAR(40) NOT NULL,
                    strategy_release_id CHAR(64) NOT NULL,
                    status VARCHAR(16) NOT NULL DEFAULT 'RUNNING',
                    latest_generation INT NOT NULL DEFAULT 0,
                    latest_fencing_token BIGINT NOT NULL DEFAULT 0,
                    canonical_receipt_uid CHAR(64) NULL,
                    started_at DATETIME NOT NULL,
                    terminal_at DATETIME NULL,
                    updated_at DATETIME NOT NULL,
                    UNIQUE KEY uk_daily_delivery_session_uid (session_uid),
                    UNIQUE KEY uk_daily_delivery_date_release (trade_date, release_id),
                    KEY idx_daily_delivery_status_date (status, trade_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {ATTEMPT_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    attempt_uid CHAR(64) NOT NULL,
                    session_uid CHAR(64) NOT NULL,
                    scheduler_run_uid VARCHAR(64) NOT NULL,
                    stage_name VARCHAR(64) NOT NULL,
                    shard_id VARCHAR(128) NOT NULL DEFAULT 'main',
                    attempt_no INT NOT NULL,
                    status VARCHAR(24) NOT NULL,
                    input_root_sha256 CHAR(64) NULL,
                    output_dataset_id VARCHAR(128) NULL,
                    lease_owner VARCHAR(128) NOT NULL,
                    lease_until DATETIME NOT NULL,
                    fencing_token BIGINT NOT NULL,
                    checkpoint_json MEDIUMTEXT NULL,
                    error_code VARCHAR(128) NULL,
                    error_detail VARCHAR(1000) NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME NULL,
                    UNIQUE KEY uk_daily_stage_attempt_uid (attempt_uid),
                    UNIQUE KEY uk_daily_stage_scheduler_run (scheduler_run_uid),
                    UNIQUE KEY uk_daily_stage_attempt_no
                        (session_uid, stage_name, shard_id, attempt_no),
                    KEY idx_daily_stage_fence
                        (session_uid, stage_name, shard_id, fencing_token),
                    KEY idx_daily_stage_lease (status, lease_until)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {RECEIPT_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    receipt_uid CHAR(64) NOT NULL,
                    session_uid CHAR(64) NOT NULL,
                    generation INT NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    scheduler_run_uid VARCHAR(64) NOT NULL,
                    stage_name VARCHAR(64) NOT NULL,
                    release_id CHAR(40) NOT NULL,
                    strategy_release_id CHAR(64) NOT NULL,
                    analysis_run_uid VARCHAR(64) NULL,
                    governance_run_uid VARCHAR(64) NULL,
                    score_snapshot_id CHAR(64) NULL,
                    canonical_pool_sha256 CHAR(64) NULL,
                    recommendation_count INT NULL,
                    retryable TINYINT(1) NOT NULL DEFAULT 0,
                    blocking_stage VARCHAR(64) NULL,
                    error_code VARCHAR(128) NULL,
                    error_detail VARCHAR(1000) NULL,
                    receipt_json MEDIUMTEXT NOT NULL,
                    receipt_sha256 CHAR(64) NOT NULL,
                    created_at DATETIME NOT NULL,
                    UNIQUE KEY uk_daily_delivery_receipt_uid (receipt_uid),
                    UNIQUE KEY uk_daily_delivery_scheduler_run (scheduler_run_uid),
                    UNIQUE KEY uk_daily_delivery_generation (session_uid, generation),
                    KEY idx_daily_delivery_receipt_status (status, created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
    return validate_daily_delivery_runtime_schema(engine)


def _one_mapping(result) -> dict[str, Any] | None:
    row = result.mappings().first()
    return dict(row) if row is not None else None


def _datetime_value(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip().replace(" ", "T"))
        except ValueError:
            return None
    return None


def _text_sha256(value: object) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _validated_completed_stage_checkpoint(
    attempt: Mapping[str, object],
    *,
    session: Mapping[str, object],
    stage_name: str,
) -> dict[str, object]:
    """Verify the sealed validator evidence behind an immutable stage success."""

    try:
        checkpoint = json.loads(str(attempt.get("checkpoint_json") or ""))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DailyDeliveryFenceLost(
            "completed daily stage checkpoint is unavailable"
        ) from exc
    if not isinstance(checkpoint, dict):
        raise DailyDeliveryFenceLost(
            "completed daily stage checkpoint is unavailable"
        )
    supplied_hash = str(checkpoint.get("evidence_sha256") or "").lower()
    core = {
        key: value
        for key, value in checkpoint.items()
        if key != "evidence_sha256"
    }
    replay_output = str(checkpoint.get("replay_output") or "")
    input_root = str(attempt.get("input_root_sha256") or "").lower()
    exit_code = checkpoint.get("exit_code")
    if (
        checkpoint.get("schema") != SCHEDULER_VALIDATION_EVIDENCE_SCHEMA
        or SHA64_RE.fullmatch(supplied_hash) is None
        or canonical_sha256(core) != supplied_hash
        or str(checkpoint.get("run_uid") or "").lower()
        != str(attempt.get("scheduler_run_uid") or "").lower()
        or str(checkpoint.get("task_type") or "") != stage_name
        or str(checkpoint.get("build_sha") or "").lower()
        != str(session.get("release_id") or "").lower()
        or str(checkpoint.get("target_trade_date") or "")
        != str(session.get("trade_date") or "")[:10]
        or (
            checkpoint.get("release_target_date") not in (None, "")
            and str(checkpoint.get("release_target_date") or "")
            != str(session.get("trade_date") or "")[:10]
        )
        or checkpoint.get("status") != "success"
        or isinstance(exit_code, bool)
        or not isinstance(exit_code, int)
        or exit_code != 0
        or checkpoint.get("validation_checked") is not True
        or checkpoint.get("validation_ok") is not True
        or SHA64_RE.fullmatch(input_root) is None
        or str(checkpoint.get("input_receipt_root_sha256") or "").lower()
        != input_root
        or str(checkpoint.get("replay_output_sha256") or "").lower()
        != _text_sha256(replay_output)
        or input_root != _text_sha256(replay_output)
    ):
        raise DailyDeliveryFenceLost(
            "completed daily stage checkpoint identity differs"
        )
    return checkpoint


def _completed_stage_replay_checkpoint(
    checkpoint: Mapping[str, object],
    *,
    scheduler_run_uid: str,
    attempt_uid: str,
    fencing_token: int,
    source_attempt: Mapping[str, object],
    now: datetime,
) -> dict[str, object]:
    marker = {
        "schema": DAILY_STAGE_IDEMPOTENT_REPLAY_SCHEMA,
        "status": "SUCCESS",
        "task_type": str(checkpoint.get("task_type") or ""),
        "trade_date": str(checkpoint.get("target_trade_date") or ""),
        "release_id": str(checkpoint.get("build_sha") or "").lower(),
        "scheduler_run_uid": scheduler_run_uid,
        "attempt_uid": attempt_uid,
        "fencing_token": int(fencing_token),
        "source_attempt_uid": str(source_attempt.get("attempt_uid") or ""),
        "source_scheduler_run_uid": str(
            source_attempt.get("scheduler_run_uid") or ""
        ),
        "source_fencing_token": int(
            source_attempt.get("fencing_token") or 0
        ),
        "input_receipt_root_sha256": str(
            source_attempt.get("input_root_sha256") or ""
        ).lower(),
        "child_process_started": False,
    }
    machine_output = json.dumps(
        marker,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    core = {
        key: value
        for key, value in dict(checkpoint).items()
        if key != "evidence_sha256"
    }
    core.update(
        {
            "run_uid": scheduler_run_uid,
            "started_at": now.replace(microsecond=0).isoformat(sep=" "),
            "validation_message": "idempotent completed daily stage replay",
            "machine_output_sha256": _text_sha256(machine_output),
            "idempotent_replay": marker,
        }
    )
    return {**core, "evidence_sha256": canonical_sha256(core)}


def _select_session(connection, session_uid: str, *, for_update: bool) -> dict[str, Any] | None:
    suffix = " FOR UPDATE" if for_update and _dialect_name(connection) != "sqlite" else ""
    return _one_mapping(
        connection.execute(
            text(
                f"SELECT * FROM {SESSION_TABLE} WHERE session_uid=:session_uid{suffix}"
            ),
            {"session_uid": session_uid},
        )
    )


def ensure_daily_delivery_session(
    connection,
    *,
    trade_date: object,
    release_id: object,
    strategy_release_id: object,
    now: datetime | None = None,
) -> dict[str, Any]:
    identity = daily_session_identity(trade_date, release_id)
    strategy_id = str(strategy_release_id or "").strip().lower()
    if SHA64_RE.fullmatch(strategy_id) is None:
        raise ValueError("strategy release identity is invalid")
    current = now or _control_now()
    params = {
        **identity,
        "strategy_release_id": strategy_id,
        "now": current,
    }
    if _dialect_name(connection) == "sqlite":
        statement = text(f"""
            INSERT OR IGNORE INTO {SESSION_TABLE}
                (session_uid, run_id, trade_date, release_id,
                 strategy_release_id, status, started_at, updated_at)
            VALUES (:session_uid, :run_id, :trade_date, :release_id,
                    :strategy_release_id, 'RUNNING', :now, :now)
        """)
    else:
        statement = text(f"""
            INSERT INTO {SESSION_TABLE}
                (session_uid, run_id, trade_date, release_id,
                 strategy_release_id, status, started_at, updated_at)
            VALUES (:session_uid, :run_id, :trade_date, :release_id,
                    :strategy_release_id, 'RUNNING', :now, :now)
            ON DUPLICATE KEY UPDATE session_uid=session_uid
        """)
    connection.execute(statement, params)
    session = _select_session(connection, identity["session_uid"], for_update=True)
    if session is None:
        raise RuntimeError("daily delivery session could not be created")
    if (
        str(session.get("run_id") or "") != identity["run_id"]
        or str(session.get("trade_date") or "")[:10] != identity["trade_date"]
        or str(session.get("release_id") or "").lower() != identity["release_id"]
        or str(session.get("strategy_release_id") or "").lower() != strategy_id
    ):
        raise RuntimeError("daily delivery session identity differs")
    return session


def load_daily_delivery_session(connection, session_uid: object) -> dict[str, Any]:
    normalized = str(session_uid or "").strip().lower()
    if SHA64_RE.fullmatch(normalized) is None:
        raise ValueError("daily delivery session identity is invalid")
    session = _select_session(connection, normalized, for_update=True)
    if session is None:
        raise RuntimeError("daily delivery session is unavailable")
    return session


def start_daily_stage_attempt(
    engine,
    *,
    scheduler_run_uid: object,
    stage_name: object,
    trade_date: object,
    release_id: object,
    strategy_release_id: object,
    lease_owner: object,
    lease_seconds: int = 90,
    shard_id: object = "main",
    input_root_sha256: object = None,
    reuse_completed_stage: bool = False,
) -> dict[str, Any]:
    run_uid = str(scheduler_run_uid or "").strip().lower()
    stage = str(stage_name or "").strip()[:64]
    shard = str(shard_id or "main").strip()[:128] or "main"
    owner = str(lease_owner or "").strip()[:128]
    input_root = str(input_root_sha256 or "").strip().lower() or None
    if RUN_UID_RE.fullmatch(run_uid) is None or not stage or not owner:
        raise ValueError("daily stage attempt identity is invalid")
    if input_root is not None and SHA64_RE.fullmatch(input_root) is None:
        raise ValueError("daily stage input root is invalid")
    current = _control_now()
    with engine.begin() as connection:
        session = ensure_daily_delivery_session(
            connection,
            trade_date=trade_date,
            release_id=release_id,
            strategy_release_id=strategy_release_id,
            now=current,
        )
        existing = _one_mapping(
            connection.execute(
                text(
                    f"SELECT * FROM {ATTEMPT_TABLE} "
                    "WHERE scheduler_run_uid=:scheduler_run_uid"
                ),
                {"scheduler_run_uid": run_uid},
            )
        )
        if existing is not None:
            if (
                str(existing.get("session_uid") or "") != session["session_uid"]
                or str(existing.get("stage_name") or "") != stage
                or str(existing.get("lease_owner") or "") != owner
            ):
                raise RuntimeError("daily stage scheduler identity differs")
            if (
                reuse_completed_stage
                and str(existing.get("status") or "") == "SUCCESS"
            ):
                checkpoint = _validated_completed_stage_checkpoint(
                    existing,
                    session=session,
                    stage_name=stage,
                )
                existing_input_root = str(
                    existing.get("input_root_sha256") or ""
                ).lower()
                if input_root is not None and input_root != existing_input_root:
                    raise DailyDeliveryFenceLost(
                        "completed daily stage input identity differs"
                    )
                replay_checkpoint = _completed_stage_replay_checkpoint(
                    checkpoint,
                    scheduler_run_uid=run_uid,
                    attempt_uid=str(existing.get("attempt_uid") or ""),
                    fencing_token=int(existing.get("fencing_token") or 0),
                    source_attempt=existing,
                    now=current,
                )
                return {
                    **existing,
                    "run_id": session["run_id"],
                    "trade_date": session["trade_date"],
                    "idempotent_replay": True,
                    "idempotent_replay_evidence": json.dumps(
                        replay_checkpoint,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "idempotent_source_attempt_uid": existing["attempt_uid"],
                    "idempotent_source_scheduler_run_uid": existing[
                        "scheduler_run_uid"
                    ],
                    "idempotent_source_fencing_token": int(
                        existing.get("fencing_token") or 0
                    ),
                }
            return existing
        if reuse_completed_stage:
            completed = _one_mapping(
                connection.execute(
                    text(f"""
                        SELECT * FROM {ATTEMPT_TABLE}
                        WHERE session_uid=:session_uid
                          AND stage_name=:stage_name
                          AND shard_id=:shard_id
                          AND status='SUCCESS'
                        ORDER BY fencing_token DESC LIMIT 1
                    """),
                    {
                        "session_uid": session["session_uid"],
                        "stage_name": stage,
                        "shard_id": shard,
                    },
                )
            )
            if completed is not None:
                checkpoint = _validated_completed_stage_checkpoint(
                    completed,
                    session=session,
                    stage_name=stage,
                )
                completed_input_root = str(
                    completed.get("input_root_sha256") or ""
                ).lower()
                if input_root is not None and input_root != completed_input_root:
                    raise DailyDeliveryFenceLost(
                        "completed daily stage input identity differs"
                    )
                counters = _one_mapping(
                    connection.execute(
                        text(f"""
                            SELECT COALESCE(MAX(attempt_no), 0) AS attempt_no
                            FROM {ATTEMPT_TABLE}
                            WHERE session_uid=:session_uid
                              AND stage_name=:stage_name
                              AND shard_id=:shard_id
                        """),
                        {
                            "session_uid": session["session_uid"],
                            "stage_name": stage,
                            "shard_id": shard,
                        },
                    )
                ) or {"attempt_no": 0}
                attempt_no = int(counters.get("attempt_no") or 0) + 1
                fencing_token = int(
                    session.get("latest_fencing_token") or 0
                ) + 1
                attempt_uid = canonical_sha256(
                    {
                        "session_uid": session["session_uid"],
                        "scheduler_run_uid": run_uid,
                        "stage_name": stage,
                        "shard_id": shard,
                        "attempt_no": attempt_no,
                        "fencing_token": fencing_token,
                    }
                )
                replay_checkpoint = _completed_stage_replay_checkpoint(
                    checkpoint,
                    scheduler_run_uid=run_uid,
                    attempt_uid=attempt_uid,
                    fencing_token=fencing_token,
                    source_attempt=completed,
                    now=current,
                )
                advanced = connection.execute(
                    text(f"""
                        UPDATE {SESSION_TABLE}
                        SET latest_fencing_token=:fencing_token,
                            status=CASE
                                WHEN status IN ('PASS','DEGRADED') THEN status
                                ELSE 'RUNNING'
                            END,
                            updated_at=:now
                        WHERE session_uid=:session_uid
                          AND latest_fencing_token=:previous_fencing_token
                    """),
                    {
                        "session_uid": session["session_uid"],
                        "fencing_token": fencing_token,
                        "previous_fencing_token": fencing_token - 1,
                        "now": current,
                    },
                )
                if int(getattr(advanced, "rowcount", 0) or 0) != 1:
                    raise DailyDeliveryFenceLost(
                        "daily stage idempotent replay lost its session fence"
                    )
                connection.execute(
                    text(f"""
                        INSERT INTO {ATTEMPT_TABLE}
                            (attempt_uid, session_uid, scheduler_run_uid,
                             stage_name, shard_id, attempt_no, status,
                             input_root_sha256, output_dataset_id, lease_owner,
                             lease_until, fencing_token, checkpoint_json,
                             started_at, finished_at)
                        VALUES
                            (:attempt_uid, :session_uid, :scheduler_run_uid,
                             :stage_name, :shard_id, :attempt_no, 'SUCCESS',
                             :input_root_sha256, :output_dataset_id, :lease_owner,
                             :lease_until, :fencing_token, :checkpoint_json,
                             :started_at, :finished_at)
                    """),
                    {
                        "attempt_uid": attempt_uid,
                        "session_uid": session["session_uid"],
                        "scheduler_run_uid": run_uid,
                        "stage_name": stage,
                        "shard_id": shard,
                        "attempt_no": attempt_no,
                        "input_root_sha256": completed_input_root,
                        "output_dataset_id": completed.get("output_dataset_id"),
                        "lease_owner": owner,
                        "lease_until": current,
                        "fencing_token": fencing_token,
                        "checkpoint_json": json.dumps(
                            replay_checkpoint,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        "started_at": current,
                        "finished_at": current,
                    },
                )
                replay = _one_mapping(
                    connection.execute(
                        text(
                            f"SELECT * FROM {ATTEMPT_TABLE} "
                            "WHERE attempt_uid=:attempt_uid"
                        ),
                        {"attempt_uid": attempt_uid},
                    )
                )
                if replay is None:
                    raise RuntimeError(
                        "daily stage idempotent replay could not be created"
                    )
                return {
                    **replay,
                    "run_id": session["run_id"],
                    "trade_date": session["trade_date"],
                    "idempotent_replay": True,
                    "idempotent_replay_evidence": json.dumps(
                        replay_checkpoint,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "idempotent_source_attempt_uid": completed["attempt_uid"],
                    "idempotent_source_scheduler_run_uid": completed[
                        "scheduler_run_uid"
                    ],
                    "idempotent_source_fencing_token": int(
                        completed.get("fencing_token") or 0
                    ),
                }
        active = _one_mapping(
            connection.execute(
                text(f"""
                    SELECT * FROM {ATTEMPT_TABLE}
                    WHERE session_uid=:session_uid AND stage_name=:stage_name
                      AND shard_id=:shard_id AND status='RUNNING'
                    ORDER BY fencing_token DESC LIMIT 1
                """),
                {
                    "session_uid": session["session_uid"],
                    "stage_name": stage,
                    "shard_id": shard,
                },
            )
        )
        if active is not None:
            active_lease_until = _datetime_value(active.get("lease_until"))
            if active_lease_until is not None and active_lease_until >= current:
                raise DailyDeliveryLeaseHeld(
                    "daily stage already has an unexpired lease: "
                    f"stage={stage} shard={shard} "
                    f"owner={active.get('lease_owner')} "
                    f"lease_until={active_lease_until.isoformat()}"
                )
            superseded = connection.execute(
                text(f"""
                    UPDATE {ATTEMPT_TABLE}
                    SET status='SUPERSEDED', finished_at=:finished_at,
                        error_code='LEASE_EXPIRED_TAKEOVER',
                        error_detail='expired lease superseded by a newer attempt'
                    WHERE attempt_uid=:attempt_uid AND status='RUNNING'
                      AND fencing_token=:fencing_token
                """),
                {
                    "attempt_uid": active["attempt_uid"],
                    "fencing_token": int(active.get("fencing_token") or 0),
                    "finished_at": current,
                },
            )
            if int(getattr(superseded, "rowcount", 0) or 0) != 1:
                raise DailyDeliveryFenceLost(
                    "expired daily stage lease changed during takeover"
                )
        counters = _one_mapping(
            connection.execute(
                text(f"""
                    SELECT COALESCE(MAX(attempt_no), 0) AS attempt_no
                    FROM {ATTEMPT_TABLE}
                    WHERE session_uid=:session_uid AND stage_name=:stage_name
                      AND shard_id=:shard_id
                """),
                {
                    "session_uid": session["session_uid"],
                    "stage_name": stage,
                    "shard_id": shard,
                },
            )
        ) or {"attempt_no": 0}
        attempt_no = int(counters.get("attempt_no") or 0) + 1
        fencing_token = int(session.get("latest_fencing_token") or 0) + 1
        attempt_uid = canonical_sha256(
            {
                "session_uid": session["session_uid"],
                "scheduler_run_uid": run_uid,
                "stage_name": stage,
                "shard_id": shard,
                "attempt_no": attempt_no,
                "fencing_token": fencing_token,
            }
        )
        connection.execute(
            text(f"""
                UPDATE {SESSION_TABLE}
                SET latest_fencing_token=:fencing_token,
                    status=CASE
                        WHEN status IN ('PASS','DEGRADED') THEN status
                        ELSE 'RUNNING'
                    END,
                    updated_at=:now
                WHERE session_uid=:session_uid
                  AND latest_fencing_token=:previous_fencing_token
            """),
            {
                "session_uid": session["session_uid"],
                "fencing_token": fencing_token,
                "previous_fencing_token": fencing_token - 1,
                "now": current,
            },
        )
        connection.execute(
            text(f"""
                INSERT INTO {ATTEMPT_TABLE}
                    (attempt_uid, session_uid, scheduler_run_uid, stage_name,
                     shard_id, attempt_no, status, input_root_sha256,
                     lease_owner, lease_until, fencing_token, started_at)
                VALUES
                    (:attempt_uid, :session_uid, :scheduler_run_uid, :stage_name,
                     :shard_id, :attempt_no, 'RUNNING', :input_root_sha256,
                     :lease_owner, :lease_until, :fencing_token, :started_at)
            """),
            {
                "attempt_uid": attempt_uid,
                "session_uid": session["session_uid"],
                "scheduler_run_uid": run_uid,
                "stage_name": stage,
                "shard_id": shard,
                "attempt_no": attempt_no,
                "input_root_sha256": input_root,
                "lease_owner": owner,
                "lease_until": current + timedelta(seconds=max(30, int(lease_seconds))),
                "fencing_token": fencing_token,
                "started_at": current,
            },
        )
        attempt = _one_mapping(
            connection.execute(
                text(f"SELECT * FROM {ATTEMPT_TABLE} WHERE attempt_uid=:attempt_uid"),
                {"attempt_uid": attempt_uid},
            )
        )
        if attempt is None:
            raise RuntimeError("daily stage attempt could not be created")
        return {**attempt, "run_id": session["run_id"], "trade_date": session["trade_date"]}


def renew_daily_stage_lease(
    engine,
    *,
    attempt_uid: object,
    fencing_token: int,
    lease_owner: object,
    lease_seconds: int = 90,
) -> bool:
    current = _control_now()
    with engine.begin() as connection:
        attempt = _one_mapping(
            connection.execute(
                text(
                    f"SELECT * FROM {ATTEMPT_TABLE} "
                    "WHERE attempt_uid=:attempt_uid"
                ),
                {"attempt_uid": str(attempt_uid or "")},
            )
        )
        if attempt is None:
            return False
        # Claim/takeover locks the same session first.  Serializing renewal on
        # that row makes the newest-token check and lease update one decision.
        if _select_session(
            connection,
            str(attempt.get("session_uid") or ""),
            for_update=True,
        ) is None:
            return False
        newest = connection.execute(
            text(f"""
                SELECT MAX(fencing_token) FROM {ATTEMPT_TABLE}
                WHERE session_uid=:session_uid AND stage_name=:stage_name
                  AND shard_id=:shard_id
            """),
            {
                "session_uid": attempt["session_uid"],
                "stage_name": attempt["stage_name"],
                "shard_id": attempt["shard_id"],
            },
        ).scalar()
        if int(newest or 0) != int(fencing_token):
            return False
        result = connection.execute(
            text(f"""
                UPDATE {ATTEMPT_TABLE}
                SET lease_until=:lease_until
                WHERE attempt_uid=:attempt_uid AND fencing_token=:fencing_token
                  AND lease_owner=:lease_owner AND status='RUNNING'
            """),
            {
                "attempt_uid": str(attempt_uid or ""),
                "fencing_token": int(fencing_token),
                "lease_owner": str(lease_owner or ""),
                "lease_until": current + timedelta(seconds=max(30, int(lease_seconds))),
            },
        )
    return int(getattr(result, "rowcount", 0) or 0) == 1


def _attempt_terminal_status(status: object) -> str:
    normalized = str(status or "failed").strip().lower()
    return {
        "success": "SUCCESS",
        "degraded": "DEGRADED",
        "blocked": "BLOCKED",
        "timeout": "TIMEOUT",
        "stopped": "STOPPED",
    }.get(normalized, "FAILED")


def finish_daily_stage_attempt(
    connection,
    *,
    scheduler_run_uid: object,
    status: object,
    output_dataset_id: object = None,
    input_root_sha256: object = None,
    error_code: object = None,
    error_detail: object = None,
    checkpoint: Mapping[str, object] | None = None,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """Fence and terminalize an attempt inside its publisher transaction."""

    run_uid = str(scheduler_run_uid or "").strip().lower()
    attempt = _one_mapping(
        connection.execute(
            text(
                f"SELECT * FROM {ATTEMPT_TABLE} "
                "WHERE scheduler_run_uid=:scheduler_run_uid"
            ),
            {"scheduler_run_uid": run_uid},
        )
    )
    if attempt is None:
        return None
    # Use the same session-first lock order as claim and renewal to avoid a
    # claim/finish deadlock while making the fence decision atomic.
    if _select_session(
        connection,
        str(attempt.get("session_uid") or ""),
        for_update=True,
    ) is None:
        raise RuntimeError("daily delivery session is unavailable")
    suffix = " FOR UPDATE" if _dialect_name(connection) != "sqlite" else ""
    attempt = _one_mapping(
        connection.execute(
            text(
                f"SELECT * FROM {ATTEMPT_TABLE} "
                f"WHERE scheduler_run_uid=:scheduler_run_uid{suffix}"
            ),
            {"scheduler_run_uid": run_uid},
        )
    )
    if attempt is None:
        return None
    if str(attempt.get("status") or "") != "RUNNING":
        if str(attempt.get("status") or "") in ATTEMPT_TERMINAL_STATUSES:
            return attempt
        raise RuntimeError("daily stage attempt status is invalid")
    current = now or _control_now()
    newest = connection.execute(
        text(f"""
            SELECT MAX(fencing_token) FROM {ATTEMPT_TABLE}
            WHERE session_uid=:session_uid AND stage_name=:stage_name
              AND shard_id=:shard_id
        """),
        {
            "session_uid": attempt["session_uid"],
            "stage_name": attempt["stage_name"],
            "shard_id": attempt["shard_id"],
        },
    ).scalar()
    owns_fence = int(newest or 0) == int(attempt.get("fencing_token") or 0)
    lease_until = _datetime_value(attempt.get("lease_until"))
    successful = str(status or "").strip().lower() in {"success", "degraded"}
    terminal_status = _attempt_terminal_status(status)
    if not owns_fence:
        terminal_status = "SUPERSEDED"
    elif successful and (not isinstance(lease_until, datetime) or lease_until < current):
        raise DailyDeliveryFenceLost("daily stage lease expired before publication")
    checkpoint_json = (
        json.dumps(
            dict(checkpoint),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        if checkpoint
        else None
    )
    result = connection.execute(
        text(f"""
            UPDATE {ATTEMPT_TABLE}
            SET status=:status, output_dataset_id=:output_dataset_id,
                input_root_sha256=COALESCE(:input_root_sha256, input_root_sha256),
                checkpoint_json=:checkpoint_json, error_code=:error_code,
                error_detail=:error_detail, finished_at=:finished_at
            WHERE attempt_uid=:attempt_uid AND status='RUNNING'
              AND fencing_token=:fencing_token
        """),
        {
            "attempt_uid": attempt["attempt_uid"],
            "fencing_token": int(attempt["fencing_token"]),
            "status": terminal_status,
            "output_dataset_id": str(output_dataset_id or "")[:128] or None,
            "input_root_sha256": str(input_root_sha256 or "").lower() or None,
            "checkpoint_json": checkpoint_json,
            "error_code": str(error_code or "")[:128] or None,
            "error_detail": str(error_detail or "")[:1000] or None,
            "finished_at": current,
        },
    )
    if int(getattr(result, "rowcount", 0) or 0) != 1:
        raise DailyDeliveryFenceLost("daily stage attempt terminalization lost its fence")
    return {**attempt, "status": terminal_status, "finished_at": current}


def build_terminal_delivery_receipt(
    *,
    session: Mapping[str, object],
    scheduler_run_uid: object,
    stage_name: object,
    status: str,
    strategy_release_id: object,
    legacy_receipt: Mapping[str, object] | None = None,
    degradations: list[Mapping[str, object]] | None = None,
    retryable: bool = False,
    error_code: object = None,
    error_detail: object = None,
) -> dict[str, object]:
    normalized_status = str(status or "").upper()
    if normalized_status not in TERMINAL_STATUSES:
        raise ValueError("daily delivery terminal status is invalid")
    strategy_id = str(strategy_release_id or "").strip().lower()
    if SHA64_RE.fullmatch(strategy_id) is None:
        raise ValueError("strategy release identity is invalid")
    if str(session.get("strategy_release_id") or "").lower() != strategy_id:
        raise RuntimeError("daily delivery strategy release identity differs")
    run_uid = str(scheduler_run_uid or "").strip().lower()
    if RUN_UID_RE.fullmatch(run_uid) is None:
        raise ValueError("scheduler run identity is invalid")
    stage = str(stage_name or "").strip()[:64]
    if not stage:
        raise ValueError("daily delivery blocking stage is invalid")
    legacy = dict(legacy_receipt or {})
    score_id = score_snapshot_identity(legacy) if normalized_status != "BLOCKED" else None
    core_inputs = _core_input_bindings(legacy)
    if normalized_status != "BLOCKED" and set(core_inputs) != {
        "kline",
        "turnover",
        "financial",
        "announcement",
    }:
        raise ValueError("daily delivery core input bindings are incomplete")
    normalized_degradations = [
        {
            str(key): value
            for key, value in sorted(dict(item).items())
        }
        for item in (degradations or [])
        if isinstance(item, Mapping)
    ]
    if normalized_status == "DEGRADED" and not normalized_degradations:
        raise ValueError("degraded daily delivery requires explicit degradations")
    if normalized_status == "PASS" and normalized_degradations:
        raise ValueError("pass daily delivery cannot contain degradations")
    core: dict[str, object] = {
        "schema": DELIVERY_RECEIPT_SCHEMA,
        "run_id": str(session.get("run_id") or ""),
        "session_uid": str(session.get("session_uid") or ""),
        "trade_date": _normalized_trade_date(session.get("trade_date")),
        "release_id": _normalized_release_id(session.get("release_id")),
        "strategy_release_id": strategy_id,
        "status": normalized_status,
        "delivery_mode": (
            str(legacy.get("status") or "").removeprefix("VERIFIED_")
            if legacy
            else "UNAVAILABLE"
        ),
        "scheduler_run_uid": run_uid,
        "terminal_stage": stage,
        "core_inputs": core_inputs,
        "feature_snapshot_id": (
            str(legacy.get("base_data_receipt_root_sha256") or "") or None
        ),
        "score_snapshot_id": score_id,
        "strategy_pool": {
            "status": legacy.get("strategy_pool_status"),
            "count": legacy.get("governance_tradable_count"),
            "root": legacy.get("governance_result_sha256"),
        },
        "formal_pool": {
            "status": legacy.get("ticket_pool_status"),
            "count": legacy.get("recommendation_count"),
            "executable_count": legacy.get("executable_count"),
            "root": legacy.get("canonical_pool_sha256"),
        },
        "analysis_run_uid": legacy.get("analysis_run_uid"),
        "governance_run_uid": legacy.get("governance_run_uid"),
        "canonical_batch_status": legacy.get("governance_status"),
        "api_checks": {
            "strategy_pool": (
                "PASS" if legacy.get("strategy_pool_api_verified") is True else None
            ),
            "formal_pool": (
                "PASS" if legacy.get("ticket_pool_api_verified") is True else None
            ),
        },
        "retryable": bool(retryable),
        "blocking_stage": stage if normalized_status == "BLOCKED" else None,
        "error_code": str(error_code or "")[:128] or None,
        "error_detail": str(error_detail or "")[:1000] or None,
        "degradations": normalized_degradations,
        "legacy_delivery_receipt_sha256": (
            legacy.get("delivery_receipt_sha256") if legacy else None
        ),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**core, "receipt_sha256": canonical_sha256(core)}


def persist_terminal_delivery_receipt(
    connection,
    *,
    receipt: Mapping[str, object],
    now: datetime | None = None,
) -> dict[str, object]:
    payload = dict(receipt)
    supplied_hash = str(payload.pop("receipt_sha256", "")).lower()
    if SHA64_RE.fullmatch(supplied_hash) is None or canonical_sha256(payload) != supplied_hash:
        raise ValueError("daily delivery receipt hash is invalid")
    status = str(payload.get("status") or "").upper()
    if status not in TERMINAL_STATUSES:
        raise ValueError("daily delivery receipt status is invalid")
    session_uid = str(payload.get("session_uid") or "")
    session = _select_session(connection, session_uid, for_update=True)
    if session is None:
        raise RuntimeError("daily delivery receipt session is unavailable")
    scheduler_run_uid = str(payload.get("scheduler_run_uid") or "").lower()
    if (
        str(payload.get("run_id") or "") != str(session.get("run_id") or "")
        or str(payload.get("trade_date") or "")[:10]
        != str(session.get("trade_date") or "")[:10]
        or str(payload.get("release_id") or "").lower()
        != str(session.get("release_id") or "").lower()
        or str(payload.get("strategy_release_id") or "").lower()
        != str(session.get("strategy_release_id") or "").lower()
    ):
        raise RuntimeError("daily delivery receipt session identity differs")
    attempt = _one_mapping(
        connection.execute(
            text(
                f"SELECT session_uid, status FROM {ATTEMPT_TABLE} "
                "WHERE scheduler_run_uid=:scheduler_run_uid"
            ),
            {"scheduler_run_uid": scheduler_run_uid},
        )
    )
    attempt_status = str((attempt or {}).get("status") or "")
    expected_attempt_statuses = (
        {"SUCCESS", "DEGRADED"}
        if status in {"PASS", "DEGRADED"}
        else {"BLOCKED", "FAILED", "TIMEOUT", "STOPPED"}
    )
    if (
        attempt is None
        or str(attempt.get("session_uid") or "") != session_uid
        or attempt_status not in expected_attempt_statuses
    ):
        raise RuntimeError("daily delivery receipt stage attempt differs")
    existing = _one_mapping(
        connection.execute(
            text(
                f"SELECT receipt_json, receipt_sha256 FROM {RECEIPT_TABLE} "
                "WHERE scheduler_run_uid=:scheduler_run_uid"
            ),
            {"scheduler_run_uid": scheduler_run_uid},
        )
    )
    if existing is not None:
        stored = json.loads(str(existing.get("receipt_json") or "{}"))
        stored_final_hash = str(stored.pop("receipt_sha256", "")).lower()
        if (
            str(existing.get("receipt_sha256") or "").lower()
            != stored_final_hash
            or SHA64_RE.fullmatch(stored_final_hash) is None
            or canonical_sha256(stored) != stored_final_hash
        ):
            raise RuntimeError("stored daily delivery receipt seal differs")
        source = {
            key: value
            for key, value in stored.items()
            if key not in {"generation", "receipt_uid"}
        }
        if canonical_sha256(source) != supplied_hash:
            raise RuntimeError("scheduler run already has a different delivery receipt")
        return {**stored, "receipt_sha256": stored_final_hash}
    generation = int(session.get("latest_generation") or 0) + 1
    receipt_uid = canonical_sha256(
        {
            "session_uid": session_uid,
            "generation": generation,
            "scheduler_run_uid": scheduler_run_uid,
            "receipt_sha256": supplied_hash,
        }
    )
    complete = {**payload, "generation": generation, "receipt_uid": receipt_uid}
    final_hash = canonical_sha256(complete)
    complete["receipt_sha256"] = final_hash
    encoded = json.dumps(
        complete,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    current = now or _control_now()
    formal_pool = complete.get("formal_pool")
    formal = dict(formal_pool) if isinstance(formal_pool, Mapping) else {}
    updated = connection.execute(
        text(f"""
            INSERT INTO {RECEIPT_TABLE}
                (receipt_uid, session_uid, generation, status,
                 scheduler_run_uid, stage_name, release_id,
                 strategy_release_id, analysis_run_uid, governance_run_uid,
                 score_snapshot_id, canonical_pool_sha256,
                 recommendation_count, retryable, blocking_stage, error_code,
                 error_detail, receipt_json, receipt_sha256, created_at)
            VALUES
                (:receipt_uid, :session_uid, :generation, :status,
                 :scheduler_run_uid, :stage_name, :release_id,
                 :strategy_release_id, :analysis_run_uid, :governance_run_uid,
                 :score_snapshot_id, :canonical_pool_sha256,
                 :recommendation_count, :retryable, :blocking_stage, :error_code,
                 :error_detail, :receipt_json, :receipt_sha256, :created_at)
        """),
        {
            "receipt_uid": receipt_uid,
            "session_uid": session_uid,
            "generation": generation,
            "status": status,
            "scheduler_run_uid": scheduler_run_uid,
            "stage_name": str(complete.get("terminal_stage") or "")[:64],
            "release_id": str(complete.get("release_id") or "").lower(),
            "strategy_release_id": str(
                complete.get("strategy_release_id") or ""
            ).lower(),
            "analysis_run_uid": str(complete.get("analysis_run_uid") or "") or None,
            "governance_run_uid": str(complete.get("governance_run_uid") or "") or None,
            "score_snapshot_id": str(complete.get("score_snapshot_id") or "") or None,
            "canonical_pool_sha256": str(formal.get("root") or "") or None,
            "recommendation_count": formal.get("count"),
            "retryable": 1 if complete.get("retryable") is True else 0,
            "blocking_stage": str(complete.get("blocking_stage") or "")[:64] or None,
            "error_code": str(complete.get("error_code") or "")[:128] or None,
            "error_detail": str(complete.get("error_detail") or "")[:1000] or None,
            "receipt_json": encoded,
            "receipt_sha256": final_hash,
            "created_at": current,
        },
    )
    connection.execute(
        text(f"""
            UPDATE {SESSION_TABLE}
            SET status=CASE
                    WHEN status='PASS' AND :status<>'PASS' THEN status
                    WHEN status='DEGRADED' AND :status='BLOCKED'
                    THEN status
                    ELSE :status
                END,
                latest_generation=:generation,
                canonical_receipt_uid=CASE
                    WHEN (status='PASS' AND :status<>'PASS')
                      OR (status='DEGRADED' AND :status='BLOCKED')
                    THEN canonical_receipt_uid ELSE :receipt_uid
                END,
                terminal_at=CASE
                    WHEN (status='PASS' AND :status<>'PASS')
                      OR (status='DEGRADED' AND :status='BLOCKED')
                    THEN terminal_at ELSE :terminal_at
                END,
                updated_at=:terminal_at
            WHERE session_uid=:session_uid
              AND latest_generation=:previous_generation
        """),
        {
            "status": status,
            "generation": generation,
            "previous_generation": generation - 1,
            "receipt_uid": receipt_uid,
            "terminal_at": current,
            "session_uid": session_uid,
        },
    )
    if int(getattr(updated, "rowcount", 0) or 0) != 1:
        raise RuntimeError("daily delivery session generation changed")
    return complete


def read_daily_delivery(
    engine,
    *,
    trade_date: object = "",
    release_id: object = "",
    attempt_limit: int = 20,
) -> dict[str, object] | None:
    """Read one materialized session, canonical receipt and recent attempts."""

    clauses: list[str] = []
    params: dict[str, object] = {"attempt_limit": max(1, min(100, int(attempt_limit)))}
    if str(trade_date or "").strip():
        params["trade_date"] = _normalized_trade_date(trade_date)
        clauses.append("trade_date=:trade_date")
    if str(release_id or "").strip():
        params["release_id"] = _normalized_release_id(release_id)
        clauses.append("release_id=:release_id")
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with engine.connect() as connection:
        session = _one_mapping(
            connection.execute(
                text(
                    f"SELECT * FROM {SESSION_TABLE}{where} "
                    "ORDER BY trade_date DESC, updated_at DESC, id DESC LIMIT 1"
                ),
                params,
            )
        )
        if session is None:
            return None
        receipt_row = _one_mapping(
            connection.execute(
                text(
                    f"SELECT receipt_json, receipt_sha256 FROM {RECEIPT_TABLE} "
                    "WHERE receipt_uid=:receipt_uid LIMIT 1"
                ),
                {"receipt_uid": session.get("canonical_receipt_uid")},
            )
        )
        attempts = [
            dict(row)
            for row in connection.execute(
                text(f"""
                    SELECT attempt_uid, scheduler_run_uid, stage_name, shard_id,
                           attempt_no, status, input_root_sha256,
                           output_dataset_id, lease_owner, lease_until,
                           fencing_token, error_code, error_detail,
                           started_at, finished_at
                    FROM {ATTEMPT_TABLE}
                    WHERE session_uid=:session_uid
                    ORDER BY fencing_token DESC LIMIT :attempt_limit
                """),
                {
                    "session_uid": session["session_uid"],
                    "attempt_limit": params["attempt_limit"],
                },
            ).mappings().all()
        ]
    receipt = None
    if receipt_row is not None:
        receipt = json.loads(str(receipt_row.get("receipt_json") or "{}"))
        supplied = str(receipt.pop("receipt_sha256", "")).lower()
        if (
            SHA64_RE.fullmatch(supplied) is None
            or supplied
            != str(receipt_row.get("receipt_sha256") or "").lower()
            or canonical_sha256(receipt) != supplied
            or str(receipt.get("receipt_uid") or "")
            != str(session.get("canonical_receipt_uid") or "")
            or str(receipt.get("session_uid") or "")
            != str(session.get("session_uid") or "")
            or str(receipt.get("run_id") or "")
            != str(session.get("run_id") or "")
            or str(receipt.get("release_id") or "").lower()
            != str(session.get("release_id") or "").lower()
            or str(receipt.get("strategy_release_id") or "").lower()
            != str(session.get("strategy_release_id") or "").lower()
            or str(receipt.get("trade_date") or "")[:10]
            != str(session.get("trade_date") or "")[:10]
            or (
                str(session.get("status") or "") in TERMINAL_STATUSES
                and str(receipt.get("status") or "")
                != str(session.get("status") or "")
            )
        ):
            raise RuntimeError("materialized daily delivery receipt seal differs")
        receipt["receipt_sha256"] = supplied
    return {"session": session, "receipt": receipt, "attempts": attempts}


__all__ = [
    "ATTEMPT_TABLE",
    "DELIVERY_RECEIPT_SCHEMA",
    "DailyDeliveryFenceLost",
    "DailyDeliveryLeaseHeld",
    "RECEIPT_TABLE",
    "SESSION_TABLE",
    "build_terminal_delivery_receipt",
    "canonical_sha256",
    "daily_session_identity",
    "ensure_daily_delivery_session",
    "finish_daily_stage_attempt",
    "load_daily_delivery_session",
    "persist_terminal_delivery_receipt",
    "privileged_migrate_daily_delivery_schema",
    "read_daily_delivery",
    "renew_daily_stage_lease",
    "score_snapshot_identity",
    "start_daily_stage_attempt",
    "strategy_release_identity",
    "validate_daily_delivery_runtime_schema",
]

"""Immutable, research-only intake for external quantitative strategy studies.

The intake boundary is deliberately separate from the executable strategy
registry.  A paper or public repository can become a reproducible research
candidate here, but it cannot acquire simulation or real-order authority.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence

from sqlalchemy import text

from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)


BATCH_TABLE = "st_quant_research_batch"
CANDIDATE_TABLE = "st_quant_research_candidate"
BATCH_SCHEMA = "probiga.quant-research-batch.v1"
CANDIDATE_SCHEMA = "probiga.quant-research-candidate.v1"
RESEARCH_ONLY = "RESEARCH_ONLY"
MAX_BATCH_BYTES = 1_048_576
MAX_METRICS_BYTES = 16_384


class QuantResearchIntakeError(RuntimeError):
    """A terminal failure naming the exact intake stage."""

    def __init__(self, stage: str, reason: str) -> None:
        self.stage = stage
        super().__init__(f"{stage}:{reason}")


EVIDENCE_STAGES = frozenset({
    "HISTORICAL_BACKTEST",
    "SIMULATION",
    "LIVE_VERIFIED",
})
REPRODUCIBILITY_STATUSES = frozenset({
    "NOT_REPRODUCED",
    "PARTIALLY_REPRODUCED",
    "INDEPENDENTLY_REPRODUCED",
})
QMT_FEASIBILITY = frozenset({
    "DIRECT",
    "ADAPTATION_REQUIRED",
    "NOT_FEASIBLE",
})
_URL_RE = re.compile(r"^https://", re.IGNORECASE)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SHANGHAI_TZ = timezone(timedelta(hours=8))


_DDL = (
    f"""
    CREATE TABLE IF NOT EXISTS {BATCH_TABLE} (
        batch_id CHAR(64) NOT NULL PRIMARY KEY,
        batch_schema VARCHAR(80) NOT NULL,
        source_task_id VARCHAR(120) NOT NULL,
        published_at DATETIME(6) NOT NULL,
        candidate_count INT NOT NULL,
        status VARCHAR(24) NOT NULL,
        automatic_real_order_submission TINYINT(1) NOT NULL DEFAULT 0,
        real_order_authority TINYINT(1) NOT NULL DEFAULT 0,
        payload_json LONGTEXT NOT NULL,
        payload_hash CHAR(64) NOT NULL,
        created_at DATETIME(6) NOT NULL,
        UNIQUE KEY uk_quant_research_batch_payload (payload_hash),
        KEY idx_quant_research_batch_latest (published_at, status),
        CONSTRAINT chk_quant_research_batch_status
            CHECK (status = 'RESEARCH_ONLY'),
        CONSTRAINT chk_quant_research_batch_no_order
            CHECK (automatic_real_order_submission = 0 AND real_order_authority = 0),
        CONSTRAINT chk_quant_research_batch_count
            CHECK (candidate_count BETWEEN 3 AND 5)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {CANDIDATE_TABLE} (
        candidate_id CHAR(64) NOT NULL PRIMARY KEY,
        batch_id CHAR(64) NOT NULL,
        rank_no INT NOT NULL,
        title VARCHAR(512) NOT NULL,
        source_url VARCHAR(2048) NOT NULL,
        source_published_date DATE NOT NULL,
        strategy_family VARCHAR(80) NOT NULL,
        market_scope VARCHAR(160) NOT NULL,
        evidence_stage VARCHAR(32) NOT NULL,
        reproducibility_status VARCHAR(40) NOT NULL,
        qmt_feasibility VARCHAR(32) NOT NULL,
        priority INT NOT NULL,
        status VARCHAR(24) NOT NULL,
        automatic_real_order_submission TINYINT(1) NOT NULL DEFAULT 0,
        real_order_authority TINYINT(1) NOT NULL DEFAULT 0,
        candidate_json LONGTEXT NOT NULL,
        candidate_hash CHAR(64) NOT NULL,
        created_at DATETIME(6) NOT NULL,
        UNIQUE KEY uk_quant_research_batch_rank (batch_id, rank_no),
        KEY idx_quant_research_candidate_stage
            (evidence_stage, reproducibility_status, qmt_feasibility),
        CONSTRAINT fk_quant_research_candidate_batch
            FOREIGN KEY (batch_id) REFERENCES {BATCH_TABLE}(batch_id),
        CONSTRAINT chk_quant_research_candidate_status
            CHECK (status = 'RESEARCH_ONLY'),
        CONSTRAINT chk_quant_research_candidate_no_order
            CHECK (automatic_real_order_submission = 0 AND real_order_authority = 0),
        CONSTRAINT chk_quant_research_candidate_priority
            CHECK (priority BETWEEN 1 AND 5)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)


QUANT_RESEARCH_SCHEMA = {
    BATCH_TABLE: RuntimeTable(
        columns={
            "batch_id": RuntimeColumn("char", False, character_length=64),
            "batch_schema": RuntimeColumn("varchar", False, character_length=80),
            "source_task_id": RuntimeColumn("varchar", False, character_length=120),
            "published_at": RuntimeColumn("datetime", False, datetime_precision=6),
            "candidate_count": RuntimeColumn("int", False),
            "status": RuntimeColumn("varchar", False, character_length=24),
            "automatic_real_order_submission": RuntimeColumn("tinyint", False),
            "real_order_authority": RuntimeColumn("tinyint", False),
            "payload_json": RuntimeColumn("longtext", False),
            "payload_hash": RuntimeColumn("char", False, character_length=64),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=6),
        },
        indexes=(
            RuntimeIndex(("batch_id",), unique=True),
            RuntimeIndex(("payload_hash",), unique=True),
            RuntimeIndex(("published_at", "status")),
        ),
    ),
    CANDIDATE_TABLE: RuntimeTable(
        columns={
            "candidate_id": RuntimeColumn("char", False, character_length=64),
            "batch_id": RuntimeColumn("char", False, character_length=64),
            "rank_no": RuntimeColumn("int", False),
            "title": RuntimeColumn("varchar", False, character_length=512),
            "source_url": RuntimeColumn("varchar", False, character_length=2048),
            "source_published_date": RuntimeColumn("date", False),
            "strategy_family": RuntimeColumn("varchar", False, character_length=80),
            "market_scope": RuntimeColumn("varchar", False, character_length=160),
            "evidence_stage": RuntimeColumn("varchar", False, character_length=32),
            "reproducibility_status": RuntimeColumn("varchar", False, character_length=40),
            "qmt_feasibility": RuntimeColumn("varchar", False, character_length=32),
            "priority": RuntimeColumn("int", False),
            "status": RuntimeColumn("varchar", False, character_length=24),
            "automatic_real_order_submission": RuntimeColumn("tinyint", False),
            "real_order_authority": RuntimeColumn("tinyint", False),
            "candidate_json": RuntimeColumn("longtext", False),
            "candidate_hash": RuntimeColumn("char", False, character_length=64),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=6),
        },
        indexes=(
            RuntimeIndex(("candidate_id",), unique=True),
            RuntimeIndex(("batch_id", "rank_no"), unique=True),
            RuntimeIndex(("evidence_stage", "reproducibility_status", "qmt_feasibility")),
        ),
    ),
}


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _required_text(item: Mapping[str, Any], key: str, limit: int) -> str:
    value = str(item.get(key) or "").strip()
    if not value or len(value) > limit:
        raise ValueError(f"quant research candidate {key} is missing or too long")
    return value


def _parse_datetime(value: Any, field: str) -> datetime:
    raw = str(value or "").strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO datetime") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI_TZ).replace(tzinfo=None)
    return parsed


def _parse_date(value: Any, field: str) -> date:
    raw = str(value or "").strip()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{field} must be YYYY-MM-DD") from exc
    if parsed.isoformat() != raw:
        raise ValueError(f"{field} must be YYYY-MM-DD")
    return parsed


def normalize_quant_research_batch(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and hash one 3-5 item research-only weekly intake batch."""

    if not isinstance(payload, Mapping):
        raise TypeError("quant research payload must be an object")
    if len(_canonical_json(payload).encode("utf-8")) > MAX_BATCH_BYTES:
        raise ValueError("quant research input exceeds byte limit")
    if payload.get("automatic_real_order_submission") not in (None, False, 0):
        raise ValueError("quant research cannot submit real orders")
    if payload.get("real_order_authority") not in (None, False, 0):
        raise ValueError("quant research cannot hold real-order authority")
    source_task_id = _required_text(payload, "source_task_id", 120)
    published_at = _parse_datetime(payload.get("published_at"), "published_at")
    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, Sequence) or isinstance(raw_candidates, (str, bytes)):
        raise ValueError("quant research candidates must be a list")
    if not 3 <= len(raw_candidates) <= 5:
        raise ValueError("quant research batch must contain 3 to 5 candidates")

    candidates: list[dict[str, Any]] = []
    identities: set[tuple[str, str]] = set()
    for index, raw in enumerate(raw_candidates, 1):
        if not isinstance(raw, Mapping):
            raise ValueError("quant research candidate must be an object")
        title = _required_text(raw, "title", 512)
        source_url = _required_text(raw, "source_url", 2048)
        if _URL_RE.match(source_url) is None:
            raise ValueError("quant research source_url must use https")
        public_code_url = str(raw.get("public_code_url") or "").strip()
        if public_code_url and _URL_RE.match(public_code_url) is None:
            raise ValueError("quant research public_code_url must use https")
        source_published_date = _parse_date(
            raw.get("source_published_date"), "source_published_date"
        )
        evidence_stage = _required_text(raw, "evidence_stage", 32).upper()
        reproducibility = _required_text(
            raw, "reproducibility_status", 40
        ).upper()
        feasibility = _required_text(raw, "qmt_feasibility", 32).upper()
        if evidence_stage not in EVIDENCE_STAGES:
            raise ValueError("quant research evidence_stage is invalid")
        if reproducibility not in REPRODUCIBILITY_STATUSES:
            raise ValueError("quant research reproducibility_status is invalid")
        if feasibility not in QMT_FEASIBILITY:
            raise ValueError("quant research qmt_feasibility is invalid")
        try:
            priority = int(raw.get("priority", index))
        except (TypeError, ValueError) as exc:
            raise ValueError("quant research priority must be an integer") from exc
        if not 1 <= priority <= 5:
            raise ValueError("quant research priority must be 1..5")
        metrics = raw.get("performance_metrics")
        if not isinstance(metrics, Mapping) or not metrics:
            raise ValueError("quant research performance_metrics is required")
        if len(_canonical_json(metrics).encode("utf-8")) > MAX_METRICS_BYTES:
            raise ValueError("quant research performance_metrics exceeds byte limit")
        candidate = {
            "schema": CANDIDATE_SCHEMA,
            "rank_no": index,
            "title": title,
            "source_url": source_url,
            "source_published_date": source_published_date.isoformat(),
            "strategy_family": _required_text(raw, "strategy_family", 80),
            "market_scope": _required_text(raw, "market_scope", 160),
            "strategy_logic": _required_text(raw, "strategy_logic", 4000),
            "evidence_stage": evidence_stage,
            "reproducibility_status": reproducibility,
            "qmt_feasibility": feasibility,
            "qmt_feasibility_note": _required_text(
                raw, "qmt_feasibility_note", 2000
            ),
            "performance_metrics": dict(metrics),
            "transaction_costs": _required_text(raw, "transaction_costs", 2000),
            "biases_and_limitations": _required_text(
                raw, "biases_and_limitations", 3000
            ),
            "public_code_url": public_code_url,
            "priority": priority,
            "status": RESEARCH_ONLY,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        identity = (title.casefold(), source_url.casefold())
        if identity in identities:
            raise ValueError("quant research candidate identity is duplicated")
        identities.add(identity)
        candidate_hash = _digest(candidate)
        candidates.append({
            **candidate,
            "candidate_hash": candidate_hash,
        })

    core = {
        "schema": BATCH_SCHEMA,
        "source_task_id": source_task_id,
        "published_at": published_at.isoformat(timespec="microseconds"),
        "candidate_count": len(candidates),
        "status": RESEARCH_ONLY,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "candidates": candidates,
    }
    payload_hash = _digest(core)
    batch_id = _digest({
        "schema": "probiga.quant-research-batch-id.v1",
        "payload_hash": payload_hash,
    })
    # Derived row identities are excluded from the content hash to avoid a
    # circular dependency.  Each immutable batch owns its candidate records,
    # even when a later week contains the same study and evidence.
    candidates = [{
        **candidate,
        "candidate_id": _digest({
            "schema": "probiga.quant-research-candidate-id.v1",
            "batch_id": batch_id,
            "candidate_hash": candidate["candidate_hash"],
        }),
    } for candidate in candidates]
    return {
        **core,
        "candidates": candidates,
        "payload_hash": payload_hash,
        "batch_id": batch_id,
    }


def privileged_migrate_quant_research_tables(engine: Any) -> None:
    """Create the research-only ledger during the fenced release window."""

    with engine.begin() as connection:
        for statement in _DDL:
            connection.execute(text(statement))
        privileged_normalize_mysql_storage(connection, QUANT_RESEARCH_SCHEMA)
        validate_quant_research_runtime(engine, connection=connection)


def validate_quant_research_runtime(engine: Any, *, connection=None) -> None:
    """Validate the physical ledger without mutating runtime schema."""

    validate_runtime_tables(
        engine,
        QUANT_RESEARCH_SCHEMA,
        context="quant_research",
        connection=connection,
    )


def _assert_exact_quant_research_readback(
    connection: Any, batch: Mapping[str, Any],
    *, check_stage: Callable[[str], None] | None = None,
) -> None:
    """Prove the complete canonical batch and all its indexed row metadata."""

    params = {"batch_id": batch["batch_id"]}
    if check_stage is not None:
        check_stage("database_batch_readback")
    stored_batch = connection.execute(
        text(f"SELECT * FROM {BATCH_TABLE} WHERE batch_id=:batch_id"), params,
    ).mappings().first()
    if stored_batch is None:
        raise RuntimeError("quant research batch database readback missing")
    expected_batch_fields = {
        "batch_id": batch["batch_id"],
        "batch_schema": batch["schema"],
        "source_task_id": batch["source_task_id"],
        "payload_hash": batch["payload_hash"],
        "status": RESEARCH_ONLY,
    }
    try:
        batch_matches = (
            all(stored_batch[key] == value for key, value in expected_batch_fields.items())
            and int(stored_batch["candidate_count"]) == batch["candidate_count"]
            and int(stored_batch["automatic_real_order_submission"]) == 0
            and int(stored_batch["real_order_authority"]) == 0
            and _parse_datetime(stored_batch["published_at"], "published_at")
            == datetime.fromisoformat(batch["published_at"])
            and _canonical_json(json.loads(stored_batch["payload_json"]))
            == _canonical_json(batch)
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("quant research batch database readback invalid") from exc
    if not batch_matches:
        raise RuntimeError("quant research batch database readback differs")

    if check_stage is not None:
        check_stage("database_candidate_readback")
    rows = connection.execute(
        text(f"SELECT * FROM {CANDIDATE_TABLE} "
             "WHERE batch_id=:batch_id ORDER BY rank_no LIMIT 6"), params,
    ).mappings().all()
    if len(rows) != batch["candidate_count"]:
        raise RuntimeError("quant research candidate database readback count differs")
    candidate_fields = (
        "candidate_id", "candidate_hash", "title", "source_url",
        "strategy_family", "market_scope", "evidence_stage",
        "reproducibility_status", "qmt_feasibility", "status",
    )
    for row, candidate in zip(rows, batch["candidates"]):
        try:
            matches = (
                row["batch_id"] == batch["batch_id"]
                and all(row[key] == candidate[key] for key in candidate_fields)
                and int(row["rank_no"]) == candidate["rank_no"]
                and int(row["priority"]) == candidate["priority"]
                and int(row["automatic_real_order_submission"]) == 0
                and int(row["real_order_authority"]) == 0
                and _parse_date(row["source_published_date"], "source_published_date")
                == date.fromisoformat(candidate["source_published_date"])
                and _canonical_json(json.loads(row["candidate_json"]))
                == _canonical_json(candidate)
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("quant research candidate database readback invalid") from exc
        if not matches:
            raise RuntimeError("quant research candidate database readback differs")


def persist_quant_research_batch(
    engine: Any,
    payload: Mapping[str, Any],
    *,
    created_at: datetime | None = None,
    deadline_seconds: float = 300,
    monotonic_fn: Callable[[], float] = time.monotonic,
    stage_fn: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Append one validated batch and return its exact database readback."""

    budget = float(deadline_seconds)
    if not 0 < budget <= 300:
        raise ValueError("quant research deadline_seconds must be positive and at most 300")
    deadline = monotonic_fn() + budget

    def check_stage(stage: str) -> None:
        if stage_fn is not None:
            stage_fn(stage)
        if monotonic_fn() >= deadline:
            raise QuantResearchIntakeError(stage, "INTAKE_DEADLINE_EXPIRED")

    check_stage("input_validation")
    batch = normalize_quant_research_batch(payload)
    check_stage("schema_validation")
    validate_quant_research_runtime(engine)
    created = (created_at or datetime.now()).replace(tzinfo=None)
    payload_json = _canonical_json(batch)
    check_stage("database_transaction")
    with engine.begin() as connection:
        check_stage("database_lookup")
        existing = connection.execute(
            text(
                f"SELECT payload_hash, candidate_count FROM {BATCH_TABLE} "
                "WHERE batch_id=:batch_id"
            ),
            {"batch_id": batch["batch_id"]},
        ).mappings().first()
        if existing is None:
            check_stage("database_batch_append")
            connection.execute(
                text(f"""
                    INSERT INTO {BATCH_TABLE} (
                        batch_id, batch_schema, source_task_id, published_at,
                        candidate_count, status,
                        automatic_real_order_submission, real_order_authority,
                        payload_json, payload_hash, created_at
                    ) VALUES (
                        :batch_id, :batch_schema, :source_task_id, :published_at,
                        :candidate_count, :status, 0, 0,
                        :payload_json, :payload_hash, :created_at
                    )
                """),
                {
                    "batch_id": batch["batch_id"],
                    "batch_schema": BATCH_SCHEMA,
                    "source_task_id": batch["source_task_id"],
                    "published_at": datetime.fromisoformat(batch["published_at"]),
                    "candidate_count": batch["candidate_count"],
                    "status": RESEARCH_ONLY,
                    "payload_json": payload_json,
                    "payload_hash": batch["payload_hash"],
                    "created_at": created,
                },
            )
            for candidate in batch["candidates"]:
                check_stage("database_candidate_append")
                connection.execute(
                    text(f"""
                        INSERT INTO {CANDIDATE_TABLE} (
                            candidate_id, batch_id, rank_no, title, source_url,
                            source_published_date, strategy_family, market_scope,
                            evidence_stage, reproducibility_status,
                            qmt_feasibility, priority, status,
                            automatic_real_order_submission, real_order_authority,
                            candidate_json, candidate_hash, created_at
                        ) VALUES (
                            :candidate_id, :batch_id, :rank_no, :title, :source_url,
                            :source_published_date, :strategy_family, :market_scope,
                            :evidence_stage, :reproducibility_status,
                            :qmt_feasibility, :priority, :status, 0, 0,
                            :candidate_json, :candidate_hash, :created_at
                        )
                    """),
                    {
                        **candidate,
                        "batch_id": batch["batch_id"],
                        "source_published_date": date.fromisoformat(
                            candidate["source_published_date"]
                        ),
                        "candidate_json": _canonical_json(candidate),
                        "created_at": created,
                    },
                )
            idempotent = False
        else:
            if (
                str(existing.get("payload_hash") or "") != batch["payload_hash"]
                or int(existing.get("candidate_count") or 0)
                != batch["candidate_count"]
            ):
                raise RuntimeError("quant research batch identity collision")
            idempotent = True

        _assert_exact_quant_research_readback(connection, batch, check_stage=check_stage)
        check_stage("database_commit")
    # A successful uncommitted SELECT is not durable completion.  Verify again
    # after commit, through a new connection, before issuing the receipt.
    check_stage("durable_readback")
    with engine.connect() as connection:
        _assert_exact_quant_research_readback(connection, batch, check_stage=check_stage)
    check_stage("completion_receipt")
    return {
        "status": "COMPLETE",
        "batch_id": batch["batch_id"],
        "payload_hash": batch["payload_hash"],
        "candidate_count": batch["candidate_count"],
        "research_only": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "idempotent": idempotent,
        "readback_verified": True,
    }


def load_quant_research_candidates(
    engine: Any,
    *,
    as_of: date | str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Read recent candidates while preserving their research-only boundary."""

    validate_quant_research_runtime(engine)
    bounded_limit = max(1, min(100, int(limit)))
    cutoff = (
        date.fromisoformat(str(as_of)[:10])
        if as_of is not None
        else date.max
    )
    with engine.connect() as connection:
        rows = connection.execute(
            text(f"""
                SELECT c.candidate_json, c.candidate_hash, c.candidate_id,
                       c.batch_id, b.published_at, b.source_task_id, b.payload_json
                  FROM {CANDIDATE_TABLE} c
                  JOIN {BATCH_TABLE} b ON b.batch_id=c.batch_id
                 WHERE c.status='RESEARCH_ONLY'
                   AND c.automatic_real_order_submission=0
                   AND c.real_order_authority=0
                   AND b.status='RESEARCH_ONLY'
                   AND b.automatic_real_order_submission=0
                   AND b.real_order_authority=0
                   AND DATE(b.published_at) <= :cutoff
                 ORDER BY b.published_at DESC, c.rank_no ASC
                 LIMIT :limit
            """),
            {"cutoff": cutoff, "limit": bounded_limit},
        ).mappings().all()
        verified_batches = set()
        for row in rows:
            batch_id = str(row["batch_id"] or "")
            if batch_id in verified_batches:
                continue
            try:
                stored = json.loads(row["payload_json"])
                expected = normalize_quant_research_batch(stored)
            except (TypeError, ValueError, KeyError) as exc:
                raise RuntimeError("quant research batch integrity check failed") from exc
            if expected["batch_id"] != batch_id:
                raise RuntimeError("quant research batch identity check failed")
            _assert_exact_quant_research_readback(connection, expected)
            verified_batches.add(batch_id)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        candidate = json.loads(str(row.get("candidate_json") or "{}"))
        candidate_hash = str(row.get("candidate_hash") or "")
        unsigned = {
            key: value for key, value in candidate.items()
            if key not in {"candidate_hash", "candidate_id"}
        }
        if (
            not isinstance(candidate, dict)
            or not _SHA256_RE.fullmatch(candidate_hash)
            or _digest(unsigned) != candidate_hash
            or candidate.get("candidate_hash") != candidate_hash
            or candidate.get("candidate_id") != row["candidate_id"]
            or candidate.get("candidate_id") != _digest({
                "schema": "probiga.quant-research-candidate-id.v1",
                "batch_id": row["batch_id"],
                "candidate_hash": candidate_hash,
            })
            or candidate.get("status") != RESEARCH_ONLY
            or candidate.get("automatic_real_order_submission") is not False
            or candidate.get("real_order_authority") is not False
        ):
            raise RuntimeError("quant research candidate integrity check failed")
        candidates.append({
            **candidate,
            "batch_id": str(row.get("batch_id") or ""),
            "batch_published_at": str(row.get("published_at") or ""),
            "source_task_id": str(row.get("source_task_id") or ""),
        })
    return {
        "status": "AVAILABLE",
        "candidate_count": len(candidates),
        "candidates": candidates,
        "research_only": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


__all__ = [
    "BATCH_SCHEMA",
    "BATCH_TABLE",
    "CANDIDATE_SCHEMA",
    "CANDIDATE_TABLE",
    "MAX_BATCH_BYTES",
    "MAX_METRICS_BYTES",
    "QuantResearchIntakeError",
    "QUANT_RESEARCH_SCHEMA",
    "RESEARCH_ONLY",
    "load_quant_research_candidates",
    "normalize_quant_research_batch",
    "persist_quant_research_batch",
    "privileged_migrate_quant_research_tables",
    "validate_quant_research_runtime",
]

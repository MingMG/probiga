# -*- coding: utf-8 -*-
"""Immutable daily concept and industry membership snapshots from BigQMT."""
from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from datetime import date, datetime, time, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from integrations.bigqmt.reference import PROVIDER_ID
from server.common.auxiliary_runtime_schema import (
    privileged_migrate_qmt_membership_snapshot_schema,
    validate_qmt_membership_snapshot_runtime_schema,
)
from server.common.batch_db import write_frame


MIN_CONCEPT_COUNT = 500
MIN_CONCEPT_RELATION_COUNT = 30_000
MIN_CONCEPT_STOCK_COUNT = 3_000
MIN_INDUSTRY_RELATION_COUNT = 5_000
MIN_INDUSTRY_STOCK_COUNT = 4_500


def _canonical_hash(rows: list[tuple[str, ...]]) -> str:
    payload = json.dumps(
        sorted(rows),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_membership_snapshot_tables(engine: Engine) -> dict[str, Any]:
    """Compatibility alias for the read-only runtime schema guard."""

    # The scheduled QMT publisher connects with the least-privilege DML
    # identity.  MySQL hides information_schema.TRIGGERS from an account that
    # does not hold the dangerous TRIGGER privilege, even when the immutable
    # triggers are installed and actively enforced.  The fenced release
    # broker owns the privileged six-trigger attestation; runtime publication
    # therefore verifies the complete table surface without falsely treating
    # an invisible trigger inventory as a missing one.
    return validate_qmt_membership_snapshot_runtime_schema(
        engine,
        require_triggers=False,
    )


def privileged_migrate_membership_snapshot_tables(engine: Engine) -> dict[str, Any]:
    """Create the immutable snapshot tables during a fenced release only."""

    return privileged_migrate_qmt_membership_snapshot_schema(engine)


def _snapshot_datetime(value: Any, *, field: str) -> datetime:
    try:
        timestamp = pd.Timestamp(value)
        if pd.isna(timestamp):
            raise ValueError("empty timestamp")
        parsed = timestamp.to_pydatetime()
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"QMT membership {field} is invalid") from exc
    if parsed.tzinfo is not None:
        raise RuntimeError(f"QMT membership {field} must be a naive local time")
    return parsed.replace(microsecond=0)


def verify_existing_membership_snapshot(
    engine: Engine | Connection,
    *,
    snapshot_date: date,
    decision_known_at: datetime | None = None,
    source: str = PROVIDER_ID,
) -> dict[str, Any]:
    """Read back one immutable, pre-cutoff QMT membership snapshot.

    This verifier never calls QMT and never writes.  It exists so a new code
    release can attest a snapshot captured by the prior active build without
    relabelling today's mutable reference data as a historical partition.
    """

    if isinstance(snapshot_date, datetime):
        raise RuntimeError("QMT membership snapshot date must not include a time")
    target = (
        snapshot_date
        if isinstance(snapshot_date, date)
        else date.fromisoformat(str(snapshot_date))
    )
    known_at = (decision_known_at or datetime.now()).replace(microsecond=0)
    if known_at.tzinfo is not None:
        raise RuntimeError("QMT membership decision time must be a naive local time")
    close_ready_at = datetime.combine(target, time(15, 10))
    knowledge_cutoff = datetime.combine(target + timedelta(days=1), time.min)
    if known_at < close_ready_at:
        raise RuntimeError("QMT membership target session is not closed")

    if isinstance(engine, Connection):
        connection_context = nullcontext(engine)
    else:
        ensure_membership_snapshot_tables(engine)
        connection_context = engine.connect()
    with connection_context as connection:
        open_count = int(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM si_trade_calendar "
                    "WHERE trade_date=:snapshot_date AND trade_status=1"
                ),
                {"snapshot_date": target},
            ).scalar()
            or 0
        )
        runs = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT snapshot_date, source, quality_status, capture_mode, "
                    "concept_count, concept_relation_count, industry_count, "
                    "industry_relation_count, concept_hash, industry_hash, captured_at "
                    "FROM qmt_membership_snapshot_run "
                    "WHERE snapshot_date=:snapshot_date AND source=:source"
                ),
                {"snapshot_date": target, "source": source},
            ).mappings().all()
        ]
        concept_rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT concept_code, concept_name, stock_code, short_name, "
                    "quality_status, captured_at FROM qmt_concept_member_snapshot "
                    "WHERE snapshot_date=:snapshot_date AND source=:source "
                    "ORDER BY concept_code, stock_code"
                ),
                {"snapshot_date": target, "source": source},
            ).mappings().all()
        ]
        industry_rows = [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT industry_code, industry_name, industry_type, stock_code, "
                    "short_name, quality_status, captured_at "
                    "FROM qmt_industry_member_snapshot "
                    "WHERE snapshot_date=:snapshot_date AND source=:source "
                    "ORDER BY industry_code, stock_code"
                ),
                {"snapshot_date": target, "source": source},
            ).mappings().all()
        ]

    if open_count != 1 or len(runs) != 1:
        raise RuntimeError("QMT membership exact target snapshot is unavailable")
    run = runs[0]
    captured_at = _snapshot_datetime(run.get("captured_at"), field="captured_at")
    if (
        str(run.get("snapshot_date"))[:10] != target.isoformat()
        or run.get("source") != source
        or run.get("quality_status") != "QMT_VALIDATED"
        or run.get("capture_mode") != "qmt_close_full_refresh"
        or captured_at < close_ready_at
        or captured_at >= knowledge_cutoff
        or captured_at > known_at
    ):
        raise RuntimeError("QMT membership snapshot provenance is not target-date PIT")

    concept_identities: list[tuple[str, ...]] = []
    industry_identities: list[tuple[str, ...]] = []
    concept_keys: set[tuple[str, str]] = set()
    industry_keys: set[tuple[str, str]] = set()
    concept_stocks: set[str] = set()
    industry_stocks: set[str] = set()
    for row in concept_rows:
        row_captured_at = _snapshot_datetime(
            row.get("captured_at"), field="concept captured_at"
        )
        code = str(row.get("concept_code") or "")
        stock_code = str(row.get("stock_code") or "")
        key = (code, stock_code)
        if (
            not code
            or len(stock_code) != 6
            or not stock_code.isdigit()
            or key in concept_keys
            or row.get("quality_status") != "QMT_VALIDATED"
            or row_captured_at != captured_at
        ):
            raise RuntimeError("QMT concept membership rows are not canonical")
        concept_keys.add(key)
        concept_stocks.add(stock_code)
        concept_identities.append(
            tuple(
                str(row.get(column) or "")
                for column in (
                    "concept_code",
                    "concept_name",
                    "stock_code",
                    "short_name",
                )
            )
        )
    for row in industry_rows:
        row_captured_at = _snapshot_datetime(
            row.get("captured_at"), field="industry captured_at"
        )
        code = str(row.get("industry_code") or "")
        stock_code = str(row.get("stock_code") or "")
        key = (code, stock_code)
        if (
            not code
            or len(stock_code) != 6
            or not stock_code.isdigit()
            or key in industry_keys
            or row.get("quality_status") != "QMT_VALIDATED"
            or row_captured_at != captured_at
        ):
            raise RuntimeError("QMT industry membership rows are not canonical")
        industry_keys.add(key)
        industry_stocks.add(stock_code)
        industry_identities.append(
            tuple(
                str(row.get(column) or "")
                for column in (
                    "industry_code",
                    "industry_name",
                    "industry_type",
                    "stock_code",
                    "short_name",
                )
            )
        )

    concept_count = len({item[0] for item in concept_identities})
    industry_count = len({item[0] for item in industry_identities})
    concept_hash = _canonical_hash(concept_identities)
    industry_hash = _canonical_hash(industry_identities)
    if (
        concept_count < MIN_CONCEPT_COUNT
        or len(concept_identities) < MIN_CONCEPT_RELATION_COUNT
        or len(concept_stocks) < MIN_CONCEPT_STOCK_COUNT
        or not industry_count
        or len(industry_identities) < MIN_INDUSTRY_RELATION_COUNT
        or len(industry_stocks) < MIN_INDUSTRY_STOCK_COUNT
        or int(run.get("concept_count") or 0) != concept_count
        or int(run.get("concept_relation_count") or 0)
        != len(concept_identities)
        or int(run.get("industry_count") or 0) != industry_count
        or int(run.get("industry_relation_count") or 0)
        != len(industry_identities)
        or str(run.get("concept_hash") or "").lower() != concept_hash
        or str(run.get("industry_hash") or "").lower() != industry_hash
    ):
        raise RuntimeError("QMT membership snapshot count/hash proof differs")
    return {
        "snapshot_date": target.isoformat(),
        "source": source,
        "quality_status": "QMT_VALIDATED",
        "capture_mode": "qmt_close_full_refresh",
        "captured_at": captured_at.isoformat(sep=" ", timespec="seconds"),
        "concept_count": concept_count,
        "concept_relation_count": len(concept_identities),
        "concept_stock_count": len(concept_stocks),
        "concept_hash": concept_hash,
        "industry_count": industry_count,
        "industry_relation_count": len(industry_identities),
        "industry_stock_count": len(industry_stocks),
        "industry_hash": industry_hash,
    }


def _concept_snapshot_frame(
    frames: dict[str, pd.DataFrame],
    *,
    snapshot_date: date,
    source: str,
    quality_status: str,
    captured_at: datetime,
) -> pd.DataFrame:
    members = frames["si_concept_constituent_east"].copy()
    catalog = frames["si_concept_code_east"].copy()
    name_map = (
        catalog.drop_duplicates("concept_code", keep="last")
        .set_index("concept_code")["name"]
        .fillna("")
        .astype(str)
        .to_dict()
    )
    out = pd.DataFrame(
        {
            "snapshot_date": snapshot_date,
            "source": source,
            "concept_code": members["concept_code"].fillna("").astype(str),
            "concept_name": members["concept_code"].map(name_map).fillna("").astype(str),
            "stock_code": members["stock_code"].fillna("").astype(str).str.zfill(6),
            "short_name": members["short_name"].fillna("").astype(str),
            "quality_status": quality_status,
            "captured_at": captured_at,
        }
    )
    return out.drop_duplicates(
        subset=["snapshot_date", "source", "concept_code", "stock_code"],
        keep="last",
    ).reset_index(drop=True)


def _industry_snapshot_frame(
    frames: dict[str, pd.DataFrame],
    *,
    snapshot_date: date,
    source: str,
    quality_status: str,
    captured_at: datetime,
) -> pd.DataFrame:
    industry = frames["si_industry_sw"].copy()
    stock_names = (
        frames["si_all_code"].drop_duplicates("stock_code", keep="last")
        .set_index("stock_code")["short_name"]
        .fillna("")
        .astype(str)
        .to_dict()
    )
    out = pd.DataFrame(
        {
            "snapshot_date": snapshot_date,
            "source": source,
            "industry_code": industry["sw_code"].fillna("").astype(str),
            "industry_name": industry["industry_name"].fillna("").astype(str),
            "industry_type": industry["industry_type"].fillna("").astype(str),
            "stock_code": industry["stock_code"].fillna("").astype(str).str.zfill(6),
            "short_name": industry["stock_code"].map(stock_names).fillna("").astype(str),
            "quality_status": quality_status,
            "captured_at": captured_at,
        }
    )
    return out.drop_duplicates(
        subset=["snapshot_date", "source", "industry_code", "stock_code"],
        keep="last",
    ).reset_index(drop=True)


def publish_membership_snapshot(
    connection: Connection,
    frames: dict[str, pd.DataFrame],
    *,
    snapshot_date: date,
    source: str = PROVIDER_ID,
    quality_status: str = "QMT_VALIDATED",
    capture_mode: str = "qmt_close_full_refresh",
    captured_at: datetime | None = None,
) -> dict[str, Any]:
    """Append one immutable snapshot inside the caller's publication transaction."""
    captured_at = (captured_at or datetime.now()).replace(microsecond=0)
    if captured_at.tzinfo is not None:
        raise RuntimeError(
            "QMT membership captured_at must be a naive local time"
        )
    close_ready_at = datetime.combine(snapshot_date, time(15, 10))
    knowledge_cutoff = datetime.combine(
        snapshot_date + timedelta(days=1),
        time.min,
    )
    if not close_ready_at <= captured_at < knowledge_cutoff:
        raise RuntimeError(
            "QMT membership snapshot cannot relabel current reference data "
            "as a different session"
        )
    concept = _concept_snapshot_frame(
        frames,
        snapshot_date=snapshot_date,
        source=source,
        quality_status=quality_status,
        captured_at=captured_at,
    )
    industry = _industry_snapshot_frame(
        frames,
        snapshot_date=snapshot_date,
        source=source,
        quality_status=quality_status,
        captured_at=captured_at,
    )
    concept_rows = [
        tuple(str(row[column]) for column in ("concept_code", "concept_name", "stock_code", "short_name"))
        for _, row in concept.iterrows()
    ]
    industry_rows = [
        tuple(
            str(row[column])
            for column in (
                "industry_code",
                "industry_name",
                "industry_type",
                "stock_code",
                "short_name",
            )
        )
        for _, row in industry.iterrows()
    ]
    concept_hash = _canonical_hash(concept_rows)
    industry_hash = _canonical_hash(industry_rows)
    existing = connection.execute(
        text(
            """
            SELECT concept_hash, industry_hash
            FROM qmt_membership_snapshot_run
            WHERE snapshot_date = :snapshot_date AND source = :source
            """
        ),
        {"snapshot_date": snapshot_date, "source": source},
    ).mappings().first()
    if existing:
        if (
            str(existing["concept_hash"]) != concept_hash
            or str(existing["industry_hash"]) != industry_hash
        ):
            raise RuntimeError(
                "immutable QMT membership snapshot collision: "
                f"{snapshot_date}/{source}"
            )
        proof = verify_existing_membership_snapshot(
            connection,
            snapshot_date=snapshot_date,
            decision_known_at=captured_at,
            source=source,
        )
        if (
            proof["concept_hash"] != concept_hash
            or proof["industry_hash"] != industry_hash
            or int(proof["concept_relation_count"]) != len(concept)
            or int(proof["industry_relation_count"]) != len(industry)
        ):
            raise RuntimeError(
                "idempotent QMT membership exact readback differs"
            )
        return {
            "status": "idempotent",
            "snapshot_date": snapshot_date.isoformat(),
            "source": source,
            "quality_status": proof["quality_status"],
            "captured_at": proof["captured_at"],
            "concept_relations": int(proof["concept_relation_count"]),
            "industry_relations": int(proof["industry_relation_count"]),
            "concept_hash": concept_hash,
            "industry_hash": industry_hash,
        }

    concept_written = write_frame(
        concept,
        "qmt_concept_member_snapshot",
        connection,
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi",
    )
    industry_written = write_frame(
        industry,
        "qmt_industry_member_snapshot",
        connection,
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi",
    )
    if int(concept_written) != len(concept) or int(industry_written) != len(industry):
        raise RuntimeError(
            "membership snapshot write mismatch: "
            f"concept={concept_written}/{len(concept)} "
            f"industry={industry_written}/{len(industry)}"
        )
    connection.execute(
        text(
            """
            INSERT INTO qmt_membership_snapshot_run
            (snapshot_date, source, quality_status, capture_mode,
             concept_count, concept_relation_count, industry_count,
             industry_relation_count, concept_hash, industry_hash, captured_at)
            VALUES
            (:snapshot_date, :source, :quality_status, :capture_mode,
             :concept_count, :concept_relation_count, :industry_count,
             :industry_relation_count, :concept_hash, :industry_hash, :captured_at)
            """
        ),
        {
            "snapshot_date": snapshot_date,
            "source": source,
            "quality_status": quality_status,
            "capture_mode": capture_mode,
            "concept_count": int(concept["concept_code"].nunique()),
            "concept_relation_count": len(concept),
            "industry_count": int(industry["industry_code"].nunique()),
            "industry_relation_count": len(industry),
            "concept_hash": concept_hash,
            "industry_hash": industry_hash,
            "captured_at": captured_at,
        },
    )
    proof = verify_existing_membership_snapshot(
        connection,
        snapshot_date=snapshot_date,
        decision_known_at=captured_at,
        source=source,
    )
    if (
        proof["concept_hash"] != concept_hash
        or proof["industry_hash"] != industry_hash
        or int(proof["concept_relation_count"]) != len(concept)
        or int(proof["industry_relation_count"]) != len(industry)
    ):
        raise RuntimeError("created QMT membership exact readback differs")
    return {
        "status": "created",
        "snapshot_date": snapshot_date.isoformat(),
        "source": source,
        "quality_status": proof["quality_status"],
        "captured_at": proof["captured_at"],
        "concepts": int(proof["concept_count"]),
        "concept_relations": int(proof["concept_relation_count"]),
        "industries": int(proof["industry_count"]),
        "industry_relations": int(proof["industry_relation_count"]),
        "concept_hash": concept_hash,
        "industry_hash": industry_hash,
    }

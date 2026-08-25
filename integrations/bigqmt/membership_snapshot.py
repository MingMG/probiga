# -*- coding: utf-8 -*-
"""Immutable daily concept and industry membership snapshots from BigQMT."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
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


def _canonical_hash(rows: list[tuple[str, ...]]) -> str:
    payload = json.dumps(
        sorted(rows),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_membership_snapshot_tables(engine: Engine) -> dict[str, Any]:
    """Compatibility alias for the read-only runtime schema guard."""

    return validate_qmt_membership_snapshot_runtime_schema(engine)


def privileged_migrate_membership_snapshot_tables(engine: Engine) -> dict[str, Any]:
    """Create the immutable snapshot tables during a fenced release only."""

    return privileged_migrate_qmt_membership_snapshot_schema(engine)


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
        return {
            "status": "idempotent",
            "snapshot_date": snapshot_date.isoformat(),
            "source": source,
            "concept_relations": len(concept),
            "industry_relations": len(industry),
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
    return {
        "status": "created",
        "snapshot_date": snapshot_date.isoformat(),
        "source": source,
        "quality_status": quality_status,
        "concepts": int(concept["concept_code"].nunique()),
        "concept_relations": len(concept),
        "industries": int(industry["industry_code"].nunique()),
        "industry_relations": len(industry),
        "concept_hash": concept_hash,
        "industry_hash": industry_hash,
    }

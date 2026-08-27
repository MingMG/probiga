#!/usr/bin/env python3
from __future__ import annotations

"""Validate and publish standard-QMT reference data to existing si_* tables."""

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, time
from pathlib import Path

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.reference import (
    PROVIDER_ID,
    fetch_all_index_codes,
    fetch_all_stock_codes,
    fetch_index_constituents,
    fetch_sector_datasets,
)
from integrations.bigqmt.membership_snapshot import (
    MIN_CONCEPT_COUNT,
    MIN_CONCEPT_RELATION_COUNT,
    MIN_CONCEPT_STOCK_COUNT,
    MIN_INDUSTRY_RELATION_COUNT,
    MIN_INDUSTRY_STOCK_COUNT,
    ensure_membership_snapshot_tables,
    publish_membership_snapshot,
    verify_existing_membership_snapshot,
)
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.sql_reader import read_sql_rows
from server.common.batch_db import write_frame
from tools.env_config import create_tool_engine, load_project_env


TARGET_COLUMNS = {
    "si_all_code": ["stock_code", "short_name", "exchange", "list_date", "etl_sync_at"],
    "si_all_index_code": ["index_code", "concept_code", "name", "source", "etl_sync_at"],
    "si_index_constituent": ["index_code", "stock_code", "short_name", "etl_sync_at"],
    "si_concept_code_east": ["concept_code", "index_code", "name", "source", "etl_sync_at"],
    "si_concept_constituent_east": ["concept_code", "stock_code", "short_name", "etl_sync_at"],
    "si_industry_sw": ["stock_code", "sw_code", "industry_name", "industry_type", "source", "etl_sync_at"],
    "si_stock_concept_east": ["stock_code", "concept_code", "name", "source", "reason", "etl_sync_at"],
    "si_stock_plate_east": ["stock_code", "plate_code", "plate_name", "plate_type", "source", "etl_sync_at"],
}
MEMBERSHIP_VERIFICATION_SCHEMA = "probiga.qmt-membership-verification.v1"
MEMBERSHIP_PUBLICATION_SCHEMA = "probiga.qmt-membership-publication.v1"
MEMBERSHIP_CLOSE_READY_TIME = time(15, 10)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _membership_verification_receipt(
    *,
    status: str,
    snapshot_date: str,
    verified_at: datetime,
    proof: dict[str, object] | None = None,
    reason: str = "",
) -> dict[str, object]:
    receipt: dict[str, object] = {
        "schema": MEMBERSHIP_VERIFICATION_SCHEMA,
        "status": status,
        "task_type": "qmt_membership_snapshot",
        "snapshot_date": snapshot_date,
        "read_only": True,
        "verified_at": verified_at.replace(microsecond=0).isoformat(
            sep=" ", timespec="seconds"
        ),
        "proof": proof or {},
    }
    if reason:
        receipt["reason"] = str(reason)[:1000]
    receipt["result_sha256"] = hashlib.sha256(
        _canonical_json(receipt).encode("utf-8")
    ).hexdigest()
    return receipt


def validate_membership_verification_receipt(
    payload: dict[str, object],
    return_code: int,
    *,
    expected_snapshot_date: str = "",
) -> str:
    supplied_hash = str(payload.get("result_sha256") or "").lower()
    unsigned = dict(payload)
    unsigned.pop("result_sha256", None)
    if (
        payload.get("schema") != MEMBERSHIP_VERIFICATION_SCHEMA
        or payload.get("task_type") != "qmt_membership_snapshot"
        or payload.get("read_only") is not True
        or len(supplied_hash) != 64
        or any(character not in "0123456789abcdef" for character in supplied_hash)
        or hashlib.sha256(_canonical_json(unsigned).encode("utf-8")).hexdigest()
        != supplied_hash
    ):
        raise ValueError("QMT membership verification receipt identity differs")
    snapshot_date = date.fromisoformat(str(payload.get("snapshot_date") or ""))
    if snapshot_date.isoformat() != payload.get("snapshot_date"):
        raise ValueError("QMT membership verification date is invalid")
    if expected_snapshot_date and snapshot_date.isoformat() != expected_snapshot_date:
        raise ValueError("QMT membership verification differs from release target")
    if payload.get("status") == "DATA_BLOCKED":
        if int(return_code) != 2 or not str(payload.get("reason") or "").startswith(
            "DATA_BLOCKED:"
        ):
            raise ValueError("QMT membership blocked receipt differs")
        return "blocked"
    proof = payload.get("proof")
    if (
        int(return_code) != 0
        or payload.get("status") != "PASS"
        or not isinstance(proof, dict)
        or proof.get("snapshot_date") != snapshot_date.isoformat()
        or proof.get("source") != PROVIDER_ID
        or proof.get("quality_status") != "QMT_VALIDATED"
        or proof.get("capture_mode") != "qmt_close_full_refresh"
        or int(proof.get("concept_count") or 0) < MIN_CONCEPT_COUNT
        or int(proof.get("concept_relation_count") or 0)
        < MIN_CONCEPT_RELATION_COUNT
        or int(proof.get("concept_stock_count") or 0)
        < MIN_CONCEPT_STOCK_COUNT
        or int(proof.get("industry_count") or 0) <= 0
        or int(proof.get("industry_relation_count") or 0)
        < MIN_INDUSTRY_RELATION_COUNT
        or int(proof.get("industry_stock_count") or 0)
        < MIN_INDUSTRY_STOCK_COUNT
    ):
        raise ValueError("QMT membership PASS receipt is incomplete")
    for field in ("concept_hash", "industry_hash"):
        value = str(proof.get(field) or "").lower()
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("QMT membership PASS receipt hash is invalid")
    datetime.fromisoformat(str(payload.get("verified_at") or ""))
    datetime.fromisoformat(str(proof.get("captured_at") or ""))
    return "complete"


def _membership_publication_receipt(
    *,
    snapshot_date: str,
    published_at: datetime,
    publish_status: str,
    proof: dict[str, object],
) -> dict[str, object]:
    receipt: dict[str, object] = {
        "schema": MEMBERSHIP_PUBLICATION_SCHEMA,
        "status": "PASS",
        "task_type": "qmt_membership_snapshot",
        "snapshot_date": snapshot_date,
        "read_only": False,
        "published_at": published_at.replace(microsecond=0).isoformat(
            sep=" ", timespec="seconds"
        ),
        "publish_status": publish_status,
        "proof": proof,
    }
    receipt["result_sha256"] = hashlib.sha256(
        _canonical_json(receipt).encode("utf-8")
    ).hexdigest()
    return receipt


def validate_membership_publication_receipt(
    payload: dict[str, object],
    return_code: int,
    *,
    expected_snapshot_date: str = "",
) -> str:
    supplied_hash = str(payload.get("result_sha256") or "").lower()
    unsigned = dict(payload)
    unsigned.pop("result_sha256", None)
    if (
        payload.get("schema") != MEMBERSHIP_PUBLICATION_SCHEMA
        or payload.get("task_type") != "qmt_membership_snapshot"
        or payload.get("status") != "PASS"
        or payload.get("read_only") is not False
        or payload.get("publish_status") not in {"created", "idempotent"}
        or int(return_code) != 0
        or len(supplied_hash) != 64
        or any(character not in "0123456789abcdef" for character in supplied_hash)
        or hashlib.sha256(_canonical_json(unsigned).encode("utf-8")).hexdigest()
        != supplied_hash
    ):
        raise ValueError("QMT membership publication receipt identity differs")
    snapshot_date = date.fromisoformat(str(payload.get("snapshot_date") or ""))
    if snapshot_date.isoformat() != payload.get("snapshot_date"):
        raise ValueError("QMT membership publication date is invalid")
    if expected_snapshot_date and snapshot_date.isoformat() != expected_snapshot_date:
        raise ValueError("QMT membership publication differs from authoritative target")
    proof = payload.get("proof")
    if (
        not isinstance(proof, dict)
        or proof.get("snapshot_date") != snapshot_date.isoformat()
        or proof.get("source") != PROVIDER_ID
        or proof.get("quality_status") != "QMT_VALIDATED"
        or proof.get("capture_mode") != "qmt_close_full_refresh"
        or int(proof.get("concept_count") or 0) < MIN_CONCEPT_COUNT
        or int(proof.get("concept_relation_count") or 0)
        < MIN_CONCEPT_RELATION_COUNT
        or int(proof.get("concept_stock_count") or 0)
        < MIN_CONCEPT_STOCK_COUNT
        or int(proof.get("industry_count") or 0) <= 0
        or int(proof.get("industry_relation_count") or 0)
        < MIN_INDUSTRY_RELATION_COUNT
        or int(proof.get("industry_stock_count") or 0)
        < MIN_INDUSTRY_STOCK_COUNT
    ):
        raise ValueError("QMT membership publication receipt proof is incomplete")
    for field in ("concept_hash", "industry_hash"):
        value = str(proof.get(field) or "").lower()
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("QMT membership publication receipt hash is invalid")
    datetime.fromisoformat(str(payload.get("published_at") or ""))
    datetime.fromisoformat(str(proof.get("captured_at") or ""))
    return "complete"


def _clean(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = frame.reindex(columns=columns).copy()
    return out.astype(object).where(pd.notna(out), None)


def fetch_and_validate(
    engine,
    *,
    force_reference_refresh: bool = False,
) -> tuple[dict[str, pd.DataFrame], dict[str, object]]:
    print("[1/4] standard QMT stock universe", flush=True)
    stocks = fetch_all_stock_codes()
    print(f"stock rows={len(stocks)}", flush=True)

    print("[2/4] standard QMT index universe", flush=True)
    indexes = fetch_all_index_codes(engine=engine)
    print(f"index rows={len(indexes)}", flush=True)

    print("[3/4] standard QMT index constituents", flush=True)
    index_members = fetch_index_constituents(indexes["index_code"].tolist())
    print(
        f"index member rows={len(index_members)} indexes={index_members['index_code'].nunique() if not index_members.empty else 0}",
        flush=True,
    )

    print("[4/4] standard QMT concepts and industries", flush=True)
    sectors = fetch_sector_datasets(force_refresh=force_reference_refresh)
    for name, frame in sectors.items():
        print(f"{name} rows={len(frame)}", flush=True)

    stock_codes = set(stocks["stock_code"].astype(str).str.zfill(6))
    index_codes = set(indexes["index_code"].astype(str).str.zfill(6))
    name_map = stocks.set_index("stock_code")["short_name"].fillna("").astype(str).to_dict()
    index_members = index_members.copy()
    index_members["index_code"] = index_members["index_code"].astype(str).str.zfill(6)
    index_members["stock_code"] = index_members["stock_code"].astype(str).str.zfill(6)
    index_members = index_members[
        index_members["index_code"].isin(index_codes) & index_members["stock_code"].isin(stock_codes)
    ].drop_duplicates(subset=["index_code", "stock_code"], keep="last")
    index_members["short_name"] = index_members["stock_code"].map(name_map).fillna("")

    catalog = sectors["concept_catalog"].drop_duplicates(subset=["concept_code"], keep="last").copy()
    concept_codes = set(catalog["concept_code"].astype(str))
    concept_members = sectors["concept_constituents"].copy()
    concept_members["stock_code"] = concept_members["stock_code"].astype(str).str.zfill(6)
    concept_members = concept_members[
        concept_members["concept_code"].astype(str).isin(concept_codes)
        & concept_members["stock_code"].isin(stock_codes)
    ].drop_duplicates(subset=["concept_code", "stock_code"], keep="last")
    concept_members["short_name"] = concept_members["stock_code"].map(name_map).fillna("")

    industry = sectors["industry_sw"].copy()
    concepts = sectors["stock_concepts"].copy()
    plates = sectors["stock_plates"].copy()
    for frame in (industry, concepts, plates):
        frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
        frame.drop(frame.index[~frame["stock_code"].isin(stock_codes)], inplace=True)

    counts = {
        "stocks": int(len(stocks)),
        "indexes": int(len(indexes)),
        "index_members": int(len(index_members)),
        "index_member_indexes": int(index_members["index_code"].nunique()),
        "concepts": int(len(catalog)),
        "concept_members": int(len(concept_members)),
        "industry_relations": int(len(industry)),
        "industry_stocks": int(industry["stock_code"].nunique()),
        "stock_concepts": int(len(concepts)),
        "concept_stocks": int(concepts["stock_code"].nunique()),
        "stock_plates": int(len(plates)),
        "plate_stocks": int(plates["stock_code"].nunique()),
        "source": PROVIDER_ID,
    }
    minimums = {
        "stocks": 5000,
        "indexes": 500,
        "index_members": 5000,
        "index_member_indexes": 100,
        "concepts": 500,
        "concept_members": 30000,
        "industry_relations": 5000,
        "industry_stocks": 4500,
        "stock_concepts": 30000,
        "concept_stocks": 3000,
        "stock_plates": 40000,
        "plate_stocks": 4500,
    }
    failures = [f"{key}={counts[key]} < {minimum}" for key, minimum in minimums.items() if counts[key] < minimum]
    if failures:
        raise RuntimeError("standard QMT reference validation failed: " + "; ".join(failures))

    now = datetime.now().replace(microsecond=0)
    frames = {
        "si_all_code": stocks,
        "si_all_index_code": indexes,
        "si_index_constituent": index_members,
        "si_concept_code_east": catalog,
        "si_concept_constituent_east": concept_members,
        "si_industry_sw": industry,
        "si_stock_concept_east": concepts,
        "si_stock_plate_east": plates,
    }
    for table_name, frame in frames.items():
        frame["etl_sync_at"] = now
        frames[table_name] = _clean(frame, TARGET_COLUMNS[table_name])
    return frames, counts


def _membership_decision_time(now: datetime | None = None) -> datetime:
    current = now or datetime.now(PRODUCTION_TIMEZONE)
    if current.tzinfo is not None:
        current = current.astimezone(PRODUCTION_TIMEZONE).replace(tzinfo=None)
    return current.replace(microsecond=0)


def validate_membership_publication_target(
    engine,
    *,
    snapshot_date: date,
    now: datetime | None = None,
) -> date:
    """Fail before DML unless one exact, currently closed session is targeted."""

    if isinstance(snapshot_date, datetime) or not isinstance(snapshot_date, date):
        raise RuntimeError("QMT membership publication target must be one date")
    current = _membership_decision_time(now)
    authoritative = authoritative_closed_trade_date(
        engine,
        now=current,
        close_ready_time=MEMBERSHIP_CLOSE_READY_TIME,
    )
    if authoritative != snapshot_date.isoformat():
        raise RuntimeError(
            "QMT membership publication target differs from the authoritative "
            f"closed session: requested={snapshot_date.isoformat()} "
            f"authoritative={authoritative or 'unavailable'}"
        )
    rows = read_sql_rows(
        engine,
        """
        SELECT COUNT(*) AS open_count
        FROM si_trade_calendar
        WHERE trade_date=:snapshot_date AND trade_status=1
        """,
        {"snapshot_date": snapshot_date},
        context="qmt_membership_publication_target",
    )
    open_count = int((rows[0] if rows else {}).get("open_count") or 0)
    if open_count != 1:
        raise RuntimeError(
            "QMT membership publication target is not one unique open session"
        )
    return snapshot_date


def resolve_snapshot_date(
    engine,
    requested: str = "",
    *,
    now: datetime | None = None,
) -> date:
    current = _membership_decision_time(now)
    if requested:
        raw = str(requested)
        try:
            target = date.fromisoformat(raw)
        except ValueError as exc:
            raise RuntimeError(
                "QMT membership snapshot date must be exact ISO YYYY-MM-DD"
            ) from exc
        if target.isoformat() != raw:
            raise RuntimeError(
                "QMT membership snapshot date must be exact ISO YYYY-MM-DD"
            )
    else:
        raw_target = authoritative_closed_trade_date(
            engine,
            now=current,
            close_ready_time=MEMBERSHIP_CLOSE_READY_TIME,
        )
        try:
            target = date.fromisoformat(raw_target)
        except ValueError as exc:
            raise RuntimeError(
                "cannot resolve completed trading date for QMT membership snapshot"
            ) from exc
    return validate_membership_publication_target(
        engine,
        snapshot_date=target,
        now=current,
    )


def publish(
    engine,
    frames: dict[str, pd.DataFrame],
    *,
    snapshot_date: date,
    captured_at: datetime | None = None,
) -> dict[str, object]:
    captured_at = _membership_decision_time(captured_at)
    validate_membership_publication_target(
        engine,
        snapshot_date=snapshot_date,
        now=captured_at,
    )
    delete_order = [
        "si_index_constituent",
        "si_concept_constituent_east",
        "si_industry_sw",
        "si_stock_concept_east",
        "si_stock_plate_east",
        "si_concept_code_east",
        "si_all_index_code",
        "si_all_code",
    ]
    insert_order = [
        "si_all_code",
        "si_all_index_code",
        "si_concept_code_east",
        "si_index_constituent",
        "si_concept_constituent_east",
        "si_industry_sw",
        "si_stock_concept_east",
        "si_stock_plate_east",
    ]
    ensure_membership_snapshot_tables(engine)
    with engine.begin() as conn:
        for table_name in delete_order:
            conn.execute(text(f"DELETE FROM `{table_name}`"))
        for table_name in insert_order:
            frame = frames[table_name]
            written = write_frame(
                frame,
                table_name,
                conn,
                if_exists="append",
                index=False,
                chunksize=2000 if len(frame) > 10000 else 1000,
                method="multi",
            )
            if int(written) != len(frame):
                raise RuntimeError(f"{table_name} write mismatch: {written}/{len(frame)}")
            print(f"published {table_name}: {written}", flush=True)
        snapshot = publish_membership_snapshot(
            conn,
            frames,
            snapshot_date=snapshot_date,
            captured_at=captured_at,
        )
        print(
            f"published membership snapshot {snapshot_date}: "
            f"concept={snapshot['concept_relations']} "
            f"industry={snapshot['industry_relations']}",
            flush=True,
        )
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="publish after all validation passes")
    parser.add_argument("--snapshot-date", default="")
    parser.add_argument(
        "--verify-existing-snapshot",
        action="store_true",
        help=(
            "read back one already-captured immutable snapshot without QMT "
            "requests or DML"
        ),
    )
    parser.add_argument(
        "--force-reference-refresh",
        action="store_true",
        help="force a fresh QMT sector pull instead of using the bridge cache",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--promote-production",
        action="store_true",
        help=(
            "after an immutable QMT snapshot is published, copy that exact "
            "hash-verified snapshot to the production database"
        ),
    )
    args = parser.parse_args()
    if args.verify_existing_snapshot:
        if (
            not args.snapshot_date
            or args.apply
            or args.force_reference_refresh
            or args.promote_production
        ):
            parser.error(
                "--verify-existing-snapshot requires only --snapshot-date "
                "and may not publish or refresh"
            )
        verified_at = datetime.now().replace(microsecond=0)
        try:
            target = date.fromisoformat(args.snapshot_date)
            load_project_env()
            engine = create_tool_engine(pool_pre_ping=True)
            try:
                proof = verify_existing_membership_snapshot(
                    engine,
                    snapshot_date=target,
                    decision_known_at=verified_at,
                )
            finally:
                engine.dispose()
            result = _membership_verification_receipt(
                status="PASS",
                snapshot_date=target.isoformat(),
                verified_at=verified_at,
                proof=proof,
            )
            print(_canonical_json(result), flush=True)
            return 0
        except Exception as exc:
            result = _membership_verification_receipt(
                status="DATA_BLOCKED",
                snapshot_date=str(args.snapshot_date),
                verified_at=verified_at,
                reason=f"DATA_BLOCKED: {type(exc).__name__}: {exc}",
            )
            print(_canonical_json(result), flush=True)
            return 2
    load_project_env()
    engine = create_tool_engine(pool_pre_ping=True)
    try:
        publication_target = (
            resolve_snapshot_date(engine, args.snapshot_date)
            if args.apply
            else None
        )
        frames, counts = fetch_and_validate(
            engine,
            force_reference_refresh=args.force_reference_refresh,
        )
        result: dict[str, object] = {
            "status": "validated",
            "counts": counts,
            "applied": False,
        }
        if args.apply:
            assert publication_target is not None
            published_at = _membership_decision_time()
            # Re-resolve immediately before publication.  A long QMT fetch may
            # cross a session boundary; stale targets must still perform zero
            # publication DML.
            snapshot_date = resolve_snapshot_date(
                engine,
                publication_target.isoformat(),
                now=published_at,
            )
            snapshot = publish(
                engine,
                frames,
                snapshot_date=snapshot_date,
                captured_at=published_at,
            )
            proof = verify_existing_membership_snapshot(
                engine,
                snapshot_date=snapshot_date,
                decision_known_at=published_at,
            )
            receipt = _membership_publication_receipt(
                snapshot_date=snapshot_date.isoformat(),
                published_at=published_at,
                publish_status=str(snapshot.get("status") or ""),
                proof=proof,
            )
            result.update({
                "status": "success",
                "applied": True,
                "membership_snapshot": snapshot,
                "membership_publication_receipt": receipt,
            })
            if args.promote_production:
                from tools.promote_qmt_membership_to_production import (
                    promote_to_production,
                )

                result["production_promotion"] = promote_to_production(
                    snapshot_date=snapshot_date,
                )
        print(
            json.dumps(result, ensure_ascii=False, default=str)
            if args.json
            else result,
            flush=True,
        )
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

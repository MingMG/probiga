#!/usr/bin/env python3
"""Import QMT local daily bars into the canonical stock daily table."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.datadir import (  # noqa: E402
    CST,
    DAILY_PERIOD,
    WORD_TIME,
    iter_daily_rows,
    load_dat,
    session_slice,
)
from integrations.qmt.datadir_import import (  # noqa: E402
    load_universe,
    new_run_id,
)
from server.common.authoritative_market_clock import (  # noqa: E402
    authoritative_closed_trade_date,
)
from server.common.batch_db import create_batch_engine  # noqa: E402
from server.common.historical_daily_bar_evidence import (  # noqa: E402
    SINA_PROVIDER,
    SINA_QUALITY,
    load_public_bars,
)
from server.common.mysql_lock import STOCK_KLINE_FREEZE_LOCK_NAME, mysql_named_lock  # noqa: E402
from server.common.official_historical_daily_evidence import (  # noqa: E402
    DAILY_SUSPENSION_KIND,
    load_daily_delisting_documents,
    load_daily_suspension_documents,
    load_historical_documents,
)
from server.common.qmt_attestation_contract import canonical_digest  # noqa: E402
from server.common.qmt_trade_calendar import (  # noqa: E402
    load_trade_calendar_receipt,
)
from server.common.qmt_datadir_result import (  # noqa: E402
    build_result,
    build_unavailable_result,
    load_receipt,
    partition_from_receipt,
    persisted_fingerprint,
)
from server.common.qmt_datadir_daily_coverage import (  # noqa: E402
    NATIVE_AMOUNT_WORD,
    NATIVE_EVIDENCE_DATASET,
    build_manifest,
    canonical_bar,
    insert_manifest,
    load_daily_manifest,
    load_daily_manifest_completion,
    load_daily_native_evidence,
    native_amount_correction,
    native_evidence_fingerprint,
    retained_listing_evidence,
    seal_native_amount_corrections,
    seal_native_public_replacement,
    suspension_evidence,
    validate_manifest,
    validate_native_revision,
)
from server.common.qmt_post_close_block_evidence import (  # noqa: E402
    KIND as POST_CLOSE_KIND,
    load_post_close_evidence,
    verify_post_close_revision,
)
from server.common.qmt_stock_catalog import load_stock_catalog  # noqa: E402
from tools.env_config import load_project_env  # noqa: E402

DATASET = "stock_daily"
SOURCE = "qmt_datadir"
TARGET_TABLE = "sm_stock_kline"
RECEIPT_TABLE = "qmt_datadir_import_partition"


def _build_sha() -> str:
    return str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()


def _task_result(
    engine,
    *,
    sessions: list[dt.date],
) -> dict:
    partitions = []
    for session in sessions:
        receipt = load_receipt(
            engine,
            dataset=DATASET,
            trade_date=session.isoformat(),
        )
        if receipt is not None:
            partitions.append(partition_from_receipt(receipt))
    return build_result(
        dataset=DATASET,
        sessions=[session.isoformat() for session in sessions],
        partitions=partitions,
        build_sha=_build_sha(),
    )


def _sessions(engine, start: dt.date, end: dt.date) -> list[dt.date]:
    with engine.connect() as connection:
        receipt = load_trade_calendar_receipt(
            connection,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            decision_known_at=dt.datetime.now(CST),
        )
    return [
        dt.date.fromisoformat(day)
        for day in receipt.sessions_between(
            start.isoformat(), end.isoformat()
        )
    ]


def _existing_sources(engine, session: dt.date) -> list[str]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                f"SELECT DISTINCT data_source FROM `{TARGET_TABLE}` "
                "WHERE trade_date=:d AND k_type=1 AND adjust_type=0"
            ),
            {"d": session},
        ).fetchall()
    return sorted(str(row[0]) for row in rows if row[0])


def _short_names(engine) -> dict[str, str]:
    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT stock_code, short_name FROM si_all_code")
        ).fetchall()
    return {
        str(row[0]).zfill(6): str(row[1] or "")
        for row in rows
    }


def _harvest(root: Path, universe, session: dt.date, *, deadline=None) -> dict:
    rows = []
    codes = set()
    digest = hashlib.sha256()
    for member in universe.ordered():
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(f"historical daily DAT harvest deadline exceeded for {session}")
        dat = load_dat(root, member.market, DAILY_PERIOD, member.code)
        if dat is None:
            continue
        selected = session_slice(dat.records, session)
        if not len(selected):
            continue
        digest.update(member.qmt_code.encode("ascii"))
        digest.update(selected.tobytes())
        for _, row in iter_daily_rows(replace(dat, records=selected), [session]):
            rows.append(row)
            codes.add(row[0])
    return {
        "rows": rows,
        "codes": codes,
        "digest": digest.hexdigest(),
    }


def _harvested_daily_rows(harvested):
    fields = ("stock_code", "trade_time", "trade_date", "open", "close", "high", "low",
              "volume", "amount", "pre_close", "change", "change_pct")
    return [dict(zip(fields, row)) for row in harvested["rows"]]


def _completion_fingerprint(engine, *, day, proof):
    native = persisted_fingerprint(engine, dataset=DATASET, trade_date=day)
    public = proof.get("historical_public_bars") or {}
    if not public:
        return native
    return {
        "row_count": native["row_count"] + len(public),
        "code_count": native["code_count"] + len(public),
        "row_hash": canonical_digest({"native_row_hash": native["row_hash"], "public_bar_evidence": public}),
    }


def _reusable_partition(engine, *, session, harvested):
    """Keep the same immutable publication when source content is unchanged."""
    cutoff = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
    day = session.isoformat()
    proof = load_daily_manifest(engine, day, decision_known_at=cutoff)
    if proof is None:
        return None
    bars = sorted((canonical_bar(row) for row in _harvested_daily_rows(harvested)),
                  key=lambda row: row["stock_code"])
    traded = [item for item in proof["entities"] if item["classification"] == "TRADED"]
    public = proof.get("historical_public_bars") or {}
    if bars != [item["evidence"] for item in traded if item["stock_code"] not in public]:
        return None
    with engine.connect() as connection:
        catalog = load_stock_catalog(connection, decision_known_at=cutoff)
        calendar = load_trade_calendar_receipt(connection, start_date=day, end_date=day,
                                               decision_known_at=cutoff)
    if ((catalog.member_set_hash != proof["catalog_member_set_hash"] and not proof.get("native_amount_correction"))
            or sorted(catalog.eligible_codes(day)) != [item["stock_code"] for item in proof["entities"]]
            or calendar.sessions_between(day, day) != [day]):
        return None
    receipt = load_receipt(engine, dataset=DATASET, trade_date=day)
    if receipt is None:
        return None
    detail = receipt.get("detail") or {}
    fingerprint = _completion_fingerprint(engine, day=day, proof=proof)
    if (receipt.get("status") != "COMPLETE" or receipt.get("run_id") != proof["run_id"]
            or int(receipt.get("row_count") or 0) != len(traded)
            or int(receipt.get("code_count") or 0) != len(traded)
            or detail.get("full_market_manifest_hash") != proof["manifest_hash"]
            or detail.get("coverage") != 1.0
            or detail.get("readback_rows") != len(traded) or detail.get("readback_codes") != len(traded)
            or detail.get("row_hash") != fingerprint["row_hash"]):
        return None
    return proof


def _coverage_manifest(engine, *, session, harvested, run_id, historical_documents=None,
                       historical_public_bars=None, previous_session="", archive_native_evidence=False):
    """Account for every catalog security before touching the live partition."""
    cutoff = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
    day = session.isoformat()
    with engine.connect() as connection:
        catalog = load_stock_catalog(connection, decision_known_at=cutoff)
        calendar = load_trade_calendar_receipt(
            connection, start_date=previous_session or day, end_date=day, decision_known_at=cutoff,
        )
    rows = _harvested_daily_rows(harvested)
    missing = sorted(set(catalog.eligible_codes(day)) - set(harvested["codes"]))
    if historical_documents is not None:
        manifest = build_manifest(
            catalog=catalog, calendar=calendar, day=day, rows=rows, run_id=run_id,
            captured_at=cutoff, historical_documents={code: historical_documents[code] for code in missing
                                                      if code in historical_documents},
            historical_public_bars=historical_public_bars, previous_session=previous_session,
            native_content_sha256=harvested["digest"] if archive_native_evidence else "",
        )
        return validate_manifest(manifest, catalog=catalog, calendar=calendar) if archive_native_evidence else manifest
    snapshot = {}
    unresolved = []
    delisting_documents = {}
    if missing:
        from integrations.bigqmt.spool import read_snapshot
        try:
            snapshot = read_snapshot("full", max_age_seconds=None)
        except (OSError, ValueError):
            snapshot = {}
        # Catalog reads may take time while the native snapshot keeps polling.
        # Bind the observation to the time it was actually read, not the
        # earlier instant at which the database authority lookup began.
        cutoff = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
        for code in missing:
            try:
                suspension_evidence(snapshot, code, day, cutoff)
            except (ValueError, KeyError, TypeError, OverflowError):
                unresolved.append(code)
    if unresolved:
        expirations = sorted(set(unresolved).intersection(
            str(member["stock_code"]) for member in catalog.members if member.get("expire_date") == day
        ))
        if expirations:
            delisting_documents = load_daily_delisting_documents(expirations, target_date=day)
            unresolved = sorted(set(unresolved) - set(delisting_documents))
    listings = {}
    if unresolved:
        listings = retained_listing_evidence(
            engine, unresolved, target_date=day, decision_known_at=cutoff,
        )
        unresolved = sorted(set(unresolved) - set(listings))
    if unresolved:
        from server.common.official_listing_evidence import load_official_listing_evidence
        listings.update(load_official_listing_evidence(
            unresolved, target_date=day, deadline_seconds=20,
        ))
        unresolved = sorted(set(unresolved) - set(listings))
    suspensions = load_daily_suspension_documents(unresolved, target_date=day) if unresolved else {}
    cutoff = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
    return build_manifest(catalog=catalog, calendar=calendar, day=day, rows=rows,
                          run_id=run_id, captured_at=cutoff, snapshot=snapshot, listings=listings,
                          daily_suspension_documents=suspensions, daily_delisting_documents=delisting_documents)


@contextmanager
def _daily_transaction(engine, *, lock_name=STOCK_KLINE_FREEZE_LOCK_NAME):
    with mysql_named_lock(engine, lock_name, timeout_seconds=10) as connection:
        connection.commit()
        with connection.begin():
            yield connection


def _require_linux_daily_evidence_reader(connection, *, evidence="no-bar"):
    from server.api.scheduler_runtime import _verified_linux_activation_build
    from server.common.component_release import runtime_component_build_sha
    from server.common.config import get_scheduler_runtime_config

    linux_sha, reason = _verified_linux_activation_build(
        connection, build_sha=runtime_component_build_sha("windows"),
        expected_poll_seconds=int(get_scheduler_runtime_config()["poll_seconds"]),
    )
    if linux_sha is None:
        raise RuntimeError(f"daily {evidence} evidence Linux reader is unavailable: {reason}")


def _archived_result(archive, *, reused=False):
    manifest, receipt = archive
    counts = native_evidence_fingerprint(manifest)
    return {
        **counts, "status": "REUSED" if reused else "COMPLETE",
        "dataset": NATIVE_EVIDENCE_DATASET, "run_id": manifest["run_id"],
        "manifest_hash": manifest["manifest_hash"], "finished_at": str(receipt["finished_at"]),
    }


def _publish_native_archive(engine, *, manifest, source_root):
    """Commit historical QMT evidence independently of the ordinary daily product."""
    day = manifest["trade_date"]
    counts = native_evidence_fingerprint(manifest)
    with _daily_transaction(engine, lock_name=f"probiga:qmt-native-daily:{day}") as connection:
        now = connection.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()
        existing = load_daily_native_evidence(connection, day, decision_known_at=now)
        if existing is not None:
            return _archived_result(existing, reused=True)
        if os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production":
            _require_linux_daily_evidence_reader(connection, evidence="historical native")
        insert_manifest(connection, manifest)
        detail = {
            "row_hash": counts["row_hash"], "coverage": 1.0,
            "readback_rows": counts["row_count"], "readback_codes": counts["code_count"],
            "eligible_code_count": len(manifest["entities"]),
            "full_market_manifest_hash": manifest["manifest_hash"],
        }
        connection.execute(text(f"""
            INSERT INTO `{RECEIPT_TABLE}`
              (dataset,trade_date,source_root,status,code_count,row_count,content_sha256,
               run_id,started_at,finished_at,detail_json)
            VALUES (:dataset,:day,:root,'COMPLETE',:codes,:rows,:sha,:run_id,:started_at,CURRENT_TIMESTAMP,:detail)
        """), {"dataset": NATIVE_EVIDENCE_DATASET, "day": day, "root": str(source_root),
               "codes": counts["code_count"], "rows": counts["row_count"],
               "sha": manifest["native_content_sha256"], "run_id": manifest["run_id"],
               "started_at": now, "detail": json.dumps(detail, ensure_ascii=False, separators=(",", ":"))})
        cutoff = connection.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()
        written = load_daily_native_evidence(connection, day, decision_known_at=cutoff)
        if written is None or written[0]["manifest_hash"] != manifest["manifest_hash"]:
            raise ValueError("historical native evidence readback differs before commit")
    committed = load_daily_native_evidence(engine, day, decision_known_at=cutoff)
    if committed is None or committed[0]["manifest_hash"] != manifest["manifest_hash"]:
        raise ValueError("historical native evidence committed readback differs")
    return _archived_result(committed)


def _historical_append_plan(connection, *, day, proof):
    """Accept only additions and native amount-word repairs against the sealed prior rows."""
    expected_corrections = proof.get("native_amount_correction") or {}
    expected_prior_hash = expected_corrections.get("prior_manifest_hash") or ""
    post_close = {row["stock_code"]: row for row in expected_corrections.get("rows", [])
                  if row.get("kind") == POST_CLOSE_KIND}
    if (proof.get("historical_reconstruction") is not True and not expected_prior_hash) or proof.get("trade_date") != day:
        raise ValueError("historical daily publication identity differs")
    canonical = {item["stock_code"]: item["evidence"] for item in proof["entities"]
                 if item["classification"] == "TRADED"}
    retained = connection.execute(text(f"""
        SELECT stock_code,trade_date,open,close,high,low,volume,amount,pre_close,
               data_source,quality_status,permission_status
        FROM `{TARGET_TABLE}` WHERE trade_date=:day AND k_type=1 AND adjust_type=0
        ORDER BY stock_code
    """), {"day": day}).mappings().all()
    seen = set()
    corrections = []
    for row in retained:
        code = row["stock_code"]
        original = canonical_bar(row)
        if (code in seen or row["data_source"] != SOURCE or row["quality_status"] != "VERIFIED"
                or row["permission_status"] != "SUPPORTED"):
            raise ValueError(f"historical daily retained business row differs for {day} {code}")
        if canonical.get(code) != original:
            correction = native_amount_correction(original, canonical.get(code))
            if code in post_close and validate_native_revision(original, canonical[code], post_close[code],
                                                               prior_hash=expected_prior_hash, cutoff=proof["captured_at"]):
                correction = post_close[code]
            if not correction or code in (proof.get("historical_public_bars") or {}):
                raise ValueError(f"historical daily retained business row differs for {day} {code}")
            corrections.append(correction)
        seen.add(code)
    previous = load_receipt(connection, dataset=DATASET, trade_date=day)
    detail = (previous or {}).get("detail") or {}
    if (not previous or not seen or previous.get("status") != "COMPLETE"
            or previous.get("row_count") != len(seen) or previous.get("code_count") != len(seen)
            or detail.get("readback_rows") != len(seen) or detail.get("readback_codes") != len(seen)
            or str(detail.get("full_market_manifest_hash") or "") != expected_prior_hash):
        raise ValueError(f"historical daily retained coarse completion differs for {day}")
    if persisted_fingerprint(connection, dataset=DATASET, trade_date=day)["row_hash"] != detail.get("row_hash"):
        raise ValueError(f"historical daily retained completion fingerprint differs for {day}")
    missing = set(canonical) - seen
    if expected_prior_hash and (missing or corrections != expected_corrections["rows"]):
        raise ValueError(f"native daily amount-only correction changed before publication for {day}")
    return missing, corrections, previous


def _exact_amount_repair_manifest(engine, *, session, harvested, run_id, required=True,
                                  post_close_code="", source_root=None):
    """Preserve the originally proved trade scope and every unrelated entity."""
    cutoff = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
    day = session.isoformat()
    original = load_daily_manifest(engine, day, decision_known_at=cutoff)
    if (original is None or original.get("historical_reconstruction")
            or original.get("native_amount_correction")):
        if not required:
            return None
        raise ValueError("native daily amount-only repair requires the original exact publication")
    bars = {row["stock_code"]: canonical_bar(row) for row in _harvested_daily_rows(harvested)}
    expected = {row["stock_code"] for row in original["entities"] if row["classification"] == "TRADED"}
    if expected != bars.keys() or len(bars) != len(harvested["rows"]):
        if not required:
            return None
        raise ValueError("native daily amount-only repair traded inventory differs")
    updated = []
    corrections = []
    for entity in original["entities"]:
        code = entity["stock_code"]
        if code not in bars or bars[code] == entity["evidence"]:
            updated.append(entity)
            continue
        correction = native_amount_correction(entity["evidence"], bars[code])
        if code == post_close_code:
            from server.common.minute_data import get_minute_engine
            block = load_post_close_evidence(get_minute_engine(), source_root=source_root, session=session,
                                             code=code, prior=original)
            cutoff = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
            correction = verify_post_close_revision(entity["evidence"], bars[code], block,
                                                    prior_hash=original["manifest_hash"], cutoff=cutoff)
        if correction is None:
            if not required:
                return None
            raise ValueError(f"native daily amount-only repair other business fields differ for {day} {code}")
        corrections.append(correction)
        core = {"stock_code": code, "classification": "TRADED", "evidence": bars[code]}
        updated.append({**core, "row_hash": canonical_digest(core)})
    if not corrections:
        if not required:
            return None
        raise ValueError("native daily exact publication has no truncated amount")
    if post_close_code and not any(row.get("kind") == POST_CLOSE_KIND for row in corrections):
        raise ValueError("requested BSE post-close scope revision is absent")
    return seal_native_amount_corrections(
        {**original, "captured_at": cutoff.isoformat(" "), "run_id": run_id, "entities": updated},
        prior_run_id=original["run_id"], prior_manifest_hash=original["manifest_hash"], corrections=corrections,
    )


def _prepare_native_public_replacement(engine, *, session, symbol, root, run_id):
    """Read one native DAT record and bind it to a previously published public bar."""
    day = session.isoformat()
    code, market = symbol.split(".")
    cutoff = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
    with engine.connect() as connection:
        prior = load_daily_manifest(connection, day, decision_known_at=cutoff)
        if prior is None:
            raise ValueError("native public replacement requires an existing complete daily publication")
        catalog = load_stock_catalog(connection, batch_id=prior["catalog_batch_id"], decision_known_at=cutoff)
        if not any(member.get("qmt_code") == symbol for member in catalog.members):
            raise ValueError("native public replacement security differs from the pinned catalog")
        receipt = load_daily_manifest_completion(connection, day, manifest=prior, decision_known_at=cutoff)
        if receipt is None:
            raise ValueError("native public replacement predecessor has no durable completion")
    dat = load_dat(root, market, DAILY_PERIOD, code)
    selected = session_slice(dat.records, session) if dat is not None else ()
    observed_at = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
    if len(selected) != 1:
        raise ValueError("native public replacement requires one exact DAT record")
    [(target, row)] = iter_daily_rows(replace(dat, records=selected), [session])
    harvested = {"rows": [row], "codes": {code}, "digest": hashlib.sha256(selected.tobytes()).hexdigest()}
    native = _harvested_daily_rows(harvested)[0]
    if code not in (prior.get("historical_public_bars") or {}):
        existing = prior.get("native_public_replacement") or {}
        if (existing.get("qmt_code") == symbol and existing.get("native_record_sha256") == harvested["digest"]
                and existing.get("native_bar_sha256") == canonical_digest(canonical_bar(native))):
            return prior, harvested, True
        raise ValueError("native public replacement requires the exact public predecessor bar")
    if target != session or row[0] != code:
        raise ValueError("native public replacement DAT record identity differs")
    proof = seal_native_public_replacement(
        prior, prior_completion=receipt, bar=native, run_id=run_id, qmt_code=symbol,
        native_event_at=dt.datetime.fromtimestamp(int(selected[0][WORD_TIME]), CST),
        observed_at=observed_at, native_record_sha256=harvested["digest"],
    )
    return proof, harvested, False


def _native_public_replacement_predecessor(connection, *, manifest, day):
    replacement = manifest["native_public_replacement"]
    cutoff = replacement["observed_at"]
    prior = load_daily_manifest(connection, day, decision_known_at=cutoff)
    receipt = load_receipt(connection, dataset=DATASET, trade_date=day)
    if (prior is None or prior["manifest_hash"] != replacement["prior_manifest_hash"] or receipt is None
            or receipt["run_id"] != replacement["prior_run_id"]
            or canonical_digest(receipt) != replacement["prior_completion_sha256"]):
        raise ValueError("native public replacement predecessor changed before publication")
    return receipt


def _with_public_bars(harvested, public, *, session):
    if set(public) & harvested["codes"]:
        raise ValueError("historical public source cannot replace an available native bar")
    rows = list(harvested["rows"])
    for code, evidence in sorted(public.items()):
        bar = evidence["sina"]["row"]
        open_price, close, high, low, volume, amount, previous = (
            Decimal(bar[key]) for key in ("open", "close", "high", "low", "volume", "amount", "pre_close")
        )
        rows.append((code, dt.datetime.combine(session, dt.time(15)), session, open_price, close, high, low,
                     volume, amount, previous, close - previous, (close - previous) / previous * 100))
    return {"rows": rows, "codes": harvested["codes"] | set(public),
            "digest": canonical_digest({"native_dat_sha256": harvested["digest"], "public_bar_evidence": public})}


def _publish(
    engine,
    *,
    session: dt.date,
    rows: list[tuple],
    covered_codes: set[str],
    short_names: dict[str, str],
    run_id: str,
    source_root: Path,
    content_sha256: str,
    eligible_code_count: int,
    coverage_manifest: dict,
) -> dict:
    now = dt.datetime.now(CST).replace(tzinfo=None, microsecond=0)
    batch_id = f"qmt_dat_daily_{session:%Y%m%d}_{run_id}"
    columns = (
        "stock_code, short_name, trade_time, trade_date, k_type, adjust_type, "
        "open, close, high, low, volume, amount, `change`, change_pct, "
        "pre_close, etl_sync_at, qmt_code, data_source, source_time, "
        "received_at, batch_id, data_version, quality_status, permission_status"
    )
    stage_columns = (
        "stage.stock_code, stage.short_name, stage.trade_time, stage.trade_date, "
        "stage.k_type, stage.adjust_type, stage.`open`, stage.`close`, "
        "stage.high, stage.low, stage.volume, stage.amount, stage.`change`, "
        "stage.change_pct, stage.pre_close, stage.etl_sync_at, "
        "stage.qmt_code, stage.data_source, stage.source_time, "
        "stage.received_at, stage.batch_id, stage.data_version, "
        "stage.quality_status, stage.permission_status"
    )
    payload: list[tuple] = []
    public = coverage_manifest.get("historical_public_bars") or {}
    for (
        code,
        _native_time,
        _trade_date,
        open_price,
        close_price,
        high,
        low,
        volume,
        amount,
        pre_close,
        change,
        change_pct,
    ) in rows:
        market = (
            "SH" if code.startswith("6")
            else "BJ" if code.startswith(("4", "8", "9"))
            else "SZ"
        )
        source_time = dt.datetime.combine(session, dt.time(15, 0))
        payload.append((
            code,
            short_names.get(code, ""),
            source_time,
            session,
            1,
            0,
            open_price,
            close_price,
            high,
            low,
            volume,
            amount,
            change,
            change_pct,
            pre_close,
            now,
            f"{code}.{market}",
            SINA_PROVIDER if code in public else SOURCE,
            source_time,
            now,
            batch_id,
            hashlib.sha256(
                (
                    f"{code}|{session}|{open_price}|{close_price}|"
                    f"{high}|{low}|{volume}|{amount}|{pre_close}"
                ).encode("utf-8")
            ).hexdigest()[:32],
            SINA_QUALITY if code in public else "VERIFIED",
            "SUPPORTED",
        ))
    row_template = "(" + ",".join(["%s"] * 24) + ")"
    temp_table = f"tmp_qmt_daily_{run_id}"
    historical = coverage_manifest.get("historical_reconstruction") is True
    native_public = coverage_manifest.get("native_public_replacement")
    repair = historical or bool(coverage_manifest.get("native_amount_correction"))
    expected_rows = len(covered_codes) if native_public else len(rows)
    with _daily_transaction(engine) as connection:
        if os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production" and (native_public or any(
            entity["evidence"].get("kind") == DAILY_SUSPENSION_KIND
            or (
                not historical and (
                    entity["classification"] == "DELISTED"
                    or str((entity["evidence"].get("native_tick") or {}).get("_probiga_observed_at") or "")[:10]
                    > session.isoformat()
                )
            )
            for entity in coverage_manifest["entities"]
        )):
            # The older DAT-only writer can start during Windows handoff.
            # Publish this new proof only after its Linux reader is active.
            _require_linux_daily_evidence_reader(
                connection, evidence="native public replacement" if native_public else "no-bar",
            )
        previous = None
        if native_public:
            previous = _native_public_replacement_predecessor(
                connection, manifest=coverage_manifest, day=session.isoformat(),
            )
            if len(payload) != 1 or payload[0][0] != native_public["stock_code"]:
                raise ValueError("native public replacement write scope differs")
            missing, corrections = set(), []
        elif repair:
            missing, corrections, previous = _historical_append_plan(connection, day=session.isoformat(), proof=coverage_manifest)
            if historical and corrections:
                coverage_manifest = seal_native_amount_corrections(coverage_manifest, prior_run_id=previous["run_id"],
                                                                    prior_manifest_hash="", corrections=corrections)
            repair_codes = missing | {row["stock_code"] for row in corrections}
            payload = [row for row in payload if row[0] in repair_codes]
        connection.exec_driver_sql(
            f"""
            CREATE TEMPORARY TABLE `{temp_table}` (
              stock_code VARCHAR(16) NOT NULL,
              short_name VARCHAR(64) NULL,
              trade_time DATETIME NOT NULL,
              trade_date DATE NOT NULL,
              k_type INT NOT NULL,
              adjust_type INT NOT NULL,
              `open` DECIMAL(50,6) NULL,
              `close` DECIMAL(50,6) NULL,
              high DECIMAL(50,6) NULL,
              low DECIMAL(50,6) NULL,
              volume DECIMAL(50,6) NULL,
              amount DECIMAL(50,6) NULL,
              `change` DECIMAL(50,6) NULL,
              change_pct DECIMAL(50,6) NULL,
              pre_close DECIMAL(50,6) NULL,
              etl_sync_at DATETIME NOT NULL,
              qmt_code VARCHAR(32) NULL,
              data_source VARCHAR(32) NOT NULL,
              source_time DATETIME NULL,
              received_at DATETIME NOT NULL,
              batch_id VARCHAR(64) NOT NULL,
              data_version VARCHAR(64) NOT NULL,
              quality_status VARCHAR(32) NOT NULL,
              permission_status VARCHAR(32) NOT NULL
            ) ENGINE=InnoDB
            """
        )
        insert_stage = (
            f"INSERT INTO `{temp_table}` ({columns}) VALUES "
        )
        for offset in range(0, len(payload), 500):
            chunk = payload[offset:offset + 500]
            statement = insert_stage + ",".join(
                [row_template] * len(chunk)
            )
            connection.exec_driver_sql(
                statement,
                tuple(value for row in chunk for value in row),
            )
        amount_repairs = [row for row in corrections if row.get("kind") != POST_CLOSE_KIND] if repair else []
        if amount_repairs:
            updated = connection.execute(text(f"""
                UPDATE `{TARGET_TABLE}` AS target
                JOIN `{temp_table}` AS stage
                  ON stage.stock_code=target.stock_code AND stage.trade_date=target.trade_date
                 AND stage.k_type=target.k_type AND stage.adjust_type=target.adjust_type
                SET target.amount=stage.amount, target.received_at=stage.received_at,
                    target.batch_id=stage.batch_id, target.data_version=stage.data_version
                WHERE target.trade_date=:day AND target.k_type=1 AND target.adjust_type=0
                  AND target.data_source=:source AND stage.amount>=:word AND target.amount=MOD(stage.amount,:word)
            """), {"day": session, "source": SOURCE, "word": NATIVE_AMOUNT_WORD})
            if updated.rowcount != len(amount_repairs):
                raise ValueError("native daily amount correction write count differs")
        close_repairs = [row for row in corrections if row.get("kind") == POST_CLOSE_KIND] if repair else []
        for revision in close_repairs:
            updated = connection.execute(text(f"""
                UPDATE `{TARGET_TABLE}` AS target
                JOIN `{temp_table}` AS stage
                  ON stage.stock_code=target.stock_code AND stage.trade_date=target.trade_date
                 AND stage.k_type=target.k_type AND stage.adjust_type=target.adjust_type
                SET target.amount=stage.amount, target.volume=stage.volume, target.received_at=stage.received_at,
                    target.batch_id=stage.batch_id, target.data_version=stage.data_version
                WHERE target.trade_date=:day AND target.stock_code=:stock_code
                  AND target.k_type=1 AND target.adjust_type=0 AND target.data_source=:source
                  AND target.amount=:previous_amount AND target.volume=:previous_volume
                  AND stage.amount=:amount AND stage.volume=:volume
            """), {"day": session, "source": SOURCE, **{key: revision[key] for key in (
                "stock_code", "previous_amount", "previous_volume", "amount", "volume",
            )}})
            if updated.rowcount != 1:
                raise ValueError("BSE post-close revision write count differs")
        if not repair or native_public:
            predecessor_guard = (
                "AND target.data_source=:prior_source AND target.quality_status=:prior_quality "
                "AND target.permission_status='SUPPORTED'"
                if native_public else ""
            )
            updated = connection.execute(
                text(
                f"""
                UPDATE `{TARGET_TABLE}` AS target
                JOIN `{temp_table}` AS stage
                  ON stage.stock_code=target.stock_code
                 AND stage.trade_date=target.trade_date
                 AND stage.k_type=target.k_type
                 AND stage.adjust_type=target.adjust_type
                SET target.short_name=stage.short_name,
                    target.trade_time=stage.trade_time,
                    target.`open`=stage.`open`,
                    target.`close`=stage.`close`,
                    target.high=stage.high,
                    target.low=stage.low,
                    target.volume=stage.volume,
                    target.amount=stage.amount,
                    target.`change`=stage.`change`,
                    target.change_pct=stage.change_pct,
                    target.pre_close=stage.pre_close,
                    target.etl_sync_at=stage.etl_sync_at,
                    target.qmt_code=stage.qmt_code,
                    target.data_source=stage.data_source,
                    target.source_time=stage.source_time,
                    target.received_at=stage.received_at,
                    target.batch_id=stage.batch_id,
                    target.data_version=stage.data_version,
                    target.quality_status=stage.quality_status,
                    target.permission_status=stage.permission_status
                WHERE target.trade_date=:d
                  AND target.k_type=1
                  AND target.adjust_type=0
                  {predecessor_guard}
                """
                ),
                {"d": session, "prior_source": SINA_PROVIDER, "prior_quality": SINA_QUALITY},
            )
            if native_public and updated.rowcount != 1:
                raise ValueError("native public replacement write count differs")
        if not native_public:
            inserted = connection.exec_driver_sql(
                f"""
                INSERT INTO `{TARGET_TABLE}` ({columns})
                SELECT {stage_columns}
                FROM `{temp_table}` AS stage
                LEFT JOIN `{TARGET_TABLE}` AS target
                  ON target.stock_code=stage.stock_code
                 AND target.trade_date=stage.trade_date
                 AND target.k_type=stage.k_type
                 AND target.adjust_type=stage.adjust_type
                WHERE target.id IS NULL
                """
            )
            if repair and inserted.rowcount != len(missing):
                raise ValueError("historical daily missing row insert count differs")
        if not repair:
            connection.execute(
                text(
                f"""
                DELETE target
                FROM `{TARGET_TABLE}` AS target
                LEFT JOIN `{temp_table}` AS stage
                  ON stage.stock_code=target.stock_code
                 AND stage.trade_date=target.trade_date
                 AND stage.k_type=target.k_type
                 AND stage.adjust_type=target.adjust_type
                WHERE target.trade_date=:d
                  AND target.k_type=1
                  AND target.adjust_type=0
                  AND stage.stock_code IS NULL
                """
                ),
                {"d": session},
            )
        fingerprint = _completion_fingerprint(connection, day=session.isoformat(), proof=coverage_manifest)
        if (fingerprint["row_count"] != expected_rows
                or fingerprint["code_count"] != len(covered_codes)):
            raise ValueError("daily database readback differs before completion")
        insert_manifest(connection, coverage_manifest)
        detail = {
            "readback_rows": fingerprint["row_count"],
            "readback_codes": fingerprint["code_count"],
            "row_hash": fingerprint["row_hash"],
            "eligible_code_count": int(eligible_code_count),
            "coverage": 1.0,
            "full_market_manifest_hash": coverage_manifest["manifest_hash"],
        }
        if previous is not None:
            detail["prior_completion"] = previous
        connection.execute(
            text(
                f"""
                INSERT INTO `{RECEIPT_TABLE}`
                  (dataset, trade_date, source_root, status, code_count,
                   row_count, content_sha256, run_id, started_at, finished_at,
                   detail_json)
                VALUES
                  (:dataset, :d, :root, 'COMPLETE', :codes, :rows, :sha,
                   :run_id, NOW(), NOW(), :detail)
                ON DUPLICATE KEY UPDATE
                  source_root=VALUES(source_root), status=VALUES(status),
                  code_count=VALUES(code_count), row_count=VALUES(row_count),
                  content_sha256=VALUES(content_sha256),
                  run_id=VALUES(run_id), finished_at=VALUES(finished_at),
                  started_at=IF(:repair,VALUES(started_at),started_at),
                  detail_json=VALUES(detail_json)
                """
            ),
            {
                "dataset": DATASET,
                "d": session,
                "root": str(source_root),
                "codes": len(covered_codes),
                "rows": expected_rows,
                "sha": content_sha256,
                "run_id": run_id,
                "repair": repair,
                "detail": json.dumps(detail, ensure_ascii=False, separators=(",", ":"), default=str),
            },
        )
        readback_at = connection.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one() if repair else now
        if load_daily_manifest(connection, session.isoformat(), decision_known_at=readback_at,
                               manifest_hash=coverage_manifest["manifest_hash"]) is None:
            raise ValueError("daily immutable proof readback failed before completion")
    result = {
        "row_count": expected_rows,
        "code_count": len(covered_codes),
        "readback_rows": int(fingerprint["row_count"]),
        "readback_codes": int(fingerprint["code_count"]),
    }
    if repair:
        receipt = load_receipt(engine, dataset=DATASET, trade_date=session.isoformat())
        if (not receipt or receipt.get("run_id") != run_id or receipt.get("status") != "COMPLETE"
                or receipt.get("detail", {}).get("full_market_manifest_hash") != coverage_manifest["manifest_hash"]):
            raise ValueError("historical daily committed receipt readback differs")
        result.update(run_id=run_id, manifest_hash=coverage_manifest["manifest_hash"],
                      finished_at=str(receipt["finished_at"]), appended_codes=sorted(missing),
                      retained_rows=previous["row_count"] - len(corrections),
                      amount_corrected_codes=[row["stock_code"] for row in amount_repairs])
        if native_public:
            result["native_replaced_code"] = native_public["qmt_code"]
        if close_repairs:
            result["post_close_scope_corrected_codes"] = [row["stock_code"] for row in close_repairs]
        if public:
            result["public_fallback_codes"] = sorted(public)
            result["bar_providers"] = {SOURCE: len(rows) - len(public), SINA_PROVIDER: len(public)}
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--latest-session", action="store_true")
    date_group.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument(
        "--datadir-root",
        default=r"D:\国金证券QMT交易端\datadir",
    )
    parser.add_argument("--replace-existing", action="store_true")
    parser.add_argument("--historical-lifecycle", type=Path,
                        help="JSON object of security codes to official CNINFO PDF locators; preserve existing rows")
    parser.add_argument("--archive-native-evidence", action="store_true",
                        help="Publish historical native evidence without changing the ordinary daily product")
    parser.add_argument("--repair-native-amount-only", action="store_true",
                        help="Correct truncated native amounts for one exact closed partition")
    parser.add_argument("--repair-native-post-close-code", default="",
                        help="Attest one published BSE block trade and correct the same exact closed partition")
    parser.add_argument("--repair-native-public-code", default="",
                        help="Supersede one exact corroborated public bar with its native QMT DAT record")
    parser.add_argument("--public-fallback-code", action="append", default=[],
                        help="Require corroborated public fallback for a missing code in one explicit historical session")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    exact_repair = bool(args.repair_native_amount_only or args.repair_native_post_close_code)
    native_public_repair = bool(args.repair_native_public_code)
    repair = bool(args.historical_lifecycle or exact_repair or native_public_repair)
    load_project_env()
    engine = (create_batch_engine(connect_args={"connect_timeout": 5, "read_timeout": 30, "write_timeout": 15})
              if repair else create_batch_engine())
    if args.latest_session:
        start = end = dt.date.fromisoformat(
            authoritative_closed_trade_date(engine)
        )
    else:
        if not args.start or not args.end:
            parser.error("--start and --end are required together")
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
    if args.public_fallback_code and (not args.historical_lifecycle or start != end):
        parser.error("public fallback requires a single explicit historical lifecycle session")
    if args.archive_native_evidence and (not args.historical_lifecycle or args.public_fallback_code):
        parser.error("native archive requires historical lifecycle evidence and exclusively native traded bars")
    locators = historical_documents = None
    deadline = None
    if args.historical_lifecycle:
        latest = dt.date.fromisoformat(authoritative_closed_trade_date(engine))
        if args.latest_session or args.replace_existing or not start <= end < latest or (end - start).days > 6:
            parser.error("historical lifecycle repair requires an explicit prior range of up to seven days")
        with args.historical_lifecycle.open("rb") as handle:
            raw_locators = handle.read(32 * 1024 + 1)
        if len(raw_locators) > 32 * 1024:
            parser.error("historical lifecycle locator file exceeds 32 KiB")
        deadline = time.monotonic() + 300
        locators = json.loads(raw_locators)
    if exact_repair or native_public_repair:
        latest = dt.date.fromisoformat(authoritative_closed_trade_date(engine))
        if (args.latest_session or args.replace_existing or args.historical_lifecycle or start != end or end > latest
                or sum(bool(mode) for mode in (args.repair_native_amount_only, args.repair_native_post_close_code,
                                               args.repair_native_public_code)) != 1):
            parser.error("exact native repair requires one explicit closed session and repair mode")
        if native_public_repair and not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", args.repair_native_public_code):
            parser.error("native public repair requires an exact six-digit QMT symbol with market suffix")
        deadline = time.monotonic() + 180
    root = Path(args.datadir_root)
    if not root.is_dir():
        parser.error(f"datadir root does not exist: {root}")
    sessions = _sessions(engine, start, end)
    if native_public_repair and sessions != [start]:
        parser.error("native public repair requires an authoritative trading session")
    universe = None if native_public_repair else load_universe(engine, start=start, end=end)
    short_names = {} if args.archive_native_evidence else _short_names(engine)
    results = []
    unavailable = {}
    for position, session in enumerate(sessions, 1):
        if repair:
            print(f"Daily repair {position}/{len(sessions)} {session}: validating DAT and authority", file=sys.stderr)
        if native_public_repair:
            run_id = new_run_id()
            proof, harvested, reused = _prepare_native_public_replacement(
                engine, session=session, symbol=args.repair_native_public_code, root=root, run_id=run_id,
            )
            if time.monotonic() >= deadline:
                raise TimeoutError("native public daily repair deadline exceeded before publication")
            if reused or not args.apply:
                results.append({"session": session.isoformat(), "status": "REUSED" if reused else "READY",
                                "native_replaced_code": args.repair_native_public_code,
                                "run_id": proof["run_id"], "manifest_hash": proof["manifest_hash"]})
                continue
            traded = {entity["stock_code"] for entity in proof["entities"]
                      if entity["classification"] == "TRADED"}
            published = _publish(
                engine, session=session, rows=harvested["rows"], covered_codes=traded, short_names=short_names,
                run_id=run_id, source_root=root, content_sha256=harvested["digest"],
                eligible_code_count=len(proof["entities"]), coverage_manifest=proof,
            )
            results.append({"session": session.isoformat(), "status": "COMPLETE", **published})
            continue
        if args.archive_native_evidence:
            archived = load_daily_native_evidence(engine, session.isoformat(), decision_known_at=dt.datetime.now(CST))
            if archived is not None:
                results.append({"session": session.isoformat(), **_archived_result(archived, reused=True)})
                continue
        sources = [] if args.archive_native_evidence else _existing_sources(engine, session)
        if sources and not args.replace_existing and not repair:
            results.append({
                "session": session.isoformat(),
                "status": "SKIP_EXISTING",
                "sources": sources,
            })
            continue
        harvested = _harvest(root, universe, session, deadline=deadline)
        reused = None if args.archive_native_evidence else _reusable_partition(engine, session=session, harvested=harvested)
        if reused is not None:
            results.append({"session": session.isoformat(), "status": "REUSED",
                            "run_id": reused["run_id"], "manifest_hash": reused["manifest_hash"]})
            continue
        if args.historical_lifecycle and historical_documents is None:
            budget = deadline - time.monotonic()
            if budget <= 0:
                raise TimeoutError("historical daily lifecycle deadline exceeded before evidence download")
            historical_documents = load_historical_documents(locators, timeout_seconds=min(75, budget))
        public, previous_day = {}, ""
        if args.public_fallback_code:
            previous = _sessions(engine, session - dt.timedelta(days=21), session)
            if len(previous) < 2 or previous[-1] != session:
                raise ValueError("authoritative public fallback previous session unavailable")
            previous_day = previous[-2].isoformat()
            if set(args.public_fallback_code) & harvested["codes"]:
                raise ValueError("historical public source requested for an existing native bar")
            budget = deadline - time.monotonic()
            if budget <= 0:
                raise TimeoutError("historical daily deadline exceeded before public source acquisition")
            public = load_public_bars(args.public_fallback_code, day=session.isoformat(), previous_day=previous_day,
                                      timeout_seconds=min(75, budget))
            harvested = _with_public_bars(harvested, public, session=session)
        eligible = universe.eligible_codes(session)
        if not harvested["codes"]:
            # A closed session with no matching bars cannot be published. In
            # particular, an empty or stale DAT cache is not per-stock IPO or
            # suspension evidence and must not launch those lookups.
            reason = (
                "QMT_NATIVE_DAILY_SOURCE_EMPTY "
                f"target={session.isoformat()} expected_codes={eligible} exact_codes=0"
            )
            unavailable[session] = reason
            results.append({"session": session.isoformat(), "status": "BLOCKED", "reason": reason})
            continue
        run_id = new_run_id()
        coverage_manifest = None
        if exact_repair or (historical_documents is None and sources == [SOURCE]):
            coverage_manifest = _exact_amount_repair_manifest(engine, session=session, harvested=harvested,
                                                              run_id=run_id, required=exact_repair,
                                                              post_close_code=args.repair_native_post_close_code, source_root=root)
        if coverage_manifest is None:
            coverage_manifest = _coverage_manifest(
                engine, session=session, harvested=harvested, run_id=run_id,
                historical_documents=historical_documents,
                historical_public_bars=public, previous_session=previous_day,
                archive_native_evidence=args.archive_native_evidence,
            )
        if args.archive_native_evidence:
            if time.monotonic() >= deadline:
                raise TimeoutError("historical native evidence deadline exceeded before publication")
            if args.apply:
                archived_result = _publish_native_archive(engine, manifest=coverage_manifest, source_root=root)
            else:
                archived_result = {
                    "status": "READY", "dataset": NATIVE_EVIDENCE_DATASET,
                    "manifest_hash": coverage_manifest["manifest_hash"],
                    "eligible_code_count": len(coverage_manifest["entities"]),
                    **native_evidence_fingerprint(coverage_manifest),
                }
            results.append({"session": session.isoformat(), **archived_result})
            continue
        if not args.apply:
            append_detail = {}
            if repair or coverage_manifest.get("native_amount_correction"):
                with engine.connect() as connection:
                    missing, corrections, prior = _historical_append_plan(connection, day=session.isoformat(), proof=coverage_manifest)
                append_detail = {"retained_rows": prior["row_count"] - len(corrections), "append_codes": sorted(missing),
                                 "amount_corrected_codes": [row["stock_code"] for row in corrections if row.get("kind") != POST_CLOSE_KIND],
                                 "no_bar_codes": {item["stock_code"]: item["classification"]
                                                  for item in coverage_manifest["entities"]
                                                  if item["classification"] != "TRADED"}}
                if args.repair_native_post_close_code:
                    append_detail["post_close_scope_corrected_codes"] = [row["stock_code"] for row in corrections
                                                                       if row.get("kind") == POST_CLOSE_KIND]
                if public:
                    append_detail["public_fallback_codes"] = sorted(public)
                    append_detail["bar_providers"] = {SOURCE: len(harvested["rows"]) - len(public),
                                                      SINA_PROVIDER: len(public)}
            results.append({
                "session": session.isoformat(),
                "status": "READY",
                "rows": len(harvested["rows"]),
                "codes": len(harvested["codes"]),
                **append_detail,
            })
            continue
        published = _publish(
            engine,
            session=session,
            rows=harvested["rows"],
            covered_codes=harvested["codes"],
            short_names=short_names,
            run_id=run_id,
            source_root=root,
            content_sha256=harvested["digest"],
            eligible_code_count=eligible,
            coverage_manifest=coverage_manifest,
        )
        results.append({
            "session": session.isoformat(),
            "status": "COMPLETE",
            **published,
        })
        if repair:
            print(f"Daily repair {position}/{len(sessions)} {session}: committed {run_id}", file=sys.stderr)
    payload = {
        "schema": "probiga.qmt-datadir-daily-import.v1",
        "source": SOURCE,
        "datadir_root": str(root),
        "applied": bool(args.apply),
        "failures": len(unavailable),
        "results": results,
    }
    if args.archive_native_evidence:
        payload["dataset"] = NATIVE_EVIDENCE_DATASET
    if not args.apply or repair:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 1 if unavailable else 0
    if unavailable:
        for reason in unavailable.values():
            print(reason, file=sys.stderr)
        task_result = build_unavailable_result(
            dataset=DATASET,
            sessions=[session.isoformat() for session in unavailable],
            reason=next(iter(unavailable.values())),
            build_sha=_build_sha(),
        )
        print(json.dumps(task_result, ensure_ascii=False, separators=(",", ":")))
        return 3
    task_result = _task_result(engine, sessions=sessions)
    print(json.dumps(task_result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

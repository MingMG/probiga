"""Exact local-cache daily coverage, including evidenced no-bar securities.

The cache is a source of bars, never an authority for missing securities.  A
publication accounts for the independent catalog with real bars, native
post-close suspension observations, or dated official suspension/listing evidence.  The
proof uses the existing append-only coverage ledger; no synthetic bar is made.
"""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
import json
import re
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.common.qmt_attestation_contract import canonical_digest
from server.common.official_historical_daily_evidence import (
    DAILY_SUSPENSION_KIND,
    historical_daily_evidence,
    official_daily_delisting_evidence,
    official_daily_suspension_evidence,
)
from server.common.historical_daily_bar_evidence import SINA_PROVIDER, SINA_QUALITY, verify_public_bar
from server.common.qmt_post_close_block_evidence import KIND as POST_CLOSE_KIND, verify_post_close_revision
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_trade_calendar import load_trade_calendar_receipt

SCHEMA = "probiga.qmt-datadir-daily-coverage.v1"
PROVIDER = "qmt_datadir"
NATIVE_EVIDENCE_DATASET = "stock_daily_native_evidence"
CHINA = ZoneInfo("Asia/Shanghai")
MANIFEST_TABLE = "qmt_history_coverage_manifest"
ENTITY_TABLE = "qmt_history_coverage_entity"
NATIVE_AMOUNT_WORD = 2 ** 32
ENTITY_INSERT_BATCH_SIZE = 256
_MAX_NATIVE_PUBLIC_REPLACEMENTS = 32
_SHA256 = re.compile(r"[a-f0-9]{64}")


def _scope(engine):
    return engine.connect() if callable(getattr(engine, "connect", None)) else nullcontext(engine)


def _stamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return result.astimezone(CHINA).replace(tzinfo=None) if result.tzinfo else result


def _zero(value: Any) -> bool:
    try:
        return value is not None and Decimal(str(value)).is_finite() and Decimal(str(value)) == 0
    except (InvalidOperation, ValueError, TypeError):
        return False


def _native_post_close_tick(snapshot: Mapping[str, Any], code: str, day: str, cutoff: datetime) -> tuple[dict, datetime]:
    if (snapshot.get("source") != "gj_big_qmt_inner"
            or snapshot.get("quote_acquisition_protocol") != "probiga.qmt-full-tick-poll.v1"
            or snapshot.get("quote_acquisition_mode") != "full_tick_poll"):
        raise ValueError("native suspension snapshot identity differs")
    generated = _stamp(snapshot.get("generated_at"))
    symbol = code + (".SH" if code.startswith("6") else ".BJ" if code.startswith(("4", "8", "9")) else ".SZ")
    raw = dict((snapshot.get("quotes") or {}).get(symbol) or {})
    observed = _stamp(raw.get("_probiga_observed_at"))
    event = datetime.fromtimestamp(float(raw["time"]) / 1000, CHINA).replace(tzinfo=None)
    closed = datetime.combine(datetime.fromisoformat(day).date(), time(15, 0))
    if (not closed <= observed <= generated <= cutoff
            or event.date().isoformat() != day
            or raw.get("_probiga_acquisition_method") != "ContextInfo.get_full_tick"):
        raise ValueError("target-day native post-close suspension is unproven: " + code)
    # A later poll can return the previous session's final native event.
    # Keep the real observation time so the receipt is unavailable before recovery.
    if observed.date() != closed.date() and (
        not closed <= event <= observed or not str(snapshot.get("batch_id") or "").strip()
    ):
        raise ValueError("target-day native post-close suspension is unproven: " + code)
    return raw, generated


def suspension_evidence(snapshot: Mapping[str, Any], code: str, day: str, cutoff: datetime) -> dict:
    """Prove the installed QMT full-tick suspension semantics at the close."""
    raw, generated = _native_post_close_tick(snapshot, code, day, cutoff)
    if (type(raw.get("stockStatus")) is not int or raw["stockStatus"] != 7
            or not all(_zero(raw.get(field)) for field in ("open", "high", "low", "volume", "amount"))):
        raise ValueError("target-day native post-close suspension is unproven: " + code)
    return {"kind": "NATIVE_QMT_SUSPENSION", "stock_code": code, "target_date": day,
            **_snapshot_evidence(snapshot, raw, generated)}


def _snapshot_evidence(snapshot: Mapping[str, Any], raw: dict, generated: datetime) -> dict:
    return {"snapshot_generated_at": generated.isoformat(" "),
            "snapshot_batch_id": str(snapshot.get("batch_id") or ""), "native_tick": raw}


def _official_suspension_with_native_tick(
    snapshot: Mapping[str, Any], documents: list[dict], code: str, day: str, cutoff: datetime,
) -> dict:
    raw, generated = _native_post_close_tick(snapshot, code, day, cutoff)
    if (not str(snapshot.get("batch_id") or "").strip()
            or not all(_zero(raw.get(field)) for field in ("open", "high", "low", "volume", "amount"))):
        raise ValueError("official daily suspension requires target-day native zero turnover: " + code)
    official = official_daily_suspension_evidence(documents, stock_code=code, day=day, cutoff=cutoff)
    return {**official, **_snapshot_evidence(snapshot, raw, generated)}


def listing_evidence(value: Mapping[str, Any], code: str, day: str) -> dict:
    """Validate a preserved exchange/issuer disclosure; never infer from no bars."""
    from server.common.official_listing_evidence import validate_official_listing_evidence
    result = validate_official_listing_evidence(value, stock_code=code, target_date=day)
    return {**result, "kind": "EXCHANGE_LISTING_DISCLOSURE", "stock_code": code,
            "target_date": day}


def canonical_bar(row: Mapping[str, Any]) -> dict:
    fields = ("open", "close", "high", "low", "volume", "amount", "pre_close")
    result = {"stock_code": str(row["stock_code"]), "trade_date": str(row["trade_date"])[:10]}
    for field in fields:
        value = Decimal(str(row[field]))
        if not value.is_finite() or value < 0 or (field not in {"volume", "amount"} and value <= 0):
            raise ValueError("invalid native daily field: " + field)
        result[field] = format(value.quantize(Decimal("0.000001")), "f")
    if (Decimal(result["high"]) < max(Decimal(result["open"]), Decimal(result["close"]))
            or Decimal(result["low"]) > min(Decimal(result["open"]), Decimal(result["close"]))):
        raise ValueError("invalid native daily OHLC range")
    return result


def native_amount_correction(old: Mapping[str, Any], corrected: Mapping[str, Any] | None) -> dict | None:
    if corrected is None or any(old[key] != corrected[key] for key in old if key != "amount"):
        return None
    old_amount, new_amount = Decimal(old["amount"]), Decimal(corrected["amount"])
    if (not NATIVE_AMOUNT_WORD <= new_amount < NATIVE_AMOUNT_WORD ** 2 or new_amount != int(new_amount)
            or old_amount != new_amount % NATIVE_AMOUNT_WORD):
        return None
    return {"stock_code": old["stock_code"], "previous_amount": old["amount"], "amount": corrected["amount"]}


def seal_native_amount_corrections(manifest: dict, *, prior_run_id: str, prior_manifest_hash: str,
                                   corrections: Sequence[dict]) -> dict:
    if not corrections:
        return manifest
    core = dict(manifest)
    core.pop("manifest_hash", None)
    core["native_amount_correction"] = {
        "kind": ("QMT_DATADIR_POST_CLOSE_SCOPE" if any(row.get("kind") == POST_CLOSE_KIND for row in corrections)
                 else "QMT_DATADIR_UINT64_AMOUNT"), "prior_run_id": prior_run_id,
        "prior_manifest_hash": prior_manifest_hash,
        "rows": sorted(corrections, key=lambda row: row["stock_code"]),
    }
    return {**core, "manifest_hash": canonical_digest(core)}


def validate_native_revision(old, new, correction, *, prior_hash, cutoff):
    if correction.get("kind") == POST_CLOSE_KIND:
        return verify_post_close_revision(old, new, correction["evidence"], prior_hash=prior_hash,
                                          cutoff=cutoff) == correction
    return native_amount_correction(old, new) == correction


def seal_native_public_replacement(
    prior: Mapping[str, Any], *, prior_completion: Mapping[str, Any], bar: Mapping[str, Any],
    run_id: str, qmt_code: str, native_event_at: datetime | str, observed_at: datetime | str,
    native_record_sha256: str,
) -> dict:
    """Replace one corroborated public bar while retaining all unrelated evidence."""
    native = canonical_bar(bar)
    code, day = native["stock_code"], prior["trade_date"]
    fallback = dict(prior.get("historical_public_bars") or {})
    if code not in fallback:
        raise ValueError("native public replacement requires the exact public predecessor bar")
    del fallback[code]
    core = {key: value for key, value in prior.items() if key not in {
        "manifest_hash", "native_amount_correction", "native_public_replacement", "historical_public_bars",
    }}
    replacement = {"stock_code": code, "classification": "TRADED", "evidence": native}
    core.update(
        run_id=run_id, captured_at=_stamp(observed_at).isoformat(" "),
        entities=[{**replacement, "row_hash": canonical_digest(replacement)} if item["stock_code"] == code else item
                  for item in prior["entities"]],
        native_public_replacement={
            "stock_code": code, "qmt_code": qmt_code, "trade_date": day, "prior_run_id": prior["run_id"],
            "prior_manifest_hash": prior["manifest_hash"],
            "prior_completion_sha256": canonical_digest(prior_completion),
            "native_event_at": _stamp(native_event_at).isoformat(" "),
            "observed_at": _stamp(observed_at).isoformat(" "),
            "native_record_sha256": native_record_sha256,
            "native_bar_sha256": canonical_digest(native),
        },
    )
    if fallback:
        core["historical_public_bars"] = fallback
    _validate_native_public_replacement(core)
    return {**core, "manifest_hash": canonical_digest(core)}


def _validate_native_public_replacement(manifest: Mapping[str, Any], *, catalog=None) -> None:
    proof = manifest["native_public_replacement"]
    day = manifest["trade_date"]
    code = proof["stock_code"]
    matches = [item for item in manifest["entities"] if item["stock_code"] == code]
    if (manifest.get("historical_reconstruction") is not True or manifest.get("native_amount_correction")
            or proof["trade_date"] != day or not re.fullmatch(r"[0-9]{6}", code)
            or proof["qmt_code"] not in {f"{code}.SH", f"{code}.SZ", f"{code}.BJ"}
            or (catalog is not None and not any(member.get("qmt_code") == proof["qmt_code"]
                                                for member in catalog.members))
            or code in (manifest.get("historical_public_bars") or {})
            or len(matches) != 1 or matches[0]["classification"] != "TRADED"
            or matches[0]["evidence"]["trade_date"] != day
            or canonical_digest(matches[0]["evidence"]) != proof["native_bar_sha256"]
            or Decimal(matches[0]["evidence"]["volume"]) <= 0
            or not proof["prior_run_id"] or proof["prior_run_id"] == manifest["run_id"]
            or any(not _SHA256.fullmatch(proof[key]) for key in (
                "prior_manifest_hash", "prior_completion_sha256", "native_record_sha256",
            ))
            or _stamp(proof["native_event_at"]).date().isoformat() != day
            or _stamp(proof["observed_at"]) != _stamp(manifest["captured_at"])
            or _stamp(proof["observed_at"]).date().isoformat() <= day):
        raise ValueError("native public replacement source or scope differs")


def _delisting_evidence(catalog, documents: list[dict], *, code: str, day: str, cutoff: datetime) -> dict:
    expiry_dates = {str(member.get("expire_date") or "") for member in catalog.members
                    if member["stock_code"] == code}
    if expiry_dates != {day}:
        raise ValueError("daily delisting differs from catalog expiry: " + code)
    return official_daily_delisting_evidence(documents, stock_code=code, day=day, cutoff=cutoff)


def build_manifest(*, catalog, calendar, day: str, rows: Sequence[Mapping[str, Any]],
                   run_id: str, captured_at: datetime, snapshot: Mapping[str, Any] | None = None,
                   listings: Mapping[str, Any] | None = None,
                   daily_suspension_documents: Mapping[str, list[dict]] | None = None,
                   daily_delisting_documents: Mapping[str, list[dict]] | None = None,
                   historical_documents: Mapping[str, list[dict]] | None = None,
                   historical_public_bars: Mapping[str, dict] | None = None,
                   previous_session: str = "", native_content_sha256: str = "") -> dict:
    captured = _stamp(captured_at).replace(microsecond=0)
    if captured < datetime.combine(datetime.fromisoformat(day).date(), time(15, 35)):
        raise ValueError("daily source is not final")
    if calendar.sessions_between(day, day) != [day]:
        raise ValueError("daily target is not an authoritative session")
    expected = sorted(catalog.eligible_codes(day))
    bars = [canonical_bar(row) for row in rows]
    by_code = {row["stock_code"]: row for row in bars}
    if not expected or len(by_code) != len(bars) or set(by_code) - set(expected):
        raise ValueError("daily rows differ from independent catalog")
    if any(row["trade_date"] != day for row in bars):
        raise ValueError("daily row date differs")
    if historical_documents is not None:
        if captured.date().isoformat() <= day or set(historical_documents) != set(expected) - by_code.keys():
            raise ValueError("historical daily lifecycle inventory differs")
    suspensions = dict(daily_suspension_documents or {})
    if suspensions and (historical_documents is not None
                        or not set(suspensions) <= set(expected) - by_code.keys() - set(listings or {})):
        raise ValueError("official daily suspension inventory differs")
    delistings = dict(daily_delisting_documents or {})
    if delistings and (historical_documents is not None or not set(delistings) <=
                       set(expected) - by_code.keys() - set(listings or {}) - set(suspensions)):
        raise ValueError("official daily delisting inventory differs")
    public = dict(historical_public_bars or {})
    if public:
        if (historical_documents is None or not set(public) <= by_code.keys()
                or calendar.sessions_between(previous_session, day) != [previous_session, day]):
            raise ValueError("historical public bar calendar or inventory differs")
        for code, evidence in public.items():
            if verify_public_bar(evidence, stock_code=code, day=day, previous_day=previous_session, cutoff=captured) != by_code[code]:
                raise ValueError("historical public source bar differs from exact publication")
    entities = []
    for code in expected:
        if code in by_code:
            kind, evidence = "TRADED", by_code[code]
        elif historical_documents is not None:
            try:
                kind, evidence = historical_daily_evidence(historical_documents[code], stock_code=code,
                                                           day=day, cutoff=captured)
            except ValueError as exc:
                raise ValueError(f"historical daily lifecycle {day} {code}: {exc}") from exc
        elif code in (listings or {}):
            kind, evidence = "NOT_LISTED", listing_evidence(listings[code], code, day)
        elif code in delistings:
            kind = "DELISTED"
            evidence = _delisting_evidence(catalog, delistings[code], code=code, day=day, cutoff=captured)
        elif code in suspensions:
            kind = "NO_TRADE"
            evidence = _official_suspension_with_native_tick(
                snapshot or {}, suspensions[code], code, day, captured,
            )
        else:
            kind, evidence = "NO_TRADE", suspension_evidence(snapshot or {}, code, day, captured)
        core = {"stock_code": code, "classification": kind, "evidence": evidence}
        entities.append({**core, "row_hash": canonical_digest(core)})
    core = {"schema": SCHEMA, "dataset": "stock_daily", "provider": PROVIDER,
            "trade_date": day, "status": "EXACT", "strategy_eligible": True,
            "run_id": run_id, "captured_at": captured.isoformat(" "),
            "catalog_batch_id": catalog.batch_id, "catalog_manifest_hash": catalog.manifest_hash,
            "catalog_member_set_hash": catalog.member_set_hash,
            "calendar_batch_id": calendar.batch_id, "calendar_manifest_hash": calendar.manifest_hash,
            "entities": entities}
    if historical_documents is not None:
        core["historical_reconstruction"] = True
    if public:
        core.update(historical_public_bars=public, previous_session=previous_session)
    if native_content_sha256:
        core.update(dataset=NATIVE_EVIDENCE_DATASET, native_content_sha256=native_content_sha256)
        native_evidence_fingerprint(core)
    return {**core, "manifest_hash": canonical_digest(core)}


def validate_manifest(manifest: Mapping[str, Any], *, catalog, calendar) -> dict:
    original = dict(manifest)
    entities = original.get("entities") or []
    snapshot = {"source": "gj_big_qmt_inner", "quote_acquisition_protocol": "probiga.qmt-full-tick-poll.v1",
                "quote_acquisition_mode": "full_tick_poll", "quotes": {}}
    historical = original.get("historical_reconstruction") is True
    public = original.get("historical_public_bars") or {}
    previous_session = original.get("previous_session", "")
    correction = original.get("native_amount_correction")
    if "native_public_replacement" in original:
        _validate_native_public_replacement(original, catalog=catalog)
    if original.get("dataset") == NATIVE_EVIDENCE_DATASET:
        native_evidence_fingerprint(original)
    no_trade = [entity for entity in entities if entity.get("classification") == "NO_TRADE"] if not historical else []
    # Each entity binds its own snapshot envelope; validate those separately
    # rather than assigning one generated timestamp to unrelated observations.
    for entity in no_trade:
        proof = entity["evidence"]
        code = entity["stock_code"]
        day = str(original["trade_date"])
        cutoff = _stamp(original["captured_at"])
        symbol = code + (".SH" if code.startswith("6") else ".BJ" if code.startswith(("4", "8", "9")) else ".SZ")
        envelope = {**snapshot, "generated_at": proof["snapshot_generated_at"],
                    "batch_id": proof["snapshot_batch_id"], "quotes": {symbol: proof["native_tick"]}}
        if proof.get("kind") == DAILY_SUSPENSION_KIND:
            if _official_suspension_with_native_tick(envelope, proof["documents"], code, day, cutoff) != proof:
                raise ValueError("official daily suspension evidence differs")
        elif suspension_evidence(envelope, code, day, cutoff) != proof:
            raise ValueError("native suspension evidence differs")
    # Rebuild canonical entities, with each no-bar proof checked above.
    expected = sorted(catalog.eligible_codes(str(original["trade_date"])))
    if [entity.get("stock_code") for entity in entities] != expected or not expected:
        raise ValueError("daily manifest catalog inventory differs")
    if (original.get("schema") != SCHEMA or original.get("provider") != PROVIDER
            or original.get("dataset") not in {"stock_daily", NATIVE_EVIDENCE_DATASET}
            or original.get("status") != "EXACT"
            or original.get("strategy_eligible") is not True
            or original.get("catalog_batch_id") != catalog.batch_id
            or original.get("catalog_manifest_hash") != catalog.manifest_hash
            or original.get("catalog_member_set_hash") != catalog.member_set_hash
            or original.get("calendar_batch_id") != calendar.batch_id
            or original.get("calendar_manifest_hash") != calendar.manifest_hash
            or calendar.sessions_between(str(original["trade_date"]), str(original["trade_date"])) != [original["trade_date"]]):
        raise ValueError("daily manifest authority roots differ")
    if _stamp(original["captured_at"]) < datetime.combine(datetime.fromisoformat(original["trade_date"]).date(), time(15, 35)):
        raise ValueError("daily manifest precedes final source time")
    if historical and _stamp(original["captured_at"]).date().isoformat() <= original["trade_date"]:
        raise ValueError("historical daily manifest was backdated")
    if public and (not historical or not set(public) <= {item["stock_code"] for item in entities
                                                           if item["classification"] == "TRADED"}
                   or calendar.sessions_between(previous_session, original["trade_date"]) != [previous_session, original["trade_date"]]):
        raise ValueError("historical public bar calendar or inventory differs")
    for entity in entities:
        core = {key: entity[key] for key in ("stock_code", "classification", "evidence")}
        if entity.get("row_hash") != canonical_digest(core):
            raise ValueError("daily entity hash differs")
        if entity["classification"] == "TRADED":
            row = canonical_bar(entity["evidence"])
            if row != entity["evidence"] or row["stock_code"] != entity["stock_code"] or row["trade_date"] != original["trade_date"]:
                raise ValueError("daily entity bar differs")
            if entity["stock_code"] in public and verify_public_bar(
                public[entity["stock_code"]], stock_code=entity["stock_code"], day=original["trade_date"],
                previous_day=previous_session, cutoff=_stamp(original["captured_at"]),
            ) != row:
                raise ValueError("historical public source bar differs from exact publication")
        elif historical:
            classification, proof = historical_daily_evidence(
                entity["evidence"]["documents"], stock_code=entity["stock_code"],
                day=original["trade_date"], cutoff=_stamp(original["captured_at"]),
            )
            if classification != entity["classification"] or proof != entity["evidence"]:
                raise ValueError("historical daily lifecycle evidence differs")
        elif entity["classification"] == "NOT_LISTED":
            proof = listing_evidence(entity["evidence"], entity["stock_code"], original["trade_date"])
            if proof != entity["evidence"]:
                raise ValueError("daily listing evidence differs")
        elif entity["classification"] == "DELISTED":
            proof = _delisting_evidence(
                catalog, entity["evidence"]["documents"], code=entity["stock_code"],
                day=original["trade_date"], cutoff=_stamp(original["captured_at"]),
            )
            if proof != entity["evidence"]:
                raise ValueError("daily delisting evidence differs")
        elif entity["classification"] != "NO_TRADE":
            raise ValueError("unresolved daily entity")
    if correction is not None:
        corrected = {item["stock_code"]: item["evidence"] for item in entities
                     if item["classification"] == "TRADED" and item["stock_code"] not in public}
        rows = correction.get("rows") or []
        codes = [row["stock_code"] for row in rows]
        post_close = sum(row.get("kind") == POST_CLOSE_KIND for row in rows)
        kind = "QMT_DATADIR_POST_CLOSE_SCOPE" if post_close else "QMT_DATADIR_UINT64_AMOUNT"
        if (not rows or sorted(set(codes)) != codes or not set(codes) <= corrected.keys()
                or post_close > 1 or (post_close and (historical or public))
                or correction.get("kind") != kind or not correction.get("prior_run_id")
                or correction["prior_run_id"] == original["run_id"]
                or (not historical and len(correction.get("prior_manifest_hash") or "") != 64)):
            raise ValueError("native daily amount correction inventory differs")
        for row in rows:
            bar = corrected[row["stock_code"]]
            old = {**bar, "amount": row["previous_amount"], "volume": row.get("previous_volume", bar["volume"])}
            if not validate_native_revision(old, bar, row, prior_hash=correction["prior_manifest_hash"],
                                            cutoff=original["captured_at"]):
                raise ValueError("native daily amount correction evidence differs")
    supplied = original.pop("manifest_hash", None)
    if supplied != canonical_digest(original):
        raise ValueError("daily manifest hash differs")
    return {**original, "manifest_hash": supplied}


def insert_manifest(connection, manifest: dict) -> None:
    """Append one immutable proof in the same transaction as its daily rows."""
    created_at = connection.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()
    entities = manifest["entities"]
    traded = [entity["stock_code"] for entity in entities if entity["classification"] == "TRADED"]
    absent = [entity["stock_code"] for entity in entities if entity["classification"] != "TRADED"]
    params = {**manifest, "schema_version": SCHEMA, "period": "1d", "source_batch_id": manifest["run_id"],
              "grid_profile": "", "expected_entity_count": len(entities), "entity_count": len(entities),
              "expected_traded_count": len(traded), "actual_traded_count": len(traded),
              "no_trade_count": len(absent), "bar_count": len(traded),
              "expected_entity_set_hash": canonical_digest([entity["stock_code"] for entity in entities]),
              "expected_traded_set_hash": canonical_digest(traded), "actual_traded_set_hash": canonical_digest(traded),
              "no_trade_set_hash": canonical_digest(absent),
              "entity_root_hash": canonical_digest([entity["row_hash"] for entity in entities]), "reason_count": 0,
              "manifest_json": json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
              "created_at": created_at}
    keys = ("manifest_hash", "schema_version", "dataset", "period", "provider", "trade_date", "status",
            "strategy_eligible", "run_id", "catalog_batch_id", "catalog_manifest_hash", "calendar_batch_id",
            "calendar_manifest_hash", "source_batch_id", "grid_profile", "expected_entity_count", "entity_count",
            "expected_traded_count", "actual_traded_count", "no_trade_count", "bar_count", "expected_entity_set_hash",
            "expected_traded_set_hash", "actual_traded_set_hash", "no_trade_set_hash", "entity_root_hash", "reason_count",
            "captured_at", "manifest_json", "created_at")
    connection.execute(
        text(f"INSERT INTO {MANIFEST_TABLE} ({','.join(keys)}) VALUES ({','.join(':'+key for key in keys)})"),
        params,
    )
    persisted_entities = []
    for entity in entities:
        traded_bar = entity["classification"] == "TRADED"
        day = manifest["trade_date"] if traded_bar else ""
        persisted_entities.append({"manifest_hash": manifest["manifest_hash"], "stock_code": entity["stock_code"],
                            "state": entity["classification"], "bars": int(traded_bar),
                            "times": canonical_digest([day] if traded_bar else []), "day": day,
                            "source_hash": canonical_digest(entity["evidence"]), "row_hash": entity["row_hash"],
                            "created_at": created_at})
    # PyMySQL batches only VALUES tuples composed entirely of bind parameters.
    statement = text(f"""
        INSERT INTO {ENTITY_TABLE}
          (manifest_hash,stock_code,expected_state,classification,bar_count,time_set_hash,
           first_time,last_time,source_row_hash,row_hash,created_at)
        VALUES
          (:manifest_hash,:stock_code,:state,:state,:bars,:times,:day,:day,:source_hash,:row_hash,:created_at)
    """)
    for offset in range(0, len(persisted_entities), ENTITY_INSERT_BATCH_SIZE):
        connection.execute(statement, persisted_entities[offset:offset + ENTITY_INSERT_BATCH_SIZE])


def native_evidence_fingerprint(manifest: Mapping[str, Any]) -> dict:
    """Fingerprint full immutable native evidence without consulting canonical vendor rows."""
    if (manifest.get("dataset") != NATIVE_EVIDENCE_DATASET
            or manifest.get("historical_reconstruction") is not True
            or manifest.get("historical_public_bars") or manifest.get("native_amount_correction")
            or manifest.get("native_public_replacement")
            or not _SHA256.fullmatch(str(manifest.get("native_content_sha256") or ""))):
        raise ValueError("historical native evidence authority differs")
    bars = [item for item in manifest["entities"] if item["classification"] == "TRADED"]
    if not bars or any(Decimal(item["evidence"]["volume"]) <= 0 for item in bars):
        raise ValueError("historical native evidence requires actual traded bars")
    return {"row_count": len(bars), "code_count": len(bars),
            "row_hash": canonical_digest([item["row_hash"] for item in bars])}


def _daily_manifest_completion(connection, day: str, manifest: Mapping[str, Any]) -> tuple[dict, bool]:
    from server.common.qmt_datadir_result import load_receipt

    dataset = manifest["dataset"]
    receipt = load_receipt(connection, dataset=dataset, trade_date=day)
    archived = False
    for _ in range(_MAX_NATIVE_PUBLIC_REPLACEMENTS + 1):
        detail = (receipt or {}).get("detail") or {}
        if (receipt is None or receipt.get("status") != "COMPLETE"
                or receipt.get("dataset") != dataset or str(receipt.get("trade_date"))[:10] != day
                or not _SHA256.fullmatch(str(detail.get("full_market_manifest_hash") or ""))):
            break
        if receipt["run_id"] == manifest["run_id"] and detail["full_market_manifest_hash"] == manifest["manifest_hash"]:
            traded_count = sum(item["classification"] == "TRADED" for item in manifest["entities"])
            if (detail.get("coverage") != 1.0 or any(value != traded_count for value in (
                receipt.get("row_count"), receipt.get("code_count"), detail.get("readback_rows"), detail.get("readback_codes"),
            ))):
                break
            replacement = manifest.get("native_public_replacement")
            if replacement and receipt.get("content_sha256") != replacement["native_record_sha256"]:
                raise ValueError("native public replacement receipt source digest differs")
            if dataset == NATIVE_EVIDENCE_DATASET and (
                receipt.get("content_sha256") != manifest["native_content_sha256"]
                or detail.get("row_hash") != native_evidence_fingerprint(manifest)["row_hash"]
                or detail.get("eligible_code_count") != len(manifest["entities"])
            ):
                raise ValueError("historical native evidence completion fingerprint differs")
            if _stamp(manifest["captured_at"]) > _stamp(receipt["finished_at"]):
                raise ValueError("repaired daily publication completion time differs")
            return receipt, archived
        preceding = detail.get("prior_completion")
        if dataset != "stock_daily" or not isinstance(preceding, dict):
            break
        link_json = connection.execute(text(f"""
            SELECT JSON_EXTRACT(manifest_json, '$.native_public_replacement') FROM {MANIFEST_TABLE}
            WHERE manifest_hash=:hash AND trade_date=:day AND run_id=:run_id
              AND schema_version=:schema AND dataset='stock_daily' AND provider=:provider
              AND status='EXACT' AND strategy_eligible=1
        """), {"hash": detail["full_market_manifest_hash"], "day": day, "run_id": receipt["run_id"],
               "schema": SCHEMA, "provider": PROVIDER}).scalar_one_or_none()
        link = json.loads(link_json) if isinstance(link_json, str) else link_json
        if (not isinstance(link, dict) or link.get("trade_date") != day
                or link.get("native_record_sha256") != receipt.get("content_sha256")
                or link.get("prior_run_id") != preceding.get("run_id")
                or link.get("prior_manifest_hash") != (preceding.get("detail") or {}).get("full_market_manifest_hash")
                or link.get("prior_completion_sha256") != canonical_digest(preceding)
                or _stamp(preceding["finished_at"]) > _stamp(link["observed_at"])
                or _stamp(link["observed_at"]) > _stamp(receipt["finished_at"])):
            break
        receipt = preceding
        archived = True
    raise ValueError("repaired daily publication completion receipt differs")


def load_daily_manifest_completion(engine, day: str, *, manifest: Mapping[str, Any],
                                   decision_known_at: datetime | str) -> dict | None:
    """Resolve an exact validated manifest's own durable completion at its cutoff."""
    with _scope(engine) as connection:
        receipt, _ = _daily_manifest_completion(connection, day, manifest)
    return receipt if _stamp(receipt["finished_at"]) <= _stamp(decision_known_at) else None


def load_daily_manifest(engine, day: str, *, decision_known_at: datetime | str,
                        manifest_hash: str = "", verify_rows: bool = True) -> dict | None:
    return _load_daily_manifest(engine, day, decision_known_at=decision_known_at, manifest_hash=manifest_hash,
                                verify_rows=verify_rows, dataset="stock_daily")


def load_daily_native_evidence(engine, day: str, *,
                               decision_known_at: datetime | str, manifest_hash: str = "") -> tuple[dict, dict] | None:
    """Read a historical native-only market proof and its own exact archival completion."""
    with _scope(engine) as connection:
        manifest = _load_daily_manifest(connection, day, decision_known_at=decision_known_at, manifest_hash=manifest_hash,
                                        verify_rows=False, dataset=NATIVE_EVIDENCE_DATASET)
        if manifest is None:
            return None
        receipt, _ = _daily_manifest_completion(connection, day, manifest)
        return manifest, receipt


def _load_daily_manifest(engine, day: str, *, decision_known_at: datetime | str, dataset: str,
                         manifest_hash: str = "", verify_rows: bool) -> dict | None:
    cutoff = _stamp(decision_known_at)
    with _scope(engine) as connection:
        condition = " AND manifest_hash=:hash" if manifest_hash else ""
        row = connection.execute(text(f"SELECT manifest_hash,manifest_json,schema_version,dataset,provider,trade_date,status,strategy_eligible,run_id,captured_at,catalog_batch_id,catalog_manifest_hash,calendar_batch_id,calendar_manifest_hash FROM {MANIFEST_TABLE} WHERE schema_version=:schema AND dataset=:dataset AND provider=:provider AND trade_date=:day AND status='EXACT' AND captured_at<=:cutoff{condition} ORDER BY captured_at DESC,manifest_hash DESC LIMIT 1"),
                                 {"schema": SCHEMA, "dataset": dataset, "provider": PROVIDER, "day": day,
                                  "cutoff": cutoff, "hash": manifest_hash}).mappings().first()
        if row is None:
            return None
        manifest = json.loads(str(row["manifest_json"]))
        if manifest.get("manifest_hash") != row["manifest_hash"]:
            raise ValueError("persisted daily manifest identity differs")
        if (manifest.get("trade_date") != day or _stamp(manifest["captured_at"]) > cutoff
                or manifest.get("schema") != row["schema_version"]
                or int(row["strategy_eligible"]) != 1 or manifest.get("run_id") != row["run_id"]
                or _stamp(manifest["captured_at"]) != _stamp(row["captured_at"])
                or str(row["trade_date"])[:10] != day
                or any(manifest.get(key) != row[key] for key in (
                    "dataset", "provider", "status", "catalog_batch_id", "catalog_manifest_hash",
                    "calendar_batch_id", "calendar_manifest_hash"))):
            raise ValueError("persisted daily manifest authority metadata differs")
        catalog = load_stock_catalog(connection, batch_id=manifest["catalog_batch_id"], decision_known_at=cutoff)
        calendar = load_trade_calendar_receipt(connection, batch_id=manifest["calendar_batch_id"],
                                               start_date=manifest.get("previous_session", day),
                                               end_date=day, decision_known_at=cutoff)
        validate_manifest(manifest, catalog=catalog, calendar=calendar)
        archived = False
        if manifest.get("historical_reconstruction") is True or manifest.get("native_amount_correction"):
            receipt, archived = _daily_manifest_completion(connection, day, manifest)
            detail = receipt["detail"]
            replacement = manifest.get("native_public_replacement")
            if _stamp(receipt["finished_at"]) > cutoff:
                if replacement and not manifest_hash:
                    return load_daily_manifest(connection, day, decision_known_at=cutoff,
                                               manifest_hash=replacement["prior_manifest_hash"], verify_rows=verify_rows)
                return None
            if replacement:
                prior = detail.get("prior_completion")
                previous = load_daily_manifest(connection, day, decision_known_at=cutoff,
                                               manifest_hash=replacement["prior_manifest_hash"], verify_rows=False)
                if (not isinstance(prior, dict) or not previous
                        or prior.get("run_id") != previous["run_id"]
                        or canonical_digest(prior) != replacement["prior_completion_sha256"]
                        or _stamp(prior["finished_at"]) > _stamp(replacement["observed_at"])
                        or _stamp(previous["captured_at"]) >= _stamp(replacement["observed_at"])):
                    raise ValueError("native public replacement predecessor differs")
                native = next(item["evidence"] for item in manifest["entities"]
                              if item["stock_code"] == replacement["stock_code"])
                expected = seal_native_public_replacement(
                    previous, prior_completion=prior, bar=native, run_id=manifest["run_id"],
                    qmt_code=replacement["qmt_code"],
                    native_event_at=replacement["native_event_at"], observed_at=replacement["observed_at"],
                    native_record_sha256=replacement["native_record_sha256"],
                )
                if manifest != expected:
                    raise ValueError("native public replacement changes unrelated evidence")
            correction = manifest.get("native_amount_correction")
            if correction:
                prior = detail.get("prior_completion") or {}
                previous_hash = correction["prior_manifest_hash"]
                if (prior.get("status") != "COMPLETE" or prior.get("run_id") != correction["prior_run_id"]
                        or str((prior.get("detail") or {}).get("full_market_manifest_hash") or "") != previous_hash):
                    raise ValueError("native daily amount correction prior receipt differs")
                if previous_hash:
                    previous = load_daily_manifest(connection, day, decision_known_at=cutoff,
                                                   manifest_hash=previous_hash, verify_rows=False)
                    if (not previous or previous.get("native_amount_correction") or previous.get("historical_reconstruction")
                            or previous["run_id"] != correction["prior_run_id"]
                            or not _stamp(previous["captured_at"]) < _stamp(manifest["captured_at"])
                            or any(previous[key] != manifest[key] for key in (
                                "catalog_batch_id", "catalog_manifest_hash", "catalog_member_set_hash",
                                "calendar_batch_id", "calendar_manifest_hash",
                            ))):
                        raise ValueError("native daily amount correction prior publication differs")
                    changes = {item["stock_code"]: item for item in correction["rows"]}
                    for old, new in zip(previous["entities"], manifest["entities"], strict=True):
                        code = old["stock_code"]
                        if code not in changes:
                            if old != new:
                                raise ValueError("native daily amount correction changes unrelated evidence")
                            continue
                        if (old["classification"] != "TRADED" or new["classification"] != "TRADED"
                                or not validate_native_revision(old["evidence"], new["evidence"], changes[code],
                                                                prior_hash=previous_hash, cutoff=manifest["captured_at"])):
                            raise ValueError("native daily amount correction changes unrelated evidence")
        stored = connection.execute(text(f"SELECT stock_code,expected_state,classification,bar_count,time_set_hash,first_time,last_time,source_row_hash,row_hash FROM {ENTITY_TABLE} WHERE manifest_hash=:hash ORDER BY stock_code"), {"hash": row["manifest_hash"]}).mappings().all()
        expected = []
        for item in manifest["entities"]:
            traded = item["classification"] == "TRADED"
            expected.append({"stock_code": item["stock_code"], "expected_state": item["classification"],
                             "classification": item["classification"], "bar_count": int(traded),
                             "time_set_hash": canonical_digest([day] if traded else []),
                             "first_time": day if traded else "", "last_time": day if traded else "",
                             "source_row_hash": canonical_digest(item["evidence"]), "row_hash": item["row_hash"]})
        if [dict(item) for item in stored] != expected:
            raise ValueError("persisted daily entity inventory differs")
        if verify_rows and not archived:
            rows = connection.execute(text("SELECT stock_code,trade_date,open,close,high,low,volume,amount,pre_close,data_source,quality_status,permission_status FROM sm_stock_kline WHERE trade_date=:day AND k_type=1 AND adjust_type=0 ORDER BY stock_code"), {"day": day}).mappings().all()
            fallback = manifest.get("historical_public_bars") or {}
            if any((item.get("data_source"), item.get("quality_status")) != (
                    (SINA_PROVIDER, SINA_QUALITY) if item["stock_code"] in fallback else (PROVIDER, "VERIFIED"))
                   or item.get("permission_status") != "SUPPORTED" for item in rows):
                raise ValueError("daily database provider or quality differs")
            actual = [canonical_bar(item) for item in rows]
            expected_bars = [item["evidence"] for item in manifest["entities"] if item["classification"] == "TRADED"]
            if actual != expected_bars:
                raise ValueError("daily database bars differ from full-market proof")
        return manifest


def retained_listing_evidence(engine, codes: Sequence[str], *, target_date: str,
                              decision_known_at: datetime) -> dict[str, dict]:
    """Reuse immutable official documents through their proven listing date."""
    requested = set(codes)
    result = {}
    with _scope(engine) as connection:
        rows = connection.execute(text(
            f"SELECT manifest_hash,manifest_json FROM {MANIFEST_TABLE} "
            "WHERE schema_version=:schema AND provider=:provider AND dataset='stock_daily' "
            "AND status='EXACT' AND captured_at<=:cutoff ORDER BY captured_at DESC LIMIT 40"
        ), {"schema": SCHEMA, "provider": PROVIDER, "cutoff": _stamp(decision_known_at)}).mappings()
        for row in rows:
            manifest = json.loads(str(row["manifest_json"]))
            supplied = manifest.pop("manifest_hash", None)
            if supplied != row["manifest_hash"] or supplied != canonical_digest(manifest):
                raise ValueError("retained listing publication seal differs")
            for entity in manifest["entities"]:
                code = entity["stock_code"]
                if code not in requested or code in result or entity["classification"] != "NOT_LISTED":
                    continue
                core = {key: entity[key] for key in ("stock_code", "classification", "evidence")}
                if entity["row_hash"] != canonical_digest(core):
                    raise ValueError("retained official listing entity seal differs")
                if str(entity["evidence"]["listing_date"]) <= target_date:
                    continue
                result[code] = listing_evidence(entity["evidence"], code, target_date)
            if set(result) == requested:
                break
    return result

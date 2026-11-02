"""Exact local-cache daily coverage, including evidenced no-bar securities.

The cache is a source of bars, never an authority for missing securities.  A
publication accounts for the independent catalog with real bars, native
post-close suspension observations, or dated exchange listing evidence.  The
proof uses the existing append-only coverage ledger; no synthetic bar is made.
"""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
import json
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_trade_calendar import load_trade_calendar_receipt

SCHEMA = "probiga.qmt-datadir-daily-coverage.v1"
PROVIDER = "qmt_datadir"
CHINA = ZoneInfo("Asia/Shanghai")
MANIFEST_TABLE = "qmt_history_coverage_manifest"
ENTITY_TABLE = "qmt_history_coverage_entity"


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


def suspension_evidence(snapshot: Mapping[str, Any], code: str, day: str, cutoff: datetime) -> dict:
    """Prove the installed QMT full-tick suspension semantics at the close."""
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
    # The bridge may serialize the same native observations after midnight.
    # Acquisition time and native event date bind the suspension to its session;
    # generated_at only dates the envelope containing those observations.
    if (not closed <= observed <= generated <= cutoff
            or observed.date().isoformat() != day
            or event.date().isoformat() != day
            or raw.get("_probiga_acquisition_method") != "ContextInfo.get_full_tick"
            or type(raw.get("stockStatus")) is not int or raw["stockStatus"] != 7
            or not all(_zero(raw.get(field)) for field in ("open", "high", "low", "volume", "amount"))):
        raise ValueError("target-day native post-close suspension is unproven: " + code)
    return {"kind": "NATIVE_QMT_SUSPENSION", "stock_code": code,
            "target_date": day, "snapshot_generated_at": generated.isoformat(" "),
            "snapshot_batch_id": str(snapshot.get("batch_id") or ""),
            "native_tick": raw}


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


def build_manifest(*, catalog, calendar, day: str, rows: Sequence[Mapping[str, Any]],
                   run_id: str, captured_at: datetime, snapshot: Mapping[str, Any] | None = None,
                   listings: Mapping[str, Any] | None = None) -> dict:
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
    entities = []
    for code in expected:
        if code in by_code:
            kind, evidence = "TRADED", by_code[code]
        elif code in (listings or {}):
            kind, evidence = "NOT_LISTED", listing_evidence(listings[code], code, day)
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
    return {**core, "manifest_hash": canonical_digest(core)}


def validate_manifest(manifest: Mapping[str, Any], *, catalog, calendar) -> dict:
    original = dict(manifest)
    entities = original.get("entities") or []
    snapshot = {"source": "gj_big_qmt_inner", "quote_acquisition_protocol": "probiga.qmt-full-tick-poll.v1",
                "quote_acquisition_mode": "full_tick_poll", "quotes": {}}
    no_trade = [entity for entity in entities if entity.get("classification") == "NO_TRADE"]
    # Each entity binds its own snapshot envelope; validate those separately
    # rather than assigning one generated timestamp to unrelated observations.
    for entity in no_trade:
        proof = entity["evidence"]
        code = entity["stock_code"]
        symbol = code + (".SH" if code.startswith("6") else ".BJ" if code.startswith(("4", "8", "9")) else ".SZ")
        envelope = {**snapshot, "generated_at": proof["snapshot_generated_at"],
                    "batch_id": proof["snapshot_batch_id"], "quotes": {symbol: proof["native_tick"]}}
        if suspension_evidence(envelope, code, str(original["trade_date"]), _stamp(original["captured_at"])) != proof:
            raise ValueError("native suspension evidence differs")
    # Rebuild canonical entities, with each no-bar proof checked above.
    expected = sorted(catalog.eligible_codes(str(original["trade_date"])))
    if [entity.get("stock_code") for entity in entities] != expected or not expected:
        raise ValueError("daily manifest catalog inventory differs")
    if (original.get("schema") != SCHEMA or original.get("provider") != PROVIDER
            or original.get("dataset") != "stock_daily" or original.get("status") != "EXACT"
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
    for entity in entities:
        core = {key: entity[key] for key in ("stock_code", "classification", "evidence")}
        if entity.get("row_hash") != canonical_digest(core):
            raise ValueError("daily entity hash differs")
        if entity["classification"] == "TRADED":
            row = canonical_bar(entity["evidence"])
            if row != entity["evidence"] or row["stock_code"] != entity["stock_code"] or row["trade_date"] != original["trade_date"]:
                raise ValueError("daily entity bar differs")
        elif entity["classification"] == "NOT_LISTED":
            proof = listing_evidence(entity["evidence"], entity["stock_code"], original["trade_date"])
            if proof != entity["evidence"]:
                raise ValueError("daily listing evidence differs")
        elif entity["classification"] != "NO_TRADE":
            raise ValueError("unresolved daily entity")
    supplied = original.pop("manifest_hash", None)
    if supplied != canonical_digest(original):
        raise ValueError("daily manifest hash differs")
    return {**original, "manifest_hash": supplied}


def insert_manifest(connection, manifest: dict) -> None:
    """Append one immutable proof in the same transaction as its daily rows."""
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
              "manifest_json": json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))}
    keys = ("manifest_hash", "schema_version", "dataset", "period", "provider", "trade_date", "status",
            "strategy_eligible", "run_id", "catalog_batch_id", "catalog_manifest_hash", "calendar_batch_id",
            "calendar_manifest_hash", "source_batch_id", "grid_profile", "expected_entity_count", "entity_count",
            "expected_traded_count", "actual_traded_count", "no_trade_count", "bar_count", "expected_entity_set_hash",
            "expected_traded_set_hash", "actual_traded_set_hash", "no_trade_set_hash", "entity_root_hash", "reason_count",
            "captured_at", "manifest_json")
    connection.execute(text(f"INSERT INTO {MANIFEST_TABLE} ({','.join(keys)},created_at) VALUES ({','.join(':'+key for key in keys)},CURRENT_TIMESTAMP)"), params)
    persisted_entities = []
    for entity in entities:
        traded_bar = entity["classification"] == "TRADED"
        day = manifest["trade_date"] if traded_bar else ""
        persisted_entities.append({"manifest_hash": manifest["manifest_hash"], "stock_code": entity["stock_code"],
                            "state": entity["classification"], "bars": int(traded_bar),
                            "times": canonical_digest([day] if traded_bar else []), "day": day,
                            "source_hash": canonical_digest(entity["evidence"]), "row_hash": entity["row_hash"]})
    connection.execute(text(f"INSERT INTO {ENTITY_TABLE} (manifest_hash,stock_code,expected_state,classification,bar_count,time_set_hash,first_time,last_time,source_row_hash,row_hash,created_at) VALUES (:manifest_hash,:stock_code,:state,:state,:bars,:times,:day,:day,:source_hash,:row_hash,CURRENT_TIMESTAMP)"), persisted_entities)


def load_daily_manifest(engine, day: str, *, decision_known_at: datetime | str,
                        manifest_hash: str = "", verify_rows: bool = True) -> dict | None:
    cutoff = _stamp(decision_known_at)
    with _scope(engine) as connection:
        condition = " AND manifest_hash=:hash" if manifest_hash else ""
        row = connection.execute(text(f"SELECT manifest_hash,manifest_json,schema_version,dataset,provider,trade_date,status,strategy_eligible,run_id,captured_at,catalog_batch_id,catalog_manifest_hash,calendar_batch_id,calendar_manifest_hash FROM {MANIFEST_TABLE} WHERE schema_version=:schema AND dataset='stock_daily' AND provider=:provider AND trade_date=:day AND status='EXACT' AND captured_at<=:cutoff{condition} ORDER BY captured_at DESC,manifest_hash DESC LIMIT 1"),
                                 {"schema": SCHEMA, "provider": PROVIDER, "day": day, "cutoff": cutoff, "hash": manifest_hash}).mappings().first()
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
        calendar = load_trade_calendar_receipt(connection, batch_id=manifest["calendar_batch_id"], start_date=day, end_date=day, decision_known_at=cutoff)
        validate_manifest(manifest, catalog=catalog, calendar=calendar)
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
        if verify_rows:
            rows = connection.execute(text("SELECT stock_code,trade_date,open,close,high,low,volume,amount,pre_close,data_source,quality_status,permission_status FROM sm_stock_kline WHERE trade_date=:day AND k_type=1 AND adjust_type=0 ORDER BY stock_code"), {"day": day}).mappings().all()
            if any(item.get("data_source") != PROVIDER or item.get("quality_status") != "VERIFIED"
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

"""Complete native Eastmoney dividend event acquisition with retained source audit.

Only this provider writes the formal dividend dataset. A complete, twice-read
pagination set proves native event presence/absence. Missing source fields stay
NULL and explicitly degrade source quality; they never become invented plans.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import re
import sys
import time
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo
import requests
from sqlalchemy import text
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.qmt_stock_catalog import load_target_stock_catalog, validate_stock_catalog_runtime_schema
from server.common.stock_dividend_schema import validate_stock_dividend_schema
from tools.env_config import create_tool_engine, load_project_env
SHANGHAI = ZoneInfo("Asia/Shanghai")
PROVIDER_ID = "eastmoney_stock_dividend"
RECEIPT_SCHEMA = "probiga.stock-dividend-eastmoney-receipt.v1"
PAGE_SIZE = 500
MAX_PAGES = 1000
MAX_RECEIPT_BYTES = 24000
SOURCE_IDENTITY = {
    "schema": "probiga.eastmoney-dividend-source.v1",
    "endpoint": "https://datacenter-web.eastmoney.com/api/data/v1/get",
    "report_name": "RPT_SHAREBONUS_DET", "columns": "ALL", "filter": "",
    "sort_columns": "SECURITY_CODE,REPORT_DATE", "sort_types": "1,1",
    "page_size": PAGE_SIZE, "complete_passes": 2,
    "identity_fields": ["SECUCODE", "REPORT_DATE"], "timeout_seconds": 25, "retries": 2,
}
ROW_COLUMNS = ("event_id", "stock_code", "qmt_code", "report_period", "report_date", "plan_notice_date",
               "dividend_plan", "ex_dividend_date", "assign_progress", "data_source", "data_version",
               "source_payload_json", "quality_status")
NATIVE_REQUIRED = frozenset({"SECUCODE", "SECURITY_CODE", "REPORT_DATE", "PLAN_NOTICE_DATE", "NOTICE_DATE",
                             "EX_DIVIDEND_DATE", "ASSIGN_PROGRESS", "IMPL_PLAN_PROFILE"})

def _canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str)

def _digest(value):
    return hashlib.sha256(_canonical_json(value).encode("utf8")).hexdigest()

def _code(value):
    code = str(value or "").strip()
    if not re.fullmatch(r"[0-9]{6}", code):
        raise RuntimeError("DIVIDEND_STOCK_IDENTITY_INVALID")
    return code

def code_set_hash(codes):
    return hashlib.sha256("\n".join(sorted({_code(code) for code in codes})).encode("ascii")).hexdigest()

def _receipt(payload):
    result = {**payload, "receipt_id": _digest(payload)}
    if len(_canonical_json(result).encode("utf8")) > MAX_RECEIPT_BYTES:
        raise RuntimeError("DIVIDEND_RECEIPT_EXCEEDS_SCHEDULER_STORAGE")
    return result

def validate_runtime_schema(engine):
    result = validate_stock_dividend_schema(engine)
    return {**result, "schema_hash": _digest(result)}

@dataclass(frozen=True)
class DividendUniverse:
    as_of: str
    codes: tuple[str, ...]
    code_set_hash: str
    catalog_batch_id: str
    catalog_manifest_hash: str
    catalog_member_set_hash: str
    catalog_captured_at: str


def load_authoritative_universe(
    engine: Any,
    *,
    as_of: str,
    known_at: datetime,
) -> DividendUniverse:
    """Bind acquisition and readback to the same immutable catalog revision.

    si_all_code is mutable current state: an IPO arriving after this catalog
    was captured must not invalidate a reproducible catalog-bound collection.
    The provider still fetches and retains its complete native event set,
    including events outside this scope; it never claims those codes were
    members of this frozen catalog. No live terminal is needed here.
    """

    validate_stock_catalog_runtime_schema(engine)
    catalog, catalog_codes = load_target_stock_catalog(
        engine,
        target_date=as_of,
        decision_known_at=known_at.replace(tzinfo=None),
    )
    qmt_codes = tuple(sorted(_code(code) for code in catalog_codes))
    if len(set(qmt_codes)) != len(qmt_codes):
        raise RuntimeError("immutable QMT dividend universe contains duplicates")
    if not qmt_codes:
        raise RuntimeError("authoritative dividend universe is empty")
    return DividendUniverse(
        as_of=as_of,
        codes=qmt_codes,
        code_set_hash=code_set_hash(qmt_codes),
        catalog_batch_id=catalog.batch_id,
        catalog_manifest_hash=catalog.manifest_hash,
        catalog_member_set_hash=catalog.member_set_hash,
        catalog_captured_at=catalog.captured_at,
    )


def _day(value, *, nullable=False):
    if value is None and nullable:
        return None
    if not isinstance(value, (str, date, datetime)):
        raise RuntimeError("DIVIDEND_NATIVE_DATE_INVALID")
    raw = str(value)
    result = date.fromisoformat(raw[:10]).isoformat()
    if raw not in (result, result + " 00:00:00"):
        raise RuntimeError("DIVIDEND_NATIVE_DATE_INVALID")
    return result


def normalize_native_row(raw: Mapping[str, Any], *, as_of: str | None = None) -> dict:
    if not isinstance(raw, dict) or NATIVE_REQUIRED - raw.keys():
        raise RuntimeError("DIVIDEND_NATIVE_FIELDS_ABSENT")
    code = _code(raw["SECURITY_CODE"])
    qmt_code = str(raw["SECUCODE"])
    exchange = "SH" if code.startswith(("60", "68")) else "SZ" if code.startswith(("00", "30")) else "BJ"
    if qmt_code != code + "." + exchange:
        raise RuntimeError("DIVIDEND_NATIVE_SECURITY_MISMATCH")
    period, notice, plan_notice = (_day(raw[name]) for name in ("REPORT_DATE", "NOTICE_DATE", "PLAN_NOTICE_DATE"))
    if as_of is not None and (notice > as_of or plan_notice > as_of):
        raise RuntimeError("DIVIDEND_SOURCE_NOTICE_AFTER_CUTOFF")
    plan, progress = raw["IMPL_PLAN_PROFILE"], raw["ASSIGN_PROGRESS"]
    for value in (plan, progress):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise RuntimeError("DIVIDEND_NATIVE_TEXT_INVALID")
    if plan is not None and len(plan) > 512 or progress is not None and len(progress) > 64:
        raise RuntimeError("DIVIDEND_NATIVE_TEXT_EXCEEDS_STORAGE")
    source_json = _canonical_json(raw)
    if len(source_json.encode("utf8")) > 64 * 1024:
        raise RuntimeError("DIVIDEND_NATIVE_ROW_EXCEEDS_STORAGE")
    return {"event_id": _digest([PROVIDER_ID, qmt_code, period]), "stock_code": code,
            "qmt_code": qmt_code, "report_period": period, "report_date": notice,
            "plan_notice_date": plan_notice, "dividend_plan": plan,
            "ex_dividend_date": _day(raw["EX_DIVIDEND_DATE"], nullable=True),
            "assign_progress": progress, "data_source": PROVIDER_ID,
            "data_version": _digest(raw), "source_payload_json": source_json,
            "quality_status": "SOURCE_FIELDS_MISSING" if plan is None or progress is None else "COMPLETE"}


def canonical_dividend_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict]:
    result, seen = [], set()
    for row in rows:
        if set(ROW_COLUMNS) - row.keys():
            raise RuntimeError("DIVIDEND_EVENT_COLUMNS_MISSING")
        try:
            raw = json.loads(row["source_payload_json"])
        except (TypeError, ValueError) as exc:
            raise RuntimeError("DIVIDEND_SOURCE_JSON_INVALID") from exc
        expected = normalize_native_row(raw)
        supplied = {key: (_day(row[key], nullable=True) if key in (
            "report_period", "report_date", "plan_notice_date", "ex_dividend_date") else row[key]) for key in ROW_COLUMNS}
        if supplied != expected:
            raise RuntimeError("DIVIDEND_EVENT_SOURCE_BINDING_DIFFERS")
        if expected["event_id"] in seen:
            raise RuntimeError("DIVIDEND_DUPLICATE_SOURCE_EVENT")
        seen.add(expected["event_id"])
        result.append(expected)
    return sorted(result, key=lambda row: row["event_id"])


class EastmoneyDividendProvider:
    def __init__(self, *, fetch_page=None):
        self._fetch_page = fetch_page

    def page(self, number: int) -> dict:
        if self._fetch_page is not None:
            payload = self._fetch_page(number)
        else:
            with requests.Session() as session:
                session.trust_env = False
                for attempt in range(SOURCE_IDENTITY["retries"] + 1):
                    try:
                        response = session.get(SOURCE_IDENTITY["endpoint"], params={
                            "reportName": SOURCE_IDENTITY["report_name"], "columns": "ALL",
                            "sortColumns": SOURCE_IDENTITY["sort_columns"], "sortTypes": "1,1",
                            "pageNumber": number, "pageSize": PAGE_SIZE, "source": "WEB", "client": "WEB",
                        }, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/yjfp/"}, timeout=25)
                        response.raise_for_status()
                        payload = response.json()
                        break
                    except (requests.RequestException, ValueError):
                        if attempt == SOURCE_IDENTITY["retries"]:
                            raise
                        time.sleep(0.5)
        if not isinstance(payload, dict) or payload.get("success") is not True or payload.get("code") != 0:
            raise RuntimeError("DIVIDEND_NATIVE_PAGE_NOT_SUCCESS")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("DIVIDEND_NATIVE_PAGINATION_MISSING")
        count, pages, rows = result.get("count"), result.get("pages"), result.get("data")
        version = payload.get("version")
        if (type(count) is not int or count <= 0 or type(pages) is not int or not 1 <= pages <= MAX_PAGES
            or pages != (count + PAGE_SIZE - 1) // PAGE_SIZE or not 1 <= number <= pages
            or not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows)
            or len(rows) != min(PAGE_SIZE, count - (number - 1) * PAGE_SIZE)
            or not isinstance(version, str) or not re.fullmatch(r"[0-9a-f]{32}", version)):
            raise RuntimeError("DIVIDEND_NATIVE_PAGINATION_INVALID")
        return {"page": number, "count": count, "pages": pages, "version": version, "rows": rows}

    def full_snapshot(self, *, as_of: str) -> tuple[list[dict], dict]:
        passes, retained = [], None
        for pass_index in range(2):
            first = self.page(1)
            pages = [first]
            with ThreadPoolExecutor(max_workers=4) as pool:
                pages.extend(pool.map(self.page, range(2, first["pages"] + 1)))
            raw_rows, receipts, seen, ordered = [], [], set(), []
            for page in pages:
                if (page["count"], page["pages"]) != (first["count"], first["pages"]):
                    raise RuntimeError("DIVIDEND_NATIVE_PAGINATION_DRIFT")
                ids = []
                for raw in page["rows"]:
                    # Capture the complete native inventory, including dates
                    # announced ahead of the current calendar day. Eligibility
                    # is checked separately before the strategy projection.
                    row = normalize_native_row(raw)
                    if row["event_id"] in seen:
                        raise RuntimeError("DIVIDEND_DUPLICATE_SOURCE_EVENT")
                    seen.add(row["event_id"])
                    ordered.append((row["stock_code"], row["report_period"]))
                    ids.append(row["event_id"])
                    raw_rows.append(raw)
                receipts.append({"page": page["page"], "row_count": len(page["rows"]),
                                 "version": page["version"], "row_hash": _digest(page["rows"]),
                                 "event_set_hash": _digest(ids)})
            if len(raw_rows) != first["count"] or ordered != sorted(ordered):
                raise RuntimeError("DIVIDEND_NATIVE_PAGINATION_ORDER_OR_COUNT")
            proof = {"source_count": first["count"], "page_count": first["pages"],
                     "event_set_hash": _digest(ordered), "page_receipts": receipts}
            if passes and any(proof[key] != passes[0][key] for key in ("source_count", "page_count", "event_set_hash")):
                raise RuntimeError("DIVIDEND_NATIVE_COMPLETE_PASSES_DIFFER")
            passes.append(proof)
            retained = raw_rows
        # A version belongs to one response page. We never invent a global
        # provider revision or claim cross-page transactional source reads.
        return retained, {"schema": "probiga.dividend-full-pagination.v1", "complete_passes": 2,
                          "source_count": len(retained), "passes": passes,
                          "snapshot_row_hash": _digest(retained)}


@dataclass(frozen=True)
class DividendCollection:
    requested_codes: tuple[str, ...]
    nonempty_codes: tuple[str, ...]
    empty_codes: tuple[str, ...]
    rows: tuple[dict, ...]
    pagination: dict
    observed_at: datetime
    source_rows: tuple[dict, ...] = ()


def collect_snapshot(codes, *, provider, as_of, observed_at):
    requested = tuple(sorted(_code(code) for code in codes))
    if not requested or len(requested) != len(set(requested)):
        raise RuntimeError("DIVIDEND_UNIVERSE_EMPTY_OR_DUPLICATED")
    if as_of != _collection_cutoff(observed_at):
        raise RuntimeError("DIVIDEND_COLLECTION_CUTOFF_DIFFERS")
    raw_rows, proof = provider.full_snapshot(as_of=as_of)
    requested_set = set(requested)
    source_codes = {raw["SECURITY_CODE"] for raw in raw_rows}
    source_rows = tuple(normalize_native_row(raw) for raw in raw_rows)
    rows = canonical_dividend_rows(row for row in source_rows
                                   if row["stock_code"] in requested_set and _eligible_at(row, as_of))
    return DividendCollection(requested, tuple(sorted(requested_set & source_codes)),
                              tuple(sorted(requested_set - source_codes)), tuple(rows), proof, observed_at, source_rows)


def _collection_cutoff(observed_at):
    current = observed_at.replace(tzinfo=SHANGHAI) if observed_at.tzinfo is None else observed_at.astimezone(SHANGHAI)
    return current.date().isoformat()


def _eligible_at(row, cutoff):
    return row["report_date"] <= cutoff and row["plan_notice_date"] <= cutoff


def validate_pagination(proof, source_rows=None):
    if (not isinstance(proof, dict) or proof.get("schema") != "probiga.dividend-full-pagination.v1"
        or proof.get("complete_passes") != 2 or not isinstance(proof.get("passes"), list) or len(proof["passes"]) != 2):
        raise RuntimeError("DIVIDEND_FULL_PAGINATION_PROOF_MISSING")
    count = proof.get("source_count")
    if type(count) is not int or not 1 <= count <= MAX_PAGES * PAGE_SIZE:
        raise RuntimeError("DIVIDEND_FULL_PAGINATION_COUNT_INVALID")
    page_count = (count + PAGE_SIZE - 1) // PAGE_SIZE
    previous = None
    for summary in proof["passes"]:
        if summary.get("source_count") != count or summary.get("page_count") != page_count:
            raise RuntimeError("DIVIDEND_FULL_PAGINATION_COUNT_INVALID")
        receipts = summary.get("page_receipts")
        if not isinstance(receipts, list) or len(receipts) != page_count:
            raise RuntimeError("DIVIDEND_FULL_PAGINATION_RECEIPTS_MISSING")
        for number, receipt in enumerate(receipts, 1):
            if (receipt.get("page") != number or receipt.get("row_count") != min(PAGE_SIZE, count-(number-1)*PAGE_SIZE)
                or not re.fullmatch(r"[0-9a-f]{32}", str(receipt.get("version") or ""))
                or any(not re.fullmatch(r"[0-9a-f]{64}", str(receipt.get(k) or "")) for k in ("row_hash", "event_set_hash"))):
                raise RuntimeError("DIVIDEND_FULL_PAGINATION_RECEIPT_INVALID")
        if not re.fullmatch(r"[0-9a-f]{64}", str(summary.get("event_set_hash") or "")) or previous is not None and previous != summary["event_set_hash"]:
            raise RuntimeError("DIVIDEND_NATIVE_COMPLETE_PASSES_DIFFER")
        previous = summary["event_set_hash"]
    if not re.fullmatch(r"[0-9a-f]{64}", str(proof.get("snapshot_row_hash") or "")):
        raise RuntimeError("DIVIDEND_NATIVE_SNAPSHOT_HASH_MISSING")
    if source_rows is not None:
        rows = sorted(canonical_dividend_rows(source_rows), key=lambda r: (r["stock_code"], r["report_period"]))
        raw = [json.loads(row["source_payload_json"]) for row in rows]
        if (len(rows) != count or _digest(raw) != proof["snapshot_row_hash"]
            or _digest([(r["stock_code"], r["report_period"]) for r in rows]) != previous):
            raise RuntimeError("DIVIDEND_NATIVE_SNAPSHOT_REPLAY_DIFFERS")
        for index, receipt in enumerate(proof["passes"][-1]["page_receipts"]):
            chunk = rows[index*PAGE_SIZE:(index+1)*PAGE_SIZE]
            if (_digest([json.loads(r["source_payload_json"]) for r in chunk]) != receipt["row_hash"]
                or _digest([r["event_id"] for r in chunk]) != receipt["event_set_hash"]):
                raise RuntimeError("DIVIDEND_NATIVE_PAGE_REPLAY_DIFFERS")


def _quality(rows):
    missing = [{"event_id": row["event_id"], "stock_code": row["stock_code"], "report_period": row["report_period"],
                "fields": [key for key in ("dividend_plan", "assign_progress") if row[key] is None]}
               for row in rows if row["quality_status"] == "SOURCE_FIELDS_MISSING"]
    return {"status": "SOURCE_FIELDS_MISSING" if missing else "COMPLETE", "missing_event_count": len(missing),
            "missing_events": missing, "missing_event_manifest_hash": _digest(missing)}


def validate_collection(collection, *, min_nonempty_code_ratio=0.2):
    validate_pagination(collection.pagination, collection.source_rows)
    requested, nonempty, empty = set(collection.requested_codes), set(collection.nonempty_codes), set(collection.empty_codes)
    if requested != nonempty | empty or nonempty & empty:
        raise RuntimeError("DIVIDEND_UNIVERSE_ACCOUNTING_DIFFERS")
    rows = canonical_dividend_rows(collection.rows)
    source = canonical_dividend_rows(collection.source_rows)
    source_codes = {row["stock_code"] for row in source}
    if source_codes & requested != nonempty or requested - source_codes != empty:
        raise RuntimeError("DIVIDEND_NONEMPTY_EVENT_SET_DIFFERS")
    cutoff = _collection_cutoff(collection.observed_at)
    scoped = [row for row in source if row["stock_code"] in requested]
    eligible = [row for row in scoped if _eligible_at(row, cutoff)]
    deferred = [row for row in scoped if not _eligible_at(row, cutoff)]
    if rows != eligible:
        raise RuntimeError("DIVIDEND_ELIGIBLE_EVENT_SET_DIFFERS")
    ratio = len(nonempty) / len(requested)
    if min_nonempty_code_ratio != 0.2 or ratio < 0.2:
        raise RuntimeError("DIVIDEND_NONEMPTY_EVIDENCE_UNREASONABLE")
    return {"requested_code_count": len(requested), "requested_code_set_hash": code_set_hash(requested),
            "responded_code_count": len(requested), "responded_code_set_hash": code_set_hash(requested),
            "nonempty_code_count": len(nonempty), "nonempty_code_set_hash": code_set_hash(nonempty),
            "authoritative_empty_code_count": len(empty), "authoritative_empty_code_set_hash": code_set_hash(empty),
            "failure_count": 0, "nonempty_code_ratio": ratio, "row_count": len(rows), "row_hash": _digest(rows),
            "response_status_manifest_hash": _digest([[c, "NONEMPTY" if c in nonempty else "ABSENT_FROM_COMPLETE_SOURCE"] for c in sorted(requested)]),
            "pagination_hash": _digest(collection.pagination), "source_quality": _quality(rows),
            "cutoff_date": cutoff, "deferred_event_count": len(deferred),
            "deferred_code_count": len({row["stock_code"] for row in deferred}),
            "deferred_event_set_hash": _digest([[r["event_id"], r["data_version"]] for r in deferred])}


def _read_scope(connection, batch_id):
    return [dict(row) for row in connection.execute(text(
        "SELECT " + ",".join(ROW_COLUMNS) + " FROM sm_dividend WHERE batch_id=:batch_id ORDER BY event_id"
    ), {"batch_id": batch_id}).mappings()]


def _insert_retained(connection, table, columns, records, key_columns):
    if not records:
        return
    # Existing source audit is never updated. Readback below detects any
    # attempted collision or externally altered historical content.
    placeholders = ",".join(":" + col for col in columns)
    sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
    if connection.dialect.name == "sqlite":
        sql += " ON CONFLICT(" + ",".join(key_columns) + ") DO NOTHING"
    else:
        sql = sql.replace("INSERT INTO", "INSERT IGNORE INTO", 1)
    connection.execute(text(sql), records)


def replace_snapshot(engine, *, collection, evidence):
    rows = canonical_dividend_rows(collection.rows)
    if evidence != validate_collection(collection):
        raise RuntimeError("DIVIDEND_COLLECTION_PROOF_DIFFERS")
    manifest = {"schema": "probiga.dividend-source-snapshot.v1", "source_identity": SOURCE_IDENTITY,
                "observed_at": collection.observed_at.isoformat(), "requested_codes": list(collection.requested_codes),
                "empty_codes": list(collection.empty_codes), "pagination": collection.pagination,
                "collection": evidence, "members": [[row["event_id"], row["data_version"]] for row in rows],
                "source_members": [[row["event_id"], row["data_version"]] for row in collection.source_rows]}
    manifest_json = _canonical_json(manifest)
    if len(manifest_json.encode("utf8")) > 32 * 1024 * 1024:
        raise RuntimeError("DIVIDEND_SNAPSHOT_EXCEEDS_STORAGE")
    batch_id = _digest(manifest)
    observed_at = collection.observed_at.replace(tzinfo=None)
    write_rows = [{**row, "batch_id": batch_id, "received_at": observed_at, "etl_sync_at": observed_at} for row in rows]
    columns = (*ROW_COLUMNS, "batch_id", "received_at", "etl_sync_at")
    sql = f"INSERT INTO sm_dividend ({','.join(columns)}) VALUES ({','.join(':'+c for c in columns)})"
    updates = [col for col in columns if col != "event_id"]
    with engine.begin() as connection:
        if connection.dialect.name == "sqlite":
            sql += " ON CONFLICT(event_id) DO UPDATE SET " + ",".join(c + "=excluded." + c for c in updates)
        else:
            sql += " ON DUPLICATE KEY UPDATE " + ",".join(c + "=VALUES(" + c + ")" for c in updates)
        for offset in range(0, len(collection.source_rows), 500):
            chunk = collection.source_rows[offset:offset+500]
            _insert_retained(connection, "sm_dividend_source_revision",
                             ("event_id", "source_hash", "source_payload_json", "first_received_at"),
                             [{"event_id": r["event_id"], "source_hash": r["data_version"], "source_payload_json": r["source_payload_json"], "first_received_at": observed_at} for r in chunk],
                             ("event_id", "source_hash"))
        for offset in range(0, len(write_rows), 500):
            connection.execute(text(sql), write_rows[offset:offset+500])
        _insert_retained(connection, "sm_dividend_source_snapshot", ("batch_id", "observed_at", "manifest_json", "manifest_hash"),
                         [{"batch_id": batch_id, "observed_at": observed_at, "manifest_json": manifest_json, "manifest_hash": batch_id}], ("batch_id",))
        _validate_persisted_batch(connection, batch_id, evidence)
    with engine.connect() as connection:
        _validate_persisted_batch(connection, batch_id, evidence)
    return {"batch_id": batch_id, "manifest_hash": batch_id, "row_count": len(rows), "row_hash": _digest(rows),
            "scope_code_count": len(collection.requested_codes), "scope_code_set_hash": code_set_hash(collection.requested_codes)}


def _validate_persisted_batch(connection, batch_id, evidence):
    stored = connection.execute(text("SELECT manifest_json,manifest_hash FROM sm_dividend_source_snapshot WHERE batch_id=:batch_id"), {"batch_id": batch_id}).mappings().first()
    if stored is None:
        raise RuntimeError("DIVIDEND_SNAPSHOT_AUDIT_MISSING")
    manifest = json.loads(stored["manifest_json"])
    if _digest(manifest) != batch_id or stored["manifest_hash"] != batch_id or manifest["collection"] != evidence:
        raise RuntimeError("DIVIDEND_SNAPSHOT_AUDIT_DIFFERS")
    rows = canonical_dividend_rows(_read_scope(connection, batch_id))
    if len(rows) != evidence["row_count"] or _digest(rows) != evidence["row_hash"]:
        raise RuntimeError("DIVIDEND_PERSISTED_SCOPE_DIFFERS")
    if [[r["event_id"], r["data_version"]] for r in rows] != manifest["members"]:
        raise RuntimeError("DIVIDEND_SNAPSHOT_MEMBERS_DIFFERS")
    retained = connection.execute(text(
        "SELECT r.event_id,r.source_hash,r.source_payload_json FROM sm_dividend_source_revision r "
        "JOIN sm_dividend c ON c.event_id=r.event_id AND c.data_version=r.source_hash WHERE c.batch_id=:batch_id"
    ), {"batch_id": batch_id}).mappings().all()
    expected = {(r["event_id"], r["data_version"]): r["source_payload_json"] for r in rows}
    if {(r["event_id"], r["source_hash"]): r["source_payload_json"] for r in retained} != expected:
        raise RuntimeError("DIVIDEND_RETAINED_SOURCE_REVISION_DIFFERS")
    source_rows = []
    members = manifest["source_members"]
    if len(members) != manifest["pagination"]["source_count"] or len({tuple(pair) for pair in members}) != len(members):
        raise RuntimeError("DIVIDEND_SOURCE_MEMBER_SET_INVALID")
    for offset in range(0, len(members), 500):
        chunk = members[offset:offset+500]
        clauses, params = [], {}
        for index, (event_id, source_hash) in enumerate(chunk):
            clauses.append(f"(event_id=:event_{index} AND source_hash=:hash_{index})")
            params[f"event_{index}"] = event_id
            params[f"hash_{index}"] = source_hash
        retained = connection.execute(text("SELECT event_id,source_hash,source_payload_json FROM sm_dividend_source_revision WHERE " + " OR ".join(clauses)), params).mappings().all()
        expected_pairs = set(map(tuple, chunk))
        if {(r["event_id"], r["source_hash"]) for r in retained} != expected_pairs:
            raise RuntimeError("DIVIDEND_SOURCE_REVISION_MISSING")
        for stored_row in retained:
            restored = normalize_native_row(json.loads(stored_row["source_payload_json"]))
            if (restored["event_id"], restored["data_version"]) != (stored_row["event_id"], stored_row["source_hash"]):
                raise RuntimeError("DIVIDEND_SOURCE_REVISION_HASH_DIFFERS")
            source_rows.append(restored)
    validate_pagination(manifest["pagination"], source_rows)
    requested = tuple(manifest["requested_codes"])
    source_codes = {r["stock_code"] for r in source_rows}
    empty = tuple(sorted(set(requested) - source_codes))
    replayed = DividendCollection(requested, tuple(sorted(set(requested) & source_codes)), empty,
                                   tuple(rows), manifest["pagination"], datetime.fromisoformat(manifest["observed_at"]), tuple(source_rows))
    if (manifest.get("source_identity") != SOURCE_IDENTITY or list(empty) != manifest["empty_codes"]
        or validate_collection(replayed) != evidence):
        raise RuntimeError("DIVIDEND_SNAPSHOT_SOURCE_ACCOUNTING_DIFFERS")
    return manifest


def validate_receipt_source_proof(payload):
    """Pure machine gate; DB replay independently verifies the actual source."""
    if (payload.get("schema") != RECEIPT_SCHEMA or payload.get("status") != "PASS"
        or payload.get("provider") != PROVIDER_ID or payload.get("source_identity") != SOURCE_IDENTITY
        or payload.get("acquisition_status") != "COMPLETE"):
        raise RuntimeError("DIVIDEND_RECEIPT_SOURCE_IDENTITY_DIFFERS")
    evidence = payload["collection"]
    deferred = evidence.get("deferred_event_count")
    deferred_codes = evidence.get("deferred_code_count")
    if (evidence.get("cutoff_date") != payload.get("sync_date")
        or type(deferred) is not int or deferred < 0
        or type(deferred_codes) is not int or not 0 <= deferred_codes <= deferred
        or deferred_codes > evidence["nonempty_code_count"]
        or not re.fullmatch(r"[0-9a-f]{64}", str(evidence.get("deferred_event_set_hash") or ""))):
        raise RuntimeError("DIVIDEND_RECEIPT_CUTOFF_ACCOUNTING_DIFFERS")
    summary = payload["pagination_summary"]
    count = summary.get("source_count")
    if (summary.get("schema") != "probiga.dividend-pagination-summary.v1" or summary.get("complete_passes") != 2
        or type(count) is not int or not 1 <= count <= MAX_PAGES * PAGE_SIZE
        or summary.get("page_count") != (count+PAGE_SIZE-1)//PAGE_SIZE
        or not isinstance(summary.get("passes"), list) or len(summary["passes"]) != 2
        or not re.fullmatch(r"[0-9a-f]{64}", str(summary.get("snapshot_row_hash") or ""))):
        raise RuntimeError("DIVIDEND_RECEIPT_PAGINATION_SUMMARY_INVALID")
    for item in summary["passes"]:
        if (item.get("source_count") != count or item.get("page_count") != summary["page_count"]
            or any(not re.fullmatch(r"[0-9a-f]{64}", str(item.get(k) or "")) for k in ("event_set_hash", "page_manifest_hash"))):
            raise RuntimeError("DIVIDEND_RECEIPT_PAGINATION_SUMMARY_INVALID")
    if evidence["row_count"] + deferred > count:
        raise RuntimeError("DIVIDEND_RECEIPT_CUTOFF_ACCOUNTING_DIFFERS")
    if summary["passes"][0]["event_set_hash"] != summary["passes"][1]["event_set_hash"]:
        raise RuntimeError("DIVIDEND_NATIVE_COMPLETE_PASSES_DIFFER")
    if evidence.get("pagination_hash") != summary.get("full_proof_hash"):
        raise RuntimeError("DIVIDEND_RECEIPT_PAGINATION_HASH_DIFFERS")
    quality = payload["source_quality"]
    missing = quality.get("missing_events")
    if (quality != evidence.get("source_quality") or not isinstance(missing, list)
        or quality.get("missing_event_count") != len(missing)
        or quality.get("missing_event_manifest_hash") != _digest(missing)
        or quality.get("status") != ("SOURCE_FIELDS_MISSING" if missing else "COMPLETE")):
        raise RuntimeError("DIVIDEND_RECEIPT_SOURCE_QUALITY_DIFFERS")
    ids = []
    for item in missing:
        if (not re.fullmatch(r"[0-9a-f]{64}", str(item.get("event_id") or ""))
            or not item.get("fields") or len(set(item["fields"])) != len(item["fields"])
            or set(item["fields"]) - {"dividend_plan", "assign_progress"}):
            raise RuntimeError("DIVIDEND_RECEIPT_SOURCE_QUALITY_INVALID")
        _code(item["stock_code"])
        _day(item["report_period"])
        ids.append(item["event_id"])
    if ids != sorted(set(ids)) or len(ids) > evidence["row_count"]:
        raise RuntimeError("DIVIDEND_RECEIPT_SOURCE_QUALITY_INVALID")
    database = payload["database"]
    if (not re.fullmatch(r"[0-9a-f]{64}", str(database.get("batch_id") or ""))
        or database.get("batch_id") != database.get("manifest_hash")):
        raise RuntimeError("DIVIDEND_RECEIPT_RETAINED_SNAPSHOT_MISSING")


def pagination_summary(proof):
    validate_pagination(proof)
    return {"schema": "probiga.dividend-pagination-summary.v1", "complete_passes": 2,
            "source_count": proof["source_count"], "page_count": proof["passes"][0]["page_count"],
            "full_proof_hash": _digest(proof), "snapshot_row_hash": proof["snapshot_row_hash"],
            "passes": [{"source_count": item["source_count"], "page_count": item["page_count"],
                        "event_set_hash": item["event_set_hash"], "page_manifest_hash": _digest(item["page_receipts"])} for item in proof["passes"]]}


def run_sync(engine, *, now=None, provider=None):
    current = (now or datetime.now(SHANGHAI)).replace(microsecond=0)
    current = current.replace(tzinfo=SHANGHAI) if current.tzinfo is None else current.astimezone(SHANGHAI)
    schema = validate_runtime_schema(engine)
    universe = load_authoritative_universe(engine, as_of=current.date().isoformat(), known_at=current)
    collection = collect_snapshot(universe.codes, provider=provider or EastmoneyDividendProvider(),
                                  as_of=universe.as_of, observed_at=current)
    evidence = validate_collection(collection)
    database = replace_snapshot(engine, collection=collection, evidence=evidence)
    return _receipt({"schema": RECEIPT_SCHEMA, "status": "PASS", "sync_date": universe.as_of,
                     "provider": PROVIDER_ID, "executor_owner": "linux_provider", "acquisition_status": "COMPLETE",
                     "source_quality": evidence["source_quality"], "collection": evidence, "database": database,
                     "source_identity": SOURCE_IDENTITY, "pagination_summary": pagination_summary(collection.pagination), "schema_hash": schema["schema_hash"],
                     "catalog": {"batch_id": universe.catalog_batch_id, "manifest_hash": universe.catalog_manifest_hash,
                                 "member_set_hash": universe.catalog_member_set_hash, "captured_at": universe.catalog_captured_at,
                                 "target_code_set_hash": universe.code_set_hash}})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if not args.execute:
        print(_canonical_json(_receipt({"schema": RECEIPT_SCHEMA, "status": "DRY_RUN", "provider": PROVIDER_ID})))
        return 0
    try:
        load_project_env()
        engine = create_tool_engine()
        try:
            result = run_sync(engine)
        finally:
            engine.dispose()
    except Exception as exc:
        print(_canonical_json(_receipt({"schema": RECEIPT_SCHEMA, "status": "DATA_BLOCKED", "provider": PROVIDER_ID,
                                       "error_type": type(exc).__name__, "error": str(exc)[:600]})), flush=True)
        return 2
    print(_canonical_json(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

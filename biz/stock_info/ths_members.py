"""Current THS members with exact native totals and atomic per-index publication."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import sys
import re
import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from biz.stock_info.ths_catalog import PublicReader, INDEX_PATTERN, _valid_text, identity_hash
from server.common.mysql_lock import mysql_named_lock
from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.acquisition_shards import AcquisitionShards


def _source_inventory(records, expected: int, *, source: str):
    if type(expected) is not int or not 0 <= expected <= 6000 or len(records) > expected:
        raise RuntimeError("THS native member raw inventory count differs")
    rows, raw = {}, []
    for code, name in records:
        if not re.fullmatch(r"[0234689][0-9]{5}", code) or not _valid_text(name):
            raise RuntimeError("THS native member identity invalid")
        row = {"stock_code": code, "short_name": name}
        if code in rows and rows[code] != row:
            raise RuntimeError(f"THS native member conflicting duplicate: {code}")
        rows[code] = row
        raw.append([code, name])
    proof = {"source": source, "expected_raw_record_count": expected,
             "raw_record_count": len(raw), "unique_security_count": len(rows),
             "duplicate_record_count": len(raw) - len(rows), "raw_inventory": raw}
    return rows, {**proof, "raw_inventory_sha256": result_hash(proof)}


def parse_rank_page(content: str, index_code: str, order: str, limit: int):
    callback = f"quotebridge_v2_blockrank_{index_code}_8_{order}{limit}"
    match = re.fullmatch(re.escape(callback) + r"\((\{.*\})\);?\s*", content.strip(), re.S)
    if match is None:
        raise RuntimeError(f"THS native member response identity differs: {index_code}")
    payload = json.loads(match.group(1))
    block, items = payload.get("block"), payload.get("items")
    if not isinstance(block, dict) or not isinstance(items, list):
        raise RuntimeError(f"THS native member payload missing: {index_code}")
    total = block.get("subcodeCount")
    if type(total) is not int or not 0 <= total <= 6000 or len(items) > limit:
        raise RuntimeError(f"THS native member total invalid or unsupported: {index_code}")
    records = []
    for item in items:
        if not isinstance(item, dict):
            raise RuntimeError(f"THS native member row invalid: {index_code}")
        code, name = str(item.get("5") or ""), str(item.get("55") or "").strip()
        records.append((code, name))
    rows, proof = _source_inventory(records, total, source=f"native_rank:{index_code}:8:{order}{limit}")
    return total, rows, proof


def parse_full_members(content: str, index_code: str, *, as_of: str):
    """Read the native dated directory, including members without quotes."""
    soup = BeautifulSoup(content, "html.parser")
    title = soup.title.get_text() if soup.title else ""
    native_title = re.fullmatch(r"(.+)\((88\d{4})\) 最新动态_F10_同花顺金融服务网", title)
    total = re.search(r"概念股数量[：:]\s*(\d+)家", soup.get_text(" ", strip=True))
    embedded = re.findall(r'''<div\b[^>]*\bid=['"]concept_data['"][^>]*>(.*?)</div>''', content, re.S)
    if not native_title or native_title.group(2) != index_code or total is None or len(embedded) != 1:
        raise RuntimeError(f"THS native full member identity differs: {index_code}")
    result = json.loads(embedded[0]).get("result")
    if not isinstance(result, dict) or result.get("report") != as_of:
        raise RuntimeError(f"THS native full member date differs: {index_code}")
    series = result.get("listdata")
    if not isinstance(series, dict) or set(series) != {as_of} or not isinstance(series[as_of], list):
        raise RuntimeError(f"THS native full member dated list differs: {index_code}")
    records = []
    for raw in series[as_of]:
        if not isinstance(raw, list) or len(raw) != 8:
            raise RuntimeError(f"THS native full member row differs: {index_code}")
        code, name = str(raw[0]), str(raw[1]).strip()
        records.append((code, name))
    expected = int(total.group(1))
    rows, proof = _source_inventory(records, expected, source=f"dated_f10:{index_code}:{as_of}")
    return expected, rows, proof


def collect_members(index_code: str, *, as_of: str, fetch=None) -> dict[str, Any]:
    if not INDEX_PATTERN.fullmatch(index_code):
        raise ValueError("THS member index must be one native six-digit index")
    fetch = fetch or PublicReader()
    def page(order, limit):
        url = f"https://d.10jqka.com.cn/v2/blockrank/{index_code}/8/{order}{limit}.js"
        return parse_rank_page(fetch(url), index_code, order, limit)
    expected, initial, primary = page("d", 15)
    rows = initial
    if expected > 15:
        pages = [("d", min(3000, ((expected + 14) // 15) * 15))]
        rows = {}
        for order, limit in pages:
            total, received, primary = page(order, limit)
            if total != expected:
                raise RuntimeError(f"THS native member total changed: {index_code}")
            for code, row in received.items():
                if code in rows and rows[code] != row:
                    raise RuntimeError(f"THS native member changed across pages: {index_code}:{code}")
                rows[code] = row
    proofs = [primary]
    if primary["raw_record_count"] < expected or primary["duplicate_record_count"]:
        # The native quote ranking excludes members without quotations and
        # caps large lists. The provider's dated F10 directory covers these
        # identities. Both native totals must agree; the exact union, never a
        # row threshold or an assumed missing stock, controls publication.
        total, full, proof = parse_full_members(fetch(f"https://basic.10jqka.com.cn/48/{index_code}/"),
                                                index_code, as_of=as_of)
        if total != expected:
            raise RuntimeError(f"THS native member totals disagree: {index_code}")
        proofs.append(proof)
        if primary["raw_record_count"] == expected:
            if set(full) != set(rows):
                raise RuntimeError(f"THS native member unique inventories disagree: {index_code}")
        elif proof["raw_record_count"] == expected:
            if not set(rows) <= set(full):
                raise RuntimeError(f"THS native member unique inventories disagree: {index_code}")
            primary = proof
        else:
            # Some native descending caches omit a duplicate source record.
            # The opposite bounded bulk ordering must independently return
            # the entire declared raw inventory and the dated F10 identities.
            rounded = min(3000, ((expected + 14) // 15) * 15)
            # Native per-size caches can disagree about the duplicate raw
            # record. These three bounded views are the same provider and
            # inventory, not fabricated missing identities or a row floor.
            views = list(dict.fromkeys([("a", rounded), ("a", 3000), ("d", 3000)]))
            for order, limit in views:
                total, alternate, alternate_proof = page(order, limit)
                if total != expected:
                    raise RuntimeError(f"THS native member total changed: {index_code}")
                if alternate_proof["raw_record_count"] == expected:
                    if set(alternate) != set(full):
                        raise RuntimeError(f"THS native member unique inventories disagree: {index_code}")
                    rows, primary = alternate, alternate_proof
                    proofs.append(alternate_proof)
                    break
        for code, row in full.items():
            # F10 retains the close's short name (including XD/XR prefixes).
            # The native stock code binds membership; use the current quote
            # name where present, and the directory name for unquoted members.
            rows.setdefault(code, row)
    if (primary["raw_record_count"] != expected
            or len(rows) != primary["unique_security_count"]):
        raise RuntimeError(f"THS native members incomplete: {index_code}: expected={expected}, received={len(rows)}")
    return {"index_code": index_code, "expected_count": len(rows),
            "expected_raw_record_count": expected,
            "raw_record_count": primary["raw_record_count"],
            "unique_security_count": len(rows),
            "duplicate_record_count": primary["duplicate_record_count"],
            "raw_inventory_sha256": primary["raw_inventory_sha256"],
            "source_proofs": proofs,
            "members": [rows[code] for code in sorted(rows)],
            "member_set_hash": identity_hash(rows), "source_trade_date": as_of,
            "observed_at": datetime.now().replace(microsecond=0)}


def publish_members(engine, collection: dict[str, Any], *, concept_code: str) -> int:
    validate_collection(collection)
    index = collection["index_code"]
    records = collection["members"]
    if (not INDEX_PATTERN.fullmatch(index) or not re.fullmatch(r"[0-9]{6}", concept_code)
        or len(records) != collection["expected_count"]
        or len({row["stock_code"] for row in records}) != len(records)
        or identity_hash(row["stock_code"] for row in records) != collection["member_set_hash"]):
        raise RuntimeError("THS member publication proof differs")
    with mysql_named_lock(engine, f"probiga.ths.members.{index}", timeout_seconds=30) as conn:
        if conn.in_transaction():
            conn.commit()
        with conn.begin():
            conn.execute(text("DELETE FROM si_concept_constituent_ths "
                              "WHERE (query_type='index_code' AND query_key=:index) "
                              "OR (query_type='concept_code' AND query_key=:concept)"),
                         {"index": index, "concept": concept_code})
            if records:
                frame = pd.DataFrame(records).assign(query_type="index_code", query_key=index,
                                                     etl_sync_at=collection["observed_at"])
                frame.to_sql("si_concept_constituent_ths", conn, if_exists="append", index=False,
                             chunksize=500, method="multi")
            stored = [dict(row) for row in conn.execute(text(
                "SELECT stock_code,short_name FROM si_concept_constituent_ths "
                "WHERE query_type='index_code' AND query_key=:index ORDER BY stock_code"
            ), {"index": index}).mappings()]
            if stored != records:
                raise RuntimeError(f"THS member persisted content differs: {index}")
    return len(records)


def validate_collection(collection):
    """A checkpoint must retain a complete raw source and its exact projection."""
    proofs, verified = collection.get("source_proofs"), []
    if not isinstance(proofs, list) or not proofs:
        raise RuntimeError("THS member source inventory proof missing")
    for proof in proofs:
        records = proof.get("raw_inventory")
        if not isinstance(records, list):
            raise RuntimeError("THS raw member inventory missing")
        rows, observed = _source_inventory(records, proof["expected_raw_record_count"], source=proof["source"])
        if observed != proof:
            raise RuntimeError("THS raw member inventory proof differs")
        verified.append((proof, rows))
    primary = next(((proof, rows) for proof, rows in verified
                    if proof["raw_inventory_sha256"] == collection.get("raw_inventory_sha256")), None)
    if primary is None:
        raise RuntimeError("THS complete raw member inventory missing")
    proof, rows = primary
    members = collection["members"]
    known_rows = {(code, name) for item, _ in verified for code, name in item["raw_inventory"]}
    if (proof["raw_record_count"] != proof["expected_raw_record_count"]
            or collection.get("expected_raw_record_count") != proof["expected_raw_record_count"]
            or any(collection.get(key) != proof[key] for key in (
                "raw_record_count", "unique_security_count", "duplicate_record_count"))
            or len(members) != collection["expected_count"] or len(members) != len(rows)
            or {row["stock_code"] for row in members} != set(rows)
            or identity_hash(rows) != collection.get("member_set_hash")
            or any((row["stock_code"], row["short_name"]) not in known_rows for row in members)):
        raise RuntimeError("THS exact unique member projection differs from native records")


RESULT_SCHEMA = "probiga.ths-members-collection.v1"


def result_hash(payload):
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                   separators=(",", ":")).encode("utf8")).hexdigest()


def _retryable_member_source_error(error: Exception) -> bool:
    """Retry transport failures, never incomplete identities or totals."""
    cause = error
    for _ in range(8):
        if isinstance(cause, (requests.Timeout, requests.ConnectionError)):
            return True
        if isinstance(cause, requests.HTTPError):
            return getattr(cause.response, "status_code", None) in {408, 429, 500, 502, 503, 504}
        cause = cause.__cause__
        if cause is None:
            break
    return False


def sync_member_partitions(engine, catalog):
    if catalog is None or catalog.empty or not {"index_code", "concept_code"}.issubset(catalog.columns):
        raise RuntimeError("THS native catalog is empty or lacks identity columns")
    if catalog["index_code"].duplicated().any() or catalog["concept_code"].duplicated().any():
        raise RuntimeError("THS native catalog has duplicated identities")
    as_of = authoritative_closed_trade_date(engine)
    if not as_of:
        raise RuntimeError("THS completed source trade date unavailable")
    reader = PublicReader()
    complete, failed, empty, reused = [], [], [], []
    written = 0
    records = sorted(catalog.to_dict("records"), key=lambda row: str(row["index_code"]))
    shards = AcquisitionShards("ths_native_members", {
        "source_trade_date": as_of,
        "catalog_sha256": result_hash([[str(row["index_code"]), str(row["concept_code"])] for row in records]),
    })
    collections, pending, last_progress_at = {}, [], None

    def remember(index, collection):
        nonlocal last_progress_at
        collections[index] = collection
        complete.append(index)
        if not collection["members"]:
            empty.append(index)
        progress = {"total_units": len(records), "completed_units": len(complete),
                    "current_unit": index, "last_progress_at": datetime.now().replace(microsecond=0).isoformat(),
                    "checkpoint": str(shards.root)}
        last_progress_at = progress["last_progress_at"]
        shards.save("progress", progress)

    for row in records:
        index = str(row["index_code"])
        collection = shards.load(index)
        try:
            if (not isinstance(collection, dict) or collection.get("index_code") != index
                    or collection.get("source_trade_date") != as_of):
                raise ValueError("THS source shard identity differs")
            validate_collection(collection)
        except (ValueError, RuntimeError, KeyError, TypeError):
            pending.append(row)
            continue
        with engine.connect() as connection:
            stored = [dict(item) for item in connection.execute(text(
                "SELECT stock_code,short_name FROM si_concept_constituent_ths "
                "WHERE query_type='index_code' AND query_key=:index ORDER BY stock_code"
            ), {"index": index}).mappings()]
        if stored == collection["members"]:
            reused.append(index)
        else:
            written += publish_members(engine, collection, concept_code=str(row["concept_code"]))
        remember(index, collection)
    for attempt in range(3):
        if attempt:
            # A gateway may stay unavailable across the reader's immediate
            # request retries. Retry only those partitions after the first
            # collection pass; never fetch or publish a successful index twice.
            time.sleep(5 * attempt)
        retry = []
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(collect_members, str(row["index_code"]), as_of=as_of, fetch=reader): row
                       for row in pending}
            for progress, future in enumerate(as_completed(futures), start=1):
                row = futures[future]
                index = str(row["index_code"])
                try:
                    collection = future.result()
                    # Store immutable verified source records before the write;
                    # a retry rechecks the DB and resumes only this partition.
                    shards.save(index, collection)
                    count = publish_members(engine, collection, concept_code=str(row["concept_code"]))
                    written += count
                    remember(index, collection)
                except Exception as exc:
                    if attempt < 2 and _retryable_member_source_error(exc):
                        retry.append(row)
                    else:
                        # An incomplete native list retains its prior data and
                        # timestamp. Retrying a network error never relaxes the
                        # exact member-count and identity publication checks.
                        message = str(exc) if isinstance(exc, RuntimeError) and str(exc).startswith("THS ") else type(exc).__name__
                        failed.append({"index_code": index, "error": message[:300]})
                if progress % 20 == 0 or progress == len(futures):
                    print(f"THS members: pass={attempt + 1} checked={progress}/{len(futures)} complete={len(complete)} failed={len(failed)} retry={len(retry)}", file=sys.stderr, flush=True)
        if not retry:
            break
        pending = retry
    source_root = [[index, result_hash({"source_trade_date": value["source_trade_date"],
        "members": value["members"], "source_proofs": value["source_proofs"]})]
        for index, value in sorted(collections.items())]
    result = {"schema": RESULT_SCHEMA, "provider": "ths_native_members",
              "status": "PARTIAL" if failed else "COMPLETE",
              "source_trade_date": as_of,
              "observed_at": datetime.now().replace(microsecond=0).isoformat(),
              "catalog_count": len(catalog), "completed_indices": sorted(complete),
              "empty_indices": sorted(empty), "failed_indices": sorted(failed, key=lambda r:r["index_code"]),
              "rows_written": written, "publication_scope": "exact_native_index_partition"}
    result.update(checkpoint=str(shards.root), total_units=len(records), completed_units=len(complete),
        current_unit=failed[0]["index_code"] if failed else None, last_progress_at=last_progress_at,
        reused_indices=sorted(reused), source_partition_root_sha256=result_hash(source_root),
        raw_record_count=sum(value["raw_record_count"] for value in collections.values()),
        unique_security_count=sum(value["unique_security_count"] for value in collections.values()),
        duplicate_record_count=sum(value["duplicate_record_count"] for value in collections.values()),
        duplicate_indices=[[index, value["raw_record_count"], value["unique_security_count"],
                            value["duplicate_record_count"], value["raw_inventory_sha256"]]
            for index, value in sorted(collections.items()) if value["duplicate_record_count"]])
    return {**result, "result_sha256": result_hash(result)}


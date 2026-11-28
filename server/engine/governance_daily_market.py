"""Read native QMT DAT closing evidence for strategy governance."""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import json
import time
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.exc import DBAPIError

from server.common.pit_facts import normalize_decision_at
from server.common.formal_analysis_store import FormalAnalysisStore
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_daily_coverage import (
    ENTITY_TABLE,
    MANIFEST_TABLE,
    NATIVE_EVIDENCE_DATASET,
    SCHEMA as DATADIR_COVERAGE_SCHEMA,
    canonical_bar,
    load_daily_manifest,
    load_daily_manifest_completion,
    load_daily_native_evidence,
)
from server.common.qmt_datadir_result import PROVIDER, RECEIPT_TABLE
from server.common.qmt_stock_catalog import a_share_stock_code_sql


def _partition_hashes(connection, *, source, fields, condition, params):
    """Commit to every selected row, including duplicates, SQL NULLs and order-independent membership."""
    if not params["days"]:
        return {}
    row_json = f"JSON_ARRAY({fields})"
    mysql = connection.dialect.name == "mysql"
    if mysql:
        # Each leaf is fixed-width hex. Validate the concatenation length so
        # a server ignoring the SET_VAR hint cannot accept a truncated hash.
        leaves = f"GROUP_CONCAT(SHA2({row_json},256) ORDER BY BINARY SHA2({row_json},256) SEPARATOR '')"
        remaining_ms = int(1000 * (params["deadline"] - time.monotonic()))
        if remaining_ms <= 0:
            raise TimeoutError(f"QMT source fingerprint deadline: {source.split()[0]} {params['days']}")
        sql = f"""
            SELECT /*+ MAX_EXECUTION_TIME({min(12000, remaining_ms)}) SET_VAR(group_concat_max_len=16777216) */
                s.trade_date, COUNT(*) AS n, SHA2({leaves},256) AS digest, LENGTH({leaves}) AS size
            FROM {source} WHERE s.trade_date IN :days AND {condition}
            GROUP BY s.trade_date
        """
    else:
        sql = f"SELECT s.trade_date,{row_json} FROM {source} WHERE s.trade_date IN :days AND {condition}"
    try:
        rows = connection.execute(text(sql).bindparams(bindparam("days", expanding=True)), params)
    except DBAPIError as exc:
        if not mysql or getattr(exc.orig, "args", ())[:1] != (3024,):
            raise
        days = params["days"]
        if len(days) == 1:
            raise TimeoutError(f"QMT source fingerprint deadline: {source.split()[0]} {days[0]}") from exc
        midpoint = len(days) // 2
        result = {}
        for group in (days[:midpoint], days[midpoint:]):
            result.update(_partition_hashes(connection, source=source, fields=fields, condition=condition,
                                            params={**params, "days": group}))
        return result
    if mysql:
        result = {}
        for day, count, digest, size in rows:
            if size != count * 64:
                raise ValueError(f"{day} QMT source fingerprint truncated")
            result[str(day)[:10]] = digest
        return result
    grouped = {}
    for day, encoded in rows:
        grouped.setdefault(str(day)[:10], []).append(canonical_digest(json.loads(encoded)))
    return {day: canonical_digest(sorted(leaves)) for day, leaves in grouped.items()}


def daily_close_source_versions(connection, days, *, native_days, cutoff, protocol, deadline=None,
                                sealed_attestations=False):
    """Bind the exact inputs visible to this decision using database-side daily digests."""
    params = {"days": sorted(days), "cutoff": cutoff, "protocol": protocol,
              "schema": DATADIR_COVERAGE_SCHEMA, "archive": NATIVE_EVIDENCE_DATASET, "provider": PROVIDER,
              "deadline": deadline if deadline is not None else time.monotonic() + 120}
    mysql = connection.dialect.name == "mysql"
    archive_visible = set()
    headers, receipts, entities = {}, {}, {}
    native = sorted(set(days) & native_days)
    if native:
        native_params = {**params, "days": native}
        manifests = ("s.schema_version=:schema AND s.dataset IN ('stock_daily',:archive) "
                     "AND s.provider=:provider AND s.status='EXACT' AND s.captured_at<=:cutoff")
        headers = _partition_hashes(
            connection, source=(f"{MANIFEST_TABLE} s "
                                "LEFT JOIN qmt_stock_catalog_batch c ON c.batch_id=s.catalog_batch_id "
                                "LEFT JOIN qmt_trade_calendar_batch t ON t.batch_id=s.calendar_batch_id"), fields=(
                "s.manifest_hash,s.manifest_json,s.schema_version,s.dataset,s.provider,s.status,"
                "s.strategy_eligible,s.run_id,s.captured_at,s.catalog_batch_id,s.catalog_manifest_hash,"
                "s.calendar_batch_id,s.calendar_manifest_hash,c.manifest_hash,c.status,c.member_set_hash,"
                "c.captured_at,c.captured_at<=:cutoff,t.manifest_hash,t.status,t.session_set_hash,"
                "t.known_at,t.known_at<=:cutoff"
            ), condition=manifests, params=native_params,
        )
        receipts = _native_completion_versions(connection, native_params)
        entities = _partition_hashes(
            connection, source=f"{MANIFEST_TABLE} s JOIN {ENTITY_TABLE} e ON e.manifest_hash=s.manifest_hash",
            fields=("e.manifest_hash,e.stock_code,e.expected_state,e.classification,e.bar_count,"
                    "e.time_set_hash,e.first_time,e.last_time,e.source_row_hash,e.row_hash"),
            condition=manifests, params=native_params,
        )
        binary = "BINARY " if mysql else ""
        archive_query = text(f"""
            SELECT /*+ MAX_EXECUTION_TIME(5000) */ s.trade_date FROM (
                SELECT s.trade_date,s.run_id,s.strategy_eligible,
                       ROW_NUMBER() OVER (PARTITION BY s.trade_date
                                          ORDER BY s.captured_at DESC,s.manifest_hash DESC) AS n
                FROM {MANIFEST_TABLE} s
                WHERE s.trade_date IN :days AND {manifests} AND s.dataset=:archive
            ) s JOIN {RECEIPT_TABLE} r ON r.dataset=:archive AND r.trade_date=s.trade_date AND {binary}r.run_id={binary}s.run_id
            WHERE s.n=1 AND s.strategy_eligible=1 AND r.status='COMPLETE' AND r.finished_at<=:cutoff
        """).bindparams(bindparam("days", expanding=True))
        archive_visible = {str(row)[:10] for row in connection.execute(archive_query, native_params).scalars()}
    market = {}
    legacy = sorted(set(days) - native_days)
    market_fields = ("s.stock_code,s.pre_close,s.`open`,s.`close`,s.high,s.low,s.volume,s.amount,"
                     "s.batch_id,s.data_version,s.received_at,s.source_time,s.data_source,"
                     "s.quality_status,s.permission_status,s.received_at<=:cutoff")
    market_source = "sm_stock_kline s" + (" FORCE INDEX(idx_date_ktype)" if mysql else "")
    for selected, code_filter in ((legacy, a_share_stock_code_sql("s.stock_code")),
                                  (sorted(set(native) - archive_visible), "1=1")):
        market.update(_partition_hashes(connection, source=market_source, fields=market_fields,
                                      condition="s.k_type=1 AND s.adjust_type=0 AND " + code_filter,
                                      params={**params, "days": selected}))
    # With the physical append-only contract validated by the caller, one ID
    # identifies the entire immutable row including its original availability.
    # Include future rows here; the proof's visibility interval handles cutoff.
    attestation_fields = "s.stock_code,s.attestation_id"
    attestation_filter = "s.protocol_version=:protocol AND " + a_share_stock_code_sql("s.stock_code")
    if not sealed_attestations:
        attestation_fields += (",s.run_id,s.created_at,s.source_pre_close_origin,s.protocol_version,"
                               "s.target_id,s.qmt_id,s.source_data_version,s.source_pre_close,s.attested_open,"
                               "s.attested_close,s.attested_high,s.attested_low,s.attested_volume,s.attested_amount")
        attestation_filter += " AND s.created_at<=:cutoff"
    attestation = _partition_hashes(
        connection, source="qmt_kline_attestation_row s" + (
            " FORCE INDEX(idx_qmt_kline_attestation_row_date)" if mysql else ""),
        fields=attestation_fields, condition=attestation_filter,
        params={**params, "days": legacy},
    )
    return {day: canonical_digest([day, market.get(day), attestation.get(day), headers.get(day),
                                   receipts.get(day), entities.get(day)]) for day in days}


def _native_completion_versions(connection, params):
    # Two compact receipts per day at most. A replacement carries its prior
    # completions, each with its own visibility interval for historical reads.
    query = text(f"""
        SELECT /*+ MAX_EXECUTION_TIME(5000) */ trade_date,dataset,source_root,status,code_count,row_count,
               content_sha256,run_id,started_at,finished_at,detail_json
        FROM {RECEIPT_TABLE} WHERE trade_date IN :days AND dataset IN ('stock_daily',:archive)
        ORDER BY trade_date,dataset
    """).bindparams(bindparam("days", expanding=True))
    by_day = {}
    for row in connection.execute(query, params).mappings():
        detail = json.loads(row["detail_json"] or "{}")
        finished = row["finished_at"]
        visibility = []
        while finished is not None:
            visibility.append(normalize_decision_at(finished) <= params["cutoff"])
            prior = detail.get("prior_completion") if isinstance(detail, dict) else None
            if not isinstance(prior, dict):
                break
            finished, detail = prior.get("finished_at"), prior.get("detail")
        by_day.setdefault(str(row["trade_date"])[:10], []).append(canonical_digest([dict(row), visibility]))
    return by_day


# Content generation of the previously published MySQL V2 row-page query.
# Only its SQL-hash-checked rows on the sealed append-only ledger may be used.
_V1_MYSQL_PAGE_GENERATION = "a59003969924bac9428bc7316a7b36cde159cc504dcb8d233fe10f56b69b813c"
_PRICE_FIELDS = ("source_pre_close", "attested_open", "attested_close", "attested_high",
                 "attested_low", "attested_volume", "attested_amount")


def _legacy_exact_page_rows(page):
    rows, reread = [], []
    for original in page["rows"]:
        row = dict(original)
        for field in _PRICE_FIELDS:
            value = row[field]
            if type(value) is float:
                # The old reader converted DECIMAL(...,6) to binary64. Below
                # 2**33 its rounding error is strictly less than half a unit
                # at scale 6, so rounding the exact float recovers the SQL value.
                if not abs(value) < 2 ** 33:
                    reread.append(row["attestation_id"])
                    break
                row[field] = str(Decimal.from_float(value).quantize(Decimal("0.000001")))
        else:
            rows.append(row)
    return rows, reread


def read_legacy_attestations(read, params, *, database, deadline, sealed_attestations=False, original_times=None):
    """Resolve exact immutable evidence pages and filter on original availability."""
    mysql = database.dialect.name == "mysql"
    index = " FORCE INDEX (idx_qmt_kline_attestation_row_date)" if mysql else ""
    primary = " FORCE INDEX (PRIMARY)" if mysql else ""
    binary = "BINARY " if mysql else ""
    sql = (
        "SELECT /*+ MAX_EXECUTION_TIME(12000) */ "
        "a.attestation_id, a.created_at AS attestation_created_at, a.trade_date, a.stock_code, a.run_id, "
        "CAST(a.source_pre_close AS CHAR) AS source_pre_close, CAST(a.attested_open AS CHAR) AS attested_open, "
        "CAST(a.attested_close AS CHAR) AS attested_close, CAST(a.attested_high AS CHAR) AS attested_high, "
        "CAST(a.attested_low AS CHAR) AS attested_low, CAST(a.attested_volume AS CHAR) AS attested_volume, "
        "CAST(a.attested_amount AS CHAR) AS attested_amount "
        f"FROM qmt_kline_attestation_row a{primary} "
        "WHERE a.trade_date=:day AND a.protocol_version=:protocol_version "
        f"AND {binary}a.protocol_version={binary}:protocol_version AND a.qmt_id>0 "
        f"AND {binary}a.source_pre_close_origin={binary}'NATIVE_QMT' "
        f"AND {binary}a.attestation_id={binary}SHA2(CONCAT_WS('|', "
        "a.protocol_version, a.target_id, a.qmt_id, a.source_data_version, a.source_pre_close, "
        "a.attested_open, a.attested_close, a.attested_high, a.attested_low, "
        "a.attested_volume, a.attested_amount), 256) "
        f"AND {a_share_stock_code_sql('a.stock_code')} "
    )
    page_stage = "governance_attestation_rows"
    generation = canonical_digest({"schema": "probiga.governance-attestation-pages.v2",
                                   "sql": sql, "protocol": params["protocol_version"]})
    store = FormalAnalysisStore(database) if sealed_attestations else None
    old_pages_allowed = bool(store and mysql and params["protocol_version"] == "QMT_DAILY_UNADJUSTED_PRECLOSE_V2")

    def exact_rows(day, ids):
        if time.monotonic() >= deadline:
            raise TimeoutError(f"QMT certification read deadline: {day} after {ids[0]}")
        binds = {f"id{i}": value for i, value in enumerate(ids)}
        placeholders = ",".join(":" + name for name in binds)
        return read(sql + f" AND a.attestation_id IN ({placeholders})",
                    {**params, "day": day, **binds}, database=database)

    result = []
    day = datetime.fromisoformat(str(params["start_date"])[:10]).date()
    ending = datetime.fromisoformat(str(params["as_of_date"])[:10]).date()
    while day <= ending:
        selected_day = day.isoformat()
        code, row_id = "", ""
        inventory = {}
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"QMT certification inventory deadline: {selected_day}")
            rows = read(
                "SELECT /*+ MAX_EXECUTION_TIME(5000) */ a.stock_code,a.attestation_id "
                f"FROM qmt_kline_attestation_row a{index} WHERE a.trade_date=:day "
                "AND a.protocol_version=:protocol_version AND a.stock_code>=:code "
                "AND (a.stock_code>:code OR a.attestation_id>:row_id) "
                f"AND {a_share_stock_code_sql('a.stock_code')} "
                "ORDER BY a.stock_code,a.attestation_id LIMIT 8192",
                {**params, "day": selected_day, "code": code, "row_id": row_id}, database=database,
            )
            for row in rows:
                if row["attestation_id"] in inventory:
                    raise ValueError(f"QMT certification inventory repeats a primary key: {selected_day}")
                inventory[row["attestation_id"]] = row["stock_code"]
            if len(inventory) > 250000:
                raise ValueError(f"QMT certification inventory exceeds its bound: {selected_day}")
            if len(rows) < 8192:
                break
            next_key = rows[-1]["stock_code"], rows[-1]["attestation_id"]
            if next_key <= (code, row_id):
                raise ValueError(f"QMT certification cursor did not advance: {selected_day}")
            code, row_id = next_key

        ids = sorted(inventory)
        for offset in range(0, len(ids), 2048):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"QMT certification read deadline: {selected_day} {offset}/{len(ids)}")
            pages = {canonical_digest(ids[i:i + 64]): ids[i:i + 64]
                     for i in range(offset, min(offset + 2048, len(ids)), 64)}
            retained = store.get_many(generation, page_stage, list(pages)) if store else {}
            old = (store.get_many(_V1_MYSQL_PAGE_GENERATION, page_stage, [k for k in pages if k not in retained])
                   if old_pages_allowed and len(retained) < len(pages) else {})
            recovered = {key: _legacy_exact_page_rows(page) for key, page in old.items()}
            ambiguous = sorted(row_id for _, missing in recovered.values() for row_id in missing)
            corrected = {}
            for pos in range(0, len(ambiguous), 64):
                requested = ambiguous[pos:pos + 64]
                fetched = {row["attestation_id"]: row for row in exact_rows(selected_day, requested)}
                if fetched.keys() != set(requested):
                    raise ValueError(f"QMT immutable certification page changed: {selected_day}")
                corrected.update(fetched)
            pending = {}
            try:
                for key, selected in pages.items():
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"QMT certification read deadline: {selected_day} after {selected[0]}")
                    page = retained.get(key) or old.get(key)
                    if page is not None and page["ids"] != selected:
                        raise ValueError(f"QMT certification page identity differs: {selected_day}")
                    if key in retained:
                        rows, missing = page["rows"], []
                    elif page is not None:
                        rows, missing = recovered[key]
                        if missing:
                            rows = sorted([*rows, *(corrected[value] for value in missing)],
                                          key=lambda row: row["attestation_id"])
                    else:
                        rows = sorted(exact_rows(selected_day, selected), key=lambda row: row["attestation_id"])
                        missing = selected
                    row_ids = [row["attestation_id"] for row in rows]
                    if (len(set(row_ids)) != len(row_ids) or not set(row_ids) <= set(selected)
                            or any(str(row["trade_date"])[:10] != selected_day
                                   or row["stock_code"] != inventory[row["attestation_id"]] for row in rows)):
                        raise ValueError(f"QMT certification primary-key scope differs: {selected_day}")
                    if store and missing:
                        pending[key] = {"ids": selected, "rows": rows}
                    for row in rows:
                        available = normalize_decision_at(row["attestation_created_at"])
                        if original_times is not None:
                            original_times.add(available.isoformat())
                        if available <= params["decision_known_at"]:
                            result.append(row)
                    if len(pending) == 8:
                        ready, pending = pending, {}
                        store.put_many(generation, page_stage, ready)
            finally:
                if pending:
                    store.put_many(generation, page_stage, pending)
        day += timedelta(days=1)
    return result


@dataclass(frozen=True)
class NativeDatadirClose:
    manifest_hash: str
    run_id: str
    completed_at: datetime
    rows: tuple[dict[str, Any], ...]


def read_close_batches(batches, read, *, deadline):
    """Keep a bounded number of exact reads in flight on separate connections."""
    # Four concurrent full-market reads exhaust the production database's
    # response budget; isolated reads of the same partitions verify correctly.
    parallelism = 2
    remaining = iter(batches)
    workers = ThreadPoolExecutor(max_workers=parallelism, thread_name_prefix="governance-close")
    try:
        pending = {workers.submit(read, batch): batch
                   for _, batch in zip(range(parallelism), remaining)}
        while pending:
            done, _ = wait(pending, timeout=max(0, deadline - time.monotonic()), return_when=FIRST_COMPLETED)
            if not done:
                raise TimeoutError(f"QMT 收盘证据读取超时：{next(iter(pending.values()))}")
            for future in done:
                batch = pending.pop(future)
                try:
                    result = future.result()
                except Exception as exc:
                    original = getattr(exc, "orig", exc)
                    args = getattr(original, "args", ())
                    errno = args[0] if args and type(args[0]) is int else None
                    raise ValueError(f"QMT 收盘证据读取失败：{batch} ({type(exc).__name__}, errno={errno})") from exc
                next_batch = next(remaining, None)
                if next_batch is not None:
                    pending[workers.submit(read, next_batch)] = next_batch
                yield batch, result
    finally:
        workers.shutdown(wait=False, cancel_futures=True)


def datadir_manifest_days(connection, days: list[str], *, cutoff: datetime) -> set[str]:
    """Find dates with a published manifest without transferring manifest bodies."""
    if not days:
        return set()
    query = text(f"""
        SELECT /*+ MAX_EXECUTION_TIME(5000) */ DISTINCT trade_date
        FROM {MANIFEST_TABLE}
        WHERE schema_version=:schema AND dataset IN ('stock_daily', :archive_dataset) AND provider=:provider
          AND status='EXACT' AND strategy_eligible=1 AND captured_at<=:cutoff
          AND trade_date IN :days
    """).bindparams(bindparam("days", expanding=True))
    return {
        str(day)[:10]
        for day in connection.execute(query, {
            "schema": DATADIR_COVERAGE_SCHEMA, "archive_dataset": NATIVE_EVIDENCE_DATASET,
            "provider": PROVIDER, "cutoff": cutoff, "days": days,
        }).scalars()
    }


def load_native_datadir_close(
    connection, day: str, *, cutoff: datetime,
) -> NativeDatadirClose:
    """Verify the completed DAT manifest and exact current native database rows."""
    archived = load_daily_native_evidence(connection, day, decision_known_at=cutoff)
    if archived is not None:
        manifest, completion = archived
        completed_at = datetime.fromisoformat(str(completion["finished_at"]))
        rows = tuple({
            **entity["evidence"],
            "data_source": PROVIDER,
            "quality_status": "VERIFIED",
            "permission_status": "SUPPORTED",
            "source_time": f"{day}T15:00:00",
            "received_at": completed_at.isoformat(timespec="seconds"),
            "batch_id": manifest["run_id"],
            "data_version": entity["row_hash"],
            "attestation_id": canonical_digest({
                "manifest_hash": manifest["manifest_hash"], "entity_row_hash": entity["row_hash"],
            }),
            "pre_close_attestation_protocol": DATADIR_COVERAGE_SCHEMA,
            "source_pre_close_origin": "NATIVE_QMT",
        } for entity in manifest["entities"] if entity["classification"] == "TRADED")
        return NativeDatadirClose(manifest["manifest_hash"], manifest["run_id"], completed_at, rows)
    manifest = load_daily_manifest(connection, day, decision_known_at=cutoff, verify_rows=False)
    if manifest is None:
        raise ValueError(f"{day} 缺少截止时点前的 QMT DAT 全市场清单")
    public_codes = sorted(manifest.get("historical_public_bars") or {})
    if public_codes:
        raise ValueError(
            f"{day} 有 {len(public_codes)} 只股票缺少 QMT 原生收盘证据："
            + "、".join(public_codes[:5])
        )
    traded = [item for item in manifest["entities"] if item["classification"] == "TRADED"]
    expected = [item["evidence"] for item in traded]
    receipt = load_daily_manifest_completion(
        connection, day, manifest=manifest, decision_known_at=cutoff,
    ) or {}
    details = receipt.get("detail") or {}
    count = len(expected)
    if (
        not count or receipt.get("status") != "COMPLETE" or receipt.get("run_id") != manifest["run_id"]
        or details.get("full_market_manifest_hash") != manifest["manifest_hash"]
        or details.get("coverage") != 1.0
        or any(value != count for value in (
            receipt.get("row_count"), receipt.get("code_count"),
            details.get("readback_rows"), details.get("readback_codes"),
        ))
    ):
        raise ValueError(f"{day} 的 QMT DAT 完成回执与清单不一致")
    close_time = datetime.fromisoformat(f"{day}T15:00:00")
    completed_at = datetime.fromisoformat(str(receipt["finished_at"]))
    if not close_time <= datetime.fromisoformat(manifest["captured_at"]) <= completed_at <= cutoff:
        raise ValueError(f"{day} 的 QMT DAT 完成时间超出知识截止范围")
    query = text("""
        SELECT /*+ MAX_EXECUTION_TIME(10000) */ stock_code, trade_date,
               `open`, `close`, high, low, volume, amount, pre_close,
               data_source, quality_status, permission_status,
               source_time, received_at, batch_id, data_version
        FROM sm_stock_kline
        WHERE trade_date=:day AND k_type=1 AND adjust_type=0
        ORDER BY stock_code
    """)
    rows = [dict(row) for row in connection.execute(query, {"day": day}).mappings()]
    if [canonical_bar(row) for row in rows] != expected:
        raise ValueError(f"{day} 的 QMT DAT 全市场行情与清单不一致")
    binding_rows = []
    for row, entity in zip(rows, traded, strict=True):
        source = datetime.fromisoformat(str(row["source_time"]))
        received = datetime.fromisoformat(str(row["received_at"]))
        if (
            (row["data_source"], row["quality_status"], row["permission_status"])
            != (PROVIDER, "VERIFIED", "SUPPORTED")
            or not close_time <= source <= received <= completed_at
            or not row["batch_id"] or not row["data_version"]
        ):
            raise ValueError(f"{day} 的 QMT DAT 行情来源或收盘时间不一致")
        binding_rows.append({
            **row,
            "attestation_id": canonical_digest({
                "manifest_hash": manifest["manifest_hash"], "entity_row_hash": entity["row_hash"],
            }),
            "pre_close_attestation_protocol": DATADIR_COVERAGE_SCHEMA,
            "source_pre_close_origin": "NATIVE_QMT",
        })
    return NativeDatadirClose(manifest["manifest_hash"], manifest["run_id"], completed_at, tuple(binding_rows))

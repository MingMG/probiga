"""Read-only daily acquisition observation; never launches or repairs writers.

Business partitions are inspected on their configured databases. A bounded
single reader fills an in-process cache so a calendar request does not scan
hundreds of millions of minutes on an API request thread. Unknown, pending and
expired checks are first-class states, never successful acquisition evidence.
"""
from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
import csv
import io
import json
import logging
import re
from threading import Event, Lock
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.daily_stock_universe import load_daily_stock_universe
from server.common.kline_data import get_kline_engine
from server.common.minute_data import get_minute_engine
from server.common.qmt_history_coverage import minute_time_grid

CHINA = ZoneInfo("Asia/Shanghai")
log = logging.getLogger(__name__)
MAX_DAYS = 366
CACHE_SECONDS = 1800
MAX_CACHED_DAYS = 400
MAX_PENDING_DAYS = 366
MAX_DETAILS = 200


def now() -> datetime:
    return datetime.now(CHINA).replace(tzinfo=None, microsecond=0)


def iso(value: Any) -> str | None:
    return str(value).replace("T", " ")[:19] if value is not None else None


def daystr(value: Any) -> str:
    return str(value or "")[:10]


@dataclass(frozen=True)
class Dataset:
    key: str
    name: str
    group: str
    table: str
    store: str
    code: str = "stock_code"
    date_column: str = "trade_date"
    scope: str = ""
    deadline: str = "18:00"
    task_type: str = ""
    empty_allowed: bool = False


DATASETS = (
    Dataset("daily", "股票日线", "market", "sm_stock_kline", "kline", scope="全市场 · 日线 · 不复权", task_type="qmt_stock_daily_canonical"),
    Dataset("minute", "股票分钟线", "market", "sm_stock_minute", "kline", scope="全市场 · 1 分钟", deadline="15:35", task_type="qmt_stock_minute_canonical"),
    Dataset("flow", "每日资金流", "flow", "sm_stock_capital_flow_daily", "minute", scope="全市场 · 当日有成交", task_type="capital_flow_batch_fast"),
    Dataset("minute_flow", "分钟资金流", "flow", "sm_stock_capital_flow_min", "minute", date_column="trade_time", scope="全市场 · 原生 1 分钟", deadline="24:00", task_type="qmt_stock_minute_flow_canonical"),
    Dataset("alist", "龙虎榜列表", "event", "st_a_list_daily", "business", scope="当日完整披露清单", task_type="alist_daily", empty_allowed=True),
    Dataset("alist_info", "龙虎榜详情", "event", "st_a_list_info", "business", scope="当日席位明细", task_type="alist_info", empty_allowed=True),
    Dataset("index", "指数行情", "market", "sm_index_kline", "kline", code="index_code", scope="原生指数目录 · 日线", task_type="qmt_index_kline"),
    Dataset("concept", "概念行情", "market", "sm_concept_east_kline", "kline", code="index_code", scope="东财原生概念目录 · 日线", task_type="eastmoney_concept_kline"),
    Dataset("hot", "东财热门榜单", "event", "st_hot_pop_rank_east", "business", date_column="snapshot_date", scope="东财人气榜 · Top 100", deadline="17:14", task_type="hot_pop_east"),
    Dataset("hot_ths", "同花顺热门榜单", "event", "st_hot_rank_ths", "business", date_column="snapshot_date", scope="同花顺热股榜 · Top 100", deadline="17:12", task_type="hot_rank_ths"),
)
BY_KEY = {item.key: item for item in DATASETS}
BAD = {"partial", "missing", "unknown"}


def date_range(start: date, end: date) -> list[str]:
    if end < start or (end - start).days >= MAX_DAYS:
        raise ValueError("日期范围须为 1 至 366 个自然日")
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def due_at(spec: Dataset, day: str) -> datetime:
    midnight = datetime.combine(date.fromisoformat(day), time())
    hour, minute = map(int, spec.deadline.split(":"))
    return midnight + timedelta(hours=hour, minutes=minute)


def blank_cell(spec: Dataset, day: str, status: str, reason: str) -> dict:
    return dict(dataset=spec.key, trade_date=day, status=status, reason=reason,
                expected_count=None, actual_count=None, observed_count=None,
                missing_count=None, invalid_count=None, coverage_ratio=None,
                checked_at=None, data_updated_at=None, source=None, unit="条",
                evidence=None, missing=[], missing_total=None, tasks=[],
                task_state=None, stale=False, check_state="unchecked",
                due_at=iso(due_at(spec, day)))


def assess(expected: set[str] | None, groups: list[dict], *, slots: int = 1,
           expected_slots: list[str] | None = None, reason: str = "",
           allowed: set[str] | None = None,
           missing_limit: int | None = None) -> dict:
    """Compare identities, not row counts; invalid/duplicate rows never fill gaps."""
    actual_by_code = {str(r["code"]): r for r in groups}
    observed = sum(int(r.get("n") or 0) for r in groups)
    invalid = sum(int(r.get("bad") or 0) + max(0, int(r.get("n") or 0) - int(r.get("unique_n") or 0)) for r in groups)
    if expected is None:
        return dict(status="partial" if invalid else "unknown", expected_count=None,
                    actual_count=None, observed_count=observed, missing_count=None,
                    invalid_count=invalid, coverage_ratio=None, missing=[], missing_total=None,
                    reason=reason or "应采范围尚未获得可靠证据")
    valid = 0
    missing = []
    missing_total = 0

    def record_missing(item: dict) -> None:
        nonlocal missing_total
        missing_total += 1
        if missing_limit is None or len(missing) < max(0, missing_limit):
            missing.append(item)

    for code in sorted(expected):
        r = actual_by_code.get(code, {})
        count = min(slots, int(r.get("valid_n") or 0))
        valid += count
        duplicate = max(0, int(r.get("n") or 0)-int(r.get("unique_n") or 0))
        if count < slots or duplicate or int(r.get("bad") or 0):
            item = dict(stock_code=code, missing_count=slots-count,
                        reason="未入库" if not r else ("存在重复或无效记录" if duplicate or r.get("bad") else "缺少有效记录"))
            if expected_slots is not None:
                present = set(str(r.get("valid_times") or "").split(","))
                item["missing_times"] = compact_times([t for t in expected_slots if t not in present])
            record_missing(item)
    unexpected = sorted(set(actual_by_code) - (allowed if allowed is not None else expected))
    invalid += sum(int(actual_by_code[c].get("n") or 0) for c in unexpected)
    for code in unexpected:
        record_missing(dict(stock_code=code, missing_count=0,
                            reason="不在当日应采范围", missing_times=[]))
    count = len(expected) * slots
    if not count:
        status = "unknown"
        reason = reason or "应采范围为空，需要确认源端合法空数据证据"
    elif valid == count and not invalid:
        status = "full"
        reason = reason or "应采范围全部到齐，记录与关键字段校验通过"
    elif observed == 0:
        status = "missing"
        reason = "已过应完成时间，未发现当日数据"
    else:
        status = "partial"
        reason = "当日仍有缺口或数据校验异常"
    return dict(status=status, expected_count=count, actual_count=valid,
                observed_count=observed, missing_count=count-valid, invalid_count=invalid,
                coverage_ratio=valid/count if count else None, missing=missing,
                missing_total=missing_total, reason=reason)


def compact_times(times: list[str]) -> list[str]:
    ranges = []
    start = previous = None
    for value in times:
        stamp = datetime.strptime(value, "%H:%M:%S")
        if previous is not None and stamp-previous != timedelta(minutes=1):
            ranges.append(start.strftime("%H:%M") + ("–"+previous.strftime("%H:%M") if start != previous else ""))
            start = None
        start = start or stamp
        previous = stamp
    if previous is not None:
        ranges.append(start.strftime("%H:%M") + ("–"+previous.strftime("%H:%M") if start != previous else ""))
    return ranges


def calendar_map(rows: list[dict]) -> dict[str, int | None]:
    result: dict[str, int | None] = {}
    for row in rows:
        day = daystr(row["trade_date"])
        value = row.get("trade_status")
        value = int(value) if value in (0, 1, "0", "1") else None
        if day in result and result[day] != value:
            result[day] = None
        else:
            result[day] = value
    return result


def summarize(datasets: list[dict], days: list[str], calendar: dict) -> dict:
    complete = gaps = unknown = due = 0
    earliest = continuous = None
    uninterrupted = True
    for i, day in enumerate(days):
        if calendar.get(day) == 0:
            continue
        cells = [row["days"][i] for row in datasets]
        states = {c["status"] for c in cells}
        if not cells or states == {"pending"}:
            continue
        if calendar.get(day) != 1:
            unknown += 1
            uninterrupted = False
            continue
        due += 1
        if states == {"full"}:
            complete += 1
            if uninterrupted:
                continuous = day
        else:
            uninterrupted = False
        if states & {"partial", "missing"}:
            gaps += 1
            earliest = earliest or day
        if "unknown" in states:
            unknown += 1
    return dict(due_days=due, complete_days=complete, gap_days=gaps,
                unknown_days=unknown, earliest_gap=earliest, continuous_through=continuous)


class Observer:
    def __init__(self, business=None, kline=None, minute=None):
        self.engines = {"business": business or get_engine(), "kline": kline or get_kline_engine(),
                        "minute": minute or get_minute_engine()}
        self._columns: dict[tuple[str, str], set[str]] = {}

    def read(self, store: str, sql: str, params=None) -> list[dict]:
        if getattr(self, "stop_event", None) is not None and self.stop_event.is_set():
            raise RuntimeError("MONITOR_STOPPING")
        with self.engines[store].connect() as conn:
            return [dict(r) for r in conn.execute(text(sql), params or {}).mappings()]

    def columns(self, store: str, table: str) -> set[str]:
        key = (store, table)
        if key not in self._columns:
            self._columns[key] = {r["COLUMN_NAME"] for r in self.read(store,
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table", {"table": table})}
        return self._columns[key]

    def calendar(self, start: str, end: str) -> dict:
        return calendar_map(self.read("business", "SELECT trade_date, trade_status FROM si_trade_calendar "
                                      "WHERE trade_date BETWEEN :start AND :end", {"start": start, "end": end}))

    def _partition(self, spec: Dataset, day: str) -> tuple[str, str, dict]:
        predicate = f"`{spec.date_column}`=:day"
        params = {"day": day, "end": (date.fromisoformat(day)+timedelta(days=1)).isoformat()}
        if spec.date_column == "trade_time":
            predicate = "trade_time>=:day AND trade_time<:end"
        if spec.key == "daily":
            predicate += " AND k_type=1 AND adjust_type=0"
        elif spec.key in {"index", "concept"}:
            predicate += " AND k_type=1"
        return spec.table, predicate, params

    def counts(self, spec: Dataset, day: str) -> dict:
        table, predicate, params = self._partition(spec, day)
        columns = self.columns(spec.store, table)
        if not columns:
            raise RuntimeError("MONITOR_TABLE_UNAVAILABLE")
        updated = next((c for c in ("received_at", "etl_sync_at", "updated_at") if c in columns), None)
        source = "GROUP_CONCAT(DISTINCT data_source)" if "data_source" in columns else "NULL"
        rows = self.read(spec.store, f"SELECT COUNT(*) n, COUNT(DISTINCT `{spec.code}`) codes, "
                         f"{('MAX(`'+updated+'`)') if updated else 'NULL'} updated, {source} source "
                         f"FROM `{table}` WHERE {predicate}", params)
        return rows[0]

    def groups(self, spec: Dataset, day: str, *, grid: list[str] | None = None,
               codes: set[str] | None = None, _chunk=False, _source=False) -> list[dict]:
        table, predicate, params = self._partition(spec, day)
        columns = self.columns(spec.store, table)
        if not columns:
            raise RuntimeError("MONITOR_TABLE_UNAVAILABLE")
        if spec.key == "minute" and "data_source" in columns:
            if _source is False:
                sources = self.read(spec.store, f"SELECT DISTINCT data_source FROM `{table}` WHERE {predicate}", params)
                combined = {}
                for source in sources:
                    for row in self.groups(spec, day, grid=grid, codes=codes, _source=source["data_source"]):
                        previous = combined.get(row["code"])
                        if previous is None:
                            combined[row["code"]] = row
                            continue
                        old = set(filter(None, str(previous["valid_times"]).split(",")))
                        new = set(filter(None, str(row["valid_times"]).split(",")))
                        previous["n"] += row["n"]
                        previous["unique_n"] += row["unique_n"]-len(old & new)
                        previous["valid_n"] = len(old | new)
                        previous["bad"] += row["bad"]
                        previous["valid_times"] = ",".join(sorted(old | new))
                        previous["source"] = ",".join(filter(None, (previous.get("source"), row.get("source"))))
                        previous["updated"] = max(filter(None, (previous.get("updated"), row.get("updated"))), default=None)
                return list(combined.values())
            # Existing date/source/code/time index now serves GROUP BY in order,
            # avoiding a disk sort of the complete million-row market session.
            if _source is None:
                predicate += " AND data_source IS NULL"
            else:
                predicate += " AND data_source=:source"
                params["source"] = _source
        hint = ""
        if spec.key == "minute" and not _chunk:
            # Enumerate stored identities as well as the independent scope so
            # unexpected codes remain visible. Bound each expensive aggregation.
            stored = self.read(spec.store, f"SELECT DISTINCT stock_code FROM `{table}` WHERE {predicate}", params)
            all_codes = sorted({str(r["stock_code"]) for r in stored} | (codes or set()))
            result = []
            for offset in range(0, len(all_codes), 100):
                result.extend(self.groups(spec, day, grid=grid, codes=set(all_codes[offset:offset+100]),
                                          _chunk=True, _source=_source))
            return result
        if spec.key == "minute" and _chunk:
            hint = " FORCE INDEX (idx_smm_date_source_code_time)"
            params.update({f"c{i}": c for i, c in enumerate(sorted(codes))})
            predicate += " AND stock_code IN ("+",".join(f":c{i}" for i in range(len(codes)))+")"
        if spec.key == "minute_flow":
            if codes is None:
                raise ValueError("minute flow requires an authoritative stock scope")
            if not _chunk:
                all_codes = sorted(codes)
                result = []
                for offset in range(0, len(all_codes), 150):
                    result.extend(self.groups(spec, day, grid=grid, codes=set(all_codes[offset:offset+150]), _chunk=True))
                return result
            # This table is indexed by stock/time, not by date. Never scan its
            # entire retained history once per day while building the calendar.
            hint = " FORCE INDEX (idx_scfm_code_time)"
            params.update({f"c{i}": c for i, c in enumerate(sorted(codes))})
            predicate += " AND stock_code IN ("+",".join(f":c{i}" for i in range(len(codes)))+")"
        if spec.key in {"daily", "index", "concept"}:
            valid = "open>0 AND close>0 AND high>=GREATEST(open,close) AND low<=LEAST(open,close) AND low>0 AND volume>=0 AND amount>=0"
        elif spec.key in {"flow", "minute_flow"}:
            valid = " AND ".join(f"`{c}` IS NOT NULL" for c in ("main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"))
            valid += " AND ABS(main_net_inflow-max_net_inflow-lg_net_inflow)<=GREATEST(1000000,ABS(main_net_inflow)*0.001)"
        else:
            valid = "price>0 AND volume>=0 AND amount>=0"
        if "quality_status" in columns:
            valid += " AND COALESCE(quality_status,'') NOT IN ('QUARANTINED','INVALID','FAILED','REJECTED')"
        if "permission_status" in columns:
            valid += " AND COALESCE(permission_status,'') NOT IN ('NOT_AUTHORIZED','UNSUPPORTED_CLIENT','DENIED')"
        if grid is None:
            slots_sql = "1"
            valid_sql = f"CASE WHEN COUNT(*)=1 AND MIN(CASE WHEN {valid} THEN 1 ELSE 0 END)=1 THEN 1 ELSE 0 END"
            time_sql = "NULL"
        else:
            if grid != list(minute_time_grid()):
                raise ValueError("unrecognized minute grid")
            clock = "TIME(trade_time)"
            valid += (f" AND (({clock} BETWEEN '09:30:00' AND '11:30:00') OR "
                      f"({clock} BETWEEN '13:01:00' AND '15:00:00')) "
                      "AND SECOND(trade_time)=0 AND MICROSECOND(trade_time)=0 AND DATE(trade_time)=:day")
            ordinal = ("CASE WHEN TIME(trade_time)<='11:30:00' "
                       "THEN HOUR(trade_time)*60+MINUTE(trade_time)-569 "
                       "ELSE HOUR(trade_time)*60+MINUTE(trade_time)-659 END")
            # Return at most one compact row per stock, not a million raw bars.
            # Ordinals 1..241 take < 1024 bytes, MySQL's default GROUP_CONCAT limit.
            updated = "MAX(etl_sync_at)" if "etl_sync_at" in columns else "NULL"
            source = "GROUP_CONCAT(DISTINCT data_source)" if "data_source" in columns else "NULL"
            groups = self.read(spec.store, f"SELECT /*+ MAX_EXECUTION_TIME(20000) */ `{spec.code}` code,"
                f"COUNT(*) n,COUNT(DISTINCT trade_time) unique_n,"
                f"COUNT(DISTINCT CASE WHEN {valid} THEN trade_time END) valid_n,"
                f"SUM(CASE WHEN {valid} THEN 0 ELSE 1 END) bad,"
                f"GROUP_CONCAT(DISTINCT CASE WHEN {valid} THEN {ordinal} END) ordinals,"
                f"{updated} updated,{source} source FROM `{table}`{hint} WHERE {predicate} GROUP BY `{spec.code}`", params)
            for row in groups:
                row["valid_times"] = ",".join(grid[int(i)-1] for i in str(row.pop("ordinals") or "").split(",") if i)
            return groups
        return self.read(spec.store, f"SELECT `{spec.code}` code, COUNT(*) n, {slots_sql} unique_n, "
                         f"{valid_sql} valid_n, SUM(CASE WHEN {valid} THEN 0 ELSE 1 END) bad, "
                         f"{time_sql} valid_times FROM `{table}` WHERE {predicate} GROUP BY `{spec.code}`", params)

    def daily_context(self, day: str, current: datetime, *,
                      missing_limit: int | None = None) -> tuple[dict, set[str] | None, set[str] | None]:
        spec = BY_KEY["daily"]
        universe = None
        universe_error = ""
        try:
            universe = load_daily_stock_universe(self.engines["kline"], day, decision_known_at=current)
        except Exception as exc:
            universe_error = "历史股票范围或合法停牌豁免证据不可用（" + type(exc).__name__ + "）"
        groups = self.groups(spec, day)
        expected = set(universe.expected_codes) if universe else None
        result = assess(expected, groups, reason=universe_error,
                        missing_limit=missing_limit)
        result["unit"] = "只"
        if universe:
            result["evidence"] = {"catalog_batch_id": universe.catalog_batch_id,
                "catalog_manifest_hash": universe.catalog_manifest_hash,
                "excluded_no_row_count": len(universe.excluded_no_row_codes),
                "no_row_exception_proof_sha256": universe.no_row_exception_proof_sha256}
        traded = None
        if result["status"] == "full":
            traded = {str(r["stock_code"]) for r in self.read("kline", "SELECT stock_code FROM sm_stock_kline "
                       "WHERE trade_date=:day AND k_type=1 AND adjust_type=0 AND (volume>0 OR amount>0)", {"day": day})}
        return result, expected, traded

    def _receipt(self, spec: Dataset, day: str, current: datetime) -> dict | None:
        """Inspect completed executions since the target day, including later repairs.

        Run date is deliberately not used as the data date. Legacy runs without
        a machine receipt cannot prove an empty or complete source partition.
        """
        from server.common import scheduler_validation as validation
        parsers = {"alist": validation._eastmoney_alist_payload,
                   "alist_info": validation._eastmoney_alist_payload,
                   "index": validation._qmt_index_edge_payload,
                   "concept": validation._eastmoney_concept_market_payload,
                   "hot_ths": validation._ths_hot_payload}
        history = self.read("business", "SELECT id,run_uid,build_sha,run_at,finished_at,output "
            "FROM st_scheduled_task_history WHERE task_type=:task AND status='success' "
            "AND exit_code=0 AND finished_at IS NOT NULL AND run_at>=:day AND finished_at<=:now "
            "ORDER BY run_at DESC,id DESC LIMIT 500", {"task": spec.task_type, "day": day, "now": current})
        for row in history:
            output = str(row.get("output") or "")
            # Release records contain the original machine output in a hash-bound envelope.
            if '"replay_output"' in output:
                try:
                    from tools.ensure_quality_gate import _extract_release_validation_evidence
                    wrapper = _extract_release_validation_evidence(output)
                    if wrapper.get("run_uid") != row["run_uid"] or wrapper.get("build_sha") != row["build_sha"]:
                        continue
                    output = str(wrapper["replay_output"])
                except (ValueError, KeyError, TypeError):
                    continue
            if spec.key == "hot":
                from server.common.hot_rank_source_contract import parse_hot_rank_receipt
                payload = parse_hot_rank_receipt(output)
            else:
                payload = parsers[spec.key](output)
            if not payload:
                continue
            dates = payload.get("sessions") or payload.get("manifest", {}).get("sessions") or []
            target = payload.get("trade_date") or payload.get("target_trade_date") or payload.get("snapshot_date") or payload.get("requested_date")
            if day != target and day not in dates:
                continue
            if payload.get("build_sha") and payload["build_sha"] != row["build_sha"]:
                continue
            return {"payload": payload, "history_id": row["id"], "started_at": iso(row["run_at"]), "finished_at": iso(row["finished_at"])}
        return None

    def source_partition(self, spec: Dataset, day: str, current: datetime) -> dict:
        counts = self.counts(spec, day)
        observed = int(counts["n"])
        base = dict(observed_count=observed, actual_count=None, expected_count=None,
                    missing_count=None, coverage_ratio=None, invalid_count=None,
                    missing=[], missing_total=None, source=counts["source"],
                    data_updated_at=iso(counts["updated"]), unit="条")
        receipt = self._receipt(spec, day, current)
        if receipt is None:
            return dict(base, status="unknown" if observed or spec.empty_allowed else "missing",
                        reason="缺少当日完整源回执，已有记录不能证明全部到齐" if observed else
                        ("未发现记录，尚无源端合法空数据证据" if spec.empty_allowed else "已过完成时间，未发现当日记录"))
        payload = receipt["payload"]
        base["evidence"] = {"history_id": receipt["history_id"], "source_checked_at": receipt["finished_at"]}
        if spec.key in {"alist", "alist_info"}:
            from tools import sync_eastmoney_alist_exact as source
            if source.validate_task_result(payload, 0) != "complete":
                raise ValueError("invalid source receipt")
            dataset = "daily" if spec.key == "alist" else "info"
            if payload.get("dataset") != dataset:
                raise ValueError("source dataset differs")
            with self.engines["business"].connect() as conn:
                actual = source.database_proof(source._read_partition(conn, dataset=dataset, trade_date=day, include_storage=True), dataset=dataset)
            expected = payload["database"]
            match = actual == expected
            expected_count = int(expected["row_count"])
        elif spec.key == "index":
            from tools import sync_qmt_index_edge as source
            if source.validate_task_result(payload, 0) != "complete" or payload.get("dataset") != "kline":
                raise ValueError("invalid index receipt")
            # Historical evidence remains meaningful across software releases;
            # compare its source partition, not today's build identity.
            manifest = payload["manifest"]
            if manifest.get("sessions") != [day]:
                return dict(base, status="unknown", reason="源回执跨多个日期，需要逐日独立证据")
            import pandas as pd
            catalog = source._load_index_catalog(self.engines["business"], expected_batch_id=str(manifest["catalog_batch_id"]))
            if source._digest([asdict(member) for member in catalog]) != manifest["catalog_member_hash"]:
                raise ValueError("source index catalog differs")
            expected_by_session = source.expected_codes_by_session(catalog, [day])
            expected_count = sum(len(c) for c in expected_by_session.values())
            frame = source._read_published(dataset="kline", primary_engine=self.engines["business"],
                history_engine=self.engines["kline"], catalog=catalog,
                codes=sorted(set().union(*expected_by_session.values())), sessions=[day])
            verified = source._normalize_storage_precision(source.validate_kline_frame(frame, catalog=catalog,
                expected_by_session=expected_by_session, captured_at=datetime.fromisoformat(manifest["captured_at"])))
            digest = source._digest(verified.astype(object).where(pd.notna(verified), None).to_dict("records"))
            match = len(verified) == expected_count == int(manifest["expected_row_count"]) and digest == manifest["source_frame_hash"]
        elif spec.key == "concept":
            from tools import sync_eastmoney_concept_market as source
            if not validation_hash(payload, "result_sha256") or "kline" not in payload.get("datasets", []):
                raise ValueError("invalid concept receipt")
            # Native directory membership/hash rather than a QMT concept count.
            expected_count, match = self._concept_proof(payload, day)
        elif spec.key == "hot":
            from server.common.hot_rank_source_contract import validate_persisted_hot_rank_receipt
            if payload.get("task_type") != spec.task_type:
                raise ValueError("hot-rank dataset differs")
            proof = validate_persisted_hot_rank_receipt(self.engines["business"], payload,
                started_at=datetime.fromisoformat(receipt["started_at"]), now=current, expected_target_date=day)
            expected_count = 100
            match = bool(proof)
        elif spec.key == "hot_ths":
            from server.common import scheduler_validation as validation
            ok, _message = validation._validate_ths_hot_receipt(
                self.engines["business"],
                task_type=spec.task_type,
                output=json.dumps(payload, ensure_ascii=False, sort_keys=True),
                started_at=datetime.fromisoformat(receipt["started_at"]),
                now=current,
            )
            expected_count = int(payload.get("row_count") or 0)
            match = ok and expected_count == observed
        else:
            raise ValueError("unsupported source-backed dataset")
        base.update(expected_count=expected_count, actual_count=observed if match else None,
                    missing_count=0 if match else max(0, expected_count-observed),
                    coverage_ratio=1.0 if match else None,
                    status="full" if match else ("missing" if not observed and expected_count else "partial"),
                    reason=("完整源回执与实际入库内容一致" if match else "当前入库内容与完整源回执不一致"),
                    invalid_count=0 if match else None)
        return base

    def _concept_proof(self, payload: dict, day: str) -> tuple[int, bool]:
        from tools import sync_eastmoney_concept_market as source
        from server.common.scheduler_validation import _eastmoney_concept_market_output_status
        import hashlib
        if _eastmoney_concept_market_output_status("eastmoney_concept_kline", json.dumps(payload), return_code=0) != "success":
            raise ValueError("invalid concept publication")
        expected = payload["dataset_results"]["kline"]
        # Recheck historical content without requiring it to be today's latest session.
        rows = self.read("kline", "SELECT index_code,trade_date,trade_time,etl_sync_at,"
            "open,close,high,low,volume,amount,`change`,change_pct,k_type "
            "FROM sm_concept_east_kline WHERE trade_date=:day AND k_type=1 ORDER BY index_code,trade_time", {"day": day})
        codes = sorted({str(r["index_code"]) for r in rows})
        digest = hashlib.sha256("\n".join(codes).encode("utf-8")).hexdigest()
        match = (len(rows) == int(expected["row_count"]) == len(codes)
                 and digest == expected["code_set_sha256"] == payload["directory"]["code_set_sha256"]
                 and source.daily_content_hash(rows) == expected["content_sha256"])
        return int(expected["row_count"]), match

    def inspect_day(self, day: str, current: datetime, *,
                    datasets: set[str] | None = None,
                    missing_limit: int | None = MAX_DETAILS) -> dict[str, dict]:
        requested = tuple(
            spec for spec in DATASETS
            if datasets is None or spec.key in datasets
        )
        if datasets is not None and {spec.key for spec in requested} != datasets:
            raise ValueError("unknown monitored dataset")
        result = {}
        daily = traded = expected = None
        daily_error: Exception | None = None
        if any(spec.key in {"daily", "minute", "minute_flow", "flow"}
               for spec in requested):
            try:
                daily, expected, traded = self.daily_context(
                    day, current, missing_limit=missing_limit
                )
            except Exception as exc:
                daily_error = exc
        for spec in requested:
            cell = blank_cell(spec, day, "unknown", "等待检查")
            if current < due_at(spec, day):
                cell.update(status="pending", reason="尚未到该数据的应完成时间", check_state="not_due")
                result[spec.key] = cell
                continue
            try:
                if (daily_error is not None and
                        spec.key in {"daily", "minute", "minute_flow", "flow"}):
                    raise daily_error
                if spec.key == "daily":
                    info = daily
                elif spec.key in {"minute", "minute_flow", "flow"}:
                    scope = traded
                    grid = list(minute_time_grid()) if spec.key != "flow" else None
                    groups = self.groups(spec, day, grid=grid, codes=expected)
                    info = assess(scope, groups, slots=len(grid) if grid else 1,
                                  expected_slots=grid, allowed=expected if spec.key == "flow" else None,
                                  reason="当日日线尚未完整核验，无法确定有成交股票范围" if scope is None else "",
                                  missing_limit=missing_limit)
                    info["unit"] = "只" if spec.key == "flow" else "条"
                    info["evidence"] = {"basis": "已核验日线有成交集合", "grid_profile": "CN_A_SHARE_QMT_NATIVE_241_V1" if grid else None}
                    if grid:
                        info["data_updated_at"] = max((iso(g.get("updated")) for g in groups if g.get("updated")), default=None)
                        info["source"] = ",".join(sorted({str(g["source"]) for g in groups if g.get("source")})) or None
                else:
                    info = self.source_partition(spec, day, current)
                cell.update(info)
                if spec.key in {"daily", "flow"}:
                    counts = self.counts(spec, day)
                    cell.update(data_updated_at=iso(counts["updated"]), source=counts["source"])
            except Exception as exc:
                log.warning("data monitor check failed dataset=%s day=%s error_type=%s", spec.key, day, type(exc).__name__)
                cell.update(status="unknown", reason="当日检查未通过或数据源不可访问（"+type(exc).__name__+"），请重新检查")
            cell.update(checked_at=iso(now()), check_state="checked")
            result[spec.key] = cell
        return result

    def tasks(self, start: str, end: str, current: datetime) -> list[dict]:
        """Only a live scheduler execution is running; RETRYING gaps are queued."""
        rows = self.read("business", "SELECT h.id,h.task_name,h.task_type,h.status,h.run_at,h.finished_at,h.output,"
            "h.run_uid,h.scheduler_instance_id,r.heartbeat_at FROM st_scheduled_task_history h "
            "LEFT JOIN st_scheduler_runtime r ON r.instance_id=h.scheduler_instance_id "
            "WHERE h.run_at>=:start AND h.run_at<=:now "
            "AND (h.task_type LIKE '%repair%' OR h.task_type LIKE '%backfill%' OR h.task_type LIKE '%canonical%') "
            "ORDER BY h.run_at DESC,h.id DESC LIMIT 30", {"start": start, "now": current})
        out = []
        for row in rows:
            started = datetime.fromisoformat(iso(row["run_at"]))
            heartbeat = datetime.fromisoformat(iso(row["heartbeat_at"])) if row["heartbeat_at"] else None
            live = row["status"] == "running" and not row["finished_at"] and heartbeat is not None and timedelta(0) <= current-heartbeat <= timedelta(seconds=180)
            output = str(row.get("output") or "")
            targets = sorted(set(re.findall(r'"(?:trade_date|target_trade_date)"\s*:\s*"(\d{4}-\d{2}-\d{2})"', output)))
            targets = [d for d in targets if start <= d <= end]
            out.append(dict(id=row["id"], name=str(row["task_name"]), task_type=row["task_type"],
                status="running" if live else ("unknown" if row["status"] == "running" else row["status"]),
                started_at=iso(started), finished_at=iso(row["finished_at"]), heartbeat_at=iso(heartbeat),
                target_dates=targets, progress=None,
                progress_note="任务未提供可核验的缺口总量，不显示估算进度"))
        return out

    def gaps(self, day: str, dataset: str) -> list[dict]:
        names = {"daily": "sm_stock_kline.1d", "minute": "sm_stock_minute.1m"}
        if dataset not in names:
            return []
        return self.read("business", "SELECT id,status,reason,retry_count,next_retry_at,updated_at "
            "FROM sys_data_gap WHERE dataset=:dataset AND gap_start<:end AND gap_end>=:day "
            "AND status IN ('PENDING','RETRYING','FAILED','DEFERRED') ORDER BY id DESC LIMIT 50",
            {"dataset": names[dataset], "day": day, "end": (date.fromisoformat(day)+timedelta(days=1)).isoformat()})


def validation_hash(payload: dict, key: str) -> bool:
    from server.common.qmt_history_coverage import canonical_digest
    return payload.get(key) == canonical_digest({k: v for k, v in payload.items() if k != key})


def gap_csv(dataset: str, day: str, cell: dict) -> str:
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(["数据类型", "数据日期", "股票或指数代码", "缺失数量", "缺失时段", "原因", "检查时间"])
    for item in cell.get("missing", []):
        writer.writerow([BY_KEY[dataset].name, day, item["stock_code"], item["missing_count"],
                         "；".join(item.get("missing_times", [])), item["reason"], cell["checked_at"]])
    return "\ufeff"+stream.getvalue()


class Monitor:
    def __init__(self, observer_factory=Observer, clock=now, *, executor=None):
        self.observer_factory = observer_factory
        self.clock = clock
        self.executor = executor or ThreadPoolExecutor(max_workers=1, thread_name_prefix="data-monitor")
        self.lock = Lock()
        self.cache: OrderedDict[str, dict] = OrderedDict()
        self.pending: OrderedDict[str, None] = OrderedDict()
        self.active: set[str] = set()
        self.worker_running = False
        self.stopping = Event()

    def close(self):
        self.stopping.set()
        with self.lock:
            self.pending.clear()
        self.executor.shutdown(wait=True, cancel_futures=True)

    def _work(self, day: str):
        try:
            observer = self.observer_factory()
            observer.stop_event = self.stopping
            data = observer.inspect_day(day, self.clock())
            with self.lock:
                self.cache[day] = {"at": self.clock(), "data": data}
                self.cache.move_to_end(day)
                while len(self.cache) > MAX_CACHED_DAYS:
                    self.cache.popitem(last=False)
        except Exception as exc:
            data = {s.key: dict(blank_cell(s, day, "unknown", "检查服务暂不可用（"+type(exc).__name__+"）"),
                                checked_at=iso(self.clock()), check_state="checked") for s in DATASETS}
            with self.lock:
                self.cache[day] = {"at": self.clock(), "data": data}
                while len(self.cache) > MAX_CACHED_DAYS:
                    self.cache.popitem(last=False)
        finally:
            with self.lock:
                self.active.discard(day)

    def _drain(self):
        while True:
            with self.lock:
                if not self.pending:
                    self.worker_running = False
                    return
                day, _ = self.pending.popitem(last=False)
                self.active.add(day)
            self._work(day)

    def enqueue(self, day: str, *, force=False, priority=False):
        with self.lock:
            if self.stopping.is_set():
                return
            entry = self.cache.get(day)
            fresh = entry and (self.clock()-entry["at"]).total_seconds() < CACHE_SECONDS
            if fresh and any(entry["data"][s.key]["status"] == "pending" and self.clock() >= due_at(s, day) for s in DATASETS):
                fresh = False
            if day in self.pending:
                if priority:
                    self.pending.move_to_end(day, last=False)
                return
            if day in self.active or (fresh and not force) or len(self.pending) >= MAX_PENDING_DAYS:
                return
            self.pending[day] = None
            if priority:
                self.pending.move_to_end(day, last=False)
            if force and entry:
                # Never retain a green result while the user explicitly rechecks it.
                entry["at"] = self.clock()-timedelta(seconds=CACHE_SECONDS+1)
            start_worker = not self.worker_running
            self.worker_running = True
        try:
            if start_worker:
                self.executor.submit(self._drain)
        except Exception:
            with self.lock:
                self.pending.pop(day, None)
                self.worker_running = False
            raise

    def overview(self, start: date, end: date) -> dict:
        days = date_range(start, end)
        current = self.clock()
        observer = self.observer_factory()
        errors = []
        try:
            calendar = observer.calendar(days[0], days[-1])
        except Exception as exc:
            calendar = {}
            errors.append("交易日历不可用（"+type(exc).__name__+"）")
        for day in reversed(days):
            if calendar.get(day) == 1 and any(current >= due_at(s, day) for s in DATASETS):
                self.enqueue(day)
        try:
            tasks = observer.tasks(days[0], days[-1], current)
        except Exception as exc:
            tasks = []
            errors.append("补数任务状态不可用（"+type(exc).__name__+"）")
        datasets = []
        with self.lock:
            for spec in DATASETS:
                cells = []
                for day in days:
                    entry = self.cache.get(day)
                    if calendar.get(day) == 0:
                        cell = blank_cell(spec, day, "closed", "交易日历确认休市")
                    elif calendar.get(day) != 1:
                        cell = blank_cell(spec, day, "unknown", "交易日历未覆盖该日或记录存在冲突")
                    elif current < due_at(spec, day):
                        cell = blank_cell(spec, day, "pending", "尚未到该数据的应完成时间")
                    elif entry:
                        cell = dict(entry["data"][spec.key])
                        if cell["status"] == "pending":
                            cell.update(status="unknown", reason="已到完成时限，等待最新检查")
                        elif (current-entry["at"]).total_seconds() >= CACHE_SECONDS:
                            cell.update(status="unknown", stale=True, reason="检查结果已过期，等待复检")
                    else:
                        cell = blank_cell(spec, day, "unknown", "正在逐日核验实际入库数据")
                    if day in self.pending or day in self.active:
                        cell["check_state"] = "checking"
                    related = [t for t in tasks if day in t["target_dates"] and t["task_type"] == spec.task_type]
                    cell["tasks"] = [t["id"] for t in related]
                    cell["task_state"] = "running" if any(t["status"] == "running" for t in related) else (related[0]["status"] if related else None)
                    cells.append({k: v for k, v in cell.items() if k not in {"missing", "evidence"}})
                datasets.append({**asdict(spec), "days": cells})
            checked = sum(1 for d in days if d in self.cache and (current-self.cache[d]["at"]).total_seconds() < CACHE_SECONDS)
            pending = sum(1 for d in days if d in self.pending or d in self.active)
        return dict(schema="probiga.data-monitor.v1", generated_at=iso(current), start_date=days[0], end_date=days[-1],
                    dates=[dict(date=d, trade_status=calendar.get(d)) for d in days], datasets=datasets,
                    summary=summarize(datasets, days, calendar), tasks=tasks, errors=errors,
                    scan=dict(checked_days=checked, pending_days=pending), cache_seconds=CACHE_SECONDS,
                    refresh_seconds=30, timezone="Asia/Shanghai")

    def detail(self, dataset: str, day: date, *, recheck=False) -> dict:
        spec = BY_KEY[dataset]
        key = day.isoformat()
        overview = self.overview(day, day)
        summary = next(r["days"][0] for r in overview["datasets"] if r["key"] == dataset)
        if recheck and overview["dates"][0]["trade_status"] == 1 and self.clock() >= due_at(spec, key):
            self.enqueue(key, force=True, priority=True)
            summary.update(status="unknown", check_state="checking", reason="正在重新核验当日数据")
        elif overview["dates"][0]["trade_status"] == 1 and self.clock() >= due_at(spec, key):
            self.enqueue(key, priority=True)
        with self.lock:
            entry = self.cache.get(key)
            result = dict(entry["data"][dataset]) if entry else blank_cell(spec, key, summary["status"], summary["reason"])
        result.update(summary)
        result["definition"] = asdict(spec)
        result["tasks"] = [t for t in overview["tasks"] if t["id"] in summary["tasks"]]
        result["errors"] = overview["errors"]
        try:
            result["gaps"] = self.observer_factory().gaps(key, dataset)
        except Exception as exc:
            result["gaps"] = []
            result["errors"].append("补数队列不可用（"+type(exc).__name__+"）")
        return result

    def export(self, dataset: str, day: date) -> str:
        detail = self.detail(dataset, day)
        if detail["status"] not in {"full", "partial", "missing"}:
            raise ValueError("请等待当日检查完成后导出已确认缺口")
        observer = self.observer_factory()
        observer.stop_event = self.stopping
        data = observer.inspect_day(
            day.isoformat(), self.clock(), datasets={dataset},
            missing_limit=None,
        )
        cell = data[dataset]
        if cell["status"] not in {"full", "partial", "missing"}:
            raise ValueError("当前数据无法生成已确认缺口")
        return gap_csv(dataset, day.isoformat(), cell)


_monitor = None
_monitor_lock = Lock()


def get_monitor() -> Monitor:
    global _monitor
    with _monitor_lock:
        if _monitor is None:
            _monitor = Monitor()
        return _monitor


def stop_monitor() -> None:
    global _monitor
    with _monitor_lock:
        monitor, _monitor = _monitor, None
    if monitor is not None:
        monitor.close()

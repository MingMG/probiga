#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Publish an exact, date-proven Eastmoney popularity Top100 snapshot."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import requests
from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from server.common.batch_db import create_batch_engine, replace_table_rows
from server.common.hot_rank_schema import validate_hot_rank_runtime_schema
from server.common.hot_rank_source_contract import (
    AUTHORITATIVE_DATED_HISTORY,
    CURRENT_SNAPSHOT_ONLY,
    EAST_HISTORY_PROVIDER,
    HOT_POP_EAST_TASK_TYPE,
    HOT_RANK_CURRENT_PROVIDERS,
    HotRankDataBlocked,
    batch_timestamp,
    build_blocked_receipt,
    build_pass_receipt,
    require_current_capture_window,
    shanghai_now,
    validate_east_history_date_evidence,
    validate_rank_inventory,
)


_CURRENT_URL = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
_HISTORY_URL = "https://emappdata.eastmoney.com/stockrank/getHisList"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 "
        "Safari/537.36"
    ),
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://guba.eastmoney.com",
    "Referer": "https://guba.eastmoney.com/",
}
_BASE_PAYLOAD = {
    "appId": "appId01",
    "globalId": "786e4c21-70dc-435a-93bb-38",
    "marketType": "",
}


def _ensure_snapshot_date_column(engine) -> None:
    validate_hot_rank_runtime_schema(engine, tables={"st_hot_pop_rank_east"})


def _post_data(url: str, payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            response = requests.post(
                url,
                json=dict(payload),
                headers=_HEADERS,
                timeout=20,
            )
            response.raise_for_status()
            body = response.json()
            data = body.get("data") or []
            if not isinstance(data, list):
                raise RuntimeError("Eastmoney response data is not a list")
            return [dict(item) for item in data if isinstance(item, Mapping)]
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(attempt * 2)
    raise RuntimeError(f"Eastmoney request failed: {last_error}") from last_error


def _fetch_current_items() -> list[dict[str, Any]]:
    data = _post_data(
        _CURRENT_URL,
        {**_BASE_PAYLOAD, "pageNo": 1, "pageSize": 100},
    )
    if not data:
        raise RuntimeError("no Eastmoney current popularity rows fetched")
    return data


def _normalise_code(value: Any) -> str:
    raw = str(value or "").upper().strip()
    if len(raw) == 8 and raw[:2] in {"SH", "SZ", "BJ"}:
        raw = raw[2:]
    return raw.zfill(6)


def _src_security_code(stock_code: str) -> str:
    code = _normalise_code(stock_code)
    if code.startswith(("6", "9")):
        return f"SH{code}"
    if code.startswith(("0", "3")):
        return f"SZ{code}"
    if code.startswith(("4", "8")):
        return f"BJ{code}"
    raise ValueError(f"unsupported A-share code: {stock_code}")


def _fetch_history_date_row(
    stock_code: str,
    target_date: str,
) -> dict[str, Any] | None:
    data = _post_data(
        _HISTORY_URL,
        {**_BASE_PAYLOAD, "srcSecurityCode": _src_security_code(stock_code)},
    )
    matches = [
        item
        for item in data
        if str(item.get("calcTime") or "")[:10] == target_date
    ]
    if len(matches) > 1:
        raise RuntimeError(
            f"Eastmoney returned duplicate history rows for {stock_code} {target_date}"
        )
    if not matches:
        return None
    provider_response = dict(matches[0])
    rank = int(provider_response.get("rank") or 0)
    if rank < 1 or rank > 100:
        return None
    src_security_code = _src_security_code(stock_code)
    return {
        "rank": rank,
        "stock_code": _normalise_code(stock_code),
        # Preserve the provider fields verbatim.  The source contract converts
        # this into bounded replay evidence and binds it to the DB batch; a
        # current-list item (``rk``/``sc`` and no ``calcTime``) cannot satisfy
        # the historical contract.
        "request_src_security_code": src_security_code,
        "provider_response": {
            "calcTime": provider_response.get("calcTime"),
            "rank": provider_response.get("rank"),
        },
    }


def _history_candidate_codes(engine) -> tuple[list[str], dict[str, str]]:
    """Load the full A-share universe, with recent hot ranks only as seeds."""

    names: dict[str, str] = {}
    with engine.connect() as connection:
        master_rows = connection.execute(text("""
            SELECT stock_code, short_name
              FROM si_all_code
             WHERE stock_code LIKE '0%'
                OR stock_code LIKE '3%'
                OR stock_code LIKE '4%'
                OR stock_code LIKE '6%'
                OR stock_code LIKE '8%'
                OR stock_code LIKE '9%'
             ORDER BY stock_code
        """)).fetchall()
    master_codes: list[str] = []
    for raw_code, raw_name in master_rows:
        code = _normalise_code(raw_code)
        master_codes.append(code)
        names[code] = str(raw_name or "").strip()
    if len(set(master_codes)) < 100:
        raise RuntimeError("si_all_code does not contain a usable A-share universe")

    seeds: list[str] = []
    for table in (
        "st_hot_pop_rank_east",
        "st_hot_rank_ths",
        "st_hot_rank_sina",
        "st_hot_rank_xq",
    ):
        try:
            with engine.connect() as connection:
                rows = connection.execute(text(f"""
                    SELECT stock_code
                      FROM `{table}`
                     WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM `{table}`)
                     ORDER BY `rank`, stock_code
                """)).fetchall()
            seeds.extend(_normalise_code(row[0]) for row in rows)
        except Exception:
            continue
    return list(dict.fromkeys([*seeds, *master_codes])), names


def _fetch_historical_rows(
    engine,
    target_date: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates, names = _history_candidate_codes(engine)
    workers = max(
        1,
        min(16, int(os.environ.get("HOT_RANK_EAST_HISTORY_WORKERS", "8"))),
    )
    batch_size = max(64, workers * 8)
    evidence_by_rank: dict[int, dict[str, Any]] = {}

    for start in range(0, len(candidates), batch_size):
        selected = candidates[start:start + batch_size]
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_fetch_history_date_row, code, target_date): code
                for code in selected
            }
            for future in as_completed(futures):
                item = future.result()
                if item is None:
                    continue
                rank = int(item["rank"])
                previous = evidence_by_rank.get(rank)
                if previous is not None and previous["stock_code"] != item["stock_code"]:
                    raise RuntimeError(
                        "Eastmoney historical rank maps to multiple stock codes: "
                        f"rank={rank} codes={previous['stock_code']},{item['stock_code']}"
                    )
                evidence_by_rank[rank] = item
        if set(evidence_by_rank) == set(range(1, 101)):
            break

    evidence = [evidence_by_rank[rank] for rank in sorted(evidence_by_rank)]
    validate_east_history_date_evidence(evidence, target_date=target_date)
    rows = [
        {
            "rank": item["rank"],
            "stock_code": item["stock_code"],
            "short_name": names.get(item["stock_code"], ""),
            "rank_change": None,
            "his_rank": None,
            "price": None,
            "price_change": None,
            "change_pct": None,
            "hot_value": round(101 - int(item["rank"]), 1),
            "pop_tag": "历史排名",
            "concept_tag": None,
        }
        for item in evidence
    ]
    return rows, evidence


def _current_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        rank = int(item.get("rk") or 0)
        rank_change = int(item.get("rc") or 0)
        rows.append({
            "rank": rank,
            "stock_code": _normalise_code(item.get("sc")),
            "short_name": "",
            "rank_change": rank_change,
            "his_rank": int(item.get("hisRc") or 0),
            "price": None,
            "price_change": None,
            "change_pct": None,
            "hot_value": round(101 - rank, 1),
            "pop_tag": (
                "排名上升" if rank_change > 0
                else "排名下降" if rank_change < 0
                else "排名持平"
            ),
            "concept_tag": None,
        })
    return rows


def _enrich_current_rows(
    engine,
    rows: list[dict[str, Any]],
    snapshot_date: str,
) -> None:
    codes = [row["stock_code"] for row in rows]
    names_query = text("""
        SELECT stock_code, short_name
          FROM si_all_code
         WHERE stock_code IN :codes
    """).bindparams(bindparam("codes", expanding=True))
    try:
        with engine.connect() as connection:
            name_rows = connection.execute(names_query, {"codes": codes}).fetchall()
        name_map = {str(code): str(name or "") for code, name in name_rows}
        for row in rows:
            row["short_name"] = name_map.get(row["stock_code"], "")
    except Exception:
        pass

    concept_query = text("""
        SELECT c.stock_code,
               GROUP_CONCAT(DISTINCT cp.concept_name ORDER BY cp.rank SEPARATOR ';') AS concepts
          FROM si_concept_constituent_ths c
          JOIN st_hot_concept_ths_daily cp
            ON cp.concept_code=c.query_key
           AND cp.snapshot_date=:snapshot_date
           AND cp.plate_type=1
         WHERE c.stock_code IN :codes
         GROUP BY c.stock_code
    """).bindparams(bindparam("codes", expanding=True))
    try:
        with engine.connect() as connection:
            concept_rows = connection.execute(
                concept_query,
                {"snapshot_date": snapshot_date, "codes": codes},
            ).fetchall()
        concept_map = {str(code): value for code, value in concept_rows}
        for row in rows:
            row["concept_tag"] = concept_map.get(row["stock_code"])
    except Exception as exc:
        print(f"  概念板块查询略过: {exc}")

    sina_codes = ",".join(
        ("sh" + code if code.startswith(("6", "9")) else "sz" + code)
        for code in codes
        if code.startswith(("0", "3", "6", "9"))
    )
    if not sina_codes:
        return
    try:
        response = requests.get(
            f"https://hq.sinajs.cn/list={sina_codes}",
            headers={"Referer": "https://finance.sina.com.cn"},
            timeout=15,
        )
        response.raise_for_status()
        quotes: dict[str, dict[str, float]] = {}
        for line in response.text.strip().split("\n"):
            if "=" not in line or '\"\"' in line:
                continue
            variable, raw_values = line.split("=", 1)
            code = variable.split("_")[-1][2:]
            fields = raw_values.strip('\";\r ').split(",")
            if len(fields) < 4:
                continue
            try:
                previous = float(fields[2])
                current = float(fields[3])
                if current <= 0:
                    current = float(fields[1]) or previous
                change = current - previous
                quotes[code] = {
                    "price": round(current, 2),
                    "price_change": round(change, 2),
                    "change_pct": round(change / previous * 100, 2) if previous else 0,
                }
            except (ValueError, IndexError):
                continue
        for row in rows:
            row.update(quotes.get(row["stock_code"], {}))
    except Exception as exc:
        print(f"  新浪行情获取失败: {exc}")


def _readback_hot_rank(engine, snapshot_date: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(text("""
                SELECT snapshot_date, `rank`, stock_code, short_name,
                       rank_change, his_rank, price, price_change, change_pct,
                       hot_value, pop_tag, concept_tag, etl_sync_at
                  FROM st_hot_pop_rank_east
                 WHERE snapshot_date=:snapshot_date
                 ORDER BY `rank`, stock_code
            """), {"snapshot_date": snapshot_date}).mappings().all()
        ]


def fetch_hot_pop_rank_east(
    snapshot_date: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = shanghai_now(now)
    current_date = current.date().isoformat()
    if snapshot_date > current_date:
        raise HotRankDataBlocked("FUTURE_DATE_PROHIBITED")

    print(f"开始获取东财人气榜TOP100，快照日期: {snapshot_date}")
    engine = create_batch_engine()
    _ensure_snapshot_date_column(engine)

    date_evidence: dict[str, Any] | None = None
    if snapshot_date == current_date:
        started_at = require_current_capture_window(
            engine,
            task_type=HOT_POP_EAST_TASK_TYPE,
            requested_date=snapshot_date,
            now=current,
        )
        rows = _current_rows(_fetch_current_items())
        _enrich_current_rows(engine, rows, snapshot_date)
        provider = HOT_RANK_CURRENT_PROVIDERS[HOT_POP_EAST_TASK_TYPE]
        source_capability = CURRENT_SNAPSHOT_ONLY
    else:
        started_at = current
        rows, evidence = _fetch_historical_rows(engine, snapshot_date)
        date_evidence = validate_east_history_date_evidence(
            evidence,
            target_date=snapshot_date,
        )
        provider = EAST_HISTORY_PROVIDER
        source_capability = AUTHORITATIVE_DATED_HISTORY

    source_inventory = validate_rank_inventory(
        rows,
        task_type=HOT_POP_EAST_TASK_TYPE,
    )
    captured_at = shanghai_now()
    batch_at = captured_at.isoformat(sep=" ", timespec="seconds")
    frame = pd.DataFrame(rows)
    frame["snapshot_date"] = snapshot_date
    frame["etl_sync_at"] = captured_at
    frame = frame[[
        "snapshot_date", "rank", "stock_code", "short_name", "rank_change",
        "his_rank", "price", "price_change", "change_pct", "hot_value",
        "pop_tag", "concept_tag", "etl_sync_at",
    ]].replace({np.nan: None, pd.NaT: None})

    replace_table_rows(
        frame,
        "st_hot_pop_rank_east",
        engine,
        where_sql="snapshot_date = :d",
        params={"d": snapshot_date},
        chunksize=500,
        method="multi",
    )
    persisted = _readback_hot_rank(engine, snapshot_date)
    persisted_inventory = validate_rank_inventory(
        persisted,
        task_type=HOT_POP_EAST_TASK_TYPE,
        target_date=snapshot_date,
    )
    if (
        persisted_inventory["provider_payload_sha256"]
        != source_inventory["provider_payload_sha256"]
        or batch_timestamp(persisted) != batch_at
    ):
        raise RuntimeError("persisted Eastmoney hot-rank batch differs from provider response")

    receipt = build_pass_receipt(
        task_type=HOT_POP_EAST_TASK_TYPE,
        provider=provider,
        source_capability=source_capability,
        requested_date=snapshot_date,
        started_at=started_at,
        captured_at=captured_at,
        published_at=shanghai_now(),
        batch_at=batch_at,
        inventory=persisted_inventory,
        date_evidence=date_evidence,
    )
    print(
        f"写入完成: st_hot_pop_rank_east, 共 {len(frame)} 行, "
        f"快照日期: {snapshot_date}"
    )
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description="同步日期可信的东财人气榜Top100")
    parser.add_argument("date", help="快照日期，格式：YYYY-MM-DD")
    args = parser.parse_args()
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}", file=sys.stderr)
        return 1

    started_at = shanghai_now()
    try:
        result = fetch_hot_pop_rank_east(args.date, now=started_at)
    except HotRankDataBlocked as exc:
        result = build_blocked_receipt(
            task_type=HOT_POP_EAST_TASK_TYPE,
            requested_date=args.date,
            started_at=started_at,
            reason=str(exc),
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        print(f"Eastmoney hot rank sync blocked: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Eastmoney hot rank sync failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

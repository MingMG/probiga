#!/usr/bin/env python3
"""从 datacenter-web.eastmoney.com 获取概念资金流向，写入 sm_concept_capital_flow_east。

push2.eastmoney.com 被服务器 IP 封了，改用 datacenter-web API。
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import date, datetime, time as datetime_time, timedelta
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
REPORT_NAME = "RPT_CONCEPT_FUNDFLOW"
RESULT_SCHEMA = "probiga.eastmoney-concept-flow-result.v1"
MANIFEST_SCHEMA = "probiga.eastmoney-concept-flow-manifest.v1"
_SHANGHAI = ZoneInfo("Asia/Shanghai")
CONCEPT_FLOW_CLOSE_READY_TIME = datetime_time(15, 10)

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Referer": "https://data.eastmoney.com/",
})

_FIELD_MAP = {
    "BOARD_CODE": "index_code",
    "BOARD_NAME": "index_name",
    "CHANGE_RATE": "change_pct",
    "SUPERDEAL_NET": "max_net_inflow",
    "SUPERDEAL_NET_RATIO": "max_net_inflow_rate",
    "BIGDEAL_NET": "lg_net_inflow",
    "BIGDEAL_NET_RATIO": "lg_net_inflow_rate",
    "MIDDEAL_NET": "mid_net_inflow",
    "MIDDEAL_NET_RATIO": "mid_net_inflow_rate",
    "SMALLDEAL_NET": "sm_net_inflow",
    "SMALLDEAL_NET_RATIO": "sm_net_inflow_rate",
    "MAX_NETINFLOW_SEC": "stock_name",
}


from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.batch_db import create_batch_engine, replace_table_rows
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_trade_calendar import (
    load_trade_calendar_receipt,
    validate_trade_calendar_runtime_schema,
)


def _fetch_page(date_str: str, page: int, page_size: int = 500) -> dict | None:
    date_filter = f"(TRADE_DATE='{date_str}')"
    params = {
        "reportName": REPORT_NAME,
        "columns": "ALL",
        "pageNumber": page,
        "pageSize": page_size,
        "sortTypes": "-1",
        "sortColumns": "NET_INFLOW",
        "filter": date_filter,
        "source": "WEB",
        "client": "WEB",
    }
    for attempt in range(3):
        try:
            r = _SESSION.get(API_URL, params=params, timeout=20)
            r.raise_for_status()
            j = r.json()
            if j.get("success") and j.get("result"):
                return j["result"]
            return None
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
    return None


def _transform_rows(all_rows: list[dict]) -> pd.DataFrame:
    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    out = pd.DataFrame()
    for src, dst in _FIELD_MAP.items():
        if src in df.columns:
            out[dst] = df[src]

    out["main_net_inflow"] = (
        pd.to_numeric(out.get("max_net_inflow", 0), errors="coerce").fillna(0)
        + pd.to_numeric(out.get("lg_net_inflow", 0), errors="coerce").fillna(0)
    )
    out["main_net_inflow_rate"] = (
        pd.to_numeric(out.get("max_net_inflow_rate", 0), errors="coerce").fillna(0)
        + pd.to_numeric(out.get("lg_net_inflow_rate", 0), errors="coerce").fillna(0)
    )

    num_cols = [
        "change_pct", "main_net_inflow", "main_net_inflow_rate",
        "max_net_inflow", "max_net_inflow_rate",
        "lg_net_inflow", "lg_net_inflow_rate",
        "mid_net_inflow", "mid_net_inflow_rate",
        "sm_net_inflow", "sm_net_inflow_rate",
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out["stock_code"] = ""
    return out


def _fetch_all_for_date_capture(date_str: str) -> tuple[pd.DataFrame, dict]:
    result = _fetch_page(date_str, 1)
    if not result or not result.get("data"):
        return pd.DataFrame(), {
            "source_date": date_str,
            "page_count": 0,
            "provider_row_count": 0,
            "observed_row_count": 0,
            "pagination_complete": False,
        }

    all_rows = list(result["data"])
    try:
        total_pages = int(result.get("pages") or 1)
        provider_count = int(result.get("count") or len(all_rows))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "DATA_BLOCKED: Eastmoney concept-flow pagination metadata is invalid"
        ) from exc
    if total_pages < 1 or provider_count < 1:
        raise RuntimeError(
            "DATA_BLOCKED: Eastmoney concept-flow pagination metadata is empty"
        )
    for page in range(2, total_pages + 1):
        time.sleep(0.15)
        result = _fetch_page(date_str, page)
        if not result or not result.get("data"):
            raise RuntimeError(
                "DATA_BLOCKED: Eastmoney concept-flow pagination is incomplete: "
                f"source_date={date_str}, missing_page={page}/{total_pages}"
            )
        try:
            page_total = int(result.get("pages") or total_pages)
            page_count = int(result.get("count") or provider_count)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "DATA_BLOCKED: Eastmoney concept-flow page metadata is invalid"
            ) from exc
        if page_total != total_pages or page_count != provider_count:
            raise RuntimeError(
                "DATA_BLOCKED: Eastmoney concept-flow pagination metadata changed"
            )
        all_rows.extend(result["data"])

    raw_codes = [str(row.get("BOARD_CODE") or "").strip() for row in all_rows]
    raw_dates = {
        str(row.get("TRADE_DATE") or "")[:10] for row in all_rows
    }
    if (
        len(all_rows) != provider_count
        or any(not code for code in raw_codes)
        or len(raw_codes) != len(set(raw_codes))
        or raw_dates != {date_str}
    ):
        raise RuntimeError(
            "DATA_BLOCKED: Eastmoney concept-flow provider row/code inventory differs: "
            f"source_date={date_str}, observed={len(all_rows)}, reported={provider_count}"
        )
    return _transform_rows(all_rows), {
        "source_date": date_str,
        "page_count": total_pages,
        "provider_row_count": provider_count,
        "observed_row_count": len(all_rows),
        "pagination_complete": True,
        "code_set_hash": hashlib.sha256(
            json.dumps(sorted(raw_codes), separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


def _fetch_all_for_date(date_str: str) -> pd.DataFrame:
    frame, _evidence = _fetch_all_for_date_capture(date_str)
    return frame


def _lookup_stock_codes(engine, names: list[str]) -> dict[str, str]:
    if not names:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT short_name, stock_code FROM si_all_code WHERE short_name IN :names"),
            {"names": tuple(names)},
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def _fetch_latest_available_snapshot(
    *,
    now: datetime | None = None,
    lookback_days: int | None = None,
) -> tuple[pd.DataFrame, str]:
    current = now or datetime.now(_SHANGHAI)
    if current.tzinfo is not None:
        current = current.astimezone(_SHANGHAI)
    maximum = int(
        lookback_days
        if lookback_days is not None
        else os.environ.get("CONCEPT_FLOW_LOOKBACK_DAYS", "7")
    )
    if maximum < 1:
        raise RuntimeError(
            "DATA_BLOCKED: CONCEPT_FLOW_LOOKBACK_DAYS must be positive"
        )

    for offset in range(maximum):
        source_date = (current.date() - timedelta(days=offset)).isoformat()
        frame = _fetch_all_for_date(source_date)
        if frame is not None and not frame.empty:
            return frame, source_date
        print(f"  {source_date} 无数据，继续回看")
    raise RuntimeError(
        "DATA_BLOCKED: no Eastmoney concept-flow rows in the latest "
        f"{maximum} calendar days"
    )


def _authoritative_closed_session(
    engine,
    *,
    now: datetime,
    requested_trade_date: str = "",
) -> tuple[str, object]:
    current = now.astimezone(_SHANGHAI) if now.tzinfo is not None else now
    requested = str(requested_trade_date or "").strip()
    try:
        target = authoritative_closed_trade_date(
            engine,
            now=current,
            close_ready_time=CONCEPT_FLOW_CLOSE_READY_TIME,
        )
        parsed_target = date.fromisoformat(str(target or ""))
        parsed_requested = date.fromisoformat(requested) if requested else None
    except Exception as exc:
        raise RuntimeError(
            "DATA_BLOCKED: authoritative concept-flow closed session is unavailable"
        ) from exc
    if parsed_target.isoformat() != target:
        raise RuntimeError(
            "DATA_BLOCKED: authoritative concept-flow closed session is unavailable"
        )
    if parsed_requested is not None and (
        parsed_requested.isoformat() != requested or requested > target
    ):
        raise RuntimeError(
            "DATA_BLOCKED: requested concept-flow date is not yet authoritative: "
            f"requested={requested}, latest_closed={target}"
        )
    source_date = requested or target
    start_date = min(
        source_date,
        (current.date() - timedelta(days=14)).isoformat(),
    )
    end_date = current.date().isoformat()
    try:
        validate_trade_calendar_runtime_schema(engine)
        with engine.connect() as connection:
            receipt = load_trade_calendar_receipt(
                connection,
                start_date=start_date,
                end_date=end_date,
                decision_known_at=current.replace(tzinfo=None, microsecond=0),
            )
    except Exception as exc:
        raise RuntimeError(
            "DATA_BLOCKED: immutable QMT calendar receipt is unavailable for concept flow"
        ) from exc
    sessions = receipt.sessions_between(source_date, source_date)
    if tuple(sessions) != (source_date,):
        raise RuntimeError(
            "DATA_BLOCKED: concept-flow target lacks an immutable open-session receipt"
        )
    return source_date, receipt


def _strict_provider_snapshot(
    engine,
    *,
    now: datetime,
    requested_trade_date: str = "",
) -> tuple[pd.DataFrame, str, dict, object]:
    authority_kwargs = {"now": now}
    if requested_trade_date:
        authority_kwargs["requested_trade_date"] = requested_trade_date
    source_date, calendar = _authoritative_closed_session(
        engine,
        **authority_kwargs,
    )
    frame, evidence = _fetch_all_for_date_capture(source_date)
    if frame is None or frame.empty:
        raise RuntimeError(
            "DATA_BLOCKED: Eastmoney concept-flow has no rows for authoritative session "
            + source_date
        )
    if (
        evidence.get("pagination_complete") is not True
        or int(evidence.get("provider_row_count") or 0) != len(frame)
        or int(evidence.get("observed_row_count") or 0) != len(frame)
        or evidence.get("source_date") != source_date
    ):
        raise RuntimeError(
            "DATA_BLOCKED: Eastmoney concept-flow response is not an exact snapshot"
        )
    return frame, source_date, evidence, calendar


def _verify_strict_publish(
    engine,
    *,
    source_date: str,
    expected_codes: list[str],
) -> int:
    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT index_code, snapshot_at
            FROM sm_concept_capital_flow_east
            ORDER BY index_code
        """)).mappings().all()
    observed_codes = [str(row.get("index_code") or "").strip() for row in rows]
    observed_dates = {
        pd.Timestamp(row.get("snapshot_at")).date().isoformat() for row in rows
    }
    if (
        observed_codes != sorted(expected_codes)
        or len(observed_codes) != len(set(observed_codes))
        or observed_dates != {source_date}
    ):
        raise RuntimeError(
            "DATA_BLOCKED: concept-flow database verification differs from provider manifest"
        )
    return len(rows)


def fetch_concept_flow(
    *,
    strict_authority: bool = False,
    now: datetime | None = None,
    trade_date: str = "",
) -> dict:
    engine = create_batch_engine()

    print("开始获取概念资金流向 (datacenter-web)")

    capture_time = now or datetime.now(_SHANGHAI)
    evidence: dict = {}
    calendar = None
    if strict_authority:
        df, source_date, evidence, calendar = _strict_provider_snapshot(
            engine,
            now=capture_time,
            requested_trade_date=trade_date,
        )
    else:
        if now is None:
            df, source_date = _fetch_latest_available_snapshot()
        else:
            df, source_date = _fetch_latest_available_snapshot(now=capture_time)
    # Keep the publisher's atomic-replacement threshold aligned with the
    # scheduler's output contract.  A smaller snapshot must not replace the
    # last complete one only to be rejected after the write.
    minimum_rows = int(os.environ.get("CONCEPT_FLOW_MIN_ROWS", "100"))
    if len(df) < minimum_rows:
        raise RuntimeError(
            "DATA_BLOCKED: Eastmoney concept-flow snapshot is incomplete: "
            f"source_date={source_date}, rows={len(df)}, minimum={minimum_rows}"
        )
    df = df.copy()
    df["index_code"] = df["index_code"].fillna("").astype(str).str.strip()
    if (
        (df["index_code"] == "").any()
        or df["index_code"].duplicated(keep=False).any()
    ):
        raise RuntimeError(
            "DATA_BLOCKED: Eastmoney concept-flow contains empty/duplicate codes"
        )

    print(f"  获取到 {len(df)} 条概念资金流向数据，源日期: {source_date}")

    stock_names = [n for n in df["stock_name"].dropna().unique() if n]
    name_to_code = _lookup_stock_codes(engine, stock_names)
    df["stock_code"] = df["stock_name"].map(lambda n: name_to_code.get(n, ""))

    now = datetime.now(_SHANGHAI).replace(tzinfo=None, microsecond=0)
    df["days_type"] = 1
    df["snapshot_at"] = datetime.combine(
        date.fromisoformat(source_date), datetime_time.min
    )
    df["etl_sync_at"] = now

    out_cols = [
        "days_type", "index_code", "index_name", "change_pct",
        "main_net_inflow", "main_net_inflow_rate",
        "max_net_inflow", "max_net_inflow_rate",
        "lg_net_inflow", "lg_net_inflow_rate",
        "mid_net_inflow", "mid_net_inflow_rate",
        "sm_net_inflow", "sm_net_inflow_rate",
        "stock_code", "stock_name",
        "snapshot_at", "etl_sync_at",
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = None

    df = df[out_cols].replace({np.nan: None, pd.NaT: None})

    replace_table_rows(
        df,
        "sm_concept_capital_flow_east",
        engine,
        chunksize=500,
        method="multi",
    )

    print(f"写入完成: sm_concept_capital_flow_east, 共 {len(df)} 行")

    codes = sorted(df["index_code"].astype(str).tolist())
    verified_rows = len(df)
    if strict_authority:
        verified_rows = _verify_strict_publish(
            engine,
            source_date=source_date,
            expected_codes=codes,
        )
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "provider": "eastmoney.datacenter-web",
        "report_name": REPORT_NAME,
        "source_date": source_date,
        "row_count": len(df),
        "verified_row_count": verified_rows,
        "code_count": len(codes),
        "code_set_hash": canonical_digest(codes),
        "provider_code_set_hash": evidence.get("code_set_hash"),
        "strict_authority": bool(strict_authority),
        "captured_at": (
            capture_time.astimezone(_SHANGHAI).replace(tzinfo=None)
            if capture_time.tzinfo is not None
            else capture_time
        ).replace(microsecond=0).isoformat(sep=" "),
        "provider_page_count": int(evidence.get("page_count") or 0),
        "provider_reported_row_count": int(
            evidence.get("provider_row_count") or len(df)
        ),
        "provider_pagination_complete": (
            evidence.get("pagination_complete") is True
            if strict_authority else None
        ),
        "calendar_batch_id": getattr(calendar, "batch_id", None),
        "calendar_manifest_hash": getattr(calendar, "manifest_hash", None),
        "calendar_session_set_hash": getattr(calendar, "session_set_hash", None),
    }
    return {
        "schema": RESULT_SCHEMA,
        "status": "COMPLETE",
        "source_date": source_date,
        "provider": "eastmoney.datacenter-web",
        "strict_authority": bool(strict_authority),
        "written_rows": len(df),
        "db_verified_rows": verified_rows,
        "manifest": manifest,
        "manifest_hash": canonical_digest(manifest),
    }


def validate_task_result(
    payload: dict,
    return_code: int,
    *,
    expected_session: str = "",
) -> str:
    if payload.get("schema") != RESULT_SCHEMA:
        raise ValueError("Eastmoney concept-flow result schema differs")
    if payload.get("status") == "BLOCKED":
        if (
            int(return_code) != 3
            or not str(payload.get("reason") or "").startswith("DATA_BLOCKED:")
        ):
            raise ValueError("Eastmoney concept-flow blocked result differs")
        return "blocked"
    manifest = payload.get("manifest")
    if not isinstance(manifest, dict):
        raise ValueError("Eastmoney concept-flow manifest is missing")
    row_count = int(manifest.get("row_count") or 0)
    expected = str(expected_session or "").strip()
    if expected:
        try:
            parsed_expected = date.fromisoformat(expected)
        except ValueError as exc:
            raise ValueError(
                "Eastmoney concept-flow expected session is invalid"
            ) from exc
        if (
            parsed_expected.isoformat() != expected
            or payload.get("source_date") != expected
            or manifest.get("source_date") != expected
        ):
            raise ValueError(
                "Eastmoney concept-flow receipt differs from release target expected session"
            )
    if (
        int(return_code) != 0
        or payload.get("status") != "COMPLETE"
        or payload.get("strict_authority") is not True
        or manifest.get("schema") != MANIFEST_SCHEMA
        or manifest.get("strict_authority") is not True
        or payload.get("source_date") != manifest.get("source_date")
        or manifest.get("provider_pagination_complete") is not True
        or not re.fullmatch(
            r"[0-9a-f]{64}", str(manifest.get("provider_code_set_hash") or "")
        )
        or not str(manifest.get("calendar_batch_id") or "")
        or not re.fullmatch(
            r"[0-9a-f]{64}", str(manifest.get("calendar_manifest_hash") or "")
        )
        or not re.fullmatch(
            r"[0-9a-f]{64}", str(manifest.get("calendar_session_set_hash") or "")
        )
        or row_count < 100
        or int(manifest.get("code_count") or 0) != row_count
        or int(manifest.get("provider_reported_row_count") or 0) != row_count
        or int(payload.get("written_rows") or 0) != row_count
        or int(payload.get("db_verified_rows") or 0) != row_count
        or canonical_digest(manifest) != payload.get("manifest_hash")
    ):
        raise ValueError("Eastmoney concept-flow exact result proof differs")
    return "complete"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict-authority", action="store_true")
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = fetch_concept_flow(
            strict_authority=args.strict_authority,
            trade_date=args.trade_date,
        )
        return_code = 0
    except RuntimeError as exc:
        reason = str(exc)
        if reason.startswith("DATA_BLOCKED:"):
            payload = {
                "schema": RESULT_SCHEMA,
                "status": "BLOCKED",
                "provider": "eastmoney.datacenter-web",
                "reason": reason,
            }
            return_code = 3
        else:
            payload = {
                "schema": RESULT_SCHEMA,
                "status": "FAILED",
                "provider": "eastmoney.datacenter-web",
                "reason": reason,
            }
            return_code = 2
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())

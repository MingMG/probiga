"""
股票财务核心指标同步脚本

调用 adata stock.finance.get_core_index() 获取东方财富财务数据，
写入 si_stock_finance 表。

用法：
    python biz/stock_finance/sync_finance.py                 # 全量同步（增量：只拉新报告期）
    python biz/stock_finance/sync_finance.py --code 600396   # 同步单只股票
    python biz/stock_finance/sync_finance.py --limit 100      # 只同步前100只
"""

import argparse
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
import hashlib
import json
import re
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from sqlalchemy import bindparam, text

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from server.common.batch_db import create_batch_engine, read_frame
from server.common.finance_coverage import (
    FinanceDisclosureGate,
    coerce_optional_date,
    finance_disclosure_gate,
    report_period_gate_applies,
)
from server.common.pit_facts import (
    FINANCE_INCREMENTAL_DISCOVERY_CODE,
    FINANCE_INCREMENTAL_DISCOVERY_SCHEMA,
    FINANCE_INCREMENTAL_DISCOVERY_SOURCE,
    append_finance_atomic_batch_seal,
    append_finance_expected_unavailable,
    append_finance_revision,
    append_source_coverage,
    canonical_hash,
)


MAX_FETCH_WORKERS = 16
FETCH_PREFETCH_MULTIPLIER = 2
LEGACY_FINANCE_SOURCE = "adata.finance.core_index"
PRIMARY_FINANCE_SOURCE = "eastmoney.finance.mainfinadata.direct"
AUTHORITATIVE_FINANCE_SOURCES = (
    LEGACY_FINANCE_SOURCE,
    PRIMARY_FINANCE_SOURCE,
)
EASTMONEY_FINANCE_ENDPOINT = (
    "https://datacenter.eastmoney.com/securities/api/data/get"
)
EASTMONEY_FINANCE_REPORT = "RPT_F10_FINANCE_MAINFINADATA"
EASTMONEY_FINANCE_STYLE = "APP_F10_MAINFINADATA"
EASTMONEY_FINANCE_PAGE_SIZE = 500
EASTMONEY_FINANCE_MAX_PAGES = 20
FINANCE_DISCOVERY_SWEEPS = 2
FINANCE_DISCOVERY_MAX_DAYS = 14
CNINFO_FINANCE_NONFILING_SOURCE = "cninfo.finance.nonfiling"
CNINFO_ANNOUNCEMENT_ENDPOINT = (
    "https://www.cninfo.com.cn/new/hisAnnouncement/query"
)
CNINFO_STATIC_ROOT = "https://static.cninfo.com.cn/"
FINANCE_NONFILING_REASON = "CNINFO_REGULATORY_PERIODIC_REPORT_NOT_FILED"
FINANCE_NONFILING_MAX_AGE_DAYS = 7
FINANCE_NONFILING_QUERY_DAYS = 45
CNINFO_NONFILING_ISSUERS: dict[str, str] = {
    # ST Cuìhuá did not file its 2025 annual report or 2026 Q1 report by the
    # statutory deadline.  CNInfo org ids are issuer identities, not secrets.
    "002731": "9900022974",
}
CNINFO_NONFILING_PROVEN_THROUGH: dict[str, date] = {
    "002731": date(2026, 3, 31),
}

FINANCE_COLUMN_MAP = {
    "SECURITY_CODE": "stock_code",
    "SECURITY_NAME_ABBR": "short_name",
    "REPORT_DATE": "report_date",
    "REPORT_TYPE": "report_type",
    "NOTICE_DATE": "notice_date",
    "UPDATE_DATE": "source_update_date",
    "EPSJB": "basic_eps",
    "EPSKCJB": "diluted_eps",
    "EPSXS": "non_gaap_eps",
    "BPS": "net_asset_ps",
    "MGZBGJ": "cap_reserve_ps",
    "MGWFPLR": "undist_profit_ps",
    "MGJYXJJE": "oper_cf_ps",
    "TOTALOPERATEREVE": "total_rev",
    "MLR": "gross_profit",
    "PARENTNETPROFIT": "net_profit_attr_sh",
    "KCFJCXSYJLR": "non_gaap_net_profit",
    "TOTALOPERATEREVETZ": "total_rev_yoy_gr",
    "PARENTNETPROFITTZ": "net_profit_yoy_gr",
    "KCFJCXSYJLRTZ": "non_gaap_net_profit_yoy_gr",
    "YYZSRGDHBZC": "total_rev_qoq_gr",
    "NETPROFITRPHBZC": "net_profit_qoq_gr",
    "ROEJQ": "roe_wtd",
    "ROEKCJQ": "roe_non_gaap_wtd",
    "ZZCJLL": "roa_wtd",
    "XSMLL": "gross_margin",
    "XSJLL": "net_margin",
    "LD": "curr_ratio",
    "SD": "quick_ratio",
    "XJLLB": "cash_flow_ratio",
    "ZCFZL": "asset_liab_ratio",
}


class FinanceStaleResponse(RuntimeError):
    """A non-empty, identity-valid provider response with an old period."""


def get_engine():
    return create_batch_engine(pool_size=5, max_overflow=10)


def get_finance_stock_universe(engine) -> dict[str, date | None]:
    """Load the current authoritative A-share universe and listing dates."""

    df = read_frame(
        text(
            "SELECT stock_code, list_date FROM si_all_code "
            "WHERE stock_code REGEXP '^(0|3|4|6|8|9)[0-9]{5}$' "
            "AND (list_date IS NULL OR list_date <= CURRENT_DATE) "
            "ORDER BY stock_code"
        ),
        engine,
    )
    universe: dict[str, date | None] = {}
    for row in df.to_dict("records"):
        raw = str(row.get("stock_code") or "").strip()
        if not raw:
            raise RuntimeError("DATA_BLOCKED: finance universe contains empty code")
        code = raw.zfill(6)
        if code in universe:
            raise RuntimeError(
                f"DATA_BLOCKED: finance universe contains duplicate code {code}"
            )
        universe[code] = coerce_optional_date(row.get("list_date"))
    return universe


def get_all_stock_codes(engine) -> list[str]:
    """Backward-compatible ordered code list from the finance universe."""

    return list(get_finance_stock_universe(engine))


def _capture_now() -> datetime:
    return datetime.now(ZoneInfo("Asia/Shanghai")).replace(
        tzinfo=None,
        microsecond=0,
    )


def _eastmoney_secu_code(stock_code: str) -> str:
    code = str(stock_code or "").strip().zfill(6)
    prefix = code[:2]
    suffix = {
        "00": ".SZ", "20": ".SZ", "30": ".SZ",
        "43": ".BJ", "60": ".SH", "68": ".SH",
        "83": ".BJ", "87": ".BJ", "90": ".SH", "92": ".BJ",
    }.get(prefix)
    if suffix is None:
        raise RuntimeError(f"DATA_BLOCKED: {code} 交易所身份无法确定")
    return code + suffix


def _eastmoney_page(
    *,
    filter_expression: str,
    sort_fields: list[str],
    page: int,
) -> tuple[dict[str, Any], str, str]:
    response = requests.get(
        EASTMONEY_FINANCE_ENDPOINT,
        params={
            "type": EASTMONEY_FINANCE_REPORT,
            "sty": EASTMONEY_FINANCE_STYLE,
            "quoteColumns": "",
            "filter": filter_expression,
            "p": str(page),
            "ps": str(EASTMONEY_FINANCE_PAGE_SIZE),
            "sr": ",".join("-1" if index == 0 else "1" for index, _ in enumerate(sort_fields)),
            "st": ",".join(sort_fields),
            "source": "HSF10",
            "client": "PC",
        },
        headers={
            "Accept": "application/json",
            "Referer": "https://emweb.securities.eastmoney.com/",
            "User-Agent": "Mozilla/5.0 ProBigAFinance/2.0",
        },
        timeout=30,
    )
    response.raise_for_status()
    raw = bytes(response.content)
    try:
        payload = response.json()
    except (TypeError, ValueError) as exc:
        raise RuntimeError("DATA_BLOCKED: EastMoney finance response is not JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("DATA_BLOCKED: EastMoney finance response is malformed")
    if payload.get("code") == 9201 and payload.get("result") is None:
        if str(payload.get("message") or "") != "返回数据为空":
            raise RuntimeError("DATA_BLOCKED: EastMoney empty response identity differs")
        payload = {**payload, "result": {"pages": 1, "count": 0, "data": []}}
    elif payload.get("code") != 0 or not isinstance(payload.get("result"), dict):
        raise RuntimeError(
            "DATA_BLOCKED: EastMoney finance query failed: "
            f"code={payload.get('code')} message={payload.get('message')}"
        )
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return (
        payload,
        hashlib.sha256(raw).hexdigest(),
        hashlib.sha256(canonical).hexdigest(),
    )


def _fetch_eastmoney_result_set(
    *,
    filter_expression: str,
    sort_fields: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    first, first_raw_hash, first_content_hash = _eastmoney_page(
        filter_expression=filter_expression,
        sort_fields=sort_fields,
        page=1,
    )
    first_result = first["result"]
    try:
        pages = int(first_result.get("pages"))
        total = int(first_result.get("count"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("DATA_BLOCKED: EastMoney pagination metadata is invalid") from exc
    if not 1 <= pages <= EASTMONEY_FINANCE_MAX_PAGES or total < 0:
        raise RuntimeError("DATA_BLOCKED: EastMoney pagination bounds are invalid")

    all_rows: list[dict[str, Any]] = []
    page_raw_hashes: list[str] = []
    page_content_hashes: list[str] = []
    page_row_counts: list[int] = []
    for page in range(1, pages + 1):
        if page == 1:
            payload = first
            raw_hash = first_raw_hash
            content_hash = first_content_hash
        else:
            payload, raw_hash, content_hash = _eastmoney_page(
                filter_expression=filter_expression,
                sort_fields=sort_fields,
                page=page,
            )
        result = payload.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("DATA_BLOCKED: EastMoney page result is malformed")
        try:
            observed_pages = int(result.get("pages"))
            observed_total = int(result.get("count"))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("DATA_BLOCKED: EastMoney page metadata is invalid") from exc
        rows = result.get("data")
        if (
            observed_pages != pages
            or observed_total != total
            or not isinstance(rows, list)
            or any(not isinstance(row, dict) for row in rows)
        ):
            raise RuntimeError("DATA_BLOCKED: EastMoney pagination changed mid-sweep")
        all_rows.extend(dict(row) for row in rows)
        page_raw_hashes.append(raw_hash)
        page_content_hashes.append(content_hash)
        page_row_counts.append(len(rows))
    if len(all_rows) != total:
        raise RuntimeError(
            "DATA_BLOCKED: EastMoney pagination incomplete: "
            f"expected={total} actual={len(all_rows)}"
        )
    return all_rows, {
        "page_size": EASTMONEY_FINANCE_PAGE_SIZE,
        "page_count": pages,
        "total_count": total,
        "row_count": len(all_rows),
        "page_row_counts": page_row_counts,
        "page_raw_sha256": page_raw_hashes,
        "page_content_sha256": page_content_hashes,
    }


def _normalized_finance_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=list(FINANCE_COLUMN_MAP.values()))
    frame = pd.DataFrame(rows).rename(columns=FINANCE_COLUMN_MAP)
    for column in FINANCE_COLUMN_MAP.values():
        if column not in frame.columns:
            frame[column] = None
    frame = frame[list(FINANCE_COLUMN_MAP.values())].copy()
    for column in ("report_date", "notice_date", "source_update_date"):
        parsed = pd.to_datetime(frame[column], errors="coerce")
        frame[column] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), None)
    return frame.sort_values(
        by=["report_date", "report_type"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def fetch_finance(stock_code: str) -> pd.DataFrame:
    """Fetch one issuer once per sweep, retaining source update evidence.

    The prior adata adapter made four requests per issuer (one per report type)
    and discarded ``UPDATE_DATE``.  This uses the same EastMoney report with a
    single all-report query and requires two identical result sweeps.
    """

    code = str(stock_code or "").strip().zfill(6)
    secu_code = _eastmoney_secu_code(code)
    sweeps: list[dict[str, Any]] = []
    stable_rows: list[dict[str, Any]] | None = None
    stable_hash = ""
    for sweep_no in range(1, FINANCE_DISCOVERY_SWEEPS + 1):
        started = _capture_now()
        rows, pagination = _fetch_eastmoney_result_set(
            filter_expression=f'(SECUCODE="{secu_code}")',
            sort_fields=["REPORT_DATE", "REPORT_TYPE"],
        )
        normalized_source_rows = sorted(rows, key=lambda row: json.dumps(
            row,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ))
        content_hash = canonical_hash({
            "schema": "probiga.eastmoney-finance-issuer-response.v1",
            "stock_code": code,
            "rows": normalized_source_rows,
        })
        completed = _capture_now()
        if stable_rows is None:
            stable_rows = normalized_source_rows
            stable_hash = content_hash
        elif content_hash != stable_hash or normalized_source_rows != stable_rows:
            raise RuntimeError(
                f"DATA_BLOCKED: {code} EastMoney finance response changed between sweeps"
            )
        sweeps.append({
            "sweep_no": sweep_no,
            "started_at": started.isoformat(),
            "completed_at": completed.isoformat(),
            "content_sha256": content_hash,
            **pagination,
        })
    frame = _normalized_finance_rows(stable_rows or [])
    if not frame.empty:
        observed_codes = {
            str(value or "").strip().zfill(6)
            for value in frame["stock_code"].tolist()
        }
        if observed_codes != {code}:
            raise RuntimeError(
                f"DATA_BLOCKED: {code} EastMoney finance identity differs"
            )
    frame.attrs["source_receipt"] = {
        "schema": "probiga.eastmoney-finance-issuer-capture.v1",
        "source": PRIMARY_FINANCE_SOURCE,
        "endpoint": EASTMONEY_FINANCE_ENDPOINT,
        "stock_code": code,
        "captured_at": _capture_now().isoformat(),
        "stability_status": "STABLE_DOUBLE_SWEEP",
        "stable_sweep_count": len(sweeps),
        "stable_content_sha256": stable_hash,
        "sweeps": sweeps,
    }
    return frame


def _discovery_event(
    row: Mapping[str, Any],
    *,
    query_field: str,
    query_date: date,
) -> dict[str, Any]:
    source_code = str(row.get("SECURITY_CODE") or "").strip()
    code = source_code.zfill(6) if re.fullmatch(r"\d{1,6}", source_code) else ""
    report_date = coerce_optional_date(row.get("REPORT_DATE"))
    report_type = str(row.get("REPORT_TYPE") or "").strip()
    notice_date = coerce_optional_date(row.get("NOTICE_DATE"))
    update_date = coerce_optional_date(row.get("UPDATE_DATE"))
    exact_value = notice_date if query_field == "NOTICE_DATE" else update_date
    if (
        not re.fullmatch(r"[A-Za-z0-9]{1,16}", source_code)
        or report_date is None
        or not report_type
        or exact_value != query_date
    ):
        raise RuntimeError(
            "DATA_BLOCKED: EastMoney discovery returned a non-exact identity"
        )
    row_hash = canonical_hash({
        "schema": "probiga.eastmoney-finance-discovery-source-row.v1",
        "row": dict(row),
    })
    return {
        "query_field": query_field,
        "query_date": query_date.isoformat(),
        "source_security_code": source_code,
        "stock_code": code,
        "report_date": report_date.isoformat(),
        "report_type": report_type,
        "notice_date": notice_date.isoformat() if notice_date else None,
        "update_date": update_date.isoformat() if update_date else None,
        "row_content_sha256": row_hash,
    }


def fetch_finance_incremental_discovery(
    *,
    window_start: date,
    window_end: date,
    universe_codes: list[str],
) -> dict[str, Any]:
    """Double-scan exact NOTICE_DATE/UPDATE_DATE shards for changed issuers."""

    if window_start > window_end:
        raise ValueError("finance discovery window is invalid")
    day_count = (window_end - window_start).days + 1
    if day_count > FINANCE_DISCOVERY_MAX_DAYS:
        raise RuntimeError(
            "DATA_BLOCKED: finance discovery gap exceeds bounded daily window"
        )
    normalized_universe = sorted({
        str(code).strip().zfill(6) for code in universe_codes if str(code).strip()
    })
    if not normalized_universe:
        raise RuntimeError("DATA_BLOCKED: finance discovery universe is empty")

    stable_events: list[dict[str, Any]] | None = None
    stable_root = ""
    sweep_receipts: list[dict[str, Any]] = []
    fields = ["NOTICE_DATE", "UPDATE_DATE"]
    for sweep_no in range(1, FINANCE_DISCOVERY_SWEEPS + 1):
        sweep_started = _capture_now()
        sweep_events: list[dict[str, Any]] = []
        queries: list[dict[str, Any]] = []
        for offset in range(day_count):
            query_date = window_start + timedelta(days=offset)
            for query_field in fields:
                rows, pagination = _fetch_eastmoney_result_set(
                    filter_expression=(
                        f"({query_field}='{query_date.isoformat()} 00:00:00')"
                    ),
                    sort_fields=[
                        query_field,
                        "SECURITY_CODE",
                        "REPORT_DATE",
                        "REPORT_TYPE",
                    ],
                )
                events = sorted(
                    [
                        _discovery_event(
                            row,
                            query_field=query_field,
                            query_date=query_date,
                        )
                        for row in rows
                    ],
                    key=lambda item: json.dumps(
                        item,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
                identities = [
                    (
                        item["query_field"],
                        item["query_date"],
                        item["source_security_code"],
                        item["report_date"],
                        item["report_type"],
                    )
                    for item in events
                ]
                if len(set(identities)) != len(identities):
                    raise RuntimeError(
                        "DATA_BLOCKED: EastMoney discovery contains duplicate identities"
                    )
                query_hash = canonical_hash({
                    "schema": "probiga.pit-finance-discovery-query-result.v1",
                    "query_field": query_field,
                    "query_date": query_date.isoformat(),
                    "events": events,
                })
                queries.append({
                    "query_field": query_field,
                    "query_date": query_date.isoformat(),
                    **pagination,
                    "content_sha256": query_hash,
                })
                sweep_events.extend(events)
        queries.sort(key=lambda item: (item["query_date"], item["query_field"]))
        sweep_events.sort(key=lambda item: json.dumps(
            item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ))
        sweep_root = canonical_hash({
            "schema": "probiga.pit-finance-discovery-sweep.v1",
            "queries": queries,
        })
        if stable_events is None:
            stable_events = sweep_events
            stable_root = sweep_root
        elif sweep_root != stable_root or sweep_events != stable_events:
            raise RuntimeError(
                "DATA_BLOCKED: EastMoney discovery changed between sweeps"
            )
        sweep_receipts.append({
            "sweep_no": sweep_no,
            "started_at": sweep_started.isoformat(),
            "completed_at": _capture_now().isoformat(),
            "query_count": len(queries),
            "page_count": sum(int(item["page_count"]) for item in queries),
            "row_count": sum(int(item["row_count"]) for item in queries),
            "content_sha256": sweep_root,
            "queries": queries,
        })

    events = stable_events or []
    universe_set = set(normalized_universe)
    changed_codes = sorted({
        item["stock_code"] for item in events
        if item["stock_code"] in universe_set
    })
    return {
        "schema": FINANCE_INCREMENTAL_DISCOVERY_SCHEMA,
        "source": FINANCE_INCREMENTAL_DISCOVERY_SOURCE,
        "endpoint": EASTMONEY_FINANCE_ENDPOINT,
        "query_mode": "EXACT_DATE",
        "query_fields": fields,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "universe_code_count": len(normalized_universe),
        "universe_code_set_sha256": canonical_hash({
            "schema": "probiga.pit-finance-discovery-universe.v1",
            "codes": normalized_universe,
        }),
        "stable_sweep_count": len(sweep_receipts),
        "stability_status": "STABLE_DOUBLE_SWEEP",
        "stable_content_sha256": stable_root,
        "event_count": len(events),
        "event_set_sha256": canonical_hash({
            "schema": "probiga.pit-finance-discovery-event-set.v1",
            "events": events,
        }),
        "events": events,
        "changed_codes": changed_codes,
        "changed_code_set_sha256": canonical_hash({
            "schema": "probiga.pit-finance-discovery-changed-code-set.v1",
            "codes": changed_codes,
        }),
        "sweeps": sweep_receipts,
    }


def append_finance_incremental_discovery(
    engine,
    evidence: Mapping[str, Any],
) -> str:
    captured_at = _capture_now()
    window_start = date.fromisoformat(str(evidence["window_start"]))
    window_end = date.fromisoformat(str(evidence["window_end"]))
    receipt = append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code=FINANCE_INCREMENTAL_DISCOVERY_CODE,
        window_start=window_start,
        window_end=window_end,
        known_at=captured_at,
        received_at=captured_at,
        covered_through_at=captured_at,
        watermark_kind="CAPTURED_AT",
        watermark_evidence=dict(evidence),
        source_rows=[],
        fact_bindings=[],
        source=FINANCE_INCREMENTAL_DISCOVERY_SOURCE,
        batch_id=(
            "eastmoney-finance-discovery-"
            + str(evidence.get("stable_content_sha256") or "")[:32]
        ),
    )
    return receipt.coverage_id


def append_new_listing_finance_empty_coverage(
    engine,
    *,
    stock_code: str,
    listing_date: date,
    as_of: date,
    disclosure_deadline: date,
    source_receipt: Mapping[str, Any],
) -> str:
    """Record audited legal no-data for a post-deadline new listing."""

    if listing_date > as_of or listing_date <= disclosure_deadline:
        raise ValueError("new-listing finance empty disposition is inapplicable")
    captured_at = _capture_now()
    from server.common.qmt_stock_catalog import load_stock_catalog

    with engine.connect() as connection:
        catalog = load_stock_catalog(
            connection,
            decision_known_at=captured_at,
        )
    catalog_member = next(
        (
            item for item in catalog.members
            if str(item.get("stock_code") or "").zfill(6) == stock_code
        ),
        None,
    )
    if (
        catalog_member is None
        or coerce_optional_date(catalog_member.get("list_date")) != listing_date
        or stock_code not in catalog.eligible_codes(as_of.isoformat())
    ):
        raise ValueError("new-listing finance disposition catalog binding differs")
    receipt = append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code=stock_code,
        window_start="1900-01-01",
        window_end=as_of,
        known_at=captured_at,
        received_at=captured_at,
        covered_through_at=captured_at,
        watermark_kind="CAPTURED_AT",
        watermark_evidence={
            "provider": PRIMARY_FINANCE_SOURCE,
            "capture": "stable_eastmoney_result_set",
            "resolution_type": "STATUTORY_NOT_APPLICABLE",
            "reason_code": "NEW_LISTING_AFTER_DISCLOSURE_DEADLINE",
            "stock_code": stock_code,
            "listing_date": listing_date.isoformat(),
            "disclosure_deadline": disclosure_deadline.isoformat(),
            "as_of_date": as_of.isoformat(),
            "catalog_batch_id": catalog.batch_id,
            "catalog_manifest_hash": catalog.manifest_hash,
            "catalog_member_set_hash": catalog.member_set_hash,
            "catalog_member_count": catalog.member_count,
            "source_receipt": dict(source_receipt),
            "source_timestamp_guard": {
                "status": "PASS",
                "as_of_date": as_of.isoformat(),
                "captured_at": captured_at.isoformat(),
                "maximum_notice_date": None,
                "maximum_update_date": None,
            },
        },
        source_rows=[],
        fact_bindings=[],
        source=PRIMARY_FINANCE_SOURCE,
        batch_id=(
            "eastmoney-finance-new-listing-empty-"
            + captured_at.strftime("%Y%m%dT%H%M%S")
        ),
    )
    return receipt.coverage_id


def fetch_cninfo_nonfiling_evidence(
    stock_code: str,
    *,
    as_of: date,
    expected_report_date: date,
) -> dict[str, Any]:
    """Return recent official proof that a required periodic report is absent."""

    code = str(stock_code or "").strip().zfill(6)
    org_id = CNINFO_NONFILING_ISSUERS.get(code)
    if not org_id:
        raise RuntimeError(
            f"DATA_BLOCKED: {code} has no reviewed CNInfo non-filing identity"
        )
    start = as_of - timedelta(days=FINANCE_NONFILING_QUERY_DAYS)
    response = requests.post(
        CNINFO_ANNOUNCEMENT_ENDPOINT,
        data={
            "pageNum": "1",
            "pageSize": "30",
            "column": "szse",
            "tabName": "fulltext",
            "plate": "",
            "stock": f"{code},{org_id}",
            "searchkey": "",
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": f"{start.isoformat()}~{as_of.isoformat()}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "false",
        },
        headers={
            "Accept": "application/json",
            "Referer": "https://www.cninfo.com.cn/",
            "User-Agent": "Mozilla/5.0 ProBigAFinance/1.0",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=30,
    )
    response.raise_for_status()
    api_raw = bytes(response.content)
    try:
        payload = response.json()
    except (TypeError, ValueError) as exc:
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing response is not JSON") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing response is malformed")
    announcements = payload.get("announcements")
    if not isinstance(announcements, list):
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing catalogue is malformed")
    candidates: list[tuple[datetime, Mapping[str, Any]]] = []
    for item in announcements:
        if not isinstance(item, Mapping):
            raise RuntimeError("DATA_BLOCKED: CNInfo announcement row is malformed")
        if (
            str(item.get("secCode") or "").zfill(6) != code
            or str(item.get("orgId") or "") != org_id
        ):
            raise RuntimeError("DATA_BLOCKED: CNInfo announcement identity differs")
        title = str(item.get("announcementTitle") or "")
        if not (
            "未在规定期限内披露定期报告" in title
            or "无法在法定期限内披露定期报告" in title
            or (
                "无法在规定期限内披露" in title
                and "年度报告" in title
            )
        ):
            continue
        try:
            published = datetime.fromtimestamp(
                int(item.get("announcementTime")) / 1000,
                tz=ZoneInfo("Asia/Shanghai"),
            ).replace(tzinfo=None)
        except (TypeError, ValueError, OSError, OverflowError) as exc:
            raise RuntimeError("DATA_BLOCKED: CNInfo announcement time is invalid") from exc
        candidates.append((published, item))
    if not candidates:
        raise RuntimeError("DATA_BLOCKED: no official recent non-filing proof")
    published, selected = max(candidates, key=lambda pair: pair[0])
    announcement_date = published.date()
    valid_until = announcement_date + timedelta(days=FINANCE_NONFILING_MAX_AGE_DAYS)
    if announcement_date > as_of or as_of > valid_until:
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing proof is outside validity")
    announcement_id = str(selected.get("announcementId") or "")
    adjunct = str(selected.get("adjunctUrl") or "")
    if (
        not re.fullmatch(r"\d+", announcement_id)
        or not re.fullmatch(
            rf"finalpage/\d{{4}}-\d{{2}}-\d{{2}}/{re.escape(announcement_id)}\.PDF",
            adjunct,
        )
    ):
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing document identity differs")
    document_url = CNINFO_STATIC_ROOT + adjunct
    document = requests.get(
        document_url,
        headers={"User-Agent": "Mozilla/5.0 ProBigAFinance/1.0"},
        timeout=30,
    )
    document.raise_for_status()
    document_raw = bytes(document.content)
    if len(document_raw) < 1024 or not document_raw.startswith(b"%PDF"):
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing document is not a PDF")
    next_retry = min(as_of + timedelta(days=1), valid_until)
    return {
        "source": CNINFO_FINANCE_NONFILING_SOURCE,
        "reason_code": FINANCE_NONFILING_REASON,
        "stock_code": code,
        "expected_report_date": expected_report_date.isoformat(),
        "announcement_id": announcement_id,
        "announcement_title": str(selected.get("announcementTitle") or ""),
        "announcement_published_at": published.replace(microsecond=0).isoformat(),
        "announcement_url": document_url,
        "announcement_document_sha256": hashlib.sha256(document_raw).hexdigest(),
        "catalog_response_sha256": hashlib.sha256(api_raw).hexdigest(),
        "valid_from": announcement_date.isoformat(),
        "valid_until": valid_until.isoformat(),
        "next_retry_date": next_retry.isoformat(),
    }


def get_finance_incremental_baselines(
    engine,
) -> dict[str, dict[str, Any]]:
    """Load compact latest coverage metadata without materializing payload rows."""

    sources_sql = ",".join(f"'{source}'" for source in AUTHORITATIVE_FINANCE_SOURCES)
    frame = read_frame(
        text(
            "WITH ranked AS ("
            " SELECT coverage.stock_code, coverage.coverage_id,"
            " coverage.window_start, coverage.window_end, coverage.known_at,"
            " coverage.coverage_status, coverage.result_count,"
            " coverage.source,"
            " ROW_NUMBER() OVER (PARTITION BY coverage.stock_code"
            " ORDER BY coverage.known_at DESC, coverage.revision_no DESC) AS rn"
            " FROM st_pit_source_coverage AS coverage"
            " WHERE coverage.fact_kind='finance'"
            f" AND coverage.source IN ({sources_sql})"
            ")"
            " SELECT ranked.stock_code, ranked.coverage_id, ranked.window_start,"
            " ranked.window_end, ranked.known_at, ranked.coverage_status,"
            " ranked.result_count, ranked.source,"
            " latest.latest_report_date"
            " FROM ranked LEFT JOIN ("
            " SELECT stock_code, MAX(report_date) AS latest_report_date"
            " FROM si_stock_finance GROUP BY stock_code"
            ") AS latest ON latest.stock_code=ranked.stock_code"
            " WHERE ranked.rn=1 ORDER BY ranked.stock_code"
        ),
        engine,
    )
    rows = {
        str(row.get("stock_code") or "").strip().zfill(6): dict(row)
        for row in frame.to_dict("records")
        if str(row.get("stock_code") or "").strip()
    }
    late_ids = [
        str(row.get("coverage_id") or "")
        for row in rows.values()
        if (
            coerce_optional_date(row.get("window_end")) is not None
            and not pd.isna(pd.to_datetime(row.get("known_at"), errors="coerce"))
            and pd.to_datetime(row.get("known_at"), errors="coerce").date()
            > coerce_optional_date(row.get("window_end"))
        )
    ]
    for offset in range(0, len(late_ids), 100):
        identifiers = late_ids[offset : offset + 100]
        if not identifiers:
            continue
        guard_frame = read_frame(
            text(
                "SELECT coverage_id,"
                " JSON_UNQUOTE(JSON_EXTRACT(payload_json,"
                " '$.watermark.evidence.source_timestamp_guard.status'))"
                " AS guard_status,"
                " JSON_UNQUOTE(JSON_EXTRACT(payload_json,"
                " '$.watermark.evidence.source_timestamp_guard.as_of_date'))"
                " AS guard_as_of_date"
                " FROM st_pit_source_coverage WHERE coverage_id IN :coverage_ids"
            ).bindparams(bindparam("coverage_ids", expanding=True)),
            engine,
            params={"coverage_ids": identifiers},
        )
        guards = {
            str(item.get("coverage_id") or ""): dict(item)
            for item in guard_frame.to_dict("records")
        }
        for row in rows.values():
            guard = guards.get(str(row.get("coverage_id") or ""))
            if guard:
                row.update({
                    "guard_status": guard.get("guard_status"),
                    "guard_as_of_date": guard.get("guard_as_of_date"),
                })
    return rows


def build_finance_incremental_plan(
    engine,
    *,
    universe: Mapping[str, date | None],
    as_of: date,
    disclosure_gate: FinanceDisclosureGate,
) -> dict[str, Any]:
    """Choose refresh vs immutable reuse; any uncertain proof refreshes."""

    codes = sorted(universe)
    baselines = get_finance_incremental_baselines(engine)
    refresh_reasons: dict[str, str] = {}
    reusable: dict[str, dict[str, Any]] = {}
    for code in codes:
        row = baselines.get(code)
        if row is None:
            refresh_reasons[code] = "MISSING_PRIMARY_COVERAGE"
            continue
        coverage_id = str(row.get("coverage_id") or "")
        window_start = coerce_optional_date(row.get("window_start"))
        window_end = coerce_optional_date(row.get("window_end"))
        latest_report = coerce_optional_date(row.get("latest_report_date"))
        known_at = pd.to_datetime(row.get("known_at"), errors="coerce")
        if (
            not re.fullmatch(r"[0-9a-f]{64}", coverage_id)
            or str(row.get("coverage_status") or "") != "COMPLETE"
            or int(row.get("result_count") or 0) <= 0
            or window_start is None
            or window_start > date(1900, 1, 1)
            or window_end is None
            or window_end > as_of
            or pd.isna(known_at)
        ):
            refresh_reasons[code] = "INVALID_PRIMARY_COVERAGE"
            continue
        if (
            known_at.date() > window_end
            and not (
                str(row.get("guard_status") or "") == "PASS"
                and str(row.get("guard_as_of_date") or "")
                == window_end.isoformat()
            )
        ):
            refresh_reasons[code] = "UNGUARDED_LATE_CAPTURE"
            continue
        if (
            report_period_gate_applies(universe[code], disclosure_gate)
            and (
                latest_report is None
                or latest_report < disclosure_gate.minimum_report_date
            )
        ):
            refresh_reasons[code] = "STALE_REQUIRED_REPORT_PERIOD"
            continue
        if (as_of - window_end).days > FINANCE_DISCOVERY_MAX_DAYS:
            refresh_reasons[code] = "EXPIRED_INCREMENTAL_BASELINE"
            continue
        reusable[code] = {
            "coverage_id": coverage_id,
            "window_end": window_end,
        }

    gap_starts = [
        row["window_end"] + timedelta(days=1)
        for row in reusable.values()
        if row["window_end"] < as_of
    ]
    if not gap_starts:
        return {
            "mode": "EXACT_REUSE_AND_TARGETED_REFRESH",
            "fetch_codes": sorted(refresh_reasons),
            "reused_codes": sorted(reusable),
            "refresh_reasons": refresh_reasons,
            "discovery": {},
            "discovery_coverage_id": "",
            "fallback_reason": "",
        }

    discovery_start = min(gap_starts)
    try:
        discovery = fetch_finance_incremental_discovery(
            window_start=discovery_start,
            window_end=as_of,
            universe_codes=codes,
        )
        discovery_coverage_id = append_finance_incremental_discovery(
            engine,
            discovery,
        )
    except Exception as exc:
        return {
            "mode": "FULL_PRIMARY_FALLBACK",
            "fetch_codes": codes,
            "reused_codes": [],
            "refresh_reasons": {
                code: "DISCOVERY_UNAVAILABLE" for code in codes
            },
            "discovery": {},
            "discovery_coverage_id": "",
            "fallback_reason": f"{type(exc).__name__}:{exc}",
        }

    changed_by_code: dict[str, list[date]] = {}
    for event in discovery.get("events") or []:
        code = str(event.get("stock_code") or "").zfill(6)
        event_date = coerce_optional_date(event.get("query_date"))
        if code in universe and event_date is not None:
            changed_by_code.setdefault(code, []).append(event_date)
    for code, baseline in reusable.items():
        if any(
            baseline["window_end"] < changed_date <= as_of
            for changed_date in changed_by_code.get(code, ())
        ):
            refresh_reasons[code] = "SOURCE_NOTICE_OR_UPDATE_CHANGED"
    fetch_codes = sorted(refresh_reasons)
    return {
        "mode": "INCREMENTAL_DISCOVERY",
        "fetch_codes": fetch_codes,
        "reused_codes": sorted(set(codes) - set(fetch_codes)),
        "refresh_reasons": refresh_reasons,
        "discovery": discovery,
        "discovery_coverage_id": discovery_coverage_id,
        "fallback_reason": "",
    }


def _fetch_finance_with_cooldown(
    stock_code: str,
    *,
    sleep_seconds: float,
) -> pd.DataFrame:
    """Fetch one provider response and apply one worker-local cooldown."""

    try:
        return fetch_finance(stock_code)
    finally:
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def iter_finance_fetches(
    codes: list[str],
    *,
    workers: int,
    sleep_seconds: float,
):
    """Yield provider responses in input order with bounded fetch concurrency.

    Only the external provider call runs in worker threads.  The caller keeps
    response validation and every database transaction on its main thread.
    Keeping at most ``workers * 2`` futures in flight bounds memory while still
    overlapping provider latency with the previous response's database write.
    """

    if workers <= 1 or len(codes) <= 1:
        for index, code in enumerate(codes):
            try:
                yield code, fetch_finance(code), None
            except Exception as exc:
                yield code, None, exc
            if sleep_seconds > 0 and index < len(codes) - 1:
                time.sleep(sleep_seconds)
        return

    max_inflight = min(
        len(codes),
        max(workers, workers * FETCH_PREFETCH_MULTIPLIER),
    )
    code_iterator = iter(codes)
    pending: deque[tuple[str, Future[pd.DataFrame]]] = deque()
    with ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="finance-fetch",
    ) as executor:
        for _ in range(max_inflight):
            code = next(code_iterator, None)
            if code is None:
                break
            pending.append((
                code,
                executor.submit(
                    _fetch_finance_with_cooldown,
                    code,
                    sleep_seconds=sleep_seconds,
                ),
            ))

        while pending:
            code, future = pending.popleft()
            try:
                frame = future.result()
                error = None
            except Exception as exc:
                frame = None
                error = exc
            yield code, frame, error

            next_code = next(code_iterator, None)
            if next_code is not None:
                pending.append((
                    next_code,
                    executor.submit(
                        _fetch_finance_with_cooldown,
                        next_code,
                        sleep_seconds=sleep_seconds,
                    ),
                ))


def upsert_finance(
    engine,
    df: pd.DataFrame,
    *,
    coverage_end: date,
    stock_code: str | None = None,
    observed_at: datetime | None = None,
) -> int:
    """Append immutable PIT revisions before refreshing the legacy cache."""
    df = df if df is not None else pd.DataFrame()
    now_dt = (observed_at or _capture_now()).replace(microsecond=0)
    if coverage_end > now_dt.date():
        raise ValueError("finance coverage end cannot be later than capture")
    now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    batch_id = f"adata-finance-{now_dt.strftime('%Y%m%dT%H%M%S')}"
    cols = [
        "stock_code", "short_name", "report_date", "report_type", "notice_date",
        "basic_eps", "diluted_eps", "non_gaap_eps", "net_asset_ps",
        "cap_reserve_ps", "undist_profit_ps", "oper_cf_ps",
        "total_rev", "gross_profit", "net_profit_attr_sh", "non_gaap_net_profit",
        "total_rev_yoy_gr", "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr",
        "total_rev_qoq_gr", "net_profit_qoq_gr",
        "roe_wtd", "roe_non_gaap_wtd", "roa_wtd", "gross_margin", "net_margin",
        "curr_ratio", "quick_ratio", "cash_flow_ratio", "asset_liab_ratio",
    ]

    # 只保留存在的列
    available = [c for c in cols if c in df.columns]
    source_columns = [
        *available,
        *(
            ["source_update_date"]
            if "source_update_date" in df.columns
            and "source_update_date" not in available
            else []
        ),
    ]
    frame = df[source_columns].copy() if source_columns else pd.DataFrame()
    source_receipt = dict(df.attrs.get("source_receipt") or {})

    # 数值列转为 float（防止 pandas object 类型）
    for c in frame.columns:
        if c not in (
            "stock_code", "short_name", "report_date", "report_type",
            "notice_date", "source_update_date",
        ):
            frame[c] = pd.to_numeric(frame[c], errors="coerce")

    codes = sorted({
        str(value).strip().zfill(6)
        for value in (frame.get("stock_code", pd.Series(dtype=str)).tolist())
        if str(value).strip()
    })
    requested_code = str(stock_code or "").strip().zfill(6)
    if requested_code and requested_code != "000000":
        if codes and codes != [requested_code]:
            raise ValueError("finance response stock identity differs from request")
        codes = [requested_code]
    if not codes:
        raise ValueError("finance coverage requires the requested stock code")
    if len(codes) != 1:
        raise ValueError("finance coverage transaction must contain one stock")

    if frame.empty:
        raise ValueError("finance provider response must contain at least one row")
    missing_identity = {"stock_code", "report_date"} - set(frame.columns)
    if missing_identity:
        raise ValueError(
            "finance response is missing required identity columns: "
            + ", ".join(sorted(missing_identity))
        )
    if now_dt.date() > coverage_end and (
        source_receipt.get("stability_status") != "STABLE_DOUBLE_SWEEP"
        or "source_update_date" not in frame.columns
    ):
        raise ValueError(
            "DATA_BLOCKED: historical finance capture lacks stable UPDATE_DATE evidence"
        )
    source_max_dates: dict[str, str | None] = {}
    for field in ("notice_date", "source_update_date"):
        if field not in frame.columns:
            source_max_dates[field] = None
            continue
        parsed = pd.to_datetime(frame[field], errors="coerce").dt.date
        future = sorted({value for value in parsed if value > coverage_end})
        if future:
            raise ValueError(
                "DATA_BLOCKED: finance mutable provider contains post-target "
                f"{field}: target={coverage_end} first={future[0]}"
            )
        valid = [value for value in parsed if pd.notna(value)]
        source_max_dates[field] = max(valid).isoformat() if valid else None
    source_timestamp_guard = {
        "status": "PASS",
        "as_of_date": coverage_end.isoformat(),
        "captured_at": now_dt.isoformat(),
        "maximum_notice_date": source_max_dates["notice_date"],
        "maximum_update_date": source_max_dates["source_update_date"],
    }

    # 构造 INSERT SQL
    placeholders = ", ".join([f":{c}" for c in available])
    col_names = ", ".join(available)
    update_clause = ", ".join([f"{c} = VALUES({c})" for c in available if c not in ("stock_code", "report_date")])
    update_clause += ", etl_sync_at = VALUES(etl_sync_at)"

    sql = text(f"""
        INSERT INTO si_stock_finance ({col_names}, etl_sync_at)
        VALUES ({placeholders}, :etl_sync_at)
        ON DUPLICATE KEY UPDATE {update_clause}
    """)

    count = 0
    with engine.begin() as conn:
        source_rows: list[dict] = []
        fact_bindings: list[dict] = []
        for _, row in frame.iterrows():
            source_params = {
                c: (None if pd.isna(row[c]) else row[c])
                for c in source_columns
            }
            params = {c: source_params[c] for c in available}
            # The append-only fact is the strategy source of truth.  A missing
            # PIT schema or an invalid identity aborts the transaction before
            # the mutable display cache can advance on its own.
            receipt = append_finance_revision(
                conn,
                source_params,
                known_at=now_dt,
                received_at=now_dt,
                source=PRIMARY_FINANCE_SOURCE,
                batch_id=batch_id,
            )
            source_rows.append(dict(source_params))
            fact_bindings.append({
                "revision_id": receipt.revision_id,
                "content_hash": receipt.content_hash,
            })
            params["etl_sync_at"] = now
            conn.execute(sql, params)
            count += 1
        append_source_coverage(
            conn,
            fact_kind="finance",
            stock_code=codes[0],
            window_start="1900-01-01",
            window_end=coverage_end,
            known_at=now_dt,
            received_at=now_dt,
            covered_through_at=now_dt,
            watermark_kind="CAPTURED_AT",
            watermark_evidence={
                "provider": PRIMARY_FINANCE_SOURCE,
                "capture": "stable_eastmoney_result_set",
                "source_receipt": source_receipt,
                "source_timestamp_guard": source_timestamp_guard,
            },
            source_rows=source_rows,
            fact_bindings=fact_bindings,
            source=PRIMARY_FINANCE_SOURCE,
            batch_id=batch_id,
        )

    return count


def minimum_expected_report_date(as_of: date) -> date:
    """Backward-compatible accessor for the current disclosure-period floor."""

    return finance_disclosure_gate(as_of).minimum_report_date


def validate_finance_response(
    stock_code: str,
    frame: pd.DataFrame,
    *,
    as_of: date,
    minimum_report_date: date,
    listing_date: date | None = None,
    disclosure_deadline: date | None = None,
) -> date:
    """Validate non-empty provider identity and a reasonable latest period."""

    if frame is None or frame.empty:
        raise RuntimeError(
            f"DATA_BLOCKED: {stock_code} 财务源返回空结果，禁止记录完整覆盖"
        )
    required = {"stock_code", "report_date"}
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(
            f"DATA_BLOCKED: {stock_code} 财务响应缺少字段: "
            + ", ".join(sorted(missing))
        )
    requested = str(stock_code).strip().zfill(6)
    observed_codes = {
        str(value or "").strip().zfill(6)
        for value in frame["stock_code"].tolist()
    }
    if observed_codes != {requested}:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 财务响应代码集合不一致: "
            f"{sorted(observed_codes)}"
        )
    report_dates = pd.to_datetime(frame["report_date"], errors="coerce").dt.date
    if report_dates.isna().any():
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 财务响应含无效报告期"
        )
    invalid_periods = [
        value for value in report_dates
        if (value.month, value.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)}
    ]
    if invalid_periods:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 财务响应含非标准报告期: "
            f"{sorted(set(invalid_periods))[:5]}"
        )
    latest = max(report_dates)
    if latest > as_of:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 最新报告期 {latest} 晚于采集日 {as_of}"
        )
    for field in ("notice_date", "source_update_date"):
        if field not in frame.columns:
            continue
        source_dates = pd.to_datetime(frame[field], errors="coerce").dt.date
        post_target = sorted({
            value for value in source_dates
            if pd.notna(value) and value > as_of
        })
        if post_target:
            raise RuntimeError(
                f"DATA_BLOCKED: {requested} {field} 晚于目标日 {as_of}: "
                f"{post_target[0]}"
            )
    gate = FinanceDisclosureGate(
        minimum_report_date=minimum_report_date,
        disclosure_deadline=disclosure_deadline or minimum_report_date,
    )
    if report_period_gate_applies(listing_date, gate) and latest < minimum_report_date:
        raise FinanceStaleResponse(
            f"DATA_BLOCKED: {requested} 最新报告期过旧: latest={latest}, "
            f"minimum={minimum_report_date}"
        )
    return latest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="同步股票财务核心指标")
    parser.add_argument("--code", type=str, default=None, help="同步单只股票代码")
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="全市场同步起始偏移；用于可验证的互不重叠人工分片",
    )
    parser.add_argument("--limit", type=int, default=None, help="只同步前N只")
    parser.add_argument("--sleep", type=float, default=0.3, help="每只股票间隔秒数（防限流）")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "财务源并发抓取线程数；默认1保持兼容，正式并发建议4，"
            f"允许1..{MAX_FETCH_WORKERS}"
        ),
    )
    parser.add_argument(
        "--min-code-coverage",
        type=float,
        default=1.0,
        help="非空股票覆盖率；正式任务固定要求 1.0，不能降低",
    )
    parser.add_argument(
        "--min-report-date",
        default="",
        help="最新报告期下限 YYYY-MM-DD；默认按法定披露窗口推导",
    )
    parser.add_argument(
        "--as-of-date",
        default="",
        help=(
            "本次覆盖/封存的权威交易日 YYYY-MM-DD；调度补跑必须显式绑定，"
            "默认仅用于人工当日运行"
        ),
    )
    parser.add_argument(
        "--seal-existing",
        action="store_true",
        help=(
            "不请求外部源；严格复核当前全目录PIT coverage并追加原子批次完成水位"
        ),
    )
    args = parser.parse_args(argv)

    if args.min_code_coverage != 1.0:
        print(
            "[ERROR] DATA_BLOCKED: finance production code coverage is fixed at 1.0",
            file=sys.stderr,
        )
        return 2
    if not 1 <= args.workers <= MAX_FETCH_WORKERS:
        print(
            "[ERROR] DATA_BLOCKED: finance fetch workers must be between "
            f"1 and {MAX_FETCH_WORKERS}",
            file=sys.stderr,
        )
        return 2
    if args.offset < 0 or (args.code and args.offset != 0):
        print(
            "[ERROR] DATA_BLOCKED: finance offset must be non-negative and "
            "cannot be combined with --code",
            file=sys.stderr,
        )
        return 2
    try:
        run_as_of = (
            date.fromisoformat(args.as_of_date)
            if args.as_of_date
            else datetime.now().date()
        )
    except ValueError:
        print("[ERROR] --as-of-date 必须为 YYYY-MM-DD", file=sys.stderr)
        return 2
    if run_as_of > datetime.now().date():
        print("[ERROR] --as-of-date 不能晚于当前日期", file=sys.stderr)
        return 2
    try:
        disclosure_gate = finance_disclosure_gate(run_as_of)
        if args.min_report_date:
            explicit_minimum = datetime.strptime(
                args.min_report_date, "%Y-%m-%d"
            ).date()
            disclosure_gate = FinanceDisclosureGate(
                minimum_report_date=explicit_minimum,
                disclosure_deadline=run_as_of,
            )
        min_report_date = disclosure_gate.minimum_report_date
    except ValueError:
        print("[ERROR] --min-report-date 必须为 YYYY-MM-DD", file=sys.stderr)
        return 2
    if min_report_date > run_as_of:
        print("[ERROR] --min-report-date 不能晚于当前日期", file=sys.stderr)
        return 2

    engine = get_engine()
    try:
        if args.seal_existing:
            if args.code or args.offset or args.limit:
                print(
                    "[ERROR] DATA_BLOCKED: --seal-existing requires the full catalog",
                    file=sys.stderr,
                )
                return 2
            try:
                seal = append_finance_atomic_batch_seal(
                    engine,
                    as_of_date=run_as_of,
                    completed_known_at=datetime.now().replace(microsecond=0),
                )
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "schema": "probiga.finance-atomic-batch-result.v1",
                            "status": "DATA_BLOCKED",
                            "as_of": run_as_of.isoformat(),
                            "reason": f"{type(exc).__name__}:{exc}",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
                return 1
            print(
                json.dumps(
                    {
                        **seal,
                        "seal_schema": seal.get("schema"),
                        "schema": "probiga.finance-atomic-batch-result.v1",
                        "status": "PASS",
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        universe = get_finance_stock_universe(engine)
        if args.code:
            requested_code = args.code.strip().zfill(6)
            if requested_code not in universe:
                print(
                    "[ERROR] DATA_BLOCKED: requested finance stock is absent "
                    f"from si_all_code: {requested_code}"
                )
                return 2
            codes = [requested_code]
        else:
            codes = list(universe)
            if args.offset:
                codes = codes[args.offset:]
            if args.limit and args.limit > 0:
                codes = codes[: args.limit]

        codes = list(dict.fromkeys(str(code).strip().zfill(6) for code in codes))
        if not codes:
            print("[ERROR] DATA_BLOCKED: finance stock universe is empty")
            return 2

        full_catalog_run = bool(
            not args.code and args.offset == 0 and not args.limit
        )
        incremental_plan: dict[str, Any] = {
            "mode": "EXPLICIT_SCOPE_FULL_PRIMARY",
            "fetch_codes": codes,
            "reused_codes": [],
            "refresh_reasons": {},
            "discovery": {},
            "discovery_coverage_id": "",
            "fallback_reason": "",
        }
        if full_catalog_run:
            try:
                incremental_plan = build_finance_incremental_plan(
                    engine,
                    universe=universe,
                    as_of=run_as_of,
                    disclosure_gate=disclosure_gate,
                )
            except Exception as exc:
                incremental_plan = {
                    **incremental_plan,
                    "mode": "FULL_PRIMARY_FALLBACK",
                    "fallback_reason": f"{type(exc).__name__}:{exc}",
                }
        fetch_codes = list(incremental_plan["fetch_codes"])
        reused_codes = list(incremental_plan["reused_codes"])
        print(
            f"[INFO] 财务范围 {len(codes)} 只；模式 {incremental_plan['mode']}；"
            f"本次抓取 {len(fetch_codes)} 只；严格复用 {len(reused_codes)} 只"
        )
        if incremental_plan.get("fallback_reason"):
            print(
                "[WARN] 增量发现证据不可用，回退全主源扫描: "
                f"{incremental_plan['fallback_reason']}"
            )

        total_rows = 0
        failures: list[dict[str, str]] = []
        completed_codes: list[str] = []
        latest_periods: dict[str, str] = {}
        applicable_latest_periods: dict[str, str] = {}
        exempt_new_listing_codes: list[str] = []
        legal_empty_new_listing_codes: list[str] = []
        expected_unavailable_codes: list[str] = []
        expected_unavailable_details: dict[str, dict[str, str]] = {}
        fetches = iter_finance_fetches(
            fetch_codes,
            workers=args.workers,
            sleep_seconds=args.sleep,
        )
        for i, (code, df, fetch_error) in enumerate(fetches):
            try:
                if fetch_error is not None:
                    raise fetch_error
                if df is None:
                    raise RuntimeError(
                        f"DATA_BLOCKED: {code} 财务源未返回响应"
                    )
                if df.empty:
                    listing_date = universe[code]
                    if (
                        listing_date is None
                        or listing_date <= disclosure_gate.disclosure_deadline
                    ):
                        raise RuntimeError(
                            f"DATA_BLOCKED: {code} 财务源返回空结果"
                        )
                    append_new_listing_finance_empty_coverage(
                        engine,
                        stock_code=code,
                        listing_date=listing_date,
                        as_of=run_as_of,
                        disclosure_deadline=(
                            disclosure_gate.disclosure_deadline
                        ),
                        source_receipt=dict(df.attrs.get("source_receipt") or {}),
                    )
                    legal_empty_new_listing_codes.append(code)
                    exempt_new_listing_codes.append(code)
                    continue
                try:
                    latest = validate_finance_response(
                        code,
                        df,
                        as_of=run_as_of,
                        minimum_report_date=min_report_date,
                        listing_date=universe[code],
                        disclosure_deadline=disclosure_gate.disclosure_deadline,
                    )
                except FinanceStaleResponse:
                    if code not in CNINFO_NONFILING_ISSUERS:
                        raise
                    if min_report_date > CNINFO_NONFILING_PROVEN_THROUGH.get(
                        code, date.min
                    ):
                        raise RuntimeError(
                            "DATA_BLOCKED: reviewed CNInfo non-filing proof does not "
                            f"cover required period {min_report_date}"
                        )
                    evidence = fetch_cninfo_nonfiling_evidence(
                        code,
                        as_of=run_as_of,
                        expected_report_date=min_report_date,
                    )
                    observed_at = _capture_now()
                    with engine.begin() as connection:
                        receipt = append_finance_expected_unavailable(
                            connection,
                            stock_code=code,
                            expected_report_date=min_report_date,
                            known_at=observed_at,
                            received_at=observed_at,
                            official_evidence=evidence,
                            batch_id=(
                                "cninfo-finance-nonfiling-"
                                + observed_at.strftime("%Y%m%dT%H%M%S")
                            ),
                        )
                    expected_unavailable_codes.append(code)
                    expected_unavailable_details[code] = {
                        "reason_code": str(evidence["reason_code"]),
                        "announcement_id": str(evidence["announcement_id"]),
                        "valid_until": str(evidence["valid_until"]),
                        "next_retry_date": str(evidence["next_retry_date"]),
                        "disposition_id": receipt.coverage_id,
                    }
                else:
                    rows = upsert_finance(
                        engine,
                        df,
                        stock_code=code,
                        coverage_end=run_as_of,
                    )
                    if rows <= 0:
                        raise RuntimeError(
                            f"DATA_BLOCKED: {code} 财务源未提交任何报告期"
                        )
                    total_rows += rows
                    completed_codes.append(code)
                    latest_periods[code] = latest.isoformat()
                    if report_period_gate_applies(universe[code], disclosure_gate):
                        applicable_latest_periods[code] = latest.isoformat()
                    else:
                        exempt_new_listing_codes.append(code)
            except Exception as exc:
                print(f"  [WARN] {code} 获取/写入失败: {exc}")
                failures.append({"stock_code": code, "error": str(exc)})

            if (i + 1) % 50 == 0:
                print(
                    f"[PROGRESS] {i + 1}/{len(fetch_codes)}, 已写入 {total_rows} 条, "
                    f"失败 {len(failures)}"
                )

        nonempty_codes = set(completed_codes) | set(reused_codes)
        coverage = len(nonempty_codes) / len(codes)
        resolved_codes = (
            nonempty_codes
            | set(expected_unavailable_codes)
            | set(legal_empty_new_listing_codes)
        )
        resolved_count = len(resolved_codes)
        resolution_coverage = resolved_count / len(codes)
        atomic_batch: dict[str, Any] = {}
        if (
            full_catalog_run
            and not failures
            and resolution_coverage == 1.0
        ):
            try:
                atomic_batch = append_finance_atomic_batch_seal(
                    engine,
                    as_of_date=run_as_of,
                    completed_known_at=_capture_now(),
                    incremental_discovery_coverage_id=str(
                        incremental_plan.get("discovery_coverage_id") or ""
                    ),
                )
            except Exception as exc:
                failures.append({
                    "stock_code": "ATOMIC_BATCH_SEAL",
                    "error": f"{type(exc).__name__}:{exc}",
                })
        report = {
            "schema": "probiga.finance-sync-result.v1",
            "status": (
                "PASS"
                if not failures and resolution_coverage == 1.0
                else "DATA_BLOCKED"
            ),
            "as_of": run_as_of.isoformat(),
            "minimum_report_date": min_report_date.isoformat(),
            "minimum_report_disclosure_deadline": (
                disclosure_gate.disclosure_deadline.isoformat()
            ),
            "requested_code_count": len(codes),
            "execution_mode": incremental_plan["mode"],
            "provider_fetch_code_count": len(fetch_codes),
            "reused_immutable_code_count": len(reused_codes),
            "incremental_discovery_coverage_id": str(
                incremental_plan.get("discovery_coverage_id") or ""
            ),
            "incremental_changed_code_count": len(
                (incremental_plan.get("discovery") or {}).get(
                    "changed_codes"
                )
                or []
            ),
            "incremental_fallback_reason": str(
                incremental_plan.get("fallback_reason") or ""
            ),
            "nonempty_code_count": len(nonempty_codes),
            "nonempty_code_coverage": coverage,
            "expected_unavailable_code_count": len(expected_unavailable_codes),
            "expected_unavailable_code_sample": expected_unavailable_details,
            "legal_empty_new_listing_code_count": len(
                legal_empty_new_listing_codes
            ),
            "legal_empty_new_listing_code_sample": (
                legal_empty_new_listing_codes[:20]
            ),
            "resolved_code_count": resolved_count,
            "resolution_coverage": resolution_coverage,
            "written_report_count": total_rows,
            "failure_count": len(failures),
            "failure_sample": failures[:20],
            "atomic_batch": atomic_batch,
            "report_period_applicable_code_count": len(applicable_latest_periods),
            "new_listing_period_exempt_code_count": len(exempt_new_listing_codes),
            "oldest_latest_report_date": (
                min(latest_periods.values()) if latest_periods else None
            ),
            "oldest_latest_applicable_report_date": (
                min(applicable_latest_periods.values())
                if applicable_latest_periods else None
            ),
        }
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        if failures or resolution_coverage != 1.0:
            print(
                f"[FAILED] DATA_BLOCKED: 财务同步未完整: {len(codes)} 只股票, "
                f"非空覆盖 {len(completed_codes)}/{len(codes)} ({coverage:.2%}), "
                f"依法暂不可得 {len(expected_unavailable_codes)}, "
                f"写入 {total_rows} 条报告期, 失败 {len(failures)}"
            )
            return 1
        print(
            f"[OK] 同步完成: {len(codes)} 只股票, "
            f"已解析 {resolved_count}/{len(codes)}, "
            f"依法暂不可得 {len(expected_unavailable_codes)}, "
            f"写入 {total_rows} 条报告期, 失败 0"
        )
        return 0
    finally:
        dispose = getattr(engine, "dispose", None)
        if callable(dispose):
            dispose()


if __name__ == "__main__":
    raise SystemExit(main())

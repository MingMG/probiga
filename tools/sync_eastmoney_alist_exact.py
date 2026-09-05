#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Publish one exact Eastmoney dragon-tiger-list partition.

The legacy sentiment wrapper silently kept partial dates and capped seat detail
at 80 stocks.  This publisher instead consumes every page from Eastmoney's
date-scoped reports, proves the full daily-code/detail relationship, and only
then replaces the requested date in one transaction.  A provider-declared
empty result is publishable only when the exact Eastmoney ``9201`` empty
response is observed; transport errors and structurally empty success payloads
fail closed.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.mysql_lock import mysql_named_lock  # noqa: E402
from server.common.qmt_stock_catalog import (  # noqa: E402
    load_target_stock_catalog,
    validate_stock_catalog_runtime_schema,
)
from tools.env_config import create_tool_engine, load_project_env  # noqa: E402


RESULT_SCHEMA = "probiga.eastmoney-alist-result.v1"
PROVIDER_ID = "eastmoney_datacenter"
PROVIDER_ENDPOINT = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EXECUTOR_OWNER = "linux_provider"
SHANGHAI = ZoneInfo("Asia/Shanghai")
PAGE_SIZE = 500
EMPTY_CODE = 9201
EMPTY_MESSAGE = "返回数据为空"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
CODE_RE = re.compile(r"^[0-9]{6}$")
QMT_CODE_RE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
A_SHARE_CODE_RE = re.compile(r"^(?:(?:00|30|60|68|92)[0-9]{4}|[48][0-9]{5})$")

DAILY_REPORT = "RPT_DAILYBILLBOARD_DETAILSNEW"
DETAIL_REPORTS = (
    "RPT_BILLBOARD_DAILYDETAILSBUY",
    "RPT_BILLBOARD_DAILYDETAILSSELL",
)
TASK_TYPES = {"daily": "alist_daily", "info": "alist_info"}
TABLES = {"daily": "st_a_list_daily", "info": "st_a_list_info"}
LOCKS = {
    "daily": "probiga:dragon_tiger_daily",
    "info": "probiga:dragon_tiger_info",
}

DAILY_BUSINESS_COLUMNS = (
    "trade_date",
    "short_name",
    "stock_code",
    "close",
    "change_cpt",
    "turnover_ratio",
    "a_net_amount",
    "a_buy_amount",
    "a_sell_amount",
    "a_amount",
    "amount",
    "net_amount_rate",
    "a_amount_rate",
    "reason",
)
DAILY_NUMERIC_COLUMNS = DAILY_BUSINESS_COLUMNS[3:13]
DAILY_INSERT_COLUMNS = DAILY_BUSINESS_COLUMNS + (
    "etl_sync_at",
    "qmt_code",
    "data_source",
    "source_time",
    "received_at",
    "batch_id",
    "data_version",
    "quality_status",
    "permission_status",
)
INFO_BUSINESS_COLUMNS = (
    "trade_date",
    "stock_code",
    "operate_code",
    "operate_name",
    "a_net_amount",
    "a_buy_amount",
    "a_sell_amount",
    "a_buy_amount_rate",
    "a_sell_amount_rate",
    "reason",
)
INFO_NUMERIC_COLUMNS = INFO_BUSINESS_COLUMNS[4:9]
INFO_INSERT_COLUMNS = INFO_BUSINESS_COLUMNS + ("etl_sync_at",)

DAILY_RENAME = {
    "TRADE_DATE": "trade_date",
    "SECURITY_NAME_ABBR": "short_name",
    "SECURITY_CODE": "stock_code",
    "CLOSE_PRICE": "close",
    "CHANGE_RATE": "change_cpt",
    "TURNOVERRATE": "turnover_ratio",
    "BILLBOARD_NET_AMT": "a_net_amount",
    "BILLBOARD_BUY_AMT": "a_buy_amount",
    "BILLBOARD_SELL_AMT": "a_sell_amount",
    "BILLBOARD_DEAL_AMT": "a_amount",
    "ACCUM_AMOUNT": "amount",
    "DEAL_NET_RATIO": "net_amount_rate",
    "DEAL_AMOUNT_RATIO": "a_amount_rate",
    "EXPLANATION": "reason",
}
INFO_RENAME = {
    "TRADE_DATE": "trade_date",
    "SECURITY_CODE": "stock_code",
    "OPERATEDEPT_CODE": "operate_code",
    "OPERATEDEPT_NAME": "operate_name",
    "NET": "a_net_amount",
    "BUY": "a_buy_amount",
    "SELL": "a_sell_amount",
    "TOTAL_BUYRIO": "a_buy_amount_rate",
    "TOTAL_SELLRIO": "a_sell_amount_rate",
    "EXPLANATION": "reason",
}


class AListDataBlocked(RuntimeError):
    """The requested date cannot be proven complete."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _signed(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["receipt_id"] = _digest(result)
    return result


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=30,
    )
    return completed.stdout.strip().lower()


def resolve_build_sha(explicit: str = "") -> str:
    value = str(
        explicit
        or os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or _git_head()
    ).strip().lower()
    if SHA40.fullmatch(value) is None or value == "0" * 40:
        raise AListDataBlocked("DATA_BLOCKED: exact alist build SHA unavailable")
    environment_value = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or ""
    ).strip().lower()
    if environment_value and environment_value != value:
        raise AListDataBlocked("DATA_BLOCKED: alist scheduler build SHA differs")
    if os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production":
        # The deployer already binds this root-owned release to the service SHA.
        # Match the existing turnover publisher's artifact identity contract;
        # runtime Git ownership checks are neither needed nor portable here.
        code_root = os.environ.get("PROBIGA_CODE_ROOT", "").replace("\\", "/").rstrip("/")
        actual_root = str(ROOT).replace("\\", "/").rstrip("/")
        if not environment_value or code_root != actual_root or code_root != f"/opt/ProBigA-releases/{value}":
            raise AListDataBlocked("DATA_BLOCKED: alist production release identity differs")
        return value
    if _git_head() != value:
        raise AListDataBlocked("DATA_BLOCKED: alist checkout differs from scheduler build")
    return value


def _iso_date(value: Any, *, field: str = "trade_date") -> str:
    raw = str(value or "").strip()[:10]
    try:
        normalized = date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise AListDataBlocked(f"DATA_BLOCKED: {field} is invalid") from exc
    if raw != normalized:
        raise AListDataBlocked(f"DATA_BLOCKED: {field} is invalid")
    return normalized


def _code(value: Any) -> str:
    normalized = str(value or "").strip().zfill(6)
    if CODE_RE.fullmatch(normalized) is None or normalized == "000000":
        raise AListDataBlocked(f"DATA_BLOCKED: invalid alist stock code {value!r}")
    return normalized


def _is_a_share_code(value: Any) -> bool:
    return A_SHARE_CODE_RE.fullmatch(_code(value)) is not None


def _text(value: Any, *, field: str, maximum: int, allow_empty: bool = False) -> str:
    normalized = str(value or "").strip()
    if (not normalized and not allow_empty) or len(normalized) > maximum:
        raise AListDataBlocked(f"DATA_BLOCKED: invalid alist {field}")
    return normalized


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise AListDataBlocked(f"DATA_BLOCKED: alist {field} is not numeric") from exc
    if not number.is_finite():
        raise AListDataBlocked(f"DATA_BLOCKED: alist {field} is not finite")
    return number.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _canonical_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value.quantize(Decimal("0.000001")), ".6f")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise AListDataBlocked("DATA_BLOCKED: non-finite alist database value")
        return format(_decimal(value, field="database value"), ".6f")
    return str(value)


def code_set_hash(codes: Iterable[Any]) -> str:
    normalized = sorted({_code(code) for code in codes})
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


@dataclass(frozen=True)
class ReportEvidence:
    report: str
    trade_date: str
    rows: tuple[dict[str, Any], ...]
    declared_count: int
    declared_pages: int
    fetched_pages: int
    authoritative_empty: bool
    response_hash: str

    def receipt(self) -> dict[str, Any]:
        return {
            "report": self.report,
            "trade_date": self.trade_date,
            "declared_count": self.declared_count,
            "declared_pages": self.declared_pages,
            "fetched_pages": self.fetched_pages,
            "fetched_row_count": len(self.rows),
            "authoritative_empty": self.authoritative_empty,
            "response_hash": self.response_hash,
        }


class EastmoneyAListProvider:
    """Strict date-scoped Eastmoney datacenter client."""

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout: float = 20.0,
        attempts: int = 4,
        retry_delay: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._owned_session = session is None
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 ProBigA-AList-Exact/1.0",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://data.eastmoney.com/stock/tradedetail.html",
            }
        )
        self.timeout = max(1.0, float(timeout))
        self.attempts = max(1, int(attempts))
        self.retry_delay = max(0.0, float(retry_delay))
        self.sleep = sleep

    def close(self) -> None:
        if self._owned_session:
            self.session.close()

    def _request(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        last_error: BaseException | None = None
        for attempt in range(1, self.attempts + 1):
            try:
                response = self.session.get(
                    PROVIDER_ENDPOINT,
                    params=dict(params),
                    timeout=self.timeout,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, Mapping):
                    raise ValueError("Eastmoney response is not an object")
                return payload
            except (requests.RequestException, TypeError, ValueError) as exc:
                last_error = exc
                if attempt < self.attempts:
                    self.sleep(self.retry_delay * (2 ** (attempt - 1)))
        raise AListDataBlocked(
            f"DATA_BLOCKED: Eastmoney request failed after {self.attempts} attempts: "
            f"{type(last_error).__name__}"
        ) from last_error

    @staticmethod
    def _sort(report: str) -> tuple[str, str]:
        if report == DAILY_REPORT:
            return "SECURITY_CODE,TRADE_DATE,TRADE_ID", "1,-1,1"
        if report in DETAIL_REPORTS:
            return (
                "SECURITY_CODE,TRADE_ID,OPERATEDEPT_CODE,OPERATEDEPT_NAME",
                "1,1,1,1",
            )
        raise ValueError(f"unsupported alist report {report!r}")

    def fetch_report(self, report: str, trade_date: str) -> ReportEvidence:
        target = _iso_date(trade_date)
        sort_columns, sort_types = self._sort(report)
        common = {
            "reportName": report,
            "columns": "ALL",
            "filter": f"(TRADE_DATE='{target}')",
            "pageSize": PAGE_SIZE,
            "sortColumns": sort_columns,
            "sortTypes": sort_types,
            "source": "WEB",
            "client": "WEB",
        }
        first = self._request({**common, "pageNumber": 1})
        success = first.get("success") is True
        try:
            code = int(first.get("code"))
        except (TypeError, ValueError):
            code = -1
        if (
            not success
            and code == EMPTY_CODE
            and str(first.get("message") or "").strip() == EMPTY_MESSAGE
            and first.get("result") is None
        ):
            evidence = {
                "report": report,
                "trade_date": target,
                "empty_code": code,
                "empty_message": EMPTY_MESSAGE,
            }
            return ReportEvidence(
                report=report,
                trade_date=target,
                rows=(),
                declared_count=0,
                declared_pages=0,
                fetched_pages=1,
                authoritative_empty=True,
                response_hash=_digest(evidence),
            )
        if not success or code != 0 or first.get("result") is None:
            raise AListDataBlocked(
                f"DATA_BLOCKED: Eastmoney {report} did not return a complete result "
                f"(success={success},code={code})"
            )

        result = first["result"]
        if not isinstance(result, Mapping):
            raise AListDataBlocked("DATA_BLOCKED: Eastmoney result is malformed")
        try:
            declared_count = int(result.get("count"))
            declared_pages = int(result.get("pages"))
        except (TypeError, ValueError) as exc:
            raise AListDataBlocked(
                "DATA_BLOCKED: Eastmoney pagination counters are malformed"
            ) from exc
        if declared_count <= 0 or declared_pages <= 0:
            raise AListDataBlocked(
                "DATA_BLOCKED: Eastmoney success payload cannot prove an empty report"
            )
        if declared_pages != math.ceil(declared_count / PAGE_SIZE):
            raise AListDataBlocked("DATA_BLOCKED: Eastmoney page/count evidence differs")

        pages: list[Mapping[str, Any]] = [first]
        for page in range(2, declared_pages + 1):
            pages.append(self._request({**common, "pageNumber": page}))
        raw_rows: list[dict[str, Any]] = []
        page_hashes: list[str] = []
        for page_number, payload in enumerate(pages, 1):
            try:
                page_code = int(payload.get("code"))
            except (TypeError, ValueError):
                page_code = -1
            page_result = payload.get("result")
            if payload.get("success") is not True or page_code != 0 or not isinstance(page_result, Mapping):
                raise AListDataBlocked(
                    f"DATA_BLOCKED: Eastmoney {report} page {page_number} is incomplete"
                )
            try:
                page_count = int(page_result.get("count"))
                page_total = int(page_result.get("pages"))
            except (TypeError, ValueError) as exc:
                raise AListDataBlocked(
                    "DATA_BLOCKED: Eastmoney pagination changed during collection"
                ) from exc
            if page_count != declared_count or page_total != declared_pages:
                raise AListDataBlocked(
                    "DATA_BLOCKED: Eastmoney report changed during pagination"
                )
            data = page_result.get("data")
            if not isinstance(data, list) or not data:
                raise AListDataBlocked(
                    f"DATA_BLOCKED: Eastmoney {report} page {page_number} has no rows"
                )
            clean = [dict(row) for row in data if isinstance(row, Mapping)]
            if len(clean) != len(data):
                raise AListDataBlocked("DATA_BLOCKED: Eastmoney report contains malformed rows")
            raw_rows.extend(clean)
            page_hashes.append(_digest(clean))
        if len(raw_rows) != declared_count:
            raise AListDataBlocked(
                f"DATA_BLOCKED: Eastmoney report coverage differs: "
                f"fetched={len(raw_rows)} declared={declared_count}"
            )
        response_proof = {
            "report": report,
            "trade_date": target,
            "declared_count": declared_count,
            "declared_pages": declared_pages,
            "page_hashes": page_hashes,
        }
        return ReportEvidence(
            report=report,
            trade_date=target,
            rows=tuple(raw_rows),
            declared_count=declared_count,
            declared_pages=declared_pages,
            fetched_pages=len(pages),
            authoritative_empty=False,
            response_hash=_digest(response_proof),
        )


def _normalize_report_row(
    raw: Mapping[str, Any],
    *,
    rename: Mapping[str, str],
    business_columns: Sequence[str],
    numeric_columns: Sequence[str],
    target_date: str,
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for source, target in rename.items():
        if source not in raw:
            raise AListDataBlocked(f"DATA_BLOCKED: Eastmoney row omitted {source}")
        row[target] = raw[source]
    row["trade_date"] = _iso_date(row["trade_date"])
    if row["trade_date"] != target_date:
        raise AListDataBlocked(
            f"DATA_BLOCKED: Eastmoney returned wrong alist date {row['trade_date']}"
        )
    row["stock_code"] = _code(row["stock_code"])
    for column in numeric_columns:
        value = row[column]
        if (
            tuple(business_columns) == INFO_BUSINESS_COLUMNS
            and column in {
                "a_buy_amount",
                "a_sell_amount",
                "a_buy_amount_rate",
                "a_sell_amount_rate",
            }
            and value in (None, "")
        ):
            row[column] = None
        else:
            row[column] = _decimal(value, field=column)
    if "short_name" in row:
        row["short_name"] = _text(
            row["short_name"], field="short_name", maximum=128
        ).replace(" ", "")
    if "operate_code" in row:
        row["operate_code"] = _text(
            row["operate_code"], field="operate_code", maximum=64, allow_empty=True
        )
    if "operate_name" in row:
        row["operate_name"] = _text(
            row["operate_name"], field="operate_name", maximum=512
        )
    row["reason"] = _text(row["reason"], field="reason", maximum=512)
    if set(row) != set(business_columns):
        raise AListDataBlocked("DATA_BLOCKED: normalized alist columns differ")
    return row


def canonical_business_rows(
    rows: Iterable[Mapping[str, Any]], *, dataset: str
) -> list[dict[str, Any]]:
    columns = DAILY_BUSINESS_COLUMNS if dataset == "daily" else INFO_BUSINESS_COLUMNS
    numeric_columns = (
        set(DAILY_NUMERIC_COLUMNS) if dataset == "daily" else set(INFO_NUMERIC_COLUMNS)
    )
    canonical = [
        {
            column: (
                None
                if row.get(column) is None
                else format(_decimal(row.get(column), field=column), ".6f")
            )
            if column in numeric_columns
            else _canonical_value(row.get(column))
            for column in columns
        }
        for row in rows
    ]
    canonical.sort(key=lambda row: tuple("<NULL>" if row[c] is None else str(row[c]) for c in columns))
    return canonical


def canonical_storage_rows(
    rows: Iterable[Mapping[str, Any]], *, dataset: str
) -> list[dict[str, Any]]:
    columns = DAILY_INSERT_COLUMNS if dataset == "daily" else INFO_INSERT_COLUMNS
    numeric_columns = (
        set(DAILY_NUMERIC_COLUMNS) if dataset == "daily" else set(INFO_NUMERIC_COLUMNS)
    )
    canonical = [
        {
            column: (
                None
                if row.get(column) is None
                else format(_decimal(row.get(column), field=column), ".6f")
            )
            if column in numeric_columns
            else _canonical_value(row.get(column))
            for column in columns
        }
        for row in rows
    ]
    canonical.sort(
        key=lambda row: tuple(
            "<NULL>" if row[column] is None else str(row[column])
            for column in columns
        )
    )
    return canonical


def _metadata_datetime(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip().replace("T", " ")[:19]
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise AListDataBlocked(
                f"DATA_BLOCKED: alist {field} metadata is invalid"
            ) from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(SHANGHAI).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def validate_storage_metadata(
    rows: Iterable[Mapping[str, Any]], *, dataset: str
) -> None:
    for row in rows:
        synced = _metadata_datetime(row.get("etl_sync_at"), field="etl_sync_at")
        if dataset != "daily":
            continue
        code = _code(row.get("stock_code"))
        qmt_code = str(row.get("qmt_code") or "").strip().upper()
        source_time = _metadata_datetime(row.get("source_time"), field="source_time")
        received = _metadata_datetime(row.get("received_at"), field="received_at")
        if (
            QMT_CODE_RE.fullmatch(qmt_code) is None
            or qmt_code[:6] != code
            or str(row.get("data_source") or "") != PROVIDER_ID
            or source_time.date().isoformat() != _iso_date(row.get("trade_date"))
            or source_time.time() != wall_time(15, 0)
            or synced != received
            or received < source_time
            or re.fullmatch(r"[0-9a-f]{64}", str(row.get("batch_id") or ""))
            is None
            or SHA40.fullmatch(str(row.get("data_version") or "")) is None
            or str(row.get("data_version")) == "0" * 40
            or str(row.get("quality_status") or "") != "PROVIDER_COMPLETE"
            or str(row.get("permission_status") or "") != "PUBLIC"
        ):
            raise AListDataBlocked(
                "DATA_BLOCKED: alist daily storage metadata is incomplete"
            )


def partition_proof(rows: Iterable[Mapping[str, Any]], *, dataset: str) -> dict[str, Any]:
    canonical = canonical_business_rows(rows, dataset=dataset)
    codes = [_code(row["stock_code"]) for row in canonical]
    return {
        "row_count": len(canonical),
        "row_hash": _digest(canonical),
        "code_count": len(set(codes)),
        "code_set_hash": code_set_hash(codes),
        "authoritative_empty": len(canonical) == 0,
    }


def database_proof(
    rows: Iterable[Mapping[str, Any]], *, dataset: str
) -> dict[str, Any]:
    materialized = list(rows)
    validate_storage_metadata(materialized, dataset=dataset)
    business = partition_proof(materialized, dataset=dataset)
    return {
        **business,
        "storage_row_hash": _digest(
            canonical_storage_rows(materialized, dataset=dataset)
        ),
        "metadata_complete": True,
    }


def normalize_daily(
    evidence: ReportEvidence,
    *,
    observed_at: datetime,
    build_sha: str,
    allowed_codes: Iterable[Any] | None = None,
    qmt_by_stock: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if evidence.report != DAILY_REPORT:
        raise ValueError("daily normalization requires the daily report")
    if evidence.authoritative_empty:
        return []
    allowed = (
        {_code(code) for code in allowed_codes}
        if allowed_codes is not None
        else None
    )
    qmt_mapping: dict[str, str] | None = None
    if qmt_by_stock is not None:
        qmt_mapping = {}
        for raw_code, raw_qmt_code in qmt_by_stock.items():
            code = _code(raw_code)
            qmt_code = str(raw_qmt_code or "").strip().upper()
            if QMT_CODE_RE.fullmatch(qmt_code) is None or qmt_code[:6] != code:
                raise AListDataBlocked(
                    "DATA_BLOCKED: invalid alist catalog QMT instrument identity"
                )
            qmt_mapping[code] = qmt_code
        if allowed is not None and set(qmt_mapping) != allowed:
            raise AListDataBlocked(
                "DATA_BLOCKED: alist catalog code/QMT identity sets differ"
            )
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for raw in evidence.rows:
        raw_code = _code(raw.get("SECURITY_CODE"))
        # The Eastmoney report also contains convertible bonds.  They are
        # fetched (and therefore included in page/count evidence) but never
        # written into the A-share table.
        if not _is_a_share_code(raw_code):
            continue
        if allowed is not None and raw_code not in allowed:
            raise AListDataBlocked(
                "DATA_BLOCKED: Eastmoney returned an A-share absent from the "
                f"immutable target-date catalog: {raw_code}"
            )
        if qmt_mapping is not None and raw_code not in qmt_mapping:
            raise AListDataBlocked(
                "DATA_BLOCKED: Eastmoney A-share lacks a catalog QMT identity: "
                f"{raw_code}"
            )
        row = _normalize_report_row(
            raw,
            rename=DAILY_RENAME,
            business_columns=DAILY_BUSINESS_COLUMNS,
            numeric_columns=DAILY_NUMERIC_COLUMNS,
            target_date=evidence.trade_date,
        )
        identity = (row["trade_date"], row["stock_code"], row["reason"])
        if identity in seen:
            raise AListDataBlocked(
                "DATA_BLOCKED: Eastmoney daily report contains duplicate business identities"
            )
        seen.add(identity)
        row.update(
            {
                "etl_sync_at": observed_at,
                "qmt_code": qmt_mapping.get(raw_code) if qmt_mapping is not None else None,
                "data_source": PROVIDER_ID,
                "source_time": datetime.combine(
                    date.fromisoformat(evidence.trade_date), wall_time(15, 0)
                ),
                "received_at": observed_at,
                "batch_id": evidence.response_hash,
                "data_version": build_sha,
                "quality_status": "PROVIDER_COMPLETE",
                "permission_status": "PUBLIC",
            }
        )
        rows.append(row)
    return rows


def normalize_info(
    reports: Sequence[ReportEvidence],
    *,
    daily_codes: Iterable[Any],
    observed_at: datetime,
    allowed_codes: Iterable[Any] | None = None,
) -> list[dict[str, Any]]:
    if tuple(report.report for report in reports) != DETAIL_REPORTS:
        raise ValueError("info normalization requires the BUY and SELL reports")
    if len({report.trade_date for report in reports}) != 1:
        raise AListDataBlocked("DATA_BLOCKED: detail report dates differ")
    target_date = reports[0].trade_date
    expected_codes = {_code(code) for code in daily_codes}
    allowed = (
        {_code(code) for code in allowed_codes}
        if allowed_codes is not None
        else None
    )
    normalized_by_identity: dict[str, dict[str, Any]] = {}
    for report in reports:
        report_seen: set[str] = set()
        for raw in report.rows:
            raw_code = _code(raw.get("SECURITY_CODE"))
            if not _is_a_share_code(raw_code):
                continue
            if allowed is not None and raw_code not in allowed:
                raise AListDataBlocked(
                    "DATA_BLOCKED: Eastmoney detail contains an A-share absent "
                    f"from the immutable target-date catalog: {raw_code}"
                )
            row = _normalize_report_row(
                raw,
                rename=INFO_RENAME,
                business_columns=INFO_BUSINESS_COLUMNS,
                numeric_columns=INFO_NUMERIC_COLUMNS,
                target_date=target_date,
            )
            buy = row["a_buy_amount"] or Decimal("0")
            sell = row["a_sell_amount"] or Decimal("0")
            if (
                row["a_buy_amount"] is None
                and row["a_sell_amount"] is None
            ) or row["a_net_amount"] != buy - sell:
                raise AListDataBlocked(
                    "DATA_BLOCKED: Eastmoney detail buy/sell/net accounting differs"
                )
            canonical = canonical_business_rows([row], dataset="info")[0]
            identity = _digest(canonical)
            if identity in report_seen:
                raise AListDataBlocked(
                    f"DATA_BLOCKED: Eastmoney {report.report} contains duplicate rows"
                )
            report_seen.add(identity)
            normalized_by_identity.setdefault(identity, row)
    rows = list(normalized_by_identity.values())
    actual_codes = {_code(row["stock_code"]) for row in rows}
    if actual_codes != expected_codes:
        raise AListDataBlocked(
            "DATA_BLOCKED: alist detail code set differs from the complete daily report: "
            f"expected={len(expected_codes)} actual={len(actual_codes)} "
            f"missing={sorted(expected_codes - actual_codes)[:20]} "
            f"unexpected={sorted(actual_codes - expected_codes)[:20]}"
        )
    for row in rows:
        row["etl_sync_at"] = observed_at
    return rows


def _rows(result: Any) -> list[dict[str, Any]]:
    mappings = getattr(result, "mappings", None)
    if callable(mappings):
        return [dict(row) for row in mappings().all()]
    return [dict(row) for row in result]


def _read_partition(
    connection: Any,
    *,
    dataset: str,
    trade_date: str,
    include_storage: bool = False,
) -> list[dict[str, Any]]:
    business_columns = (
        DAILY_BUSINESS_COLUMNS if dataset == "daily" else INFO_BUSINESS_COLUMNS
    )
    columns = (
        DAILY_INSERT_COLUMNS if dataset == "daily" else INFO_INSERT_COLUMNS
    ) if include_storage else business_columns
    table = TABLES[dataset]
    selected = ",".join(f"`{column}`" for column in columns)
    ordering = ",".join(f"`{column}`" for column in business_columns)
    result = connection.execute(
        text(
            f"SELECT {selected} FROM `{table}` "
            "WHERE trade_date=:trade_date "
            f"ORDER BY {ordering}"
        ),
        {"trade_date": trade_date},
    )
    return _rows(result)


def _insert_statement(dataset: str) -> Any:
    table = TABLES[dataset]
    columns = DAILY_INSERT_COLUMNS if dataset == "daily" else INFO_INSERT_COLUMNS
    column_sql = ",".join(f"`{column}`" for column in columns)
    value_sql = ",".join(f":{column}" for column in columns)
    return text(f"INSERT INTO `{table}` ({column_sql}) VALUES ({value_sql})")


def _proof_equal(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    keys = (
        "row_count",
        "row_hash",
        "code_count",
        "code_set_hash",
        "authoritative_empty",
    )
    return all(actual.get(key) == expected.get(key) for key in keys)


def publish_partition(
    engine: Any,
    *,
    dataset: str,
    trade_date: str,
    rows: Sequence[Mapping[str, Any]],
    lock_timeout: int = 30,
) -> dict[str, Any]:
    """Atomically replace and independently read back one exact date."""

    target = _iso_date(trade_date)
    if any(_iso_date(row.get("trade_date")) != target for row in rows):
        raise AListDataBlocked(
            "DATA_BLOCKED: alist publish rows differ from the exact target date"
        )
    expected = database_proof(rows, dataset=dataset)
    table = TABLES[dataset]
    columns = DAILY_INSERT_COLUMNS if dataset == "daily" else INFO_INSERT_COLUMNS
    prepared = [{column: row.get(column) for column in columns} for row in rows]
    with mysql_named_lock(
        engine,
        LOCKS[dataset],
        timeout_seconds=max(0, int(lock_timeout)),
    ) as connection:
        if connection.in_transaction():
            connection.commit()
        with connection.begin():
            connection.execute(
                text(f"DELETE FROM `{table}` WHERE trade_date=:trade_date"),
                {"trade_date": target},
            )
            statement = _insert_statement(dataset)
            for offset in range(0, len(prepared), 1000):
                connection.execute(statement, prepared[offset : offset + 1000])
            transaction_proof = database_proof(
                _read_partition(
                    connection,
                    dataset=dataset,
                    trade_date=target,
                    include_storage=True,
                ),
                dataset=dataset,
            )
            if transaction_proof != expected:
                raise AListDataBlocked(
                    "DATA_BLOCKED: alist transaction readback differs before commit"
                )
    with engine.connect() as connection:
        committed = database_proof(
            _read_partition(
                connection,
                dataset=dataset,
                trade_date=target,
                include_storage=True,
            ),
            dataset=dataset,
        )
    if committed != expected:
        raise AListDataBlocked("DATA_BLOCKED: committed alist partition differs")
    return committed


def validate_runtime_schema(engine: Any) -> dict[str, Any]:
    validate_stock_catalog_runtime_schema(engine)
    required = {
        "si_trade_calendar": {"trade_date", "trade_status"},
        "st_a_list_daily": {"id", *DAILY_INSERT_COLUMNS},
        "st_a_list_info": {"id", *INFO_INSERT_COLUMNS},
    }
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT TABLE_NAME,COLUMN_NAME
                  FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA=DATABASE()
                   AND TABLE_NAME IN
                       ('si_trade_calendar','st_a_list_daily','st_a_list_info')
                """
            )
        ).fetchall()
    observed: dict[str, set[str]] = {table: set() for table in required}
    for table, column in rows:
        if str(table) in observed:
            observed[str(table)].add(str(column))
    missing = {
        table: sorted(columns - observed.get(table, set()))
        for table, columns in required.items()
        if columns - observed.get(table, set())
    }
    if missing:
        raise AListDataBlocked(f"DATA_BLOCKED: alist runtime schema differs: {missing}")
    manifest = {table: sorted(observed[table]) for table in sorted(observed)}
    return {"schema_hash": _digest(manifest), "tables": manifest}


def require_trade_session(engine: Any, *, trade_date: str, now: datetime) -> None:
    target = _iso_date(trade_date)
    if target > now.date().isoformat():
        raise AListDataBlocked("DATA_BLOCKED: alist target date is in the future")
    if target == now.date().isoformat() and now.time() < wall_time(16, 30):
        raise AListDataBlocked("DATA_BLOCKED: current alist session is not final")
    with engine.connect() as connection:
        status = connection.execute(
            text(
                "SELECT trade_status FROM si_trade_calendar "
                "WHERE trade_date=:trade_date LIMIT 1"
            ),
            {"trade_date": target},
        ).scalar()
    try:
        is_open = int(status) == 1
    except (TypeError, ValueError):
        is_open = False
    if not is_open:
        raise AListDataBlocked(
            "DATA_BLOCKED: immutable calendar does not prove an open alist session"
        )


def resolve_requested_trade_date(
    engine: Any,
    *,
    trade_date: str,
    latest_session: bool,
    now: datetime,
) -> str:
    """Resolve CLI convenience to one explicit, closed calendar session."""

    if bool(str(trade_date or "").strip()) == bool(latest_session):
        raise AListDataBlocked(
            "DATA_BLOCKED: choose exactly one of trade_date/latest_session"
        )
    if trade_date:
        return _iso_date(trade_date)
    current = now
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    current = current.astimezone(SHANGHAI)
    latest_allowed = current.date()
    if current.time() < wall_time(16, 30):
        latest_allowed -= timedelta(days=1)
    with engine.connect() as connection:
        observed = connection.execute(
            text(
                "SELECT MAX(trade_date) FROM si_trade_calendar "
                "WHERE trade_status=1 AND trade_date<=:latest_allowed"
            ),
            {"latest_allowed": latest_allowed.isoformat()},
        ).scalar()
    if observed is None:
        raise AListDataBlocked(
            "DATA_BLOCKED: calendar has no closed alist session"
        )
    return _iso_date(observed)


def _source_receipt(
    *,
    daily_report: ReportEvidence,
    daily_rows: Sequence[Mapping[str, Any]],
    detail_reports: Sequence[ReportEvidence] = (),
    detail_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    result = {
        "daily_report": daily_report.receipt(),
        "daily_partition": partition_proof(daily_rows, dataset="daily"),
    }
    if detail_reports:
        result["detail_reports"] = [report.receipt() for report in detail_reports]
        result["detail_report_manifest_hash"] = _digest(result["detail_reports"])
        result["detail_partition"] = partition_proof(detail_rows, dataset="info")
    return result


def run_sync(
    engine: Any,
    *,
    dataset: str,
    trade_date: str,
    apply: bool,
    expected_build_sha: str = "",
    provider: EastmoneyAListProvider | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if dataset not in TASK_TYPES:
        raise ValueError(f"unsupported alist dataset {dataset!r}")
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    current = current.astimezone(SHANGHAI).replace(microsecond=0)
    started_at = current.isoformat()
    target = _iso_date(trade_date)
    build_sha = resolve_build_sha(expected_build_sha)
    schema = validate_runtime_schema(engine)
    require_trade_session(engine, trade_date=target, now=current)
    catalog, catalog_codes = load_target_stock_catalog(
        engine,
        target_date=target,
        decision_known_at=current.replace(tzinfo=None),
    )
    allowed_codes = {_code(code) for code in catalog_codes}
    qmt_by_stock = {
        _code(member["stock_code"]): str(member["qmt_code"]).strip().upper()
        for member in catalog.members
        if _code(member["stock_code"]) in allowed_codes
    }
    if set(qmt_by_stock) != allowed_codes:
        raise AListDataBlocked(
            "DATA_BLOCKED: target-date alist catalog/QMT identities differ"
        )
    source = provider or EastmoneyAListProvider()
    owns_provider = provider is None
    try:
        daily_evidence = source.fetch_report(DAILY_REPORT, target)
        daily_rows = normalize_daily(
            daily_evidence,
            observed_at=current.replace(tzinfo=None),
            build_sha=build_sha,
            allowed_codes=allowed_codes,
            qmt_by_stock=qmt_by_stock,
        )
        detail_evidence: tuple[ReportEvidence, ...] = ()
        output_rows: Sequence[Mapping[str, Any]] = daily_rows
        if dataset == "info":
            expected_daily = partition_proof(daily_rows, dataset="daily")
            with engine.connect() as connection:
                current_daily = partition_proof(
                    _read_partition(
                        connection,
                        dataset="daily",
                        trade_date=target,
                    ),
                    dataset="daily",
                )
            if not _proof_equal(current_daily, expected_daily):
                raise AListDataBlocked(
                    "DATA_BLOCKED: persisted daily alist partition differs from current "
                    "complete provider report; rerun alist_daily first"
                )
            detail_evidence = tuple(
                source.fetch_report(report, target) for report in DETAIL_REPORTS
            )
            output_rows = normalize_info(
                detail_evidence,
                daily_codes=(row["stock_code"] for row in daily_rows),
                observed_at=current.replace(tzinfo=None),
                allowed_codes=allowed_codes,
            )
        collection = _source_receipt(
            daily_report=daily_evidence,
            daily_rows=daily_rows,
            detail_reports=detail_evidence,
            detail_rows=output_rows if dataset == "info" else (),
        )
        expected_database = database_proof(output_rows, dataset=dataset)
        if apply:
            database = publish_partition(
                engine,
                dataset=dataset,
                trade_date=target,
                rows=output_rows,
            )
            status = "PASS"
        else:
            database = {**expected_database, "not_written": True}
            status = "DRY_RUN"
        finished = datetime.now(SHANGHAI).replace(microsecond=0)
        return _signed(
            {
                "schema": RESULT_SCHEMA,
                "status": status,
                "dataset": dataset,
                "task_type": TASK_TYPES[dataset],
                "executor_owner": EXECUTOR_OWNER,
                "provider": PROVIDER_ID,
                "provider_endpoint": PROVIDER_ENDPOINT,
                "trade_date": target,
                "build_sha": build_sha,
                "started_at": started_at,
                "finished_at": finished.isoformat(),
                "runtime_schema_hash": schema["schema_hash"],
                "catalog": {
                    "batch_id": catalog.batch_id,
                    "manifest_hash": catalog.manifest_hash,
                    "member_set_hash": catalog.member_set_hash,
                    "captured_at": catalog.captured_at,
                    "history_complete_from": catalog.history_complete_from,
                    "eligible_code_count": len(allowed_codes),
                    "eligible_code_set_hash": code_set_hash(allowed_codes),
                },
                "collection": collection,
                "database": database,
            }
        )
    finally:
        if owns_provider:
            source.close()


def _failure(*, dataset: str, trade_date: str, error: BaseException) -> dict[str, Any]:
    message = str(error)
    terminal_markers = (
        "build SHA",
        "checkout differs",
        "runtime schema differs",
        "choose exactly one of trade_date/latest_session",
        "unsupported alist dataset",
    )
    retryable = not any(marker in message for marker in terminal_markers)
    return _signed(
        {
            "schema": RESULT_SCHEMA,
            "status": "DATA_BLOCKED",
            "dataset": dataset,
            "task_type": TASK_TYPES.get(dataset, ""),
            "executor_owner": EXECUTOR_OWNER,
            "provider": PROVIDER_ID,
            "trade_date": str(trade_date or "")[:10],
            "retryable": retryable,
            "error_type": type(error).__name__,
            "error": message[:1000],
        }
    )


def _valid_partition_receipt(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    try:
        row_count = int(value["row_count"])
        code_count = int(value["code_count"])
    except (KeyError, TypeError, ValueError):
        return False
    return (
        row_count >= code_count >= 0
        and re.fullmatch(r"[0-9a-f]{64}", str(value.get("row_hash") or ""))
        is not None
        and re.fullmatch(
            r"[0-9a-f]{64}", str(value.get("code_set_hash") or "")
        )
        is not None
        and bool(value.get("authoritative_empty")) == (row_count == 0)
    )


def _valid_database_receipt(value: Any) -> bool:
    return (
        _valid_partition_receipt(value)
        and isinstance(value, Mapping)
        and re.fullmatch(
            r"[0-9a-f]{64}", str(value.get("storage_row_hash") or "")
        )
        is not None
        and value.get("metadata_complete") is True
    )


def _valid_report_receipt(value: Any, *, report: str, trade_date: str) -> bool:
    if not isinstance(value, Mapping):
        return False
    try:
        declared_count = int(value["declared_count"])
        declared_pages = int(value["declared_pages"])
        fetched_pages = int(value["fetched_pages"])
        fetched_rows = int(value["fetched_row_count"])
    except (KeyError, TypeError, ValueError):
        return False
    if (
        value.get("report") != report
        or value.get("trade_date") != trade_date
        or re.fullmatch(
            r"[0-9a-f]{64}", str(value.get("response_hash") or "")
        )
        is None
    ):
        return False
    if value.get("authoritative_empty") is True:
        return (
            declared_count == 0
            and declared_pages == 0
            and fetched_pages == 1
            and fetched_rows == 0
        )
    return (
        value.get("authoritative_empty") is False
        and declared_count > 0
        and declared_pages == math.ceil(declared_count / PAGE_SIZE)
        and fetched_pages == declared_pages
        and fetched_rows == declared_count
    )


def validate_task_result(payload: Mapping[str, Any], return_code: int) -> str:
    if payload.get("schema") != RESULT_SCHEMA:
        return "failed"
    unsigned = dict(payload)
    supplied = unsigned.pop("receipt_id", None)
    if supplied != _digest(unsigned):
        return "failed"
    if payload.get("status") == "DATA_BLOCKED" and int(return_code) == 2:
        if payload.get("retryable") is True:
            return "failed"
        if payload.get("retryable") is False:
            return "blocked"
        return "failed"
    if payload.get("status") != "PASS" or int(return_code) != 0:
        return "failed"
    try:
        dataset = str(payload["dataset"])
        target = _iso_date(payload["trade_date"])
        build_sha = str(payload["build_sha"])
        database = payload["database"]
        collection = payload["collection"]
        catalog = payload["catalog"]
    except (KeyError, TypeError, AListDataBlocked):
        return "failed"
    daily_report = collection.get("daily_report") if isinstance(collection, Mapping) else None
    daily_partition = (
        collection.get("daily_partition") if isinstance(collection, Mapping) else None
    )
    valid = (
        dataset in TASK_TYPES
        and payload.get("task_type") == TASK_TYPES[dataset]
        and payload.get("provider") == PROVIDER_ID
        and payload.get("executor_owner") == EXECUTOR_OWNER
        and target == payload.get("trade_date")
        and SHA40.fullmatch(build_sha) is not None
        and build_sha != "0" * 40
        and isinstance(database, Mapping)
        and isinstance(collection, Mapping)
        and isinstance(catalog, Mapping)
        and _valid_report_receipt(
            daily_report, report=DAILY_REPORT, trade_date=target
        )
        and _valid_partition_receipt(daily_partition)
        and _valid_database_receipt(database)
        and bool(str(catalog.get("batch_id") or "").strip())
        and re.fullmatch(
            r"[0-9a-f]{64}", str(catalog.get("manifest_hash") or "")
        )
        is not None
        and re.fullmatch(
            r"[0-9a-f]{64}", str(catalog.get("member_set_hash") or "")
        )
        is not None
        and re.fullmatch(
            r"[0-9a-f]{64}", str(catalog.get("eligible_code_set_hash") or "")
        )
        is not None
    )
    if not valid:
        return "failed"
    try:
        row_count = int(database["row_count"])
        code_count = int(database["code_count"])
        daily_code_count = int(daily_partition["code_count"])
        eligible_code_count = int(catalog["eligible_code_count"])
        provider_daily_count = int(daily_report["fetched_row_count"])
    except (KeyError, TypeError, ValueError):
        return "failed"
    if (
        eligible_code_count <= 0
        or daily_code_count > eligible_code_count
        or int(daily_partition["row_count"]) > provider_daily_count
    ):
        return "failed"
    if dataset == "info":
        detail = collection.get("detail_partition")
        reports = collection.get("detail_reports")
        if (
            not _valid_partition_receipt(detail)
            or not _proof_equal(detail, database)
            or not isinstance(reports, list)
            or len(reports) != 2
            or not all(isinstance(item, Mapping) for item in reports)
            or [item.get("report") for item in reports] != list(DETAIL_REPORTS)
            or any(
                not _valid_report_receipt(
                    report, report=expected_report, trade_date=target
                )
                for report, expected_report in zip(reports, DETAIL_REPORTS)
            )
            or collection.get("detail_report_manifest_hash") != _digest(reports)
            or detail.get("code_count") != daily_partition.get("code_count")
            or detail.get("code_set_hash") != daily_partition.get("code_set_hash")
            or row_count > sum(int(report["fetched_row_count"]) for report in reports)
        ):
            return "failed"
    elif not _proof_equal(daily_partition, database):
        return "failed"
    return "complete"


def validate_persisted_result(
    engine: Any,
    payload: Mapping[str, Any],
    *,
    now: datetime | None = None,
    expected_session: str = "",
) -> dict[str, Any]:
    """Independently re-read a PASS receipt's catalog and exact DB partition."""

    if validate_task_result(payload, 0) != "complete":
        raise AListDataBlocked("DATA_BLOCKED: alist task receipt is invalid")
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    current = current.astimezone(SHANGHAI).replace(microsecond=0)
    finished = _metadata_datetime(payload.get("finished_at"), field="finished_at")
    if finished > current.replace(tzinfo=None) + timedelta(minutes=5):
        raise AListDataBlocked("DATA_BLOCKED: alist receipt is from the future")
    if _git_head() != str(payload.get("build_sha") or ""):
        raise AListDataBlocked(
            "DATA_BLOCKED: persisted alist receipt build differs from checkout"
        )
    validate_runtime_schema(engine)
    target = _iso_date(payload["trade_date"])
    expected = (
        _iso_date(expected_session, field="expected session")
        if str(expected_session or "").strip()
        else ""
    )
    if expected and target != expected:
        raise AListDataBlocked(
            "DATA_BLOCKED: alist receipt session differs from release target"
        )
    catalog_receipt = payload["catalog"]
    catalog, eligible_codes = load_target_stock_catalog(
        engine,
        target_date=target,
        decision_known_at=current.replace(tzinfo=None),
        batch_id=str(catalog_receipt["batch_id"]),
    )
    normalized_codes = {_code(code) for code in eligible_codes}
    observed_catalog = {
        "batch_id": catalog.batch_id,
        "manifest_hash": catalog.manifest_hash,
        "member_set_hash": catalog.member_set_hash,
        "captured_at": catalog.captured_at,
        "history_complete_from": catalog.history_complete_from,
        "eligible_code_count": len(normalized_codes),
        "eligible_code_set_hash": code_set_hash(normalized_codes),
    }
    if observed_catalog != dict(catalog_receipt):
        raise AListDataBlocked(
            "DATA_BLOCKED: persisted alist catalog receipt differs"
        )
    dataset = str(payload["dataset"])
    with engine.connect() as connection:
        observed_database = database_proof(
            _read_partition(
                connection,
                dataset=dataset,
                trade_date=target,
                include_storage=True,
            ),
            dataset=dataset,
        )
        if dataset == "info":
            observed_daily = partition_proof(
                _read_partition(
                    connection,
                    dataset="daily",
                    trade_date=target,
                ),
                dataset="daily",
            )
        else:
            observed_daily = observed_database
    if observed_database != dict(payload["database"]):
        raise AListDataBlocked(
            "DATA_BLOCKED: persisted alist database receipt differs"
        )
    if not _proof_equal(observed_daily, payload["collection"]["daily_partition"]):
        raise AListDataBlocked(
            "DATA_BLOCKED: persisted alist daily prerequisite differs"
        )
    return {
        "dataset": dataset,
        "trade_date": target,
        "row_count": observed_database["row_count"],
        "code_count": observed_database["code_count"],
        "row_hash": observed_database["row_hash"],
        "storage_row_hash": observed_database["storage_row_hash"],
        "catalog_manifest_hash": catalog.manifest_hash,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=sorted(TASK_TYPES), required=True)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--trade-date", default="")
    date_group.add_argument("--latest-session", action="store_true")
    parser.add_argument("--expected-build-sha", default="")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    target = args.trade_date
    try:
        load_project_env()
        engine = create_tool_engine()
        try:
            current = datetime.now(SHANGHAI).replace(microsecond=0)
            target = resolve_requested_trade_date(
                engine,
                trade_date=args.trade_date,
                latest_session=args.latest_session,
                now=current,
            )
            result = run_sync(
                engine,
                dataset=args.dataset,
                trade_date=target,
                apply=args.apply,
                expected_build_sha=args.expected_build_sha,
                now=current,
            )
        finally:
            engine.dispose()
        code = 0
    except Exception as exc:  # final machine evidence, never stale fallback
        result = _failure(
            dataset=args.dataset,
            trade_date=target,
            error=exc,
        )
        code = 2
    print(_canonical_json(result), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())

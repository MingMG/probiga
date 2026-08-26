# -*- coding: utf-8 -*-
"""
东财「个股公告列表」拉取并写入 MySQL ``probiga.si_notice_eastmoney``（标题级，非 PDF 正文）。

该表只服务页面展示，不再写入策略 PIT 事实或覆盖凭证。策略唯一权威
公告源是官方 QMT ``announcement`` 全市场原子批次；QMT 不可用时策略
必须 ``DATA_BLOCKED``，不得回退到本脚本的轮转子集。

前置::

  pip install -r requirements-platform.txt

执行（仓库根）::

  python -m biz.notice.sync_notice_em --mode incremental --stock 600519
  python -m biz.notice.sync_notice_em --mode incremental --from-si-all-code --limit 0
  python -m biz.notice.sync_notice_em --mode historical-repair --from-si-all-code --limit 0 --history-state-file /var/lib/probiga/notice-history-repair-v1.json

环境变量：
  MYSQL_URL  必填，MySQL 连接串；也可写入项目根目录 ``.env``
说明：
  - 接口为 ``https://np-anotice-stock.eastmoney.com/api/security/ann``，需网络；请控制 ``--limit``、``--sleep``，避免封 IP。
  - 正式唯一键为 ``(stock_code, art_code)``；逐股 source ``codes`` 身份验证后才置为可展示。
  - 日常任务只精确替换当前目录的有界日期窗；全历史错配修复会覆盖当前目录与遗留关联代码的并集，且必须显式使用可恢复分片模式。
  - 详情链接为构造 URL，若东财改版请以列表页为准：``https://data.eastmoney.com/notices/stock/{code}.html``
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    import httpx
except ModuleNotFoundError:  # allows schema/parser tests without network extras
    httpx = None  # type: ignore[assignment]
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sync_notice_em")

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.legacy_table_surface import validate_required_table_surface
from integrations.qmt.backend import to_qmt_symbol

DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_si_notice_eastmoney.sql"
SHANGHAI = ZoneInfo("Asia/Shanghai")
NOTICE_ENDPOINT = "https://np-anotice-stock.eastmoney.com/api/security/ann"
HISTORY_LEDGER_SCHEMA = "probiga.notice-history-repair-ledger.v1"
HISTORY_RESULT_SCHEMA = "probiga.notice-history-repair-result.v1"
HISTORY_TASK_TYPE = "notice_eastmoney_historical_repair"
HISTORY_DATASET = "notice_eastmoney_full_history"
_EMPTY_CHAIN_HASH = "0" * 64
_HISTORY_GENERATION_MARKER = ".generation-"
NOTICE_PROVIDER_ID = "eastmoney_notice"
NOTICE_DATA_VERSION = hashlib.sha256(
    b"probiga.eastmoney-notice-exact-association.v2"
).hexdigest()
NOTICE_QUALITY_STATUS = "SOURCE_IDENTITY_VALIDATED"
NOTICE_PERMISSION_STATUS = "PUBLIC"


@dataclass(frozen=True)
class NoticeFetchResult:
    rows: list[dict[str, Any]]
    captured_at: datetime
    window_start: date
    exhausted: bool
    page_count: int
    total_hits: int = 0
    expected_pages: int = 0
    window_end: date | None = None
    bounded: bool = False


@dataclass(frozen=True)
class NoticePersistResult:
    written_count: int
    deleted_count: int
    persisted_count: int
    persisted_row_hash: str


def _shanghai_now() -> datetime:
    return datetime.now(SHANGHAI).replace(tzinfo=None, microsecond=0)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _sha256(value: Any) -> str:
    raw = value if isinstance(value, str) else _canonical_json(value)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = _parse_notice_date(value)
    return parsed.date() if parsed is not None else None


def _canonical_timestamp(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("T", " "))
        except ValueError:
            return str(value).strip()
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(SHANGHAI).replace(tzinfo=None)
    return parsed.replace(microsecond=0).isoformat(sep=" ", timespec="seconds")


def _canonical_notice_row(row: dict[str, Any]) -> dict[str, Any]:
    notice_date = _coerce_date(row.get("notice_date"))
    return {
        "stock_code": str(row.get("stock_code") or "").strip().zfill(6),
        "art_code": str(row.get("art_code") or "").strip(),
        "notice_date": notice_date.isoformat() if notice_date is not None else "",
        "title": str(row.get("title") or "").strip(),
        "column_name": str(row.get("column_name") or "").strip(),
        "display_time": str(row.get("display_time") or "").strip(),
        "detail_url": str(row.get("detail_url") or "").strip(),
        "association_validated": int(row.get("association_validated") or 0),
        "qmt_code": str(row.get("qmt_code") or "").strip().upper(),
        "data_source": str(row.get("data_source") or "").strip(),
        "source_time": _canonical_timestamp(row.get("source_time")),
        "received_at": _canonical_timestamp(row.get("received_at")),
        "batch_id": str(row.get("batch_id") or "").strip(),
        "data_version": str(row.get("data_version") or "").strip(),
        "quality_status": str(row.get("quality_status") or "").strip(),
        "permission_status": str(row.get("permission_status") or "").strip(),
    }


def _notice_row_hash(rows: list[dict[str, Any]]) -> str:
    canonical = sorted(
        (_canonical_notice_row(row) for row in rows),
        key=lambda row: (row["stock_code"], row["art_code"]),
    )
    return _sha256(canonical)

UPSERT_SQL = text(
    """
    INSERT INTO si_notice_eastmoney (
        stock_code, art_code, notice_date, title, column_name, display_time,
        detail_url, association_validated, etl_sync_at, qmt_code,
        data_source, source_time, received_at, batch_id, data_version,
        quality_status, permission_status
    ) VALUES (
        :stock_code, :art_code, :notice_date, :title, :column_name,
        :display_time, :detail_url, :association_validated, :etl_sync_at,
        :qmt_code, :data_source, :source_time, :received_at, :batch_id,
        :data_version, :quality_status, :permission_status
    )
    ON DUPLICATE KEY UPDATE
        notice_date = VALUES(notice_date),
        title = VALUES(title),
        column_name = VALUES(column_name),
        display_time = VALUES(display_time),
        detail_url = VALUES(detail_url),
        association_validated = VALUES(association_validated),
        etl_sync_at = VALUES(etl_sync_at),
        qmt_code = VALUES(qmt_code),
        data_source = VALUES(data_source),
        source_time = VALUES(source_time),
        received_at = VALUES(received_at),
        batch_id = VALUES(batch_id),
        data_version = VALUES(data_version),
        quality_status = VALUES(quality_status),
        permission_status = VALUES(permission_status)
    """
)


def run_ddl(engine: Engine) -> None:
    """Legacy entrypoint retained as a read-only prepared-schema guard."""

    validate_required_table_surface(
        engine,
        {"si_notice_eastmoney"},
        context="Eastmoney notice display collector",
        required_columns={
            "si_notice_eastmoney": {
                "stock_code",
                "art_code",
                "notice_date",
                "title",
                "column_name",
                "display_time",
                "detail_url",
                "association_validated",
                "etl_sync_at",
                "qmt_code",
                "data_source",
                "source_time",
                "received_at",
                "batch_id",
                "data_version",
                "quality_status",
                "permission_status",
            },
        },
    )
    inspector = inspect(engine)
    unique_shapes = {
        tuple(str(column) for column in item.get("column_names") or [])
        for item in inspector.get_indexes("si_notice_eastmoney")
        if item.get("unique")
    }
    unique_shapes.update(
        tuple(str(column) for column in item.get("column_names") or [])
        for item in inspector.get_unique_constraints("si_notice_eastmoney")
    )
    if ("stock_code", "art_code") not in unique_shapes:
        raise RuntimeError(
            "Eastmoney notice display collector requires prepared unique key "
            "(stock_code, art_code)"
        )


def _detail_url(stock_code: str, art_code: str) -> str:
    c = str(stock_code).strip().zfill(6)
    return f"https://data.eastmoney.com/notices/detail/{c}/{art_code}.html"


def _parse_notice_date(raw: Any) -> datetime | None:
    if raw is None:
        return None
    s = str(raw).strip()[:10]
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except ValueError:
            return None
    return None


def _event_date_from_item(item: dict[str, Any]) -> datetime | None:
    """Business/event date; it is not evidence of publication time."""
    for key in ("notice_date", "art_date", "display_time", "eiTime"):
        parsed = _parse_notice_date(item.get(key))
        if parsed:
            return parsed
    return None


def _parse_source_publication_time(raw: Any) -> datetime | None:
    """Strictly parse Eastmoney's exact Shanghai publication timestamp."""
    if raw is None:
        return None
    value = str(raw).strip()
    # Eastmoney sometimes encodes milliseconds with a colon after seconds.
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?::(\d{1,6}))?",
        value,
    )
    if match:
        fraction = (match.group(3) or "").ljust(6, "0")
        canonical = f"{match.group(1)} {match.group(2)}"
        if fraction:
            canonical += f".{fraction}"
        try:
            parsed = datetime.fromisoformat(canonical)
        except ValueError:
            return None
        if parsed.time() == datetime.min.time():
            return None
        return parsed
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
    if parsed.time() == datetime.min.time():
        return None
    return parsed


def _publication_time_from_item(item: dict[str, Any]) -> datetime | None:
    for key in ("display_time", "eiTime"):
        parsed = _parse_source_publication_time(item.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_item(
    stock_code: str,
    item: dict[str, Any],
    etl: datetime,
    *,
    validated_stock_identity: bool = False,
    batch_id: str = "",
) -> dict[str, Any]:
    code = str(stock_code).strip().zfill(6)
    if validated_stock_identity:
        identities = {
            str(identity.get("stock_code") or "").strip().zfill(6)
            for identity in (item.get("codes") or [])
            if isinstance(identity, dict)
        }
        if code not in identities:
            raise ValueError("notice source stock identity differs from request")
        if re.fullmatch(r"[0-9a-f]{64}", batch_id) is None:
            raise ValueError("validated notice row requires one exact batch identity")
    art = (item.get("art_code") or "").strip()
    title = (item.get("title") or item.get("title_ch") or "").strip()[:1024]
    cols = item.get("columns") or []
    col_name = ""
    if isinstance(cols, list) and cols and isinstance(cols[0], dict):
        col_name = str(cols[0].get("column_name") or "")[:256]
    disp = str(item.get("display_time") or "")[:64]
    nd = _event_date_from_item(item)
    published_at = _publication_time_from_item(item)
    if published_at is not None and published_at > etl:
        raise ValueError(
            "notice source publication time is later than local receipt"
        )
    received_at = etl.replace(microsecond=0)
    qmt_code = to_qmt_symbol(code)
    if validated_stock_identity and qmt_code is None:
        raise ValueError("validated notice row lacks a canonical QMT identity")
    source_time = (
        published_at.replace(microsecond=0)
        if published_at is not None
        else received_at
    )
    return {
        "stock_code": code,
        "art_code": art,
        "notice_date": nd.date() if nd else None,
        "event_date": nd.date() if nd else None,
        "published_at": (
            published_at.isoformat(sep=" ", timespec="microseconds")
            if published_at is not None
            else None
        ),
        "title": title or None,
        "column_name": col_name or None,
        "display_time": disp or None,
        "detail_url": _detail_url(stock_code, art) if art else None,
        "association_validated": 1 if validated_stock_identity else 0,
        "etl_sync_at": received_at,
        "qmt_code": qmt_code,
        "data_source": NOTICE_PROVIDER_ID if validated_stock_identity else None,
        "source_time": source_time if validated_stock_identity else None,
        "received_at": received_at if validated_stock_identity else None,
        "batch_id": batch_id if validated_stock_identity else None,
        "data_version": (
            NOTICE_DATA_VERSION if validated_stock_identity else None
        ),
        "quality_status": (
            NOTICE_QUALITY_STATUS if validated_stock_identity else None
        ),
        "permission_status": (
            NOTICE_PERMISSION_STATUS if validated_stock_identity else None
        ),
    }


def fetch_pages(
    client: httpx.Client,
    stock_code: str,
    *,
    page_size: int,
    max_pages: int,
    begin_date: date | None = None,
    end_date: date | None = None,
) -> NoticeFetchResult:
    code = str(stock_code).strip().zfill(6)
    if re.fullmatch(r"\d{6}", code) is None:
        raise ValueError("notice request requires one exact six-digit code")
    if page_size <= 0 or max_pages <= 0:
        raise ValueError("notice pagination bounds must be positive")
    if (begin_date is None) != (end_date is None):
        raise ValueError("notice date scope requires both begin_date and end_date")
    if begin_date is not None and end_date is not None and begin_date > end_date:
        raise ValueError("notice date scope begin_date cannot exceed end_date")
    out: list[dict[str, Any]] = []
    exhausted = False
    page_count = 0
    total_hits: int | None = None
    expected_pages: int | None = None
    seen_art_codes: set[str] = set()
    for page in range(1, max_pages + 1):
        params = {
            "sr": -1,
            "page_size": page_size,
            "page_index": page,
            "ann_type": "A",
            "client_source": "web",
            "f_node": 0,
            "s_node": 0,
            "stock_list": code,
        }
        if begin_date is not None and end_date is not None:
            params["begin_time"] = begin_date.isoformat()
            params["end_time"] = end_date.isoformat()
        last_error: Exception | None = None
        response: httpx.Response | None = None
        for attempt in range(3):
            try:
                response = client.get(
                    NOTICE_ENDPOINT,
                    params=params,
                    timeout=20.0,
                )
                response.raise_for_status()
                break
            except httpx.HTTPError as e:
                last_error = e
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if response is None:
            raise last_error or RuntimeError("notice source returned no response")
        payload = response.json()
        if payload.get("success") != 1:
            raise RuntimeError(
                f"{code} 第 {page} 页 source success!=1: {payload.get('error')}"
            )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError(f"{code} 第 {page} 页 source data is missing")
        try:
            source_page = int(data.get("page_index"))
            source_page_size = int(data.get("page_size"))
            source_total_hits = int(data.get("total_hits"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                f"{code} 第 {page} 页 pagination metadata is invalid"
            ) from exc
        if (
            source_page != page
            or source_page_size != page_size
            or source_total_hits < 0
        ):
            raise RuntimeError(
                f"{code} 第 {page} 页 pagination identity differs"
            )
        if total_hits is None:
            total_hits = source_total_hits
            expected_pages = (
                (total_hits + page_size - 1) // page_size
                if total_hits
                else 0
            )
            if expected_pages > max_pages:
                raise RuntimeError(
                    f"{code} notice pagination ceiling is insufficient: "
                    f"required_pages={expected_pages} max_pages={max_pages}"
                )
        elif source_total_hits != total_hits:
            raise RuntimeError(
                f"{code} notice total_hits changed during pagination"
            )
        page_count = page
        raw_list = data.get("list")
        if raw_list is None:
            raw_list = []
        if not isinstance(raw_list, list):
            raise RuntimeError(f"{code} 第 {page} 页 list is not an array")
        lst: list[dict[str, Any]] = []
        for item in raw_list:
            if not isinstance(item, dict):
                raise RuntimeError(
                    f"{code} 第 {page} 页 contains a non-object notice"
                )
            identities = {
                str(identity.get("stock_code") or "").strip().zfill(6)
                for identity in (item.get("codes") or [])
                if isinstance(identity, dict)
            }
            if code not in identities:
                raise RuntimeError(
                    f"{code} 第 {page} 页 source stock identity differs"
                )
            art_code = str(item.get("art_code") or "").strip()
            if not art_code or art_code in seen_art_codes:
                raise RuntimeError(
                    f"{code} 第 {page} 页 notice identity is missing or duplicated"
                )
            seen_art_codes.add(art_code)
            lst.append(item)
        if expected_pages is None:
            raise RuntimeError(
                f"{code} 第 {page} 页 pagination page count is unavailable"
            )
        expected_page_rows = (
            0
            if expected_pages == 0
            else (
                page_size
                if page < expected_pages
                else int(total_hits or 0) - page_size * (expected_pages - 1)
            )
        )
        if len(lst) != expected_page_rows:
            raise RuntimeError(
                f"{code} 第 {page} 页 row count differs from total_hits: "
                f"rows={len(lst)} expected={expected_page_rows}"
            )
        if expected_pages == 0:
            exhausted = True
            break
        out.extend(lst)
        if page == expected_pages:
            exhausted = True
            break
    if total_hits is None or expected_pages is None:
        raise RuntimeError(f"{code} notice pagination produced no metadata")
    if not exhausted or len(out) != total_hits:
        raise RuntimeError(
            f"{code} notice pagination is not exhaustive: "
            f"rows={len(out)} total_hits={total_hits}"
        )
    captured_at = _shanghai_now()
    parsed_dates = [
        value.date()
        for value in (_event_date_from_item(item) for item in out)
        if value is not None
    ]
    if len(parsed_dates) != len(out):
        raise RuntimeError("notice response contains rows without a source date")
    if any(left < right for left, right in zip(parsed_dates, parsed_dates[1:])):
        raise RuntimeError("notice response is not in the requested descending order")
    if begin_date is not None and end_date is not None:
        if any(value < begin_date or value > end_date for value in parsed_dates):
            raise RuntimeError("notice response contains a row outside the requested date scope")
        window_start = begin_date
        window_end = end_date
    else:
        window_start = date(1900, 1, 1)
        window_end = None
    return NoticeFetchResult(
        rows=out,
        captured_at=captured_at,
        window_start=window_start,
        exhausted=exhausted,
        page_count=page_count,
        total_hits=total_hits,
        expected_pages=expected_pages,
        window_end=window_end,
        bounded=begin_date is not None,
    )


def upsert_rows(
    engine: Engine,
    rows: list[dict[str, Any]],
    *,
    stock_code: str | None = None,
    window_start: date | str | None = None,
    captured_at: datetime | None = None,
    fetch_evidence: dict[str, Any] | None = None,
) -> int:
    observed_at = captured_at or datetime.now()
    code = str(stock_code or (rows[0].get("stock_code") if rows else "")).strip().zfill(6)
    if not code or code == "000000":
        raise ValueError("notice coverage requires the requested stock code")
    if any(str(row.get("stock_code") or "").zfill(6) != code for row in rows):
        raise ValueError("notice response stock identity differs from request")
    if any(not row.get("art_code") for row in rows):
        raise ValueError("notice response contains an event without stable identity")
    n = 0
    # ``window_start``/``fetch_evidence`` remain accepted for CLI/backward
    # compatibility, but deliberately cannot create strategy evidence.
    _ = window_start, fetch_evidence
    with engine.begin() as conn:
        for row in rows:
            payload = {**row, "etl_sync_at": observed_at}
            conn.execute(UPSERT_SQL, payload)
            n += 1
    return n


def reconcile_rows(
    engine: Engine,
    rows: list[dict[str, Any]],
    *,
    stock_code: str,
    captured_at: datetime,
    window_start: date | None = None,
    window_end: date | None = None,
    full_history: bool = False,
) -> NoticePersistResult:
    """Atomically replace one authoritative provider scope for one stock.

    Daily synchronization replaces only the bounded Eastmoney date request.
    Historical repair replaces the stock's whole display-table history.  The
    delete and insert share one transaction, so a failed write cannot expose a
    half-reconciled stock.
    """

    code = str(stock_code or "").strip().zfill(6)
    if re.fullmatch(r"\d{6}", code) is None:
        raise ValueError("notice reconciliation requires one exact stock code")
    if full_history:
        if window_start is not None or window_end is not None:
            raise ValueError("full-history replacement cannot carry a date window")
    elif window_start is None or window_end is None or window_start > window_end:
        raise ValueError("incremental replacement requires one valid date window")

    canonical_rows: list[dict[str, Any]] = []
    seen_art_codes: set[str] = set()
    expected_qmt_code = to_qmt_symbol(code)
    if expected_qmt_code is None:
        raise ValueError("notice reconciliation lacks a canonical QMT identity")
    for raw in rows:
        row = dict(raw)
        if str(row.get("stock_code") or "").strip().zfill(6) != code:
            raise ValueError("notice response stock identity differs from request")
        art_code = str(row.get("art_code") or "").strip()
        row_date = _coerce_date(row.get("notice_date"))
        if (
            not art_code
            or art_code in seen_art_codes
            or row_date is None
            or not str(row.get("title") or "").strip()
        ):
            raise ValueError("notice response contains an incomplete or duplicate event")
        if (
            int(row.get("association_validated") or 0) != 1
            or str(row.get("qmt_code") or "").strip().upper()
            != expected_qmt_code
            or row.get("data_source") != NOTICE_PROVIDER_ID
            or _canonical_timestamp(row.get("source_time")) == ""
            or _canonical_timestamp(row.get("received_at")) == ""
            or _canonical_timestamp(row.get("source_time"))
            > _canonical_timestamp(row.get("received_at"))
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(row.get("batch_id") or ""),
            )
            is None
            or row.get("data_version") != NOTICE_DATA_VERSION
            or row.get("quality_status") != NOTICE_QUALITY_STATUS
            or row.get("permission_status") != NOTICE_PERMISSION_STATUS
        ):
            raise ValueError("notice response lacks validated association provenance")
        if (
            not full_history
            and window_start is not None
            and window_end is not None
            and not window_start <= row_date <= window_end
        ):
            raise ValueError("notice response event lies outside its replacement scope")
        seen_art_codes.add(art_code)
        row["stock_code"] = code
        row["art_code"] = art_code
        row["notice_date"] = row_date
        row["etl_sync_at"] = captured_at
        canonical_rows.append(row)

    with engine.begin() as connection:
        if full_history:
            deleted = connection.execute(
                text(
                    "DELETE FROM si_notice_eastmoney "
                    "WHERE stock_code=:stock_code"
                ),
                {"stock_code": code},
            )
        else:
            deleted = connection.execute(
                text(
                    "DELETE FROM si_notice_eastmoney "
                    "WHERE stock_code=:stock_code "
                    "AND notice_date>=:window_start "
                    "AND notice_date<=:window_end"
                ),
                {
                    "stock_code": code,
                    "window_start": window_start,
                    "window_end": window_end,
                },
            )
        if canonical_rows:
            connection.execute(UPSERT_SQL, canonical_rows)

        query = (
            "SELECT stock_code, art_code, notice_date, title, column_name, "
            "display_time, detail_url, association_validated, qmt_code, "
            "data_source, source_time, received_at, batch_id, data_version, "
            "quality_status, permission_status FROM si_notice_eastmoney "
            "WHERE stock_code=:stock_code AND association_validated=1"
        )
        params: dict[str, Any] = {"stock_code": code}
        if not full_history:
            query += " AND notice_date>=:window_start AND notice_date<=:window_end"
            params.update(
                {"window_start": window_start, "window_end": window_end}
            )
        query += " ORDER BY art_code"
        persisted_rows = [
            dict(row)
            for row in connection.execute(text(query), params).mappings().all()
        ]
        expected_hash = _notice_row_hash(canonical_rows)
        persisted_hash = _notice_row_hash(persisted_rows)
        if (
            len(persisted_rows) != len(canonical_rows)
            or persisted_hash != expected_hash
        ):
            raise RuntimeError(
                f"{code} notice persisted scope differs from provider: "
                f"database={len(persisted_rows)} source={len(canonical_rows)}"
            )

    deleted_count = int(getattr(deleted, "rowcount", 0) or 0)
    return NoticePersistResult(
        written_count=len(canonical_rows),
        deleted_count=max(0, deleted_count),
        persisted_count=len(persisted_rows),
        persisted_row_hash=persisted_hash,
    )


def read_codes_from_db(engine: Engine, offset: int, limit: int) -> list[str]:
    if limit < 0:
        raise ValueError("notice stock limit cannot be negative")
    if limit == 0:
        q = text(
            "SELECT stock_code FROM si_all_code "
            "ORDER BY stock_code LIMIT 18446744073709551615 OFFSET :off"
        )
        params = {"off": offset}
    else:
        q = text(
            "SELECT stock_code FROM si_all_code "
            "ORDER BY stock_code LIMIT :lim OFFSET :off"
        )
        params = {"lim": limit, "off": offset}
    with engine.connect() as c:
        rows = c.execute(q, params).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


def read_history_repair_codes(engine: Engine) -> list[str]:
    """Return current catalog plus every legacy notice association to repair."""

    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT stock_code
            FROM (
                SELECT stock_code FROM si_all_code
                UNION
                SELECT stock_code FROM si_notice_eastmoney
            ) AS notice_repair_universe
            ORDER BY stock_code
        """)).fetchall()
    return [str(row[0]).strip().zfill(6) for row in rows]


def _coverage_ratio(numerator: int, denominator: int) -> float:
    return float(numerator) / float(denominator) if denominator > 0 else 0.0


def _code_set_hash(codes: list[str]) -> str:
    normalized = sorted({str(code).strip().zfill(6) for code in codes})
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


def _ordered_code_hash(codes: list[str]) -> str:
    normalized = [str(code).strip().zfill(6) for code in codes]
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key in history ledger: {key}")
        result[key] = value
    return result


def _history_batch_contract(codes: list[str]) -> dict[str, Any]:
    return {
        "schema": HISTORY_LEDGER_SCHEMA,
        "provider": "eastmoney_np_anotice_stock",
        "endpoint": NOTICE_ENDPOINT,
        "request_scope": "one_stock_full_history",
        "pagination_contract": "exact_total_hits_v1",
        "requested_code_count": len(codes),
        "requested_code_set_hash": _code_set_hash(codes),
        "ordered_code_sha256": _ordered_code_hash(codes),
    }


def _new_history_ledger(codes: list[str], *, now: datetime) -> dict[str, Any]:
    contract = _history_batch_contract(codes)
    return {
        **contract,
        "batch_id": _sha256(contract),
        "generation": 1,
        "parent_ledger_sha256": None,
        "inherited_entry_count": 0,
        "requested_codes": list(codes),
        "status": "PROGRESS",
        "created_at": now.isoformat(sep=" ", timespec="microseconds"),
        "updated_at": now.isoformat(sep=" ", timespec="microseconds"),
        "completed_at": None,
        "next_offset": 0,
        "completed_code_count": 0,
        "completed_code_set_hash": _code_set_hash([]),
        "completed_entries": [],
        "evidence_chain_sha256": _EMPTY_CHAIN_HASH,
        "last_failure": None,
    }


def _new_history_generation_ledger(
    parent: Mapping[str, Any],
    codes: list[str],
    *,
    now: datetime,
) -> dict[str, Any]:
    """Create one append-only generation by inheriting a verified COMPLETE prefix."""

    parent_codes = [str(code) for code in parent.get("requested_codes") or []]
    parent_entries = [dict(entry) for entry in parent.get("completed_entries") or []]
    if (
        parent.get("status") != "COMPLETE"
        or not re.fullmatch(r"[0-9a-f]{64}", str(parent.get("ledger_sha256") or ""))
        or len(parent_codes) != len(parent_entries)
        or codes[: len(parent_codes)] != parent_codes
        or len(codes) <= len(parent_codes)
    ):
        raise ValueError("history generation parent or inherited prefix is invalid")
    ledger = _new_history_ledger(codes, now=now)
    inherited_count = len(parent_codes)
    return {
        **ledger,
        "generation": int(parent.get("generation") or 1) + 1,
        "parent_ledger_sha256": str(parent["ledger_sha256"]),
        "inherited_entry_count": inherited_count,
        "next_offset": inherited_count,
        "completed_code_count": inherited_count,
        "completed_code_set_hash": _code_set_hash(parent_codes),
        "completed_entries": parent_entries,
        "evidence_chain_sha256": _history_entry_chain(parent_entries),
    }


def _history_entry_chain(entries: list[dict[str, Any]]) -> str:
    chain = _EMPTY_CHAIN_HASH
    for entry in entries:
        chain = _sha256({"previous_sha256": chain, "entry": entry})
    return chain


def _sealed_ledger(ledger: dict[str, Any]) -> dict[str, Any]:
    sealed = dict(ledger)
    sealed.pop("ledger_sha256", None)
    sealed["ledger_sha256"] = _sha256(sealed)
    return sealed


def _validate_history_ledger_location(path: Path) -> None:
    if not path.is_absolute():
        raise ValueError("history ledger path must be absolute")
    parent = path.parent
    if not parent.is_dir():
        raise ValueError("history ledger parent directory must already exist")
    if parent.is_symlink() or (path.exists() and path.is_symlink()):
        raise ValueError("history ledger cannot be a symbolic link")

    configured_root = str(os.environ.get("PROBIGA_JOB_LOG_ROOT") or "").strip()
    production = str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower()
    if production == "production" and not configured_root:
        raise ValueError("production history ledger requires PROBIGA_JOB_LOG_ROOT")
    if configured_root:
        protected_root = Path(configured_root)
        if not protected_root.is_absolute() or not protected_root.is_dir():
            raise ValueError("history ledger protected root is invalid")
        if protected_root.is_symlink():
            raise ValueError("history ledger protected root cannot be a symbolic link")
        if parent.resolve(strict=True) != protected_root.resolve(strict=True):
            raise ValueError("history ledger must be directly inside the protected root")
        if os.name != "nt":
            root_mode = protected_root.stat().st_mode & 0o777
            if root_mode & 0o077:
                raise ValueError("history ledger protected root permissions are unsafe")
            if path.exists() and path.stat().st_mode & 0o077:
                raise ValueError("history ledger file permissions are unsafe")


def _atomic_write_history_ledger(path: Path, ledger: dict[str, Any]) -> dict[str, Any]:
    _validate_history_ledger_location(path)
    parent = path.parent
    sealed = _sealed_ledger(ledger)
    if path.exists():
        existing = _read_history_ledger(path, codes=None)
        if (
            existing.get("status") == "COMPLETE"
            and existing.get("ledger_sha256") != sealed.get("ledger_sha256")
        ):
            raise ValueError("history COMPLETE ledger is immutable")
    temporary = parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            if os.name != "nt":
                os.chmod(temporary, 0o600)
            handle.write(_canonical_json(sealed))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory_fd = os.open(str(parent), os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()
    return sealed


def _validate_history_ledger(
    raw: dict[str, Any],
    *,
    codes: list[str] | None,
) -> dict[str, Any]:
    stored_codes = raw.get("requested_codes")
    if (
        not isinstance(stored_codes, list)
        or not stored_codes
        or any(
            not isinstance(code, str)
            or re.fullmatch(r"\d{6}", code) is None
            for code in stored_codes
        )
        or len(stored_codes) != len(set(stored_codes))
    ):
        raise ValueError("history ledger requested code universe is invalid")
    if codes is not None and list(codes) != stored_codes:
        raise ValueError("history ledger requested code universe differs")
    codes = list(stored_codes)
    try:
        generation = int(raw.get("generation", 1))
        inherited_entry_count = int(raw.get("inherited_entry_count", 0))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("history ledger generation counters are invalid") from exc
    parent_ledger_sha256 = raw.get("parent_ledger_sha256")
    if (
        isinstance(raw.get("generation"), bool)
        or isinstance(raw.get("inherited_entry_count"), bool)
        or generation < 1
        or inherited_entry_count < 0
        or inherited_entry_count > len(codes)
        or (
            generation == 1
            and (
                inherited_entry_count != 0
                or parent_ledger_sha256 not in {None, ""}
            )
        )
        or (
            generation > 1
            and (
                inherited_entry_count <= 0
                or re.fullmatch(
                    r"[0-9a-f]{64}", str(parent_ledger_sha256 or "")
                )
                is None
            )
        )
    ):
        raise ValueError("history ledger generation contract is invalid")
    expected_contract = _history_batch_contract(codes)
    unsigned = dict(raw)
    actual_ledger_hash = str(unsigned.pop("ledger_sha256", ""))
    if actual_ledger_hash != _sha256(unsigned):
        raise ValueError("history ledger checksum differs")
    for key, expected in expected_contract.items():
        if raw.get(key) != expected:
            raise ValueError(f"history ledger batch contract differs: {key}")
    if raw.get("batch_id") != _sha256(expected_contract):
        raise ValueError("history ledger batch identity differs")
    entries = raw.get("completed_entries")
    if not isinstance(entries, list) or any(
        not isinstance(entry, dict) for entry in entries
    ):
        raise ValueError("history ledger entries are invalid")
    try:
        next_offset = int(raw.get("next_offset"))
        completed_count = int(raw.get("completed_code_count"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("history ledger progress counters are invalid") from exc
    if (
        isinstance(raw.get("next_offset"), bool)
        or isinstance(raw.get("completed_code_count"), bool)
        or next_offset != completed_count
        or completed_count != len(entries)
        or not 0 <= next_offset <= len(codes)
        or completed_count < inherited_entry_count
    ):
        raise ValueError("history ledger progress differs from entries")
    entry_codes: list[str] = []
    for index, entry in enumerate(entries):
        code = str(entry.get("stock_code") or "").strip().zfill(6)
        if code != codes[index]:
            raise ValueError("history ledger is not the authoritative code prefix")
        try:
            total_hits = int(entry.get("total_hits"))
            written = int(entry.get("written_count"))
            persisted = int(entry.get("persisted_count"))
            page_count = int(entry.get("page_count"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("history ledger entry counters are invalid") from exc
        if (
            total_hits < 0
            or written != total_hits
            or persisted != total_hits
            or page_count < 1
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(entry.get("source_row_hash") or ""),
            )
            or entry.get("persisted_row_hash") != entry.get("source_row_hash")
            or not str(entry.get("captured_at") or "")
        ):
            raise ValueError("history ledger entry evidence is invalid")
        entry_codes.append(code)
    if raw.get("completed_code_set_hash") != _code_set_hash(entry_codes):
        raise ValueError("history ledger completed code hash differs")
    if raw.get("evidence_chain_sha256") != _history_entry_chain(entries):
        raise ValueError("history ledger evidence chain differs")
    expected_status = "COMPLETE" if next_offset == len(codes) else "PROGRESS"
    if raw.get("status") != expected_status:
        raise ValueError("history ledger completion status differs")
    if expected_status == "COMPLETE" and not str(raw.get("completed_at") or ""):
        raise ValueError("completed history ledger lacks completion time")
    if expected_status == "PROGRESS" and raw.get("completed_at") is not None:
        raise ValueError("incomplete history ledger has a completion time")
    return dict(raw)


def _read_history_ledger(
    path: Path,
    *,
    codes: list[str] | None,
) -> dict[str, Any]:
    _validate_history_ledger_location(path)
    if not path.is_file() or path.is_symlink():
        raise ValueError("history ledger is missing or is a symbolic link")
    raw_text = path.read_text(encoding="utf-8")
    if not raw_text or len(raw_text.encode("utf-8")) > 16 * 1024 * 1024:
        raise ValueError("history ledger size is invalid")
    parsed = json.loads(raw_text, object_pairs_hook=_strict_json_object)
    if not isinstance(parsed, dict):
        raise ValueError("history ledger root must be an object")
    return _validate_history_ledger(parsed, codes=codes)


def _create_history_ledger_exclusive(
    path: Path,
    ledger: dict[str, Any],
) -> dict[str, Any]:
    """Create a generation once; a concurrent winner is loaded, never replaced."""

    _validate_history_ledger_location(path)
    sealed = _sealed_ledger(ledger)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.create"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            if os.name != "nt":
                os.chmod(temporary, 0o600)
            handle.write(_canonical_json(sealed))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
            created = True
        except FileExistsError:
            created = False
        if os.name != "nt":
            directory_fd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        return sealed if created else _read_history_ledger(path, codes=None)
    finally:
        if temporary.exists():
            temporary.unlink()


def _history_generation_path(base_path: Path, codes: list[str]) -> Path:
    code_hash = _code_set_hash(codes)
    return base_path.with_name(
        f"{base_path.stem}{_HISTORY_GENERATION_MARKER}{code_hash}"
        f"{base_path.suffix}"
    )


def _history_generation_paths(base_path: Path) -> list[Path]:
    _validate_history_ledger_location(base_path)
    paths = [base_path] if base_path.exists() else []
    prefix = f"{base_path.stem}{_HISTORY_GENERATION_MARKER}"
    suffix = base_path.suffix
    for candidate in base_path.parent.iterdir():
        name = candidate.name
        if not name.startswith(prefix) or suffix and not name.endswith(suffix):
            continue
        token_end = len(name) - len(suffix) if suffix else len(name)
        token = name[len(prefix) : token_end]
        if re.fullmatch(r"[0-9a-f]{64}", token) is None:
            continue
        paths.append(candidate)
    if len(paths) > 1024:
        raise ValueError("history ledger generation count exceeds safety limit")
    return sorted(set(paths), key=lambda item: item.name)


def _load_history_generations(
    base_path: Path,
) -> list[tuple[Path, dict[str, Any]]]:
    loaded = [
        (path, _read_history_ledger(path, codes=None))
        for path in _history_generation_paths(base_path)
    ]
    if not loaded:
        return []
    by_hash: dict[str, tuple[Path, dict[str, Any]]] = {}
    generations: dict[int, str] = {}
    for path, ledger in loaded:
        ledger_hash = str(ledger["ledger_sha256"])
        generation = int(ledger.get("generation") or 1)
        if ledger_hash in by_hash:
            raise ValueError("history ledger generation hash is duplicated")
        if generation in generations and generations[generation] != ledger_hash:
            raise ValueError("history ledger generation number is forked")
        by_hash[ledger_hash] = (path, ledger)
        generations[generation] = ledger_hash
    for _path, ledger in loaded:
        generation = int(ledger.get("generation") or 1)
        if generation == 1:
            continue
        parent_hash = str(ledger.get("parent_ledger_sha256") or "")
        parent_item = by_hash.get(parent_hash)
        if parent_item is None:
            raise ValueError("history ledger generation parent is missing")
        parent = parent_item[1]
        inherited_count = int(ledger.get("inherited_entry_count") or 0)
        parent_codes = list(parent["requested_codes"])
        parent_entries = list(parent["completed_entries"])
        if (
            parent.get("status") != "COMPLETE"
            or int(parent.get("generation") or 1) + 1 != generation
            or inherited_count != len(parent_codes)
            or list(ledger["requested_codes"][:inherited_count]) != parent_codes
            or list(ledger["completed_entries"][:inherited_count]) != parent_entries
        ):
            raise ValueError("history ledger inherited generation differs from parent")
        parent_completed = datetime.fromisoformat(str(parent["completed_at"]))
        child_created = datetime.fromisoformat(str(ledger["created_at"]))
        if parent_completed > child_created:
            raise ValueError("history ledger generation predates its parent")
    return sorted(
        loaded,
        key=lambda item: (int(item[1].get("generation") or 1), item[0].name),
    )


def _select_or_create_history_generation(
    base_path: Path,
    *,
    current_codes: list[str],
    now: datetime,
) -> tuple[Path, dict[str, Any]]:
    normalized = sorted({str(code).strip().zfill(6) for code in current_codes})
    if (
        not normalized
        or any(re.fullmatch(r"\d{6}", code) is None for code in normalized)
    ):
        raise ValueError("history generation current universe is invalid")
    generations = _load_history_generations(base_path)
    if not generations:
        ledger = _create_history_ledger_exclusive(
            base_path,
            _new_history_ledger(normalized, now=now),
        )
        return base_path, _validate_history_ledger(ledger, codes=normalized)

    progressing = [item for item in generations if item[1].get("status") == "PROGRESS"]
    if len(progressing) > 1:
        raise ValueError("multiple history ledger generations are in progress")
    if progressing:
        return progressing[0]

    parent_path, parent = max(
        generations,
        key=lambda item: int(item[1].get("generation") or 1),
    )
    del parent_path
    parent_codes = [str(code) for code in parent["requested_codes"]]
    missing = sorted(set(normalized) - set(parent_codes))
    if not missing:
        return next(
            item for item in generations
            if item[1]["ledger_sha256"] == parent["ledger_sha256"]
        )
    generation_codes = [*parent_codes, *missing]
    generation_path = _history_generation_path(base_path, generation_codes)
    ledger = _create_history_ledger_exclusive(
        generation_path,
        _new_history_generation_ledger(parent, generation_codes, now=now),
    )
    refreshed = _load_history_generations(base_path)
    for path, candidate in refreshed:
        if path == generation_path:
            return path, candidate
    raise ValueError("created history ledger generation is unavailable")


def _load_or_create_history_ledger(
    path: Path,
    *,
    codes: list[str] | None,
    now: datetime,
) -> dict[str, Any]:
    _validate_history_ledger_location(path)
    if path.exists():
        return _read_history_ledger(path, codes=codes)
    if not codes:
        raise ValueError("a new history ledger requires the frozen union universe")
    return _create_history_ledger_exclusive(
        path,
        _new_history_ledger(codes, now=now),
    )


def _history_repair_result(
    *,
    started_at: datetime,
    finished_at: datetime,
    codes: list[str],
    ledger: dict[str, Any],
    processed_this_run: int,
    failed_code: str = "",
    failure_type: str = "",
    failure_retryable: bool = True,
) -> dict[str, Any]:
    completed = int(ledger["completed_code_count"])
    status = (
        "DATA_BLOCKED"
        if failed_code
        else ("PASS" if ledger.get("status") == "COMPLETE" else "PROGRESS")
    )
    receipt = {
        "schema": HISTORY_RESULT_SCHEMA,
        "receipt_id": uuid.uuid4().hex,
        "status": status,
        "task_type": HISTORY_TASK_TYPE,
        "dataset": HISTORY_DATASET,
        "executor_owner": "linux_provider",
        "provider": NOTICE_PROVIDER_ID,
        "retryable": bool(
            status == "PROGRESS"
            or (status == "DATA_BLOCKED" and failure_retryable)
        ),
        "batch_id": ledger["batch_id"],
        "ledger_generation": int(ledger.get("generation") or 1),
        "parent_ledger_sha256": ledger.get("parent_ledger_sha256"),
        "inherited_entry_count": int(ledger.get("inherited_entry_count") or 0),
        "started_at": started_at.isoformat(sep=" ", timespec="microseconds"),
        "finished_at": finished_at.isoformat(sep=" ", timespec="microseconds"),
        "requested_code_count": len(codes),
        "requested_code_set_hash": _code_set_hash(codes),
        "ordered_code_sha256": _ordered_code_hash(codes),
        "completed_code_count": completed,
        "completed_code_set_hash": ledger["completed_code_set_hash"],
        "remaining_code_count": len(codes) - completed,
        "processed_code_count_this_run": int(processed_this_run),
        "failed_code": failed_code,
        "failure_type": failure_type,
        "ledger_status": ledger["status"],
        "ledger_schema": HISTORY_LEDGER_SCHEMA,
        "ledger_sha256": ledger["ledger_sha256"],
        "evidence_chain_sha256": ledger["evidence_chain_sha256"],
        "pagination_evidence": "eastmoney_exact_stock_total_hits_v1",
        "replacement_scope": "one_stock_full_history",
        "universe_evidence": (
            "si_all_code_union_existing_notice_stock_code_frozen_v1"
        ),
    }
    receipt["result_sha256"] = _sha256(receipt)
    return receipt


def _history_error_retryable(error: BaseException) -> bool:
    message = str(error).lower()
    terminal_markers = (
        "ledger checksum",
        "ledger batch contract",
        "ledger batch identity",
        "ledger requested code universe",
        "ledger progress",
        "ledger entries",
        "ledger evidence",
        "ledger completion status",
        "ledger generation",
        "history generation",
        "multiple history ledger generations",
        "history complete ledger is immutable",
        "ledger path must be absolute",
        "ledger parent directory",
        "ledger cannot be a symbolic link",
        "ledger protected root",
        "ledger must be directly inside",
        "ledger file permissions are unsafe",
        "production history ledger requires",
        "prepared unique key",
        "requires prepared unique key",
        "validated association provenance",
        "canonical qmt identity",
        "stock universe is invalid or duplicated",
    )
    return not any(marker in message for marker in terminal_markers)


def _history_failure_result(
    *,
    started_at: datetime,
    finished_at: datetime,
    error: BaseException,
    codes: list[str] | None = None,
) -> dict[str, Any]:
    requested = list(codes or [])
    receipt = {
        "schema": HISTORY_RESULT_SCHEMA,
        "receipt_id": uuid.uuid4().hex,
        "status": "DATA_BLOCKED",
        "task_type": HISTORY_TASK_TYPE,
        "dataset": HISTORY_DATASET,
        "executor_owner": "linux_provider",
        "provider": NOTICE_PROVIDER_ID,
        "retryable": _history_error_retryable(error),
        "started_at": started_at.isoformat(sep=" ", timespec="microseconds"),
        "finished_at": finished_at.isoformat(sep=" ", timespec="microseconds"),
        "batch_id": "",
        "ledger_generation": 0,
        "parent_ledger_sha256": None,
        "inherited_entry_count": 0,
        "requested_code_count": len(requested),
        "requested_code_set_hash": _code_set_hash(requested),
        "ordered_code_sha256": _ordered_code_hash(requested),
        "completed_code_count": 0,
        "completed_code_set_hash": _code_set_hash([]),
        "remaining_code_count": len(requested),
        "processed_code_count_this_run": 0,
        "failed_code": "",
        "failure_type": type(error).__name__,
        "error": str(error)[:1000],
        "ledger_status": "UNAVAILABLE",
        "ledger_schema": HISTORY_LEDGER_SCHEMA,
        "ledger_sha256": "",
        "evidence_chain_sha256": "",
        "pagination_evidence": "eastmoney_exact_stock_total_hits_v1",
        "replacement_scope": "one_stock_full_history",
        "universe_evidence": (
            "si_all_code_union_existing_notice_stock_code_frozen_v1"
        ),
    }
    receipt["result_sha256"] = _sha256(receipt)
    return receipt


def _notice_sync_result(
    *,
    started_at: datetime,
    finished_at: datetime,
    codes: list[str],
    succeeded_codes: list[str],
    nonempty_codes: list[str],
    failed_codes: list[str],
    failure_sample: list[dict[str, str]],
    written_rows: int,
    minimum_coverage: float,
    minimum_row_coverage: float,
    request_window_start: date | None = None,
    request_window_end: date | None = None,
    replaced_existing_rows: int = 0,
    source_manifest: list[dict[str, Any]] | None = None,
    persisted_manifest: list[dict[str, Any]] | None = None,
    batch_id: str = "",
) -> dict[str, Any]:
    requested = len(codes)
    succeeded = len(succeeded_codes)
    nonempty = len(nonempty_codes)
    failed = len(failed_codes)
    empty_codes = sorted(set(succeeded_codes) - set(nonempty_codes))
    coverage = _coverage_ratio(succeeded, requested)
    row_coverage = _coverage_ratio(nonempty, requested)
    passed = bool(
        requested > 0
        and succeeded + failed == requested
        and coverage >= minimum_coverage
        and row_coverage >= minimum_row_coverage
    )
    receipt = {
        "schema": "probiga.notice-sync-result.v1",
        "receipt_id": uuid.uuid4().hex,
        "status": "PASS" if passed else "DATA_BLOCKED",
        "started_at": started_at.isoformat(sep=" ", timespec="microseconds"),
        "finished_at": finished_at.isoformat(sep=" ", timespec="microseconds"),
        "requested_code_count": requested,
        "requested_code_set_hash": _code_set_hash(codes),
        "succeeded_code_count": succeeded,
        "succeeded_code_set_hash": _code_set_hash(succeeded_codes),
        "nonempty_code_count": nonempty,
        "nonempty_code_set_hash": _code_set_hash(nonempty_codes),
        "authoritative_empty_code_count": len(empty_codes),
        "authoritative_empty_code_set_hash": _code_set_hash(empty_codes),
        "failed_code_count": failed,
        "failed_code_set_hash": _code_set_hash(failed_codes),
        "pagination_exhausted_code_count": succeeded,
        "pagination_exhausted_code_set_hash": _code_set_hash(
            succeeded_codes
        ),
        "failure_sample": failure_sample[:20],
        "written_notice_count": int(written_rows),
        "replaced_existing_notice_count": int(replaced_existing_rows),
        "request_coverage": coverage,
        "row_coverage": row_coverage,
        "minimum_request_coverage": float(minimum_coverage),
        "minimum_row_coverage": float(minimum_row_coverage),
        "pagination_evidence": "eastmoney_exact_stock_total_hits_v1",
        "empty_result_evidence": "eastmoney_exact_stock_total_hits_v1",
        "sync_mode": "incremental",
        "request_window_start": (
            request_window_start.isoformat()
            if request_window_start is not None
            else None
        ),
        "request_window_end": (
            request_window_end.isoformat()
            if request_window_end is not None
            else None
        ),
        "request_scope_evidence": "eastmoney_stock_date_window_v1",
        "source_manifest_sha256": _sha256(source_manifest or []),
        "persisted_manifest_sha256": _sha256(
            sorted(
                persisted_manifest or [],
                key=lambda item: str(item.get("stock_code") or ""),
            )
        ),
        "batch_id": batch_id,
        "association_validated": 1,
        "data_source": NOTICE_PROVIDER_ID,
        "data_version": NOTICE_DATA_VERSION,
        "quality_status": NOTICE_QUALITY_STATUS,
        "permission_status": NOTICE_PERMISSION_STATUS,
    }
    receipt["result_sha256"] = _sha256(receipt)
    return receipt


def _run_incremental(
    *,
    engine: Engine,
    codes: list[str],
    started_at: datetime,
    window_start: date,
    window_end: date,
    page_size: int,
    max_pages: int,
    sleep_seconds: float,
    minimum_coverage: float,
    minimum_row_coverage: float,
) -> int:
    total_items = 0
    replaced_existing = 0
    succeeded_codes: list[str] = []
    nonempty_codes: list[str] = []
    failed_codes: list[str] = []
    failure_sample: list[dict[str, str]] = []
    source_manifest: list[dict[str, Any]] = []
    persisted_manifest: list[dict[str, Any]] = []
    batch_id = _sha256(
        {
            "mode": "incremental",
            "started_at": started_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "requested_code_set_hash": _code_set_hash(codes),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        }
    )
    with httpx.Client(
        headers={"User-Agent": "Mozilla/5.0 ProBigA-notice-sync"},
        trust_env=False,
    ) as client:
        for index, code in enumerate(codes):
            try:
                fetch = fetch_pages(
                    client,
                    code,
                    page_size=page_size,
                    max_pages=max_pages,
                    begin_date=window_start,
                    end_date=window_end,
                )
                if (
                    not fetch.bounded
                    or fetch.window_start != window_start
                    or fetch.window_end != window_end
                    or not fetch.exhausted
                    or len(fetch.rows) != fetch.total_hits
                ):
                    raise RuntimeError(
                        f"{code} notice response lacks exact bounded pagination proof"
                    )
                rows = [
                    _parse_item(
                        code,
                        item,
                        fetch.captured_at,
                        validated_stock_identity=True,
                        batch_id=batch_id,
                    )
                    for item in fetch.rows
                ]
                source_hash = _notice_row_hash(rows)
                persisted = reconcile_rows(
                    engine,
                    rows,
                    stock_code=code,
                    captured_at=fetch.captured_at,
                    window_start=window_start,
                    window_end=window_end,
                )
                if (
                    persisted.written_count != len(rows)
                    or persisted.persisted_count != len(rows)
                    or persisted.persisted_row_hash != source_hash
                ):
                    raise RuntimeError(
                        f"{code} notice persisted proof differs from source"
                    )
                total_items += persisted.written_count
                replaced_existing += persisted.deleted_count
                succeeded_codes.append(code)
                if rows:
                    nonempty_codes.append(code)
                source_manifest.append(
                    {
                        "stock_code": code,
                        "total_hits": fetch.total_hits,
                        "page_count": fetch.page_count,
                        "row_hash": source_hash,
                    }
                )
                persisted_manifest.append(
                    {
                        "stock_code": code,
                        "row_count": persisted.persisted_count,
                        "row_hash": persisted.persisted_row_hash,
                    }
                )
                logger.info(
                    "%s/%s %s：日期窗精确返回 %s 条，替换旧行 %s 条",
                    index + 1,
                    len(codes),
                    code,
                    len(rows),
                    persisted.deleted_count,
                )
            except Exception as exc:  # noqa: BLE001
                failed_codes.append(code)
                failure_sample.append(
                    {"stock_code": code, "error_type": type(exc).__name__}
                )
                logger.warning("%s 失败：%s", code, exc)
            if index + 1 < len(codes):
                time.sleep(max(0.0, sleep_seconds))

    coverage = _coverage_ratio(len(succeeded_codes), len(codes))
    row_coverage = _coverage_ratio(len(nonempty_codes), len(codes))
    logger.info(
        "Notice incremental sync completed: stocks=%s succeeded=%s failed=%s "
        "empty=%s nonempty=%s rows=%s replaced=%s coverage=%.1f%%",
        len(codes),
        len(succeeded_codes),
        len(failed_codes),
        len(succeeded_codes) - len(nonempty_codes),
        len(nonempty_codes),
        total_items,
        replaced_existing,
        coverage * 100.0,
    )
    result = _notice_sync_result(
        started_at=started_at,
        finished_at=_shanghai_now(),
        codes=codes,
        succeeded_codes=succeeded_codes,
        nonempty_codes=nonempty_codes,
        failed_codes=failed_codes,
        failure_sample=failure_sample,
        written_rows=total_items,
        minimum_coverage=minimum_coverage,
        minimum_row_coverage=minimum_row_coverage,
        request_window_start=window_start,
        request_window_end=window_end,
        replaced_existing_rows=replaced_existing,
        source_manifest=source_manifest,
        persisted_manifest=persisted_manifest,
        batch_id=batch_id,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if coverage < minimum_coverage or row_coverage < minimum_row_coverage:
        logger.error(
            "DATA_BLOCKED: notice coverage below threshold: "
            "coverage=%.6f minimum=%.6f row_coverage=%.6f row_minimum=%.6f",
            coverage,
            minimum_coverage,
            row_coverage,
            minimum_row_coverage,
        )
        return 1
    return 0


def _run_history_repair(
    *,
    engine: Engine,
    codes: list[str],
    started_at: datetime,
    ledger_path: Path,
    shard_size: int,
    page_size: int,
    max_pages: int,
    sleep_seconds: float,
) -> int:
    ledger = _load_or_create_history_ledger(
        ledger_path,
        codes=codes,
        now=started_at,
    )
    if ledger.get("status") == "COMPLETE":
        result = _history_repair_result(
            started_at=started_at,
            finished_at=_shanghai_now(),
            codes=codes,
            ledger=ledger,
            processed_this_run=0,
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0

    initial_offset = int(ledger["next_offset"])
    stop_offset = min(len(codes), initial_offset + shard_size)
    processed_this_run = 0
    with httpx.Client(
        headers={"User-Agent": "Mozilla/5.0 ProBigA-notice-history-repair"},
        trust_env=False,
    ) as client:
        for offset in range(initial_offset, stop_offset):
            code = codes[offset]
            try:
                fetch = fetch_pages(
                    client,
                    code,
                    page_size=page_size,
                    max_pages=max_pages,
                )
                if (
                    fetch.bounded
                    or not fetch.exhausted
                    or len(fetch.rows) != fetch.total_hits
                ):
                    raise RuntimeError(
                        f"{code} notice response lacks exact full-history proof"
                    )
                rows = [
                    _parse_item(
                        code,
                        item,
                        fetch.captured_at,
                        validated_stock_identity=True,
                        batch_id=str(ledger["batch_id"]),
                    )
                    for item in fetch.rows
                ]
                source_hash = _notice_row_hash(rows)
                persisted = reconcile_rows(
                    engine,
                    rows,
                    stock_code=code,
                    captured_at=fetch.captured_at,
                    full_history=True,
                )
                if (
                    persisted.written_count != fetch.total_hits
                    or persisted.persisted_count != fetch.total_hits
                    or persisted.persisted_row_hash != source_hash
                ):
                    raise RuntimeError(
                        f"{code} notice historical persisted proof differs"
                    )
                entry = {
                    "stock_code": code,
                    "captured_at": fetch.captured_at.isoformat(
                        sep=" ", timespec="microseconds"
                    ),
                    "total_hits": fetch.total_hits,
                    "page_count": fetch.page_count,
                    "written_count": persisted.written_count,
                    "deleted_count": persisted.deleted_count,
                    "persisted_count": persisted.persisted_count,
                    "source_row_hash": source_hash,
                    "persisted_row_hash": persisted.persisted_row_hash,
                }
                entries = [*ledger["completed_entries"], entry]
                completed_codes = [
                    str(item["stock_code"]) for item in entries
                ]
                now = _shanghai_now()
                next_offset = offset + 1
                ledger = {
                    **ledger,
                    "status": (
                        "COMPLETE" if next_offset == len(codes) else "PROGRESS"
                    ),
                    "updated_at": now.isoformat(
                        sep=" ", timespec="microseconds"
                    ),
                    "completed_at": (
                        now.isoformat(sep=" ", timespec="microseconds")
                        if next_offset == len(codes)
                        else None
                    ),
                    "next_offset": next_offset,
                    "completed_code_count": next_offset,
                    "completed_code_set_hash": _code_set_hash(completed_codes),
                    "completed_entries": entries,
                    "evidence_chain_sha256": _history_entry_chain(entries),
                    "last_failure": None,
                }
                ledger = _atomic_write_history_ledger(ledger_path, ledger)
                ledger = _validate_history_ledger(ledger, codes=codes)
                processed_this_run += 1
                logger.info(
                    "历史修复 %s/%s %s：精确写入 %s 条，移除旧行 %s 条",
                    next_offset,
                    len(codes),
                    code,
                    persisted.written_count,
                    persisted.deleted_count,
                )
            except Exception as exc:  # noqa: BLE001
                now = _shanghai_now()
                retryable = _history_error_retryable(exc)
                ledger = {
                    **ledger,
                    "updated_at": now.isoformat(
                        sep=" ", timespec="microseconds"
                    ),
                    "last_failure": {
                        "stock_code": code,
                        "error_type": type(exc).__name__,
                        "retryable": retryable,
                        "failed_at": now.isoformat(
                            sep=" ", timespec="microseconds"
                        ),
                    },
                }
                ledger = _atomic_write_history_ledger(ledger_path, ledger)
                result = _history_repair_result(
                    started_at=started_at,
                    finished_at=now,
                    codes=codes,
                    ledger=ledger,
                    processed_this_run=processed_this_run,
                    failed_code=code,
                    failure_type=type(exc).__name__,
                    failure_retryable=retryable,
                )
                print(json.dumps(result, ensure_ascii=False, sort_keys=True))
                logger.error(
                    "DATA_BLOCKED: notice historical repair stopped at %s: %s",
                    code,
                    exc,
                )
                return 2
            if offset + 1 < stop_offset:
                time.sleep(max(0.0, sleep_seconds))

    result = _history_repair_result(
        started_at=started_at,
        finished_at=_shanghai_now(),
        codes=codes,
        ledger=ledger,
        processed_this_run=processed_this_run,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if ledger.get("status") == "COMPLETE" else 2


def _run_history_cli(
    args: argparse.Namespace,
    *,
    started_at: datetime,
    ledger_path: Path,
) -> int:
    codes: list[str] = []
    try:
        engine = create_batch_engine()
        if not args.skip_ddl:
            run_ddl(engine)
        current_codes = read_history_repair_codes(engine)
        active_path, active_ledger = _select_or_create_history_generation(
            ledger_path,
            current_codes=current_codes,
            now=started_at,
        )
        codes = [str(code).strip().zfill(6) for code in active_ledger["requested_codes"]]
        if (
            not codes
            or len(codes) != len(set(codes))
            or any(re.fullmatch(r"\d{6}", code) is None for code in codes)
        ):
            raise RuntimeError(
                "notice historical stock universe is invalid or duplicated"
            )
        return _run_history_repair(
            engine=engine,
            codes=codes,
            started_at=started_at,
            ledger_path=active_path,
            shard_size=int(args.history_shard_size),
            page_size=max(5, min(100, int(args.page_size))),
            max_pages=max(1, int(args.max_pages)),
            sleep_seconds=max(0.0, float(args.sleep)),
        )
    except Exception as exc:  # noqa: BLE001
        finished_at = _shanghai_now()
        result = _history_failure_result(
            started_at=started_at,
            finished_at=finished_at,
            error=exc,
            codes=codes,
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        logger.error(
            "DATA_BLOCKED: notice historical repair preflight failed: %s",
            exc,
        )
        return 2


def main(argv: list[str] | None = None) -> int:
    if httpx is None:
        raise RuntimeError("httpx is required to synchronize Eastmoney notices")
    p = argparse.ArgumentParser(description="东财个股公告 → si_notice_eastmoney")
    p.add_argument(
        "--mode",
        choices=("incremental", "historical-repair"),
        default="incremental",
        help="日常日期窗精确同步，或一次性可恢复全历史修复",
    )
    p.add_argument("--stock", type=str, default="", help="单只股票 6 位代码")
    p.add_argument("--from-si-all-code", action="store_true", help="从 si_all_code 按顺序批量拉取")
    p.add_argument("--offset", type=int, default=0)
    p.add_argument(
        "--limit",
        type=int,
        default=50,
        help="配合 --from-si-all-code；0 表示从 offset 开始的完整股票池",
    )
    p.add_argument("--page-size", type=int, default=100, help="每页条数，默认 100")
    p.add_argument(
        "--max-pages",
        type=int,
        default=1000,
        help="每只股票安全页数上限；总页数超过上限时整只股票失败",
    )
    p.add_argument("--sleep", type=float, default=0.3, help="股票间隔秒数")
    p.add_argument(
        "--lookback-days",
        type=int,
        default=45,
        help="incremental 模式按上海日回看的自然日数",
    )
    p.add_argument(
        "--forward-days",
        type=int,
        default=1,
        help="incremental 模式覆盖东财次日公告标签的前瞻自然日数",
    )
    p.add_argument(
        "--as-of-date",
        default="",
        help="incremental 模式基准上海日期 YYYY-MM-DD；默认当天",
    )
    p.add_argument(
        "--history-state-file",
        default="",
        help="historical-repair 必填的绝对 checkpoint ledger 路径",
    )
    p.add_argument(
        "--history-shard-size",
        type=int,
        default=25,
        help="historical-repair 每次最多完成的连续股票数",
    )
    p.add_argument(
        "--min-coverage",
        type=float,
        default=1.0,
        help="成功请求股票数/目标股票数下限，范围 0..1",
    )
    p.add_argument(
        "--min-row-coverage",
        type=float,
        default=0.0,
        help="返回至少一条公告的股票数/目标股票数下限，范围 0..1",
    )
    p.add_argument(
        "--skip-ddl",
        action="store_true",
        help="跳过预置表只读合同检查（不会执行 runtime DDL）",
    )
    args = p.parse_args(argv)

    if args.offset < 0:
        p.error("--offset cannot be negative")
    if args.limit < 0:
        p.error("--limit cannot be negative")
    if args.lookback_days < 0:
        p.error("--lookback-days cannot be negative")
    if args.forward_days < 0:
        p.error("--forward-days cannot be negative")
    if args.history_shard_size <= 0:
        p.error("--history-shard-size must be positive")
    if not 0.0 <= args.min_coverage <= 1.0:
        p.error("--min-coverage must be between 0 and 1")
    if not 0.0 <= args.min_row_coverage <= 1.0:
        p.error("--min-row-coverage must be between 0 and 1")

    stock = args.stock.strip()
    if not stock and not args.from_si_all_code:
        p.print_help()
        print("\n请指定 --stock 或 --from-si-all-code", file=sys.stderr)
        return 2
    if stock and re.fullmatch(r"\d{6}", stock) is None:
        p.error("--stock must be one exact six-digit code")
    if args.mode == "historical-repair":
        if stock or not args.from_si_all_code or args.offset != 0 or args.limit != 0:
            p.error(
                "historical-repair requires the complete --from-si-all-code "
                "universe with --offset 0 --limit 0"
            )
        if not args.history_state_file:
            p.error("historical-repair requires --history-state-file")
        ledger_path = Path(args.history_state_file)
        if not ledger_path.is_absolute():
            p.error("--history-state-file must be an absolute path")
    else:
        ledger_path = None
        if args.history_state_file:
            p.error("--history-state-file is only valid in historical-repair mode")

    if args.as_of_date:
        try:
            as_of_date = date.fromisoformat(args.as_of_date)
        except ValueError:
            p.error("--as-of-date must be YYYY-MM-DD")
    else:
        as_of_date = _shanghai_now().date()

    started_at = _shanghai_now()
    if args.mode == "historical-repair":
        assert ledger_path is not None
        return _run_history_cli(
            args,
            started_at=started_at,
            ledger_path=ledger_path,
        )

    engine = create_batch_engine()
    if not args.skip_ddl:
        run_ddl(engine)

    codes: list[str] = []
    if stock:
        codes = [stock]
    else:
        codes = read_codes_from_db(engine, args.offset, args.limit)
    codes = [str(code).strip().zfill(6) for code in codes]
    if (
        len(codes) != len(set(codes))
        or any(re.fullmatch(r"\d{6}", code) is None for code in codes)
    ):
        logger.error("DATA_BLOCKED: notice stock universe is invalid or duplicated")
        return 2
    if not codes:
        logger.error("DATA_BLOCKED: notice stock universe is empty")
        return 2
    page_size = max(5, min(100, int(args.page_size)))
    max_pages = max(1, int(args.max_pages))
    sleep_seconds = max(0.0, float(args.sleep))
    window_start = as_of_date - timedelta(days=int(args.lookback_days))
    window_end = as_of_date + timedelta(days=int(args.forward_days))
    return _run_incremental(
        engine=engine,
        codes=codes,
        started_at=started_at,
        window_start=window_start,
        window_end=window_end,
        page_size=page_size,
        max_pages=max_pages,
        sleep_seconds=sleep_seconds,
        minimum_coverage=float(args.min_coverage),
        minimum_row_coverage=float(args.min_row_coverage),
    )


if __name__ == "__main__":
    raise SystemExit(main())

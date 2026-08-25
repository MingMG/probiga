# -*- coding: utf-8 -*-
"""
东财「个股公告列表」拉取并写入 MySQL ``probiga.si_notice_eastmoney``（标题级，非 PDF 正文）。

该表只服务页面展示，不再写入策略 PIT 事实或覆盖凭证。策略唯一权威
公告源是官方 QMT ``announcement`` 全市场原子批次；QMT 不可用时策略
必须 ``DATA_BLOCKED``，不得回退到本脚本的轮转子集。

前置::

  pip install -r requirements-platform.txt

执行（仓库根）::

  python -m biz.notice.sync_notice_em --stock 600519 --max-pages 5
  python -m biz.notice.sync_notice_em --from-si-all-code --offset 0 --limit 100 --max-pages 2 --sleep 0.35

环境变量：
  MYSQL_URL  必填，MySQL 连接串；也可写入项目根目录 ``.env``
说明：
  - 接口为 ``https://np-anotice-stock.eastmoney.com/api/security/ann``，需网络；请控制 ``--limit``、``--sleep``，避免封 IP。
  - ``art_code`` 全局唯一，重复运行同一公告为 upsert 更新。
  - 详情链接为构造 URL，若东财改版请以列表页为准：``https://data.eastmoney.com/notices/stock/{code}.html``
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import logging
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    import httpx
except ModuleNotFoundError:  # allows schema/parser tests without network extras
    httpx = None  # type: ignore[assignment]
from sqlalchemy import text
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

DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_si_notice_eastmoney.sql"


@dataclass(frozen=True)
class NoticeFetchResult:
    rows: list[dict[str, Any]]
    captured_at: datetime
    window_start: date
    exhausted: bool
    page_count: int

UPSERT_SQL = text(
    """
    INSERT INTO si_notice_eastmoney (
        stock_code, art_code, notice_date, title, column_name, display_time, detail_url, etl_sync_at
    ) VALUES (
        :stock_code, :art_code, :notice_date, :title, :column_name, :display_time, :detail_url, :etl_sync_at
    )
    ON DUPLICATE KEY UPDATE
        notice_date = VALUES(notice_date),
        title = VALUES(title),
        column_name = VALUES(column_name),
        display_time = VALUES(display_time),
        detail_url = VALUES(detail_url),
        etl_sync_at = VALUES(etl_sync_at)
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
                "etl_sync_at",
            },
        },
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


def _parse_item(stock_code: str, item: dict[str, Any], etl: datetime) -> dict[str, Any]:
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
    return {
        "stock_code": str(stock_code).strip().zfill(6),
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
        "etl_sync_at": etl,
    }


def fetch_pages(
    client: httpx.Client,
    stock_code: str,
    *,
    page_size: int,
    max_pages: int,
) -> NoticeFetchResult:
    code = str(stock_code).strip().zfill(6)
    out: list[dict[str, Any]] = []
    exhausted = False
    page_count = 0
    for page in range(1, max_pages + 1):
        url = (
            "https://np-anotice-stock.eastmoney.com/api/security/ann?sr=-1"
            f"&page_size={page_size}&page_index={page}&client_source=web&stock_list={code}"
        )
        last_error: Exception | None = None
        response: httpx.Response | None = None
        for attempt in range(3):
            try:
                response = client.get(url, timeout=20.0)
                response.raise_for_status()
                break
            except httpx.HTTPError as e:
                last_error = e
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if response is None:
            raise last_error or RuntimeError("notice source returned no response")
        data = response.json()
        if not data.get("success"):
            raise RuntimeError(
                f"{code} 第 {page} 页 source success!=1: {data.get('error')}"
            )
        page_count = page
        lst = (data.get("data") or {}).get("list") or []
        if not lst:
            exhausted = True
            break
        out.extend(lst)
        if len(lst) < page_size:
            exhausted = True
            break
    captured_at = datetime.now()
    parsed_dates = [
        value.date()
        for value in (_event_date_from_item(item) for item in out)
        if value is not None
    ]
    if len(parsed_dates) != len(out):
        raise RuntimeError("notice response contains rows without a source date")
    if any(left < right for left, right in zip(parsed_dates, parsed_dates[1:])):
        raise RuntimeError("notice response is not in the requested descending order")
    window_start = date(1900, 1, 1) if exhausted or not out else min(parsed_dates)
    return NoticeFetchResult(
        rows=out,
        captured_at=captured_at,
        window_start=window_start,
        exhausted=exhausted,
        page_count=page_count,
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


def read_codes_from_db(engine: Engine, offset: int, limit: int) -> list[str]:
    q = text("SELECT stock_code FROM si_all_code ORDER BY stock_code LIMIT :lim OFFSET :off")
    with engine.connect() as c:
        rows = c.execute(q, {"lim": limit, "off": offset}).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


def main() -> int:
    if httpx is None:
        raise RuntimeError("httpx is required to synchronize Eastmoney notices")
    p = argparse.ArgumentParser(description="东财个股公告 → si_notice_eastmoney")
    p.add_argument("--stock", type=str, default="", help="单只股票 6 位代码")
    p.add_argument("--from-si-all-code", action="store_true", help="从 si_all_code 按顺序批量拉取")
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--limit", type=int, default=50, help="配合 --from-si-all-code，默认 50")
    p.add_argument("--page-size", type=int, default=30, help="每页条数，默认 30")
    p.add_argument("--max-pages", type=int, default=3, help="每只股票最多翻页数，默认 3")
    p.add_argument("--sleep", type=float, default=0.3, help="股票间隔秒数")
    p.add_argument("--skip-ddl", action="store_true", help="不执行建表 SQL")
    args = p.parse_args()

    stock = args.stock.strip()
    if not stock and not args.from_si_all_code:
        p.print_help()
        print("\n请指定 --stock 或 --from-si-all-code", file=sys.stderr)
        return 2

    engine = create_batch_engine()
    if not args.skip_ddl:
        run_ddl(engine)

    codes: list[str] = []
    if stock:
        codes = [stock.zfill(6)]
    else:
        codes = read_codes_from_db(engine, max(0, args.offset), max(1, args.limit))

    total_items = 0
    with httpx.Client(headers={"User-Agent": "Mozilla/5.0 ProBigA-notice-sync"}, trust_env=False) as client:
        for i, code in enumerate(codes):
            try:
                fetch = fetch_pages(
                    client,
                    code,
                    page_size=max(5, min(50, int(args.page_size))),
                    max_pages=max(1, int(args.max_pages)),
                )
                rows = [
                    _parse_item(code, item, fetch.captured_at)
                    for item in fetch.rows
                    if isinstance(item, dict)
                ]
                n = upsert_rows(
                    engine,
                    rows,
                    stock_code=code,
                    window_start=fetch.window_start,
                    captured_at=fetch.captured_at,
                    fetch_evidence={
                        "page_count": fetch.page_count,
                        "exhausted": fetch.exhausted,
                        "sort": "published_descending",
                    },
                )
                total_items += n
                logger.info("%s/%s %s：本批解析 %s 条，写入/更新 %s 行", i + 1, len(codes), code, len(rows), n)
            except Exception as e:  # noqa: BLE001
                logger.warning("%s 失败：%s", code, e)
            if i + 1 < len(codes):
                time.sleep(max(0.0, float(args.sleep)))
    logger.info("完成：共处理 %s 只股票，累计写入/更新约 %s 行公告记录。", len(codes), total_items)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

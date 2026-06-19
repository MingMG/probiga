# -*- coding: utf-8 -*-
"""
东财「个股公告列表」拉取并写入 MySQL ``probiga.si_notice_eastmoney``（标题级，非 PDF 正文）。

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
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import create_engine, text
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

from server.common.config import get_mysql_url

DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_si_notice_eastmoney.sql"

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


def _mysql_url() -> str:
    return get_mysql_url(required=True)


def run_ddl(engine: Engine) -> None:
    if not DDL_PATH.is_file():
        logger.warning("DDL 文件不存在：%s", DDL_PATH)
        return
    raw = DDL_PATH.read_text(encoding="utf-8")
    buf: list[str] = []
    stmts: list[str] = []
    for line in raw.splitlines():
        ls = line.strip()
        if not ls or ls.startswith("--"):
            continue
        if ls.upper().startswith("USE "):
            continue
        buf.append(line)
        if ls.endswith(";"):
            stmts.append("\n".join(buf).strip())
            buf = []
    if buf:
        stmts.append("\n".join(buf).strip())
    with engine.begin() as conn:
        for stmt in stmts:
            s = stmt.rstrip(";").strip()
            if not s:
                continue
            try:
                conn.execute(text(s))
            except Exception as e:  # noqa: BLE001
                if "1050" in str(e) or "1007" in str(e) or "already exists" in str(e).lower():
                    continue
                logger.warning("DDL 执行提示：%s", e)


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


def _notice_date_from_item(item: dict[str, Any]) -> datetime | None:
    for key in ("display_time", "notice_date", "eiTime", "art_date"):
        parsed = _parse_notice_date(item.get(key))
        if parsed:
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
    nd = _notice_date_from_item(item)
    return {
        "stock_code": str(stock_code).strip().zfill(6),
        "art_code": art,
        "notice_date": nd.date() if nd else None,
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
) -> list[dict[str, Any]]:
    code = str(stock_code).strip().zfill(6)
    out: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        url = (
            "https://np-anotice-stock.eastmoney.com/api/security/ann?sr=-1"
            f"&page_size={page_size}&page_index={page}&client_source=web&stock_list={code}"
        )
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                r = client.get(url, timeout=20.0)
                r.raise_for_status()
                break
            except httpx.HTTPError as e:
                last_error = e
                if attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if last_error is not None and "r" not in locals():
            raise last_error
        data = r.json()
        if not data.get("success"):
            logger.warning("%s 第 %s 页 success!=1：%s", code, page, data.get("error"))
            break
        lst = (data.get("data") or {}).get("list") or []
        if not lst:
            break
        out.extend(lst)
        if len(lst) < page_size:
            break
    return out


def upsert_rows(engine: Engine, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    n = 0
    with engine.begin() as conn:
        for row in rows:
            if not row.get("art_code"):
                continue
            conn.execute(UPSERT_SQL, row)
            n += 1
    return n


def read_codes_from_db(engine: Engine, offset: int, limit: int) -> list[str]:
    q = text("SELECT stock_code FROM si_all_code ORDER BY stock_code LIMIT :lim OFFSET :off")
    with engine.connect() as c:
        rows = c.execute(q, {"lim": limit, "off": offset}).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


def main() -> int:
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

    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    if not args.skip_ddl:
        run_ddl(engine)

    codes: list[str] = []
    if stock:
        codes = [stock.zfill(6)]
    else:
        codes = read_codes_from_db(engine, max(0, args.offset), max(1, args.limit))

    etl = datetime.now().replace(microsecond=0)
    total_items = 0
    with httpx.Client(headers={"User-Agent": "Mozilla/5.0 ProBigA-notice-sync"}, trust_env=False) as client:
        for i, code in enumerate(codes):
            try:
                raw = fetch_pages(
                    client,
                    code,
                    page_size=max(5, min(50, int(args.page_size))),
                    max_pages=max(1, int(args.max_pages)),
                )
                rows = [_parse_item(code, it, etl) for it in raw if isinstance(it, dict)]
                n = upsert_rows(engine, rows)
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

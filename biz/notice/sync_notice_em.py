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
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sync_notice_em")
NOTICE_HTTP_TIMEOUT_SECONDS = 20.0
NOTICE_MIN_COVERAGE = float(os.environ.get("NOTICE_MIN_COVERAGE", "0.90"))
NOTICE_MIN_ROW_COVERAGE = float(os.environ.get("NOTICE_MIN_ROW_COVERAGE", "0.50"))

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine

DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_si_notice_eastmoney.sql"

UPSERT_SQL = text(
    """
    INSERT INTO si_notice_eastmoney (
        stock_code, art_code, notice_date, title, column_name, display_time,
        detail_url, association_validated, etl_sync_at
    ) VALUES (
        :stock_code, :art_code, :notice_date, :title, :column_name, :display_time,
        :detail_url, :association_validated, :etl_sync_at
    )
    ON DUPLICATE KEY UPDATE
        notice_date = VALUES(notice_date),
        title = VALUES(title),
        column_name = VALUES(column_name),
        display_time = VALUES(display_time),
        detail_url = VALUES(detail_url),
        association_validated = VALUES(association_validated),
        etl_sync_at = VALUES(etl_sync_at)
    """
)


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


    ensure_notice_validation_schema(engine)


def ensure_notice_validation_schema(engine: Engine) -> None:
    """Add the non-destructive stock-association trust marker."""
    with engine.begin() as conn:
        columns = {
            str(row[0])
            for row in conn.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'si_notice_eastmoney'
                    """
                )
            ).fetchall()
        }
        if "association_validated" not in columns:
            conn.execute(
                text(
                    """
                    ALTER TABLE si_notice_eastmoney
                    ADD COLUMN association_validated TINYINT(1) NOT NULL DEFAULT 0
                    COMMENT '1=source payload explicitly contains this stock code'
                    AFTER detail_url
                    """
                )
            )
        indexes = {
            str(row[0])
            for row in conn.execute(
                text(
                    """
                    SELECT INDEX_NAME
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'si_notice_eastmoney'
                    """
                )
            ).fetchall()
        }
        if "idx_notice_validated_stock_date" not in indexes:
            conn.execute(
                text(
                    """
                    CREATE INDEX idx_notice_validated_stock_date
                    ON si_notice_eastmoney
                       (association_validated, stock_code, notice_date)
                    """
                )
            )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS si_notice_sync_run (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    stock_code VARCHAR(16) NOT NULL,
                    started_at DATETIME NOT NULL,
                    completed_at DATETIME NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    fetched_rows INT NOT NULL DEFAULT 0,
                    validated_rows INT NOT NULL DEFAULT 0,
                    error_text VARCHAR(1000) NULL,
                    PRIMARY KEY (id),
                    KEY idx_notice_sync_code_time
                        (stock_code, completed_at, status),
                    KEY idx_notice_sync_time (completed_at, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )


def record_sync_run(
    engine: Engine,
    *,
    stock_code: str,
    started_at: datetime,
    completed_at: datetime,
    status: str,
    fetched_rows: int,
    validated_rows: int,
    error_text: str = "",
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO si_notice_sync_run
                    (stock_code, started_at, completed_at, status,
                     fetched_rows, validated_rows, error_text)
                VALUES
                    (:stock_code, :started_at, :completed_at, :status,
                     :fetched_rows, :validated_rows, :error_text)
                """
            ),
            {
                "stock_code": str(stock_code).strip().zfill(6),
                "started_at": started_at,
                "completed_at": completed_at,
                "status": str(status or "FAILED")[:16],
                "fetched_rows": max(0, int(fetched_rows)),
                "validated_rows": max(0, int(validated_rows)),
                "error_text": str(error_text or "")[:1000] or None,
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


def _notice_date_from_item(item: dict[str, Any]) -> datetime | None:
    today = datetime.now().date()
    for key in ("display_time", "notice_date", "eiTime", "art_date"):
        parsed = _parse_notice_date(item.get(key))
        if parsed and parsed.date() <= today:
            return parsed
    return None


def _item_stock_codes(item: dict[str, Any]) -> set[str]:
    codes: set[str] = set()
    raw_codes = item.get("codes") or []
    if not isinstance(raw_codes, list):
        return codes
    for raw in raw_codes:
        if isinstance(raw, dict):
            value = (
                raw.get("stock_code")
                or raw.get("code")
                or raw.get("security_code")
                or ""
            )
        else:
            value = raw
        digits = "".join(ch for ch in str(value or "") if ch.isdigit())
        if len(digits) >= 6:
            codes.add(digits[-6:])
    return codes


def _parse_item(
    stock_code: str,
    item: dict[str, Any],
    etl: datetime,
) -> dict[str, Any] | None:
    requested_code = str(stock_code).strip().zfill(6)
    if requested_code not in _item_stock_codes(item):
        return None
    art = (item.get("art_code") or "").strip()
    title = (item.get("title") or item.get("title_ch") or "").strip()[:1024]
    if not art or not title:
        return None
    cols = item.get("columns") or []
    col_name = ""
    if isinstance(cols, list) and cols and isinstance(cols[0], dict):
        col_name = str(cols[0].get("column_name") or "")[:256]
    disp = str(item.get("display_time") or "")[:64]
    nd = _notice_date_from_item(item)
    return {
        "stock_code": requested_code,
        "art_code": art,
        "notice_date": nd.date() if nd else None,
        "title": title or None,
        "column_name": col_name or None,
        "display_time": disp or None,
        "detail_url": _detail_url(stock_code, art) if art else None,
        "association_validated": 1,
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
            f"&page_size={page_size}&page_index={page}"
            f"&ann_type=A&client_source=web&stock_list={code}"
            "&f_node=0&s_node=0"
        )
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                r = client.get(url, timeout=NOTICE_HTTP_TIMEOUT_SECONDS)
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
    sql = """
        SELECT stock_code
        FROM si_all_code
        WHERE stock_code REGEXP '^(0|3|6)'
        ORDER BY stock_code
    """
    params = {"off": max(0, int(offset))}
    if int(limit) > 0:
        sql += " LIMIT :lim OFFSET :off"
        params["lim"] = int(limit)
    else:
        # MySQL's maximum unsigned BIGINT limit means "all remaining rows".
        sql += " LIMIT 18446744073709551615 OFFSET :off"
    with engine.connect() as c:
        rows = c.execute(text(sql), params).fetchall()
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
    p.add_argument(
        "--min-coverage",
        type=float,
        default=NOTICE_MIN_COVERAGE,
        help="Minimum successful stock-request coverage; default 90%%.",
    )
    p.add_argument(
        "--min-row-coverage",
        type=float,
        default=NOTICE_MIN_ROW_COVERAGE,
        help="Minimum stock coverage with at least one parsed notice; default 50%%.",
    )
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
        codes = read_codes_from_db(engine, max(0, args.offset), args.limit)

    etl = datetime.now().replace(microsecond=0)
    total_items = 0
    succeeded = 0
    failed = 0
    empty = 0
    nonempty = 0
    with httpx.Client(
        headers={"User-Agent": "Mozilla/5.0 ProBigA-notice-sync"},
        timeout=NOTICE_HTTP_TIMEOUT_SECONDS,
        trust_env=False,
    ) as client:
        for i, code in enumerate(codes):
            started_at = datetime.now().replace(microsecond=0)
            try:
                raw = fetch_pages(
                    client,
                    code,
                    page_size=max(5, min(50, int(args.page_size))),
                    max_pages=max(1, int(args.max_pages)),
                )
                rows = [
                    row
                    for it in raw
                    if isinstance(it, dict)
                    for row in [_parse_item(code, it, etl)]
                    if row is not None
                ]
                n = upsert_rows(engine, rows)
                record_sync_run(
                    engine,
                    stock_code=code,
                    started_at=started_at,
                    completed_at=datetime.now().replace(microsecond=0),
                    status="SUCCESS",
                    fetched_rows=len(raw),
                    validated_rows=n,
                )
                total_items += n
                succeeded += 1
                if not rows:
                    empty += 1
                else:
                    nonempty += 1
                logger.info("%s/%s %s：本批解析 %s 条，写入/更新 %s 行", i + 1, len(codes), code, len(rows), n)
            except Exception as e:  # noqa: BLE001
                try:
                    record_sync_run(
                        engine,
                        stock_code=code,
                        started_at=started_at,
                        completed_at=datetime.now().replace(microsecond=0),
                        status="FAILED",
                        fetched_rows=0,
                        validated_rows=0,
                        error_text=str(e),
                    )
                except Exception as ledger_exc:  # noqa: BLE001
                    logger.warning(
                        "%s notice sync ledger write failed: %s",
                        code,
                        ledger_exc,
                    )
                logger.warning("%s 失败：%s", code, e)
            if i + 1 < len(codes):
                time.sleep(max(0.0, float(args.sleep)))
    logger.info("完成：共处理 %s 只股票，累计写入/更新约 %s 行公告记录。", len(codes), total_items)
    failed = max(0, len(codes) - succeeded)
    coverage = succeeded / max(len(codes), 1)
    row_coverage = nonempty / max(len(codes), 1)
    logger.info(
        "Notice sync completed: stocks=%s succeeded=%s failed=%s empty=%s nonempty=%s rows=%s coverage=%.1f%% row_coverage=%.1f%%",
        len(codes), succeeded, failed, empty, nonempty, total_items, coverage * 100, row_coverage * 100,
    )
    if not codes:
        logger.error("No stock codes were read; refusing to report notice sync success")
        return 2
    if coverage < max(0.0, min(1.0, float(args.min_coverage))):
        logger.error(
            "Notice sync failed: successful stock-request coverage %.1f%% < %.1f%%",
            coverage * 100,
            float(args.min_coverage) * 100,
        )
        return 3
    min_row_coverage = max(0.0, min(1.0, float(args.min_row_coverage)))
    if row_coverage < min_row_coverage:
        logger.error(
            "Notice sync failed: non-empty stock coverage %.1f%% < %.1f%%",
            row_coverage * 100,
            min_row_coverage * 100,
        )
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

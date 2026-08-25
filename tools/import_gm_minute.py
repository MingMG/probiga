#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将掘金量化本地 Avro 缓存（storage.dat）中的分钟线数据导入 MySQL。

用法::

  python tools/import_gm_minute.py --data-path E:\\ProBigADate\\data\\storage.dat
  python tools/import_gm_minute.py --data-path E:\\ProBigADate\\data\\storage.dat --dry-run
  python tools/import_gm_minute.py --data-path E:\\ProBigADate\\data\\storage.dat --batch-size 50000

环境变量：
  MYSQL_URL   MySQL 连接串（也可写入 .env）
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("import_gm_minute")

_BEIJING_TZ = timezone(timedelta(hours=8))
_AVRO_MAGIC = b"Obj\x01"
_REQUIRED_COLUMNS = frozenset({
    "stock_code", "trade_time", "trade_date", "open", "high", "low",
    "close", "volume", "amount", "open_interest", "pre_close",
    "etl_sync_at",
})

_INSERT_SQL = (
    "INSERT IGNORE INTO `sm_stock_minute_gm` "
    "(`stock_code`,`trade_time`,`trade_date`,"
    "`open`,`high`,`low`,`close`,`volume`,`amount`,"
    "`open_interest`,`pre_close`,`etl_sync_at`) "
    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _mysql_url() -> str:
    from server.common.config import get_mysql_url
    return get_mysql_url(required=True)


def _symbol_to_code(symbol: str) -> str:
    return symbol.split(".")[-1] if "." in symbol else symbol


def _utc_to_beijing(utc_dt: datetime) -> datetime:
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(_BEIJING_TZ).replace(tzinfo=None)


def _now() -> datetime:
    return datetime.now()


def _validate_runtime_table(cursor) -> None:
    """Fail closed when the fenced migration has not prepared the target."""

    cursor.execute(
        "SELECT engine FROM information_schema.tables "
        "WHERE table_schema=DATABASE() AND table_name=%s",
        ("sm_stock_minute_gm",),
    )
    table = cursor.fetchone()
    if table is None or str(table[0] or "").lower() != "innodb":
        raise RuntimeError(
            "sm_stock_minute_gm is not prepared as an InnoDB runtime table"
        )
    cursor.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=DATABASE() AND table_name=%s",
        ("sm_stock_minute_gm",),
    )
    columns = {str(row[0]) for row in cursor.fetchall()}
    missing = sorted(_REQUIRED_COLUMNS - columns)
    if missing:
        raise RuntimeError(
            f"sm_stock_minute_gm runtime schema is missing columns: {missing}"
        )


def _scan_container_offsets(path: str) -> list[int]:
    offsets: list[int] = []
    chunk_size = 64 * 1024 * 1024
    overlap = len(_AVRO_MAGIC) - 1
    with open(path, "rb") as f:
        prev_tail = b""
        file_offset = 0
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            search_data = prev_tail + chunk
            pos = 0
            while True:
                idx = search_data.find(_AVRO_MAGIC, pos)
                if idx == -1:
                    break
                offsets.append(file_offset - len(prev_tail) + idx)
                pos = idx + 1
            prev_tail = chunk[-overlap:]
            file_offset += len(chunk)
    return offsets


# ---------------------------------------------------------------------------
# core import (LOAD DATA LOCAL INFILE via temp CSV)
# ---------------------------------------------------------------------------

def import_gm_minute(
    data_path: str,
    *,
    batch_size: int = 50000,
    skip_ddl: bool = False,
    dry_run: bool = False,
) -> None:
    import fastavro
    import pymysql

    data_path = str(Path(data_path).resolve())
    if not Path(data_path).exists():
        logger.error("文件不存在：%s", data_path)
        sys.exit(1)

    # 解析 MySQL 连接参数
    from sqlalchemy import make_url
    url = make_url(_mysql_url())

    # 1. 扫描容器偏移量
    logger.info("扫描 Avro 容器索引：%s", data_path)
    t0 = time.time()
    offsets = _scan_container_offsets(data_path)
    logger.info("发现 %d 个容器，耗时 %.1fs", len(offsets), time.time() - t0)

    if dry_run:
        # dry-run: 只统计
        total_records = 0
        with open(data_path, "rb") as f:
            for i, offset in enumerate(offsets):
                f.seek(offset)
                try:
                    reader = fastavro.block_reader(f)
                    block = next(reader)
                    total_records += sum(1 for _ in block)
                except Exception:
                    pass
                if (i + 1) % 10000 == 0:
                    logger.info("[dry-run] %d/%d 容器, %d 行", i + 1, len(offsets), total_records)
        logger.info("[dry-run] 完成！%d 容器, %d 行, 耗时 %.1fs", len(offsets), total_records, time.time() - t0)
        return

    # 2. 连接 MySQL
    conn = pymysql.connect(
        host=url.host or "127.0.0.1",
        port=url.port or 3306,
        user=url.username or "root",
        password=url.password or "",
        database=url.database or "probiga",
        charset="utf8mb4",
        local_infile=True,
    )
    cur = conn.cursor()

    # Persistent DDL is release-owned.  ``skip_ddl`` remains only for CLI/API
    # compatibility; every import now performs the same read-only guard.
    _ = skip_ddl
    _validate_runtime_table(cur)

    # 优化写入速度
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    cur.execute("SET UNIQUE_CHECKS=0")

    # 3. 逐容器读取 → 写临时 CSV → LOAD DATA LOCAL INFILE
    t1 = time.time()
    total_blocks = 0
    total_records = 0
    batch_rows: list[tuple] = []
    etl_now = _now().strftime("%Y-%m-%d %H:%M:%S")

    def _flush(rows: list[tuple]) -> None:
        if not rows:
            return
        fd, tmp_path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(fd, "w", newline="", encoding="utf-8") as tmp:
                writer = csv.writer(tmp)
                for r in rows:
                    writer.writerow(r)
            cur.execute(
                "LOAD DATA LOCAL INFILE %s IGNORE INTO TABLE `sm_stock_minute_gm` "
                "CHARACTER SET utf8mb4 "
                "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\\' "
                "LINES TERMINATED BY '\\n' "
                "(`stock_code`,`trade_time`,`trade_date`,"
                "`open`,`high`,`low`,`close`,`volume`,`amount`,"
                "`open_interest`,`pre_close`,`etl_sync_at`)",
                (tmp_path.replace("\\", "/"),),
            )
            conn.commit()
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    with open(data_path, "rb") as f:
        for i, offset in enumerate(offsets):
            f.seek(offset)
            try:
                reader = fastavro.block_reader(f)
                block = next(reader)
                records = list(block)
            except Exception:
                continue

            total_blocks += 1
            for rec in records:
                stock_code = _symbol_to_code(rec["symbol"])
                trade_time = _utc_to_beijing(rec["eob"])
                trade_date = trade_time.date()
                batch_rows.append((
                    stock_code,
                    trade_time.strftime("%Y-%m-%d %H:%M:%S"),
                    trade_date.isoformat(),
                    rec["open"], rec["high"], rec["low"], rec["close"],
                    rec["volume"], rec["amount"],
                    rec["position"], rec["pre_close"],
                    etl_now,
                ))

            if len(batch_rows) >= batch_size:
                _flush(batch_rows)
                total_records += len(batch_rows)
                logger.info("容器 %d/%d, 写入 %d 行 (累计 %d)", i + 1, len(offsets), len(batch_rows), total_records)
                batch_rows.clear()

            if total_blocks % 10000 == 0:
                logger.info("进度：%d/%d 容器, 耗时 %.1fs", total_blocks, len(offsets), time.time() - t1)

    # 最后一批
    if batch_rows:
        _flush(batch_rows)
        total_records += len(batch_rows)
        logger.info("最后一批写入 %d 行", len(batch_rows))

    # 恢复设置
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    cur.execute("SET UNIQUE_CHECKS=1")

    # 汇总
    cur.execute("SELECT COUNT(*) FROM sm_stock_minute_gm")
    total = cur.fetchone()[0]
    elapsed = time.time() - t1
    logger.info("导入完成！表中共 %d 行, 处理 %d 容器, 耗时 %.1fs", total, total_blocks, elapsed)

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="导入掘金量化 Avro 分钟线到 MySQL")
    parser.add_argument("--data-path", required=True, help="storage.dat 文件路径")
    parser.add_argument("--batch-size", type=int, default=50000, help="每批写入行数（默认 50000）")
    parser.add_argument(
        "--skip-ddl",
        action="store_true",
        help="兼容旧命令；运行期始终只校验表结构，不再执行持久 DDL",
    )
    parser.add_argument("--dry-run", action="store_true", help="只统计不写入")
    args = parser.parse_args()

    import_gm_minute(
        data_path=args.data_path,
        batch_size=args.batch_size,
        skip_ddl=args.skip_ddl,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

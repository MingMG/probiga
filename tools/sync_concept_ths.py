#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import text

from server.common.batch_db import create_batch_engine
from server.common.config import get_mysql_url

mysql_url = (os.environ.get("MYSQL_URL") or get_mysql_url(required=True)).strip()
engine = create_batch_engine(mysql_url)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SLEEP_SEC = float(os.environ.get("SI_REQUEST_SLEEP", "0.3"))
MAX_CONCEPTS = int(os.environ.get("THS_CONCEPT_MAX_CONCEPTS", "0"))
FRESH_HOURS = int(os.environ.get("THS_CONCEPT_FRESH_HOURS", "72"))
MIN_CONSTITUENT_ROWS = int(os.environ.get("THS_CONCEPT_MIN_CONSTITUENT_ROWS", "50000"))


def _now():
    return datetime.now().replace(microsecond=0)


def _sleep():
    time.sleep(SLEEP_SEC)


def _clean_object_df(df):
    if df is None or df.empty:
        return df
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip().replace({"": None, "nan": None, "None": None, "<NA>": None})
    return df


def df_to_table(engine, df, table):
    if df is None or df.empty:
        logger.info("表 %s：无数据，跳过。", table)
        return
    df = _clean_object_df(df)
    df.to_sql(table, engine, if_exists="append", index=False, chunksize=1000, method="multi")
    logger.info("表 %s：写入 %s 行。", table, len(df))


_REPLACEABLE_TABLES = {
    "si_concept_code_ths",
    "si_concept_constituent_ths",
}


def replace_table_transactionally(engine, df, table):
    """Replace one concept table without exposing an empty partial refresh."""
    if table not in _REPLACEABLE_TABLES:
        raise ValueError(f"unsupported concept table: {table}")
    if df is None or df.empty:
        raise ValueError(f"refuse to replace {table} with an empty dataset")
    if table == "si_concept_constituent_ths" and len(df) < MIN_CONSTITUENT_ROWS:
        raise ValueError(
            f"refuse to replace {table} with only {len(df)} rows; "
            f"expected at least {MIN_CONSTITUENT_ROWS}"
        )
    cleaned = _clean_object_df(df.copy())
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM `{table}`"))
        cleaned.to_sql(
            table,
            conn,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
    logger.info("表 %s：原子替换 %s 行。", table, len(cleaned))


def retry_remote(fn, *args, max_retries=3, **kwargs):
    last_err = None
    for attempt in range(max_retries):
        try:
            res = fn(*args, **kwargs)
            return res
        except Exception as e:
            last_err = e
            wait = 2 ** attempt
            logger.warning("请求失败(第%d次): %s, 等待%ds重试", attempt + 1, str(e)[:80], wait)
            time.sleep(wait)
    logger.error("请求最终失败: %s", str(last_err)[:120])
    return None


def _has_fresh_constituents(engine, fresh_hours: int = FRESH_HOURS) -> bool:
    if fresh_hours <= 0:
        return False
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT COUNT(*) AS cnt, MAX(etl_sync_at) AS max_sync
            FROM si_concept_constituent_ths
        """)).mappings().first()
    if not row:
        return False
    cnt = int(row.get("cnt") or 0)
    max_sync = row.get("max_sync")
    if cnt < 50000 or max_sync is None:
        return False
    age_hours = (datetime.now() - max_sync).total_seconds() / 3600
    if age_hours <= fresh_hours:
        logger.info(
            "si_concept_constituent_ths is fresh enough: %s rows, last_sync=%s, age=%.1fh; skip full refresh.",
            cnt,
            max_sync,
            age_hours,
        )
        return True
    return False


def _is_bad_ths_result(res):
    if res is None:
        return True
    if isinstance(res, Exception):
        return True
    if isinstance(res, pd.DataFrame) and res.empty:
        return True
    return False


def sync_concept_code_ths(engine, info):
    logger.info("===== 同步同花顺概念列表 (si_concept_code_ths) =====")
    ts = _now()
    df = retry_remote(info.all_concept_code_ths)
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        logger.error("获取同花顺概念列表失败或为空！")
        return pd.DataFrame()
    df = _clean_object_df(df)
    df["etl_sync_at"] = ts
    replace_table_transactionally(engine, df, "si_concept_code_ths")
    logger.info("概念列表: %d 条", len(df))
    _sleep()
    return df


def sync_concept_constituent_ths(engine, info, df_ths):
    logger.info("===== 同步同花顺概念成分股 (si_concept_constituent_ths) =====")
    ts = _now()
    if df_ths is None or df_ths.empty:
        logger.error("概念列表为空，无法同步成分股！")
        return

    parts = []

    def append_result(query_type, query_key, res):
        if _is_bad_ths_result(res):
            return
        d = res.copy()
        d["query_type"] = query_type
        d["query_key"] = query_key
        d["etl_sync_at"] = ts
        parts.append(d)

    idx_series = df_ths["index_code"].dropna().astype(str).str.strip().replace("", np.nan).dropna().unique()
    if MAX_CONCEPTS > 0:
        idx_series = idx_series[:MAX_CONCEPTS]
    logger.info("按 index_code 同步: %d 个", len(idx_series))
    for i, ic in enumerate(idx_series):
        res = retry_remote(info.concept_constituent_ths, index_code=str(ic), wait_time=300)
        append_result("index_code", str(ic), res)
        if (i + 1) % 20 == 0:
            logger.info("index_code 进度: %s/%s", i + 1, len(idx_series))
        _sleep()

    cc_series = df_ths["concept_code"].dropna().astype(str).str.strip().replace("", np.nan).dropna().unique()
    if MAX_CONCEPTS > 0:
        cc_series = cc_series[:MAX_CONCEPTS]
    logger.info("按 concept_code 同步: %d 个", len(cc_series))
    for i, cc in enumerate(cc_series):
        res = retry_remote(info.concept_constituent_ths, concept_code=str(cc), wait_time=300)
        append_result("concept_code", str(cc), res)
        if (i + 1) % 20 == 0:
            logger.info("concept_code 进度: %s/%s", i + 1, len(cc_series))
        _sleep()

    if parts:
        out = pd.concat(parts, ignore_index=True)
        replace_table_transactionally(
            engine,
            out,
            "si_concept_constituent_ths",
        )
        logger.info("成分股总计: %d 条", len(out))
    else:
        logger.error("未获取到任何成分股数据！")


def main():
    logger.info("开始同步同花顺概念数据...")
    logger.info("数据库: %s", mysql_url.split("@")[-1] if "@" in mysql_url else mysql_url)

    from adata.stock.info import info

    if _has_fresh_constituents(engine):
        return

    df_ths = sync_concept_code_ths(engine, info)
    sync_concept_constituent_ths(engine, info, df_ths)

    with engine.connect() as conn:
        cnt1 = conn.execute(text("SELECT COUNT(*) FROM si_concept_code_ths")).scalar()
        cnt2 = conn.execute(text("SELECT COUNT(*) FROM si_concept_constituent_ths")).scalar()
    logger.info("最终: si_concept_code_ths=%d, si_concept_constituent_ths=%d", cnt1, cnt2)
    logger.info("Done!")


if __name__ == "__main__":
    main()

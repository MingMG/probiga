#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按「表名」触发一次同步（probiga）。在仓库根目录执行::

  python tools/run_single_table.py si_all_index_code
  python tools/run_single_table.py sm_stock_current
  python tools/run_single_table.py --list
  python tools/run_single_table.py --write-windows-cmds   # 生成 tools/single_table/*.cmd
  python tools/run_single_table.py --run-all              # 按顺序跑完 HANDLERS 全部表（很慢）

STOCK-INFO 的 si_* 在本脚本内直接调函数；STOCK-MARKET / 舆情走子进程 ``-m``。
"""
from __future__ import annotations

import argparse
from datetime import datetime
from functools import wraps
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# 直接执行 python tools/run_single_table.py 时，sys.path[0] 往往是 tools/，找不到 biz
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from tools.env_config import load_project_env


load_project_env()

from sqlalchemy import text

from server.common.batch_db import create_batch_engine, read_frame
from server.common.process_env import build_child_env, child_process_timeout, temporary_env


def _first_env(env: dict[str, str], *names: str, default: str = "") -> str:
    for name in names:
        value = env.get(name, "").strip()
        if value:
            return value
    return default


def _enabled(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _run_subprocess(cmd: list[str], env: dict[str, str]) -> int:
    print("Run:", " ".join(cmd), flush=True)
    timeout = child_process_timeout(2 * 60 * 60, env_name="PROBIGA_RUN_SINGLE_TABLE_TIMEOUT")
    try:
        return subprocess.run(cmd, cwd=str(ROOT), env=env, timeout=timeout).returncode
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT after {timeout}s: {' '.join(cmd)}", file=sys.stderr, flush=True)
        return 124


def _child_env() -> dict[str, str]:
    return build_child_env(ROOT)


_SI_STEP_DEFAULTS = {
    "SI_SKIP_GLOBAL_TRUNCATE": "1",
    "SI_SYNC_SKIP_ALL_CODE": "1",
}


def _si_env(extra: dict[str, str] | None = None):
    values = dict(_SI_STEP_DEFAULTS)
    if extra:
        values.update(extra)
    return temporary_env(values, overwrite=False)


def _with_temporary_env(defaults):
    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            values = defaults() if callable(defaults) else defaults
            with temporary_env(values, overwrite=False):
                return fn(*args, **kwargs)

        return wrapped

    return decorate


def _with_si_env(extra=None):
    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            values = None
            dynamic_extra = extra() if callable(extra) else extra
            if dynamic_extra:
                values = dynamic_extra
            with _si_env(values):
                return fn(*args, **kwargs)

        return wrapped

    return decorate


def _si_index_extra_env() -> dict[str, str]:
    qmt_index_source = _first_env(os.environ, "SI_ALL_INDEX_CODE_SOURCE", "DATA_SOURCE_INDEX_LIST").strip().lower()
    if qmt_index_source not in {"qmt", "bigqmt", "big_qmt", "qmt_big"} and os.environ.get("SI_INDEX_TRY_EAST_FIRST") != "1":
        return {"SI_INDEX_PRIMARY": "sina"}
    return {}


def _run_minute_crawl(minute_type: str, env: dict[str, str]) -> int:
    env.setdefault("MINUTE_REQUEST_DELAY", "0.12")
    env.setdefault("MINUTE_REQUEST_JITTER", "0.08")
    env.setdefault("MINUTE_BATCH_EVERY", "0")
    env.setdefault("MINUTE_MIN_COVERAGE", "0.70")
    cmd = [sys.executable, "tools/crawl_minute_kline.py", "--type", minute_type]
    if _enabled(env.get("MINUTE_SKIP_CLOSED")):
        cmd.append("--skip-closed")
    return _run_subprocess(cmd, env)


def _run_flow_daily_fallback(date_str: str, env: dict[str, str]) -> int:
    env.setdefault("FLOW_SOURCES", "efinance,push2his,baidu")
    env.setdefault("FLOW_WORKERS", "4")
    env.setdefault("FLOW_REQUEST_DELAY", "0.15")
    env.setdefault("FLOW_REQUEST_JITTER", "0.15")
    env.setdefault("FLOW_BATCH_PAUSE_EVERY", "0")
    target_date = date_str.strip() or datetime.now().strftime("%Y-%m-%d")
    cmd = [sys.executable, "tools/fetch_sm_stock_capital_flow_daily.py", target_date]
    return _run_subprocess(cmd, env)


def _run_flow_daily_fast_current(env: dict[str, str]) -> int:
    env.setdefault("FLOW_FAST_MIN_COVERAGE", "0.70")
    cmd = [
        sys.executable,
        "tools/crawl_realtime_batch.py",
        "--only",
        "flow",
        "--min-coverage",
        env["FLOW_FAST_MIN_COVERAGE"],
        "--json",
    ]
    return _run_subprocess(cmd, env)


def _latest_trade_date() -> str:
    engine = create_batch_engine()
    queries = [
        "SELECT MAX(trade_date) FROM si_trade_calendar WHERE trade_status = 1 AND trade_date <= CURDATE()",
        "SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type = 1 AND trade_date <= CURDATE()",
    ]
    with engine.connect() as conn:
        for sql in queries:
            try:
                value = conn.execute(text(sql)).scalar()
            except Exception:
                continue
            if value:
                return str(value)[:10]
    return datetime.now().strftime("%Y-%m-%d")


def _sub_run_stock_market(only: str, extra_args: list[str] | None = None) -> int:
    env = _child_env()
    env.setdefault("SM_MAX_STOCKS", "0")
    env.setdefault("SM_MAX_INDEXES", "0")
    env.setdefault("SM_MAX_CONCEPTS", "0")
    env.setdefault("SM_HTTP_RETRIES", "3")
    env.setdefault("SM_HTTP_BACKOFF", "2")
    env.setdefault("SM_REQUEST_SLEEP", "0.3")
    cmd = [
        sys.executable,
        "-m",
        "biz.stock_market.sync_stock_market",
        "--only",
        only,
        "--limit",
        "-1",
    ]
    if extra_args:
        cmd.extend(extra_args)
    return _run_subprocess(cmd, env)


def _sub_run_script(script_path: str, extra_args: list[str] | None = None) -> int:
    env = _child_env()
    cmd = [sys.executable, script_path]
    if extra_args:
        cmd.extend(extra_args)
    return _run_subprocess(cmd, env)


def _sub_run_sentiment(only: str, date_str: str = "") -> int:
    env = _child_env()
    env.setdefault("SE_SKIP_GLOBAL_TRUNCATE", "1")
    if date_str.strip() and "a_list" in only:
        env["SE_A_LIST_DATE"] = date_str.strip()
    cmd = [sys.executable, "-m", "biz.sentiment.sync_sentiment", "--only", only]
    return _run_subprocess(cmd, env)


def _sub_run_flow_daily(date_str: str = "") -> int:
    env = _child_env()
    flow_source = _first_env(
        env,
        "DATA_SOURCE_FLOW_DAILY",
        "SM_STOCK_FLOW_DAILY_SOURCE",
        "DATA_SOURCE_STOCK_FLOW_DAILY",
    ).strip().lower()
    if flow_source == "qmt":
        cmd = [
            sys.executable,
            "-m",
            "biz.stock_market.sync_stock_market",
            "--only",
            "stock_flow_daily",
            "--limit",
            "-1",
        ]
        if date_str.strip():
            cmd.extend(["--flow-date", date_str.strip()])
        rc = _run_subprocess(cmd, env)
        if rc == 0:
            return 0
        print(f"QMT daily flow failed with exit={rc}; falling back to external flow fetch.", flush=True)
    if not date_str.strip():
        rc = _run_flow_daily_fast_current(env)
        if rc == 0:
            return 0
        print(f"Fast current-day flow failed with exit={rc}; falling back to historical flow fetch.", flush=True)
    return _run_flow_daily_fallback(date_str, env)


def _sub_run_kline_daily(date_str: str = "") -> int:
    env = _child_env()
    kline_source = _first_env(env, "DATA_SOURCE_KLINE", "SM_STOCK_KLINE_SOURCE").strip().lower()
    if kline_source in {"myquant", "gm", "emquant", "goldminer", "qmt", "bigqmt", "big_qmt", "qmt_big"}:
        target_date = date_str.strip() or (
            _latest_trade_date() if kline_source in {"qmt", "bigqmt", "big_qmt", "qmt_big"} else ""
        )
        canonical_source = "bigqmt" if kline_source in {"bigqmt", "big_qmt", "qmt_big"} else kline_source
        cmd = [
            sys.executable,
            "-m",
            "biz.stock_market.sync_stock_market",
            "--only",
            "stock_kline",
            "--kline-source",
            canonical_source or "myquant",
            "--kline-incremental",
            "--limit",
            "0",
        ]
        if target_date:
            cmd.extend(["--kline-start", target_date, "--kline-end", target_date])
        rc = _run_subprocess(cmd, env)
        if rc == 0:
            return 0
        if canonical_source not in {"qmt", "bigqmt"}:
            return rc
        if not _enabled(
            env.get("QMT_PRIMARY_ALLOW_EXTERNAL_FALLBACK", "1")
        ):
            print(
                f"QMT daily kline failed with exit={rc}; "
                "external fallback is disabled for this provenance-strict run.",
                flush=True,
            )
            return rc
        print(f"QMT daily kline failed with exit={rc}; falling back to external daily kline fetch.", flush=True)
    env.setdefault("KLINE_DAILY_WORKERS", "2")
    env.setdefault("KLINE_DAILY_REQUEST_DELAY", "0.25")
    env.setdefault("KLINE_DAILY_REQUEST_JITTER", "0.1")
    env.setdefault("KLINE_DAILY_MIN_COVERAGE", "0.90")
    cmd = [sys.executable, "tools/fetch_sm_stock_kline_daily.py"]
    if date_str.strip():
        cmd.append(date_str.strip())
    return _run_subprocess(cmd, env)


def _sub_run_minute(minute_type: str, date_str: str = "") -> int:
    env = _child_env()
    if date_str.strip():
        # All registered minute backends, including QMT, resolve their target
        # day through this existing canonical override.
        env["MYQUANT_MINUTE_DATE"] = date_str.strip()
    source = _first_env(
        env,
        "DATA_SOURCE_MINUTE",
        "SM_STOCK_MINUTE_SOURCE",
        "SM_MARKET_DATA_SOURCE",
    ).strip().lower()
    if minute_type == "stock" and source in {"myquant", "gm", "emquant", "goldminer", "qmt", "bigqmt", "big_qmt", "qmt_big"}:
        cmd = [
            sys.executable,
            "-m",
            "biz.stock_market.sync_stock_market",
            "--only",
            "stock_minute",
            "--limit",
            "-1",
        ]
        rc = _run_subprocess(cmd, env)
        if rc == 0:
            return 0
        if not _enabled(env.get("QMT_PRIMARY_ALLOW_EXTERNAL_FALLBACK", "1")):
            print(
                f"QMT stock minute failed with exit={rc}; "
                "external fallback is disabled for this provenance-strict run.",
                flush=True,
            )
            return rc
        print(f"QMT stock minute failed with exit={rc}; falling back to Eastmoney minute fetch.", flush=True)
        return _run_minute_crawl("stock", env)
    if minute_type == "index":
        source = _first_env(env, "DATA_SOURCE_INDEX_MINUTE", "SM_INDEX_MINUTE_SOURCE").strip().lower()
        if source in {"qmt", "bigqmt", "big_qmt", "qmt_big"}:
            cmd = [sys.executable, "-m", "biz.stock_market.sync_stock_market", "--only", "index_minute", "--limit", "-1"]
            rc = _run_subprocess(cmd, env)
            if rc == 0:
                return 0
            print(f"QMT index minute failed with exit={rc}; falling back to Eastmoney minute fetch.", flush=True)
            return _run_minute_crawl("index", env)
    if minute_type == "concept":
        source = _first_env(env, "DATA_SOURCE_CONCEPT_MINUTE", "SM_CONCEPT_MINUTE_SOURCE").strip().lower()
        if source == "qmt":
            cmd = [sys.executable, "-m", "biz.stock_market.sync_stock_market", "--only", "concept_east_minute", "--limit", "-1"]
            rc = _run_subprocess(cmd, env)
            if rc == 0:
                return 0
            print(f"QMT concept minute failed with exit={rc}; falling back to Eastmoney minute fetch.", flush=True)
            return _run_minute_crawl("concept", env)
    if minute_type == "flow":
        source = _first_env(env, "DATA_SOURCE_FLOW_MIN", "SM_STOCK_FLOW_MIN_SOURCE", "DATA_SOURCE_STOCK_FLOW_MIN").strip().lower()
        if source == "qmt":
            cmd = [sys.executable, "-m", "biz.stock_market.sync_stock_market", "--only", "stock_flow_min", "--limit", "-1"]
            rc = _run_subprocess(cmd, env)
            if rc == 0:
                return 0
            print(f"QMT minute flow failed with exit={rc}; falling back to Eastmoney minute fetch.", flush=True)
            return _run_minute_crawl("flow", env)
    return _run_minute_crawl(minute_type, env)


def _si_engine_info():
    from server.common.adata_release import ensure_adata_import_path

    ensure_adata_import_path(ROOT)

    from biz.stock_info.sync_stock_info import load_info, run_ddl

    eng = create_batch_engine()
    run_ddl(eng)
    info = load_info()
    return eng, info


@_with_si_env(_si_index_extra_env)
def run_si_all_index_code() -> int:
    # 东财 push2 常不可用：默认先只拉新浪；若要仍试东财再设 SI_INDEX_TRY_EAST_FIRST=1
    from biz.stock_info.sync_stock_info import sync_all_index_code

    eng, info = _si_engine_info()
    try:
        df = sync_all_index_code(eng, info)
    except Exception as exc:
        source = _first_env(os.environ, "SI_ALL_INDEX_CODE_SOURCE", "DATA_SOURCE_INDEX_LIST").lower()
        if source not in {"qmt", "bigqmt", "big_qmt", "qmt_big"}:
            raise
        print(f"QMT index list failed; falling back to the external index list: {exc}", flush=True)
        with temporary_env({"SI_ALL_INDEX_CODE_SOURCE": "sina"}):
            df = sync_all_index_code(eng, info)
    if df is None or getattr(df, "empty", True):
        print(
            "【结果】本次未写入 si_all_index_code（东财限流/断连时常见）。"
            "默认已跳过，可先同步其它表；需要指数时再换网络或加大 SI_COOLDOWN_BEFORE_INDEX / SI_INDEX_FALLBACK_PAGE_SLEEP 后重试。",
            flush=True,
        )
    else:
        print(f"【结果】si_all_index_code 已更新，约 {len(df)} 条指数（去重后）。", flush=True)
    return 0


@_with_si_env()
def run_si_all_code() -> int:
    from biz.stock_info.sync_stock_info import sync_all_code

    eng, info = _si_engine_info()
    try:
        df = sync_all_code(eng, info)
    except Exception as exc:
        source = _first_env(os.environ, "SI_ALL_CODE_SOURCE", "DATA_SOURCE_CODE_LIST").lower()
        if source not in {"qmt", "bigqmt", "big_qmt", "qmt_big"}:
            raise
        print(f"QMT stock list failed; falling back to the external stock list: {exc}", flush=True)
        with temporary_env({"SI_ALL_CODE_SOURCE": "adata"}):
            df = sync_all_code(eng, info)
    print(f"【结果】si_all_code 已更新，约 {0 if df is None else len(df)} 条。", flush=True)
    return 0


@_with_si_env()
def run_si_index_constituent() -> int:
    from biz.stock_info.sync_stock_info import (
        load_info,
        run_ddl,
        sync_all_index_code,
        sync_index_constituent,
    )

    eng = create_batch_engine()
    run_ddl(eng)
    info = load_info()
    df = read_frame(text("SELECT * FROM si_all_index_code"), eng)
    if df.empty:
        print("si_all_index_code 为空，先拉指数列表…", flush=True)
        df = sync_all_index_code(eng, info)
    sync_index_constituent(eng, info, df)
    return 0


@_with_si_env()
def run_si_concept_code_east() -> int:
    from biz.stock_info.sync_stock_info import sync_concept_code_east

    eng, info = _si_engine_info()
    df = sync_concept_code_east(eng, info)
    print(f"【结果】si_concept_code_east 已更新，约 {0 if df is None else len(df)} 条。", flush=True)
    return 0


@_with_si_env()
def run_si_concept_constituent_east() -> int:
    from biz.stock_info.sync_stock_info import (
        load_info,
        run_ddl,
        sync_concept_code_east,
        sync_concept_constituent_east,
    )

    eng = create_batch_engine()
    run_ddl(eng)
    info = load_info()
    df = read_frame(text("SELECT * FROM si_concept_code_east"), eng)
    if df.empty:
        print("si_concept_code_east 为空，先拉东财概念列表…", flush=True)
        df = sync_concept_code_east(eng, info)
    sync_concept_constituent_east(eng, info, df)
    return 0


@_with_temporary_env({"SI_SKIP_GLOBAL_TRUNCATE": "1"})
def run_si_stock_relations() -> int:
    from biz.stock_info.sync_stock_info import (
        load_info,
        run_ddl,
        sync_all_code,
        sync_per_stock_tables,
    )

    eng = create_batch_engine()
    run_ddl(eng)
    info = load_info()
    df = read_frame(text("SELECT stock_code FROM si_all_code ORDER BY stock_code"), eng)
    if df.empty:
        print("si_all_code 为空，先同步股票代码列表…", flush=True)
        df = sync_all_code(eng, info)
    sync_per_stock_tables(eng, info, df)
    print("【结果】si_industry_sw / si_stock_concept_east / si_stock_plate_east 已更新。", flush=True)
    return 0


HANDLERS: dict[str, tuple[str, list[str] | None]] = {
    "si_all_code": ("py_si_all_code", None),
    "si_all_index_code": ("py_si_index_list", None),
    "si_concept_code_east": ("py_si_concept_code_east", None),
    "si_index_constituent": ("py_si_index_const", None),
    "si_concept_constituent_east": ("py_si_east_const", None),
    "si_industry_sw": ("py_si_stock_rel", None),
    "si_stock_concept_east": ("py_si_stock_rel", None),
    "si_stock_plate_east": ("py_si_stock_rel", None),
    "sm_concept_capital_flow_east": ("subprocess_sm", ["concept_flow_east"]),
    "sm_concept_east_current": ("subprocess_concept_current", None),
    "sm_concept_east_kline": ("subprocess_sm", ["concept_east_kline"]),
    "sm_concept_east_minute": ("subprocess_minute", ["concept"]),
    "sm_concept_ths_current": ("subprocess_sm", ["concept_ths_current"]),
    "sm_concept_ths_kline": ("subprocess_sm", ["concept_ths_kline"]),
    "sm_concept_ths_minute": ("subprocess_sm", ["concept_ths_minute"]),
    "sm_dividend": ("subprocess_sm", ["dividend"]),
    "sm_index_current": ("subprocess_sm", ["index_current"]),
    "sm_index_kline": ("subprocess_sm", ["index_kline"]),
    "sm_index_minute": ("subprocess_minute", ["index"]),
    "sm_stock_bar": ("subprocess_sm", ["stock_bar"]),
    "sm_stock_capital_flow_daily": ("subprocess_flow_daily", None),
    "sm_stock_capital_flow_min": ("subprocess_minute", ["flow"]),
    "sm_stock_kline": ("subprocess_kline_daily", None),
    "sm_stock_current": ("subprocess_sm", ["stock_current"]),
    "sm_stock_five_level": ("subprocess_sm", ["stock_five"]),
    "sm_stock_minute": ("subprocess_minute", ["stock"]),
    "st_a_list_daily": ("subprocess_se", ["a_list_daily"]),
    "st_a_list_info": ("subprocess_se", ["a_list_daily", "a_list_info"]),
}

# --run-all 顺序：先 SI 基础，再个股/指数/概念（全市场较慢），最后舆情
RUN_ALL_ORDER: list[str] = [
    "si_all_code",
    "si_all_index_code",
    "si_concept_code_east",
    "si_index_constituent",
    "si_concept_constituent_east",
    "si_stock_plate_east",
    "sm_dividend",
    "sm_stock_capital_flow_daily",
    "sm_stock_capital_flow_min",
    "sm_stock_minute",
    "sm_stock_current",
    "sm_stock_five_level",
    "sm_stock_bar",
    "sm_index_current",
    "sm_index_minute",
    "sm_index_kline",
    "sm_concept_capital_flow_east",
    "sm_concept_east_current",
    "sm_concept_east_minute",
    "sm_concept_east_kline",
    "sm_concept_ths_current",
    "sm_concept_ths_minute",
    "sm_concept_ths_kline",
    "st_a_list_daily",
    "st_a_list_info",
]


def _run_one_table(key: str, date_str: str = "") -> int:
    kind, payload = HANDLERS[key]
    extra = []
    if date_str:
        if key in ("sm_stock_capital_flow_daily", "sm_concept_capital_flow_east"):
            extra.append(f"--flow-date={date_str}")
    if key == "sm_index_kline":
        index_source = _first_env(
            os.environ,
            "DATA_SOURCE_INDEX_KLINE",
            "SM_INDEX_KLINE_SOURCE",
        ).strip().lower()
        if index_source == "qmt":
            # A scheduled incremental run must never silently expand into the
            # multi-year SM_INDEX_START default.
            target_date = date_str.strip() or _latest_trade_date()
            extra.extend(["--kline-start", target_date, "--kline-end", target_date])
    if kind == "subprocess_sm":
        assert payload
        rc = _sub_run_stock_market(",".join(payload), extra_args=extra or None)
        if rc == 0 or key != "sm_index_kline":
            return rc
        index_source = _first_env(os.environ, "DATA_SOURCE_INDEX_KLINE", "SM_INDEX_KLINE_SOURCE").lower()
        if index_source != "qmt":
            return rc
        print(f"QMT index kline failed with exit={rc}; falling back to external index K-line.", flush=True)
        with temporary_env(
            {"DATA_SOURCE_INDEX_KLINE": "tencent", "SM_INDEX_KLINE_SOURCE": "tencent"},
            overwrite=True,
        ):
            return _sub_run_stock_market(",".join(payload), extra_args=extra or None)
    if kind == "subprocess_flow_daily":
        return _sub_run_flow_daily(date_str)
    if kind == "subprocess_kline_daily":
        return _sub_run_kline_daily(date_str)
    if kind == "subprocess_minute":
        assert payload
        return _sub_run_minute(payload[0], date_str)
    if kind == "subprocess_concept_current":
        # Keep the current snapshot on the same universe/provenance as the
        # concept reference table.  The legacy Eastmoney crawler only covers
        # BK-style concepts and would replace a full BigQMT catalog with a
        # partial snapshot at the scheduled 15:45 run.
        reference_source = _first_env(
            os.environ,
            "DATA_SOURCE_CONCEPT_LIST",
            "SI_CONCEPT_SOURCE",
            default="east",
        ).lower()
        if reference_source in {"bigqmt", "big_qmt", "qmt_big"}:
            current_source = "bigqmt"
        elif reference_source == "qmt":
            current_source = "qmt"
        else:
            current_source = _first_env(
                os.environ,
                "DATA_SOURCE_CONCEPT_CURRENT",
                "SM_CONCEPT_CURRENT_SOURCE",
                default="east",
            ).lower()
        with temporary_env(
            {
                "DATA_SOURCE_CONCEPT_CURRENT": current_source,
                "SM_CONCEPT_CURRENT_SOURCE": current_source,
            },
            overwrite=True,
        ):
            return _sub_run_stock_market("concept_east_current")
    if kind == "subprocess_se":
        assert payload
        return _sub_run_sentiment(",".join(payload), date_str)
    if kind == "subprocess_script":
        assert payload
        return _sub_run_script(payload[0])
    if kind == "subprocess_sm_akshare":
        return _sub_run_stock_market("stock_kline", extra_args=["--kline-source", "akshare"] + extra)
    if kind == "py_si_all_code":
        return run_si_all_code()
    if kind == "py_si_index_list":
        return run_si_all_index_code()
    if kind == "py_si_concept_code_east":
        return run_si_concept_code_east()
    if kind == "py_si_index_const":
        return run_si_index_constituent()
    if kind == "py_si_east_const":
        return run_si_concept_constituent_east()
    if kind == "py_si_stock_rel":
        return run_si_stock_relations()
    print("内部错误:", kind, file=sys.stderr)
    return 3


def main() -> int:
    p = argparse.ArgumentParser(description="按表名单次同步 probiga")
    p.add_argument("table", nargs="?", help="表名，见 --list")
    p.add_argument("date", nargs="?", default="", help="可选日期参数（YYYY-MM-DD），传递给子步骤（如 sm_stock_capital_flow_daily 的 --flow-date）")
    p.add_argument("--list", action="store_true", help="列出支持的表名")
    p.add_argument(
        "--run-all",
        action="store_true",
        help="按固定顺序依次跑完本脚本支持的全部表（耗时可很长；见 RUN_ALL_ORDER）",
    )
    p.add_argument(
        "--write-windows-cmds",
        action="store_true",
        help="在 tools/single_table/ 下生成与表名一一对应的 .cmd（双击运行）",
    )
    args = p.parse_args()

    if args.run_all:
        failed: list[tuple[str, int]] = []
        for k in RUN_ALL_ORDER:
            if k not in HANDLERS:
                continue
            print("\n" + "=" * 60 + f"\n>>> {k}\n" + "=" * 60, flush=True)
            rc = _run_one_table(k)
            if rc != 0:
                failed.append((k, rc))
                print(f"步骤失败 exit={rc}，继续下一步…", flush=True)
        if failed:
            print("\n以下步骤非零退出:", failed, file=sys.stderr)
            return 1
        print("\n全部步骤已跑完。", flush=True)
        return 0

    if args.write_windows_cmds:
        out = ROOT / "tools" / "single_table"
        out.mkdir(parents=True, exist_ok=True)
        for name in sorted(HANDLERS):
            body = (
                "@echo off\r\n"
                'cd /d "%~dp0..\\.."\r\n'
                f'python tools\\run_single_table.py {name}\r\n'
                "if errorlevel 1 pause\r\n"
            )
            (out / f"sync_{name}.cmd").write_text(body, encoding="utf-8")
        print("已生成:", out, "共", len(HANDLERS), "个 .cmd", flush=True)
        return 0

    if args.list or not args.table:
        for k in sorted(HANDLERS):
            print(k, flush=True)
        return 0 if args.list else 1

    key = args.table.strip()
    if key not in HANDLERS:
        print("未知表名:", key, file=sys.stderr)
        return 2

    return _run_one_table(key, date_str=args.date or "")


if __name__ == "__main__":
    raise SystemExit(main())

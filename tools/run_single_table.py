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
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# 直接执行 python tools/run_single_table.py 时，sys.path[0] 往往是 tools/，找不到 biz
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)


def _sub_run_stock_market(only: str, extra_args: list[str] | None = None) -> int:
    env = os.environ.copy()
    pp = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = _ROOT_STR if not pp else f"{_ROOT_STR}{os.pathsep}{pp}"
    env.setdefault("SM_MAX_STOCKS", "500")
    env.setdefault("SM_MAX_INDEXES", "200")
    env.setdefault("SM_MAX_CONCEPTS", "100")
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
    print("执行:", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=str(ROOT), env=env)


def _sub_run_sentiment(only: str) -> int:
    env = os.environ.copy()
    pp = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = _ROOT_STR if not pp else f"{_ROOT_STR}{os.pathsep}{pp}"
    env.setdefault("SE_SKIP_GLOBAL_TRUNCATE", "1")
    cmd = [sys.executable, "-m", "biz.sentiment.sync_sentiment", "--only", only]
    print("执行:", " ".join(cmd), flush=True)
    return subprocess.call(cmd, cwd=str(ROOT), env=env)


def _si_engine_info():
    sys.path.insert(0, str(ROOT / "adata"))
    from sqlalchemy import create_engine

    from biz.stock_info.sync_stock_info import _mysql_url, load_info, run_ddl

    os.environ.setdefault("SI_SKIP_GLOBAL_TRUNCATE", "1")
    os.environ.setdefault("SI_SYNC_SKIP_ALL_CODE", "1")
    eng = create_engine(_mysql_url(), pool_pre_ping=True)
    run_ddl(eng)
    info = load_info()
    return eng, info


def run_si_all_index_code() -> int:
    # 东财 push2 常不可用：默认先只拉新浪；若要仍试东财再设 SI_INDEX_TRY_EAST_FIRST=1
    if os.environ.get("SI_INDEX_TRY_EAST_FIRST") != "1":
        os.environ.setdefault("SI_INDEX_PRIMARY", "sina")

    from biz.stock_info.sync_stock_info import sync_all_index_code

    eng, info = _si_engine_info()
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


def run_si_index_constituent() -> int:
    import pandas as pd
    from sqlalchemy import create_engine, text

    from biz.stock_info.sync_stock_info import (
        _mysql_url,
        load_info,
        run_ddl,
        sync_all_index_code,
        sync_index_constituent,
    )

    os.environ.setdefault("SI_SKIP_GLOBAL_TRUNCATE", "1")
    os.environ.setdefault("SI_SYNC_SKIP_ALL_CODE", "1")
    eng = create_engine(_mysql_url(), pool_pre_ping=True)
    run_ddl(eng)
    info = load_info()
    df = pd.read_sql(text("SELECT * FROM si_all_index_code"), eng)
    if df.empty:
        print("si_all_index_code 为空，先拉指数列表…", flush=True)
        df = sync_all_index_code(eng, info)
    sync_index_constituent(eng, info, df)
    return 0


def run_si_concept_constituent_east() -> int:
    import pandas as pd
    from sqlalchemy import create_engine, text

    from biz.stock_info.sync_stock_info import (
        _mysql_url,
        load_info,
        run_ddl,
        sync_concept_code_east,
        sync_concept_constituent_east,
    )

    os.environ.setdefault("SI_SKIP_GLOBAL_TRUNCATE", "1")
    os.environ.setdefault("SI_SYNC_SKIP_ALL_CODE", "1")
    eng = create_engine(_mysql_url(), pool_pre_ping=True)
    run_ddl(eng)
    info = load_info()
    df = pd.read_sql(text("SELECT * FROM si_concept_code_east"), eng)
    if df.empty:
        print("si_concept_code_east 为空，先拉东财概念列表…", flush=True)
        df = sync_concept_code_east(eng, info)
    sync_concept_constituent_east(eng, info, df)
    return 0


HANDLERS: dict[str, tuple[str, list[str] | None]] = {
    "si_all_index_code": ("py_si_index_list", None),
    "si_index_constituent": ("py_si_index_const", None),
    "si_concept_constituent_east": ("py_si_east_const", None),
    "sm_concept_capital_flow_east": ("subprocess_sm", ["concept_flow_east"]),
    "sm_concept_east_current": ("subprocess_sm", ["concept_east_current"]),
    "sm_concept_east_kline": ("subprocess_sm", ["concept_east_kline"]),
    "sm_concept_east_minute": ("subprocess_sm", ["concept_east_minute"]),
    "sm_concept_ths_current": ("subprocess_sm", ["concept_ths_current"]),
    "sm_concept_ths_kline": ("subprocess_sm", ["concept_ths_kline"]),
    "sm_concept_ths_minute": ("subprocess_sm", ["concept_ths_minute"]),
    "sm_dividend": ("subprocess_sm", ["dividend"]),
    "sm_index_current": ("subprocess_sm", ["index_current"]),
    "sm_index_kline": ("subprocess_sm", ["index_kline"]),
    "sm_index_minute": ("subprocess_sm", ["index_minute"]),
    "sm_stock_bar": ("subprocess_sm", ["stock_bar"]),
    "sm_stock_capital_flow_daily": ("subprocess_sm", ["stock_flow_daily"]),
    "sm_stock_capital_flow_min": ("subprocess_sm", ["stock_flow_min"]),
    "sm_stock_kline": ("subprocess_sm_akshare", None),
    "sm_stock_current": ("subprocess_sm", ["stock_current"]),
    "sm_stock_five_level": ("subprocess_sm", ["stock_five"]),
    "sm_stock_minute": ("subprocess_sm", ["stock_minute"]),
    "st_a_list_daily": ("subprocess_se", ["a_list_daily"]),
    "st_a_list_info": ("subprocess_se", ["a_list_daily", "a_list_info"]),
}

# --run-all 顺序：先 SI 基础，再个股/指数/概念（全市场较慢），最后舆情
RUN_ALL_ORDER: list[str] = [
    "si_all_index_code",
    "si_index_constituent",
    "si_concept_constituent_east",
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
    if kind == "subprocess_sm":
        assert payload
        return _sub_run_stock_market(",".join(payload), extra_args=extra or None)
    if kind == "subprocess_se":
        assert payload
        return _sub_run_sentiment(",".join(payload))
    if kind == "subprocess_sm_akshare":
        return _sub_run_stock_market("stock_kline", extra_args=["--kline-source", "akshare"])
    if kind == "py_si_index_list":
        return run_si_all_index_code()
    if kind == "py_si_index_const":
        return run_si_index_constituent()
    if kind == "py_si_east_const":
        return run_si_concept_constituent_east()
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

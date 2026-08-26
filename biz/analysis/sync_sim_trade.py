# -*- coding: utf-8 -*-
"""
模拟交易定时任务

盘中每1分钟运行一次（只查推荐股+持仓，请求量极小）：
1. 检查实时模拟卖出信号 → 执行卖出
2. 检查盘中验证卖出信号 → 执行卖出
3. 检查三种策略的实时模拟买入信号 → 执行买入

使用方法：
    python -m biz.analysis.sync_sim_trade
"""

import argparse
import hashlib
import json
import os
import sys
import logging
import uuid
from pathlib import Path as _Path
from datetime import date, datetime, time
from typing import Any, Mapping

_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.engine.sim_trade_engine import SimTradeEngine, _previous_trade_date

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SIM_TRADE_TASK_RESULT_SCHEMA = "probiga.sim-trade-task-result.v1"


def _is_intraday_runtime() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    return time(9, 20) <= now.time() <= time(15, 5)


def _recommendation_count(pick_date: str) -> int:
    try:
        with get_engine().connect() as conn:
            return int(conn.execute(
                text(
                    "SELECT COUNT(*) FROM st_recommended_stocks "
                    "WHERE pick_date = :d "
                    "AND (recommend_status IS NULL OR recommend_status = 'ALLOW')"
                ),
                {"d": pick_date[:10]},
            ).scalar() or 0)
    except Exception:
        return 0


def ensure_recommendations_for_signal_date(
    signal_date: str,
    *,
    execution_time: str = "",
    top_n: int = 80,
    min_score: float = 62.0,
    min_kline_coverage: float = 0.80,
) -> dict:
    """Read-only prerequisite check; simulation never generates recommendations."""
    del execution_time, top_n, min_score, min_kline_coverage
    signal_date = (signal_date or "")[:10]
    if not signal_date:
        return {"status": "error", "error": "signal_date is required"}
    count = _recommendation_count(signal_date)
    return {
        "status": "exists" if count > 0 else "missing",
        "signal_date": signal_date,
        "count": count,
        "read_only": True,
    }


def prepare_signals(
    trade_date: str = "",
    signal_date: str = "",
    reset: bool = False,
    ensure_recommendations: bool = False,
    execution_time: str = "",
    top_n: int = 80,
    min_score: float = 62.0,
    min_kline_coverage: float = 0.80,
) -> dict:
    trade_date = (trade_date or date.today().isoformat())[:10]
    signal_date = (signal_date or _previous_trade_date(trade_date))[:10]
    del execution_time, top_n, min_score, min_kline_coverage
    if ensure_recommendations and (
        str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower()
        == "production"
    ):
        return {
            "status": "error",
            "trade_date": trade_date,
            "signal_date": signal_date,
            "error": "--ensure-recommendations is retired in production",
        }
    ensure_result = ensure_recommendations_for_signal_date(signal_date)
    if ensure_result.get("status") != "exists":
        return {
            "status": "error",
            "trade_date": trade_date,
            "signal_date": signal_date,
            "error": "上一交易日 AI 推荐不存在，模拟信号池拒绝准备",
            "recommendation_prerequisite": ensure_result,
        }
    engine = SimTradeEngine()
    result = engine.prepare_signal_pool(
        trade_date=trade_date,
        signal_date=signal_date,
        strict=True,
        reset=reset,
    )
    result["recommendation_prerequisite"] = ensure_result
    if result.get("status") == "ok":
        logger.info(
            "模拟交易信号池准备完成: trade_date=%s signal_date=%s allowed=%s rejected=%s counts=%s",
            result.get("trade_date"),
            result.get("signal_date"),
            result.get("allowed_count"),
            result.get("rejected_count"),
            result.get("counts"),
        )
    elif result.get("status") == "skipped":
        logger.info("模拟交易信号池准备跳过: %s", result.get("reason"))
    else:
        logger.warning("模拟交易信号池准备失败: %s", result)
    return result


def run_sim_trade_scan():
    """Run one three-stage simulated trading tick."""
    logger.info("=" * 50)
    logger.info("sim trade event tick start %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    engine = SimTradeEngine()
    result = engine.run_event_tick(auto_prepare=True, strict=True)
    live_sold = len(result.get("sell_signals") or [])
    forward_sold = len(result.get("forward_sell_signals") or [])
    buy_order_count = len(result.get("buy_orders") or [])
    matched = result.get("match_results") or []
    buy_filled = len([r for r in matched if r.get("side") == "BUY" and r.get("status") == "filled"])
    sell_filled = len([r for r in matched if r.get("side") == "SELL" and r.get("status") == "filled"])
    logger.info(
        "sim trade tick done: sell_signals=%d forward_sells=%d buy_orders=%d buy_fills=%d sell_fills=%d expired=%d signal_counts=%s order_counts=%s",
        live_sold,
        forward_sold,
        buy_order_count,
        buy_filled,
        sell_filled,
        result.get("expired_count", 0),
        result.get("signal_counts"),
        result.get("order_counts"),
    )
    logger.info("=" * 50)
    try:
        from biz.analysis.trading_wecom import notify_sim_trade_result
        notification = notify_sim_trade_result(result)
    except Exception as exc:
        notification = {"status": "error", "error": str(exc)[:300]}
    return {
        "sell_count": live_sold + forward_sold,
        "live_sell_count": live_sold,
        "forward_sell_count": forward_sold,
        "buy_count": buy_filled,
        "buy_order_count": buy_order_count,
        "buy_fill_count": buy_filled,
        "sell_fill_count": sell_filled,
        "notification": notification,
        "details": result,
    }


def _nonnegative_int(value: Any) -> int:
    try:
        result = int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0
    return max(0, result)


def _signal_count(output: Mapping[str, Any]) -> int:
    counts = output.get("counts")
    if isinstance(counts, Mapping):
        for key in ("total", "all", "signal_count"):
            if key in counts:
                return _nonnegative_int(counts.get(key))
        return sum(_nonnegative_int(value) for value in counts.values())
    return _nonnegative_int(output.get("signal_count"))


def _empty_identity_hash() -> str:
    return hashlib.sha256(b"[]").hexdigest()


def build_task_receipt(
    output: Mapping[str, Any],
    *,
    task_mode: str,
    requested_trade_date: str,
    requested_signal_date: str,
    started_at: datetime,
    finished_at: datetime,
) -> dict[str, Any]:
    """Build the one scheduler-facing result for a sim task invocation."""

    source_status = str(output.get("status") or "").strip().lower()
    prerequisite = output.get("recommendation_prerequisite")
    prerequisite = prerequisite if isinstance(prerequisite, Mapping) else {}
    trade_date = str(
        output.get("trade_date")
        or requested_trade_date
        or date.today().isoformat()
    )[:10]
    signal_date = str(
        output.get("signal_date")
        or prerequisite.get("signal_date")
        or requested_signal_date
    )[:10]
    if task_mode == "prepare_signals":
        recommendation_count = _nonnegative_int(prerequisite.get("count"))
        total_recommendations = _nonnegative_int(
            output.get("total_recommendations")
        )
        if (
            source_status == "ok"
            and recommendation_count > 0
            and total_recommendations > 0
        ):
            status = "PASS"
        elif source_status == "skipped":
            status = "SKIPPED"
        else:
            status = "DATA_BLOCKED"
    else:
        recommendation_count = _nonnegative_int(prerequisite.get("count"))
        total_recommendations = _nonnegative_int(
            output.get("total_recommendations")
        )
        status = "SKIPPED" if source_status == "skipped" else "PASS"

    receipt: dict[str, Any] = {
        "schema": SIM_TRADE_TASK_RESULT_SCHEMA,
        "receipt_id": uuid.uuid4().hex,
        "status": status,
        "task_mode": task_mode,
        "trade_date": trade_date,
        "signal_date": signal_date or None,
        "recommendation_count": recommendation_count,
        "total_recommendations": total_recommendations,
        "recommendation_code_count": _nonnegative_int(
            output.get("recommendation_code_count")
        ),
        "recommendation_code_set_hash": str(
            output.get("recommendation_code_set_hash")
            or _empty_identity_hash()
        ),
        "strategy_count": _nonnegative_int(output.get("strategy_count")),
        "signal_count": _signal_count(output),
        "allowed_count": _nonnegative_int(output.get("allowed_count")),
        "rejected_count": _nonnegative_int(output.get("rejected_count")),
        "signal_identity_count": _nonnegative_int(
            output.get("signal_identity_count")
        ),
        "signal_identity_hash": str(
            output.get("signal_identity_hash") or _empty_identity_hash()
        ),
        "buy_order_count": _nonnegative_int(output.get("buy_order_count")),
        "buy_fill_count": _nonnegative_int(output.get("buy_fill_count")),
        "sell_fill_count": _nonnegative_int(output.get("sell_fill_count")),
        "started_at": started_at.isoformat(sep=" ", timespec="seconds"),
        "finished_at": finished_at.isoformat(sep=" ", timespec="seconds"),
    }
    error = str(output.get("error") or output.get("reason") or "").strip()
    if error:
        receipt["reason"] = error[:500]
    if task_mode == "prepare_signals" and receipt["status"] == "PASS":
        expected_decisions = (
            receipt["total_recommendations"] * receipt["strategy_count"]
        )
        exact_identity = bool(
            receipt["recommendation_code_count"]
            == receipt["total_recommendations"]
            == receipt["recommendation_count"]
            and receipt["strategy_count"] > 0
            and receipt["allowed_count"] + receipt["rejected_count"]
            == expected_decisions
            and receipt["signal_identity_count"]
            == receipt["signal_count"]
            == receipt["allowed_count"]
            and len(receipt["recommendation_code_set_hash"]) == 64
            and len(receipt["signal_identity_hash"]) == 64
        )
        if not exact_identity:
            receipt["status"] = "DATA_BLOCKED"
            receipt["reason"] = "signal pool identity/count contract is incomplete"
    unsigned = json.dumps(
        receipt,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    receipt["result_sha256"] = hashlib.sha256(unsigned.encode("utf-8")).hexdigest()
    return receipt


def _receipt_exit_code(receipt: Mapping[str, Any]) -> int:
    status = str(receipt.get("status") or "").upper()
    if status in {"PASS", "SKIPPED"}:
        return 0
    if status == "DATA_BLOCKED":
        return 2
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="事件驱动模拟交易任务")
    parser.add_argument("date_arg", nargs="?", default="", help="兼容旧调度传入的日期参数")
    parser.add_argument("--prepare-signals", action="store_true", help="只准备今日信号池")
    parser.add_argument("--tick", action="store_true", help="执行一次事件驱动tick")
    parser.add_argument("--trade-date", default="", help="交易日期，默认今天")
    parser.add_argument("--signal-date", default="", help="信号日期，默认交易日前一交易日")
    parser.add_argument("--reset", action="store_true", help="重置未成交信号后重新准备")
    parser.add_argument("--ensure-recommendations", action="store_true", help="准备信号池前，如上一交易日AI推荐缺失则先严格补生成")
    parser.add_argument("--execution-time", default="", help="严格AI推荐的执行时间，默认当前本地时间")
    parser.add_argument("--top-n", type=int, default=80, help="补生成AI推荐时保留的推荐数量")
    parser.add_argument("--min-score", type=float, default=62.0, help="补生成AI推荐时使用的最低分")
    parser.add_argument("--min-kline-coverage", type=float, default=0.80, help="补生成AI推荐时使用的K线覆盖率")
    parser.add_argument("--json", action="store_true", help="JSON 输出")
    parser.add_argument("--skip-outside-intraday", action="store_true", help="非盘中时段直接跳过tick")
    args = parser.parse_args(argv)

    if args.ensure_recommendations and (
        str(os.environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower()
        == "production"
    ):
        raise RuntimeError(
            "--ensure-recommendations is retired in production; "
            "the AI recommendation task must complete first"
        )

    trade_date = args.trade_date or (args.date_arg if args.date_arg and args.date_arg.startswith("20") else "")
    started_at = datetime.now().replace(microsecond=0)
    if args.prepare_signals:
        output = prepare_signals(
            trade_date=trade_date,
            signal_date=args.signal_date,
            reset=args.reset,
            ensure_recommendations=args.ensure_recommendations,
            execution_time=args.execution_time,
            top_n=args.top_n,
            min_score=args.min_score,
            min_kline_coverage=args.min_kline_coverage,
        )
    else:
        if args.skip_outside_intraday and not _is_intraday_runtime():
            output = {"status": "skipped", "reason": "outside_intraday_runtime"}
        else:
            output = run_sim_trade_scan()

    receipt = build_task_receipt(
        output,
        task_mode="prepare_signals" if args.prepare_signals else "tick",
        requested_trade_date=trade_date,
        requested_signal_date=args.signal_date,
        started_at=started_at,
        finished_at=datetime.now().replace(microsecond=0),
    )

    if args.json:
        print(json.dumps(receipt, ensure_ascii=False, sort_keys=True, default=str))
    else:
        if receipt["status"] == "SKIPPED":
            print("结果: 非盘中时段，跳过")
        elif args.prepare_signals:
            print(
                "结果: 信号池 "
                f"{receipt['status']} trade_date={receipt['trade_date']} "
                f"signal_date={receipt['signal_date']} "
                f"recommendations={receipt['recommendation_count']} "
                f"signals={receipt['signal_count']}"
            )
        else:
            print(
                f"\n结果: 卖出 {_nonnegative_int(output.get('sell_count'))} 笔, "
                f"买入 {_nonnegative_int(output.get('buy_count'))} 笔"
            )
    return _receipt_exit_code(receipt)


if __name__ == "__main__":
    raise SystemExit(main())

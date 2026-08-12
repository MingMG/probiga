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
import json
import sys
import logging
from pathlib import Path as _Path
from datetime import date, datetime, time

_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.engine.sim_trade_engine import SimTradeEngine, _is_trade_date, _previous_trade_date

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _is_intraday_runtime() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    return time(9, 20) <= now.time() <= time(15, 5)


def _recommendation_count(pick_date: str) -> int:
    try:
        with get_engine().connect() as conn:
            return int(conn.execute(
                text("SELECT COUNT(*) FROM st_recommended_stocks WHERE pick_date = :d"),
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
    """Ensure the previous-trading-day AI recommendation exists before signal-pool conversion."""
    signal_date = (signal_date or "")[:10]
    if not signal_date:
        return {"status": "error", "error": "signal_date is required"}
    before_count = _recommendation_count(signal_date)
    if before_count > 0:
        return {"status": "exists", "signal_date": signal_date, "count": before_count}

    from biz.analysis.sync_analysis_fast import run_batch

    execution_time = execution_time or datetime.now().replace(microsecond=0).isoformat(sep=" ")
    logger.warning(
        "AI recommendations missing for %s; running strict recommendation batch before signal preparation",
        signal_date,
    )
    stats = run_batch(
        engine=get_engine(),
        trade_date=signal_date,
        top_n=top_n,
        min_score=min_score,
        strict_prev_trade_day=True,
        execution_time=execution_time,
        min_kline_coverage=min_kline_coverage,
        auto_repair_missing_kline=True,
    )
    after_count = _recommendation_count(signal_date)
    return {
        "status": "generated",
        "signal_date": signal_date,
        "execution_time": execution_time,
        "before_count": before_count,
        "after_count": after_count,
        "analysis_count": stats.analysis_count,
        "recommendation_count": stats.recommendation_count,
        "flow_date": stats.flow_date,
        "hot_date": stats.hot_date,
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
    engine = SimTradeEngine()
    trade_date = (trade_date or date.today().isoformat())[:10]
    signal_date = (signal_date or _previous_trade_date(trade_date))[:10]
    ensure_result = None
    if ensure_recommendations and _is_trade_date(trade_date) and signal_date:
        ensure_result = ensure_recommendations_for_signal_date(
            signal_date,
            execution_time=execution_time,
            top_n=top_n,
            min_score=min_score,
            min_kline_coverage=min_kline_coverage,
        )
    result = engine.prepare_signal_pool(
        trade_date=trade_date,
        signal_date=signal_date,
        strict=True,
        reset=reset,
    )
    if ensure_result is not None:
        result["ensure_recommendations"] = ensure_result
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


if __name__ == "__main__":
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
    args = parser.parse_args()

    trade_date = args.trade_date or (args.date_arg if args.date_arg and args.date_arg.startswith("20") else "")
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

    if args.json:
        print(json.dumps(output, ensure_ascii=False, default=str))
    else:
        if output.get("status") == "skipped":
            print("结果: 非盘中时段，跳过")
        elif args.prepare_signals:
            print(f"结果: 信号池 {output.get('status')} counts={output.get('counts')}")
        else:
            print(f"\n结果: 卖出 {output['sell_count']} 笔, 买入 {output['buy_count']} 笔")

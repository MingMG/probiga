# -*- coding: utf-8 -*-
"""
模拟交易定时任务

盘中每1分钟运行一次（只查推荐股+持仓，请求量极小）：
1. 检查卖出信号 → 执行卖出
2. 检查三种策略的买入信号 → 执行买入

使用方法：
    python -m biz.analysis.sync_sim_trade
"""

import sys
import logging
from pathlib import Path as _Path
from datetime import datetime

_ROOT = _Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from server.engine.sim_trade_engine import SimTradeEngine, STRATEGY_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_sim_trade_scan():
    """
    执行一次模拟交易信号扫描。
    盘中运行，以当前价执行交易。
    """
    logger.info("=" * 50)
    logger.info("模拟交易信号扫描开始 %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    engine = SimTradeEngine()

    # 1. 检查卖出信号
    sell_signals = engine.check_sell_signals()
    logger.info("检测到 %d 个卖出信号", len(sell_signals))

    for sig in sell_signals:
        try:
            ret = engine.execute_sell(sig)
            logger.info(
                "卖出 %s(%s) %s 价格%.2f 盈亏%.2f(%.2f%%) 原因:%s",
                sig["short_name"], sig["stock_code"], sig["strategy_type"],
                sig["sell_price"], sig["profit"], sig["profit_rate"],
                sig["reason_label"],
            )
        except Exception as e:
            logger.error("卖出 %s 失败: %s", sig["stock_code"], e)

    # 2. 检查买入信号(三种策略)
    total_bought = 0
    for stype, cfg in STRATEGY_CONFIG.items():
        try:
            buy_signals = engine.check_buy_signals(stype)
            logger.info("[%s] 检测到 %d 个买入信号", cfg["name"], len(buy_signals))

            for sig in buy_signals:
                try:
                    ret = engine.execute_buy(sig)
                    total_bought += 1
                    logger.info(
                        "买入 %s(%s) %s 价格%.2f 股数%d 金额%.0f AI评分%.1f",
                        sig["short_name"], sig["stock_code"], cfg["name"],
                        sig["price"], sig["shares"], sig["amount"],
                        sig["ai_score"],
                    )
                except Exception as e:
                    logger.error("买入 %s 失败: %s", sig["stock_code"], e)
        except Exception as e:
            logger.error("[%s] 检查买入信号失败: %s", cfg["name"], e)

    logger.info("模拟交易扫描完成: 卖出%d笔, 买入%d笔", len(sell_signals), total_bought)
    logger.info("=" * 50)

    return {
        "sell_count": len(sell_signals),
        "buy_count": total_bought,
    }


if __name__ == "__main__":
    result = run_sim_trade_scan()
    print(f"\n结果: 卖出 {result['sell_count']} 笔, 买入 {result['buy_count']} 笔")

"""Cash-constrained daily-bar research replay; no actual execution claims."""
from __future__ import annotations

from datetime import date, datetime

from server.common.pit_execution_guard import daily_bar_execution_disposition
from server.engine.strategy_statistical_guards import newey_west_nav_statistics
from server.trading_v3.backtest import _execution_fee_with_impact
from server.trading_v2.instrument_registry import _stock_rule
from server.engine.strategy_rule_language import finite


ACCOUNT = {"minimum_commission_cny": 5, "commission_rate": 0.0003,
           "transfer_fee_rate": 0.00001, "sell_stamp_duty_rate": 0.0005, "default_slippage_rate": 0.001}


def replay_rule(rule, signals, bars, sessions, *, initial_cash=250000.0, evaluation_start=None):
    """Stops observed in a daily bar schedule the next executable open exit."""
    if sessions != sorted(set(sessions)) or not sessions:
        raise ValueError("研究回放交易日必须唯一且有序")
    evaluation_start = evaluation_start or sessions[0]
    if evaluation_start not in sessions:
        raise ValueError("回测统计起点不在行情窗口内")
    prices = {}
    for row in bars:
        identity = (str(row["trade_date"])[:10], str(row["stock_code"]))
        if identity in prices:
            raise ValueError("研究回放行情业务键重复")
        prices[identity] = row
    entries = {}
    dispositions, trades, nav, positions = [], [], [], {}
    for signal in signals:
        known = datetime.fromisoformat(signal["signal_known_at"])
        possible = [day for day in sessions if datetime.fromisoformat(day + " 09:30:00") > known]
        if not possible:
            dispositions.append({"stock_code": signal["stock_code"], "status": "RIGHT_CENSORED", "reason": "尚无决策后的交易日"})
        else:
            entries.setdefault(possible[0], []).append(signal)
    cash, previous_nav = float(initial_cash), float(initial_cash)
    invalid_nav = False
    for index, day in enumerate(sessions):
        # Opening exits and entries use the same opening event; an exit cannot
        # fund an entry retroactively. Entry budgets use cash carried overnight.
        opening_cash = cash
        for code in list(positions):
            position = positions[code]
            row = prices.get((day, code))
            previous = prices.get((sessions[index - 1], code)) if index else None
            reference = finite(row.get("pre_close")) if row else None
            prior_close = finite(previous.get("close")) if previous else None
            if (reference is not None and prior_close is not None and position["entry_index"] < index
                    and abs(reference - prior_close) > max(0.011, prior_close * 1e-4)):
                invalid_nav = True
                position["cashflow_unknown"] = True
                dispositions.append({"stock_code": code, "trade_date": day, "status": "DATA_BLOCKED", "reason": "公司行动或价格版本变化缺少可核对现金流证据"})
            if position["exit_pending"]:
                disposition = daily_bar_execution_disposition(row, side="SELL")
                if not disposition["executable"]:
                    dispositions.append({"stock_code": code, "trade_date": day, "side": "SELL", **disposition})
                    continue
                price = float(disposition["open_price"])
                value = position["quantity"] * price
                amount = float(row["amount"])
                fee, impact = _execution_fee_with_impact(value, account=ACCOUNT, sell=True,
                    participation_rate=value / amount, maximum_participation_rate=0.01)
                if value / amount > 0.01:
                    dispositions.append({"stock_code": code, "trade_date": day, "side": "SELL", "status": "KNOWN_UNFILLED", "reason": "流动性约束，顺延退出"})
                    continue
                cash += value - fee
                pnl = (value - fee - position["entry_value"] - position["entry_fee"]
                       if not position.get("cashflow_unknown") else None)
                trades.append({"stock_code": code, "stock_name": position["stock_name"], "entry_at": position["entry_day"] + " 09:30:00", "exit_at": day + " 09:30:00",
                    "signal_known_at": position["signal_known_at"], "entry_price": position["entry_price"], "exit_price": price,
                    "quantity": position["quantity"], "estimated_fee_cny": position["entry_fee"] + fee,
                    "estimated_impact_cny": position["entry_impact"] + impact, "net_pnl_cny": pnl,
                    "instrument_rule_version": position["instrument_rule_version"],
                    "net_return_pct": pnl / (position["entry_value"] + position["entry_fee"]) * 100 if pnl is not None else None,
                    "exit_reason": position["exit_reason"], "execution_mode": "MODELED_DAILY_BAR", "funding_eligible": False})
                del positions[code]
        for signal in sorted(entries.get(day, []), key=lambda s: (s["rank"], s["stock_code"])):
            code, row = signal["stock_code"], prices.get((day, signal["stock_code"]))
            disposition = daily_bar_execution_disposition(row, side="BUY")
            if not disposition["executable"]:
                dispositions.append({"stock_code": code, "trade_date": day, "side": "BUY", **disposition})
                continue
            if not signal.get("industry"):
                dispositions.append({"stock_code": code, "trade_date": day, "status": "DATA_BLOCKED", "reason": "缺少决策时点可用的行业证据"})
                continue
            if not signal.get("stock_name"):
                dispositions.append({"stock_code": code, "trade_date": day, "status": "DATA_BLOCKED", "reason": "缺少冻结证券名称，无法检查特殊处理状态"})
                continue
            instrument = _stock_rule({"stock_code": code, "short_name": signal["stock_name"]}, date.fromisoformat(day))
            if not instrument["can_buy"]:
                dispositions.append({"stock_code": code, "trade_date": day, "status": "KNOWN_UNFILLED", "reason": "冻结特殊处理状态不允许买入"})
                continue
            if code in positions or len(positions) >= 5:
                dispositions.append({"stock_code": code, "trade_date": day, "status": "KNOWN_UNFILLED", "reason": "重复持仓、仓位容量或行业证据限制"})
                continue
            industry_value = sum(p["entry_value"] for p in positions.values() if p["industry"] == signal["industry"])
            price = float(disposition["open_price"])
            prior_amounts = [float(prices[(prior, code)]["amount"]) for prior in sessions[max(0, index - 20):index]
                             if (prior, code) in prices and finite(prices[(prior, code)].get("amount")) is not None
                             and float(prices[(prior, code)]["amount"]) >= 0]
            if len(prior_amounts) != 20:
                dispositions.append({"stock_code": code, "trade_date": day, "status": "DATA_BLOCKED", "reason": "缺少入场前20个交易日流动性证据"})
                continue
            cap = sum(prior_amounts) / 20 * 0.01
            budget = min(initial_cash * 0.1, initial_cash * 0.3 - industry_value, opening_cash, cash, cap)
            lot = instrument["buy_lot_size"]
            quantity = max(0, int(budget / (price * 1.01) / lot) * lot)
            if quantity < instrument["first_buy_minimum"]:
                dispositions.append({"stock_code": code, "trade_date": day, "status": "KNOWN_UNFILLED", "reason": "现金或成交容量不足一手"})
                continue
            value = quantity * price
            if value / float(row["amount"]) > 0.01:
                dispositions.append({"stock_code": code, "trade_date": day, "status": "KNOWN_UNFILLED", "reason": "事后成交容量不足，原订单不按未来成交额缩量重算"})
                continue
            fee, impact = _execution_fee_with_impact(value, account=ACCOUNT, sell=False,
                participation_rate=value / (sum(prior_amounts) / 20), maximum_participation_rate=0.01)
            if value + fee > min(opening_cash, cash):
                dispositions.append({"stock_code": code, "trade_date": day, "status": "KNOWN_UNFILLED", "reason": "含费用后现金不足"})
                continue
            cash -= value + fee
            opening_cash -= value + fee
            positions[code] = {"quantity": quantity, "stock_name": signal["stock_name"], "industry": signal["industry"], "entry_value": value,
                "entry_fee": fee, "entry_impact": impact, "entry_price": price, "entry_day": day,
                "entry_index": index, "signal_known_at": signal["signal_known_at"],
                "instrument_rule_version": instrument["rule_version"],
                "stop": price * (1 - float(rule["stop_loss_pct"]) / 100), "exit_pending": False, "exit_reason": ""}
        marked = 0.0
        for code, position in positions.items():
            row = prices.get((day, code))
            if row is None or float(row.get("close") or 0) <= 0:
                invalid_nav = True
                position["cashflow_unknown"] = True
                dispositions.append({"stock_code": code, "trade_date": day, "status": "DATA_BLOCKED", "reason": "持仓估值缺行情"})
                continue
            marked += position["quantity"] * float(row["close"])
            if float(row["low"]) <= position["stop"] or index - position["entry_index"] + 1 >= rule["holding_days"]:
                position.update(exit_pending=True, exit_reason="保护止损" if float(row["low"]) <= position["stop"] else "持有期到期")
        equity = cash + marked
        if cash < -0.01:
            raise ValueError("研究模拟发生现金透支")
        if day >= evaluation_start:
            nav.append({"trade_date": day, "nav_cny": equity if not invalid_nav else None,
                        "return_pct": (equity / previous_nav - 1) * 100 if not invalid_nav and previous_nav > 0 else None})
        previous_nav = equity
    exclusions = [d for d in dispositions if d["status"] == "DATA_BLOCKED"]
    partial = bool(exclusions) and not invalid_nav
    statistics = newey_west_nav_statistics(nav) if not invalid_nav and not partial else None
    return {"status": "BLOCKED" if invalid_nav else "PARTIAL" if partial else "COMPLETE", "execution_mode": "MODELED_DAILY_BAR",
            "initial_cash_cny": initial_cash, "trades": trades, "dispositions": dispositions, "nav": nav,
            "open_position_count": len(positions), "completed_trade_count": len(trades),
            "net_return_pct": (previous_nav / initial_cash - 1) * 100 if not invalid_nav and not partial else None,
            "verified_input_subset_return_pct": (previous_nav / initial_cash - 1) * 100 if not invalid_nav else None,
            "input_exclusion_count": len(exclusions),
            "return_scope": "VERIFIED_INPUT_SUBSET" if partial else "FULL_REQUESTED_INPUTS" if not invalid_nav else "UNKNOWN",
            "statistical_evidence": statistics, "fees": {"kind": "ESTIMATE", **ACCOUNT},
            "out_of_sample_status": "RETROSPECTIVE_NOT_PRISTINE", "funding_eligible": False,
            "real_order_authority": False, "automatic_real_order_submission": False}

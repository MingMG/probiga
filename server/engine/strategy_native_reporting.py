"""Reconcile persisted native replay evidence before publishing performance."""

from collections import Counter
from decimal import Decimal, ROUND_HALF_UP
from math import sqrt
from statistics import mean, stdev

from server.common.research_execution_math import _execution_fee_with_impact


def _decimal(value):
    return Decimal(str(value))


def _money(value):
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def reconcile_native(report, frozen_prices):
    cash = _decimal(report["initial_cash_cny"])
    positions = Counter()
    days, differences = [], []
    ledger, nav = report["ledger"], report["nav"]
    dates = [row["trade_date"] for row in nav]
    if not dates or dates != sorted(set(dates)) or dates != [row["trade_date"] for row in ledger]:
        return {"status": "FAILED", "differences": [{"check": "DAILY_NAV_COVERAGE"}], "days": []}

    for day, observed, equity in zip(dates, ledger, nav):
        issues, fees = [], Decimal(0)
        sides = Counter()
        for fill in observed["fills"]:
            code, side, quantity = fill["stock_code"], fill["side"], fill["quantity"]
            if side not in ("BUY", "SELL") or type(quantity) is not int or quantity <= 0:
                issues.append({"check": "INVALID_FILL", "stock_code": code})
                continue
            price, fee = _decimal(fill["price"]), _decimal(fill["fee_cny"])
            if not price.is_finite() or price <= 0 or not fee.is_finite() or fee < 0:
                issues.append({"check": "INVALID_FILL_PRICE_OR_FEE", "stock_code": code})
                continue
            if frozen_prices.get((day, code), {}).get("open") != float(price):
                issues.append({"check": "FROZEN_OPEN_PRICE", "stock_code": code, "side": side})
            sell = side == "SELL"
            expected_fee, _ = _execution_fee_with_impact(float(price) * quantity,
                account=report["fees"], sell=sell, participation_rate=fill["participation_rate"],
                maximum_participation_rate=.01)
            if _money(fee) != _money(_decimal(expected_fee)):
                issues.append({"check": "FILL_FEE", "stock_code": code, "side": side})
            value = price * quantity
            if sell and positions[code] < quantity:
                issues.append({"check": "SELL_EXCEEDS_POSITION", "stock_code": code})
            positions[code] += -quantity if sell else quantity
            cash += (value if sell else -value) - fee
            fees += fee
            sides[side] += 1

        expected_positions = {code: quantity for code, quantity in positions.items() if quantity}
        marks = {mark["stock_code"]: mark for mark in observed["marks"]}
        if len(marks) != len(observed["marks"]) or expected_positions != {
                code: mark["quantity"] for code, mark in marks.items()}:
            issues.append({"check": "POSITION_MARK_COVERAGE"})
        marked_value = Decimal(0)
        for code, mark in marks.items():
            close = _decimal(mark["close"])
            if not close.is_finite() or close <= 0:
                issues.append({"check": "INVALID_CLOSE", "stock_code": code})
                continue
            if frozen_prices.get((day, code), {}).get("close") != float(close):
                issues.append({"check": "FROZEN_CLOSE_PRICE", "stock_code": code})
            marked_value += expected_positions.get(code, 0) * close
        for check, computed, saved in [
            ("CASH", cash, observed["cash_cny"]), ("HOLDINGS", marked_value, observed["holdings_cny"]),
            ("NAV", cash + marked_value, equity["nav_cny"]),
        ]:
            if saved is None or not _decimal(saved).is_finite():
                issues.append({"check": check, "difference_cny": None})
            elif (delta := _money(computed - _decimal(saved))) != 0:
                issues.append({"check": check, "difference_cny": float(delta)})
        differences.extend({"trade_date": day, **issue} for issue in issues)
        days.append({"trade_date": day, "cash_cny": float(_money(cash)),
                     "holdings_cny": float(_money(marked_value)), "fees_cny": float(_money(fees)),
                     "nav_cny": float(_money(cash + marked_value)), "buy_fills": sides["BUY"],
                     "sell_fills": sides["SELL"]})
    return {"status": "FAILED" if differences else "PASSED", "differences": differences,
            "currency_precision_cny": 0.01, "days": days}


def native_report_metrics(report, receipt, signal_counts):
    daily = []
    for row in receipt["days"]:
        day = row["trade_date"]
        daily.append({**row, **signal_counts.get(day, {}),
                      "filled_orders": row["buy_fills"] + row["sell_fills"]})
    reason_counts = Counter(item["reason"] for item in report["dispositions"]
                            if item["status"] == "KNOWN_UNFILLED")
    returns = [row["return_pct"] for row in report["nav"] if row["return_pct"] is not None]
    pnls = [row["net_pnl_cny"] for row in report["trades"] if row["net_pnl_cny"] is not None]
    wins, losses = [pnl for pnl in pnls if pnl > 0], [-pnl for pnl in pnls if pnl < 0]
    peak, drawdown = report["initial_cash_cny"], 0.0
    for row in daily:
        peak = max(peak, row["nav_cny"])
        drawdown = max(drawdown, (peak - row["nav_cny"]) / peak * 100)
    volatility = stdev(returns) if len(returns) > 1 else 0
    return {"daily": daily, "unfilled_by_reason": dict(sorted(reason_counts.items())),
            "one_price_unfilled": {"buy": reason_counts["LOCKED_LIMIT_UP"], "sell": reason_counts["LOCKED_LIMIT_DOWN"]},
            "metrics": {"max_drawdown_pct": drawdown, "annualized_sharpe": mean(returns) / volatility * sqrt(252) if volatility else None,
                        "win_loss_ratio": mean(wins) / mean(losses) if wins and losses else None,
                        "risk_free_rate_pct": 0, "annualization_sessions": 252},
            "notes": {
                "t_plus_one": "T+1 约束生效，拒单未单列，未计入其他未成交。",
                "price_limits": "采用一字板近似，普通触板成交率可能被高估。",
                "open_positions": "期末仍有持仓时，已平仓记录只覆盖部分成交。",
            }}

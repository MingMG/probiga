# -*- coding: utf-8 -*-
"""Pure portfolio calculations shared by API handlers and tests."""

PORTFOLIO_COMMISSION_RATE = 0.00025
PORTFOLIO_MIN_COMMISSION = 5.0
PORTFOLIO_STAMP_DUTY_RATE = 0.0005
PORTFOLIO_TRANSFER_FEE_RATE = 0.00001


def portfolio_cost_profit(shares: int, cur_price: float, cost_price: float) -> float | None:
    """持仓盈亏：(现价 - 成本价) × 股数。成本价会随加减仓摊薄。"""
    sh = int(shares or 0)
    pr = float(cur_price or 0)
    if sh <= 0 or pr <= 0 or cost_price is None:
        return None
    cp = float(cost_price or 0)
    return round(sh * (pr - cp), 2)


def portfolio_trade_fee(trans_type: str, price: float, shares: int) -> float:
    amount = max(float(price or 0), 0) * max(int(shares or 0), 0)
    if amount <= 0:
        return 0.0

    commission = max(amount * PORTFOLIO_COMMISSION_RATE, PORTFOLIO_MIN_COMMISSION)
    transfer_fee = amount * PORTFOLIO_TRANSFER_FEE_RATE
    stamp_duty = amount * PORTFOLIO_STAMP_DUTY_RATE if str(trans_type or "").lower() == "sell" else 0.0
    return commission + transfer_fee + stamp_duty


def portfolio_calc_next_position(
    trans_type: str,
    old_cost: float,
    old_shares: int,
    price: float,
    shares: int,
) -> dict:
    t = str(trans_type or "").strip().lower()
    old_sh = max(0, int(old_shares or 0))
    qty = int(shares or 0)
    px = float(price or 0)
    if t not in ("buy", "sell"):
        return {"status": "error", "error": "交易类型无效"}
    if px <= 0 or qty <= 0:
        return {"status": "error", "error": "请输入有效价格和股数"}
    if t == "sell" and old_sh <= 0:
        return {"status": "error", "error": "当前无持仓，不能卖出"}
    trade_shares = min(qty, old_sh) if t == "sell" else qty
    new_shares = (old_sh - trade_shares) if t == "sell" else (old_sh + trade_shares)
    return {
        "status": "ok",
        "trans_type": t,
        "trade_shares": trade_shares,
        "new_shares": new_shares,
    }


def portfolio_recalc_cost_from_history(
    stock_code: str,
    read_sql_fn,
) -> dict:
    """从交易流水表全量重算成本价（东财算法，忽略手续费）。

    成本价 = (累计买入总额 - 累计卖出总额) / 当前持仓
    当前持仓 = 累计买入股数 - 累计卖出股数

    read_sql_fn: callable(sql, params) -> list[dict]
    """
    rows = read_sql_fn(
        "SELECT trans_type, price, shares FROM st_portfolio_trans_log "
        "WHERE stock_code = :c ORDER BY created_at, id",
        {"c": stock_code},
    )
    total_buy_amount = 0.0
    total_sell_amount = 0.0
    total_buy_shares = 0
    total_sell_shares = 0
    for r in rows:
        tt = str(r.get("trans_type") or "").strip().lower()
        px = float(r.get("price") or 0)
        sh = int(r.get("shares") or 0)
        if tt == "buy":
            total_buy_amount += px * sh
            total_buy_shares += sh
        elif tt == "sell":
            total_sell_amount += px * sh
            total_sell_shares += sh

    current_shares = total_buy_shares - total_sell_shares
    if current_shares <= 0:
        return {
            "status": "ok",
            "new_shares": max(current_shares, 0),
            "new_cost": 0.0,
            "total_buy_amount": round(total_buy_amount, 2),
            "total_sell_amount": round(total_sell_amount, 2),
        }

    cost_price = (total_buy_amount - total_sell_amount) / current_shares
    return {
        "status": "ok",
        "new_shares": current_shares,
        "new_cost": round(cost_price, 4),
        "total_buy_amount": round(total_buy_amount, 2),
        "total_sell_amount": round(total_sell_amount, 2),
    }

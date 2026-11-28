"""Read accepted pairs only. Mark entry-day inventory to the archived closing bid."""

from collections import Counter, defaultdict
from decimal import Decimal

from sqlalchemy import text

from server.trading_v3.intraday_pair import ENTITY, VERSION, _read

D = Decimal


def daily_pnl(arm):
    buy = arm.get("entry")
    if not buy or buy["quantity"] is None or buy["status"] == "RISK_REJECTED":
        return None
    first, last = arm["entry_at"][:10], arm["exit_at"][:10]
    n = buy["quantity"]
    if n == 0:
        return {first: D(0), last: D(0)}
    sell = arm.get("exit")
    mark = arm.get("entry_close_mark")
    if not sell or sell["quantity"] != n or not mark:
        return None
    inventory = D(str(mark["bid1"])) * n
    return {
        first: inventory - D(buy["gross"]) - D(buy["fee"]),
        last: D(sell["gross"]) - D(sell["fee"]) - inventory,
    }


def metrics(pnl, capital):
    if not pnl:
        return None
    value = peak = capital
    worst_day, worst_return, drawdown = None, None, D(0)
    for day, change in sorted(pnl.items()):
        ret = change / value
        value += change
        peak = max(peak, value)
        drawdown = max(drawdown, 1 - value / peak)
        if worst_return is None or ret < worst_return:
            worst_day, worst_return = day, ret
    return dict(
        net_return_pct=float((value / capital - 1) * 100),
        max_drawdown_pct=float(drawdown * 100),
        worst_day=worst_day,
        worst_day_return_pct=float(worst_return * 100),
    )


def report(c):
    head = _read(c, VERSION)
    if head is None:
        return {"status": "NOT_REGISTERED"}
    # At most 40 non-overlapping three-session observations per registered experiment.
    keys = (
        c.execute(
            text("""SELECT /*+ MAX_EXECUTION_TIME(1500) */ DISTINCT entity_key
        FROM st_strategy_governance_audit WHERE entity_type=:entity AND entity_key LIKE :prefix
        ORDER BY entity_key LIMIT 42"""),
            {"entity": ENTITY, "prefix": VERSION + "/20%"},
        )
        .scalars()
        .all()
    )
    totals = {arm: defaultdict(Decimal) for arm in head["contract"]["arms"]}
    complete_inputs = {arm: defaultdict(Decimal) for arm in totals}
    feature_complete_keys = []
    statuses = Counter()
    included = []
    pairs = []
    for key in keys:
        pair = _read(c, key)
        pairs.append(
            {
                k: val
                for k, val in pair.items()
                if k not in ("sources", "rule_snapshot", "fee_snapshot")
            }
        )
        statuses[pair["status"]] += 1
        if (
            pair.get("segment") != "HOLDOUT"
            or pair["status"] != "OBSERVATIONS_RECORDED"
        ):
            continue
        values = {arm: daily_pnl(a) for arm, a in pair["arms"].items()}
        if set(values) != set(totals) or any(
            value is None for value in values.values()
        ):
            continue
        included.append(key)
        if pair["non_probability_inputs_complete"]:
            feature_complete_keys.append(key)
        days = sorted(set().union(*[set(p) for p in values.values()]))
        for arm, pnl in values.items():
            for day in days:
                totals[arm][day] += pnl.get(day, D(0))
                if pair["non_probability_inputs_complete"]:
                    complete_inputs[arm][day] += pnl.get(day, D(0))
    return {
        "registration": head,
        "pair_status_counts": dict(statuses),
        "pairs": pairs,
        "covered_holdout_pair_keys": included,
        "feature_complete_holdout_observations": feature_complete_keys,
        "feature_complete_observation_metrics": {
            arm: metrics(pnl, D(head["contract"]["capital"]))
            for arm, pnl in complete_inputs.items()
        },
        "population": "paired prospective observations only",
        "original_strategy_qualified_signals": 0,
        "covered_holdout_metrics": {
            arm: metrics(pnl, D(head["contract"]["capital"]))
            for arm, pnl in totals.items()
        },
    }

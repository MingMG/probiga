"""Prospective, isolated research pairs. Neither arm can submit an OMS order."""

import json
from dataclasses import asdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

from sqlalchemy import text

from server.common.strategy_audit_heads import latest_head
from server.engine.strategy_governance import _append_audit_connection, _digest
from server.trading_v2.domain import (
    AccountSnapshot,
    IntentAction,
    OrderSide,
    Quote,
    TradeIntent,
)
from server.trading_v2.execution import _fee_profile, _price_limit
from server.trading_v2.matcher import PaperMatcher
from server.trading_v2.planner import _instrument_rule
from server.trading_v2.policy import RiskAdjudicator, load_portfolio_policy
from server.trading_v3.config import load_v3_config
from server.trading_v3.intraday_pair_features import snapshot
from server.trading_v3.sleeves import sleeve_required_features

ENTITY = "INTRADAY_ENTRY_PAIR"
VERSION = "intraday_surprise.entry_pair.v1"
D = Decimal
QUOTE_FIELDS = """quote_event_id,stock_code,quote_at,received_at,source_provider,source_batch_id,
    bid1,ask1,bid1_volume,ask1_volume,last_price,pre_close,upper_limit,lower_limit,suspended"""


def contract():
    v3 = load_v3_config()
    policy = load_portfolio_policy()
    return dict(
        version=VERSION,
        arms=["INTRADAY_LIMIT", "NEXT_SESSION_LIMIT"],
        design="one prospective observed L1 event every third exchange session; 120 signal-calendar sessions",
        holdout_from_session=61,
        calibration="UNKNOWN",
        capital=str(policy.initial_cash),
        risk_policy_hash=policy.config_hash,
        target_weight="0.01",
        stop_pct="3.5",
        premium_pct=str(v3["paper_execution"]["maximum_entry_premium_pct"]),
        slippage=str(v3["account"]["default_slippage_rate"]),
        sample_window="10:05-11:25 or 13:10-14:45 Asia/Shanghai",
        sampling="first scheduled poll with a fresh ordinary A share within latest 8 QMT events; lexicographic code",
        signal="cold start observation; no qualified original signal while fill probability is unknown",
        features="5 closed minutes, same slots over 20 prior sessions, population sigma; 5-minute VWAP; PIT SW2 equal-weight vs SSE Composite",
        execution="single first subsequent complete L1 check within 15s; one opposing-depth consumption, IOC remainder",
        entry="A effective after sampling; B 09:35 next session; common price ceiling and requested quantity",
        exit="09:35 first session after own buy; full or partial; residual exposure is unresolved",
        fees=policy.fee_profile_version,
        rules=policy.instrument_rule_version,
        code_hash=_implementation_hash(),
        real_order_authority=False,
    )


def _implementation_hash():
    import hashlib
    from server.trading_v3 import intraday_pair_features
    from server.trading_v2 import matcher, policy, ledger

    h = hashlib.sha256()
    for filename in (
        __file__,
        intraday_pair_features.__file__,
        matcher.__file__,
        policy.__file__,
        ledger.__file__,
    ):
        h.update(
            Path(filename).read_text(encoding="utf-8").replace("\r\n", "\n").encode()
        )
    return h.hexdigest()


def _read(c, key):
    head = latest_head(c, ENTITY, key)
    if not head:
        return None
    row = (
        c.execute(
            text(
                "SELECT payload_json,audit_hash,after_json FROM st_strategy_governance_audit WHERE audit_id=:id"
            ),
            {"id": head["audit_id"]},
        )
        .mappings()
        .one()
    )
    payload, state = json.loads(row["payload_json"]), json.loads(row["after_json"])
    if _digest(payload) != row["audit_hash"] or payload["after"] != state:
        raise ValueError("paired study checkpoint integrity differs")
    return state


def _save(c, key, action, state):
    _append_audit_connection(
        c,
        entity_type=ENTITY,
        entity_key=key,
        action=action,
        operator="trading_v2_paper_tick",
        reason="盘中配对模拟研究",
        before=None,
        after=state,
        evidence={"version": VERSION, "real_order_authority": False},
    )


def register(c, now):
    spec = contract()
    head = _read(c, VERSION)
    if head:
        if head["contract"] != spec:
            raise ValueError(
                "paired study contract changed; register a distinct version"
            )
        return head
    start = c.execute(
        text(
            "SELECT MIN(trade_date) FROM si_trade_calendar WHERE trade_status=1 AND trade_date>:d"
        ),
        {"d": now.date()},
    ).scalar_one()
    if not start:
        raise ValueError("next authoritative exchange session unavailable")
    parent = (
        c.execute(
            text("""SELECT r.current_version,v.version_hash,v.parameters_json
        FROM st_strategy_registry r JOIN st_strategy_version v
        ON v.strategy_key=r.strategy_key AND v.version=r.current_version
        WHERE r.strategy_key='intraday_surprise' AND v.source_kind='immutable_v3_sleeve'""")
        )
        .mappings()
        .one()
    )
    if (
        json.loads(parent["parameters_json"])["shadow_execution_policy"]["entry_timing"]
        != "NEXT_AUTHORITATIVE_SESSION"
    ):
        raise ValueError(
            "paired next-session arm differs from the registered entry policy"
        )
    arm_versions = {
        arm: {"version": VERSION + "/" + arm, "hash": _digest([spec, arm])}
        for arm in spec["arms"]
    }
    head = {
        "contract": spec,
        "source_strategy_version": parent["current_version"],
        "source_strategy_hash": parent["version_hash"],
        "hash": _digest(spec),
        "registered_at": str(now),
        "first_session": str(start),
        "arm_versions": arm_versions,
    }
    _save(c, VERSION, "REGISTER", head)
    return head


def _observed_quote(c, now):
    ids = (
        c.execute(
            text("""SELECT /*+ MAX_EXECUTION_TIME(1200) */ quote_event_id
        FROM st_quote_event_v2 FORCE INDEX(idx_quote_event_v2_time_quality)
        WHERE quote_at BETWEEN :start AND :end AND received_at BETWEEN quote_at AND :end
        AND source_provider='gj_big_qmt_inner' ORDER BY quote_at DESC,received_at DESC,source_provider DESC,
        bid1 DESC,ask1 DESC,bid1_volume DESC,ask1_volume DESC,quote_event_id DESC LIMIT 8"""),
            {"start": now - timedelta(seconds=15), "end": now},
        )
        .scalars()
        .all()
    )
    if not ids:
        return None
    from sqlalchemy import bindparam

    rows = (
        c.execute(
            text(f"""SELECT /*+ MAX_EXECUTION_TIME(1500) */ {QUOTE_FIELDS} FROM st_quote_event_v2 FORCE INDEX(PRIMARY)
        WHERE quote_event_id IN :ids ORDER BY quote_at DESC,received_at DESC,quote_event_id""").bindparams(
                bindparam("ids", expanding=True)
            ),
            {"ids": ids},
        )
        .mappings()
        .all()
    )
    by_code = {}
    for r in rows:
        if (
            r["stock_code"].startswith(("60", "00"))
            and not r["suspended"]
            and r["last_price"]
            and r["last_price"] > 0
            and r["bid1"]
            and r["ask1"]
            and 0 < r["bid1"] <= r["ask1"]
            and min(r["ask1_volume"] or 0, r["bid1_volume"] or 0) > 0
        ):
            by_code.setdefault(r["stock_code"], dict(r))
    return by_code[min(by_code)] if by_code else None


def _specs(c, code, known, protocol):
    day = known.date()
    rule = _instrument_rule(
        c, stock_code=code, effective_date=day, rule_version=protocol["rules"]
    )
    fee = _fee_profile(
        c, version=protocol["fees"], security_type="A_SHARE", trade_date=day
    )
    if (
        not rule
        or rule.security_type != "A_SHARE"
        or rule.settlement_days != 1
        or not fee
    ):
        return None, None
    published = c.execute(
        text("""SELECT
        (SELECT created_at FROM st_instrument_rule_v2 WHERE stock_code=:code AND rule_version=:rules
         AND effective_from=:start) AS rule_at,
        (SELECT created_at FROM st_fee_profile_v2 WHERE fee_profile_version=:fees AND security_type='A_SHARE'
         AND effective_from<=:d AND (effective_to IS NULL OR effective_to>=:d)
         AND confirmation_status IN ('CONFIRMED','PAPER_ASSUMPTION')
         ORDER BY effective_from DESC LIMIT 1) AS fee_at"""),
        dict(
            code=code,
            rules=rule.rule_version,
            start=rule.effective_from,
            fees=fee.version,
            d=day,
        ),
    ).one()
    if any(stamp is None or stamp > known for stamp in published):
        return None, None
    return rule, fee


def _plan(c, head, day, number, dates, observation, quote, features, sources, accepted):
    spec = head["contract"]
    code = quote["stock_code"]
    rule, fee = _specs(c, code, accepted, spec)
    key = VERSION + "/" + str(day)
    state = dict(
        session=number,
        key=key,
        segment="HOLDOUT" if number >= 61 else "CALIBRATION",
        as_of=str(observation),
        accepted_at=str(accepted),
        stock_code=code,
        features=features,
        sources=sources,
        kind="OBSERVATION_ONLY",
        original_signal=False,
        protocol_hash=head["hash"],
        pair_id=_digest([key, quote["quote_event_id"], str(observation), head["hash"]]),
        arms={},
    )
    state["missing_inputs"] = [
        k
        for k in sleeve_required_features("intraday_surprise")
        if features.get(k) is None
    ]
    state["non_probability_inputs_complete"] = not (
        set(state["missing_inputs"]) - {"fill_probability"}
    )
    if not rule or rule.validate_for_buy() or rule.fee_profile_version != spec["fees"]:
        state["status"] = "RULE_OR_FEE_UNAVAILABLE"
        return state
    limit = (
        quote["last_price"] * (1 + D(spec["premium_pct"]) / 100) / rule.tick_size
    ).to_integral_value(rounding=ROUND_DOWN) * rule.tick_size
    capital = D(spec["capital"])
    qty = rule.floor_buy_quantity(int(capital * D(spec["target_weight"]) / limit))
    stop = limit * (1 - D(spec["stop_pct"]) / 100)
    intent = TradeIntent(
        key,
        key,
        key,
        VERSION,
        code,
        IntentAction.OPEN,
        0,
        qty,
        D(spec["target_weight"]),
        accepted,
        accepted + timedelta(seconds=15),
        limit,
        limit,
        stop,
        stop,
        "study stop risk budget",
        "RESEARCH_LABEL",
        (),
        key,
    )
    # Each independent pair starts flat and fully funded. RISK_OFF is a fixed
    # conservative research risk budget, not a reconstructed market regime.
    risk = RiskAdjudicator().adjudicate(
        intent,
        AccountSnapshot(key, capital, capital, capital),
        rule,
        market_regime="RISK_OFF",
        estimated_fee=fee.calculate(OrderSide.BUY, limit * qty, quantity=qty),
    )
    state.update(
        limit_price=str(limit),
        requested=qty,
        quantity=risk.approved_quantity,
        risk_status=risk.status.value,
        risk_reason=risk.first_failure,
        rule_snapshot=asdict(rule),
        fee_snapshot=asdict(fee),
    )
    if risk.approved_quantity <= 0:
        state["status"] = "RISK_REJECTED"
        return state
    # Pending conditional probes are journalled together before either is eligible.
    for arm, entry, exit_day in zip(
        spec["arms"],
        [accepted + timedelta(seconds=15), datetime.combine(dates[1], time(9, 35))],
        dates[1:3],
    ):
        state["arms"][arm] = {
            "arm_version": VERSION + "/" + arm,
            "entry_at": str(entry),
            "exit_at": str(datetime.combine(exit_day, time(9, 35))),
        }
    state["status"] = "PENDING"
    return state


def _match(c, state, arm, side, now, protocol):
    slot = "entry" if side == OrderSide.BUY else "exit"
    effective = datetime.fromisoformat(arm[slot + "_at"])
    deadline = effective + timedelta(seconds=15)
    if now <= deadline:
        return None
    qty = state["quantity"] if slot == "entry" else arm["entry"]["quantity"]
    code = state["stock_code"]
    ev = (
        c.execute(
            text(f"""SELECT /*+ MAX_EXECUTION_TIME(900) */ {QUOTE_FIELDS}
        FROM st_quote_event_v2 FORCE INDEX(idx_quote_event_v2_latest)
        WHERE stock_code=:code AND quote_at>:start AND quote_at<=:end AND received_at>=quote_at
        AND received_at<=:end AND source_provider='gj_big_qmt_inner'
        ORDER BY received_at,quote_at,quote_event_id LIMIT 1"""),
            {"code": code, "start": effective, "end": deadline},
        )
        .mappings()
        .first()
    )
    rule, fee = _specs(c, code, effective, protocol)
    if not ev or not rule or not fee:
        return {
            "status": "COVERAGE_INSUFFICIENT",
            "quantity": None,
            "effective_at": str(effective),
        }
    r = dict(ev)
    evidence = {
        "quote": r,
        "effective_at": str(effective),
        "rule_version": rule.rule_version,
        "fee_version": fee.version,
    }
    if (
        not r["bid1"]
        or not r["ask1"]
        or not 0 < r["bid1"] <= r["ask1"]
        or min(r["bid1_volume"] or 0, r["ask1_volume"] or 0) <= 0
        or not r["pre_close"]
        or r["pre_close"] <= 0
    ):
        return {**evidence, "status": "COVERAGE_INSUFFICIENT", "quantity": None}
    upper = r["upper_limit"] or _price_limit(
        r["pre_close"], rule.limit_ratio, rule.tick_size, upper=True
    )
    lower = r["lower_limit"] or _price_limit(
        r["pre_close"], rule.limit_ratio, rule.tick_size, upper=False
    )
    if not upper or not lower:
        return {**evidence, "status": "COVERAGE_INSUFFICIENT", "quantity": None}
    if side == OrderSide.BUY and rule.validate_for_buy():
        return {**evidence, "status": "RISK_REJECTED", "quantity": 0}
    # Use the stored depth units directly, as the existing paper matcher does.
    quote = Quote(
        code,
        r["quote_event_id"],
        r["quote_at"],
        r["received_at"],
        r["bid1"],
        r["bid1_volume"],
        r["ask1"],
        r["ask1_volume"],
        r["last_price"],
        upper,
        lower,
        bool(r["suspended"] or rule.is_suspended),
    )
    result = PaperMatcher().match(
        side=side,
        remaining_quantity=qty,
        approved_remaining_quantity=qty,
        limit_price=D(state["limit_price"]) if side == OrderSide.BUY else lower,
        quote=quote,
        now=r["received_at"],
        tick_size=rule.tick_size,
        liquidity_quantity=qty,
        slippage_rate=D(protocol["slippage"]),
    )
    n = result.fill_quantity
    gross = n * (result.fill_price or D(0))
    return {
        **evidence,
        "status": result.status if n else "UNFILLED",
        "quantity": n,
        "requested": qty,
        "price": str(result.fill_price) if n else None,
        "gross": str(gross),
        "fee": str(fee.calculate(side, gross, quantity=n)),
        "reason": result.waiting_reason,
    }


def _close_mark(c, code, day):
    close = datetime.combine(day, time(15))
    row = (
        c.execute(
            text("""SELECT /*+ MAX_EXECUTION_TIME(700) */ quote_event_id,bid1,quote_at,received_at
        FROM st_quote_event_v2 FORCE INDEX(idx_quote_event_v2_latest) WHERE stock_code=:code
        AND quote_at BETWEEN :earliest AND :close AND received_at BETWEEN quote_at AND :known
        AND source_provider='gj_big_qmt_inner' AND bid1>0 AND bid1_volume>0 AND ask1>=bid1
        ORDER BY quote_at DESC,quote_event_id LIMIT 1"""),
            {
                "code": code,
                "earliest": close - timedelta(seconds=15),
                "close": close,
                "known": close + timedelta(seconds=15),
            },
        )
        .mappings()
        .first()
    )
    return dict(row) if row else None


def advance(c, state, now, spec):
    changed = False
    for arm in state["arms"].values():
        for slot, side in [("entry", OrderSide.BUY), ("exit", OrderSide.SELL)]:
            if slot in arm:
                continue
            if slot == "exit" and not arm.get("entry", {}).get("quantity"):
                continue
            result = _match(c, state, arm, side, now, spec)
            if result is not None:
                arm[slot] = result
                changed = True
    if changed:
        done = all(
            "entry" in a and (not a["entry"]["quantity"] or "exit" in a)
            for a in state["arms"].values()
        )
        if done:
            state["status"] = "OBSERVATIONS_RECORDED"
            for a in state["arms"].values():
                if a["entry"].get("quantity"):
                    a["entry_close_mark"] = _close_mark(
                        c, state["stock_code"], date.fromisoformat(a["entry_at"][:10])
                    )
        _save(c, state["key"], "CHECKPOINT_" + str(now), state)


def tick(c, head, now):
    dates = list(
        c.execute(
            text("""SELECT DISTINCT trade_date FROM si_trade_calendar
        WHERE trade_status=1 AND trade_date>=:first ORDER BY trade_date LIMIT 122"""),
            {"first": head["first_session"]},
        ).scalars()
    )
    if not dates:
        return {"status": "WAIT_CALENDAR", "first_session": head["first_session"]}
    i = sum(d <= now.date() for d in dates) - 1
    if i < 0:
        return {"status": "REGISTERED", "first_session": head["first_session"]}
    spec = head["contract"]
    # Resume oldest pending signals first, including the first tick after downtime.
    cursor_key = VERSION + "/cursor"
    cursor = _read(c, cursor_key)
    start = cursor["next_index"] if cursor else 0
    for k in range(start, min(i + 1, 120), 3):
        if k + 2 >= len(dates):
            return {"status": "WAIT_CALENDAR", "day": str(dates[k])}
        day = dates[k]
        key = VERSION + "/" + str(day)
        state = _read(c, key)
        if state is None:
            can_sample = day == now.date() and (
                time(10, 5) <= now.time() <= time(11, 25)
                or time(13, 10) <= now.time() <= time(14, 45)
            )
            if can_sample:
                quote = _observed_quote(c, now)
                if not quote:
                    return {"status": "WAIT_QUOTE", "day": str(day)}
                features, sources = snapshot(c, quote, now)
                accepted = _now()
                if (accepted - now).total_seconds() > 12:
                    return {"status": "SAMPLE_EXPIRED", "day": str(day)}
                state = _plan(
                    c,
                    head,
                    day,
                    k + 1,
                    dates[k : k + 3],
                    now,
                    quote,
                    features,
                    sources,
                    accepted,
                )
                _save(c, key, "PAIR_FROZEN", state)
                if _now() >= accepted + timedelta(seconds=10):
                    raise TimeoutError(
                        "pair acceptance missed its prospective entry deadline"
                    )
                return {"status": state["status"], "pair_id": state["pair_id"]}
            if day == now.date() and now.time() <= time(14, 45):
                return {"status": "WAIT_WINDOW", "day": str(day)}
            _save(
                c,
                key,
                "MISSED_WINDOW",
                {
                    "key": key,
                    "session": k + 1,
                    "status": "NO_PROSPECTIVE_OBSERVATION",
                    "arms": {},
                },
            )
        elif state["status"] == "PENDING":
            advance(c, state, now, spec)
            if state["status"] == "PENDING":
                return {"status": "PENDING", "pair_id": state["pair_id"]}
        _save(c, cursor_key, "ADVANCE_" + str(k), {"next_index": k + 3})
        return {"status": "CHECKPOINT", "day": str(day)}
    return {"status": "IDLE", "next_index": start}


def _now():
    from server.engine.autonomous_strategy_research import now_shanghai

    return now_shanghai().replace(tzinfo=None)


def run_study(engine, *, registration_only=False):
    with engine.connect() as c:
        if not c.execute(text("SELECT GET_LOCK(:key,0)"), {"key": VERSION}).scalar():
            return {"status": "BUSY"}
        try:
            head = register(c, _now())
            result = (
                {"status": "REGISTERED", "first_session": head["first_session"]}
                if registration_only
                else tick(c, head, _now())
            )
            c.commit()
            return {**result, "study": VERSION, "contract_hash": head["hash"]}
        finally:
            c.rollback()
            c.execute(text("SELECT RELEASE_LOCK(:key)"), {"key": VERSION})

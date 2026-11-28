"""Point-in-time inputs for the paired entry observation protocol."""

from datetime import datetime, timedelta
from statistics import mean, pstdev

from sqlalchemy import bindparam, text

from server.common.analysis_pool_receipt import canonical_sha256
from server.trading_v3.order_flow import calculate_order_flow


def _minutes(c, code, dates, end, cutoff):
    # Constant code/time ranges use idx_sm_code_time. Only final minute labels
    # are eligible; older backfills are OK if received before this decision.
    params = {"code": code, "cutoff": cutoff}
    ranges = []
    for i, day in enumerate(dates):
        last = datetime.combine(day, end)
        params[f"a{i}"] = last - timedelta(minutes=5)
        params[f"b{i}"] = last
        ranges.append(f"trade_time BETWEEN :a{i} AND :b{i}")
    rows = (
        c.execute(
            text(
                """
        SELECT /*+ MAX_EXECUTION_TIME(1500) */ trade_time,price,amount,volume,
               data_source,source_time,received_at,batch_id,data_version
        FROM sm_stock_minute FORCE INDEX(idx_sm_code_time)
        WHERE stock_code=:code AND received_at BETWEEN DATE_ADD(trade_time, INTERVAL 1 MINUTE) AND :cutoff
          AND source_time<=:cutoff
          AND data_source IN ('qmt_datadir','gj_big_qmt_inner')
          AND quality_status='VERIFIED' AND permission_status='SUPPORTED'
          AND batch_id IS NOT NULL AND data_version IS NOT NULL AND (
        """
                + " OR ".join(ranges)
                + ") ORDER BY trade_time LIMIT 300"
            ),
            params,
        )
        .mappings()
        .all()
    )
    grouped = {}
    for row in rows:
        grouped.setdefault(row["trade_time"].date(), []).append(dict(row))
    return grouped


def _sector(c, code, start, end, cutoff, stock_endpoints_known):
    run = (
        c.execute(
            text("""SELECT /*+ MAX_EXECUTION_TIME(700) */ snapshot_date,industry_hash,captured_at
        FROM qmt_membership_snapshot_run WHERE source='gj_big_qmt_inner'
        AND snapshot_date<=:day AND captured_at<=:cutoff AND quality_status='QMT_VALIDATED'
        ORDER BY snapshot_date DESC LIMIT 1"""),
            {"day": cutoff.date(), "cutoff": cutoff},
        )
        .mappings()
        .first()
    )
    if not run:
        return {}, {}
    group = (
        c.execute(
            text("""SELECT DISTINCT industry_code FROM qmt_industry_member_snapshot
        WHERE snapshot_date=:day AND source='gj_big_qmt_inner' AND stock_code=:code
        AND industry_code LIKE 'SW2%' ORDER BY industry_code LIMIT 2"""),
            {"day": run["snapshot_date"], "code": code},
        )
        .scalars()
        .all()
    )
    proof = dict(run)
    if len(group) != 1:
        return {}, proof
    members = (
        c.execute(
            text("""SELECT DISTINCT stock_code FROM qmt_industry_member_snapshot
        WHERE source='gj_big_qmt_inner' AND snapshot_date=:day AND industry_code=:industry
        ORDER BY stock_code LIMIT 301"""),
            {"day": run["snapshot_date"], "industry": group[0]},
        )
        .scalars()
        .all()
    )
    proof.update(industry=group[0], member_count=len(members))
    if not members or len(members) > 300:
        return {}, proof
    if code in members and not stock_endpoints_known:
        return {}, {**proof, "missing_member": code, "reason": "MEMBER_WINDOW_MISSING"}
    query = text("""SELECT /*+ MAX_EXECUTION_TIME(1500) */ stock_code,trade_time,price,
        source_time,received_at,batch_id,data_version FROM sm_stock_minute FORCE INDEX(idx_sm_code_time)
        WHERE stock_code IN :codes AND trade_time IN (:a,:b) AND source_time<=:cutoff
        AND received_at BETWEEN DATE_ADD(trade_time, INTERVAL 1 MINUTE) AND :cutoff
        AND data_source IN ('qmt_datadir','gj_big_qmt_inner')
        AND quality_status='VERIFIED' AND permission_status='SUPPORTED'
        AND batch_id IS NOT NULL AND data_version IS NOT NULL AND price>0 ORDER BY stock_code,trade_time
        """).bindparams(bindparam("codes", expanding=True))
    rows = []
    for offset in range(0, len(members), 16):
        batch = members[offset : offset + 16]
        result = (
            c.execute(query, {"codes": batch, "a": start, "b": end, "cutoff": cutoff})
            .mappings()
            .all()
        )
        rows.extend(result)
        if len({(r["stock_code"], r["trade_time"]) for r in result}) != 2 * len(
            batch
        ) or len(result) != 2 * len(batch):
            proof.update(
                reason="MEMBER_ENDPOINT_COVERAGE_INCOMPLETE",
                checked_members=offset + len(batch),
            )
            break
    prices = {}
    for r in rows:
        prices.setdefault(r["stock_code"], []).append(r["price"])
    returns = [float((p[1] / p[0] - 1) * 100) for p in prices.values() if len(p) == 2]
    proof.update(
        covered=len(returns),
        source_hash=canonical_sha256([list(map(str, r.values())) for r in rows]),
    )
    if len(returns) != len(members) or len(rows) != 2 * len(members):
        return {}, proof
    proof["member_prices"] = [dict(r) for r in rows]
    idx = (
        c.execute(
            text("""SELECT /*+ MAX_EXECUTION_TIME(800) */ trade_time,price,snapshot_at,etl_sync_at
        FROM sm_index_minute WHERE index_code='000001' AND trade_time IN (:a,:b)
        AND snapshot_at BETWEEN DATE_ADD(trade_time, INTERVAL 1 MINUTE) AND :t
        AND etl_sync_at<=:t AND price>0 ORDER BY trade_time LIMIT 3"""),
            {"a": start, "b": end, "t": cutoff},
        )
        .mappings()
        .all()
    )
    result = {"sector_breadth_pct": 100 * sum(x > 0 for x in returns) / len(returns)}
    proof["benchmark"] = [dict(r) for r in idx]
    if len(idx) == 2 and idx[0]["trade_time"] == start and idx[1]["trade_time"] == end:
        result["sector_relative_return_pct"] = mean(returns) - float(
            (idx[1]["price"] / idx[0]["price"] - 1) * 100
        )
    return result, proof


def snapshot(c, quote, cutoff):
    end = cutoff.replace(second=0, microsecond=0) - timedelta(minutes=1)
    start = end - timedelta(minutes=5)
    days = (
        c.execute(
            text("""SELECT DISTINCT trade_date FROM si_trade_calendar WHERE trade_status=1
        AND trade_date<=:day ORDER BY trade_date DESC LIMIT 21"""),
            {"day": cutoff.date()},
        )
        .scalars()
        .all()
    )
    bars = _minutes(c, quote["stock_code"], days[:1], end.time(), cutoff)
    if len(bars.get(cutoff.date(), [])) == 6:
        # A measured 21-range query timed out on the formal minute ledger.
        for offset in range(1, len(days), 5):
            bars.update(
                _minutes(
                    c,
                    quote["stock_code"],
                    days[offset : offset + 5],
                    end.time(),
                    cutoff,
                )
            )
    features = {
        "spread_bps": calculate_order_flow([quote])["spread_bps"],
        "fill_probability": None,
    }
    amounts = {}
    for day, part in bars.items():
        times = [r["trade_time"].time() for r in part]
        if times != [(start + timedelta(minutes=k)).time() for k in range(6)]:
            continue
        if any(
            r["amount"] is None or r["amount"] < 0 or not r["price"] or r["price"] <= 0
            for r in part
        ):
            continue
        amounts[day] = float(sum(r["amount"] for r in part[1:]))
    history = [a for d, a in amounts.items() if d < cutoff.date()]
    if cutoff.date() in amounts:
        part = bars[cutoff.date()]
        features["interval_return_pct"] = float(
            (part[-1]["price"] / part[0]["price"] - 1) * 100
        )
        if len(history) == 20 and pstdev(history) > 0:
            features["intraday_amount_surprise_z"] = (
                amounts[cutoff.date()] - mean(history)
            ) / pstdev(history)
        # Both QMT minute adapters retain volume in lots of 100 shares.
        if all(r["volume"] is not None and r["volume"] >= 0 for r in part[1:]):
            shares = sum(float(r["volume"]) * 100 for r in part[1:])
            turnover = amounts[cutoff.date()]
            if shares > 0 and turnover > 0:
                features["price_vs_vwap_pct"] = (
                    float(quote["last_price"]) * shares / turnover - 1
                ) * 100
    endpoints = {
        r["trade_time"]
        for r in bars.get(cutoff.date(), [])
        if r["price"] and r["price"] > 0
    }
    sector, proof = _sector(
        c, quote["stock_code"], start, end, cutoff, {start, end} <= endpoints
    )
    features.update(sector)
    sources = [dict(r) for day in sorted(bars) for r in bars[day]]
    return features, {
        "sector": proof,
        "minute_rows": sources,
        "history_days": len(history),
        "window": [str(start), str(end)],
        "quote": dict(quote),
    }

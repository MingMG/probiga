"""Pure calendar/unit planning; no strategy, release or application-version inputs."""
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .models import WorkUnit

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _day(value):
    return str(value)[:10]


def sessions(calendar, start, end):
    cursor, stop = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    result = []
    while cursor <= stop:
        key = cursor.isoformat()
        if key not in calendar or calendar[key] not in (0, 1):
            raise ValueError(f"CALENDAR_MISSING:{key}")
        if calendar[key] == 1:
            result.append(key)
        cursor += timedelta(days=1)
    return result


def latest_closed(calendar, now, ready_time=time(15, 30)):
    now = now.astimezone(SHANGHAI) if now.tzinfo else now.replace(tzinfo=SHANGHAI)
    current = now.date()
    # Require each intervening day, not just the latest open row. Missing
    # calendar metadata cannot turn into a plausible but stale target.
    for offset in range(31):
        day = current - timedelta(days=offset)
        key = day.isoformat()
        if calendar.get(key) not in (0, 1):
            raise ValueError(f"CALENDAR_MISSING:{key}")
        if calendar[key] == 1 and (offset or now.time() >= ready_time):
            return key
    raise ValueError("NO_CLOSED_SESSION")


def eligible_codes(spec, catalog, target_date):
    result = []
    for code, item in sorted(catalog.items()):
        listed = item.get("list_date") or item.get("listed_date")
        ended = item.get("last_trade_date") or item.get("delist_date") or item.get("expire_date")
        if listed and _day(listed) > target_date:
            continue
        if ended and _day(ended) < target_date:
            continue
        if spec.name == "capital_flow_daily" and not (
            code.endswith((".SH", ".SZ")) and code[:2] in {"00", "30", "60", "68"}
        ):
            continue
        result.append(code)
    return result


def plan_units(spec, target_date, catalog, states, *, now=None, refresh=False, refresh_after=None):
    now = now or datetime.now(SHANGHAI)
    now = now.astimezone(SHANGHAI).replace(tzinfo=None) if now.tzinfo else now
    indexed = {(str(s["target_date"])[:10], s["partition_key"]): s for s in states
               if not s.get("source") or s["source"] == spec.source}
    result = []
    for code in eligible_codes(spec, catalog, target_date):
        for adjustment in spec.adjustments:
            unit = WorkUnit(spec.name, spec.source, target_date, code, spec.period, adjustment)
            state = indexed.get((target_date, unit.partition_key))
            if state:
                if state["status"] == "running":
                    continue  # recovery owns it; never abandon it by advancing a date
                if spec.name == "capital_flow_daily" and state["status"] == "staged":
                    continue  # durable row awaits whole-date atomic publication
                due = state.get("next_retry_at")
                if due and datetime.fromisoformat(str(due)) > now:
                    continue
                if state["status"] in {"complete", "no_data"}:
                    if not refresh:
                        continue
                    succeeded = state.get("last_success_at")
                    if refresh_after and succeeded and datetime.fromisoformat(str(succeeded)) >= refresh_after:
                        continue  # one overlap refresh per publication slot, not every five minutes
            result.append(unit)
    return result


def daily_candidate_days(spec, days, catalog, state_counts):
    """Keep the latest session plus dates whose current-source progress is incomplete."""
    if not days:
        return []
    by_day = {}
    for item in state_counts:
        if item.get("source") and item["source"] != spec.source:
            continue
        statuses = by_day.setdefault(_day(item["target_date"]), {})
        status = str(item["status"])
        statuses[status] = statuses.get(status, 0) + int(item["unit_count"])
    result = []
    latest = days[-1]
    for target_date in days:
        if target_date == latest:
            result.append(target_date)
            continue
        current = by_day.get(target_date, {})
        if spec.name == "capital_flow_daily":
            # A flow date has only traded securities. Atomic publication turns
            # every expected staged unit to complete in one transaction, so a
            # complete-only date needs no repeated stock-day dependency scan.
            if current.get("complete", 0) > 0 and sum(current.values()) == current["complete"]:
                continue
            result.append(target_date)
            continue
        expected = (
            len(eligible_codes(spec, catalog, target_date))
            * len(spec.adjustments)
        )
        terminal = current.get("complete", 0) + current.get("no_data", 0)
        # Extra terminal rows can remain after a reference correction. They do
        # not make an otherwise completed date run forever; latest and explicit
        # backfill still perform exact-key planning.
        if terminal < expected or sum(current.values()) != terminal:
            result.append(target_date)
    return result


def flow_dependency(catalog, target_date, stock_daily_states, *, source=None):
    """Do not shrink the denominator when the prerequisite lost a security."""
    states = {s["partition_key"].split(":")[0]: s for s in stock_daily_states
              if _day(s["target_date"]) == target_date
              and s["partition_key"].endswith(":1d:none")
              and (not source or not s.get("source") or s["source"] == source)}
    import json
    traded, no_trade, missing = [], [], []
    for code in sorted(catalog):
        item = catalog[code]
        listed = item.get("list_date") or item.get("listed_date")
        ended = item.get("last_trade_date") or item.get("delist_date") or item.get("expire_date")
        if (listed and _day(listed) > target_date) or (ended and _day(ended) < target_date):
            continue
        if not (code.endswith((".SH", ".SZ")) and code[:2] in {"00", "30", "60", "68"}):
            continue
        state = states.get(code)
        if state and state["status"] == "no_data":
            detail = state.get("detail_json") or "{}"
            detail = json.loads(detail) if isinstance(detail, str) else detail
            if detail.get("reason") in {"no_trades", "suspended", "not_listed", "delisted"}:
                no_trade.append(code)
                continue
        if state and state["status"] == "complete":
            # The writer records the actual daily activity, not just row count.
            detail = state.get("detail_json") or "{}"
            detail = json.loads(detail) if isinstance(detail, str) else detail
            if detail.get("traded") is True:
                traded.append(code)
                continue
            if detail.get("traded") is False:
                no_trade.append(code)
                continue
        missing.append(code)
    return {"traded": traded, "no_trade": no_trade, "missing_daily": missing}


def summarize(spec, target_date, catalog, states):
    expected = {WorkUnit(spec.name, spec.source, target_date, code, spec.period, adjustment).partition_key
                for code in eligible_codes(spec, catalog, target_date) for adjustment in spec.adjustments}
    indexed = {s["partition_key"]: s for s in states
               if _day(s["target_date"]) == target_date
               and (not s.get("source") or s["source"] == spec.source)}
    complete = {key for key in expected if indexed.get(key, {}).get("status") == "complete"}
    no_data = {key for key in expected if indexed.get(key, {}).get("status") == "no_data"}
    missing = expected - complete - no_data
    related = [indexed[key] for key in expected if key in indexed]
    success = [str(s["last_success_at"]) for s in related if s.get("last_success_at")]
    retries = [str(s["next_retry_at"]) for s in related if s.get("next_retry_at")]
    return {"dataset": spec.name, "source": spec.source, "target_date": target_date,
            "status": "complete" if expected and not missing else "partial",
            "expected": len(expected), "complete": len(complete), "no_data": len(no_data),
            "missing": len(missing), "missing_sample": sorted(missing)[:20],
            "last_success_at": max(success) if success else None,
            "next_retry_at": min(retries) if retries else None,
            "errors": sorted({s["last_error_code"] for s in related if s.get("last_error_code")})}


def refresh_cutoff(spec, now):
    """Most recent source publication slot, in the database's Shanghai clock."""
    local = now.astimezone(SHANGHAI).replace(tzinfo=None) if now.tzinfo else now
    slots = [spec.ready_time]
    if spec.event_data:
        slots.append(time(21, 30))
    candidates = [datetime.combine(local.date() - timedelta(days=offset), slot)
                  for offset in (0, 1) for slot in slots]
    return max(value for value in candidates if value <= local)

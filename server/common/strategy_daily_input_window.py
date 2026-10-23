"""Calendar-bound daily history consumed by the production strategy jobs."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from datetime import date, datetime
import re
from typing import Any, Iterable

from sqlalchemy import bindparam, text

from server.common.pit_facts import normalize_decision_at
from server.common.qmt_attestation_contract import canonical_digest


ANALYSIS_DAILY_INPUT_SESSIONS = 60
V3_DAILY_INPUT_SESSIONS = 70
STRATEGY_DAILY_INPUT_SESSION_COUNTS = {
    "analysis_fast": ANALYSIS_DAILY_INPUT_SESSIONS,
    "trading_v3_close_decision": V3_DAILY_INPUT_SESSIONS,
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@contextmanager
def daily_input_snapshot(engine: Any):
    """Keep source validation and all consumer reads on one immutable DB view."""

    with engine.connect() as connection:
        if connection.dialect.name == "mysql":
            connection = connection.execution_options(isolation_level="REPEATABLE READ")
        with connection.begin():
            yield connection


def resolve_daily_input_sessions(
    engine: Any,
    *,
    target_trade_date: str,
    session_count: int,
    decision_known_at: datetime | str,
) -> list[str]:
    """Resolve an exact calendar tail without filling missing market dates."""

    from server.common.qmt_trade_calendar import load_trade_calendar_window_receipt

    target = date.fromisoformat(str(target_trade_date))
    if target.isoformat() != target_trade_date or session_count < 1:
        raise ValueError("daily strategy input window is invalid")
    cutoff = normalize_decision_at(decision_known_at)
    if target > cutoff.date():
        raise RuntimeError("daily strategy input target is after its knowledge cutoff")
    with (engine.connect() if hasattr(engine, "connect") else nullcontext(engine)) as connection:
        calendar = load_trade_calendar_window_receipt(
            connection,
            end_date=target_trade_date,
            required_sessions=session_count,
            decision_known_at=cutoff,
        )
        sessions = [
            day for day in calendar.sessions_between(calendar.start_date, target_trade_date)
            if day <= target_trade_date
        ][-session_count:]
    if (
        len(sessions) != session_count
        or sessions != sorted(set(sessions))
        or sessions[-1] != target_trade_date
    ):
        raise RuntimeError(
            f"immutable QMT calendar does not cover the {session_count}-session strategy input window"
        )
    return sessions


def validate_daily_input_sessions(
    connection: Any,
    *,
    sessions: Iterable[str],
    decision_known_at: datetime | str,
) -> dict[str, Any]:
    """Validate each consumed market partition once, independently of stocks.

    QMT remains preferred when a completed native attestation exists. If the
    vendor-restricted embedded runtime cannot publish one, the independently
    sourced, provider-labelled daily receipt is validated against the same
    catalog and exact database rows. A newly listed stock therefore does not
    create a fictitious pre-listing gap.
    """

    from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
    from server.common.verified_daily_market_truth import (
        FREE_DAILY_DATASET,
        FREE_DAILY_PARTITION_KEY,
        FREE_DAILY_PROVIDERS,
        load_retained_qmt_daily_market_truth,
        load_verified_daily_market_truth,
    )

    days = list(sessions)
    if not days or days != sorted(set(days)):
        raise ValueError("daily strategy input sessions are invalid")
    cutoff = normalize_decision_at(decision_known_at)
    truth_hashes: list[str] = []
    catalogs: dict[str, str] = {}
    providers: dict[str, str] = {}
    partition_roots: dict[str, str] = {}
    attested_rows = 0
    latest_truth: dict[str, Any] | None = None
    free_days: set[str] = set()
    try:
        free_rows = connection.execute(
            text(
                """
                SELECT target_date
                  FROM acquisition_partition_state
                 WHERE dataset=:dataset
                   AND partition_key=:partition_key
                   AND status='complete'
                   AND target_date IN :dates
                   AND source IN :sources
                """
            ).bindparams(
                bindparam("dates", expanding=True),
                bindparam("sources", expanding=True),
            ),
            {
                "dataset": FREE_DAILY_DATASET,
                "partition_key": FREE_DAILY_PARTITION_KEY,
                "dates": days,
                "sources": sorted(FREE_DAILY_PROVIDERS),
            },
        )
        free_days = {
            str(row[0])[:10]
            for row in free_rows
            if str(row[0])[:10] in days
        }
    except Exception:
        free_days = set()

    def add_qmt_truth(qmt_truth: Any, group: list[str]) -> None:
        nonlocal attested_rows, latest_truth
        source_truth = qmt_truth.as_dict()
        for grouped_day in group:
            catalogs[grouped_day] = qmt_truth.catalog_batch_id
            providers[grouped_day] = "qmt"
            stable_truth = dict(source_truth)
            stable_truth.pop("decision_known_at", None)
            stable_truth.pop("truth_hash", None)
            partition_roots[grouped_day] = canonical_digest(
                {"day": grouped_day, "truth": stable_truth}
            )
            if grouped_day == days[-1]:
                latest_truth = source_truth
        truth_hashes.append(str(qmt_truth.truth_hash))
        attested_rows += int(qmt_truth.attested_row_count)

    def add_free_truth(day: str) -> None:
        nonlocal attested_rows, latest_truth
        free_truth = load_verified_daily_market_truth(
            connection,
            start_date=day,
            end_date=day,
            decision_known_at=cutoff,
        )
        source_truth = free_truth.as_dict()
        row_count = int(free_truth.row_count)
        if (
            source_truth.get("requested_sessions") != [day]
            or row_count <= 0
            or _SHA256.fullmatch(str(source_truth.get("truth_hash") or ""))
            is None
            or not isinstance(free_truth.catalog_batch_id, str)
            or not free_truth.catalog_batch_id
        ):
            raise RuntimeError("daily truth identity differs")
        truth_hashes.append(str(source_truth["truth_hash"]))
        catalogs[day] = free_truth.catalog_batch_id
        providers[day] = free_truth.provider
        stable_truth = dict(source_truth)
        stable_truth.pop("decision_known_at", None)
        stable_truth.pop("truth_hash", None)
        partition_roots[day] = canonical_digest(stable_truth)
        attested_rows += row_count
        if day == days[-1]:
            latest_truth = source_truth

    def add_retained_truth(day: str) -> None:
        nonlocal attested_rows, latest_truth
        retained_truth = load_retained_qmt_daily_market_truth(
            connection,
            start_date=day,
            end_date=day,
            decision_known_at=cutoff,
        )
        source_truth = retained_truth.as_dict()
        truth_hashes.append(str(source_truth["truth_hash"]))
        catalogs[day] = retained_truth.catalog_batch_id
        providers[day] = retained_truth.provider
        stable_truth = dict(source_truth)
        stable_truth.pop("decision_known_at", None)
        stable_truth.pop("truth_hash", None)
        partition_roots[day] = canonical_digest(stable_truth)
        attested_rows += int(retained_truth.row_count)
        if day == days[-1]:
            latest_truth = source_truth

    qmt_days = [day for day in days if day not in free_days]
    if free_days and qmt_days:
        for day in qmt_days:
            try:
                add_retained_truth(day)
            except Exception:
                add_free_truth(day)
        for day in sorted(free_days):
            add_free_truth(day)
    else:
        for day in days:
            if day in free_days:
                add_free_truth(day)
                continue
            try:
                truth = load_qmt_daily_market_truth(
                    connection,
                    start_date=day,
                    end_date=day,
                    decision_known_at=cutoff,
                    verify_rows=False,
                )
            except Exception as qmt_error:
                try:
                    add_retained_truth(day)
                    continue
                except Exception:
                    try:
                        add_free_truth(day)
                        continue
                    except Exception as free_error:
                        raise RuntimeError(
                            f"daily strategy input truth differs for {day}: "
                            f"qmt={qmt_error}; free={free_error}"
                        ) from free_error
            add_qmt_truth(truth, [day])
    if latest_truth is None:
        raise RuntimeError("daily strategy input truth is unavailable")
    return {
        "schema": "probiga.strategy-daily-input-window.v1",
        "sessions": days,
        "session_count": len(days),
        "session_set_sha256": canonical_digest(days),
        "daily_truth_sha256": canonical_digest(truth_hashes),
        "daily_attested_row_count": attested_rows,
        "catalog_batches_by_session": catalogs,
        "daily_providers_by_session": providers,
        "daily_partition_roots": partition_roots,
        "latest_daily_truth": latest_truth,
        "decision_known_at": cutoff.isoformat(sep=" ", timespec="seconds"),
    }


def daily_input_catalog_join(window: dict[str, Any], *, alias: str = "k") -> tuple[str, dict[str, str]]:
    """Restrict reads to the exact listing universe verified for each day."""

    if re.fullmatch(r"[a-z_]+", alias) is None:
        raise ValueError("daily input query alias is invalid")
    bindings = window["catalog_batches_by_session"]
    if set(bindings) != set(window["sessions"]):
        raise ValueError("daily input catalog bindings differ")
    selects: list[str] = []
    params: dict[str, str] = {}
    for index, day in enumerate(window["sessions"]):
        selects.append(
            f"SELECT :daily_input_day_{index} AS trade_date, "
            f":daily_input_catalog_{index} AS catalog_batch_id"
        )
        params[f"daily_input_day_{index}"] = day
        params[f"daily_input_catalog_{index}"] = bindings[day]
    return (
        " JOIN (" + " UNION ALL ".join(selects) + ") AS daily_input_scope "
        f"ON daily_input_scope.trade_date={alias}.trade_date "
        "JOIN qmt_stock_catalog_member AS daily_input_member "
        "ON daily_input_member.batch_id=daily_input_scope.catalog_batch_id "
        f"AND daily_input_member.stock_code=SUBSTR({alias}.stock_code,1,6) "
        "AND daily_input_member.instrument_type='STOCK' "
        f"AND daily_input_member.list_date<={alias}.trade_date "
        f"AND (daily_input_member.expire_date IS NULL OR daily_input_member.expire_date>={alias}.trade_date) ",
        params,
    )


def load_daily_input_window(
    engine: Any,
    *,
    target_trade_date: str,
    session_count: int,
    decision_known_at: datetime | str,
    daily_engine: Any = None,
) -> dict[str, Any]:
    sessions = resolve_daily_input_sessions(
        engine,
        target_trade_date=target_trade_date,
        session_count=session_count,
        decision_known_at=decision_known_at,
    )
    source = daily_engine if daily_engine is not None else engine
    with (source.connect() if hasattr(source, "connect") else nullcontext(source)) as connection:
        return validate_daily_input_sessions(
            connection, sessions=sessions, decision_known_at=decision_known_at,
        )

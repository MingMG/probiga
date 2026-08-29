"""Frozen, fail-closed outcomes for catalog members with no daily bar.

These are not general missing-data tolerances.  Every use is operator-explicit,
limited to a reviewed stock-code set and an exact historical window, and bound
to immutable catalog/calendar roots.  V2 additionally records exact historical
catalog/date pairs for which no attestable native QMT bar remains available.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Mapping, Sequence

from server.common.qmt_attestation_contract import canonical_digest


NO_ROW_EXCEPTION_CONTRACT_SCHEMA = (
    "probiga.qmt-daily-no-row-exceptions.v1"
)
HISTORICAL_UNAVAILABLE_CONTRACT_SCHEMA = (
    "probiga.qmt-daily-no-row-exceptions.v2"
)
CURRENT_REVIEWED_UNAVAILABLE_CONTRACT_SCHEMA = (
    "probiga.qmt-daily-no-row-exceptions.v3"
)
EXACT_LIFECYCLE_NO_ROW_CANDIDATES = frozenset({"002231", "603056"})
NOT_YET_LISTED_NO_ROW_CANDIDATES = frozenset({
    "301688", "301689", "301697", "301699",
})
NO_ROW_REVIEWED_START_DATE = "2026-03-06"
LEGACY_NO_ROW_REVIEWED_END_DATE = "2026-08-27"
NO_ROW_REVIEWED_END_DATE = "2026-08-28"
NOT_YET_LISTED_PROOF_CUTOFF = NO_ROW_REVIEWED_END_DATE

_A_SHARE_CODE_RE = re.compile(r"^(?:0|3|4|6|8|9)\d{5}$")
_QMT_CODE_RE = re.compile(r"^(?:0|3|4|6|8|9)\d{5}\.(?:SH|SZ|BJ)$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTRACT_KEYS = frozenset({
    "schema", "policy", "start_date", "end_date",
    "catalog_batch_id", "catalog_member_set_hash", "catalog_manifest_hash",
    "calendar_batch_id", "calendar_session_set_hash",
    "calendar_manifest_hash", "calendar_known_at", "sessions",
    "exact_lifecycle_no_row_codes", "not_yet_listed_no_row_codes",
    "entities", "proof_sha256",
})
_ENTITY_KEYS = frozenset({
    "category", "stock_code", "qmt_code", "list_date", "expire_date",
    "affected_trade_dates", "affected_trade_dates_sha256",
    "target_rows", "history_rows",
})


def _date(value: Any, *, label: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{label} must be an exact ISO date")
    try:
        normalized = datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"{label} must be an exact ISO date") from exc
    if normalized != value:
        raise ValueError(f"{label} must be an exact ISO date")
    return value


def explicit_no_row_codes(
    raw_codes: str,
    *,
    category: str,
) -> list[str]:
    """Parse an exact comma list without zfill, suffix or whitespace repair."""

    raw = str(raw_codes or "")
    if not raw:
        return []
    candidates = (
        EXACT_LIFECYCLE_NO_ROW_CANDIDATES
        if category == "EXACT_LIFECYCLE_NO_ROW"
        else NOT_YET_LISTED_NO_ROW_CANDIDATES
        if category == "NOT_YET_LISTED_NO_ROW"
        else None
    )
    if candidates is None:
        raise ValueError("QMT no-row exception category is invalid")
    values = raw.split(",")
    if (
        any(not value or value != value.strip() for value in values)
        or any(_A_SHARE_CODE_RE.fullmatch(value) is None for value in values)
        or len(set(values)) != len(values)
        or not set(values).issubset(candidates)
    ):
        raise ValueError(
            f"{category.lower().replace('_', ' ')} codes must be unique exact "
            f"six-digit reviewed codes; allowed={sorted(candidates)}"
        )
    return sorted(values)


def _canonical_codes(
    values: Sequence[str],
    *,
    candidates: frozenset[str],
) -> list[str]:
    codes = list(values)
    if (
        codes != sorted(set(codes))
        or any(type(code) is not str or code not in candidates for code in codes)
    ):
        raise RuntimeError("QMT no-row codes are not an exact reviewed subset")
    return codes


def build_no_row_exception_contract(
    *,
    catalog: Any,
    calendar: Any,
    start_date: str,
    end_date: str,
    exact_lifecycle_codes: Sequence[str] = (),
    not_yet_listed_codes: Sequence[str] = (),
    target_rows_by_code: Mapping[str, int],
    history_rows_by_code: Mapping[str, int],
    historical_unavailable_dates_by_code: Mapping[
        str, Sequence[str]
    ] | None = None,
    target_rows_by_pair: Mapping[tuple[str, str], int] | None = None,
    history_rows_by_pair: Mapping[tuple[str, str], int] | None = None,
) -> dict[str, Any]:
    """Build one root- and window-bound proof for reviewed no-row outcomes."""

    start = _date(start_date, label="QMT no-row start_date")
    end = _date(end_date, label="QMT no-row end_date")
    if start >= end:
        raise RuntimeError("QMT no-row proof requires a multi-date window")
    lifecycle = _canonical_codes(
        exact_lifecycle_codes,
        candidates=EXACT_LIFECYCLE_NO_ROW_CANDIDATES,
    )
    not_listed = _canonical_codes(
        not_yet_listed_codes,
        candidates=NOT_YET_LISTED_NO_ROW_CANDIDATES,
    )
    all_codes = sorted(lifecycle + not_listed)
    historical_by_code: dict[str, list[str]] = {}
    for raw_code, raw_dates in dict(
        historical_unavailable_dates_by_code or {}
    ).items():
        code = str(raw_code)
        dates = list(raw_dates)
        if (
            _A_SHARE_CODE_RE.fullmatch(code) is None
            or dates != sorted(set(dates))
            or any(_date(day, label="historical unavailable date") != day
                   for day in dates)
            or not dates
        ):
            raise RuntimeError(
                "historical unavailable pairs must be canonical exact pairs"
            )
        historical_by_code[code] = dates
    historical_pairs = sorted(
        (code, day)
        for code, dates in historical_by_code.items()
        for day in dates
    )
    if (
        (not all_codes and not historical_pairs)
        or len(all_codes) != len(set(all_codes))
    ):
        raise RuntimeError("QMT no-row proof requires distinct explicit codes")
    reviewed_window = (start, end)
    legacy_window = (
        NO_ROW_REVIEWED_START_DATE,
        LEGACY_NO_ROW_REVIEWED_END_DATE,
    )
    current_window = (
        NO_ROW_REVIEWED_START_DATE,
        NO_ROW_REVIEWED_END_DATE,
    )
    if reviewed_window not in {legacy_window, current_window}:
        raise RuntimeError(
            "QMT no-row proof must equal one exact reviewed window: "
            f"{legacy_window[0]}..{legacy_window[1]} or "
            f"{current_window[0]}..{current_window[1]}"
        )
    if not_listed and end > NOT_YET_LISTED_PROOF_CUTOFF:
        raise RuntimeError(
            "NOT_YET_LISTED proof cannot extend beyond its reviewed cutoff"
        )
    if (
        set(target_rows_by_code) != set(all_codes)
        or set(history_rows_by_code) != set(all_codes)
    ):
        raise RuntimeError("QMT no-row zero-row inventory differs")
    if any(
        type(rows) is not int or rows != 0
        for rows in list(target_rows_by_code.values())
        + list(history_rows_by_code.values())
    ):
        raise RuntimeError("QMT no-row code already has daily rows")
    target_pair_rows = dict(target_rows_by_pair or {})
    history_pair_rows = dict(history_rows_by_pair or {})
    if (
        set(target_pair_rows) != set(historical_pairs)
        or set(history_pair_rows) != set(historical_pairs)
        or any(
            type(rows) is not int or rows != 0
            for rows in list(target_pair_rows.values())
            + list(history_pair_rows.values())
        )
    ):
        raise RuntimeError(
            "historical unavailable pair inventory already has daily rows"
        )

    sessions = list(calendar.sessions_between(start, end))
    if not sessions or sessions != sorted(set(sessions)):
        raise RuntimeError("QMT no-row calendar session proof differs")
    members = {
        str(member.get("stock_code") or ""): dict(member)
        for member in catalog.members
    }
    eligible_by_day = {
        day: set(catalog.eligible_codes(day)) for day in sessions
    }
    entities: list[dict[str, Any]] = []
    for category, codes in (
        ("EXACT_LIFECYCLE_NO_ROW", lifecycle),
        ("NOT_YET_LISTED_NO_ROW", not_listed),
    ):
        for code in codes:
            member = members.get(code)
            if member is None:
                raise RuntimeError(
                    f"QMT no-row code is absent from catalog: {code}"
                )
            qmt_code = str(member.get("qmt_code") or "").upper()
            list_date = str(member.get("list_date") or "")[:10]
            raw_expire = member.get("expire_date")
            expire_date = (
                str(raw_expire)[:10]
                if raw_expire not in (None, "", "NaT")
                else None
            )
            if (
                _QMT_CODE_RE.fullmatch(qmt_code) is None
                or qmt_code[:6] != code
            ):
                raise RuntimeError(f"QMT no-row qmt_code differs: {code}")
            _date(list_date, label="QMT no-row list_date")
            affected = [day for day in sessions if code in eligible_by_day[day]]
            if category == "EXACT_LIFECYCLE_NO_ROW":
                if list_date == "1970-01-01" or expire_date is None:
                    raise RuntimeError(
                        "exact lifecycle no-row code lacks a finite in-window "
                        f"catalog lifecycle: {code}"
                    )
                _date(expire_date, label="QMT no-row expire_date")
                lifecycle_days = [
                    day for day in sessions if list_date <= day <= expire_date
                ]
                if (
                    not start < expire_date <= end
                    or not affected
                    or affected != lifecycle_days
                ):
                    raise RuntimeError(
                        "exact lifecycle no-row code lacks a finite in-window "
                        f"catalog lifecycle/session proof: {code}"
                    )
            else:
                if list_date != "1970-01-01" or expire_date is not None:
                    raise RuntimeError(
                        f"NOT_YET_LISTED catalog sentinel differs: {code}"
                    )
                if affected != sessions:
                    raise RuntimeError(
                        f"NOT_YET_LISTED catalog/session proof differs: {code}"
                    )
            entities.append({
                "category": category,
                "stock_code": code,
                "qmt_code": qmt_code,
                "list_date": list_date,
                "expire_date": expire_date,
                "affected_trade_dates": affected,
                "affected_trade_dates_sha256": canonical_digest({
                    "schema": "probiga.qmt-daily-no-row-dates.v1",
                    "stock_code": code,
                    "trade_dates": affected,
                }),
                "target_rows": 0,
                "history_rows": 0,
            })
    existing_pairs = {
        (str(entity["stock_code"]), day)
        for entity in entities
        for day in entity["affected_trade_dates"]
    }
    for code, affected in sorted(historical_by_code.items()):
        member = members.get(code)
        if member is None:
            raise RuntimeError(
                f"historical unavailable code is absent from catalog: {code}"
            )
        qmt_code = str(member.get("qmt_code") or "").upper()
        list_date = str(member.get("list_date") or "")[:10]
        raw_expire = member.get("expire_date")
        expire_date = (
            str(raw_expire)[:10]
            if raw_expire not in (None, "", "NaT")
            else None
        )
        if (
            _QMT_CODE_RE.fullmatch(qmt_code) is None
            or qmt_code[:6] != code
        ):
            raise RuntimeError(
                f"historical unavailable qmt_code differs: {code}"
            )
        _date(list_date, label="historical unavailable list_date")
        if expire_date is not None:
            _date(expire_date, label="historical unavailable expire_date")
        if any(
            day not in eligible_by_day
            or code not in eligible_by_day[day]
            or (code, day) in existing_pairs
            for day in affected
        ):
            raise RuntimeError(
                "historical unavailable pair is outside the exact eligible grid"
            )
        entities.append({
            "category": "HISTORICAL_DATA_UNAVAILABLE",
            "stock_code": code,
            "qmt_code": qmt_code,
            "list_date": list_date,
            "expire_date": expire_date,
            "affected_trade_dates": affected,
            "affected_trade_dates_sha256": canonical_digest({
                "schema": "probiga.qmt-daily-no-row-dates.v1",
                "stock_code": code,
                "trade_dates": affected,
            }),
            "target_rows": 0,
            "history_rows": 0,
        })
    if reviewed_window == current_window:
        schema = CURRENT_REVIEWED_UNAVAILABLE_CONTRACT_SCHEMA
    else:
        schema = (
            HISTORICAL_UNAVAILABLE_CONTRACT_SCHEMA
            if historical_pairs
            else NO_ROW_EXCEPTION_CONTRACT_SCHEMA
        )
    core = {
        "schema": schema,
        "policy": (
            "OPERATOR_ACCEPTED_FIXED_WINDOW_HISTORY_UNAVAILABLE_EXACT_PAIRS"
            if historical_pairs
            else "OPERATOR_EXPLICIT_REVIEWED_WINDOW_ZERO_DAILY_ROWS"
        ),
        "start_date": start,
        "end_date": end,
        "catalog_batch_id": str(catalog.batch_id),
        "catalog_member_set_hash": str(catalog.member_set_hash),
        "catalog_manifest_hash": str(catalog.manifest_hash),
        "calendar_batch_id": str(calendar.batch_id),
        "calendar_session_set_hash": str(calendar.session_set_hash),
        "calendar_manifest_hash": str(calendar.manifest_hash),
        "calendar_known_at": str(calendar.known_at),
        "sessions": sessions,
        "exact_lifecycle_no_row_codes": lifecycle,
        "not_yet_listed_no_row_codes": not_listed,
        "entities": entities,
    }
    for key in (
        "catalog_member_set_hash", "catalog_manifest_hash",
        "calendar_session_set_hash", "calendar_manifest_hash",
    ):
        if _SHA256_RE.fullmatch(str(core[key])) is None:
            raise RuntimeError(f"QMT no-row immutable root is invalid: {key}")
    return {**core, "proof_sha256": canonical_digest(core)}


def validate_no_row_exception_contract_shape(
    value: Any,
    *,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _CONTRACT_KEYS:
        raise ValueError("QMT no-row exception contract fields differ")
    schema = value.get("schema")
    if schema not in {
        NO_ROW_EXCEPTION_CONTRACT_SCHEMA,
        HISTORICAL_UNAVAILABLE_CONTRACT_SCHEMA,
        CURRENT_REVIEWED_UNAVAILABLE_CONTRACT_SCHEMA,
    }:
        raise ValueError("QMT no-row exception schema differs")
    if (
        value.get("start_date") != start_date
        or value.get("end_date") != end_date
    ):
        raise ValueError("QMT no-row exception window differs")
    supplied_window = (str(value.get("start_date")), str(value.get("end_date")))
    legacy_window = (
        NO_ROW_REVIEWED_START_DATE,
        LEGACY_NO_ROW_REVIEWED_END_DATE,
    )
    current_window = (
        NO_ROW_REVIEWED_START_DATE,
        NO_ROW_REVIEWED_END_DATE,
    )
    if (
        schema == CURRENT_REVIEWED_UNAVAILABLE_CONTRACT_SCHEMA
        and supplied_window != current_window
    ) or (
        schema in {
            NO_ROW_EXCEPTION_CONTRACT_SCHEMA,
            HISTORICAL_UNAVAILABLE_CONTRACT_SCHEMA,
        }
        and supplied_window != legacy_window
    ):
        raise ValueError("QMT no-row exception schema/window differs")
    if (
        type(value.get("entities")) is not list
        or any(
            type(row) is not dict or set(row) != _ENTITY_KEYS
            for row in value["entities"]
        )
    ):
        raise ValueError("QMT no-row exception entity fields differ")
    historical_entities = [
        row for row in value["entities"]
        if row.get("category") == "HISTORICAL_DATA_UNAVAILABLE"
    ]
    if (
        (schema == NO_ROW_EXCEPTION_CONTRACT_SCHEMA and historical_entities)
        or (
            schema == HISTORICAL_UNAVAILABLE_CONTRACT_SCHEMA
            and not historical_entities
        )
        or any(
            row.get("category") not in {
                "EXACT_LIFECYCLE_NO_ROW",
                "NOT_YET_LISTED_NO_ROW",
                "HISTORICAL_DATA_UNAVAILABLE",
            }
            for row in value["entities"]
        )
    ):
        raise ValueError("QMT no-row exception entity category differs")
    core = {key: value[key] for key in value if key != "proof_sha256"}
    if (
        type(value.get("proof_sha256")) is not str
        or canonical_digest(core) != value["proof_sha256"]
    ):
        raise ValueError("QMT no-row exception proof hash differs")
    return value


def project_catalog_daily_codes(
    *,
    catalog: Any,
    calendar: Any,
    start_date: str,
    end_date: str,
    contract: Mapping[str, Any],
) -> dict[str, list[str]]:
    """Rebuild the proof against roots and return catalog minus exact pairs."""

    shaped = validate_no_row_exception_contract_shape(
        dict(contract), start_date=start_date, end_date=end_date,
    )
    codes = list(shaped["exact_lifecycle_no_row_codes"])
    not_listed = list(shaped["not_yet_listed_no_row_codes"])
    all_codes = sorted(codes + not_listed)
    historical_by_code = {
        str(entity["stock_code"]): list(entity["affected_trade_dates"])
        for entity in shaped["entities"]
        if entity["category"] == "HISTORICAL_DATA_UNAVAILABLE"
    }
    historical_pairs = {
        (code, day)
        for code, days in historical_by_code.items()
        for day in days
    }
    rebuilt = build_no_row_exception_contract(
        catalog=catalog,
        calendar=calendar,
        start_date=start_date,
        end_date=end_date,
        exact_lifecycle_codes=codes,
        not_yet_listed_codes=not_listed,
        target_rows_by_code={code: 0 for code in all_codes},
        history_rows_by_code={code: 0 for code in all_codes},
        historical_unavailable_dates_by_code=historical_by_code,
        target_rows_by_pair={pair: 0 for pair in historical_pairs},
        history_rows_by_pair={pair: 0 for pair in historical_pairs},
    )
    if shaped != rebuilt:
        raise RuntimeError("QMT no-row exception immutable proof differs")
    excluded_by_day: dict[str, set[str]] = {
        day: set() for day in shaped["sessions"]
    }
    for entity in shaped["entities"]:
        for day in entity["affected_trade_dates"]:
            excluded_by_day[day].add(entity["stock_code"])
    return {
        day: sorted(set(catalog.eligible_codes(day)) - excluded_by_day[day])
        for day in shaped["sessions"]
    }


__all__ = [
    "CURRENT_REVIEWED_UNAVAILABLE_CONTRACT_SCHEMA",
    "EXACT_LIFECYCLE_NO_ROW_CANDIDATES",
    "HISTORICAL_UNAVAILABLE_CONTRACT_SCHEMA",
    "LEGACY_NO_ROW_REVIEWED_END_DATE",
    "NOT_YET_LISTED_NO_ROW_CANDIDATES",
    "NOT_YET_LISTED_PROOF_CUTOFF",
    "NO_ROW_REVIEWED_END_DATE",
    "NO_ROW_REVIEWED_START_DATE",
    "NO_ROW_EXCEPTION_CONTRACT_SCHEMA",
    "build_no_row_exception_contract",
    "explicit_no_row_codes",
    "project_catalog_daily_codes",
    "validate_no_row_exception_contract_shape",
]

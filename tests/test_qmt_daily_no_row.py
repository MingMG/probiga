from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from server.common.qmt_attestation_contract import (
    bound_stock_set_contract,
    build_qmt_v2_manifest,
    daily_market_source_batch_id,
    validated_no_row_exception_contract,
    validated_universe_manifest,
)
from server.common.qmt_daily_market_truth import _validate_bound_daily_entries
from server.common.qmt_daily_no_row import (
    CURRENT_REVIEWED_UNAVAILABLE_CONTRACT_SCHEMA,
    HISTORICAL_UNAVAILABLE_CONTRACT_SCHEMA,
    NATIVE_QMT_NO_TRADE_CONTRACT_SCHEMA,
    build_native_qmt_no_trade_contract,
    build_no_row_exception_contract,
    explicit_no_row_codes,
    project_catalog_daily_codes,
    validate_no_row_exception_contract_shape,
)


SESSIONS = [
    "2026-03-06", "2026-03-25", "2026-03-26", "2026-08-27",
]


class _Catalog:
    batch_id = "catalog-1"
    member_count = 6
    member_set_hash = "a" * 64
    manifest_hash = "b" * 64
    members = (
        {
            "stock_code": "000001", "qmt_code": "000001.SZ",
            "list_date": "1991-04-03", "expire_date": None,
        },
        {
            "stock_code": "002231", "qmt_code": "002231.SZ",
            "list_date": "2008-05-12", "expire_date": "2026-03-26",
        },
        *(
            {
                "stock_code": code, "qmt_code": f"{code}.SZ",
                "list_date": "1970-01-01", "expire_date": None,
            }
            for code in ("301688", "301689", "301697", "301699")
        ),
    )

    def eligible_codes(self, day):
        return sorted(
            row["stock_code"]
            for row in self.members
            if row["list_date"] <= day
            and (row["expire_date"] is None or day <= row["expire_date"])
        )


class _Calendar:
    batch_id = "calendar-1"
    session_set_hash = "c" * 64
    manifest_hash = "d" * 64
    known_at = "2026-08-27 18:00:00"

    def sessions_between(self, start, end):
        return [day for day in SESSIONS if start <= day <= end]


def _contract(*, start_date="2026-03-06", end_date="2026-08-27"):
    codes = ["002231", "301688", "301689", "301697", "301699"]
    return build_no_row_exception_contract(
        catalog=_Catalog(),
        calendar=_Calendar(),
        start_date=start_date,
        end_date=end_date,
        exact_lifecycle_codes=["002231"],
        not_yet_listed_codes=["301688", "301689", "301697", "301699"],
        target_rows_by_code={code: 0 for code in codes},
        history_rows_by_code={code: 0 for code in codes},
    )


def test_native_unfilled_response_projects_exact_no_trade_pair():
    catalog = _Catalog()
    calendar = _Calendar()
    day = "2026-08-27"
    pair = ("000001", day)
    source_batch_id = daily_market_source_batch_id(
        catalog_manifest_hash=catalog.manifest_hash,
        calendar_manifest_hash=calendar.manifest_hash,
    )
    proof = build_native_qmt_no_trade_contract(
        catalog=catalog,
        calendar=calendar,
        start_date=day,
        end_date=day,
        no_trade_dates_by_code={"000001": [day]},
        target_rows_by_pair={pair: 0},
        history_rows_by_pair={pair: 0},
        source_batch_by_date={day: source_batch_id},
    )

    assert proof["schema"] == NATIVE_QMT_NO_TRADE_CONTRACT_SCHEMA
    assert proof["entities"][0]["category"] == "NATIVE_QMT_NO_TRADE"
    assert "000001" not in project_catalog_daily_codes(
        catalog=catalog,
        calendar=calendar,
        start_date=day,
        end_date=day,
        contract=proof,
    )[day]
    manifest = build_qmt_v2_manifest({
        day: bound_stock_set_contract(
            day,
            ["301688", "301689", "301697", "301699"],
            catalog_batch_id=catalog.batch_id,
            catalog_member_count=catalog.member_count,
            catalog_member_set_hash=catalog.member_set_hash,
            catalog_manifest_hash=catalog.manifest_hash,
            source_batch_id=source_batch_id,
            calendar_batch_id=calendar.batch_id,
            calendar_session_set_hash=calendar.session_set_hash,
            calendar_manifest_hash=calendar.manifest_hash,
            calendar_known_at=calendar.known_at,
        )
    }, no_row_exception_contract=proof)
    assert validated_universe_manifest(
        manifest,
        start_date=day,
        end_date=day,
    )[day]["stock_count"] == 4


def test_reviewed_no_row_contract_projects_only_exact_window_pairs():
    proof = _contract()
    projected = project_catalog_daily_codes(
        catalog=_Catalog(),
        calendar=_Calendar(),
        start_date="2026-03-06",
        end_date="2026-08-27",
        contract=proof,
    )

    assert proof["exact_lifecycle_no_row_codes"] == ["002231"]
    assert proof["not_yet_listed_no_row_codes"] == [
        "301688", "301689", "301697", "301699",
    ]
    assert len(proof["proof_sha256"]) == 64
    assert projected == {day: ["000001"] for day in SESSIONS}


def test_historical_unavailable_contract_excludes_only_exact_pairs():
    pair = ("000001", "2026-03-25")
    proof = build_no_row_exception_contract(
        catalog=_Catalog(),
        calendar=_Calendar(),
        start_date="2026-03-06",
        end_date="2026-08-27",
        target_rows_by_code={},
        history_rows_by_code={},
        historical_unavailable_dates_by_code={
            "000001": ["2026-03-25"],
        },
        target_rows_by_pair={pair: 0},
        history_rows_by_pair={pair: 0},
    )
    projected = project_catalog_daily_codes(
        catalog=_Catalog(),
        calendar=_Calendar(),
        start_date="2026-03-06",
        end_date="2026-08-27",
        contract=proof,
    )

    assert proof["schema"] == HISTORICAL_UNAVAILABLE_CONTRACT_SCHEMA
    assert proof["entities"] == [{
        "category": "HISTORICAL_DATA_UNAVAILABLE",
        "stock_code": "000001",
        "qmt_code": "000001.SZ",
        "list_date": "1991-04-03",
        "expire_date": None,
        "affected_trade_dates": ["2026-03-25"],
        "affected_trade_dates_sha256": (
            proof["entities"][0]["affected_trade_dates_sha256"]
        ),
        "target_rows": 0,
        "history_rows": 0,
    }]
    assert "000001" in projected["2026-03-06"]
    assert "000001" not in projected["2026-03-25"]
    assert "000001" in projected["2026-03-26"]


def test_historical_unavailable_contract_rejects_existing_pair_rows():
    pair = ("000001", "2026-03-25")
    with pytest.raises(RuntimeError, match="inventory already has daily rows"):
        build_no_row_exception_contract(
            catalog=_Catalog(),
            calendar=_Calendar(),
            start_date="2026-03-06",
            end_date="2026-08-27",
            target_rows_by_code={},
            history_rows_by_code={},
            historical_unavailable_dates_by_code={
                "000001": ["2026-03-25"],
            },
            target_rows_by_pair={pair: 1},
            history_rows_by_pair={pair: 0},
        )


def test_current_reviewed_window_uses_versioned_contract_without_rewriting_legacy():
    original_sessions = list(SESSIONS)
    SESSIONS.append("2026-08-28")
    pair = ("000001", "2026-08-28")
    try:
        proof = build_no_row_exception_contract(
            catalog=_Catalog(),
            calendar=_Calendar(),
            start_date="2026-03-06",
            end_date="2026-08-28",
            target_rows_by_code={},
            history_rows_by_code={},
            historical_unavailable_dates_by_code={
                "000001": ["2026-08-28"],
            },
            target_rows_by_pair={pair: 0},
            history_rows_by_pair={pair: 0},
        )
        projected = project_catalog_daily_codes(
            catalog=_Catalog(),
            calendar=_Calendar(),
            start_date="2026-03-06",
            end_date="2026-08-28",
            contract=proof,
        )
    finally:
        SESSIONS[:] = original_sessions

    assert proof["schema"] == CURRENT_REVIEWED_UNAVAILABLE_CONTRACT_SCHEMA
    assert "000001" not in projected["2026-08-28"]
    assert _contract()["schema"] != proof["schema"]


def test_no_row_schema_cannot_be_relabelled_across_reviewed_windows():
    from server.common.qmt_attestation_contract import canonical_digest

    relabelled = deepcopy(_contract())
    relabelled["schema"] = CURRENT_REVIEWED_UNAVAILABLE_CONTRACT_SCHEMA
    core = {
        key: value
        for key, value in relabelled.items()
        if key != "proof_sha256"
    }
    relabelled["proof_sha256"] = canonical_digest(core)

    with pytest.raises(ValueError, match="schema/window differs"):
        validate_no_row_exception_contract_shape(
            relabelled,
            start_date="2026-03-06",
            end_date="2026-08-27",
        )


def test_no_row_contract_rejects_unreviewed_code_future_window_and_rows():
    with pytest.raises(ValueError, match="allowed"):
        explicit_no_row_codes(
            "300344", category="EXACT_LIFECYCLE_NO_ROW",
        )
    with pytest.raises(ValueError, match="allowed"):
        explicit_no_row_codes(
            "688835", category="NOT_YET_LISTED_NO_ROW",
        )
    with pytest.raises(RuntimeError, match="exact reviewed window"):
        _contract(end_date="2026-08-29")
    codes = ["301688"]
    with pytest.raises(RuntimeError, match="already has daily rows"):
        build_no_row_exception_contract(
            catalog=_Catalog(), calendar=_Calendar(),
            start_date="2026-03-06", end_date="2026-08-27",
            not_yet_listed_codes=codes,
            target_rows_by_code={"301688": 1},
            history_rows_by_code={"301688": 0},
        )


@pytest.mark.parametrize(
    ("start_date", "end_date"),
    (
        ("2026-03-05", "2026-08-27"),
        ("2026-03-06", "2026-08-26"),
        ("2026-02-01", "2026-03-31"),
        ("2026-08-21", "2026-08-24"),
    ),
)
def test_no_row_contract_rejects_every_non_reviewed_window(
    start_date,
    end_date,
):
    with pytest.raises(RuntimeError, match="exact reviewed window"):
        _contract(start_date=start_date, end_date=end_date)


def test_manifest_binds_no_row_proof_and_rejects_hash_tamper():
    proof = _contract()
    daily = {
        day: {"stock_count": 1, "stock_set_hash": "e" * 64}
        for day in SESSIONS
    }
    manifest = build_qmt_v2_manifest(
        daily, no_row_exception_contract=proof,
    )

    assert validated_universe_manifest(
        manifest, start_date=SESSIONS[0], end_date=SESSIONS[-1],
    ) == daily
    assert validated_no_row_exception_contract(
        manifest, start_date=SESSIONS[0], end_date=SESSIONS[-1],
    )["proof_sha256"] == proof["proof_sha256"]

    tampered = deepcopy(manifest)
    tampered["no_row_exception_contract"]["entities"][0][
        "affected_trade_dates"
    ].append("2026-08-26")
    with pytest.raises(ValueError, match="proof hash differs"):
        validated_universe_manifest(
            tampered, start_date=SESSIONS[0], end_date=SESSIONS[-1],
        )


def test_market_truth_validates_projected_expected_sets_against_proof():
    catalog = _Catalog()
    calendar = _Calendar()
    proof = _contract()
    projected = project_catalog_daily_codes(
        catalog=catalog,
        calendar=calendar,
        start_date=SESSIONS[0],
        end_date=SESSIONS[-1],
        contract=proof,
    )
    source_id = daily_market_source_batch_id(
        catalog_manifest_hash=catalog.manifest_hash,
        calendar_manifest_hash=calendar.manifest_hash,
    )
    daily = {
        day: bound_stock_set_contract(
            day, codes,
            catalog_batch_id=catalog.batch_id,
            catalog_member_count=catalog.member_count,
            catalog_member_set_hash=catalog.member_set_hash,
            catalog_manifest_hash=catalog.manifest_hash,
            source_batch_id=source_id,
            calendar_batch_id=calendar.batch_id,
            calendar_session_set_hash=calendar.session_set_hash,
            calendar_manifest_hash=calendar.manifest_hash,
            calendar_known_at=calendar.known_at,
        )
        for day, codes in projected.items()
    }

    assert _validate_bound_daily_entries(
        daily,
        catalog=catalog,
        calendar=calendar,
        run_start_date=SESSIONS[0],
        run_end_date=SESSIONS[-1],
        no_row_exception_contract=proof,
    ) == len(SESSIONS)

    altered = deepcopy(proof)
    altered["catalog_manifest_hash"] = "f" * 64
    altered_core = {k: v for k, v in altered.items() if k != "proof_sha256"}
    from server.common.qmt_attestation_contract import canonical_digest
    altered["proof_sha256"] = canonical_digest(altered_core)
    with pytest.raises(RuntimeError, match="immutable proof differs"):
        _validate_bound_daily_entries(
            daily,
            catalog=catalog,
            calendar=calendar,
            run_start_date=SESSIONS[0],
            run_end_date=SESSIONS[-1],
            no_row_exception_contract=altered,
        )

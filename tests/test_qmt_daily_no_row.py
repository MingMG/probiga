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
    build_no_row_exception_contract,
    explicit_no_row_codes,
    project_catalog_daily_codes,
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


def _contract(*, end_date="2026-08-27"):
    codes = ["002231", "301688", "301689", "301697", "301699"]
    return build_no_row_exception_contract(
        catalog=_Catalog(),
        calendar=_Calendar(),
        start_date="2026-03-06",
        end_date=end_date,
        exact_lifecycle_codes=["002231"],
        not_yet_listed_codes=["301688", "301689", "301697", "301699"],
        target_rows_by_code={code: 0 for code in codes},
        history_rows_by_code={code: 0 for code in codes},
    )


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


def test_no_row_contract_rejects_unreviewed_code_future_window_and_rows():
    with pytest.raises(ValueError, match="allowed"):
        explicit_no_row_codes(
            "300344", category="EXACT_LIFECYCLE_NO_ROW",
        )
    with pytest.raises(ValueError, match="allowed"):
        explicit_no_row_codes(
            "688835", category="NOT_YET_LISTED_NO_ROW",
        )
    with pytest.raises(RuntimeError, match="reviewed cutoff"):
        _contract(end_date="2026-08-28")
    codes = ["301688"]
    with pytest.raises(RuntimeError, match="already has daily rows"):
        build_no_row_exception_contract(
            catalog=_Catalog(), calendar=_Calendar(),
            start_date="2026-03-06", end_date="2026-08-27",
            not_yet_listed_codes=codes,
            target_rows_by_code={"301688": 1},
            history_rows_by_code={"301688": 0},
        )


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

from __future__ import annotations

from copy import deepcopy

import pytest

from integrations.bigqmt.reference import PROVIDER_ID
from server.engine import strategy_center


TARGET = "2026-08-21"
CAPTURED_AT = "2026-08-21 15:30:00"


def _concept_rows() -> list[dict]:
    return [
        {
            "snapshot_date": TARGET,
            "source": PROVIDER_ID,
            "concept_code": "BK001",
            "concept_name": "人工智能",
            "stock_code": "000001",
            "short_name": "平安银行",
            "quality_status": "QMT_VALIDATED",
            "captured_at": CAPTURED_AT,
        },
        {
            "snapshot_date": TARGET,
            "source": PROVIDER_ID,
            "concept_code": "BK001",
            "concept_name": "人工智能",
            "stock_code": "000002",
            "short_name": "万科A",
            "quality_status": "QMT_VALIDATED",
            "captured_at": CAPTURED_AT,
        },
    ]


def _industry_rows() -> list[dict]:
    return [
        {
            "snapshot_date": TARGET,
            "source": PROVIDER_ID,
            "industry_code": "801780",
            "industry_name": "银行",
            "industry_type": "申万一级",
            "stock_code": "000001",
            "short_name": "平安银行",
            "quality_status": "QMT_VALIDATED",
            "captured_at": CAPTURED_AT,
        },
        {
            "snapshot_date": TARGET,
            "source": PROVIDER_ID,
            "industry_code": "801180",
            "industry_name": "房地产",
            "industry_type": "申万一级",
            "stock_code": "000002",
            "short_name": "万科A",
            "quality_status": "QMT_VALIDATED",
            "captured_at": CAPTURED_AT,
        },
    ]


def _run(concept_rows: list[dict], industry_rows: list[dict]) -> dict:
    return {
        "snapshot_date": TARGET,
        "source": PROVIDER_ID,
        "quality_status": "QMT_VALIDATED",
        "capture_mode": "qmt_close_full_refresh",
        "concept_count": len({row["concept_code"] for row in concept_rows}),
        "concept_relation_count": len(concept_rows),
        "industry_count": len({row["industry_code"] for row in industry_rows}),
        "industry_relation_count": len(industry_rows),
        "concept_hash": strategy_center._membership_snapshot_hash(
            concept_rows, member_type="concept"
        ),
        "industry_hash": strategy_center._membership_snapshot_hash(
            industry_rows, member_type="industry"
        ),
        "captured_at": CAPTURED_AT,
    }


def _install_reader(
    monkeypatch: pytest.MonkeyPatch,
    *,
    run_rows: list[dict],
    concept_rows: list[dict],
    industry_rows: list[dict],
) -> None:
    monkeypatch.setattr(strategy_center, "_kline_table_exists", lambda _name: True)
    monkeypatch.setattr(strategy_center, "get_kline_engine", lambda: object())

    def read(_engine, sql, params=None, **_kwargs):
        normalized = " ".join(str(sql).split())
        if "FROM qmt_membership_snapshot_run" in normalized:
            if "snapshot_date = :snapshot_date" in normalized:
                target = str((params or {}).get("snapshot_date") or "")
                return [
                    deepcopy(row)
                    for row in run_rows
                    if str(row.get("snapshot_date")) == target
                ]
            return deepcopy(run_rows)
        if "FROM `qmt_concept_member_snapshot`" in normalized:
            return deepcopy(concept_rows)
        if "FROM `qmt_industry_member_snapshot`" in normalized:
            return deepcopy(industry_rows)
        raise AssertionError(normalized)

    monkeypatch.setattr(strategy_center, "read_sql_rows", read)


def test_membership_history_replays_full_snapshot_before_display_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    concepts = _concept_rows()
    industries = _industry_rows()
    run = _run(concepts, industries)
    _install_reader(
        monkeypatch,
        run_rows=[run],
        concept_rows=concepts,
        industry_rows=industries,
    )

    result = strategy_center.load_membership_snapshot_history(
        member_type="concept", limit=1
    )

    assert result["status"] == "verified"
    assert result["snapshot_complete"] is True
    assert result["published_relation_count"] == 2
    assert result["verified_relation_count"] == 2
    assert result["filtered_relation_count"] == 2
    assert result["total_returned"] == 1
    assert result["display_truncated"] is True
    assert result["published_snapshot_hash"] == result["replayed_snapshot_hash"]
    assert result["verified_full_snapshot_before_filters"] is True
    assert result["data_category"] == (
        "POINT_IN_TIME_CONSTITUENT_MEMBERSHIP"
    )
    assert result["excluded_data_categories"] == [
        "SECTOR_HEAT_HISTORY",
        "SECTOR_ROTATION_HISTORY",
    ]
    assert "不代表板块热度" in result["data_semantics"]
    assert result["automatic_real_order_submission"] is False


def test_membership_history_rejects_hash_count_and_member_fact_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    concepts = _concept_rows()
    industries = _industry_rows()
    run = _run(concepts, industries)
    run["concept_hash"] = "f" * 64
    concepts[0]["quality_status"] = "UNVERIFIED"
    _install_reader(
        monkeypatch,
        run_rows=[run],
        concept_rows=concepts,
        industry_rows=industries,
    )

    result = strategy_center.load_membership_snapshot_history(
        member_type="concept"
    )

    assert result["status"] == "integrity_error"
    assert result["snapshot_complete"] is False
    assert result["data"] == []
    assert "验真状态" in result["reason"]
    assert "canonical hash" in result["reason"]


def test_membership_history_explicit_date_never_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    concepts = _concept_rows()
    industries = _industry_rows()
    run = _run(concepts, industries)
    _install_reader(
        monkeypatch,
        run_rows=[run],
        concept_rows=concepts,
        industry_rows=industries,
    )

    result = strategy_center.load_membership_snapshot_history(
        snapshot_date="2026-08-20", member_type="industry"
    )

    assert result["status"] == "empty"
    assert result["snapshot_date"] == "2026-08-20"
    assert result["data"] == []
    assert "不回退旧日数据" in result["reason"]


@pytest.mark.parametrize(
    "raw_date",
    ("2026-08-20T15:00:00", "2026-08-20-extra", "2026/08/20"),
)
def test_membership_history_requires_an_exact_iso_date(
    raw_date: str,
) -> None:
    with pytest.raises(ValueError, match="ISO calendar date"):
        strategy_center.load_membership_snapshot_history(
            snapshot_date=raw_date,
            member_type="industry",
        )


def test_membership_history_industry_filter_keeps_verified_full_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    concepts = _concept_rows()
    industries = _industry_rows()
    run = _run(concepts, industries)
    _install_reader(
        monkeypatch,
        run_rows=[run],
        concept_rows=concepts,
        industry_rows=industries,
    )

    result = strategy_center.load_membership_snapshot_history(
        snapshot_date=TARGET,
        member_type="industry",
        stock_code="1",
    )

    assert result["status"] == "verified"
    assert result["verified_relation_count"] == 2
    assert result["filtered_relation_count"] == 1
    assert result["data"][0]["stock_code"] == "000001"
    assert result["data"][0]["industry_type"] == "申万一级"


def test_membership_history_unvalidated_run_is_not_displayed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    concepts = _concept_rows()
    industries = _industry_rows()
    run = _run(concepts, industries)
    run["quality_status"] = "PENDING"
    _install_reader(
        monkeypatch,
        run_rows=[run],
        concept_rows=concepts,
        industry_rows=industries,
    )

    result = strategy_center.load_membership_snapshot_history(
        member_type="industry"
    )

    assert result["status"] == "not_ready"
    assert result["snapshot_complete"] is False
    assert result["data"] == []


def test_membership_history_rejects_wrong_run_source_even_on_exact_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    concepts = _concept_rows()
    industries = _industry_rows()
    run = _run(concepts, industries)
    run["source"] = "UNTRUSTED_SOURCE"
    _install_reader(
        monkeypatch,
        run_rows=[run],
        concept_rows=concepts,
        industry_rows=industries,
    )

    result = strategy_center.load_membership_snapshot_history(
        snapshot_date=TARGET,
        member_type="concept",
        limit=1,
    )

    assert result["status"] == "integrity_error"
    assert result["snapshot_complete"] is False
    assert result["data"] == []
    assert "来源不是" in result["reason"]


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("concept_count", 99, "发布分组数"),
        ("concept_relation_count", 1, "发布关系数"),
        ("capture_mode", "incremental", "全量冻结模式"),
        ("captured_at", "2026-08-21 14:59:59", "收盘后窗口"),
    ],
)
def test_membership_history_validates_full_run_contract_before_limit(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
    reason: str,
) -> None:
    concepts = _concept_rows()
    industries = _industry_rows()
    run = _run(concepts, industries)
    run[field] = value
    _install_reader(
        monkeypatch,
        run_rows=[run],
        concept_rows=concepts,
        industry_rows=industries,
    )

    result = strategy_center.load_membership_snapshot_history(
        member_type="concept",
        limit=1,
    )

    assert result["status"] == "integrity_error"
    assert result["data"] == []
    assert reason in result["reason"]

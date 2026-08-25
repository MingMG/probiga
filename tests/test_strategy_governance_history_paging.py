import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.routers import strategy_center as router
from server.engine import strategy_governance as governance


HISTORY_TIME = "2026-08-24 10:00:00"


def _lifecycle_row(number: int, *, entity_key: str = "trend") -> dict:
    payload = {
        "schema": "probiga.strategy-lifecycle-event.v1",
        "sequence": number,
    }
    return {
        "event_id": f"{number:032x}",
        "entity_type": "STRATEGY",
        "entity_key": entity_key,
        "entity_version": "v1",
        "previous_status": "SHADOW",
        "next_status": "ACTIVE",
        "reason": f"第{number}次恢复理由",
        "trigger_type": "AUTOMATIC_GATE",
        "event_hash": governance._digest(payload),
        "operator_name": "daily-governance",
        "occurred_at": HISTORY_TIME,
        "payload_json": json.dumps(payload, ensure_ascii=False),
    }


def _audit_row(number: int, *, entity_key: str = "trend") -> dict:
    payload = {
        "schema": "probiga.strategy-governance-audit.v1",
        "sequence": number,
    }
    return {
        "audit_id": f"{number:032x}",
        "entity_type": "STRATEGY",
        "entity_key": entity_key,
        "action": "LIFECYCLE_TRANSITION",
        "reason": f"第{number}次审计理由",
        "operator_name": "daily-governance",
        "audit_hash": governance._digest(payload),
        "created_at": HISTORY_TIME,
        "payload_json": json.dumps(payload, ensure_ascii=False),
    }


def _history_reader(lifecycle_rows, audit_rows):
    def read(sql, params=None):
        params = params or {}
        source = audit_rows if "st_strategy_governance_audit" in sql else lifecycle_rows
        timestamp_key = "created_at" if source is audit_rows else "occurred_at"
        id_key = "audit_id" if source is audit_rows else "event_id"
        rows = [dict(row) for row in source]
        entity_type = params.get("history_entity_type")
        entity_key = params.get("history_entity_key")
        action = params.get("history_action")
        if entity_type:
            rows = [row for row in rows if row["entity_type"] == entity_type]
        if entity_key:
            rows = [row for row in rows if row["entity_key"] == entity_key]
        if action:
            action_key = "action" if source is audit_rows else "trigger_type"
            rows = [row for row in rows if row[action_key] == action]
        rows.sort(key=lambda row: (row[timestamp_key], row[id_key]), reverse=True)
        if sql.startswith("SELECT (SELECT COUNT(*)"):
            return [{
                "total_count": len(rows),
                "high_timestamp": rows[0][timestamp_key] if rows else None,
                "high_id": rows[0][id_key] if rows else None,
            }]
        after_ts = params.get("history_after_ts")
        after_id = params.get("history_after_id")
        if after_ts and after_id:
            rows = [
                row for row in rows
                if (row[timestamp_key], row[id_key]) < (after_ts, after_id)
            ]
        return rows[: int(params.get("fetch_limit") or params.get("limit") or 100)]

    return read


def _single_section_reader(rows, *, timestamp_key, id_key):
    def read(sql, params=None):
        params = params or {}
        selected = [dict(row) for row in rows]
        selected.sort(
            key=lambda row: (row[timestamp_key], row[id_key]), reverse=True,
        )
        if sql.startswith("SELECT (SELECT COUNT(*)"):
            return [{
                "total_count": len(selected),
                "high_timestamp": (
                    selected[0][timestamp_key] if selected else None
                ),
                "high_id": selected[0][id_key] if selected else None,
            }]
        after_ts = params.get("history_after_ts")
        after_id = params.get("history_after_id")
        if after_ts and after_id:
            selected = [
                row for row in selected
                if (row[timestamp_key], row[id_key]) < (after_ts, after_id)
            ]
        return selected[: int(params.get("fetch_limit") or 100)]

    return read


def _metric_row(number: int) -> dict:
    metrics = {
        "completed_trades": 40,
        "coverage_days": 120,
        "net_expectancy_pct": 0.18,
        "payoff_ratio": 1.4,
        "profit_factor": 1.3,
        "max_drawdown_pct": 5.0,
        "cost_stress_expectancy_pct": 0.09,
    }
    row = {
        "evidence_id": f"{number:032x}",
        "entity_type": "STRATEGY",
        "strategy_key": "trend",
        "strategy_version": "v1",
        "as_of_date": "2026-08-23",
        "window_days": 120,
        "metrics_json": json.dumps(metrics),
        "source": "independent-review",
        "evidence_protocol": "PURGED_WALK_FORWARD_V1",
        "artifact_hash": "a" * 64,
        "source_dataset_hash": "b" * 64,
        "evidence_revision_at": "2026-08-23T18:00:00",
        "verification_status": "CONFIRMED",
        "funding_provenance": "EXTERNAL_SUBMITTED",
        "submitted_by": "submitter",
        "reviewed_by": "reviewer",
        "reviewed_at": "2026-08-24T09:00:00",
        "created_at": HISTORY_TIME,
    }
    row["evidence_hash"] = governance._digest(
        governance._metric_submission_contract(row)
    )
    return row


def _adapter_row(number: int) -> dict:
    return {
        "run_uid": f"{number:032x}",
        "strategy_key": "trend",
        "strategy_version": "v1",
        "strategy_version_hash": "a" * 64,
        "trade_date": "2026-08-23",
        "completed_at": HISTORY_TIME,
        "status": "COMPLETED",
        "execution_binding_hash": "b" * 64,
        "adapter_artifact_sha256": "c" * 64,
        "cost_model_hash": "d" * 64,
        "adapter_key": "trend_adapter",
        "adapter_version": "v1",
        "input_hash": "e" * 64,
        "output_hash": "f" * 64,
        "stable_result_hash": "1" * 64,
        "candidate_count": 5000,
        "candidate_identity_json": "[]",
        "receipt_json": "{}",
        "receipt_hash": "2" * 64,
        "created_at": HISTORY_TIME,
    }


def _run_row(number: int) -> dict:
    return {
        "run_uid": f"{number:032x}",
        "trade_date": "2026-08-23",
        "run_revision": number,
        "supersedes_run_uid": "",
        "is_canonical": 1,
        "market_state": "TREND_UP",
        "source_status": "ready",
        "input_ready": 1,
        "input_hash": "a" * 64,
        "build_commit_sha": "b" * 40,
        "decision_hash": "c" * 64,
        "result_hash": "d" * 64,
        "status": "COMPLETED",
        "strategy_count": 750,
        "formal_count": 12,
        "shadow_count": 738,
        "combination_count": 25,
        "observation_count": 750,
        "confirmation_count": 5,
        "tradable_count": 3,
        "allocation_count": 3,
        "created_at": HISTORY_TIME,
        "finished_at": HISTORY_TIME,
    }


def test_history_page_uses_timestamp_and_stable_id_without_tie_loss(monkeypatch):
    lifecycle_rows = [_lifecycle_row(number) for number in range(1, 102)]
    audit_rows = [_audit_row(number) for number in range(1, 4)]
    monkeypatch.setattr(governance, "_table_exists", lambda _table: True)
    monkeypatch.setattr(
        governance, "_db_read", _history_reader(lifecycle_rows, audit_rows),
    )

    first = governance.governance_history_section_page(
        "lifecycle", limit=100,
    )
    second = governance.governance_history_section_page(
        "lifecycle", limit=100, cursor=first["next_cursor"],
    )

    first_ids = [row["event_id"] for row in first["rows"]]
    second_ids = [row["event_id"] for row in second["rows"]]
    assert len(first_ids) == 100
    assert len(second_ids) == 1
    assert len(set(first_ids + second_ids)) == 101
    assert second["next_cursor"] is None
    assert first["total_count"] == 101
    assert first["high_watermark"] == {
        "timestamp": HISTORY_TIME,
        "stable_id": f"{101:032x}",
    }
    assert all(row["hash_valid"] is True for row in first["rows"])
    assert all("payload_json" not in row for row in first["rows"])
    assert first["raw_payload_inline"] is False
    assert first["automatic_real_order_submission"] is False
    assert first["real_order_authority"] is False


def test_history_cursor_binds_section_filter_limit_and_revision(monkeypatch):
    lifecycle_rows = [_lifecycle_row(number) for number in range(1, 4)]
    audit_rows = [_audit_row(number) for number in range(1, 4)]
    monkeypatch.setattr(governance, "_table_exists", lambda _table: True)
    monkeypatch.setattr(
        governance, "_db_read", _history_reader(lifecycle_rows, audit_rows),
    )
    first = governance.governance_history_section_page(
        "lifecycle", limit=2, entity_key="trend",
    )
    cursor = first["next_cursor"]
    assert cursor

    with pytest.raises(ValueError, match="筛选或页大小"):
        governance.governance_history_section_page(
            "lifecycle", limit=1, cursor=cursor, entity_key="trend",
        )
    with pytest.raises(ValueError, match="筛选或页大小"):
        governance.governance_history_section_page(
            "lifecycle", limit=2, cursor=cursor, entity_key="other",
        )
    with pytest.raises(ValueError, match="分页游标"):
        governance.governance_history_section_page(
            "audit", limit=2, cursor=cursor, entity_key="trend",
        )

    lifecycle_rows.append(_lifecycle_row(4))
    with pytest.raises(ValueError, match="新修订"):
        governance.governance_history_section_page(
            "lifecycle", limit=2, cursor=cursor, entity_key="trend",
        )


@pytest.mark.parametrize(
    ("section", "row_factory", "timestamp_key", "id_key"),
    (
        ("metric_evidence", _metric_row, "created_at", "evidence_id"),
        (
            "adapter_run_receipts", _adapter_row,
            "completed_at", "run_uid",
        ),
        ("runs", _run_row, "created_at", "run_uid"),
    ),
)
def test_all_history_ledgers_page_tied_timestamps_without_loss(
    monkeypatch, section, row_factory, timestamp_key, id_key,
):
    rows = [row_factory(number) for number in range(1, 102)]
    monkeypatch.setattr(governance, "_table_exists", lambda _table: True)
    monkeypatch.setattr(
        governance,
        "_db_read",
        _single_section_reader(
            rows, timestamp_key=timestamp_key, id_key=id_key,
        ),
    )
    if section == "adapter_run_receipts":
        candidate_identity = [f"{number:06d}" for number in range(5000)]
        monkeypatch.setattr(
            governance,
            "verify_persisted_strategy_adapter_run_receipt",
            lambda *_args: {"candidate_identity": candidate_identity},
        )

    first = governance.governance_history_section_page(section, limit=100)
    second = governance.governance_history_section_page(
        section, limit=100, cursor=first["next_cursor"],
    )
    ids = [row[id_key] for row in first["rows"] + second["rows"]]

    assert len(ids) == 101
    assert len(set(ids)) == 101
    assert second["next_cursor"] is None
    assert first["total_count"] == 101
    if section in {"metric_evidence", "adapter_run_receipts"}:
        assert all(row["hash_valid"] is True for row in first["rows"])
    else:
        assert all(
            row["result_hash_reference_valid"] is True
            for row in first["rows"]
        )
    assert all("metrics_json" not in row for row in first["rows"])
    assert all("receipt_json" not in row for row in first["rows"])
    assert all("candidate_identity_json" not in row for row in first["rows"])
    assert all("result_json" not in row for row in first["rows"])
    if section == "metric_evidence":
        assert all(
            row["source_authority"] == "CLIENT_DECLARED_UNATTESTED"
            and row["review_scope"] == "STRUCTURE_AND_REPRODUCIBILITY_ONLY"
            and row["funding_authority"] is False
            and row["real_order_authority"] is False
            for row in first["rows"]
        )
    if section == "adapter_run_receipts":
        assert all(
            row["candidate_identity_count"] == 5000
            and "candidate_identity" not in row
            for row in first["rows"]
        )
def test_legacy_history_does_not_inline_lifecycle_or_audit_json(monkeypatch):
    lifecycle = _lifecycle_row(1)
    audit = _audit_row(1)
    seen_sql = []

    def read(sql, _params=None):
        seen_sql.append(sql)
        if "FROM st_strategy_lifecycle_event" in sql:
            return [dict(lifecycle)]
        if "FROM st_strategy_governance_audit" in sql:
            return [dict(audit)]
        return []

    monkeypatch.setattr(governance, "_table_exists", lambda _table: True)
    monkeypatch.setattr(governance, "_db_read", read)

    result = governance.governance_history(limit=10)

    assert "payload_json" not in result["lifecycle_events"][0]
    assert "payload_json" not in result["audit_events"][0]
    assert result["lifecycle_events"][0]["hash_valid"] is True
    assert result["audit_events"][0]["hash_valid"] is True
    assert result["next_cursors"]["lifecycle_events"] == ""
    assert result["next_cursors"]["audit_events"] == ""
    assert result["next_cursors"]["metric_evidence"] == ""
    assert result["next_cursors"]["adapter_run_receipts"] == ""
    assert result["next_cursors"]["runs"] == ""
    assert result["history_section_pages"]["cursor_identity"] == (
        "timestamp_and_stable_id"
    )
    assert set(result["history_section_pages"]) == {
        "lifecycle", "audit", "metric_evidence",
        "adapter_run_receipts", "runs", "cursor_identity",
    }
    assert not any(
        "SELECT * FROM st_strategy_lifecycle_event" in sql
        or "SELECT * FROM st_strategy_governance_audit" in sql
        for sql in seen_sql
    )


def _valid_page(**overrides):
    page = {
        "schema": "probiga.strategy-governance-history-section-page.v1",
        "section": "lifecycle",
        "rows": [],
        "row_count": 0,
        "total_count": 0,
        "history_revision_hash": "b" * 64,
        "next_cursor": None,
        "raw_payload_inline": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    page.update(overrides)
    page["page_hash"] = router._governance_page_digest(page)
    return page


def test_history_query_validation_is_422_not_internal_contract_failure(monkeypatch):
    monkeypatch.setattr(
        router,
        "governance_history",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValueError("历史筛选实体类型无效")
        ),
    )
    app = FastAPI()
    app.include_router(router.router, prefix="/api")
    response = TestClient(app).get(
        "/api/strategy-center/governance/history?entity_type=BAD"
    )

    assert response.status_code == 422
    assert response.json()["error"] == "invalid_governance_history_query"
    assert response.json()["automatic_real_order_submission"] is False
    assert response.json()["real_order_authority"] is False


@pytest.mark.parametrize(
    "unsafe_page",
    (
        _valid_page(automatic_real_order_submission=True),
        _valid_page(automatic_real_order_submission="false"),
        _valid_page(real_order_authority=True),
        _valid_page(real_order_authority="false"),
        _valid_page(real_order_authority=None),
        _valid_page(rows=[{"event_id": "a" * 32, "payload_json": "{}"}], row_count=1),
    ),
)
def test_history_page_route_rejects_unsafe_or_inline_contracts(
    monkeypatch, unsafe_page,
):
    monkeypatch.setattr(
        router, "governance_history_section_page",
        lambda *_args, **_kwargs: unsafe_page,
    )
    app = FastAPI()
    app.include_router(router.router, prefix="/api")
    response = TestClient(app).get(
        "/api/strategy-center/governance/history/lifecycle"
    )

    assert response.status_code == 500
    assert response.json()["error"] == (
        "invalid_governance_history_page_contract"
    )
    assert response.json()["automatic_real_order_submission"] is False
    assert response.json()["real_order_authority"] is False


def test_history_page_route_returns_bounded_safe_page(monkeypatch):
    monkeypatch.setattr(
        router, "governance_history_section_page",
        lambda *_args, **_kwargs: _valid_page(),
    )
    app = FastAPI()
    app.include_router(router.router, prefix="/api")
    response = TestClient(app).get(
        "/api/strategy-center/governance/history/lifecycle?limit=50"
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["page"]["raw_payload_inline"] is False
    assert response.json()["automatic_real_order_submission"] is False
    assert response.json()["real_order_authority"] is False


def test_hyphenated_history_route_maps_to_internal_section(monkeypatch):
    observed = []

    def page(section, **_kwargs):
        observed.append(section)
        return _valid_page(section=section)

    monkeypatch.setattr(router, "governance_history_section_page", page)
    app = FastAPI()
    app.include_router(router.router, prefix="/api")

    for public_section, internal_section in (
        ("metric-evidence", "metric_evidence"),
        ("adapter-run-receipts", "adapter_run_receipts"),
        ("runs", "runs"),
    ):
        response = TestClient(app).get(
            "/api/strategy-center/governance/history/" + public_section
        )
        assert response.status_code == 200
        assert response.json()["page"]["section"] == internal_section

    assert observed == ["metric_evidence", "adapter_run_receipts", "runs"]


def test_history_ui_pages_every_ledger_without_hidden_slices():
    source = open("server/static/js/app.js", encoding="utf-8").read()

    assert "history/metric-evidence?limit=50" in source
    assert "history/adapter-run-receipts?limit=50" in source
    assert "history/runs?limit=50" in source
    assert "metric_evidence:'metric-evidence'" in source
    assert "adapter_run_receipts:'adapter-run-receipts'" in source
    assert "metricEvidence.slice(0, 30)" not in source
    assert "adapterRunReceipts.slice(0, 30)" not in source
    assert "runs.slice(0, 20)" not in source

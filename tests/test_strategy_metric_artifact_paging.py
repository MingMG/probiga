from __future__ import annotations

import json
from contextlib import nullcontext

import pytest

from server.api.routers import strategy_center as router
from server.engine import strategy_governance as governance


def _artifact() -> dict:
    trades = [
        {
            "evidence_id": f"{index:064x}",
            "trade_date": "2026-08-21",
            "label_available_at": "2026-08-21T15:00:00",
            "observed_at": "2026-08-21T15:00:00",
            "net_return_pct": 0.1,
            "cost_pct": 0.01,
        }
        for index in range(250)
    ]
    equity_curve = [
        {"trade_date": f"2026-08-{index + 1:02d}", "equity": 100 + index}
        for index in range(20)
    ]
    segments = []
    for segment_index in range(1, 4):
        train_dataset = [
            {
                "observation_id": f"{segment_index * 1000 + index:064x}",
                "observed_at": "2026-08-01T15:00:00",
                "label_available_at": "2026-08-02T15:00:00",
                "feature_snapshot_hash": "a" * 64,
                "label_snapshot_hash": "b" * 64,
            }
            for index in range(125)
        ]
        segments.append({
            "train_start": "2026-07-01",
            "train_end": "2026-07-31",
            "test_start": "2026-08-01",
            "test_end": "2026-08-21",
            "completed_trades": 1,
            "net_expectancy_pct": 0.1,
            "train_dataset": train_dataset,
            "train_dataset_hash": governance._digest(train_dataset),
            "test_dataset_hash": "c" * 64,
        })
    return {
        "schema_version": "probiga.strategy-validation-artifact.v3",
        "source_dataset_hash": "d" * 64,
        "trades": trades,
        "equity_curve": equity_curve,
        "segments": segments,
    }


class _FakeResult:
    def __init__(self, *, rows=None, scalar=None):
        self._rows = list(rows or [])
        self._scalar = scalar

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def one(self):
        assert len(self._rows) == 1
        return self._rows[0]

    def all(self):
        return self._rows

    def scalar(self):
        return self._scalar


class _ArtifactConnection:
    def __init__(self, artifact, *, storage_bytes=None, storage_hash=None):
        self.artifact = artifact
        self.storage_bytes = storage_bytes or len(
            governance._json_text(artifact).encode("utf-8")
        )
        self.storage_hash = storage_hash or governance._digest(artifact)
        self.queries = []

    def execution_options(self, **options):
        assert options == {"isolation_level": "REPEATABLE READ"}
        return self

    def begin(self):
        return nullcontext()

    def close(self):
        return None

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.queries.append((sql, dict(params)))
        if "OCTET_LENGTH(artifact_json)" in sql:
            segments = self.artifact["segments"]
            return _FakeResult(rows=[{
                "evidence_id": "e" * 32,
                "entity_type": "STRATEGY",
                "strategy_key": "alpha",
                "strategy_version": "v1",
                "as_of_date": "2026-08-21",
                "window_days": 60,
                "metrics_json": "{}",
                "source": "EXTERNAL_SUBMITTED",
                "evidence_protocol": "probiga.strategy-validation-artifact.v3",
                "artifact_hash": governance._digest(self.artifact),
                "source_dataset_hash": self.artifact["source_dataset_hash"],
                "evidence_revision_at": "2026-08-21 15:00:00",
                "verification_status": "CONFIRMED",
                "funding_provenance": "EXTERNAL_SUBMITTED",
                "submitted_by": "submitter",
                "reviewed_by": "reviewer",
                "reviewed_at": "2026-08-21 16:00:00",
                "evidence_hash": "f" * 64,
                "created_at": "2026-08-21 15:01:00",
                "artifact_json_bytes": self.storage_bytes,
                "artifact_storage_sha256": self.storage_hash,
                "artifact_json_valid": 1,
                "artifact_type": "OBJECT",
                "artifact_schema_version": "probiga.strategy-validation-artifact.v3",
                "artifact_source_dataset_hash": self.artifact["source_dataset_hash"],
                "trades_type": "ARRAY",
                "trade_count": len(self.artifact["trades"]),
                "equity_curve_type": "ARRAY",
                "equity_point_count": len(self.artifact["equity_curve"]),
                "segments_type": "ARRAY",
                "segment_count": len(segments),
                "segment_training_observation_count": sum(
                    len(item["train_dataset"]) for item in segments
                ),
            }])
        if "SELECT COUNT(*) FROM st_strategy_version" in sql:
            return _FakeResult(scalar=1)
        if "declared_section_hash" in sql:
            segment = self.artifact["segments"][
                int(params["segment_path"].split("[")[1].split("]")[0])
            ]
            return _FakeResult(rows=[{
                "section_count": len(segment["train_dataset"]),
                "declared_section_hash": segment["train_dataset_hash"],
            }])
        offset = params.get("offset", 0)
        page_end = params.get("page_end", 0)
        if "'$.trades[*]'" in sql:
            source = self.artifact["trades"]
        elif "'$.equity_curve[*]'" in sql:
            source = self.artifact["equity_curve"]
        elif "'$.segments[*]'" in sql:
            source = [{
                **{key: value for key, value in item.items()
                   if key != "train_dataset"},
                "segment_index": index,
                "train_dataset_count": len(item["train_dataset"]),
            } for index, item in enumerate(self.artifact["segments"], 1)]
        elif "'.train_dataset')" in sql:
            index = int(params["segment_path"].split("[")[1].split("]")[0])
            source = self.artifact["segments"][index]["train_dataset"]
        else:
            raise AssertionError(sql)
        rows = []
        for ordinal, item in enumerate(source[offset:page_end], offset + 1):
            projected = dict(item)
            projected["item_ordinal"] = ordinal
            rows.append(projected)
        return _FakeResult(rows=rows)


class _ArtifactEngine:
    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return self.connection


def test_metric_artifact_summary_never_inlines_large_arrays():
    artifact = _artifact()
    summary = governance._metric_artifact_summary(
        artifact,
        evidence_id="e" * 32,
        artifact_hash=governance._digest(artifact),
    )

    assert summary["trade_count"] == 250
    assert summary["equity_point_count"] == 20
    assert summary["segment_count"] == 3
    assert summary["segment_training_observation_count"] == 375
    assert summary["inline_arrays"] is False
    assert summary["source_authority"] == "CLIENT_DECLARED_UNATTESTED"
    assert summary["review_scope"] == "STRUCTURE_AND_REPRODUCIBILITY_ONLY"
    assert summary["funding_authority"] is False
    assert "trades" not in summary
    assert "equity_curve" not in summary
    assert len(json.dumps(summary).encode("utf-8")) < 4096


def test_metric_artifact_pages_are_bounded_and_cursor_is_identity_bound(
    monkeypatch,
):
    artifact = _artifact()
    connection = _ArtifactConnection(artifact)
    monkeypatch.setattr(
        governance, "get_engine", lambda: _ArtifactEngine(connection),
    )

    first = governance.metric_evidence_artifact_page(
        "e" * 32, section="trades", limit=100,
    )
    second = governance.metric_evidence_artifact_page(
        "e" * 32,
        section="trades",
        cursor=first["next_cursor"],
        limit=100,
    )

    assert first["total_count"] == 250
    assert len(first["items"]) == 100
    assert first["offset"] == 0
    assert second["offset"] == 100
    assert len(second["items"]) == 100
    assert len(json.dumps(first).encode("utf-8")) < 128 * 1024
    assert first["automatic_real_order_submission"] is False
    assert first["real_order_authority"] is False
    assert first["source_authority"] == "CLIENT_DECLARED_UNATTESTED"
    assert first["review_scope"] == "STRUCTURE_AND_REPRODUCIBILITY_ONLY"
    assert first["funding_authority"] is False
    tampered_cursor = (
        first["next_cursor"][:-1]
        + ("0" if first["next_cursor"][-1] != "0" else "1")
    )
    with pytest.raises(ValueError, match="规范证据身份"):
        governance.metric_evidence_artifact_page(
            "e" * 32,
            section="trades",
            cursor=tampered_cursor,
            limit=100,
        )
    with pytest.raises(ValueError, match="规范证据身份"):
        governance.metric_evidence_artifact_page(
            "e" * 32,
            section="trades",
            cursor=first["next_cursor"],
            limit=50,
        )


def test_metric_segment_page_does_not_inline_training_set(monkeypatch):
    artifact = _artifact()
    connection = _ArtifactConnection(artifact)
    monkeypatch.setattr(
        governance, "get_engine", lambda: _ArtifactEngine(connection),
    )

    segments = governance.metric_evidence_artifact_page(
        "e" * 32, section="segments", limit=100,
    )
    training = governance.metric_evidence_artifact_page(
        "e" * 32,
        section="segment_train_dataset",
        segment_index=1,
        limit=100,
    )

    assert len(segments["items"]) == 3
    assert all("train_dataset" not in row for row in segments["items"])
    assert segments["items"][0]["train_dataset_count"] == 125
    assert segments["items"][0]["train_dataset_inline"] is False
    assert training["total_count"] == 125
    assert len(training["items"]) == 100


def test_metric_pages_never_select_or_materialize_artifact_json(monkeypatch):
    artifact = _artifact()
    connection = _ArtifactConnection(
        artifact, storage_bytes=governance.METRIC_ARTIFACT_MAX_BYTES,
    )
    monkeypatch.setattr(
        governance, "get_engine", lambda: _ArtifactEngine(connection),
    )

    page = governance.metric_evidence_artifact_page(
        "e" * 32, section="trades", limit=1,
    )

    assert len(page["items"]) == 1
    assert len(json.dumps(page).encode("utf-8")) < 16 * 1024
    metadata_sql, projection_sql = connection.queries[0][0], connection.queries[2][0]
    assert "SELECT *" not in metadata_sql.upper()
    assert " artifact_json AS artifact_json" not in metadata_sql
    assert "OCTET_LENGTH(artifact_json)" in metadata_sql
    assert "LOWER(SHA2(artifact_json, 256))" in metadata_sql
    assert "JSON_TABLE" in projection_sql
    assert ":page_end" in projection_sql


def test_metric_compact_detail_rejects_database_sha_drift(monkeypatch):
    artifact = _artifact()
    connection = _ArtifactConnection(artifact, storage_hash="0" * 64)
    monkeypatch.setattr(
        governance, "get_engine", lambda: _ArtifactEngine(connection),
    )
    monkeypatch.setattr(governance, "_table_exists", lambda _name: True)

    with pytest.raises(ValueError, match="数据库身份"):
        governance.metric_evidence_detail("e" * 32)


@pytest.mark.parametrize(
    "unsafe",
    [
        {"automatic_real_order_submission": False},
        {
            "automatic_real_order_submission": True,
            "real_order_authority": False,
        },
        {
            "automatic_real_order_submission": False,
            "real_order_authority": "false",
        },
    ],
)
def test_metric_detail_api_does_not_mask_authority(monkeypatch, unsafe):
    monkeypatch.setattr(
        router, "metric_evidence_detail", lambda _evidence_id: dict(unsafe),
    )

    response = router.strategy_center_metric_evidence_detail("e" * 32)

    assert response.status_code == 500
    body = json.loads(response.body.decode("utf-8"))
    assert body["error"] == "invalid_metric_evidence_authority_contract"
    assert body["automatic_real_order_submission"] is False
    assert body["real_order_authority"] is False


def test_metric_artifact_page_api_preserves_bounded_contract(monkeypatch):
    monkeypatch.setattr(
        router,
        "metric_evidence_artifact_page",
        lambda *_args, **_kwargs: {
            "schema": "probiga.strategy-metric-artifact-page.v1",
            "items": [],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )

    response = router.strategy_center_metric_evidence_artifact_page(
        "e" * 32, "trades", "", 50, None,
    )

    assert response["status"] == "ok"
    assert response["automatic_real_order_submission"] is False
    assert response["real_order_authority"] is False

import json
import logging
import re

from server.api.routers import trading_v2


_PRIVATE_ERROR = (
    "mysql+pymysql://admin:" + "super-secret@private-db.internal/PROBIGA"
)


class _FailingRepository:
    def latest_snapshot(self):
        return {}

    def intraday_summary(self, **_kwargs):
        raise RuntimeError(_PRIVATE_ERROR)

    def etf_forward_summary(self, _limit):
        raise RuntimeError(_PRIVATE_ERROR)

    def operations_summary(self):
        raise RuntimeError(_PRIVATE_ERROR)

    def execution_capability(self, _capability_key):
        return {"status": "UNAVAILABLE"}


def _identity_envelope(data, **_kwargs):
    return data


def test_degraded_v2_reads_do_not_return_private_exception_text(monkeypatch, caplog):
    repository = _FailingRepository()
    monkeypatch.setattr(trading_v2, "_repo", lambda: repository)
    monkeypatch.setattr(trading_v2, "_envelope", _identity_envelope)

    with caplog.at_level(logging.ERROR, logger="server.api.routers.trading_v2"):
        responses = [
            trading_v2.intraday_decisions("paper-main-v2", 1),
            trading_v2.etf_forward(1),
            trading_v2.system_operations(),
        ]

    serialized = json.dumps(responses, ensure_ascii=False)
    assert _PRIVATE_ERROR not in serialized
    assert "super-secret" not in serialized
    assert {row["error_code"] for row in responses} == {
        "intraday_summary_unavailable",
        "etf_forward_unavailable",
        "operations_unavailable",
    }
    assert all(row["error"] == "数据暂不可用，请稍后重试" for row in responses)
    assert all(re.fullmatch(r"[0-9a-f]{32}", row["incident_id"]) for row in responses)
    assert "exception_type=RuntimeError" in caplog.text
    assert "intraday_summary" in caplog.text
    assert "etf_forward" in caplog.text
    assert "operations" in caplog.text
    assert "super-secret" not in caplog.text
    assert "private-db.internal" not in caplog.text


def test_data_evidence_component_errors_are_stable_codes(monkeypatch):
    repository = _FailingRepository()
    monkeypatch.setattr(trading_v2, "_repo", lambda: repository)
    monkeypatch.setattr(trading_v2, "_envelope", _identity_envelope)
    monkeypatch.setattr(
        trading_v2,
        "load_qmt_kline_attestation_status",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError(_PRIVATE_ERROR)),
    )
    monkeypatch.setattr(
        trading_v2,
        "load_membership_snapshot_history",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError(_PRIVATE_ERROR)),
    )

    result = trading_v2.data_evidence()

    serialized = json.dumps(result, ensure_ascii=False)
    assert _PRIVATE_ERROR not in serialized
    assert "super-secret" not in serialized
    assert result["errors"] == [
        "qmt_attestation_unavailable",
        "concept_membership_unavailable",
        "industry_membership_unavailable",
    ]
    assert result["membership_and_kline_history_ready"] is False
    assert result["all_historical_data_ready"] is False

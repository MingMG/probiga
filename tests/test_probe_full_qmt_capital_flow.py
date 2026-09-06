from __future__ import annotations

import json

import pytest

from tools import probe_full_qmt_capital_flow as probe


CODE = "000001.SZ"
TRADE_DATE = "2026-09-04"


def _rows():
    return [{field: str(index + 1) for index, field in enumerate(probe.FLOW_FIELDS)}]


class FakeTransport:
    instances = []

    def __init__(self, root):
        self.root = root
        self.request = None
        self.activated = None
        self.archived = None
        self.rows = _rows()
        self.__class__.instances.append(self)

    def prepare(self, request):
        self.request = request

    def recover(self):
        return {"active": None}

    def activate(self, request_id):
        self.activated = request_id

    def wait_result(self, request_id, timeout):
        assert request_id == self.request["request_id"]
        return {
            "request": self.request,
            "received_at": "2026-09-04T16:00:00+08:00",
            "source_method": "ContextInfo.get_market_data_ex",
            "outcomes": {CODE: {"status": "data", "rows": self.rows}},
        }

    def archive(self, request_id):
        self.archived = request_id


def _install_fakes(monkeypatch, tmp_path):
    FakeTransport.instances.clear()
    config = type("FakeConfig", (), {"state_dir": tmp_path})()
    monkeypatch.setattr(probe.Config, "load", lambda _path: config)
    monkeypatch.setattr(probe, "QmtTransport", FakeTransport)


def test_probe_sends_one_exact_daily_flow_request_and_archives_success(monkeypatch, tmp_path):
    _install_fakes(monkeypatch, tmp_path)

    result = probe.run_probe("config.json", CODE, TRADE_DATE, 12.5)

    transport = FakeTransport.instances[0]
    request = transport.request
    assert transport.root == str(tmp_path / "qmt")
    assert request["dataset"] == "capital_flow_daily"
    assert request["source"] == "guojin_qmt"
    assert request["codes"] == [CODE]
    assert request["start_date"] == request["end_date"] == TRADE_DATE
    assert request["period"] == "transactioncount1d"
    assert request["adjustment"] == "none"
    assert transport.activated == transport.archived == request["request_id"]
    assert result == {
        "status": "ok",
        "source_method": "ContextInfo.get_market_data_ex",
        "field_names": list(probe.FLOW_FIELDS),
        "row_count": 1,
    }


@pytest.mark.parametrize("bad_row", [
    {field: "1" for field in probe.FLOW_FIELDS if field != "offSmallAmount"},
    dict({field: "1" for field in probe.FLOW_FIELDS}, bidMostAmount="NaN"),
])
def test_probe_rejects_invalid_fields_and_releases_completed_request(
    monkeypatch, tmp_path, bad_row
):
    _install_fakes(monkeypatch, tmp_path)
    original_wait = FakeTransport.wait_result

    def wait_result(self, request_id, timeout):
        self.rows = [bad_row]
        return original_wait(self, request_id, timeout)

    monkeypatch.setattr(FakeTransport, "wait_result", wait_result)

    with pytest.raises(ValueError):
        probe.run_probe("config.json", CODE, TRADE_DATE, 10)

    transport = FakeTransport.instances[0]
    assert transport.archived == transport.request["request_id"]


def test_cli_failure_has_only_safe_summary_fields(monkeypatch, capsys):
    def fail(*_args, **_kwargs):
        raise RuntimeError("credential=must-not-be-printed market-value=123")

    monkeypatch.setattr(probe, "run_probe", fail)
    exit_code = probe.main([
        "--config", "secret-config.json",
        "--qualified-code", CODE,
        "--trade-date", TRADE_DATE,
        "--timeout", "10",
    ])

    assert exit_code == 1
    output = json.loads(capsys.readouterr().out)
    assert output == {
        "status": "error",
        "source_method": "",
        "field_names": [],
        "row_count": 0,
    }


def test_probe_does_not_prepare_behind_an_active_acquisition(monkeypatch, tmp_path):
    _install_fakes(monkeypatch, tmp_path)
    monkeypatch.setattr(FakeTransport, "recover", lambda _self: {"active": {"request_id": "daily"}})

    with pytest.raises(RuntimeError, match="another QMT request"):
        probe.run_probe("config.json", CODE, TRADE_DATE, 10)

    assert FakeTransport.instances[0].request is None


def test_probe_retains_active_request_when_wait_times_out(monkeypatch, tmp_path):
    _install_fakes(monkeypatch, tmp_path)

    def wait_result(_self, _request_id, timeout):
        del timeout
        raise TimeoutError

    monkeypatch.setattr(FakeTransport, "wait_result", wait_result)

    with pytest.raises(TimeoutError):
        probe.run_probe("config.json", CODE, TRADE_DATE, 10)

    transport = FakeTransport.instances[0]
    assert transport.activated == transport.request["request_id"]
    assert transport.archived is None

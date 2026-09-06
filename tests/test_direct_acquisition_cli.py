"""Offline CLI dispatch and installer contract; never register a Windows task."""
import json
from pathlib import Path

import pytest

from acquisition import __main__ as cli


def configuration(tmp_path, *, write_enabled=True):
    path = tmp_path / "explicit-config.json"
    path.write_text(json.dumps({
        "start_date": "2026-09-01", "state_dir": str(tmp_path / "private-state"),
        "write_enabled": write_enabled,
        "datasets": ["stock_daily", "etf_daily", "stock_current", "reference"],
    }), encoding="utf-8")
    return str(path)


@pytest.fixture
def fake_runner(monkeypatch):
    calls = []
    class FakeRunner:
        def __init__(self, config):
            calls.append(("init",))
        def run(self, datasets, **kwargs):
            calls.append(("run", datasets, kwargs))
            return {"status": "run_finished"}
        def status(self, datasets, **kwargs):
            calls.append(("status", datasets, kwargs))
            return {"status": "complete"}
        def live_once(self):
            calls.append(("live",))
            return {"stock_current": {"complete": 1}}
        def reference(self, asset, codes, **kwargs):
            calls.append(("reference", asset, codes, kwargs))
            return {"status": "complete"}
        def close(self):
            calls.append(("close",))
    monkeypatch.setattr(cli, "Runner", FakeRunner)
    return calls


def test_daily_only_due_enabled_non_live_products(tmp_path, fake_runner):
    assert cli.main(["--config", configuration(tmp_path), "daily"]) == 0
    assert fake_runner[1] == ("run", ["stock_daily", "etf_daily"],
                              {"requested": "latest", "budget_seconds": 1200, "due": True})
    assert fake_runner[-1] == ("close",)


def test_backfill_explicit_dates_does_not_enable_due_mode(tmp_path, fake_runner):
    args = ["--config", configuration(tmp_path), "backfill", "--start", "2026-09-01",
            "--end", "2026-09-04", "--datasets", "stock_daily", "--budget-seconds", "60"]
    assert cli.main(args) == 0
    assert fake_runner[1] == ("run", ["stock_daily"],
                              {"start": "2026-09-01", "end": "2026-09-04", "budget_seconds": 60})


def test_status_is_read_only_with_disabled_writes(tmp_path, fake_runner):
    path = configuration(tmp_path, write_enabled=False)
    assert cli.main(["--config", path, "status", "--datasets", "stock_daily", "--json"]) == 0
    assert fake_runner[1][0] == "status"
    assert not (tmp_path / "private-state").exists()


@pytest.mark.parametrize("command", [["daily"], ["live", "--once"],
                                      ["reference", "--asset-class", "stock", "--codes", "000001.SZ"]])
def test_disabled_writes_do_not_construct_or_invoke_writer(tmp_path, fake_runner, command):
    assert cli.main(["--config", configuration(tmp_path, write_enabled=False), *command]) == 2
    assert not fake_runner


def test_reference_preserves_explicit_scope(tmp_path, fake_runner):
    assert cli.main(["--config", configuration(tmp_path), "reference", "--asset-class", "index",
                     "--codes", "000001.SH,980001.SZ", "--period", "instrument", "--target", "2026-09-04"]) == 0
    assert fake_runner[1] == ("reference", "index", ["000001.SH", "980001.SZ"],
                              {"period": "instrument", "target": "2026-09-04"})


def test_live_loop_is_bounded_and_closes_runner(tmp_path, fake_runner, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(cli.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    assert cli.main(["--config", configuration(tmp_path), "live", "--duration-seconds", "31"]) == 0
    assert sum(call[0] == "live" for call in fake_runner) == 3
    assert clock[0] == 31 and fake_runner[-1] == ("close",)


@pytest.mark.parametrize("command", [["daily", "--datasets", "index_daily"],
                                      ["daily", "--datasets", "stock_current"],
                                      ["live", "--duration-seconds", "301"]])
def test_cli_does_not_expand_enabled_scope_or_loop_budget(tmp_path, fake_runner, command):
    assert cli.main(["--config", configuration(tmp_path), *command]) == 2
    assert not any(call[0] in {"run", "live"} for call in fake_runner)


def test_config_is_required_and_help_does_not_read_environment(fake_runner):
    with pytest.raises(SystemExit) as missing:
        cli.main(["daily"])
    assert missing.value.code == 2
    assert not fake_runner


def test_installer_uses_two_hidden_nonoverlapping_interactive_tasks():
    source = (Path(__file__).resolve().parents[1] / "tools" / "register_direct_acquisition.ps1").read_text(encoding="utf-8")
    assert "-MultipleInstances IgnoreNew" in source
    assert "-WindowStyle Hidden" in source
    assert "-RepetitionInterval (New-TimeSpan -Minutes 5)" in source
    assert "live --duration-seconds 295" in source
    assert "-AtLogOn -User $Identity" in source
    assert "-LogonType Interactive" in source
    assert "-StartWhenAvailable" in source
    assert "write_enabled" in source
    for forbidden in ("Start-ScheduledTask", "Start-Service", "New-Service", "Stop-Process", "Start-Process", "XtItClient.exe"):
        assert forbidden not in source

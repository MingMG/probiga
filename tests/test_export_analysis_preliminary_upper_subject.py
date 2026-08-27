from __future__ import annotations

import json

from tools import export_analysis_preliminary_upper_subject as command


def test_cli_exports_exact_read_only_preliminary_receipt(
    monkeypatch, tmp_path, capsys,
) -> None:
    engine = object()
    receipt = {
        "trade_date": "2026-08-21",
        "decision_at": "2099-08-27T18:50:00",
        "receipt_sha256": "a" * 64,
        "ordered_candidate_sha256": "b" * 64,
        "code_set_sha256": "c" * 64,
    }
    observed = {}
    monkeypatch.setattr(command, "load_project_env", lambda: None)
    monkeypatch.setattr(command, "create_tool_engine", lambda: engine)
    monkeypatch.setattr(
        command,
        "authoritative_closed_trade_date",
        lambda *_args, **_kwargs: "2026-08-21",
    )
    monkeypatch.setattr(command, "resolve_build_sha", lambda _value: "d" * 40)

    def prepare(actual_engine, **kwargs):
        observed["engine"] = actual_engine
        observed.update(kwargs)
        return receipt

    monkeypatch.setattr(
        command,
        "prepare_preliminary_upper_subject_receipt",
        prepare,
    )
    output = tmp_path / "preliminary.json"

    assert command.main([
        "--target-date", "2026-08-21",
        "--decision-at", "2099-08-27T18:50:00",
        "--expected-build-sha", "d" * 40,
        "--output", str(output),
    ]) == 0

    assert observed == {
        "engine": engine,
        "trade_date": "2026-08-21",
        "decision_at": command._decision_at("2099-08-27T18:50:00"),
        "build_sha": "d" * 40,
        "min_score": 62.0,
    }
    assert json.loads(output.read_text(encoding="utf-8")) == receipt
    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == "COMPLETED"
    assert summary["receipt_sha256"] == "a" * 64

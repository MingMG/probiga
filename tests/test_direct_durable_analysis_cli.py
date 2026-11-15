"""The supported direct CLI must emit prepared scores for scheduler publication."""
import json
import sys
from types import SimpleNamespace

import pytest

from biz.analysis import sync_analysis_fast as analysis
from server.common.formal_score_publication import FORMAL_SCORE_WORK_SCHEMA
from server.common.scheduler_validation import _single_nested_machine_payload


@pytest.mark.parametrize("has_reference", [True, False])
def test_direct_production_initial_leaves_publication_to_scheduler(monkeypatch, capsys, has_reference):
    target = "2026-11-13"
    run_uid, build_sha = "1" * 32, "2" * 40
    cutoff = "2026-11-15T17:02:44"
    reference = {
        "schema": FORMAL_SCORE_WORK_SCHEMA, "run_uid": run_uid, "build_sha": build_sha,
        "trade_date": target, "decision_at": cutoff, "analysis_count": 5571,
        "recommendation_count": 30, "executable_count": 0,
    }
    bind_events, terminal_writes, disposed = [], [], []
    engine = SimpleNamespace(dispose=lambda: disposed.append(True))
    monkeypatch.setattr(analysis, "create_batch_engine", lambda: engine)
    monkeypatch.setattr(analysis, "_prebind_direct_publication_history", lambda _, **kw: bind_events.append(kw))
    monkeypatch.setattr(analysis, "_finish_direct_publication_history", lambda _, **kw: terminal_writes.append(kw))

    def prepared_batch(**kwargs):
        assert kwargs["publication_run_uid"] == run_uid
        assert kwargs["pool_decision_phase"] == "INITIAL"
        assert kwargs["execution_time"] == cutoff
        return analysis.BatchStats(
            target, 5571, 30, 53.0, target, "", prepared_publication=reference if has_reference else None,
        )

    monkeypatch.setattr(analysis, "run_batch", prepared_batch)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_SCHEDULER_HISTORY_RUN_UID", run_uid)
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_TYPE", "analysis_fast")
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", build_sha)
    monkeypatch.setattr(sys, "argv", [
        "sync_analysis_fast.py", "--date", target, "--pool-decision-phase", "INITIAL", "--execution-time", cutoff,
    ])

    assert analysis.main() == (0 if has_reference else 2)
    output = capsys.readouterr().out
    assert len(bind_events) == 1 and bind_events[0]["run_uid"] == run_uid
    assert disposed == [True]
    if has_reference:
        assert _single_nested_machine_payload(output, schema=FORMAL_SCORE_WORK_SCHEMA) == reference
        assert terminal_writes == []
    else:
        assert json.loads(output)["retryable"] is False
        assert json.loads(output)["reason_code"] == "FORMAL_SCORE_CONTRACT_INVALID"
        assert len(terminal_writes) == 1 and terminal_writes[0]["success"] is False

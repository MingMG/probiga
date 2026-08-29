from __future__ import annotations

from datetime import datetime, timedelta
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from server.api import scheduler_runtime
from server.common import release_data_readiness_contract as readiness
from server.common import scheduler_validation
from server.common.scheduler_args import build_scheduler_task_args
from tools import crawl_realtime_batch as flow


TARGET = "2026-08-26"


def _timestamp(day: str, hour: int = 15) -> int:
    parsed = datetime.fromisoformat(f"{day}T{hour:02d}:00:00").replace(
        tzinfo=flow.SHANGHAI
    )
    return int(parsed.timestamp())


def _item(day: str, code: str = "600000") -> dict:
    return {
        "f12": code,
        "f14": "test",
        "f62": 100,
        "f66": 40,
        "f72": 30,
        "f78": 20,
        "f84": 10,
        "f124": _timestamp(day),
    }


def _receipt(day: str = TARGET, *, generated_at: str | None = None) -> dict:
    return flow._signed_receipt(
        {
            "schema": flow.CAPITAL_FLOW_RESULT_SCHEMA,
            "status": "PASS",
            "task_type": flow.CAPITAL_FLOW_TASK_TYPE,
            "dataset": flow.CAPITAL_FLOW_DATASET,
            "trade_date": day,
            "source_trade_date": day,
            "source_timestamp_required": True,
            "row_count": 5000,
            "elapsed_seconds": 1.0,
            "generated_at": generated_at or f"{day}T15:21:00",
        }
    )


def test_release_capital_flow_is_closed_target_and_analysis_prerequisite():
    assert (
        "capital_flow_batch_fast"
        in readiness.RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES
    )
    for task_type in ("analysis_fast", "analysis_morning_strict"):
        assert (
            "capital_flow_batch_fast"
            in readiness.RELEASE_DATA_CATCHUP_DEPENDENCIES[task_type]
        )


def test_release_capital_flow_args_bind_exact_closed_date_without_changing_cron():
    row = {
        "task_type": "capital_flow_batch_fast",
        "script_args": "--only flow --min-coverage 0.70 --json",
        "date_param": "",
    }
    assert build_scheduler_task_args(
        row,
        "tools/crawl_realtime_batch.py",
        "2026-08-27",
    ) == ["--only", "flow", "--min-coverage", "0.70", "--json"]
    assert build_scheduler_task_args(
        {**row, "_trigger_source": "release_catchup"},
        "tools/crawl_realtime_batch.py",
        TARGET,
    )[-2:] == ["--trade-date", TARGET]

    with pytest.raises(ValueError, match="authoritative target"):
        build_scheduler_task_args(
            {
                **row,
                "script_args": (
                    "--only flow --min-coverage 0.70 --json "
                    "--trade-date 2026-08-25"
                ),
                "_trigger_source": "release_catchup",
            },
            "tools/crawl_realtime_batch.py",
            TARGET,
        )


def test_release_capital_flow_target_rolls_over_at_1800():
    row = {"task_type": "capital_flow_batch_fast"}
    with patch(
        "server.api.scheduler_runtime._release_catchup_closed_target_date",
        return_value=TARGET,
    ):
        assert scheduler_runtime._attach_release_catchup_expected_targets(
            object(),
            [row],
            now=datetime(2026, 8, 27, 17, 59),
        )
    assert row["_release_expected_target_date"] == TARGET

    with patch(
        "server.api.scheduler_runtime._release_catchup_closed_target_date",
        return_value="2026-08-27",
    ):
        assert scheduler_runtime._attach_release_catchup_expected_targets(
            object(),
            [row],
            now=datetime(2026, 8, 27, 18, 0),
        )
    assert row["_release_expected_target_date"] == "2026-08-27"


def test_ordinary_1520_then_release_retry_never_backlabels_and_converges(
    monkeypatch,
):
    build_sha = "c" * 40
    row = {
        "id": 884,
        "task_type": "capital_flow_batch_fast",
        "_release_history_available": True,
        "_release_catchup_authorized": True,
        "_release_expected_target_required": True,
        "_release_expected_target_available": True,
        "_release_expected_target_date": TARGET,
        # A successful ordinary 15:20 run deliberately has no hash-bound
        # release target and therefore cannot satisfy release readiness.
        "_release_terminal_status": "success",
        "_release_terminal_build_sha": build_sha,
        "_release_terminal_exit_code": 0,
        "_release_terminal_output": "ordinary cron success",
    }
    monkeypatch.setattr(
        scheduler_runtime,
        "_scheduler_build_commit_sha",
        lambda: build_sha,
    )
    assert scheduler_runtime._release_build_catchup_allowed(
        row,
        now=datetime(2026, 8, 27, 15, 21),
    )

    replace = MagicMock()
    monkeypatch.setattr(flow, "_replace_table_rows_flow_partition_exact", replace)
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda *_args, **_kwargs: [_item("2026-08-27")],
    )
    with pytest.raises(RuntimeError, match="newer than target"):
        flow.refresh_flow(
            object(),
            trade_date=TARGET,
            min_coverage=0.7,
            require_source_date=True,
        )
    replace.assert_not_called()

    # A failed pre-rollover attempt retains bounded retry backoff.  Once that
    # backoff expires after the 18:00 target rollover, today's exact snapshot
    # is eligible and the same source-date guard admits it.
    row.update(
        {
            "_release_expected_target_date": "2026-08-27",
            "_release_terminal_status": "failed",
            "_release_terminal_finished_at": datetime(2026, 8, 27, 17, 59),
            "_release_terminal_exit_code": 1,
        }
    )
    assert not scheduler_runtime._release_build_catchup_allowed(
        row,
        now=datetime(2026, 8, 27, 18, 0),
    )
    retry_at = datetime(2026, 8, 27, 17, 59) + timedelta(
        minutes=scheduler_runtime.RELEASE_CATCHUP_RETRY_INTERVAL_MINUTES
    )
    assert scheduler_runtime._release_build_catchup_allowed(row, now=retry_at)
    assert flow._exact_flow_source_items(
        [_item("2026-08-27")],
        trade_date="2026-08-27",
    ) == [_item("2026-08-27")]


def test_exact_source_date_guard_runs_before_any_capital_flow_write(monkeypatch):
    replace = MagicMock()
    monkeypatch.setattr(flow, "_replace_table_rows_flow_partition_exact", replace)
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "_latest_stock_universe_count", lambda _engine: 1)
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda *_args, **_kwargs: [_item("2026-08-27")],
    )

    with pytest.raises(RuntimeError, match="newer than target"):
        flow.refresh_flow(
            object(),
            trade_date=TARGET,
            min_coverage=0.7,
            require_source_date=True,
        )
    replace.assert_not_called()


def test_exact_source_date_guard_drops_old_rows_and_publishes_only_target(
    monkeypatch,
):
    published = []

    def replace(engine, frame, *, trade_date, expected_codes):
        published.append((engine, frame.copy(), trade_date, expected_codes))
        return len(frame)

    monkeypatch.setattr(flow, "_replace_table_rows_flow_partition_exact", replace)
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda _engine, _day: {"600000"},
    )
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda *_args, **_kwargs: [
            _item("2026-08-25", "000001"),
            _item(TARGET),
        ],
    )

    assert flow.refresh_flow(
        object(),
        trade_date=TARGET,
        min_coverage=0.7,
        require_source_date=True,
    ) == 1
    frame = published[0][1]
    assert frame["stock_code"].tolist() == ["600000"]
    assert frame["trade_date"].tolist() == [TARGET]
    assert frame["data_source"].tolist() == ["east_push2delay"]
    assert published[0][2:] == (TARGET, {"600000"})


def test_exact_flow_includes_new_bse_selector_and_keeps_real_provider_sources(
    monkeypatch,
):
    assert "m:0+t:81+s:2048" in flow.CAPITAL_FLOW_MARKETS
    captured = []
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(
        flow,
        "fetch_batch",
        lambda fs, fields, **kwargs: (
            captured.append((fs, fields, kwargs))
            or [_item(TARGET, "600000")]
        ),
    )
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: {"600000", "920001"},
    )
    monkeypatch.setattr(
        flow,
        "_fetch_missing_flow_rows",
        lambda codes, *, trade_date: pd.DataFrame(
            [
                {
                    "stock_code": "920001",
                    "trade_date": trade_date,
                    "main_net_inflow": 1,
                    "max_net_inflow": 2,
                    "lg_net_inflow": 3,
                    "mid_net_inflow": 4,
                    "sm_net_inflow": 5,
                    "data_source": "push2his",
                }
            ]
        )
        if codes == {"920001"}
        else pytest.fail(f"unexpected fallback codes: {codes}"),
    )
    published = []
    monkeypatch.setattr(
        flow,
        "_replace_table_rows_flow_partition_exact",
        lambda engine, frame, **kwargs: published.append(frame.copy()) or len(frame),
    )

    assert flow.refresh_flow(
        object(),
        trade_date=TARGET,
        require_source_date=True,
    ) == 2
    assert captured[0][2] == {"fid": "f12", "po": "0"}
    frame = published[0].set_index("stock_code")
    assert frame.loc["600000", "data_source"] == "east_push2delay"
    assert frame.loc["920001", "data_source"] == "push2his"


def test_unresolved_target_code_blocks_before_any_flow_publication(monkeypatch):
    monkeypatch.setattr(flow, "_require_open_trade_date", lambda *_args: None)
    monkeypatch.setattr(flow, "fetch_batch", lambda *_args, **_kwargs: [_item(TARGET)])
    monkeypatch.setattr(
        flow,
        "_read_target_traded_flow_codes",
        lambda *_args: {"600000", "920001"},
    )
    monkeypatch.setattr(
        flow,
        "_fetch_missing_flow_rows",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    publish = MagicMock()
    monkeypatch.setattr(
        flow,
        "_replace_table_rows_flow_partition_exact",
        publish,
    )

    with pytest.raises(RuntimeError, match="missing_count=1"):
        flow.refresh_flow(
            object(),
            trade_date=TARGET,
            require_source_date=True,
        )
    publish.assert_not_called()


def test_fallback_row_rejects_unproven_ths_date_and_missing_components():
    base = {
        "stock_code": "920001",
        "trade_date": TARGET,
        "main_net_inflow": 1,
        "max_net_inflow": 2,
        "lg_net_inflow": 3,
        "mid_net_inflow": 4,
        "sm_net_inflow": 5,
        "data_source": "push2his",
    }
    assert flow._validated_fallback_row(
        pd.DataFrame([base]),
        stock_code="920001",
        trade_date=TARGET,
    )["data_source"] == "push2his"
    assert flow._validated_fallback_row(
        pd.DataFrame([{**base, "data_source": "baidu"}]),
        stock_code="920001",
        trade_date=TARGET,
    ) is None
    assert flow._validated_fallback_row(
        pd.DataFrame([{**base, "data_source": "ths"}]),
        stock_code="920001",
        trade_date=TARGET,
    ) is None
    assert flow._validated_fallback_row(
        pd.DataFrame([{**base, "trade_date": "2026-08-25"}]),
        stock_code="920001",
        trade_date=TARGET,
    ) is None
    assert flow._validated_fallback_row(
        pd.DataFrame([{**base, "max_net_inflow": "--"}]),
        stock_code="920001",
        trade_date=TARGET,
    ) is None


def test_missing_code_fallback_uses_only_strict_source_identity_parser(
    monkeypatch,
):
    calls = []

    def fetch_one(code, trade_date):
        calls.append((code, trade_date))
        return {
            "stock_code": code,
            "trade_date": trade_date,
            "main_net_inflow": 1,
            "max_net_inflow": 2,
            "lg_net_inflow": 3,
            "mid_net_inflow": 4,
            "sm_net_inflow": 5,
            "data_source": "push2his",
        }

    monkeypatch.setattr(flow, "_fetch_exact_push2his_flow_row", fetch_one)
    result = flow._fetch_missing_flow_rows(
        {"430001", "830001", "920001"},
        trade_date=TARGET,
    )

    assert set(calls) == {
        ("430001", TARGET),
        ("830001", TARGET),
        ("920001", TARGET),
    }
    assert set(result["stock_code"]) == {"430001", "830001", "920001"}
    assert set(result["trade_date"]) == {TARGET}
    assert set(result["data_source"]) == {"push2his"}


class _FlowResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _FlowClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _FlowResponse(self.payload)


def _push2his_payload(
    *,
    code: str = "920001",
    market: int = 0,
    line: str = f"{TARGET},1,2,3,4,5,6,7,8,9,10",
):
    return {
        "rc": 0,
        "data": {
            "code": code,
            "market": market,
            "klines": [line],
        },
    }


def test_strict_push2his_fallback_binds_source_code_market_date_and_fields():
    client = _FlowClient(_push2his_payload())

    row = flow._fetch_exact_push2his_flow_row(
        "920001", TARGET, client=client
    )

    assert row == {
        "stock_code": "920001",
        "trade_date": TARGET,
        "main_net_inflow": 1.0,
        "sm_net_inflow": 2.0,
        "mid_net_inflow": 3.0,
        "lg_net_inflow": 4.0,
        "max_net_inflow": 5.0,
        "data_source": "push2his",
    }
    assert client.calls[0][1]["params"]["secid"] == "0.920001"


@pytest.mark.parametrize(
    "payload",
    (
        _push2his_payload(code="920002"),
        _push2his_payload(market=1),
    ),
)
def test_strict_push2his_fallback_rejects_request_identity_backfill(payload):
    with pytest.raises(RuntimeError, match="response identity differs"):
        flow._fetch_exact_push2his_flow_row(
            "920001", TARGET, client=_FlowClient(payload)
        )


@pytest.mark.parametrize(
    "line",
    (
        f"{TARGET},1,--,3,4,5,6,7,8,9,10",
        f"{TARGET},1,2,3,4",
        f"{TARGET},1,2,NaN,4,5,6,7,8,9,10",
    ),
)
def test_strict_push2his_fallback_never_coerces_missing_components_to_zero(line):
    with pytest.raises((RuntimeError, ValueError)):
        flow._fetch_exact_push2his_flow_row(
            "920001",
            TARGET,
            client=_FlowClient(_push2his_payload(line=line)),
        )


def test_capital_flow_machine_receipt_and_release_target_are_strict(monkeypatch):
    output = json.dumps(
        _receipt(TARGET, generated_at="2026-08-27T03:06:00"),
        sort_keys=True,
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_requirement",
        lambda *_args, **_kwargs: (True, "target table verified"),
    )
    monkeypatch.setattr(
        scheduler_validation,
        "_validate_daily_universe_coverage",
        lambda *_args, **_kwargs: (True, "full universe verified"),
    )
    task = {
        "task_type": "capital_flow_batch_fast",
        "_trigger_source": "release_catchup",
        "_release_target_date": TARGET,
    }

    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=0,
    ) == "success"
    validated = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=object(),
        output=output,
        started_at=datetime(2026, 8, 27, 3, 5),
        now=datetime(2026, 8, 27, 3, 7),
    )
    assert validated.checked and validated.ok

    mismatch = scheduler_validation.validate_scheduler_task_result(
        {**task, "_release_target_date": "2026-08-27"},
        engine=object(),
        output=output,
        started_at=datetime(2026, 8, 27, 18, 0),
        now=datetime(2026, 8, 27, 18, 1),
    )
    assert mismatch.checked and not mismatch.ok
    assert "receipt date differs" in mismatch.message

    tampered = json.loads(output)
    tampered["trade_date"] = "2026-08-27"
    assert scheduler_validation.scheduler_output_status(
        task,
        json.dumps(tampered),
        return_code=0,
    ) == "failed"


def test_flow_only_cli_emits_signed_target_bound_receipt(monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(flow, "create_batch_engine", lambda: object())
    monkeypatch.setattr(flow, "_latest_open_trade_date", lambda _engine: TARGET)
    monkeypatch.setattr(
        flow,
        "refresh_flow",
        lambda *_args, **kwargs: calls.append(kwargs) or 5000,
    )
    monkeypatch.setattr(
        flow.sys,
        "argv",
        ["crawl_realtime_batch.py", "--only", "flow", "--json"],
    )

    assert flow.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == flow.CAPITAL_FLOW_RESULT_SCHEMA
    assert payload["trade_date"] == payload["source_trade_date"] == TARGET
    assert payload["source_timestamp_required"] is True
    assert scheduler_validation.scheduler_output_status(
        {"task_type": "capital_flow_batch_fast"},
        json.dumps(payload),
        return_code=0,
    ) == "success"
    assert calls == [
        {
            "trade_date": TARGET,
            "min_coverage": 0.0,
            "require_source_date": True,
        }
    ]

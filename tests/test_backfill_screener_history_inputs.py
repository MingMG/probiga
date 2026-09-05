from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.sentiment.sync_sentiment import _finalize_a_list_info_df
from tools import backfill_screener_history_inputs as backfill
from tools import fetch_sm_stock_capital_flow_daily as daily_flow
from tools.backfill_screener_history_inputs import (
    flow_components_valid,
    normalize_flow_rows,
    normalize_lhb_daily,
    _json_default,
    _info_exact_key,
)


def test_flow_curl_timeout_kills_process_without_disabling_tls(monkeypatch):
    from tools import crawl_stock_fund_flow as transport
    calls = []
    class Process:
        def communicate(self, timeout=None):
            calls.append(timeout)
            if timeout is not None:
                raise transport.subprocess.TimeoutExpired("curl", timeout)
            return b"", b""
        def kill(self):
            calls.append("killed")
    def popen(command, **kwargs):
        assert "-k" not in command and "--insecure" not in command
        assert command[command.index("--max-time") + 1] == "20"
        assert "push2his.eastmoney.com:443:61.129.129.48" in command
        return Process()
    monkeypatch.setattr(transport.subprocess, "Popen", popen)
    assert transport._fetch_push2his_curl("000001", resolve_ip="61.129.129.48") is None
    assert calls == [25, "killed", None]


def test_baidu_fetch_batches_multiple_dates_for_one_stock(monkeypatch):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "Result": {
                    "content": [
                        {
                            "date": "2026/08/19",
                            "extMainIn": "30",
                            "littleNetIn": "-15",
                            "mediumNetIn": "-15",
                            "largeNetIn": "20",
                            "superNetIn": "10",
                        },
                        {
                            "date": "2026/08/18",
                            "extMainIn": "40",
                            "littleNetIn": "-20",
                            "mediumNetIn": "-20",
                            "largeNetIn": "25",
                            "superNetIn": "15",
                        },
                    ]
                }
            }

    urls = []

    def fake_get(url, **_kwargs):
        urls.append(url)
        return Response()

    monkeypatch.setattr(daily_flow._SESSION, "get", fake_get)

    frame = daily_flow._fetch_baidu_dates(
        "600000", {"2026-08-18", "2026-08-19"}
    )

    assert frame is not None
    assert sorted(frame["trade_date"].tolist()) == ["2026-08-18", "2026-08-19"]
    assert len(urls) == 1
    assert "rn=20" in urls[0]


def test_baidu_backfill_normalizes_all_requested_dates_in_one_call(monkeypatch):
    frame = pd.DataFrame([
        {
            "stock_code": "600000",
            "trade_date": "2026-08-18",
            "main_net_inflow": 40,
            "max_net_inflow": 15,
            "lg_net_inflow": 25,
            "mid_net_inflow": -20,
            "sm_net_inflow": -20,
        },
        {
            "stock_code": "600000",
            "trade_date": "2026-08-19",
            "main_net_inflow": 30,
            "max_net_inflow": 10,
            "lg_net_inflow": 20,
            "mid_net_inflow": -15,
            "sm_net_inflow": -15,
        },
    ])
    calls = []

    def fake_fetch(code, dates):
        calls.append((code, set(dates)))
        return frame

    monkeypatch.setattr(backfill, "_fetch_baidu_dates", fake_fetch)

    code, rows, error = backfill._fetch_flow_code_baidu(
        "600000", {"2026-08-18", "2026-08-19"}
    )

    assert code == "600000"
    assert len(rows) == 2
    assert error == ""
    assert calls == [("600000", {"2026-08-18", "2026-08-19"})]
    assert {row["_data_source"] for row in rows} == {"baidu"}


def test_flow_component_validation_detects_rotated_buckets():
    correct = {
        "main_net_inflow": -64_265_948,
        "max_net_inflow": -51_160_151,
        "lg_net_inflow": -13_105_797,
        "mid_net_inflow": -4_737_456,
        "sm_net_inflow": 69_003_403,
    }
    rotated = dict(correct)
    rotated.update({
        "max_net_inflow": 69_003_403,
        "lg_net_inflow": -4_737_456,
        "mid_net_inflow": -13_105_797,
        "sm_net_inflow": -51_160_151,
    })

    assert flow_components_valid(correct) is True
    assert flow_components_valid(rotated) is False


def test_normalize_flow_rows_keeps_only_requested_valid_pairs():
    rows = [{
        "stock_code": "1",
        "trade_date": "2026-06-09",
        "main_net_inflow": 30,
        "max_net_inflow": 10,
        "lg_net_inflow": 20,
        "mid_net_inflow": -15,
        "sm_net_inflow": -15,
    }, {
        "stock_code": "1",
        "trade_date": "2026-06-10",
        "main_net_inflow": 30,
        "max_net_inflow": 10,
        "lg_net_inflow": 20,
        "mid_net_inflow": -15,
        "sm_net_inflow": -15,
    }]

    result = normalize_flow_rows(rows, {"000001": {"2026-06-09"}})

    assert len(result) == 1
    assert result[0]["stock_code"] == "000001"
    assert result[0]["trade_date"] == "2026-06-09"


def test_lhb_daily_normalization_filters_scope_and_deduplicates_stock_date():
    frame = pd.DataFrame([
        {"trade_date": "2026-08-07", "stock_code": "000001", "reason": "first"},
        {"trade_date": "2026-08-07", "stock_code": "000001", "reason": "last"},
        {"trade_date": "2026-08-07", "stock_code": "900915", "reason": "B share"},
    ])

    result = normalize_lhb_daily(frame)

    assert result[["stock_code", "reason"]].to_dict("records") == [
        {"stock_code": "000001", "reason": "last"},
    ]


def test_lhb_info_finalizer_removes_buy_sell_report_overlap():
    row = {
        "trade_date": "2026-08-07",
        "stock_code": "000603",
        "operate_code": "10634757",
        "operate_name": "深股通专用",
        "a_buy_amount": 100,
        "a_sell_amount": 80,
        "a_net_amount": 20,
        "a_buy_amount_rate": 1,
        "a_sell_amount_rate": 0.8,
        "reason": "日涨幅偏离值",
    }

    result = _finalize_a_list_info_df(pd.DataFrame([row, row]))

    assert len(result) == 1


def test_lhb_info_finalizer_deduplicates_at_database_precision():
    base = {
        "trade_date": "2026-08-07",
        "stock_code": "000603",
        "operate_code": "10634757",
        "operate_name": "深股通专用",
        "a_buy_amount": 100,
        "a_sell_amount": 80,
        "a_net_amount": 20,
        "a_sell_amount_rate": 0.8,
        "reason": "日涨幅偏离值",
    }
    first = dict(base, a_buy_amount_rate=1.00000001)
    second = dict(base, a_buy_amount_rate=1.00000002)

    result = _finalize_a_list_info_df(pd.DataFrame([first, second]))

    assert len(result) == 1
    assert result.iloc[0]["a_buy_amount_rate"] == 1.0


def test_evidence_serializer_handles_database_dates():
    assert _json_default(date(2026, 7, 16)) == "2026-07-16"


def test_info_identity_does_not_conflate_null_and_zero():
    assert _info_exact_key({"a_buy_amount": None}) != _info_exact_key(
        {"a_buy_amount": 0}
    )


def _flow_engine(calendar, bars=()):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        conn.execute(text("""CREATE TABLE sm_stock_kline (
            stock_code TEXT, trade_date TEXT, k_type INTEGER,
            adjust_type INTEGER, volume REAL
        )"""))
        conn.execute(text("""CREATE TABLE sm_stock_capital_flow_daily (
            stock_code TEXT, trade_date TEXT, main_net_inflow REAL,
            max_net_inflow REAL, lg_net_inflow REAL, mid_net_inflow REAL,
            sm_net_inflow REAL, data_source TEXT
        )"""))
        for day, status in calendar:
            conn.execute(text("INSERT INTO si_trade_calendar VALUES (:day, :status)"),
                         {"day": day, "status": status})
        for code, day, volume in bars:
            conn.execute(text("INSERT INTO sm_stock_kline VALUES (:code, :day, 1, 0, :volume)"),
                         {"code": code, "day": day, "volume": volume})
    return engine


def _flow_row(code="600000", day="2026-09-04"):
    return {
        "stock_code": code, "trade_date": day,
        "main_net_inflow": 30, "max_net_inflow": 10,
        "lg_net_inflow": 20, "mid_net_inflow": -15, "sm_net_inflow": -15,
    }


@pytest.mark.parametrize("mutation", [{"main_net_inflow": None}, {"lg_net_inflow": "bad"}, {}])
def test_malformed_flow_components_are_invalid_not_task_crashes(mutation):
    row = {**_flow_row(), **mutation} if mutation else {}
    assert not flow_components_valid(row)


def test_empty_open_day_blocks_before_provider_fetch(monkeypatch, tmp_path):
    engine = _flow_engine(
        [("2026-09-03", 1), ("2026-09-04", 1)],
        [("600000", "2026-09-04", 100)],
    )
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda *_args: pytest.fail("must not fetch"))
    report = backfill.backfill_flow(
        engine, engine, "2026-09-03", "2026-09-04",
        workers=1, evidence_dir=tmp_path, dry_run=True,
    )
    assert report["status"] == "BLOCKED"
    assert report["missing_kline_dates"] == ["2026-09-03"]
    assert report["unresolved_prerequisite_count"] == 1
    assert report["fetched_pair_count"] == 0


def test_calendar_absence_is_not_a_closed_day(tmp_path):
    engine = _flow_engine([])
    report = backfill.backfill_flow(
        engine, engine, "2026-09-03", "2026-09-03",
        workers=1, evidence_dir=tmp_path, dry_run=True,
    )
    assert report["status"] == "BLOCKED"
    assert report["expected_pair_count"] == 0
    assert report["unresolved_pair_count"] == 0
    assert report["missing_calendar_dates"] == ["2026-09-03"]
    assert report["unresolved_prerequisite_count"] == 1


def test_confirmed_closed_days_need_no_stock_rows(tmp_path):
    engine = _flow_engine([("2026-09-05", 0), ("2026-09-06", 0)])
    report = backfill.backfill_flow(
        engine, engine, "2026-09-05", "2026-09-06",
        workers=1, evidence_dir=tmp_path, dry_run=True,
    )
    assert report["status"] == "COMPLETE"
    assert report["expected_pair_count"] == 0
    assert report["unresolved_prerequisite_count"] == 0


def test_eastmoney_failure_does_not_silently_switch_provider(monkeypatch, tmp_path):
    engine = _flow_engine([("2026-09-04", 1)], [("600000", "2026-09-04", 100)])
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda code, _dates: (code, [], "timeout"))
    monkeypatch.setattr(backfill, "_fetch_flow_code_baidu", lambda *_args: pytest.fail("implicit source switch"))
    report = backfill.backfill_flow(
        engine, engine, "2026-09-04", "2026-09-04",
        workers=1, evidence_dir=tmp_path, dry_run=True,
    )
    assert report["status"] == "INCOMPLETE"
    assert report["unresolved_pair_count"] == 1
    assert report["provider_policy"] == "eastmoney_transports_only"


def test_flow_checkpoint_resumes_without_refetching_completed_keys(monkeypatch, tmp_path):
    engine = _flow_engine([("2026-09-04", 1)], [("600000", "2026-09-04", 100)])
    path = tmp_path / "flow-fetch-progress.json"
    backfill._save_flow_progress(path, "2026-09-04", "2026-09-04", [_flow_row()])
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda *_: pytest.fail("completed key refetched"))
    result = backfill.backfill_flow(engine, engine, "2026-09-04", "2026-09-04",
                                   workers=1, evidence_dir=tmp_path, dry_run=True)
    assert result["status"] == "COMPLETE"
    assert result["fetch_methods"]["checkpoint"] == 1
    assert backfill._load_flow_progress(path, "2026-09-03", "2026-09-03", {}) == []
    engine.dispose()


def test_flow_exhausted_budget_does_not_schedule_the_remaining_universe(monkeypatch, tmp_path):
    engine = _flow_engine([("2026-09-04", 1)], [("600000", "2026-09-04", 100)])
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda *_: pytest.fail("budget exhausted"))
    result = backfill.backfill_flow(engine, engine, "2026-09-04", "2026-09-04",
                                   workers=1, evidence_dir=tmp_path, dry_run=True,
                                   fetch_budget_seconds=0)
    assert result["status"] == "INCOMPLETE"
    assert result["unresolved_pair_count"] == 1
    engine.dispose()


def test_explicit_source_selection_reports_existing_and_fetched_sources(monkeypatch, tmp_path):
    engine = _flow_engine(
        [("2026-09-04", 1)],
        [("600000", "2026-09-04", 100), ("000001", "2026-09-04", 100)],
    )
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO sm_stock_capital_flow_daily VALUES (
            '000001', '2026-09-04', 30, 10, 20, -15, -15, 'push2hist'
        )"""))
    monkeypatch.setattr(backfill, "_fetch_flow_code", lambda *_args: pytest.fail("Baidu explicitly selected"))
    monkeypatch.setattr(backfill, "_fetch_flow_code_baidu", lambda code, _dates: (
        code, [{**_flow_row(code), "_data_source": "baidu"}], "",
    ))
    report = backfill.backfill_flow(
        engine, engine, "2026-09-04", "2026-09-04",
        workers=1, evidence_dir=tmp_path, dry_run=True, baidu_only=True,
    )
    assert report["status"] == "COMPLETE"
    assert report["existing_source_pair_counts"] == {"push2hist": 1}
    assert report["fetched_source_pair_counts"] == {"baidu": 1}
    assert report["provider_policy"] == "baidu_explicit_only"


def test_main_returns_failure_for_missing_prerequisite_with_zero_expected_pairs(monkeypatch, tmp_path):
    monkeypatch.setattr(backfill, "create_batch_engine", lambda: object())
    monkeypatch.setattr(backfill, "get_kline_engine", lambda: object())
    monkeypatch.setattr(backfill, "backfill_flow", lambda *_args, **_kwargs: {
        "status": "BLOCKED", "unresolved_pair_count": 0,
        "unresolved_prerequisite_count": 1,
    })
    monkeypatch.setattr("sys.argv", [
        "backfill", "--start-date", "2026-09-03", "--end-date", "2026-09-03",
        "--skip-lhb", "--evidence-dir", str(tmp_path),
    ])
    assert backfill.main() == 3

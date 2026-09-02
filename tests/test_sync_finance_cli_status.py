from __future__ import annotations

import pandas as pd
import pytest
import json
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from types import SimpleNamespace
import threading
import time

from biz.stock_finance import sync_finance


def _finance_frame(code: str) -> pd.DataFrame:
    minimum = sync_finance.minimum_expected_report_date(date.today())
    return pd.DataFrame(
        [{"stock_code": code, "report_date": minimum.isoformat()}]
    )


def _stub_atomic_batch_seal(monkeypatch) -> None:
    monkeypatch.setattr(
        sync_finance,
        "append_finance_atomic_batch_seal",
        lambda *args, **kwargs: {
            "schema": "probiga.finance-atomic-batch-seal.v1",
            "status": "COMPLETE",
            "eligible_member_count": 1,
        },
    )


def test_finance_cli_fails_when_stock_universe_is_empty(monkeypatch):
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(sync_finance, "get_finance_stock_universe", lambda engine: {})

    assert sync_finance.main(["--sleep", "0"]) == 2


def test_finance_cli_fails_batch_when_any_stock_fails_but_keeps_processing(
    monkeypatch,
):
    attempted: list[str] = []
    committed: list[str] = []
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None, "000002": None},
    )

    def fake_fetch(code: str) -> pd.DataFrame:
        attempted.append(code)
        if code == "000002":
            raise RuntimeError("provider unavailable")
        return _finance_frame(code)

    def fake_upsert(engine, frame, *, stock_code: str, coverage_end: date) -> int:
        assert coverage_end == date.today()
        committed.append(stock_code)
        return 1

    monkeypatch.setattr(sync_finance, "fetch_finance", fake_fetch)
    monkeypatch.setattr(sync_finance, "upsert_finance", fake_upsert)

    assert sync_finance.main(["--sleep", "0"]) == 1
    assert attempted == ["000001", "000002"]
    assert committed == ["000001"]


def test_finance_cli_succeeds_only_when_every_stock_finishes(monkeypatch):
    _stub_atomic_batch_seal(monkeypatch)
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None, "000002": None},
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance",
        _finance_frame,
    )
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda engine, frame, *, stock_code, coverage_end: 1,
    )

    assert sync_finance.main(["--sleep", "0"]) == 0


def test_finance_cli_rejects_empty_provider_frame_and_does_not_commit(monkeypatch):
    committed: list[str] = []
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None},
    )
    monkeypatch.setattr(sync_finance, "fetch_finance", lambda code: pd.DataFrame())
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda engine, frame, *, stock_code: committed.append(stock_code) or 1,
    )

    assert sync_finance.main(["--sleep", "0"]) == 1
    assert committed == []


def test_002731_stale_primary_uses_only_official_nonfiling_disposition(
    monkeypatch,
):
    calls: list[tuple[str, str]] = []
    today = date(2026, 8, 26)

    class FakeEngine:
        def begin(self):
            return nullcontext(object())

        def dispose(self):
            return None

    evidence = {
        "source": "cninfo.finance.nonfiling",
        "reason_code": "CNINFO_REGULATORY_PERIODIC_REPORT_NOT_FILED",
        "stock_code": "002731",
        "expected_report_date": sync_finance.minimum_expected_report_date(
            today
        ).isoformat(),
        "announcement_id": "1225497518",
        "announcement_title": "未在规定期限内披露定期报告",
        "valid_until": (today + timedelta(days=7)).isoformat(),
        "next_retry_date": (today + timedelta(days=1)).isoformat(),
    }
    monkeypatch.setattr(sync_finance, "get_engine", FakeEngine)
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"002731": date(2015, 1, 1)},
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance",
        lambda code: pd.DataFrame(
            [{"stock_code": code, "report_date": "2025-09-30"}]
        ),
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_cninfo_nonfiling_evidence",
        lambda code, **kwargs: evidence,
    )
    monkeypatch.setattr(
        sync_finance,
        "append_finance_expected_unavailable",
        lambda connection, **kwargs: (
            calls.append((kwargs["stock_code"], kwargs["expected_report_date"].isoformat()))
            or SimpleNamespace(coverage_id="c" * 64)
        ),
    )
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda *args, **kwargs: pytest.fail("old finance rows must not be re-upserted"),
    )

    assert sync_finance.main([
        "--code", "002731", "--sleep", "0",
        "--as-of-date", today.isoformat(),
    ]) == 0
    assert calls == [
        ("002731", sync_finance.minimum_expected_report_date(today).isoformat())
    ]


def test_empty_finance_frame_cannot_publish_successful_empty_coverage():
    with pytest.raises(ValueError, match="at least one row"):
        sync_finance.upsert_finance(
            object(),
            pd.DataFrame(),
            stock_code="000001",
            coverage_end=date.today(),
        )


def test_stale_latest_report_period_is_data_blocked() -> None:
    with pytest.raises(RuntimeError, match="最新报告期过旧"):
        sync_finance.validate_finance_response(
            "000001",
            pd.DataFrame(
                [{"stock_code": "000001", "report_date": "2025-09-30"}]
            ),
            as_of=date(2026, 8, 26),
            minimum_report_date=date(2026, 3, 31),
        )


def test_post_deadline_new_listing_is_exempt_from_prior_period_floor() -> None:
    latest = sync_finance.validate_finance_response(
        "000001",
        pd.DataFrame(
            [{"stock_code": "000001", "report_date": "2025-12-31"}]
        ),
        as_of=date(2026, 8, 26),
        minimum_report_date=date(2026, 3, 31),
        disclosure_deadline=date(2026, 4, 30),
        listing_date=date(2026, 5, 8),
    )

    assert latest == date(2025, 12, 31)


def test_new_listing_exemption_never_turns_empty_response_into_success() -> None:
    with pytest.raises(RuntimeError, match="返回空结果"):
        sync_finance.validate_finance_response(
            "000001",
            pd.DataFrame(),
            as_of=date(2026, 8, 26),
            minimum_report_date=date(2026, 3, 31),
            disclosure_deadline=date(2026, 4, 30),
            listing_date=date(2026, 5, 8),
        )


def test_finance_coverage_threshold_cannot_be_lowered(monkeypatch):
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())

    assert sync_finance.main(["--min-code-coverage", "0.99"]) == 2


def test_finance_limit_zero_means_the_complete_loaded_universe(monkeypatch):
    attempted: list[str] = []
    _stub_atomic_batch_seal(monkeypatch)
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None, "000002": None, "600000": None},
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance",
        lambda code: attempted.append(code) or _finance_frame(code),
    )
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda engine, frame, *, stock_code, coverage_end: 1,
    )

    assert sync_finance.main(["--limit", "0", "--sleep", "0"]) == 0
    assert attempted == ["000001", "000002", "600000"]


def test_finance_seal_existing_emits_explicit_machine_result(monkeypatch, capsys):
    class FakeEngine:
        def dispose(self):
            return None

    monkeypatch.setattr(sync_finance, "get_engine", FakeEngine)
    sealed_as_of = []
    monkeypatch.setattr(
        sync_finance,
        "append_finance_atomic_batch_seal",
        lambda *args, **kwargs: sealed_as_of.append(kwargs["as_of_date"]) or {
            "schema": "probiga.pit-finance-atomic-batch.v1",
            "eligible_code_count": 5200,
            "catalog_member_count": 5200,
            "expected_unavailable_count": 1,
            "eligible_code_set_hash": "a" * 64,
            "coverage_root_sha256": "b" * 64,
            "batch_root_sha256": "c" * 64,
            "seal_coverage_id": "d" * 64,
        },
    )

    assert sync_finance.main([
        "--seal-existing", "--as-of-date", "2026-08-26",
    ]) == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["schema"] == "probiga.finance-atomic-batch-result.v1"
    assert payload["seal_schema"] == "probiga.pit-finance-atomic-batch.v1"
    assert payload["status"] == "PASS"
    assert sealed_as_of == [date(2026, 8, 26)]


def test_finance_offset_selects_one_ordered_non_overlapping_shard(monkeypatch):
    attempted: list[str] = []
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {
            "000001": None,
            "000002": None,
            "000003": None,
            "000004": None,
        },
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance",
        lambda code: attempted.append(code) or _finance_frame(code),
    )
    monkeypatch.setattr(sync_finance, "upsert_finance", lambda *a, **k: 1)

    assert sync_finance.main([
        "--offset", "2", "--limit", "2", "--sleep", "0",
    ]) == 0
    assert attempted == ["000003", "000004"]


@pytest.mark.parametrize(
    "argv",
    [
        ["--offset", "-1"],
        ["--code", "000001", "--offset", "1"],
    ],
)
def test_finance_offset_rejects_ambiguous_or_negative_scope(monkeypatch, argv):
    engine_requested = False

    def fail_if_engine_requested():
        nonlocal engine_requested
        engine_requested = True
        return object()

    monkeypatch.setattr(sync_finance, "get_engine", fail_if_engine_requested)
    assert sync_finance.main(argv) == 2
    assert engine_requested is False


def test_finance_fetch_workers_are_concurrent_but_database_writes_are_serial(
    monkeypatch,
):
    codes = ["000001", "000002", "000003", "000004"]
    active_fetches = 0
    maximum_active_fetches = 0
    fetch_lock = threading.Lock()
    release_fetches = threading.Event()
    committed: list[str] = []
    upsert_threads: list[int] = []
    main_thread = threading.get_ident()

    _stub_atomic_batch_seal(monkeypatch)
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {code: None for code in codes},
    )

    def fake_fetch(code: str) -> pd.DataFrame:
        nonlocal active_fetches, maximum_active_fetches
        with fetch_lock:
            active_fetches += 1
            maximum_active_fetches = max(maximum_active_fetches, active_fetches)
            if maximum_active_fetches >= 2:
                release_fetches.set()
        assert release_fetches.wait(timeout=2)
        time.sleep(0.01)
        with fetch_lock:
            active_fetches -= 1
        return _finance_frame(code)

    def fake_upsert(engine, frame, *, stock_code: str, coverage_end: date) -> int:
        upsert_threads.append(threading.get_ident())
        committed.append(stock_code)
        return 1

    monkeypatch.setattr(sync_finance, "fetch_finance", fake_fetch)
    monkeypatch.setattr(sync_finance, "upsert_finance", fake_upsert)

    assert sync_finance.main(["--workers", "2", "--sleep", "0"]) == 0
    assert maximum_active_fetches == 2
    assert committed == codes
    assert upsert_threads == [main_thread] * len(codes)


def test_finance_fetch_workers_keep_processing_after_provider_failure(monkeypatch):
    attempted: list[str] = []
    committed: list[str] = []
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None, "000002": None, "000003": None},
    )

    def fake_fetch(code: str) -> pd.DataFrame:
        attempted.append(code)
        if code == "000002":
            raise RuntimeError("provider unavailable")
        return _finance_frame(code)

    monkeypatch.setattr(sync_finance, "fetch_finance", fake_fetch)
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda engine, frame, *, stock_code, coverage_end: (
            committed.append(stock_code) or 1
        ),
    )

    assert sync_finance.main(["--workers", "3", "--sleep", "0"]) == 1
    assert set(attempted) == {"000001", "000002", "000003"}
    assert committed == ["000001", "000003"]


@pytest.mark.parametrize("workers", [0, sync_finance.MAX_FETCH_WORKERS + 1])
def test_finance_fetch_workers_are_safely_bounded(monkeypatch, workers):
    engine_requested = False

    def fail_if_engine_requested():
        nonlocal engine_requested
        engine_requested = True
        return object()

    monkeypatch.setattr(sync_finance, "get_engine", fail_if_engine_requested)

    assert sync_finance.main(["--workers", str(workers)]) == 2
    assert engine_requested is False


def _baseline(code: str, *, window_end: date) -> dict:
    return {
        "stock_code": code,
        "coverage_id": (code[-1] or "1") * 64,
        "window_start": date(1900, 1, 1),
        "window_end": window_end,
        "known_at": datetime.combine(window_end, datetime.min.time()),
        "coverage_status": "COMPLETE",
        "result_count": 4,
        "guard_status": None,
        "guard_as_of_date": None,
        "latest_report_date": date(2026, 6, 30),
    }


def test_incremental_plan_fetches_only_changed_and_missing_issuers(monkeypatch):
    target = date(2026, 9, 1)
    monkeypatch.setattr(
        sync_finance,
        "get_finance_incremental_baselines",
        lambda engine: {
            "000001": _baseline("000001", window_end=date(2026, 8, 31)),
            "000002": _baseline("000002", window_end=date(2026, 8, 31)),
        },
    )
    discovery = {
        "events": [{
            "stock_code": "000002",
            "query_date": target.isoformat(),
        }],
        "changed_codes": ["000002"],
    }
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance_incremental_discovery",
        lambda **kwargs: discovery,
    )
    monkeypatch.setattr(
        sync_finance,
        "append_finance_incremental_discovery",
        lambda engine, evidence: "d" * 64,
    )

    plan = sync_finance.build_finance_incremental_plan(
        object(),
        universe={"000001": None, "000002": None, "000003": None},
        as_of=target,
        disclosure_gate=sync_finance.finance_disclosure_gate(target),
    )

    assert plan["mode"] == "INCREMENTAL_DISCOVERY"
    assert plan["fetch_codes"] == ["000002", "000003"]
    assert plan["reused_codes"] == ["000001"]
    assert plan["refresh_reasons"]["000002"] == (
        "SOURCE_NOTICE_OR_UPDATE_CHANGED"
    )
    assert plan["refresh_reasons"]["000003"] == "MISSING_PRIMARY_COVERAGE"
    assert plan["discovery_coverage_id"] == "d" * 64


def test_incremental_plan_falls_back_to_full_primary_when_discovery_unproven(
    monkeypatch,
):
    target = date(2026, 9, 1)
    monkeypatch.setattr(
        sync_finance,
        "get_finance_incremental_baselines",
        lambda engine: {
            code: _baseline(code, window_end=date(2026, 8, 31))
            for code in ("000001", "000002")
        },
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance_incremental_discovery",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("provider down")),
    )

    plan = sync_finance.build_finance_incremental_plan(
        object(),
        universe={"000001": None, "000002": None},
        as_of=target,
        disclosure_gate=sync_finance.finance_disclosure_gate(target),
    )

    assert plan["mode"] == "FULL_PRIMARY_FALLBACK"
    assert plan["fetch_codes"] == ["000001", "000002"]
    assert plan["reused_codes"] == []
    assert "provider down" in plan["fallback_reason"]


def test_exact_date_discovery_requires_two_stable_complete_sweeps(monkeypatch):
    target = date(2026, 9, 1)
    calls: list[str] = []

    def fake_result_set(*, filter_expression, sort_fields):
        calls.append(filter_expression)
        field = "NOTICE_DATE" if "NOTICE_DATE" in filter_expression else "UPDATE_DATE"
        code = "000001" if field == "NOTICE_DATE" else "000002"
        rows = [{
            "SECURITY_CODE": code,
            "REPORT_DATE": "2026-06-30 00:00:00",
            "REPORT_TYPE": "中报",
            "NOTICE_DATE": "2026-09-01 00:00:00",
            "UPDATE_DATE": "2026-09-01 00:00:00",
        }]
        return rows, {
            "page_size": 500,
            "page_count": 1,
            "total_count": 1,
            "row_count": 1,
            "page_row_counts": [1],
            "page_raw_sha256": ["a" * 64],
            "page_content_sha256": ["b" * 64],
        }

    monkeypatch.setattr(
        sync_finance, "_fetch_eastmoney_result_set", fake_result_set
    )
    monkeypatch.setattr(
        sync_finance,
        "_capture_now",
        lambda: datetime(2026, 9, 1, 22, 5, 0),
    )

    evidence = sync_finance.fetch_finance_incremental_discovery(
        window_start=target,
        window_end=target,
        universe_codes=["000001", "000002"],
    )

    assert len(calls) == 4
    assert evidence["stability_status"] == "STABLE_DOUBLE_SWEEP"
    assert evidence["changed_codes"] == ["000001", "000002"]
    assert evidence["event_count"] == 2
    assert all(sweep["query_count"] == 2 for sweep in evidence["sweeps"])
    assert evidence["sweeps"][0]["content_sha256"] == (
        evidence["sweeps"][1]["content_sha256"]
    )


def test_historical_upsert_rejects_post_target_mutable_source_date():
    frame = pd.DataFrame([{
        "stock_code": "000001",
        "report_date": "2026-06-30",
        "notice_date": "2026-08-30",
        "source_update_date": "2026-09-02",
    }])
    frame.attrs["source_receipt"] = {
        "stability_status": "STABLE_DOUBLE_SWEEP",
    }
    with pytest.raises(ValueError, match="post-target"):
        sync_finance.upsert_finance(
            object(),
            frame,
            stock_code="000001",
            coverage_end=date(2026, 9, 1),
            observed_at=datetime(2026, 9, 2, 8, 0, 0),
        )


def test_new_listing_empty_requires_stable_source_and_records_no_fact(
    monkeypatch,
):
    captured: list[dict] = []

    class FakeEngine:
        def dispose(self):
            return None

    empty = pd.DataFrame()
    empty.attrs["source_receipt"] = {
        "stock_code": "601123",
        "stability_status": "STABLE_DOUBLE_SWEEP",
        "stable_sweep_count": 2,
        "stable_content_sha256": "a" * 64,
    }
    monkeypatch.setattr(sync_finance, "get_engine", FakeEngine)
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"601123": date(2026, 9, 1)},
    )
    monkeypatch.setattr(sync_finance, "fetch_finance", lambda code: empty)
    monkeypatch.setattr(
        sync_finance,
        "append_new_listing_finance_empty_coverage",
        lambda engine, **kwargs: captured.append(kwargs) or "e" * 64,
    )
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda *args, **kwargs: pytest.fail("legal empty must not create facts"),
    )

    assert sync_finance.main([
        "--code", "601123", "--as-of-date", "2026-09-01", "--sleep", "0",
    ]) == 0
    assert captured[0]["listing_date"] == date(2026, 9, 1)
    assert captured[0]["disclosure_deadline"] == date(2026, 8, 31)

from datetime import datetime

import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

from integrations.bigqmt.release_identity import render_strategy_artifact
from integrations.bigqmt.qmt_strategy import probiga_big_qmt_bridge as producer


ROOT = Path(__file__).resolve().parents[1]


def test_bigqmt_strategy_uses_only_native_pre_close():
    rows = producer._bar_rows(
        {
            "000001.SZ": [
                {
                    "time": "2026-08-20 15:00:00",
                    "open": 10,
                    "close": 10,
                    "high": 10,
                    "low": 10,
                    "volume": 1,
                    "amount": 1000,
                    "preClose": 9.8,
                },
                {
                    "time": "2026-08-21 15:00:00",
                    "open": 5,
                    "close": 5,
                    "high": 5,
                    "low": 5,
                    "volume": 1,
                    "amount": 500,
                },
            ]
        },
        "1d",
    )

    assert rows[0]["pre_close"] == 9.8
    assert rows[0]["pre_close_origin"] == "NATIVE_QMT"
    assert rows[1]["pre_close"] is None
    assert rows[1]["pre_close_origin"] == "MISSING_NATIVE_QMT"
    assert rows[1]["pre_close"] != rows[0]["close"]


def test_direct_model_loader_uses_the_hash_bound_qmt_userdata_sibling(
    monkeypatch, tmp_path,
):
    direct_source = (ROOT / "acquisition" / "qmt_model.py").read_bytes()
    direct_hash = hashlib.sha256(direct_source).hexdigest()
    assert producer.DIRECT_ACQUISITION_MODEL_SHA256 == direct_hash
    python_root = tmp_path / "python"
    bridge_root = tmp_path / "userdata" / "probiga_bridge"
    python_root.mkdir()
    bridge_root.mkdir(parents=True)
    (python_root / f"probiga_direct_acquisition_{direct_hash}.py").write_bytes(
        direct_source
    )
    monkeypatch.setattr(producer, "_bridge_root", str(bridge_root))

    instance = producer._load_direct_acquisition_model()

    assert instance.source_sha256 == direct_hash
    assert instance.native_globals is producer.__dict__
    assert Path(instance.root) == (
        tmp_path / "userdata" / "probiga_direct_acquisition" / "qmt"
    )


def test_direct_model_reuses_only_the_existing_strategy_lifecycle(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "_probiga_big_qmt_direct_lifecycle_test",
        producer.__file__,
    )
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    bridge_root = tmp_path / "userdata" / "probiga_bridge"
    bridge_root.mkdir(parents=True)
    lifecycle = []
    old_calls = []

    class DirectModel:
        source_sha256 = "d" * 64
        last_status = "created"

        def heartbeat(self, status, error_code=None):
            self.last_status = status
            lifecycle.append(("heartbeat", status, error_code))

        def poll(self, context):
            self.last_status = "idle"
            lifecycle.append(("poll", context))

    direct = DirectModel()

    class Context:
        def run_time(self, callback, period, start):
            lifecycle.append(("run_time", callback, period, start))

    context = Context()
    loaded._find_bridge_root = lambda: str(bridge_root)
    loaded._load_direct_acquisition_model = lambda: direct
    loaded._recover_inflight_requests = lambda: None
    loaded._refresh_subscription = lambda *_args, **_kwargs: old_calls.append(
        "subscription"
    )
    loaded._capabilities_payload = lambda _context: {}
    loaded._process_one_request = lambda _context: old_calls.append("request")
    loaded._refresh_full_snapshot = lambda _context: old_calls.append("snapshot")
    loaded._write_tracked_snapshot = lambda **_kwargs: old_calls.append(
        "tracked"
    )
    loaded._cleanup_queue_artifacts = lambda: old_calls.append("cleanup")

    loaded.init(context)
    assert [item[1] for item in lifecycle if item[0] == "run_time"] == [
        "bridge_tick",
        "direct_acquisition_tick",
    ]
    old_calls.clear()
    loaded.direct_acquisition_tick(context)
    assert lifecycle[-1] == ("poll", context)
    assert old_calls == []

    loaded.after_init(context)
    heartbeat = json.loads((bridge_root / "heartbeat.json").read_text())
    assert heartbeat["direct_acquisition_model_sha256"] == "d" * 64
    assert heartbeat["direct_acquisition_status"] == "idle"
    assert heartbeat["status"] == "running"

    loaded.stop(context)
    stopped = json.loads((bridge_root / "heartbeat.json").read_text())
    assert direct.last_status == "stopped"
    assert stopped["status"] == "stopped"


def test_daily_bar_time_is_normalized_to_market_close_for_every_qmt_shape():
    midnight_ms = int(datetime(2026, 8, 21).timestamp() * 1000)
    assert producer._time_text(midnight_ms, "1d") == "2026-08-21 15:00:00"
    assert producer._time_text("20260821000000", "1d") == (
        "2026-08-21 15:00:00"
    )
    assert producer._time_text("2026-08-21 00:00:00", "1d") == (
        "2026-08-21 15:00:00"
    )
    assert producer._time_text("20260821103105", "1m") == (
        "2026-08-21 10:31:05"
    )


def test_daily_reader_never_requests_synthetic_filled_bars():
    class Context:
        def __init__(self):
            self.calls = []

        def get_market_data_ex_ori(self, *args, **kwargs):
            self.calls.append(kwargs)
            return {}

    context = Context()
    params = {
        "stock_codes": ["000001.SZ"],
        "start_date": "2026-08-21",
        "end_date": "2026-08-21",
    }
    assert producer._market_rows(context, params, "1d") == []
    assert context.calls[-1]["fill_data"] is False
    assert producer._market_rows(context, params, "1m") == []
    assert context.calls[-1]["fill_data"] is False


def test_native_trading_calendar_capability_uses_context_api_without_inference():
    class Context:
        def __init__(self):
            self.calls = []

        def get_trading_dates(self, *args):
            self.calls.append(args)
            return ["20260826", "20260824", "20260825", "20260825"]

    context = Context()
    capabilities = producer._execute_request(context, "capabilities", {})
    result = producer._execute_request(
        context,
        "trading_calendar",
        {
            "market": "SH",
            "start_date": "2026-08-24",
            "end_date": "2026-08-26",
        },
    )

    assert capabilities["strategy_release_protocol"] == (
        producer.STRATEGY_RELEASE_PROTOCOL
    )
    assert capabilities["strategy_identity_frozen"] is True
    assert capabilities["strategy_identity_status"] == "UNAVAILABLE"
    assert capabilities["strategy_source_sha256"] == ""
    assert capabilities["native_capabilities"] == [
        {
            "capability": "trading_calendar",
            "action": "trading_calendar",
            "available": True,
            "source_method": "ContextInfo.get_trading_dates",
        },
        {
            "capability": "announcement",
            "action": "announcement",
            "available": False,
            "source_method": "ContextInfo.get_market_data_ex(_ori)",
        },
        {
            "capability": "index_weight",
            "action": "index_members_many",
            "available": False,
            "source_method": "membership_only_no_native_weight",
        },
    ]
    assert context.calls == [
        ("000001.SH", "20260824", "20260826", -1, "1d")
    ]
    assert result["source_method"] == "ContextInfo.get_trading_dates"
    assert result["source_stock_code"] == "000001.SH"
    assert result["requested_start_date"] == "2026-08-24"
    assert result["requested_end_date"] == "2026-08-26"
    assert result["observed_start_date"] == "2026-08-24"
    assert result["observed_end_date"] == "2026-08-26"
    assert [row["trade_date"] for row in result["rows"]] == [
        "2026-08-24",
        "2026-08-25",
        "2026-08-26",
    ]
    assert [row["day_week"] for row in result["rows"]] == [1, 2, 3]

    calendar_response = producer._strategy_identity_payload()
    calendar_response.update(result)
    release_identity_keys = (
        "strategy_release_protocol",
        "strategy_identity_protocol",
        "strategy_identity_frozen",
        "strategy_identity_status",
        "strategy_build_sha",
        "strategy_git_blob",
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256",
    )
    assert {
        key: calendar_response.get(key) for key in release_identity_keys
    } == {
        key: capabilities.get(key) for key in release_identity_keys
    }


def test_native_trading_calendar_capability_fails_closed_when_unavailable():
    capabilities = producer._execute_request(object(), "capabilities", {})
    assert capabilities["native_capabilities"][0]["available"] is False
    assert capabilities["native_capabilities"][1] == {
        "capability": "announcement",
        "action": "announcement",
        "available": False,
        "source_method": "ContextInfo.get_market_data_ex(_ori)",
    }
    assert capabilities["native_capabilities"][2] == {
        "capability": "index_weight",
        "action": "index_members_many",
        "available": False,
        "source_method": "membership_only_no_native_weight",
    }
    try:
        producer._execute_request(
            object(),
            "trading_calendar",
            {
                "market": "SH",
                "start_date": "2026-08-24",
                "end_date": "2026-08-26",
            },
        )
    except RuntimeError as exc:
        assert "get_trading_dates is unavailable" in str(exc)
    else:
        raise AssertionError("missing native calendar API must fail closed")


def test_announcement_action_serializes_native_frames_and_outer_response(
    monkeypatch, tmp_path,
):
    requests_root = tmp_path / "requests"
    responses_root = tmp_path / "responses"
    requests_root.mkdir()
    responses_root.mkdir()
    monkeypatch.setattr(producer, "_bridge_root", str(tmp_path))
    monkeypatch.setattr(producer, "_requests_root", str(requests_root))
    monkeypatch.setattr(producer, "_responses_root", str(responses_root))
    downloads = []
    monkeypatch.setattr(
        producer,
        "_download_announcement_history",
        lambda symbols, start, end: downloads.append((symbols, start, end)),
    )

    class Index:
        name = "time"

    class Frame:
        index = Index()

        @staticmethod
        def iterrows():
            return iter([(
                1787937000000,
                {"证券": "000001.SZ", "主题": "董事会公告"},
            )])

    class Context:
        def __init__(self):
            self.calls = []

        def get_market_data_ex_ori(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return {"000001.SZ": Frame()}

    request_id = "announcement-action-contract"
    (requests_root / f"{request_id}.json").write_text(json.dumps({
        "request_id": request_id,
        "action": "announcement",
        "params": {
            "stock_codes": ["000001.SZ"],
            "start_date": "20260801000000",
            "end_date": "20260828210000",
            "download_history": True,
        },
    }), encoding="utf-8")
    context = Context()

    assert producer._process_one_request(context) is True
    with gzip.open(
        responses_root / f"{request_id}.json.gz", "rt", encoding="utf-8"
    ) as handle:
        response = json.load(handle)

    assert response["status"] == "ok"
    assert response["action"] == "announcement"
    assert response["source_method"] == "ContextInfo.get_market_data_ex_ori"
    assert response["requested_stock_count"] == 1
    frame_payload = response["frames"]["000001.SZ"]
    assert frame_payload["index_name"] == "time"
    assert frame_payload["rows"] == [{
        "index": 1787937000000,
        "row": {"证券": "000001.SZ", "主题": "董事会公告"},
    }]
    assert frame_payload["estimated_uncompressed_bytes"] > 0
    assert downloads == [
        (["000001.SZ"], "20260801000000", "20260828210000")
    ]
    assert context.calls[0][0] == ([], ["000001.SZ"])
    assert context.calls[0][1]["period"] == "announcement"
    assert context.calls[0][1]["fill_data"] is False
    assert context.calls[0][1]["subscribe"] is False


def test_announcement_prefers_exact_key_reader_for_all_empty_chunk(monkeypatch):
    monkeypatch.setattr(
        producer, "_download_announcement_history", lambda *_args: None
    )

    class Index:
        name = "time"

    class EmptyFrame:
        index = Index()

        @staticmethod
        def iterrows():
            return iter(())

    class Context:
        @staticmethod
        def get_market_data_ex(*_args, **_kwargs):
            return {"000001.SZ": EmptyFrame(), "600000.SH": EmptyFrame()}

        @staticmethod
        def get_market_data_ex_ori(*_args, **_kwargs):
            return {}

    result = producer._execute_request(Context(), "announcement", {
        "stock_codes": ["000001.SZ", "600000.SH"],
        "start_date": "20260729000000",
        "end_date": "20260829182000",
        "download_history": True,
    })

    assert result["source_method"] == "ContextInfo.get_market_data_ex"
    assert set(result["frames"]) == {"000001.SZ", "600000.SH"}
    assert result["observed_stock_count"] == 2
    assert result["observed_row_count"] == 0
    assert len(result["capture_receipt_sha256"]) == 64


def test_announcement_download_fallback_never_requests_incremental_widening(
    monkeypatch,
):
    calls = []
    monkeypatch.delattr(producer, "download_history_data2", raising=False)
    monkeypatch.setattr(
        producer,
        "download_history_data",
        lambda *args, **kwargs: calls.append((args, kwargs)),
        raising=False,
    )

    producer._download_announcement_history(
        ["000001.SZ"], "20260729000000", "20260829182000"
    )

    assert calls == [(('000001.SZ', 'announcement', '20260729000000',
                       '20260829182000'), {})]


def test_calendar_spool_response_carries_complete_release_identity(
    monkeypatch, tmp_path,
):
    requests_root = tmp_path / "requests"
    responses_root = tmp_path / "responses"
    requests_root.mkdir()
    responses_root.mkdir()
    monkeypatch.setattr(producer, "_bridge_root", str(tmp_path))
    monkeypatch.setattr(producer, "_requests_root", str(requests_root))
    monkeypatch.setattr(producer, "_responses_root", str(responses_root))
    monkeypatch.setattr(producer, "_LOADED_STRATEGY_IDENTITY", {
        "strategy_identity_protocol": producer.STRATEGY_IDENTITY_PROTOCOL,
        "strategy_identity_frozen": True,
        "strategy_identity_status": "BOUND",
        "strategy_identity_error": "",
        "strategy_build_sha": "a" * 40,
        "strategy_git_blob": "b" * 40,
        "strategy_source_sha256": "c" * 64,
        "strategy_artifact_sha256": "d" * 64,
        "strategy_loaded_identity_sha256": "e" * 64,
        "strategy_identity_loaded_at": "2026-08-29 20:34:07",
    })

    request_id = "calendar-release-contract"
    (requests_root / f"{request_id}.json").write_text(json.dumps({
        "schema_version": 2,
        "request_id": request_id,
        "action": "trading_calendar",
        "params": {
            "market": "SH",
            "start_date": "2026-08-28",
            "end_date": "2026-08-28",
        },
    }), encoding="utf-8")

    class Context:
        @staticmethod
        def get_trading_dates(*_args):
            return ["20260828"]

    capabilities = producer._execute_request(Context(), "capabilities", {})
    assert producer._process_one_request(Context()) is True
    with gzip.open(
        responses_root / f"{request_id}.json.gz", "rt", encoding="utf-8"
    ) as handle:
        response = json.load(handle)

    release_identity_keys = (
        "strategy_release_protocol",
        "strategy_identity_protocol",
        "strategy_identity_frozen",
        "strategy_identity_status",
        "strategy_build_sha",
        "strategy_git_blob",
        "strategy_source_sha256",
        "strategy_artifact_sha256",
        "strategy_loaded_identity_sha256",
    )
    assert response["status"] == "ok"
    assert response["action"] == "trading_calendar"
    assert {
        key: response.get(key) for key in release_identity_keys
    } == {
        key: capabilities.get(key) for key in release_identity_keys
    }


def test_strategy_identity_is_frozen_at_module_load_not_reread_from_disk(
    tmp_path,
):
    source_path = Path(producer.__file__)
    source_bytes = source_path.read_bytes()
    source_hash = hashlib.sha256(source_bytes).hexdigest()
    rendered = render_strategy_artifact(
        source_bytes,
        build_sha="a" * 40,
        git_blob="b" * 40,
        source_sha256=source_hash,
    )
    installed_path = tmp_path / "probiga_big_qmt_bridge.py"
    installed_path.write_bytes(rendered["source_bytes"])
    manifest_path = tmp_path / producer.STRATEGY_RELEASE_MANIFEST_NAME
    manifest_path.write_text(
        json.dumps({
            "schema": producer.STRATEGY_RELEASE_MANIFEST_SCHEMA,
            "strategy_release_protocol": producer.STRATEGY_RELEASE_PROTOCOL,
            "strategy_identity_protocol": producer.STRATEGY_IDENTITY_PROTOCOL,
            "strategy_build_sha": "a" * 40,
            "strategy_git_blob": "b" * 40,
            "strategy_source_sha256": source_hash,
            "strategy_artifact_sha256": rendered["artifact_sha256"],
            "strategy_loaded_identity_sha256": rendered["identity_sha256"],
        }),
        encoding="utf-8",
    )
    spec = importlib.util.spec_from_file_location(
        "_probiga_frozen_strategy_test", installed_path
    )
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)

    before = loaded._execute_request(object(), "capabilities", {})
    installed_path.write_text("# overwritten after QMT model load\n", encoding="utf-8")
    manifest_path.write_text("{}", encoding="utf-8")
    after = loaded._execute_request(object(), "capabilities", {})

    assert before["strategy_identity_status"] == "BOUND"
    assert before["strategy_build_sha"] == "a" * 40
    assert before["strategy_git_blob"] == "b" * 40
    assert before["strategy_source_sha256"] == source_hash
    assert after["strategy_build_sha"] == before["strategy_build_sha"]
    assert after["strategy_git_blob"] == before["strategy_git_blob"]
    assert after["strategy_source_sha256"] == before["strategy_source_sha256"]


def test_embedded_strategy_identity_loads_without_dunder_file(
    monkeypatch, tmp_path,
):
    template = Path(producer.__file__).read_bytes()
    source_hash = hashlib.sha256(template).hexdigest()
    rendered = render_strategy_artifact(
        template,
        build_sha="a" * 40,
        git_blob="b" * 40,
        source_sha256=source_hash,
    )
    (tmp_path / "userdata").mkdir()
    python_dir = tmp_path / "python"
    python_dir.mkdir()
    (python_dir / producer.STRATEGY_RELEASE_MANIFEST_NAME).write_text(
        json.dumps({
            "schema": producer.STRATEGY_RELEASE_MANIFEST_SCHEMA,
            "strategy_release_protocol": producer.STRATEGY_RELEASE_PROTOCOL,
            "strategy_identity_protocol": producer.STRATEGY_IDENTITY_PROTOCOL,
            "strategy_build_sha": "a" * 40,
            "strategy_git_blob": "b" * 40,
            "strategy_source_sha256": source_hash,
            "strategy_artifact_sha256": rendered["artifact_sha256"],
            "strategy_loaded_identity_sha256": rendered["identity_sha256"],
        }),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    namespace = {"__name__": "_qmt_model_without_dunder_file"}
    exec(compile(rendered["source_bytes"], "<qmt-model>", "exec"), namespace)

    capabilities = namespace["_execute_request"](
        object(), "capabilities", {}
    )

    assert "__file__" not in namespace
    assert capabilities["strategy_identity_status"] == "BOUND"
    assert capabilities["strategy_build_sha"] == "a" * 40
    assert capabilities["strategy_loaded_identity_sha256"] == (
        rendered["identity_sha256"]
    )

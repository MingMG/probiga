from __future__ import annotations

import gzip
import json
from pathlib import Path

from integrations.qmt.pending_write import replay_pending_writes
from integrations.qmt.raw_store import archive_payload


class _Connection:
    def __init__(self, calls: list):
        self.calls = calls

    def execute(self, statement, params):
        self.calls.append((str(statement), params))


class _Transaction:
    def __init__(self, calls: list):
        self.connection = _Connection(calls)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class _Engine:
    def __init__(self):
        self.calls = []

    def begin(self):
        return _Transaction(self.calls)


class _FailingEngine:
    def begin(self):
        raise RuntimeError("database is down")


def test_archive_payload_writes_gzip_and_manifest(tmp_path: Path):
    engine = _Engine()
    payload = {"ok": True, "rows": [{"stock_code": "000001.SZ", "lastPrice": 10.2}]}

    result = archive_payload(
        engine,
        dataset="stock/tick",
        api_name="get_full_tick",
        params={"stock_codes": ["000001.SZ"]},
        payload=payload,
        period="tick",
        batch_id="batch-test",
        provenance={"client_version": "test"},
        raw_root=tmp_path,
    )

    target = Path(result.file_path)
    assert target.is_file()
    assert target.resolve().is_relative_to(tmp_path.resolve())
    with gzip.open(target, "rt", encoding="utf-8") as stream:
        envelope = json.load(stream)
    assert envelope["provider"] == "gj_qmt"
    assert envelope["payload"] == payload
    assert envelope["payload_hash"] == result.payload_hash
    assert result.row_count == 1
    assert result.symbol_count == 1
    assert len(engine.calls) == 1
    assert engine.calls[0][1]["provider"] == "gj_qmt"
    assert result.manifest_persisted is True
    assert result.pending_write_path is None


def test_archive_payload_keeps_raw_file_and_queues_manifest_when_db_is_down(tmp_path: Path, monkeypatch):
    pending_root = tmp_path / "pending"
    monkeypatch.setenv("GJ_QMT_PENDING_WRITE_ROOT", str(pending_root))
    payload = {"ok": True, "rows": [{"stock_code": "000001.SZ", "lastPrice": 10.2}]}

    result = archive_payload(
        _FailingEngine(),
        dataset="stock/tick",
        api_name="get_full_tick",
        params={"stock_codes": ["000001.SZ"]},
        payload=payload,
        period="tick",
        batch_id="batch-db-down",
        provenance={"client_version": "test"},
        raw_root=tmp_path / "raw",
    )

    target = Path(result.file_path)
    assert target.is_file()
    assert result.manifest_persisted is False
    assert result.pending_write_path is not None
    queued = Path(result.pending_write_path)
    assert queued.is_file()
    assert queued.resolve().is_relative_to(pending_root.resolve())

    replay_engine = _Engine()
    replay = replay_pending_writes(replay_engine, pending_root=pending_root)

    assert replay.attempted == 1
    assert replay.succeeded == 1
    assert replay.failed == 0
    assert replay.remaining == 0
    assert not queued.exists()
    assert len(replay_engine.calls) == 1
    assert replay_engine.calls[0][1]["manifest_key"] == result.manifest_key
    assert replay_engine.calls[0][1]["file_path"] == str(target)

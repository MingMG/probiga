from __future__ import annotations

import json
import gzip
from datetime import datetime
from pathlib import Path

from integrations.bigqmt.spool import (
    PROVIDER_ID,
    bridge_paths,
    resolve_big_qmt_home,
    snapshot_frame,
    write_watchlist,
)
from integrations.bigqmt import spool


def _fake_qmt_home(tmp_path: Path) -> Path:
    home = tmp_path / "GuojinQMT"
    (home / "bin.x64").mkdir(parents=True)
    (home / "userdata").mkdir()
    (home / "bin.x64" / "XtItClient.exe").write_bytes(b"")
    return home


def test_resolve_standard_qmt_home_and_bridge_paths(tmp_path, monkeypatch):
    home = _fake_qmt_home(tmp_path)
    monkeypatch.setenv("BIG_QMT_HOME", str(home))

    assert resolve_big_qmt_home() == home.resolve()
    paths = bridge_paths(home)
    assert paths["root"] == home / "userdata" / "probiga_bridge"
    assert "userdata_mini" not in str(paths["root"])


def test_watchlist_does_not_trigger_resubscribe_when_unchanged(tmp_path):
    home = _fake_qmt_home(tmp_path)
    path = write_watchlist(
        all_codes=["000001", "600000", "900901"],
        tracked_codes=["000001.SZ"],
        qmt_home=home,
    )
    first_mtime = path.stat().st_mtime_ns
    second = write_watchlist(
        all_codes=["000001", "600000", "900901"],
        tracked_codes=["000001.SZ"],
        qmt_home=home,
    )

    assert second.stat().st_mtime_ns == first_mtime
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["all_codes"] == ["000001.SZ", "600000.SH"]


def test_snapshot_frame_normalizes_qmt_tick_schema():
    payload = {
        "batch_id": "batch-1",
        "quotes": {
            "000001.SZ": {
                "time": 1784602800000,
                "_probiga_received_at": "2026-07-21 10:00:01",
                "lastPrice": 12.5,
                "lastClose": 12.0,
                "volume": 1234,
                "amount": 56789,
                "bidPrice": [12.49, 12.48, 12.47, 12.46, 12.45],
                "bidVol": [1200, 1100, 1000, 900, 800],
                "askPrice": [12.51, 12.52, 12.53, 12.54, 12.55],
                "askVol": [1300, 1200, 1100, 1000, 900],
            },
            "600000.SH": {
                "timetag": "20260721 10:00:00",
                "lastPrice": 0,
                "lastClose": 8.0,
                "volume": 0,
                "amount": 0,
            },
        },
    }
    frame = snapshot_frame(
        payload,
        short_name_map={"000001": "平安银行"},
        received_at=datetime(2026, 7, 21, 10, 1, 0),
    ).set_index("stock_code")

    assert list(frame.index) == ["000001", "600000"]
    assert frame.loc["000001", "short_name"] == "平安银行"
    assert frame.loc["000001", "change"] == 0.5
    assert round(frame.loc["000001", "change_pct"], 6) == round(100 / 24, 6)
    assert frame.loc["600000", "price"] == 8.0
    assert frame.loc["000001", "data_source"] == PROVIDER_ID
    assert frame.loc["000001", "qmt_code"] == "000001.SZ"
    assert frame.loc["000001", "bid1"] == 12.49
    assert frame.loc["000001", "bid1_volume"] == 1200
    assert frame.loc["000001", "ask1"] == 12.51
    assert frame.loc["000001", "ask1_volume"] == 1300
    assert frame.loc["000001", "received_at"] == "2026-07-21 10:00:01"
    assert frame.loc["000001", "data_version"] == "bigqmt_inner_v2"


def test_gzip_response_reader_retries_transient_windows_sharing_violation(tmp_path, monkeypatch):
    path = tmp_path / "response.json.gz"
    real_open = gzip.open
    with real_open(path, "wt", encoding="utf-8") as handle:
        json.dump({"status": "ok"}, handle)

    attempts = 0

    def flaky_open(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise PermissionError(13, "sharing violation", str(path))
        return real_open(*args, **kwargs)

    monkeypatch.setattr(spool.gzip, "open", flaky_open)

    assert spool._read_gzip_json(path, retry_seconds=0.2, retry_interval=0.01) == {
        "status": "ok"
    }
    assert attempts == 2


def test_atomic_json_writer_retries_transient_windows_sharing_violation(tmp_path, monkeypatch):
    path = tmp_path / "watchlist.json"
    real_replace = spool.os.replace
    attempts = 0

    def flaky_replace(source, destination):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise PermissionError(13, "sharing violation", str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr(spool.os, "replace", flaky_replace)

    spool._atomic_json_write(path, {"status": "ok"})

    assert json.loads(path.read_text(encoding="utf-8")) == {"status": "ok"}
    assert attempts == 2


def test_json_reader_retries_transient_windows_sharing_violation(tmp_path, monkeypatch):
    path = tmp_path / "tracked.json"
    path.write_text('{"version":1}', encoding="utf-8")
    real_open = Path.open
    attempts = 0

    def flaky_open(self, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if self == path and attempts == 1:
            raise PermissionError(13, "sharing violation", str(path))
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", flaky_open)

    assert spool._read_json_file(path, retry_seconds=0.2, retry_interval=0.01) == {"version": 1}
    assert attempts == 2


def test_atomic_json_writer_retries_transient_windows_sharing_violation(tmp_path, monkeypatch):
    path = tmp_path / "watchlist.json"
    real_replace = spool.os.replace
    attempts = 0

    def flaky_replace(source, destination):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise PermissionError(13, "sharing violation", str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr(spool.os, "replace", flaky_replace)

    spool._atomic_json_write(path, {"status": "ok"})

    assert json.loads(path.read_text(encoding="utf-8")) == {"status": "ok"}
    assert attempts == 2

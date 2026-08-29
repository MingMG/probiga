from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text

from server.common.pit_facts import (
    EVENT_REVISION_TABLE,
    SOURCE_COVERAGE_TABLE,
    ensure_pit_fact_schema,
    load_event_facts,
    append_event_revision,
    append_source_coverage,
    resolve_common_fact_cutoff,
)
from server.common.qmt_announcement_pit import (
    AnnouncementCatalog,
    AnnouncementCheckpoint,
    QMTAnnouncementBlocked,
    QMT_ANNOUNCEMENT_SOURCE,
    parse_qmt_announcement_frame,
    parse_qmt_publication_time,
    synchronize_qmt_announcements,
    validate_complete_qmt_announcement_batch,
)
from server.common.qmt_stock_catalog import (
    build_catalog_discovery,
    build_catalog_manifest,
)
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_trade_calendar import (
    build_calendar_manifest,
    calendar_source_batch_id,
)
from tools.sync_qmt_announcement_pit import (
    validate_existing_complete_qmt_announcement_batch,
    validate_existing_task_result,
)


HASH_A = "a" * 64
HASH_B = "b" * 64


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_pit_fact_schema(engine)
    return engine


def _catalog(*codes: tuple[str, str]) -> AnnouncementCatalog:
    mapping = dict(codes)
    return AnnouncementCatalog(
        batch_id="catalog-20260825",
        manifest_hash=HASH_A,
        member_set_hash=HASH_B,
        codes=tuple(sorted(mapping)),
        qmt_by_code={key: mapping[key] for key in sorted(mapping)},
    )


def _frame(code: str, when: str = "2026-08-25 18:10:00"):
    return pd.DataFrame(
        [{
            "time": when,
            "title": f"{code}公告",
            "announcement_id": f"A-{code}",
            "stock_code": code,
        }]
    )


class _Clock:
    def __init__(self, *values: datetime):
        self.values = list(values)
        self.last = values[-1]

    def __call__(self):
        if self.values:
            self.last = self.values.pop(0)
        return self.last


class _XtData:
    def __init__(self, frames, *, missing=(), fail_download=False):
        self.frames = dict(frames)
        self.missing = set(missing)
        self.fail_download = fail_download
        self.downloads = []
        self.reads = []
        self.connected_ports = []

    def connect(self, *, port, remember_if_success):
        assert remember_if_success is False
        self.connected_ports.append(port)
        return None

    def download_history_data(self, code, **kwargs):
        self.downloads.append((code, kwargs))
        if self.fail_download:
            raise PermissionError("VIP announcement unavailable")

    def get_market_data_ex(self, **kwargs):
        self.reads.append(kwargs)
        return {
            code: self.frames[code]
            for code in kwargs["stock_list"]
            if code not in self.missing
        }


def _patch_catalog(monkeypatch, catalog):
    monkeypatch.setattr(
        "server.common.qmt_announcement_pit._load_catalog",
        lambda _engine, _cutoff: catalog,
    )


def test_publication_parser_requires_real_time_and_rejects_date_only():
    assert parse_qmt_publication_time("20260825181030") == datetime(
        2026, 8, 25, 18, 10, 30
    )
    assert parse_qmt_publication_time(1787652630000).tzinfo is None
    with pytest.raises(ValueError, match="exact timestamp"):
        parse_qmt_publication_time("2026-08-25")
    with pytest.raises(ValueError, match="near-midnight date marker"):
        parse_qmt_publication_time("2026-08-25 00:00:00")
    with pytest.raises(ValueError, match="near-midnight date marker"):
        # Official documentation sample.  It decodes to 00:00:15.674 in
        # Shanghai and is a daily marker, not proven publication time.
        parse_qmt_publication_time(1720195215674)


def test_official_chinese_announcement_columns_and_numpy_epoch_are_supported():
    published = datetime(
        2026, 8, 25, 18, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai")
    )
    epoch_ms = int(published.timestamp() * 1000)
    frame = pd.DataFrame(
        [{
            "time": epoch_ms,
            "证券": "600050.SH",
            "主题": "中国联通董事会决议公告",
            "摘要": "",
            "格式": "TXT",
            "内容": "https://static.sse.com.cn/example.txt",
            "级别": 0,
            "类型 0-其他 1-财报类": 0,
        }]
    )
    events = parse_qmt_announcement_frame(
        stock_code="600050",
        qmt_code="600050.SH",
        frame=frame,
        fact_cutoff_at=datetime(2026, 8, 25, 18, 20),
        window_start=datetime(2026, 8, 1).date(),
    )
    assert len(events) == 1
    assert events[0]["title"] == "中国联通董事会决议公告"
    assert events[0]["published_at"] == "2026-08-25T18:10:30.000000"
    assert events[0]["source_fields"]["主题"] == "中国联通董事会决议公告"


def test_future_qmt_publication_blocks_instead_of_being_silently_dropped():
    with pytest.raises(QMTAnnouncementBlocked) as exc:
        parse_qmt_announcement_frame(
            stock_code="000001",
            qmt_code="000001.SZ",
            frame=_frame("000001", "2026-08-25 18:20:01"),
            fact_cutoff_at=datetime(2026, 8, 25, 18, 20),
            window_start=datetime(2026, 8, 1).date(),
        )
    assert exc.value.reason_code == "QMT_ANNOUNCEMENT_FUTURE_PUBLICATION"


def test_official_near_midnight_marker_blocks_batch_and_publishes_nothing(
    monkeypatch, tmp_path
):
    engine = _engine()
    catalog = _catalog(("600050", "600050.SH"))
    _patch_catalog(monkeypatch, catalog)
    frame = pd.DataFrame(
        [{
            "time": 1720195215674,
            "证券": "600050.SH",
            "主题": "官方样例中的日期标记不得伪装成精确发布时间",
            "格式": "TXT",
            "内容": "https://static.sse.com.cn/example.txt",
        }]
    )
    result = synchronize_qmt_announcements(
        engine,
        xtdata=_XtData({"600050.SH": frame}),
        checkpoint_root=tmp_path,
        now_fn=_Clock(
            datetime(2026, 8, 25, 18, 20),
            datetime(2026, 8, 25, 18, 21),
        ),
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["reason_code"] == "QMT_ANNOUNCEMENT_ROW_INVALID"
    with engine.connect() as connection:
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}")
        ).scalar_one() == 0
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {EVENT_REVISION_TABLE}")
        ).scalar_one() == 0


def test_full_catalog_batch_writes_events_and_authoritative_empty_for_beijing(
    monkeypatch, tmp_path: Path
):
    engine = _engine()
    catalog = _catalog(("000001", "000001.SZ"), ("430001", "430001.BJ"))
    _patch_catalog(monkeypatch, catalog)
    xtdata = _XtData(
        {
            "000001.SZ": _frame("000001"),
            "430001.BJ": pd.DataFrame(
                columns=["time", "title", "announcement_id", "stock_code"]
            ),
        }
    )
    result = synchronize_qmt_announcements(
        engine,
        xtdata=xtdata,
        checkpoint_root=tmp_path,
        now_fn=_Clock(
            datetime(2026, 8, 25, 18, 20),
            datetime(2026, 8, 25, 18, 25),
        ),
        batch_size=2,
    )
    assert result["status"] == "COMPLETE"
    assert result["stock_count"] == result["coverage_count"] == 2
    assert result["event_count"] == 1
    assert result["empty_stock_count"] == 1
    with engine.connect() as connection:
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {EVENT_REVISION_TABLE}")
        ).scalar_one() == 1
        rows = connection.execute(
            text(
                f"SELECT stock_code, result_count, source, batch_id "
                f"FROM {SOURCE_COVERAGE_TABLE} ORDER BY stock_code"
            )
        ).mappings().all()
    assert [row["stock_code"] for row in rows] == ["000001", "430001"]
    assert [row["result_count"] for row in rows] == [1, 0]
    assert {row["source"] for row in rows} == {QMT_ANNOUNCEMENT_SOURCE}
    assert len({row["batch_id"] for row in rows}) == 1

    second = synchronize_qmt_announcements(
        engine,
        xtdata=xtdata,
        checkpoint_root=tmp_path,
        now_fn=_Clock(
            datetime(2026, 8, 25, 18, 30),
            datetime(2026, 8, 25, 18, 35),
        ),
        batch_size=2,
    )
    assert second["status"] == "COMPLETE"
    replay = load_event_facts(
        engine,
        codes=["430001"],
        decision_at="2026-08-25 18:35:00",
        fact_cutoff_at="2026-08-25 18:30:00",
        start_date="2026-08-01",
        end_date="2026-08-25",
    )
    assert replay.status_for("430001") == "AVAILABLE"
    assert replay.facts["430001"] == []


def test_missing_one_catalog_stock_publishes_nothing(monkeypatch, tmp_path):
    monkeypatch.delenv("QMT_PORT", raising=False)
    engine = _engine()
    catalog = _catalog(("000001", "000001.SZ"), ("600000", "600000.SH"))
    _patch_catalog(monkeypatch, catalog)
    xtdata = _XtData(
        {"000001.SZ": _frame("000001"), "600000.SH": _frame("600000")},
        missing={"600000.SH"},
    )
    result = synchronize_qmt_announcements(
        engine,
        xtdata=xtdata,
        checkpoint_root=tmp_path,
        now_fn=_Clock(
            datetime(2026, 8, 25, 18, 20),
            datetime(2026, 8, 25, 18, 21),
        ),
        batch_size=2,
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["reason_code"] == "QMT_ANNOUNCEMENT_RESPONSE_MISSING_STOCK"
    assert xtdata.connected_ports == [58610]
    with engine.connect() as connection:
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}")
        ).scalar_one() == 0
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {EVENT_REVISION_TABLE}")
        ).scalar_one() == 0


def test_restart_resumes_only_same_t_checkpoint_and_skips_completed_codes(
    monkeypatch, tmp_path
):
    engine = _engine()
    catalog = _catalog(("000001", "000001.SZ"), ("600000", "600000.SH"))
    _patch_catalog(monkeypatch, catalog)
    xtdata = _XtData(
        {"000001.SZ": _frame("000001"), "600000.SH": _frame("600000")},
        missing={"600000.SH"},
    )
    first = synchronize_qmt_announcements(
        engine,
        xtdata=xtdata,
        checkpoint_root=tmp_path,
        now_fn=_Clock(
            datetime(2026, 8, 25, 18, 20),
            datetime(2026, 8, 25, 18, 21),
        ),
        batch_size=1,
    )
    assert first["status"] == "DATA_BLOCKED"
    assert [item[0] for item in xtdata.downloads] == [
        "000001.SZ", "600000.SH"
    ]

    xtdata.missing.clear()
    xtdata.downloads.clear()
    second = synchronize_qmt_announcements(
        engine,
        xtdata=xtdata,
        checkpoint_root=tmp_path,
        now_fn=_Clock(
            datetime(2026, 8, 25, 18, 22),
            datetime(2026, 8, 25, 18, 25),
        ),
        batch_size=1,
    )
    assert second["status"] == "COMPLETE"
    assert second["fact_cutoff_at"] == "2026-08-25T18:20:00.000000"
    assert [item[0] for item in xtdata.downloads] == ["600000.SH"]


def test_retry_after_complete_marker_failure_reuses_frozen_e_and_is_idempotent(
    monkeypatch, tmp_path
):
    engine = _engine()
    catalog = _catalog(("000001", "000001.SZ"), ("600000", "600000.SH"))
    _patch_catalog(monkeypatch, catalog)
    xtdata = _XtData(
        {"000001.SZ": _frame("000001"), "600000.SH": _frame("600000")}
    )
    original = AnnouncementCheckpoint.mark_complete
    attempts = 0

    def flaky_marker(self, *, batch_root_hash, received_at):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("checkpoint volume unavailable")
        return original(
            self,
            batch_root_hash=batch_root_hash,
            received_at=received_at,
        )

    monkeypatch.setattr(AnnouncementCheckpoint, "mark_complete", flaky_marker)
    first = synchronize_qmt_announcements(
        engine,
        xtdata=xtdata,
        checkpoint_root=tmp_path,
        now_fn=_Clock(
            datetime(2026, 8, 25, 18, 20),
            datetime(2026, 8, 25, 18, 25),
        ),
        batch_size=2,
    )
    assert first["status"] == "COMPLETE"
    xtdata.downloads.clear()

    second = synchronize_qmt_announcements(
        engine,
        xtdata=xtdata,
        checkpoint_root=tmp_path,
        now_fn=_Clock(datetime(2026, 8, 25, 18, 26)),
        batch_size=2,
    )
    assert second["status"] == "COMPLETE"
    assert second["fact_cutoff_at"] == first["fact_cutoff_at"]
    assert second["received_at"] == first["received_at"]
    assert second["batch_root_hash"] == first["batch_root_hash"]
    assert xtdata.downloads == []
    with engine.connect() as connection:
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}")
        ).scalar_one() == 2
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {EVENT_REVISION_TABLE}")
        ).scalar_one() == 2


def test_qmt_no_permission_is_explicit_data_blocked_without_eastmoney_fallback(
    monkeypatch, tmp_path
):
    engine = _engine()
    catalog = _catalog(("000001", "000001.SZ"))
    _patch_catalog(monkeypatch, catalog)
    result = synchronize_qmt_announcements(
        engine,
        xtdata=_XtData({"000001.SZ": _frame("000001")}, fail_download=True),
        checkpoint_root=tmp_path,
        now_fn=_Clock(
            datetime(2026, 8, 25, 18, 20),
            datetime(2026, 8, 25, 18, 20, 1),
        ),
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["reason_code"] == (
        "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED"
    )
    assert result["coverage_count"] == 0


def test_capture_over_30_minutes_keeps_staged_checkpoint_but_no_database_batch(
    monkeypatch, tmp_path
):
    engine = _engine()
    catalog = _catalog(("000001", "000001.SZ"))
    _patch_catalog(monkeypatch, catalog)
    result = synchronize_qmt_announcements(
        engine,
        xtdata=_XtData({"000001.SZ": _frame("000001")}),
        checkpoint_root=tmp_path,
        now_fn=_Clock(
            datetime(2026, 8, 25, 18, 20),
            datetime(2026, 8, 25, 18, 50, 1),
            datetime(2026, 8, 25, 18, 50, 1),
        ),
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["reason_code"] == (
        "QMT_ANNOUNCEMENT_CAPTURE_EXCEEDED_30_MINUTES"
    )
    with engine.connect() as connection:
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}")
        ).scalar_one() == 0


def test_checkpoint_refuses_mixed_fact_cutoff_or_catalog(tmp_path):
    catalog = _catalog(("000001", "000001.SZ"))
    first = AnnouncementCheckpoint.open(
        tmp_path,
        batch_id="qmt-ann-fixed",
        fact_cutoff_at=datetime(2026, 8, 25, 18, 20),
        window_start=datetime(2026, 7, 26).date(),
        window_end=datetime(2026, 8, 25).date(),
        catalog=catalog,
        resume=True,
    )
    first.save("000001", [])
    with pytest.raises(QMTAnnouncementBlocked) as exc:
        AnnouncementCheckpoint.open(
            tmp_path,
            batch_id="qmt-ann-fixed",
            fact_cutoff_at=datetime(2026, 8, 25, 18, 21),
            window_start=datetime(2026, 7, 26).date(),
            window_end=datetime(2026, 8, 25).date(),
            catalog=catalog,
            resume=True,
        )
    assert exc.value.reason_code == (
        "QMT_ANNOUNCEMENT_CHECKPOINT_MIXED_CUTOFF_OR_CATALOG"
    )


def test_checkpoint_result_hash_prevents_cross_batch_file_splice(tmp_path):
    catalog = _catalog(("000001", "000001.SZ"))
    checkpoint = AnnouncementCheckpoint.open(
        tmp_path,
        batch_id="qmt-ann-fixed",
        fact_cutoff_at=datetime(2026, 8, 25, 18, 20),
        window_start=datetime(2026, 7, 26).date(),
        window_end=datetime(2026, 8, 25).date(),
        catalog=catalog,
        resume=True,
    )
    checkpoint.save("000001", [])
    path = checkpoint.root / "results" / "000001.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["fact_cutoff_at"] = "2026-08-25T18:21:00.000000"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(QMTAnnouncementBlocked) as exc:
        checkpoint.load("000001")
    assert exc.value.reason_code == (
        "QMT_ANNOUNCEMENT_CHECKPOINT_RESULT_MIXED_OR_TAMPERED"
    )


def _install_catalog(
    engine,
    catalog: AnnouncementCatalog,
    *,
    captured_at: str = "2026-08-25 18:00:00",
):
    members = [
        {
            "qmt_code": qmt_code,
            "stock_code": stock_code,
            "list_date": "2020-01-01",
            "expire_date": None,
            "instrument_batch_id": "instrument-proof",
            "instrument_type": "STOCK",
        }
        for stock_code, qmt_code in catalog.qmt_by_code.items()
    ]
    sector_by_suffix = {"SH": "上证A股", "SZ": "深证A股", "BJ": "京市A股"}
    discovery = build_catalog_discovery(
        current_sectors=("上证A股", "深证A股", "京市A股"),
        expired_sectors=(),
        sector_members=[
            {
                "sector_name": sector_by_suffix[qmt_code[-2:]],
                "qmt_code": qmt_code,
            }
            for qmt_code in catalog.qmt_by_code.values()
        ],
    )
    manifest, normalized = build_catalog_manifest(
        batch_id=catalog.batch_id,
        captured_at=captured_at,
        history_complete_from="2020-01-01",
        members=members,
        discovery=discovery,
    )
    # Use the real validated hashes instead of the simple fake fixture hashes.
    catalog = AnnouncementCatalog(
        batch_id=catalog.batch_id,
        manifest_hash=canonical_digest(manifest),
        member_set_hash=manifest["member_set_hash"],
        codes=catalog.codes,
        qmt_by_code=catalog.qmt_by_code,
    )
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_stock_catalog_batch (
                batch_id TEXT PRIMARY KEY, captured_at DATETIME NOT NULL,
                history_complete_from DATE NOT NULL, status TEXT NOT NULL,
                member_count INTEGER NOT NULL, member_set_hash TEXT NOT NULL,
                manifest_json TEXT NOT NULL, manifest_hash TEXT NOT NULL,
                created_at DATETIME NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_stock_catalog_member (
                batch_id TEXT NOT NULL, qmt_code TEXT NOT NULL,
                stock_code TEXT NOT NULL, list_date DATE NOT NULL,
                expire_date DATE NULL, instrument_batch_id TEXT NOT NULL,
                instrument_type TEXT NOT NULL, created_at DATETIME NOT NULL,
                PRIMARY KEY (batch_id, qmt_code)
            )
        """))
        connection.execute(
            text("""
                INSERT INTO qmt_stock_catalog_batch
                (batch_id,captured_at,history_complete_from,status,member_count,
                 member_set_hash,manifest_json,manifest_hash,created_at)
                VALUES (:batch_id,:captured_at,:history_complete_from,'COMPLETE',
                        :member_count,:member_set_hash,:manifest_json,
                        :manifest_hash,:captured_at)
            """),
            {
                "batch_id": catalog.batch_id,
                "captured_at": captured_at,
                "history_complete_from": "2020-01-01",
                "member_count": len(normalized),
                "member_set_hash": catalog.member_set_hash,
                "manifest_json": json.dumps(
                    manifest, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"),
                ),
                "manifest_hash": catalog.manifest_hash,
            },
        )
        connection.execute(
            text("""
                INSERT INTO qmt_stock_catalog_member
                (batch_id,qmt_code,stock_code,list_date,expire_date,
                 instrument_batch_id,instrument_type,created_at)
                VALUES (:batch_id,:qmt_code,:stock_code,:list_date,:expire_date,
                        :instrument_batch_id,:instrument_type,
                        :created_at)
            """),
            [
                {
                    "batch_id": catalog.batch_id,
                    "created_at": captured_at,
                    **member,
                }
                for member in normalized
            ],
        )
    return catalog


def _install_authoritative_calendar(
    engine,
    *,
    target_date: str = "2026-08-25",
    start_date: str = "2026-07-26",
):
    source_batch_id = calendar_source_batch_id(
        start_date=start_date,
        end_date=target_date,
        sessions=[target_date],
    )
    manifest, sessions = build_calendar_manifest(
        batch_id=f"calendar-{target_date.replace('-', '')}",
        source_batch_id=source_batch_id,
        known_at=f"{target_date} 18:00:00",
        start_date=start_date,
        end_date=target_date,
        sessions=[target_date],
    )
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE si_trade_calendar (
                trade_date DATE PRIMARY KEY, trade_status INTEGER NOT NULL
            )
        """))
        connection.execute(
            text(
                "INSERT INTO si_trade_calendar (trade_date, trade_status) "
                "VALUES (:trade_date, 1)"
            ),
            {"trade_date": target_date},
        )
        connection.execute(text("""
            CREATE TABLE qmt_trade_calendar_batch (
                batch_id TEXT PRIMARY KEY, source_batch_id TEXT NOT NULL,
                known_at DATETIME NOT NULL, start_date DATE NOT NULL,
                end_date DATE NOT NULL, status TEXT NOT NULL,
                session_count INTEGER NOT NULL, session_set_hash TEXT NOT NULL,
                manifest_json TEXT NOT NULL, manifest_hash TEXT NOT NULL,
                created_at DATETIME NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_trade_calendar_session (
                batch_id TEXT NOT NULL, trade_date DATE NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (batch_id, trade_date)
            )
        """))
        connection.execute(
            text("""
                INSERT INTO qmt_trade_calendar_batch
                (batch_id,source_batch_id,known_at,start_date,end_date,status,
                 session_count,session_set_hash,manifest_json,manifest_hash,
                 created_at)
                VALUES
                (:batch_id,:source_batch_id,:known_at,:start_date,:end_date,
                 'COMPLETE',:session_count,:session_set_hash,:manifest_json,
                 :manifest_hash,:known_at)
            """),
            {
                **manifest,
                "manifest_json": json.dumps(
                    manifest, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"),
                ),
                "manifest_hash": canonical_digest(manifest),
            },
        )
        connection.execute(
            text("""
                INSERT INTO qmt_trade_calendar_session
                (batch_id,trade_date,created_at)
                VALUES (:batch_id,:trade_date,:created_at)
            """),
            [
                {
                    "batch_id": manifest["batch_id"],
                    "trade_date": session,
                    "created_at": manifest["known_at"],
                }
                for session in sessions
            ],
        )
    return manifest


def test_database_validator_proves_global_root_and_exact_catalog_binding():
    from server.common.qmt_announcement_pit import (
        _publish_batch,
        build_batch_root,
    )

    engine = _engine()
    catalog = _install_catalog(
        engine,
        _catalog(("000001", "000001.SZ"), ("430001", "430001.BJ")),
    )
    cutoff = datetime(2026, 8, 25, 18, 20)
    received = datetime(2026, 8, 25, 18, 25)
    results = {
        "000001": parse_qmt_announcement_frame(
            stock_code="000001", qmt_code="000001.SZ",
            frame=_frame("000001"), fact_cutoff_at=cutoff,
            window_start=datetime(2026, 7, 26).date(),
        ),
        "430001": [],
    }
    batch_id = "qmt-ann-20260825T182000-validator"
    root, entries = build_batch_root(
        batch_id=batch_id,
        fact_cutoff_at=cutoff,
        received_at=received,
        window_start=datetime(2026, 7, 26).date(),
        window_end=datetime(2026, 8, 25).date(),
        catalog=catalog,
        results=results,
    )
    append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "event_key": "eastmoney-display-only",
            "event_date": "2026-08-25",
            "published_at": "2026-08-25 18:05:00",
            "title": "东财轮转子集不得进入策略",
        },
        known_at="2026-08-25 18:06:00",
        source="eastmoney.notice",
        batch_id="eastmoney-display",
    )
    _publish_batch(
        engine,
        batch_id=batch_id,
        batch_root_hash=root,
        entries=entries,
        fact_cutoff_at=cutoff,
        received_at=received,
        window_start=datetime(2026, 7, 26).date(),
        window_end=datetime(2026, 8, 25).date(),
        catalog=catalog,
        results=results,
    )
    proof = validate_complete_qmt_announcement_batch(
        engine,
        codes=["430001"],
        decision_at="2026-08-25 18:30:00",
        fact_cutoff_at="2026-08-25 18:20:00",
        window_start="2026-08-01",
        window_end="2026-08-25",
    )
    assert proof["batch_root_hash"] == root
    assert proof["catalog_member_count"] == 2
    loaded = load_event_facts(
        engine,
        codes=["000001", "430001"],
        decision_at="2026-08-25 18:30:00",
        fact_cutoff_at="2026-08-25 18:20:00",
        start_date="2026-08-01",
        end_date="2026-08-25",
        require_qmt_complete_batch=True,
    )
    assert loaded.status_for("000001") == "AVAILABLE"
    assert loaded.status_for("430001") == "AVAILABLE"
    assert loaded.facts["430001"] == []
    assert [item["title"] for item in loaded.facts["000001"]] == [
        "000001公告"
    ]

    # A catalog row mutation makes its immutable manifest unverifiable and the
    # event batch is rejected instead of silently reinterpreting membership.
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE qmt_stock_catalog_member SET stock_code='430002' "
                "WHERE stock_code='430001'"
            )
        )
    with pytest.raises(QMTAnnouncementBlocked):
        validate_complete_qmt_announcement_batch(
            engine,
            codes=["000001"],
            decision_at="2026-08-25 18:30:00",
            window_start="2026-08-01",
            window_end="2026-08-25",
        )


def _install_complete_announcement_batch(
    engine,
    *,
    target_date: str = "2026-08-25",
    authoritative_target_date: str = "",
):
    from server.common.qmt_announcement_pit import (
        _publish_batch,
        build_batch_root,
    )

    target = datetime.fromisoformat(target_date).date()
    window_start = target - timedelta(days=30)
    catalog = _install_catalog(
        engine,
        _catalog(("000001", "000001.SZ"), ("430001", "430001.BJ")),
        captured_at=f"{target.isoformat()} 18:00:00",
    )
    authoritative_target = datetime.fromisoformat(
        authoritative_target_date or target_date
    ).date()
    _install_authoritative_calendar(
        engine,
        target_date=authoritative_target.isoformat(),
        start_date=(authoritative_target - timedelta(days=30)).isoformat(),
    )
    cutoff = datetime.combine(target, datetime.min.time()).replace(
        hour=18, minute=20
    )
    received = cutoff.replace(minute=25)
    window_end = target
    results = {
        "000001": parse_qmt_announcement_frame(
            stock_code="000001",
            qmt_code="000001.SZ",
            frame=_frame("000001", f"{target.isoformat()} 17:50:00"),
            fact_cutoff_at=cutoff,
            window_start=window_start,
        ),
        "430001": [],
    }
    batch_id = f"qmt-ann-{target.strftime('%Y%m%d')}T182000-existing"
    root, entries = build_batch_root(
        batch_id=batch_id,
        fact_cutoff_at=cutoff,
        received_at=received,
        window_start=window_start,
        window_end=window_end,
        catalog=catalog,
        results=results,
    )
    _publish_batch(
        engine,
        batch_id=batch_id,
        batch_root_hash=root,
        entries=entries,
        fact_cutoff_at=cutoff,
        received_at=received,
        window_start=window_start,
        window_end=window_end,
        catalog=catalog,
        results=results,
    )
    return batch_id, root


def test_deploy_read_only_mode_validates_existing_closed_day_batch():
    engine = _engine()
    batch_id, root = _install_complete_announcement_batch(engine)
    with engine.connect() as connection:
        before = connection.execute(
            text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}")
        ).scalar_one()

    observed_statements = []

    def observe(_conn, _cursor, statement, _params, _context, _many):
        observed_statements.append(str(statement).strip())

    event.listen(engine, "before_cursor_execute", observe)
    try:
        proof = validate_existing_complete_qmt_announcement_batch(
            engine,
            window_days=30,
            now=datetime(2026, 8, 25, 18, 30),
            expected_trade_date="2026-08-25",
        )
    finally:
        event.remove(engine, "before_cursor_execute", observe)

    with engine.connect() as connection:
        after = connection.execute(
            text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}")
        ).scalar_one()
    assert before == after == 2
    assert observed_statements
    assert all(
        statement.upper().startswith("SELECT")
        for statement in observed_statements
    )
    assert proof["status"] == "COMPLETE"
    assert proof["mode"] == "validate-existing-complete-batch"
    assert proof["trade_date"] == "2026-08-25"
    assert proof["window_start"] == "2026-07-26"
    assert proof["window_end"] == "2026-08-25"
    assert proof["batch_id"] == batch_id
    assert proof["batch_root_hash"] == root
    assert proof["source"] == "qmt.announcement"
    assert proof["funding_eligible"] is True
    assert proof["stock_count"] == proof["coverage_count"] == 2
    assert proof["database_writes"] is False
    assert proof["automatic_real_order_submission"] is False
    assert proof["real_order_authority"] is False
    assert validate_existing_task_result(
        proof, 0, expected_trade_date="2026-08-25"
    ) == "complete"
    with pytest.raises(ValueError, match="read-only result mode differs"):
        validate_existing_task_result(
            {**proof, "database_writes": True},
            0,
            expected_trade_date="2026-08-25",
        )
    with pytest.raises(ValueError, match="COMPLETE result differs"):
        validate_existing_task_result(
            {**proof, "stock_count": True, "coverage_count": True},
            0,
            expected_trade_date="2026-08-25",
        )
    with pytest.raises(ValueError, match="COMPLETE result differs"):
        validate_existing_task_result(
            {**proof, "calendar_batch_id": 20260825},
            0,
            expected_trade_date="2026-08-25",
        )


def test_deploy_read_only_mode_rejects_batch_from_previous_date():
    engine = _engine()
    _install_complete_announcement_batch(
        engine,
        target_date="2026-08-24",
        authoritative_target_date="2026-08-25",
    )

    with pytest.raises(QMTAnnouncementBlocked) as exc:
        validate_existing_complete_qmt_announcement_batch(
            engine,
            window_days=30,
            now=datetime(2026, 8, 25, 18, 30),
            expected_trade_date="2026-08-25",
        )
    assert exc.value.reason_code == (
        "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND"
    )


def test_deploy_read_only_mode_maps_weekend_to_frozen_friday():
    engine = _engine()
    batch_id, _ = _install_complete_announcement_batch(
        engine, target_date="2026-08-28"
    )

    proof = validate_existing_complete_qmt_announcement_batch(
        engine,
        window_days=30,
        now=datetime(2026, 8, 29, 9, 30),
        expected_trade_date="2026-08-28",
    )

    assert proof["trade_date"] == "2026-08-28"
    assert proof["batch_id"] == batch_id
    assert proof["database_writes"] is False


def test_second_daily_qmt_batch_validates_the_complete_coverage_chain():
    from server.common.qmt_announcement_pit import (
        _publish_batch,
        build_batch_root,
    )

    engine = _engine()
    catalog = _install_catalog(
        engine,
        _catalog(("000001", "000001.SZ")),
    )
    window_start = datetime(2026, 7, 26).date()
    window_end = datetime(2026, 8, 25).date()
    results = {
        "000001": parse_qmt_announcement_frame(
            stock_code="000001",
            qmt_code="000001.SZ",
            frame=_frame("000001", "2026-08-25 17:50:00"),
            fact_cutoff_at=datetime(2026, 8, 25, 18, 0),
            window_start=window_start,
        )
    }
    for suffix, cutoff, received in (
        ("first", datetime(2026, 8, 25, 18, 0), datetime(2026, 8, 25, 18, 5)),
        ("second", datetime(2026, 8, 25, 18, 20), datetime(2026, 8, 25, 18, 25)),
    ):
        batch_id = f"qmt-ann-20260825-{suffix}"
        root, entries = build_batch_root(
            batch_id=batch_id,
            fact_cutoff_at=cutoff,
            received_at=received,
            window_start=window_start,
            window_end=window_end,
            catalog=catalog,
            results=results,
        )
        _publish_batch(
            engine,
            batch_id=batch_id,
            batch_root_hash=root,
            entries=entries,
            fact_cutoff_at=cutoff,
            received_at=received,
            window_start=window_start,
            window_end=window_end,
            catalog=catalog,
            results=results,
        )
    append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code="000001",
        window_start="1900-01-01",
        window_end="2026-08-25",
        known_at="2026-08-25 18:24:00",
        covered_through_at="2026-08-25 18:24:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"provider": "test.finance"},
        source_rows=[],
        fact_bindings=[],
        source="test.finance",
        batch_id="finance-20260825",
    )

    resolved = resolve_common_fact_cutoff(
        engine,
        codes=["000001"],
        decision_at="2026-08-25 18:30:00",
        finance_start_date="1900-01-01",
        finance_end_date="2026-08-25",
        event_start_date="2026-08-11",
        event_end_date="2026-08-25",
        require_qmt_event_batch=True,
    )
    assert resolved["status"] == "AVAILABLE"
    event_receipt = next(
        item for item in resolved["receipts"]
        if item["fact_kind"] == "event"
    )
    assert event_receipt["batch_id"] == "qmt-ann-20260825-second"

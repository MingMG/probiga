import importlib
import json
import sys
from collections import Counter
from contextlib import contextmanager
from unittest.mock import patch

import pandas as pd
import pytest
import requests
from sqlalchemy import create_engine, text

from biz.stock_info import ths_members as members


def response(index, total, codes, limit=15, order="d"):
    payload = {"block": {"subcodeCount": total}, "items": [{"5": code, "55": "股票"} for code in codes]}
    return f"quotebridge_v2_blockrank_{index}_8_{order}{limit}({json.dumps(payload)});"


def collection(index="885001", codes=("600000",)):
    return members.collect_members(index, as_of="2026-09-11", fetch=lambda _: response(index, len(codes), codes))


def test_cli_opens_shared_tls_engine_only_when_run():
    sys.modules.pop("tools.sync_concept_ths", None)
    with patch("server.common.batch_db.create_batch_engine") as factory:
        module = importlib.import_module("tools.sync_concept_ths")
        factory.assert_not_called()
        with patch.object(module, "run_sync", return_value={"status": "PARTIAL"}):
            assert module.main() == 2
        factory.assert_called_once_with()
        factory.return_value.dispose.assert_called_once()


@pytest.mark.parametrize("body,match", [
    (response("885002", 1, ["600000"]), "identity differs"),
    (response("885001", 2, ["600000"]), "identity differs"),
    (response("885001", 2, ["600000", "600000"]), "identity invalid"),
    (response("885001", 1, ["60000"]), "identity invalid"),
    ("<html>login</html>", "identity differs"),
])
def test_native_incomplete_or_wrong_identity_never_returns_publishable_collection(body, match):
    with pytest.raises(RuntimeError, match=match):
        members.collect_members("885001", as_of="2026-09-11", fetch=lambda _: body)


def test_native_requests_exact_rounded_limit_and_refuses_changing_total():
    calls = []
    codes = [f"600{i:03d}" for i in range(16)]
    def fetch(url):
        calls.append(url)
        return response("885001", 16, codes[:15]) if len(calls) == 1 else response("885001", 16, codes, limit=30)
    assert len(members.collect_members("885001", as_of="2026-09-11", fetch=fetch)["members"]) == 16
    assert calls[-1].endswith("/d30.js")
    replies = iter([response("885001", 16, codes[:15]), response("885001", 17, codes, limit=30)])
    with pytest.raises(RuntimeError, match="total changed"):
        members.collect_members("885001", as_of="2026-09-11", fetch=lambda _: next(replies))


@pytest.mark.parametrize("day,total,codes,error", [
    ("2026-09-11", 2, ["600000", "600001"], None),
    ("2026-09-10", 2, ["600000", "600001"], "date differs"),
    ("2026-09-11", 3, ["600000", "600001"], "totals disagree"),
    ("2026-09-11", 2, ["600000"], "incomplete"),
])
def test_native_directory_resolves_non_quoted_members_only_with_matching_date_and_total(day, total, codes, error):
    data = {"result": {"report": day, "listdata": {day: [[code, "XD股票", "", 0, 0, 0, 0, 0] for code in codes]}}}
    html = f'<title>概念(885001) 最新动态_F10_同花顺金融服务网</title><p>概念股数量：{total}家</p><div id="concept_data">{json.dumps(data)}</div>'
    replies = iter([response("885001", 2, ["600000"]), html])
    if error:
        with pytest.raises(RuntimeError, match=error):
            members.collect_members("885001", as_of="2026-09-11", fetch=lambda _: next(replies))
    else:
        collected = members.collect_members("885001", as_of="2026-09-11", fetch=lambda _: next(replies))["members"]
        assert collected == [{"stock_code": "600000", "short_name": "股票"}, {"stock_code": "600001", "short_name": "XD股票"}]


@pytest.fixture
def member_db(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE si_trade_calendar (trade_date DATE,trade_status INTEGER)"))
        conn.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-09-11',1)"))
        conn.execute(text("CREATE TABLE si_concept_constituent_ths (query_type TEXT,query_key TEXT,stock_code TEXT,short_name TEXT,etl_sync_at DATETIME)"))
        for kind, key in [("index_code", "885001"), ("concept_code", "300001"), ("index_code", "885002")]:
            conn.execute(text("INSERT INTO si_concept_constituent_ths VALUES (:kind,:key,'600999','old','2026-08-11')"), {"kind": kind, "key": key})
    @contextmanager
    def lock(engine, name, **kwargs):
        with engine.connect() as conn:
            yield conn
    monkeypatch.setattr(members, "mysql_named_lock", lock)
    yield engine
    engine.dispose()


def test_verified_partition_replaces_its_alias_and_preserves_other_partition(member_db):
    assert members.publish_members(member_db, collection(), concept_code="300001") == 1
    with member_db.connect() as conn:
        rows = list(conn.execute(text("SELECT query_type,query_key,stock_code FROM si_concept_constituent_ths ORDER BY query_key")))
    assert rows == [("index_code", "885001", "600000"), ("index_code", "885002", "600999")]


def test_insert_failure_rolls_back_partition_and_alias_deletion(member_db, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("database unavailable")
    monkeypatch.setattr(pd.DataFrame, "to_sql", fail)
    with pytest.raises(RuntimeError, match="database unavailable"):
        members.publish_members(member_db, collection(), concept_code="300001")
    with member_db.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM si_concept_constituent_ths WHERE stock_code='600999'")).scalar() == 3


def test_incomplete_partition_does_not_block_other_verified_partitions(member_db, monkeypatch):
    complete = collection()
    def collect(index, **kwargs):
        if index == "885002":
            raise RuntimeError("THS native members incomplete: 885002")
        return complete
    monkeypatch.setattr(members, "collect_members", collect)
    catalog = pd.DataFrame([{"index_code": "885001", "concept_code": "300001"}, {"index_code": "885002", "concept_code": "300002"}])
    result = members.sync_member_partitions(member_db, catalog)
    assert result["status"] == "PARTIAL"
    assert result["completed_indices"] == ["885001"]
    assert result["failed_indices"][0]["index_code"] == "885002"
    digest = result.pop("result_sha256")
    assert digest == members.result_hash(result)
    with member_db.connect() as conn:
        assert conn.execute(text("SELECT etl_sync_at FROM si_concept_constituent_ths WHERE query_key='885002'")).scalar() == "2026-08-11"


def test_authoritative_empty_partition_removes_old_members_without_synthetic_rows(member_db):
    assert members.publish_members(member_db, collection(codes=()), concept_code="300001") == 0
    with member_db.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM si_concept_constituent_ths WHERE query_key IN ('885001','300001')")).scalar() == 0


def _gateway_failure():
    response = requests.Response()
    response.status_code = 502
    try:
        raise requests.HTTPError(response=response)
    except requests.HTTPError as cause:
        raise RuntimeError("THS public request failed: status=502") from cause


def test_retry_only_failed_network_partitions_without_republishing_success(member_db, monkeypatch):
    results = {index: collection(index, (code,)) for index, code in
               [("885001", "600001"), ("885002", "600002")]}
    calls, publishes, sleeps = Counter(), [], []
    publisher = members.publish_members
    def collect(index, **kwargs):
        calls[index] += 1
        if index == "885003":
            raise RuntimeError("THS native members incomplete: 885003")
        if index == "885002" and calls[index] == 1:
            _gateway_failure()
        return results[index]
    def publish(engine, payload, **kwargs):
        publishes.append(payload["index_code"])
        return publisher(engine, payload, **kwargs)
    monkeypatch.setattr(members, "collect_members", collect)
    monkeypatch.setattr(members, "publish_members", publish)
    monkeypatch.setattr(members.time, "sleep", sleeps.append)
    catalog = pd.DataFrame([{"index_code": f"88500{i}", "concept_code": f"30000{i}"} for i in (1, 2, 3)])
    result = members.sync_member_partitions(member_db, catalog)
    assert calls == {"885001": 1, "885002": 2, "885003": 1}
    assert Counter(publishes) == {"885001": 1, "885002": 1}
    assert sleeps == [5]
    assert result["completed_indices"] == ["885001", "885002"]
    assert result["failed_indices"] == [{"index_code": "885003", "error": "THS native members incomplete: 885003"}]
    assert result["rows_written"] == 2
    with member_db.connect() as conn:
        assert conn.execute(text("SELECT stock_code FROM si_concept_constituent_ths WHERE query_key='885002'")).scalar() == "600002"


def test_persistent_gateway_failure_is_bounded_and_preserves_old_partition(member_db, monkeypatch):
    calls, sleeps = [], []
    def collect(index, **kwargs):
        calls.append(index)
        _gateway_failure()
    monkeypatch.setattr(members, "collect_members", collect)
    monkeypatch.setattr(members.time, "sleep", sleeps.append)
    result = members.sync_member_partitions(member_db, pd.DataFrame([
        {"index_code": "885002", "concept_code": "300002"},
    ]))
    assert calls == ["885002"] * 3
    assert sleeps == [5, 10]
    assert result["status"] == "PARTIAL"
    assert result["rows_written"] == 0
    with member_db.connect() as conn:
        assert conn.execute(text("SELECT stock_code,etl_sync_at FROM si_concept_constituent_ths WHERE query_key='885002'")).one() == ("600999", "2026-08-11")

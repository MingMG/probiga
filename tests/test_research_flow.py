from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import json
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import IntegrityError

from biz.review import market_decision, quant_digest, research_flow
from server.common import batch_db


REVIEW = "2026-08-28"
NEXT = "2026-08-31"


def _digest():
    return {
        "review_date": REVIEW, "adjust_type": 0, "publish_status": "ready",
        "compact_review": "冻结的盘后复盘",
        "quality_json": {
            "target_date": REVIEW, "errors": [],
            "source_dates": {"target_bars": REVIEW, "previous_bars": "2026-08-27"},
        },
        "market_structure_json": {"sample_count": 5200},
        "industry_rotation_json": {},
        "factor_validation_json": {"selection_review": {"content": "已有策略审计"}},
        "data_cutoff_at": f"{REVIEW}T15:45:00",
        "generated_at": f"{REVIEW}T16:00:00+08:00",
    }


def _with_parent():
    digest = _digest()
    parent = {
        "status": "COMPLETE", "report_kind": "review", "review_date": REVIEW,
        "next_trade_date": NEXT, "content": digest["compact_review"],
        "generated_at": digest["generated_at"],
        "source_digest_sha256": market_decision.source_digest_identity(digest),
    }
    parent["research_sha256"] = research_flow.artifact_hash(parent)
    digest["factor_validation_json"]["market_decision"] = parent
    return digest


def _briefing(parent, *, generated_at=f"{NEXT}T08:00:00+08:00"):
    return {
        "status": "COMPLETE", "report_kind": "briefing", "review_date": REVIEW,
        "next_trade_date": NEXT, "content": "早报条件剧本", "generated_at": generated_at,
        "parent_research_sha256": parent["research_sha256"],
    }


@pytest.fixture
def db():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE st_quant_review_digest (
                review_date TEXT NOT NULL, adjust_type INTEGER NOT NULL,
                publish_status TEXT NOT NULL, compact_review TEXT,
                quality_json TEXT, market_structure_json TEXT,
                industry_rotation_json TEXT, factor_validation_json TEXT,
                data_cutoff_at TEXT, generated_at TEXT,
                PRIMARY KEY (review_date, adjust_type)
            )
        """))
        conn.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT PRIMARY KEY, trade_status INTEGER)"))
        conn.execute(text("INSERT INTO si_trade_calendar VALUES "
                          "('2026-08-28',1),('2026-08-29',0),('2026-08-30',0),('2026-08-31',1)"))
    yield engine
    engine.dispose()


def _insert(engine, digest):
    payload = {k: json.dumps(v, ensure_ascii=False) if k in research_flow.JSON_FIELDS else v
               for k, v in digest.items()}
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO st_quant_review_digest (" + ",".join(payload) +
                          ") VALUES (" + ",".join(":" + k for k in payload) + ")"), payload)


def _stored(engine):
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM st_quant_review_digest WHERE review_date=:d"),
                           {"d": REVIEW}).mappings().one()
    return research_flow.decode_digest(dict(row))


def test_briefing_write_is_one_transaction_and_keeps_review_and_other_factors(db):
    digest = _with_parent()
    _insert(db, digest)
    research = _briefing(digest["factor_validation_json"]["market_decision"])
    mutations = []

    def before_execute(conn, _cursor, statement, _params, _context, _many):
        if statement.lstrip().upper().startswith("UPDATE"):
            mutations.append(conn.in_transaction())

    event.listen(db, "before_cursor_execute", before_execute)
    research_flow.persist_briefing(db, digest, research)
    event.remove(db, "before_cursor_execute", before_execute)
    stored = _stored(db)

    assert mutations == [True]
    assert stored["compact_review"] == digest["compact_review"]
    assert stored["generated_at"] == digest["generated_at"]
    assert stored["data_cutoff_at"] == digest["data_cutoff_at"]
    assert stored["factor_validation_json"]["selection_review"] == {"content": "已有策略审计"}
    assert stored["factor_validation_json"]["market_decision"] == digest["factor_validation_json"]["market_decision"]
    assert stored["factor_validation_json"]["early_briefing"] == research


def test_briefing_rechecks_current_parent_instead_of_trusting_loaded_digest(db):
    digest = _with_parent()
    _insert(db, digest)
    research = _briefing(digest["factor_validation_json"]["market_decision"])
    changed = deepcopy(digest["factor_validation_json"])
    changed["market_decision"]["research_sha256"] = "new-parent"
    with db.begin() as conn:
        conn.execute(text("UPDATE st_quant_review_digest SET factor_validation_json=:f"),
                     {"f": json.dumps(changed)})

    with pytest.raises(RuntimeError, match="briefing_write: parent review changed"):
        research_flow.persist_briefing(db, digest, research)

    assert _stored(db)["factor_validation_json"] == changed


def test_older_briefing_cannot_replace_a_newer_durable_update(db):
    digest = _with_parent()
    parent = digest["factor_validation_json"]["market_decision"]
    latest = _briefing(parent, generated_at=f"{NEXT}T08:45:00+08:00")
    digest["factor_validation_json"]["early_briefing"] = latest
    _insert(db, digest)

    with pytest.raises(RuntimeError, match="newer briefing already exists"):
        research_flow.persist_briefing(db, digest, _briefing(parent))

    assert _stored(db)["factor_validation_json"]["early_briefing"] == latest


def test_failed_database_update_rolls_back_the_whole_briefing_write(db):
    digest = _with_parent()
    _insert(db, digest)
    with db.begin() as conn:
        conn.execute(text("CREATE TRIGGER reject_briefing BEFORE UPDATE ON st_quant_review_digest "
                          "BEGIN SELECT RAISE(ABORT, 'injected write failure'); END"))

    with pytest.raises(IntegrityError, match="injected write failure"):
        research_flow.persist_briefing(db, digest, _briefing(digest["factor_validation_json"]["market_decision"]))

    assert _stored(db) == digest


def test_preview_briefing_uses_exact_parent_next_open_session_and_never_writes(db, monkeypatch):
    monkeypatch.setattr(market_decision, "verify_automatic_research", Mock())
    digest = _with_parent()
    _insert(db, digest)
    before = _stored(db)
    parent = digest["factor_validation_json"]["market_decision"]
    complete = Mock(return_value=_briefing(parent))
    monkeypatch.setattr(market_decision, "generate_research", complete)
    result = research_flow.generate_briefing(db, REVIEW, persist=False, context={"events": []})

    assert complete.call_args.kwargs["next_trade_date"] == NEXT
    assert complete.call_args.kwargs["previous_research"] == parent
    assert result["parent_research_sha256"] == parent["research_sha256"]
    assert result["parent_research"] == parent
    assert result["research_sha256"] == research_flow.artifact_hash({
        key: value for key, value in result.items() if key != "research_sha256"
    })
    assert _stored(db) == before


@pytest.mark.parametrize("bad_field,bad_value", [
    ("status", "RUNNING"), ("review_date", "2026-08-27"),
    ("next_trade_date", "2026-09-01"), ("research_sha256", ""),
])
def test_briefing_requires_completed_exact_session_parent_before_model(db, monkeypatch, bad_field, bad_value):
    digest = _with_parent()
    digest["factor_validation_json"]["market_decision"][bad_field] = bad_value
    _insert(db, digest)
    complete = Mock(side_effect=AssertionError("invalid lineage must stop before model"))
    monkeypatch.setattr(market_decision, "generate_research", complete)

    with pytest.raises(RuntimeError, match="previous_review: verified research is missing or stale"):
        research_flow.generate_briefing(db, REVIEW, context={})

    complete.assert_not_called()


def test_late_review_rerun_preserves_durable_briefing_snapshot(db, monkeypatch):
    digest = _with_parent()
    morning = _briefing(digest["factor_validation_json"]["market_decision"])
    morning["parent_research"] = deepcopy(digest["factor_validation_json"]["market_decision"])
    digest["factor_validation_json"]["early_briefing"] = morning
    _insert(db, digest)
    updated = _with_parent()
    updated["compact_review"] = "重新计算后的盘后复盘"
    updated["generated_at"] = f"{NEXT}T10:00:00+08:00"
    updated["factor_validation_json"]["market_decision"]["research_sha256"] = "new-review"
    monkeypatch.setattr(quant_digest, "validate_quant_digest_runtime", lambda _engine: None)
    # Exercise the real transaction/read/merge branch with SQLite's equivalent
    # upsert; MySQL's guarded UPSERT syntax has separate dialect contract tests.
    monkeypatch.setattr(quant_digest, "_DIGEST_EXISTING_STATUS",
                        quant_digest._DIGEST_EXISTING_STATUS.replace("FOR UPDATE", ""))
    columns = tuple(updated)
    assignments = ",".join(f"{key}=excluded.{key}" for key in columns if key not in {"review_date", "adjust_type"})
    monkeypatch.setattr(quant_digest, "_DIGEST_UPSERT", "INSERT INTO st_quant_review_digest (" +
                        ",".join(columns) + ") VALUES (" + ",".join(":" + k for k in columns) +
                        ") ON CONFLICT(review_date,adjust_type) DO UPDATE SET " + assignments)

    quant_digest.persist_quant_digest(db, updated)
    stored = _stored(db)

    assert stored["compact_review"] == updated["compact_review"]
    assert stored["factor_validation_json"]["market_decision"]["research_sha256"] == "new-review"
    assert stored["factor_validation_json"]["early_briefing"] == morning


def test_source_digest_identity_survives_database_datetime_round_trip():
    digest = _with_parent()
    identity = market_decision.source_digest_identity(digest)
    loaded = deepcopy(digest)
    loaded["data_cutoff_at"] = datetime.fromisoformat(digest["data_cutoff_at"])
    assert market_decision.source_digest_identity(loaded) == identity


def test_full_research_hash_survives_json_database_scalar_normalisation():
    research = {"evidence": {"timestamp": datetime(2026, 8, 28, 16),
                             "price": Decimal("123.45"), "amount": Decimal("200000000")}}
    stored = json.loads(quant_digest._dump_json(research))
    assert research_flow.artifact_hash(stored) == research_flow.artifact_hash(research)


def test_morning_artifact_does_not_change_the_frozen_fact_identity():
    digest = _with_parent()
    identity = market_decision.source_digest_identity(digest)
    digest["factor_validation_json"]["early_briefing"] = _briefing(
        digest["factor_validation_json"]["market_decision"]
    )
    assert market_decision.source_digest_identity(digest) == identity


def test_postclose_audits_the_matching_morning_hypothesis_before_previous_evening(db, monkeypatch):
    monkeypatch.setattr(market_decision, "verify_automatic_research", Mock())
    prior = _with_parent()
    prior["review_date"] = "2026-08-27"
    prior["factor_validation_json"]["market_decision"]["next_trade_date"] = REVIEW
    morning = _briefing(prior["factor_validation_json"]["market_decision"])
    morning["next_trade_date"] = REVIEW
    prior["factor_validation_json"]["early_briefing"] = morning
    _insert(db, prior)
    complete = Mock(return_value={"content": "今日收盘验证盘前剧本"})
    monkeypatch.setattr(market_decision, "generate_research", complete)

    result = research_flow.attach_review(db, _digest(), context={})

    assert complete.call_args.kwargs["previous_research"] == morning
    assert result["compact_review"] == "今日收盘验证盘前剧本"


def test_briefing_publication_returns_the_canonical_database_readback(db, monkeypatch):
    monkeypatch.setattr(market_decision, "verify_automatic_research", Mock())
    digest = _with_parent()
    _insert(db, digest)
    monkeypatch.setattr(market_decision, "generate_research", Mock(return_value={
        "content": "统一早报正文", "generated_at": f"{NEXT}T08:30:00+08:00",
    }))

    result = research_flow.generate_briefing(db, REVIEW, digest=digest, context={})

    assert result == _stored(db)["factor_validation_json"]["early_briefing"]
    assert result["content"] == "统一早报正文"


def test_briefing_readback_mismatch_stops_publication(db, monkeypatch):
    monkeypatch.setattr(market_decision, "verify_automatic_research", Mock())
    digest = _with_parent()
    _insert(db, digest)
    monkeypatch.setattr(market_decision, "generate_research", Mock(return_value={
        "content": "统一早报正文", "generated_at": f"{NEXT}T08:30:00+08:00",
    }))
    monkeypatch.setattr(research_flow, "load_digest", lambda *_args, **_kwargs: _with_parent())

    with pytest.raises(RuntimeError, match="briefing_readback"):
        research_flow.generate_briefing(db, REVIEW, digest=digest, context={})


def test_context_reads_real_index_columns_and_excludes_after_cutoff_events(db, monkeypatch):
    monkeypatch.setattr(batch_db, "get_kline_engine", lambda: db)
    with db.begin() as conn:
        conn.execute(text("CREATE TABLE sm_index_kline (index_code TEXT, trade_date TEXT, "
                          "k_type INTEGER, open REAL, close REAL, high REAL, low REAL, "
                          "amount REAL, change_pct REAL)"))
        conn.execute(text("INSERT INTO sm_index_kline VALUES "
                          "('000001','2026-08-28',1,3000,3030,3040,2990,10000000,1),"
                          "('399001','2026-08-27',1,9000,9090,9100,8990,10000000,1)"))
        conn.execute(text("CREATE TABLE st_news_flash (source TEXT, title TEXT, content TEXT, "
                          "publish_time TEXT, first_seen_at TEXT)"))
        conn.execute(text("INSERT INTO st_news_flash VALUES "
                          "('source','before cutoff','fact','2026-08-28 16:00:00','2026-08-28 16:01:00'),"
                          "('source','after cutoff','future','2026-08-28 18:00:00','2026-08-28 18:01:00'),"
                          "('source','backfilled later','unseen at report time','2026-08-28 15:00:00','2026-08-31 08:00:00'),"
                          "('source','exact cutoff','inclusive boundary','2026-08-28 16:59:00','2026-08-28 17:00:00')"))

    context = research_flow.collect_context(db, REVIEW, now=datetime(2026, 8, 28, 17))

    assert context["indices"]["status"] == "OBSERVED"
    assert len(context["indices"]["rows"]) == 1
    assert context["indices"]["rows"][0]["index_code"] == "000001"
    assert context["indices"]["rows"][0]["change_pct"] == 1.0
    assert [row["title"] for row in context["events"]["rows"]] == ["exact cutoff", "before cutoff"]
    assert context["events"]["rows"][1]["first_seen_at"] == "2026-08-28 16:01:00"


def test_overnight_quotes_require_market_time_after_baseline_close(db):
    with db.begin() as conn:
        conn.execute(text("CREATE TABLE st_external_market_context (source TEXT, symbol TEXT, "
                          "display_name TEXT, price REAL, change_pct REAL, previous_close REAL, "
                          "market_time TEXT, captured_at TEXT, availability TEXT)"))
        conn.execute(text("INSERT INTO st_external_market_context VALUES "
                          "('source','old','陈旧快照',100,-2,102,'2026-08-28 10:13:00','2026-08-28 16:00:00','available'),"
                          "('source','fresh','隔夜报价',100,2,98,'2026-08-29 04:00:00','2026-08-29 08:00:00','available'),"
                          "('source','future','未来报价',100,2,98,'2026-09-01 04:00:00','2026-08-29 08:00:00','available'),"
                          "('source','unknown','无时间报价',100,2,98,NULL,'2026-08-29 08:00:00','available')"))

    context = research_flow.collect_context(db, REVIEW, now=datetime(2026, 8, 31, 8), overnight=True)
    quotes = {row['symbol']: row for row in context['external_market']['rows']}

    assert context['external_market']['fresh_count'] == 1
    assert quotes['fresh']['availability'] == 'available'
    assert quotes['old']['availability'] == 'STALE'
    assert 'price' not in quotes['old'] and 'change_pct' not in quotes['old']
    assert quotes['fresh']['change_pct'] == 2
    assert quotes['future']['availability'] == 'MISSING'
    assert quotes['unknown']['availability'] == 'MISSING'


def test_reported_turnover_keeps_sh_sz_scope_and_source_instead_of_bad_kline_sum():
    row = {"source": "sina", "title": "沪深两市今日成交额合计1.67万亿元",
           "content": "沪深两市全天成交额合计1.67万亿元，较上一日增量1179.46亿元。"
                      "其中，沪市成交额为7642.27亿元（上一交易日为7156.21亿元）；"
                      "深市成交额为9102.36亿元（上一交易日为8408.96亿元）。",
           "publish_time": "2026-10-30 15:01:02", "first_seen_at": "2026-10-30 15:02:28"}
    observed = research_flow.reported_turnover([row])
    assert observed["status"] == "OBSERVED"
    assert observed["reported_total_amount"] == 1_674_463_000_000
    assert observed["reported_change_amount"] == 117_946_000_000
    assert "不含北交所" in observed["scope"]
    assert observed["publish_time"] == row["publish_time"]
    assert research_flow.reported_turnover([dict(row, content="沪深市场金额口径未给出")])["status"] == "UNKNOWN"


@pytest.mark.parametrize("inconsistent,expected_amount", [(0, 100_000_000), (1, None)])
def test_review_sample_preserves_full_verified_amount_unless_it_contradicts(db, monkeypatch, inconsistent, expected_amount):
    digest = _digest()
    market = digest["market_structure_json"]
    market.update(previous_sample_count=5200, total_amount=100_000_000,
                  previous_total_amount=90_000_000, amount_change_pct=11.11)
    market["amount_quality"] = {"status": "OBSERVED", "trade_date": REVIEW, "sample_count": 5200, "checked_count": 5200}
    market["previous_amount_quality"] = {"status": "OBSERVED", "trade_date": "2026-08-27", "sample_count": 5200, "checked_count": 5200}
    context = {"theme_evidence": {"amount_quality": {
        "target": {"status": "UNKNOWN" if inconsistent else "OBSERVED", "trade_date": REVIEW,
                   "sample_count": 16, "inconsistent_count": inconsistent},
        "previous": {"status": "OBSERVED", "trade_date": "2026-08-27", "sample_count": 16}}}}
    complete = Mock(return_value={"content": "核验后的复盘"})
    monkeypatch.setattr(market_decision, "generate_research", complete)
    research_flow.attach_review(db, digest, context=context)
    assert digest["market_structure_json"]["total_amount"] == expected_amount
    assert digest["market_structure_json"]["previous_total_amount"] == 90_000_000
    assert complete.call_args.args[0] is digest


def test_editorial_parent_cannot_enter_the_automatic_briefing_flow(db, monkeypatch):
    digest = _with_parent()
    generate = Mock(side_effect=AssertionError("editorial parent reached model"))
    monkeypatch.setattr(market_decision, "generate_research", generate)
    with pytest.raises(market_decision.ResearchError, match="nonautomatic_artifact"):
        research_flow.generate_briefing(db, REVIEW, persist=False, digest=digest, context={})
    generate.assert_not_called()


def test_explicit_frozen_none_parent_and_calendar_never_reread_sources(monkeypatch):
    generate = Mock(return_value={"content": "automatic report"})
    monkeypatch.setattr(market_decision, "generate_research", generate)
    monkeypatch.setattr(research_flow, "load_digest", Mock(side_effect=AssertionError("prior reread")))
    monkeypatch.setattr(research_flow, "next_session", Mock(side_effect=AssertionError("calendar reread")))
    snapshot = {"run_id": "frozen", "stages": {}}
    progress = Mock()
    research_flow.attach_review(None, _digest(), context={}, previous_research=None,
                                next_trade_date=NEXT, checkpoint=snapshot, progress=progress)
    kwargs = generate.call_args.kwargs
    assert kwargs["previous_research"] is None and kwargs["next_trade_date"] == NEXT
    assert kwargs["checkpoint"] is snapshot and kwargs["progress"] is progress
    assert "completion" not in kwargs

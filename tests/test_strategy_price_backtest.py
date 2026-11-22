"""Deterministic fixtures are confined to tests; production uses verified bars."""
from copy import deepcopy
from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from server.engine.strategy_price_backtest import technical_history, supports_price_rule, _group_inputs, _identity_rows
from server.engine.strategy_rule_replay import replay_rule


def bars_fixture(n=75):
    days = [d.date().isoformat() for d in pd.bdate_range("2026-06-01", periods=n)]
    return [{"stock_code": code, "trade_date": day, "open": 10 * rate ** (i - 1),
        "pre_close": 10 * rate ** (i - 1), "close": 10 * rate ** i,
        "high": 10.2 * rate ** i, "low": 9.8 * rate ** i,
        "amount": 1e8 + i * 10000, "volume": 1e7}
        for code, rate in [("000001", 1.01), ("000002", .99)] for i, day in enumerate(days)]


def test_trailing_features_match_actual_close_producer_without_future_bars():
    from biz.analysis.sync_analysis_fast import _kline_technical_window
    raw = bars_fixture()
    result = technical_history(raw)
    target = sorted(set(r["trade_date"] for r in raw))[62]
    frame = pd.DataFrame(raw)
    frame["trade_date"] = pd.to_datetime(frame.trade_date).dt.date
    frame = frame.sort_values(["stock_code", "trade_date"])
    frame["change_pct"] = (frame.close / frame.pre_close - 1) * 100
    expected = _kline_technical_window(frame.copy(), date.fromisoformat(target)).set_index("stock_code")
    actual = result.loc[result.trade_date == target].set_index("stock_code")
    for key in ["pct_5", "pct_20", "previous_change_pct", "dist_ma20", "volatility_20", "amount_ratio_5", "amount_ratio_20", "drawdown_60", "from_low_60"]:
        assert actual[key].tolist() == pytest.approx(expected[key].tolist())
    changed = deepcopy(raw)
    for row in changed:
        if row["trade_date"] > target:
            for key in ["open", "high", "low", "close", "pre_close", "amount"]:
                row[key] *= 100
    pd.testing.assert_frame_equal(result.loc[result.trade_date <= target].reset_index(drop=True),
        technical_history(changed).loc[lambda f: f.trade_date <= target].reset_index(drop=True))


def test_duplicate_or_missing_market_facts_are_not_zero_filled():
    raw = bars_fixture(25)
    with pytest.raises(ValueError, match="重复"):
        technical_history(raw + [raw[0]])
    raw[0]["pre_close"] = None
    with pytest.raises(ValueError, match="缺失"):
        technical_history(raw)


def test_unrelated_ai_and_flow_scores_still_require_original_frozen_evidence():
    rule = {"market_all": [{"field": "market_breadth_pct"}], "entry_all": [{"field": "pct_20"}], "entry_any": [], "ranking": [{"field": "volatility_20"}]}
    assert supports_price_rule(rule)
    for field in ["capital_score", "technical_score", "turnover_ratio", "sector_rotation_score"]:
        rule["entry_all"][0]["field"] = field
        assert not supports_price_rule(rule)


def test_archive_uses_retained_proof_and_recent_days_use_their_actual_manifest():
    from server.common.qmt_datadir_daily_coverage import MANIFEST_TABLE, SCHEMA, PROVIDER
    from server.common.verified_daily_market_truth import FREE_DAILY_PROVIDERS
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE acquisition_partition_state(target_date TEXT,dataset TEXT,partition_key TEXT,status TEXT,source TEXT)"))
        c.execute(text("INSERT INTO acquisition_partition_state VALUES('2026-08-03','stock_daily','__daily_partition__','complete',:source)"), {"source": sorted(FREE_DAILY_PROVIDERS)[0]})
        c.execute(text(f"CREATE TABLE {MANIFEST_TABLE}(trade_date TEXT,schema_version TEXT,dataset TEXT,provider TEXT,status TEXT,strategy_eligible INT,captured_at TEXT)"))
        c.execute(text(f"INSERT INTO {MANIFEST_TABLE} VALUES('2026-07-31',:schema,'stock_daily',:provider,'EXACT',1,'2026-08-02')"), {"schema": SCHEMA, "provider": PROVIDER})
        groups = _group_inputs(c, ["2026-07-29", "2026-07-30", "2026-07-31", "2026-08-03"], "2026-11-22")
        assert _group_inputs(c, ["2026-09-28"], "2026-11-22")[0]["sources"]["2026-09-28"] == "QMT_RETAINED"
    assert groups[0]["sources"] == {"2026-07-29": "QMT_RETAINED", "2026-07-30": "QMT_RETAINED", "2026-07-31": "QMT_MANIFEST", "2026-08-03": "VERIFIED_PUBLIC"}


def test_old_day_never_borrows_present_day_name_or_industry():
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE qmt_membership_snapshot_run(snapshot_date TEXT,source TEXT)"))
        c.execute(text("INSERT INTO qmt_membership_snapshot_run VALUES('2026-11-20','gj_big_qmt_inner')"))
        rows, proof = _identity_rows(c, "2026-08-31")
    assert rows == {} and proof["status"] == "UNAVAILABLE"


def test_native_receipt_uses_original_universe_and_reuses_immutable_scope(monkeypatch):
    from server.engine import strategy_price_backtest as price
    from server.common import verified_daily_market_truth as source
    from server.common.qmt_attestation_contract import expected_stock_set_contract
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
    engine = create_engine('sqlite://')
    privileged_migrate_formal_analysis_schema(engine)
    day = '2026-10-16'
    expected = expected_stock_set_contract(day, ['000001', '000002'])
    receipt = source.VerifiedDailyMarketTruth(provider='qmt_retained_history', target_date=day,
        receipt_id='native-run', captured_at='2026-10-16 18:00:00', decision_known_at='2026-11-22 05:59:24',
        catalog_batch_id='later', catalog_manifest_hash='a' * 64, catalog_member_set_hash='b' * 64,
        row_count=2, code_count=2, code_set_hash=expected['stock_set_hash'], row_hash='c' * 64,
        coverage_ratio=1.0, truth_hash='d' * 64)
    truth = receipt.as_dict()
    monkeypatch.setattr(source, 'load_retained_qmt_daily_market_truth', lambda *_a, **_k: receipt)
    prepared = []
    def prepare(_c, run, _cutoff):
        prepared.append(run['run_id'])
        return {day: {'catalog': 'original', 'excluded_codes': ['000003'], 'stock_count': 2,
                      'stock_set_hash': expected['stock_set_hash'], 'native_receipt_id': 'native-run',
                      'available_at': '2026-10-16 18:00:00'}}
    monkeypatch.setattr(price, '_prepare_native_scopes', prepare)
    with engine.begin() as c:
        c.execute(text('CREATE TABLE qmt_kline_attestation_run(run_id TEXT,provider TEXT)'))
        c.execute(text("INSERT INTO qmt_kline_attestation_run VALUES('native-run','gj_big_qmt_inner')"))
        c.execute(text('CREATE TABLE qmt_stock_catalog_member(batch_id TEXT,stock_code TEXT,instrument_type TEXT,list_date TEXT,expire_date TEXT)'))
        for batch, codes in [('original', ['000001', '000002', '000003']), ('later', ['000001', '000002', '000003', '000004'])]:
            c.execute(text("INSERT INTO qmt_stock_catalog_member VALUES(:batch,:code,'STOCK','2020-01-01',NULL)"), [{'batch': batch, 'code': code} for code in codes])
        c.execute(text('CREATE TABLE sm_stock_kline(stock_code TEXT,trade_date TEXT,k_type INT,adjust_type INT,received_at TEXT,open REAL,high REAL,low REAL,close REAL,pre_close REAL,volume REAL,amount REAL)'))
        c.execute(text("INSERT INTO sm_stock_kline VALUES(:code,:day,1,0,'2026-10-16 18:00:00',10,11,9,10,10,1000,10000)"), [{'code': f'00000{i}', 'day': day} for i in range(1, 5)])
    group = {'sessions': [day], 'sources': {day: 'QMT_RETAINED'}}
    for _ in range(2):
        with engine.begin() as c:
            part = price._validate_group(c, group, '2026-11-22 05:59:24')
            assert part['catalogs'][day] == 'original' and part['row_counts'][day] == 2
            rows = price._read_bars(c, [day], {day: part}, '2026-11-22 05:59:24')
            assert [r['stock_code'] for r in rows] == ['000001', '000002']
            assert c.execute(text('SELECT COUNT(*) FROM sm_stock_kline')).scalar() == 4
    assert prepared == ['native-run']
    truth['code_set_hash'] = 'f' * 64
    with engine.connect() as c, pytest.raises(ValueError, match='原始证券集合'):
        price._source_scope(c, truth, day, '2026-11-22 05:59:24')


def test_missing_entry_identity_excludes_that_entry_but_preserves_known_cash_outcome():
    raw = bars_fixture(30)
    days = sorted(set(r["trade_date"] for r in raw))
    rule = {"holding_days": 2, "stop_loss_pct": 5}
    signals = [{"stock_code": "000001", "stock_name": "", "industry": None, "rank": 1,
                "signal_known_at": days[20] + " 18:00:00"},
               {"stock_code": "000001", "stock_name": "测试证券", "industry": "测试行业", "rank": 1,
                "signal_known_at": days[22] + " 18:00:00"}]
    r = replay_rule(rule, signals, raw, days, evaluation_start=days[20])
    assert r["status"] == "PARTIAL" and r["net_return_pct"] is None
    assert r["verified_input_subset_return_pct"] is not None
    assert r["input_exclusion_count"] == 1
    assert r["completed_trade_count"] == 1
    assert len(r["nav"]) == 10 and r["nav"][0]["trade_date"] == days[20]
    assert r["funding_eligible"] is False


def test_historical_bar_batches_fit_query_budget_without_losing_daily_scope(monkeypatch):
    from sqlalchemy import event
    from sqlalchemy.exc import OperationalError
    from server.engine import strategy_price_backtest as price
    from server.common.analysis_pool_receipt import canonical_sha256
    from server.common.strategy_daily_input_window import daily_input_catalog_join

    engine = create_engine('sqlite://')
    days = [d.date().isoformat() for d in pd.bdate_range('2026-08-27', periods=8)]
    codes = [f'{i:06d}' for i in range(5500)]
    with engine.begin() as c:
        c.execute(text('CREATE TABLE qmt_stock_catalog_member(batch_id TEXT,stock_code TEXT,instrument_type TEXT,list_date TEXT,expire_date TEXT,PRIMARY KEY(batch_id,stock_code))'))
        c.execute(text('CREATE TABLE sm_stock_kline(stock_code TEXT,trade_date TEXT,k_type INT,adjust_type INT,received_at TEXT,open REAL,high REAL,low REAL,close REAL,pre_close REAL,volume REAL,amount REAL)'))
        c.execute(text('CREATE INDEX idx_date_ktype ON sm_stock_kline(trade_date,k_type,adjust_type)'))
        members = [{'code': code, 'kind': 'STOCK', 'listed': '2020-01-01', 'expired': None} for code in codes]
        members += [{'code': '990001', 'kind': 'ETF', 'listed': '2020-01-01', 'expired': None},
                    {'code': '990002', 'kind': 'STOCK', 'listed': '2026-10-01', 'expired': None},
                    {'code': '990003', 'kind': 'STOCK', 'listed': '2020-01-01', 'expired': '2026-08-01'},
                    {'code': '990004', 'kind': 'STOCK', 'listed': '2020-01-01', 'expired': None}]
        c.execute(text("INSERT INTO qmt_stock_catalog_member VALUES('frozen',:code,:kind,:listed,:expired)"), members)
        c.execute(text("INSERT INTO sm_stock_kline VALUES(:code,:day,1,0,'2026-09-10',10,11,9,10,10,1000,10000)"),
                  [{'code': member['code'], 'day': day} for day in days for member in members])
        c.execute(text("INSERT INTO sm_stock_kline VALUES('000001',:day,1,0,'2026-11-23',99,99,99,99,99,1000,10000)"), [{'day': day} for day in days])
    scopes = {day: {'catalog': 'frozen', 'excluded_codes': ['990004'], 'stock_count': len(codes),
                    'stock_set_hash': canonical_sha256(codes)} for day in days}
    monkeypatch.setattr(price, '_resolved_scopes', lambda *_args: scopes)
    inputs = {'part': {'row_counts': {day: len(codes) for day in days}}}
    with engine.connect() as c:
        driver = c.connection.driver_connection
        def budget(conn, cursor, statement, parameters, context, executemany):
            if statement.lstrip().startswith('SELECT'):
                used = 0
                def progress():
                    nonlocal used
                    used += 1000
                    return int(used > 1500000)
                driver.set_progress_handler(progress, 1000)
        event.listen(c, 'before_cursor_execute', budget)
        join, params = daily_input_catalog_join({'sessions': days, 'catalog_batches_by_session': {day: 'frozen' for day in days}})
        old_query = text('SELECT SUBSTR(k.stock_code,1,6) stock_code,k.trade_date,k.open,k.high,k.low,k.close,k.pre_close,k.volume,k.amount FROM sm_stock_kline k ' + join + " WHERE k.k_type=1 AND k.adjust_type=0 AND k.trade_date BETWEEN :start AND :end AND k.received_at<=:cutoff ORDER BY k.stock_code,k.trade_date LIMIT 60001")
        try:
            with pytest.raises(OperationalError, match='interrupted'):
                c.execute(old_query, {**params, 'start': days[0], 'end': days[-1], 'cutoff': '2026-11-22'}).all()
            rows = price._read_bars(c, days, inputs, '2026-11-22')
        finally:
            driver.set_progress_handler(None, 0)
            event.remove(c, 'before_cursor_execute', budget)
    assert len(rows) == len(days) * len(codes)
    assert {row['close'] for row in rows} == {10}
    assert {row['stock_code'] for row in rows} == set(codes)

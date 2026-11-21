from datetime import datetime

from bs4 import BeautifulSoup
import pytest
from sqlalchemy import create_engine,text

from server.api.routers import research_context as context


def test_macro_release_selection_uses_official_publication_date():
    page=BeautifulSoup('''<a href="202611/t20261116_123.html">10月份国民经济运行总体平稳</a>
    <a href="202612/t20261215_456.html">11月份国民经济运行总体平稳</a>
    <a href="202611/t20261116_124.html">就国民经济运行答记者问</a>
    <a href="https://example.com/202611/t20261116_999.html">国民经济运行</a>''','html.parser')
    result=context._release_candidates(page,datetime(2026,11,18))
    assert len(result)==1 and result[0]['date']=='2026-11-16'
    assert result[0]['url']=='https://www.stats.gov.cn/sj/zxfb/202611/t20261116_123.html'


def test_macro_parser_keeps_official_units_and_missing_values():
    page=BeautifulSoup('''<table><tr><td>指 标</td><td>10月</td><td>1—10月</td></tr>
    <tr><td>绝对量</td><td>同比增长（%）</td><td>绝对量</td><td>同比增长（%）</td></tr>
    <tr><td>工业增加值</td><td>…</td><td>4.8</td><td>…</td><td>5.3</td></tr>
    <tr><td>社会消费品零售总额（亿元）</td><td>46291</td><td>2.9</td><td>420324</td><td>4.3</td></tr></table>''','html.parser')
    headers,rows=context._macro_table(page)
    assert headers==['指标','10月','1—10月']
    assert rows[0]['current_value']=='…' and rows[0]['current_yoy']=='4.8'
    assert rows[1]['indicator'].endswith('（亿元）')
    with pytest.raises(ValueError):
        context._macro_table(BeautifulSoup('<table><tr><td>未知表</td></tr></table>','html.parser'))


def test_global_context_does_not_mix_batches_or_display_future_quotes(monkeypatch):
    engine=create_engine('sqlite://')
    with engine.begin() as conn:
        conn.exec_driver_sql('CREATE TABLE st_external_market_context (id INT,context_date TEXT,snapshot_id TEXT,captured_at TEXT,symbol TEXT,display_name TEXT,price REAL,change_pct REAL,previous_close REAL,market_time TEXT,availability TEXT,source TEXT,payload_json TEXT)')
        for ident,batch,captured,symbol,market,price in [
            (1,'old','2026-11-18 08:00:00','nasdaq','2026-11-18 07:59:00',999),
            (2,'new','2026-11-18 09:00:00','nasdaq','2026-11-18 08:59:00',100),
            (3,'new','2026-11-18 09:00:00','sp500','2026-11-19 10:00:00',200),
        ]:
            conn.execute(text('INSERT INTO st_external_market_context VALUES (:ident,\'2026-11-18\',:batch,:captured,:symbol,:symbol,:price,1,99,:market,\'available\',\'verified.source\',\'{}\')'),
                         dict(ident=ident,batch=batch,captured=captured,symbol=symbol,price=price,market=market))
    monkeypatch.setattr(context,'get_engine',lambda:engine)
    result=context.build_global_context('2026-11-18')
    assert result['snapshot_id']=='new' and result['expected_count']==41
    assert result['available_count']==1 and result['acquisition_status']=='PARTIAL'
    assert [x['price'] for x in result['rows']]==[100,None]
    engine.dispose()


def test_latest_observation_is_separate_from_history_and_reads_only_one_batch(monkeypatch):
    engine=create_engine('sqlite://')
    with engine.begin() as conn:
        conn.exec_driver_sql('CREATE TABLE st_external_market_context (id INT,context_date TEXT,snapshot_id TEXT,captured_at TEXT,symbol TEXT,display_name TEXT,price REAL,change_pct REAL,previous_close REAL,market_time TEXT,availability TEXT,source TEXT,payload_json TEXT)')
        for ident in range(300):
            conn.execute(text("INSERT INTO st_external_market_context VALUES (:i,'2026-11-18',:batch,'2026-11-18 08:15:00','nasdaq','指数',100,1,99,'2026-11-18 08:00:00','available','native','{}')"),
                         {'i':ident,'batch':'batch'+str(ident)})
        conn.execute(text("INSERT INTO st_external_market_context VALUES (301,'2026-11-19','later','2026-11-19 10:00:00','nasdaq','指数',200,1,199,'2026-11-19 09:59:00','available','native','{}')"))
    monkeypatch.setattr(context,'get_engine',lambda:engine)
    assert context.build_global_context('2026-11-18','latest')['snapshot_id']=='later'
    historical=context.build_global_context('2026-11-18','history')
    assert historical['snapshot_id']=='batch299'
    assert historical['rows'][0]['price']==100
    assert historical['view_mode']=='history'
    engine.dispose()

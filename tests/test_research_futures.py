from datetime import datetime
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time

import pytest
import requests

from server.api.routers import research_futures as futures


def at(value):
    return datetime.fromisoformat(value).replace(tzinfo=futures.SHANGHAI)


def test_sina_first_minute_price_is_not_the_previous_reference_price():
    payload={"minLine_1d":[
        ["2026-11-19","90","cme","","07:00","101","0","0","100","2026-11-19 07:00:00"],
        ["07:01","102","0","0","100","2026-11-19 07:01:00"],
        ["07:02","999","0","0","100","2026-11-19 07:02:00"],
    ]}
    raw=('/* provider wrapper */=('+json.dumps(payload)+');').encode()
    bars=futures._sina_minutes(raw,at('2026-11-19T07:01:00'))
    assert bars=={at('2026-11-19T07:00:00'):101,at('2026-11-19T07:01:00'):102}


def test_taiwan_selects_actual_liquid_contract_at_receive_time_not_spot():
    rows=[dict(SymbolID=symbol,CLastPrice='100',CRefPrice='99',CTotalVolume=volume,
               CDate='20261119',CTime='120001') for symbol,volume in
          [('TXF-S','999999'),('TXFL6-F','1000'),('TXFA7-F','10')]]
    quote=futures._taifex_quote(rows,at('2026-11-19T12:00:03'),0)
    assert quote['symbol']=='TXFL6-F' and quote['contract']=='TXF 2026-12'
    assert quote['market_time']=='2026-11-19T12:00:01+08:00'
    assert futures._taifex_quote(rows,at('2026-11-19T12:00:00'),0) is None


def test_taiwan_night_minutes_cross_midnight_without_joining_other_sessions():
    quote={'symbol':'TXFL6-M','session_date':'20261118','session':'夜盘'}
    data={'SymbolID':'TXFL6-M','Field':['T','O','H','L','C','V'],'Quote':{'CDate':'20261118'},
          'Info':{'Sessions':[{'Start':'1500','End':'0500'}]},
          'Ticks':[['235900','100','101','99','100','1'],['000100','101','102','100','101','1'],
                   ['000200','1000','1001','999','1000','1']]}
    bars=futures._taifex_minutes(data,quote,at('2026-11-19T00:01:30'))
    assert bars=={at('2026-11-18T23:59:00'):100,at('2026-11-19T00:01:00'):101}
    with pytest.raises(ValueError,match='contract'):
        futures._taifex_minutes(data,{**quote,'symbol':'TXFA7-M'},at('2026-11-19T00:01:30'))


def test_return_windows_reject_gaps_and_do_not_invent_24_hour_changes():
    quote={'market_time':'2026-11-19T12:00:30+08:00','price':110}
    bars={at('2026-11-19T11:30:00'):100,at('2026-11-19T10:58:00'):90,
          at('2026-11-18T11:50:00'):80}
    result=futures._horizons(quote,bars)
    assert result['30m']['change_pct']==pytest.approx(10)
    assert result['1h']['change_pct'] is None
    assert result['4h']['change_pct'] is None and result['24h']['change_pct'] is None


def test_futures_sources_fail_independently_and_keep_the_true_coverage(monkeypatch):
    now=datetime.now(futures.SHANGHAI)
    text='var hq_str_hf_NQ="100,0,0,0,0,0,'+now.strftime('%H:%M:%S')+',99,0,0,0,0,'+now.date().isoformat()+',name";'
    def source(url,deadline,**kwargs):
        if 'hq.sinajs.cn' in url:return text.encode('gb18030')
        raise TimeoutError('source timed out')
    monkeypatch.setattr(futures,'_request',source)
    result=futures.build_global_futures()
    assert result['available_count']==1 and result['expected_count']==7
    assert result['rows'][0]['symbol']=='hf_NQ'
    assert result['rows'][0]['horizons']['30m']['change_pct'] is None
    assert result['missing_instruments'][-1].startswith('KOSPI200')
    assert len(result['source_errors'])==3  # Both exchange sessions and the NQ minute reply.


def test_slow_dripping_http_reply_has_a_total_deadline():
    class SlowReply(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Length','32768')
            self.end_headers()
            try:
                for _ in range(100):
                    self.wfile.write(b'x')
                    self.wfile.flush()
                    time.sleep(.04)
            except (BrokenPipeError,ConnectionResetError,OSError):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),SlowReply)
    thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.02),daemon=True)
    thread.start()
    started=time.monotonic()
    try:
        with pytest.raises((requests.RequestException,TimeoutError)):
            futures._request('http://127.0.0.1:'+str(server.server_port),started+.3)
        assert time.monotonic()-started<1.5
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)

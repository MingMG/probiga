"""Current, source-labelled futures observations; no orders or historical replay."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from time import monotonic
from urllib.parse import urlparse
import threading
import csv
import json
import math
import re

from fastapi import APIRouter
import requests
from urllib3.util import Timeout

from server.api.routers.market_home import SHANGHAI
from server.api.routers.research_tools import _serve

router = APIRouter(tags=["research-futures"])
SINA = {"NQ":"纳斯达克100期货CFD", "ES":"标普500期货CFD", "YM":"道指期货CFD",
        "NK":"日经225期货CFD", "HSI":"恒指期货CFD"}
TAIFEX = "https://mis.taifex.com.tw/futures/api/"


def _number(value):
    try:
        result = float(value)
    except (ValueError, TypeError):
        return None
    return result if math.isfinite(result) else None


def _request(url, deadline, *, body=None):
    remaining = deadline-monotonic()
    if remaining<=0:
        raise TimeoutError("futures source deadline exceeded")
    with requests.Session() as session:
        session.trust_env=False
        method=session.get if body is None else session.post
        kwargs={} if body is None else {"json":body}
        with method(url, **kwargs, timeout=Timeout(total=remaining,connect=min(4,remaining/2),read=min(8,remaining/2)),stream=True,
                    allow_redirects=False,headers={"Referer":"https://finance.sina.com.cn/" if urlparse(url).hostname in {"hq.sinajs.cn","stock2.finance.sina.com.cn"} else
                                                 "https://mis.taifex.com.tw/futures/","User-Agent":"Mozilla/5.0"}) as r:
            r.raise_for_status()
            if r.status_code!=200:
                raise ValueError("futures source redirected")
            raw=bytearray()
            def abort_reply():
                try:
                    r.raw.shutdown()
                except (ValueError,RuntimeError,OSError):
                    pass  # The response may already be fully consumed and released.
            watchdog=threading.Timer(max(0,deadline-monotonic()),abort_reply)
            watchdog.daemon=True
            watchdog.start()
            try:
                for block in r.iter_content(16384):
                    raw.extend(block)
                    if len(raw)>2_000_000 or monotonic()>deadline:
                        raise TimeoutError("futures source reply exceeds time or byte bound")
            finally:
                watchdog.cancel()
                watchdog.join()
            if monotonic()>deadline:
                raise TimeoutError("futures source reply deadline exceeded")
    return bytes(raw)


def _json_reply(raw):
    value=json.loads(raw)
    if value.get("RtCode")!="0" or not isinstance(value.get("RtData"),dict):
        raise ValueError("TAIFEX quote reply is unavailable")
    return value["RtData"]


def _sina_quotes(raw, now):
    found={}
    for code,body in re.findall(r'var hq_str_hf_([A-Z0-9]+)="([^"\r\n]*)";',raw.decode("gb18030")):
        if code not in SINA or code in found:
            raise ValueError("Sina futures identity differs")
        fields=next(csv.reader([body]))
        if len(fields)<14:
            continue
        price,previous=_number(fields[0]),_number(fields[7])
        try:
            at=datetime.fromisoformat(fields[12]+"T"+fields[6]).replace(tzinfo=SHANGHAI)
        except ValueError:
            continue
        if price is None or previous is None or min(price,previous)<=0 or at>now:
            continue
        found[code]={"symbol":"hf_"+code,"display_name":SINA[code],"contract":"供应商连续CFD报价",
                     "price":price,"change_pct":(price/previous-1)*100,"market_time":at.isoformat(),
                     "source":"sina.hf.CFD","source_url":"https://finance.sina.com.cn/futures/quotes/"+code+".shtml",
                     "availability":"available"}
    return found


def _sina_minutes(raw, as_of):
    text=raw.decode("utf-8")
    # The provider sends JSONP; parse the data without executing its wrapper.
    value=json.loads(text[text.index("(")+1:text.rindex(")")])
    rows=value.get("minLine_1d")
    if not isinstance(rows,list) or len(rows)>1600:
        raise ValueError("Sina minute series shape differs")
    bars={}
    for row in rows:
        if not isinstance(row,list) or len(row) not in (6,10):
            raise ValueError("Sina minute row differs")
        at=datetime.fromisoformat(row[-1]).replace(tzinfo=SHANGHAI)
        price=_number(row[5] if len(row)==10 else row[1])
        if price is not None and price>0 and at<=as_of:
            if at in bars:
                raise ValueError("duplicate Sina minute timestamp")
            bars[at]=price
    return bars


def _taifex_quote(rows, now, market):
    candidates=[]
    for row in rows:
        symbol=str(row.get("SymbolID") or "")
        if not re.fullmatch(r"TXF[A-L][0-9]-"+("F" if market==0 else "M"),symbol):
            continue
        price,reference,volume=(_number(row.get(k)) for k in ("CLastPrice","CRefPrice","CTotalVolume"))
        if price is None or reference is None or volume is None or min(price,reference)<=0 or volume<=0:
            continue
        try:
            at=datetime.strptime(row["CDate"]+row["CTime"],"%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI)
        except (ValueError,KeyError,TypeError):
            continue
        # TAIFEX CDate identifies the session's opening date; the night session
        # begins at 15:00 and its post-midnight trades belong to the next date.
        if market==1 and at.hour<15:
            at+=timedelta(days=1)
        if at>now:
            continue
        month=ord(symbol[3])-ord("A")+1
        years=[y for y in range(now.year-1,now.year+3) if y%10==int(symbol[4])]
        if len(years)!=1:
            raise ValueError("TAIFEX contract year is ambiguous")
        candidates.append((volume,symbol,{
            "symbol":symbol,"display_name":"台指期货","contract":f"TXF {years[0]}-{month:02d}",
            "price":price,"change_pct":(price/reference-1)*100,"market_time":at.isoformat(),
            "source":"taifex.exchange","source_url":"https://mis.taifex.com.tw/futures/",
            "availability":"available","session":"日盘" if market==0 else "夜盘",
            "session_date":row["CDate"],"reference_basis":"较前结算参考价", "volume":volume,
        }))
    return max(candidates,key=lambda x:(x[0],x[1]))[2] if candidates else None


def _taifex_minutes(data, quote, as_of):
    if data.get("SymbolID")!=quote["symbol"] or data.get("Field")!=["T","O","H","L","C","V"]:
        raise ValueError("TAIFEX minute contract or fields differ")
    if data.get("Quote",{}).get("CDate")!=quote["session_date"]:
        raise ValueError("TAIFEX minute session date differs")
    sessions=data.get("Info",{}).get("Sessions",[])
    if len(sessions)!=1:
        raise ValueError("TAIFEX minute trading session is unavailable")
    start,end=sessions[0]["Start"],sessions[0]["End"]
    if not re.fullmatch(r"\d{4}",start) or not re.fullmatch(r"\d{4}",end):
        raise ValueError("TAIFEX session times differ")
    if (quote["session"]=="夜盘")!=(end<start):
        raise ValueError("TAIFEX cross-midnight session differs")
    rows=data.get("Ticks")
    if not isinstance(rows,list) or len(rows)>1600:
        raise ValueError("TAIFEX minute series exceeds its bound")
    bars={}
    for row in rows:
        if not isinstance(row,list) or len(row)!=6 or not re.fullmatch(r"\d{6}",str(row[0])):
            raise ValueError("TAIFEX minute row differs")
        at=datetime.strptime(quote["session_date"]+row[0],"%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI)
        if end<start and row[0][:4]<start:
            at+=timedelta(days=1)
        price=_number(row[4])
        if price is not None and price>0 and at<=as_of:
            if at in bars:
                raise ValueError("duplicate TAIFEX minute timestamp")
            bars[at]=price
    return bars


def _horizons(quote,bars):
    as_of=datetime.fromisoformat(quote["market_time"])
    output={}
    for label,minutes in (("30m",30),("1h",60),("4h",240),("24h",1440)):
        target=as_of-timedelta(minutes=minutes)
        matches=[at for at in bars if timedelta(0)<=target-at<=timedelta(seconds=90)]
        baseline=max(matches) if matches else None
        output[label]={"change_pct":(quote["price"]/bars[baseline]-1)*100 if baseline else None,
                       "reference_at":baseline.isoformat() if baseline else None}
    return output


def build_global_futures():
    now=datetime.now(SHANGHAI)
    deadline=monotonic()+25
    errors=[]
    def safe(label,callback):
        try:
            return callback()
        except (requests.RequestException,ValueError,KeyError,TypeError,TimeoutError) as exc:
            errors.append({"source":label,"reason":str(exc)[:180]})
            return None
    def taiwan(market):
        body={"MarketType":str(market),"SymbolType":"F","KindID":"1","CID":"","ExpireMonth":"",
              "RowSize":"全部","PageNo":"","SortColumn":"","AscDesc":"A"}
        data=_json_reply(_request(TAIFEX+"getQuoteList",deadline,body=body))
        rows=data.get("QuoteList")
        if not isinstance(rows,list) or len(rows)>2000:
            raise ValueError("TAIFEX quote inventory differs")
        return _taifex_quote(rows,datetime.now(SHANGHAI),market)
    with ThreadPoolExecutor(max_workers=3) as pool:
        def sina_current():
            raw=_request("https://hq.sinajs.cn/list="+",".join("hf_"+code for code in SINA),deadline)
            return _sina_quotes(raw,datetime.now(SHANGHAI))
        raw=pool.submit(safe,"sina.hf",sina_current)
        taiwan_runs=[pool.submit(safe,"taifex",lambda market=market:taiwan(market)) for market in (0,1)]
        sina=raw.result() or {}
        native=[future.result() for future in taiwan_runs]
        native=[row for row in native if row]
        native=max(native,key=lambda row:row["market_time"]) if native else None
        quotes=[sina[code] for code in SINA if code in sina]+([native] if native else [])
        def minutes(quote):
            at=datetime.fromisoformat(quote["market_time"])
            if quote["source"]=="taifex.exchange":
                data=_json_reply(_request(TAIFEX+"getChartData1M",deadline,body={"SymbolID":quote["symbol"]}))
                return _taifex_minutes(data,quote,at)
            url="https://stock2.finance.sina.com.cn/futures/api/jsonp.php/=/GlobalFuturesService.getGlobalFuturesMinLine?symbol="+quote["symbol"][3:]
            return _sina_minutes(_request(url,deadline),at)
        pending=[pool.submit(safe,row["symbol"],lambda row=row:minutes(row)) for row in quotes]
        for row,future in zip(quotes,pending):
            bars=future.result()
            row["horizons"]=_horizons(row,bars or {})
            row["minute_count"]=len(bars or {})
            row["freshness"]="RECENT" if now-datetime.fromisoformat(row["market_time"])<=timedelta(minutes=5) else "LAST_OBSERVATION"
    now=datetime.now(SHANGHAI)
    return {"status":"available" if quotes else "unavailable","observation_date":now.date().isoformat(),
            "known_at":now.isoformat(timespec="seconds"),"evidence_mode":"CURRENT_OBSERVATION",
            "available_count":len(quotes),"expected_count":7,"rows":quotes,"source_errors":errors,
            "missing_instruments":[SINA[code] for code in SINA if code not in sina]+
                ([] if native else ["台指期货"])+["KOSPI200期货：尚未取得可核验来源，未用现货替代"],
            "method":"台指使用交易所同一时段成交量最大的TXF实际月份合约；其他标的是新浪明确标注的期货CFD参考报价。较前参考价变化与滚动窗口变化分列。",
            "horizon_method":"以报价时间为终点，取N分钟前最近的同一系列分钟价，偏差最多90秒；没有对应分钟数据时保留未知。不同到期月、日夜盘和现货均不拼接。此页为当前监测，不按全局历史日期回放。"}


@router.get("/research/global-futures")
def global_futures():
    return _serve(build_global_futures)

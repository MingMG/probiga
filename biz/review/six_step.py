"""Evidence-backed six-step A-share post-market review renderer."""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping

import pandas as pd
from sqlalchemy import bindparam, text


INDEX_NAMES = {
    "000001": "上证指数",
    "399001": "深证成指",
    "399006": "创业板指",
    "000688": "科创50",
    "899050": "北证50",
}
EVENT_LABELS = {
    "market_reversal": "市场转折",
    "sector_spread": "板块扩散",
    "sector_ebb": "板块退潮",
    "key_stock": "关键个股",
    "style_seesaw": "风格切换",
    "broad_index_support": "宽基托底",
}
NOTICE_RISK_WORDS = ("减持", "立案", "处罚", "问询", "终止", "亏损", "解禁", "质押")
NOTICE_CATALYST_WORDS = ("中标", "回购", "增持", "重组", "收购", "签订", "订单", "业绩预增")


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result and abs(result) != float("inf") else default


def _pct(value: Any) -> str:
    if value is None:
        return "未获取"
    return f"{_number(value):+.2f}%"


def _rate(value: Any) -> str:
    if value is None:
        return "未获取"
    return f"{_number(value):.2f}%"


def _amount(value: Any) -> str:
    if value is None:
        return "未获取"
    number = _number(value)
    return f"{number / 1e8:.2f}亿元" if number > 0 else "未获取"


def _plain(value: Any, default: str = "未获取") -> str:
    text_value = str(value or "").strip()
    return text_value.replace("|", "/").replace("\n", " ") if text_value else default


def _clock(value: Any) -> str:
    if value is None:
        return "未获取"
    try:
        stamp = pd.Timestamp(value)
        if pd.isna(stamp):
            return "未获取"
        return stamp.strftime("%H:%M")
    except (TypeError, ValueError):
        return "未获取"


def _json(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if value == value and abs(value) != float("inf") else None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    return str(value)


def _limit_pct(code: str, short_name: str) -> float:
    name = str(short_name or "").upper()
    if "ST" in name:
        return 5.0
    if str(code).startswith(("4", "8", "9")):
        return 30.0
    if str(code).startswith(("300", "301", "688")):
        return 20.0
    return 10.0


def _limit_price(pre_close: float, pct: float) -> float:
    if pre_close <= 0:
        return 0.0
    price = Decimal(str(pre_close)) * (
        Decimal("1") + Decimal(str(pct)) / Decimal("100")
    )
    return float(price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _read_frame(engine: Any, statement: Any, params: Mapping[str, Any] | None = None) -> pd.DataFrame:
    try:
        return pd.read_sql(statement, engine, params=dict(params or {}))
    except Exception:
        return pd.DataFrame()


def _stock_names(engine: Any) -> dict[str, str]:
    frame = _read_frame(
        engine,
        text(
            "SELECT stock_code, short_name FROM si_all_code "
            "ORDER BY stock_code"
        ),
    )
    if frame.empty:
        return {}
    return {
        str(row.stock_code).zfill(6): str(row.short_name or "").strip()
        for row in frame.itertuples(index=False)
    }


def _index_lines(engine: Any, target: date) -> tuple[list[str], list[dict[str, Any]]]:
    lines: list[str] = []
    snapshots: list[dict[str, Any]] = []
    for code, name in INDEX_NAMES.items():
        frame = _read_frame(
            engine,
            text(
                """
                SELECT trade_date, close, high, low, amount, change_pct
                FROM sm_index_kline
                WHERE index_code=:code AND k_type=1 AND trade_date<=:target
                ORDER BY trade_date DESC
                LIMIT 25
                """
            ),
            {"code": code, "target": target.isoformat()},
        )
        if frame.empty:
            lines.append(f"{name}：未获取")
            snapshots.append({"code": code, "name": name, "status": "未获取"})
            continue
        frame = frame.sort_values("trade_date")
        latest = frame.iloc[-1]
        latest_date = pd.Timestamp(latest["trade_date"]).date()
        if latest_date != target:
            lines.append(f"{name}：未获取（最新数据 {latest_date.isoformat()}）")
            snapshots.append({
                "code": code,
                "name": name,
                "status": "过期",
                "source_date": latest_date.isoformat(),
            })
            continue
        closes = pd.to_numeric(frame["close"], errors="coerce").dropna()
        close = _number(latest["close"])
        ma5 = float(closes.tail(5).mean()) if len(closes) >= 5 else None
        ma10 = float(closes.tail(10).mean()) if len(closes) >= 10 else None
        ma20 = float(closes.tail(20).mean()) if len(closes) >= 20 else None
        support = max((item for item in (ma5, ma10, ma20) if item and item <= close), default=None)
        low20 = _number(pd.to_numeric(frame["low"], errors="coerce").tail(20).min(), 0)
        high20 = _number(pd.to_numeric(frame["high"], errors="coerce").tail(20).max(), 0)
        support = support or (low20 or None)
        snap = {
            "code": code,
            "name": name,
            "status": "已获取",
            "close": close,
            "change_pct": _number(latest.get("change_pct")),
            "amount": _number(latest.get("amount")),
            "support": support,
            "resistance": high20 or None,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
        }
        snapshots.append(snap)
        lines.append(
            f"{name} {close:.2f}点 {_pct(snap['change_pct'])}，"
            f"成交额 {_amount(snap['amount'])}"
        )
    return lines, snapshots


def _sector_rank(engine: Any, target: date, previous: date | None) -> dict[str, Any]:
    def load(day: date | None) -> list[dict[str, Any]]:
        if day is None:
            return []
        frame = _read_frame(
            engine,
            text(
                """
                SELECT concept_code, concept_name, change_pct, hot_value
                FROM st_hot_concept_ths_daily
                WHERE snapshot_date=:d
                ORDER BY change_pct DESC
                """
            ),
            {"d": day.isoformat()},
        )
        if frame.empty:
            return []
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in frame.to_dict(orient="records"):
            name = str(row.get("concept_name") or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            result.append({
                "name": name,
                "concept_code": str(row.get("concept_code") or "").strip(),
                "change_pct": _number(row.get("change_pct")),
                "hot_value": _number(row.get("hot_value")),
            })
        return result

    current = load(target)
    prior = load(previous)
    positive = [item for item in current if item["change_pct"] > 0]
    return {
        "positive": positive,
        "top": positive[:8],
        "weak": sorted(current, key=lambda item: item["change_pct"])[:8],
        "previous_top_names": {item["name"] for item in prior[:10]},
        "status": "available" if current else "unavailable",
    }


def _memberships(
    engine: Any,
    *,
    sector_names: Iterable[str],
    sector_codes: Mapping[str, str],
    stock_codes: Iterable[str],
    target: date,
) -> tuple[dict[str, set[str]], dict[str, set[str]], str]:
    names = sorted({str(item).strip() for item in sector_names if str(item).strip()})
    name_by_sector_code = {
        str(code).strip(): str(name).strip()
        for code, name in sector_codes.items()
        if str(code).strip() and str(name).strip()
    }
    sector_code_values = sorted(name_by_sector_code)
    codes = sorted({str(item).zfill(6) for item in stock_codes if str(item).strip()})
    by_stock: dict[str, set[str]] = defaultdict(set)
    by_sector: dict[str, set[str]] = defaultdict(set)

    def add(stock: Any, sector: Any) -> None:
        code = str(stock or "").strip().zfill(6)
        name = str(sector or "").strip()
        if len(code) == 6 and code.isdigit() and name:
            by_stock[code].add(name)
            by_sector[name].add(code)

    sources = (
        ("si_stock_plate_east", "plate_code", "plate_name"),
        ("si_stock_concept_east", "concept_code", "name"),
    )
    for table, code_column, name_column in sources:
        if names:
            statement = text(
                f"SELECT stock_code, {name_column} AS sector_name FROM {table} "
                f"WHERE {name_column} IN :names"
            ).bindparams(bindparam("names", expanding=True))
            frame = _read_frame(engine, statement, {"names": names})
            for row in frame.to_dict(orient="records"):
                add(row.get("stock_code"), row.get("sector_name"))
        if sector_code_values:
            frame = _read_frame(
                engine,
                text(
                    f"SELECT stock_code, {code_column} AS sector_code, "
                    f"{name_column} AS sector_name FROM {table} "
                    f"WHERE {code_column} IN :sector_codes"
                ).bindparams(bindparam("sector_codes", expanding=True)),
                {"sector_codes": sector_code_values},
            )
            for row in frame.to_dict(orient="records"):
                raw_code = str(row.get("sector_code") or "").strip()
                sector_name = name_by_sector_code.get(raw_code) or row.get("sector_name")
                add(row.get("stock_code"), sector_name)
        if codes:
            statement = text(
                f"SELECT stock_code, {name_column} AS sector_name FROM {table} "
                "WHERE stock_code IN :codes"
            ).bindparams(bindparam("codes", expanding=True))
            frame = _read_frame(engine, statement, {"codes": codes})
            for row in frame.to_dict(orient="records"):
                add(row.get("stock_code"), row.get("sector_name"))
    membership_source = "东方财富板块/概念映射" if by_sector else ""
    # The THS heat names are concept names. Use the newest exact-day QMT
    # membership snapshot within the prior week when the current-day snapshot
    # is unavailable; the report exposes that source date instead of
    # pretending the mapping is same-day.
    for offset in range(0, 8):
        snapshot = target - timedelta(days=offset)
        frames: list[pd.DataFrame] = []
        if names:
            frames.append(_read_frame(
                engine,
                text(
                    """
                    SELECT stock_code, concept_name AS sector_name
                    FROM qmt_concept_member_snapshot
                    WHERE snapshot_date=:d AND concept_name IN :names
                    """
                ).bindparams(bindparam("names", expanding=True)),
                {"d": snapshot.isoformat(), "names": names},
            ))
        if codes:
            frames.append(_read_frame(
                engine,
                text(
                    """
                    SELECT stock_code, concept_name AS sector_name
                    FROM qmt_concept_member_snapshot
                    WHERE snapshot_date=:d AND stock_code IN :codes
                    """
                ).bindparams(bindparam("codes", expanding=True)),
                {"d": snapshot.isoformat(), "codes": codes},
            ))
        if sector_code_values:
            frames.append(_read_frame(
                engine,
                text(
                    """
                    SELECT stock_code, concept_code AS sector_code,
                           concept_name AS sector_name
                    FROM qmt_concept_member_snapshot
                    WHERE snapshot_date=:d AND concept_code IN :sector_codes
                    """
                ).bindparams(bindparam("sector_codes", expanding=True)),
                {
                    "d": snapshot.isoformat(),
                    "sector_codes": sector_code_values,
                },
            ))
        nonempty = [item for item in frames if not item.empty]
        if not nonempty:
            continue
        for frame in nonempty:
            for row in frame.to_dict(orient="records"):
                raw_code = str(row.get("sector_code") or "").strip()
                sector_name = (
                    name_by_sector_code.get(raw_code)
                    or row.get("sector_name")
                )
                add(row.get("stock_code"), sector_name)
        membership_source = f"QMT概念快照 {snapshot.isoformat()}"
        break
    return by_stock, by_sector, membership_source


def _daily_leaders(
    bars: pd.DataFrame,
    history: pd.DataFrame,
    names: Mapping[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if bars.empty or not {"stock_code", "close", "pre_close", "high", "amount"}.issubset(bars.columns):
        return [], []
    frame = bars.copy()
    frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
    for column in ("close", "pre_close", "high", "amount", "turnover_ratio"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame[frame["close"].gt(0) & frame["pre_close"].gt(0)].copy()
    frame["stock_name"] = frame["stock_code"].map(names).fillna(frame["stock_code"])
    frame["change_pct_calc"] = (
        frame["close"] / frame["pre_close"] - 1.0
    ) * 100.0
    frame["limit_pct"] = [
        _limit_pct(code, name)
        for code, name in zip(frame["stock_code"], frame["stock_name"])
    ]
    frame["limit_price"] = [
        _limit_price(pre_close, pct)
        for pre_close, pct in zip(frame["pre_close"], frame["limit_pct"])
    ]
    frame["is_limit_up"] = frame["change_pct_calc"] >= frame["limit_pct"] - 0.15
    frame["touched_limit"] = frame["high"] >= frame["limit_price"] - 0.005

    history_frame = history.copy() if isinstance(history, pd.DataFrame) else pd.DataFrame()
    current_history = frame.loc[:, ["stock_code", "trade_date", "close", "pre_close"]].copy()
    if history_frame.empty:
        history_frame = current_history
    else:
        history_frame = pd.concat(
            [history_frame, current_history],
            ignore_index=True,
        ).drop_duplicates(["stock_code", "trade_date"], keep="last")
    streak_map: dict[str, int] = {}
    if not history_frame.empty:
        history_frame["stock_code"] = history_frame["stock_code"].astype(str).str.zfill(6)
        history_frame["close"] = pd.to_numeric(history_frame["close"], errors="coerce")
        history_frame["pre_close"] = pd.to_numeric(history_frame["pre_close"], errors="coerce")
        history_frame["pct"] = (
            history_frame["close"] / history_frame["pre_close"] - 1.0
        ) * 100.0
        for code, group in history_frame.groupby("stock_code"):
            rows = group.sort_values("trade_date", ascending=False)
            streak = 0
            threshold = _limit_pct(code, names.get(code, ""))
            for value in rows["pct"].tolist():
                if _number(value) >= threshold - 0.15:
                    streak += 1
                else:
                    break
            streak_map[code] = streak

    def convert(row: Mapping[str, Any], status: str) -> dict[str, Any]:
        return {
            "code": row["stock_code"],
            "name": row["stock_name"],
            "change_pct": _number(row["change_pct_calc"]),
            "amount": _number(row["amount"]),
            "turnover_ratio": (
                _number(row.get("turnover_ratio"))
                if pd.notna(row.get("turnover_ratio"))
                else None
            ),
            "close": _number(row["close"]),
            "pre_close": _number(row["pre_close"]),
            "limit_price": _number(row["limit_price"]),
            "boards": streak_map.get(row["stock_code"], 1),
            "status": status,
        }

    leaders = [
        convert(row, "涨停")
        for row in frame[frame["is_limit_up"]]
        .sort_values("amount", ascending=False)
        .head(10)
        .to_dict(orient="records")
    ]
    broken = [
        convert(row, "炸板")
        for row in frame[frame["touched_limit"] & ~frame["is_limit_up"]]
        .sort_values(["change_pct_calc", "amount"], ascending=[True, False])
        .head(10)
        .to_dict(orient="records")
    ]
    return leaders, broken


def _minute_times(
    engine: Any,
    target: date,
    items: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    codes = [item["code"] for item in items]
    if not codes:
        return {}
    frame = _read_frame(
        engine,
        text(
            """
            SELECT stock_code, trade_time, price
            FROM sm_stock_minute
            WHERE trade_date=:d AND data_source='qmt_datadir'
              AND stock_code IN :codes
            ORDER BY stock_code, trade_time
            """
        ).bindparams(bindparam("codes", expanding=True)),
        {"d": target.isoformat(), "codes": codes},
    )
    if frame.empty:
        return {}
    result: dict[str, dict[str, str]] = {}
    by_code = {item["code"]: item for item in items}
    for code, rows in frame.groupby("stock_code"):
        item = by_code.get(str(code))
        if not item:
            continue
        prices = pd.to_numeric(rows["price"], errors="coerce")
        times = list(rows["trade_time"])
        if item["status"] in {"涨停", "炸板"}:
            hit = (prices >= item["limit_price"] - 0.005).to_numpy().nonzero()[0]
            if len(hit):
                first = _clock(times[int(hit[0])])
                last = _clock(times[int(hit[-1])])
                broke = item["status"] == "炸板" or int(hit[-1]) < len(rows) - 1
                result[item["code"]] = {
                    "first": first,
                    "last": last,
                    "state": "炸板" if broke else "封板",
                }
        else:
            threshold = item["pre_close"] * 0.95
            hit = (prices <= threshold).to_numpy().nonzero()[0]
            if len(hit):
                result[item["code"]] = {
                    "first": _clock(times[int(hit[0])]),
                    "last": _clock(times[int(hit[-1])]),
                    "state": "走弱",
                }
    return result


def _event_rows(engine: Any, target: date) -> list[dict[str, str]]:
    frame = _read_frame(
        engine,
        text(
            """
            SELECT first_seen_at, last_seen_at, event_type, subject_code,
                   subject_name, state, direction, evidence_json
            FROM st_intraday_alert_state
            WHERE trade_date=:d
            ORDER BY first_seen_at, severity DESC
            LIMIT 40
            """
        ),
        {"d": target.isoformat()},
    )
    rows: list[dict[str, str]] = []
    for row in frame.to_dict(orient="records"):
        evidence = {}
        try:
            evidence = json.loads(str(row.get("evidence_json") or "{}"))
        except (TypeError, ValueError):
            evidence = {}
        facts = evidence.get("facts") if isinstance(evidence, dict) else None
        if isinstance(facts, (list, tuple)):
            fact_text = "；".join(_plain(item) for item in facts[:3])
        else:
            fact_text = _plain(evidence.get("evidence") if isinstance(evidence, dict) else "")
        cause = ""
        if isinstance(evidence, dict):
            cause = _plain(
                evidence.get("causal_chain")
                or evidence.get("inference")
                or evidence.get("judgment"),
                "",
            )
        state = _plain(row.get("state"))
        rows.append({
            "time": _clock(row.get("first_seen_at")),
            "event_type": EVENT_LABELS.get(
                str(row.get("event_type") or ""),
                _plain(row.get("event_type")),
            ),
            "subject": (
                f"{_plain(row.get('subject_name'), _plain(row.get('subject_code')))}"
                f"({_plain(row.get('subject_code'))})"
            ),
            "sector": "未获取",
            "evidence": fact_text,
            "cause": cause or "事件前后关系未获取",
            "result": f"状态 {state}；最后观察 {_clock(row.get('last_seen_at'))}",
        })
    return rows


def _minute_event_rows(
    leaders: list[dict[str, Any]],
    broken: list[dict[str, Any]],
    smashers: list[dict[str, Any]],
    times: Mapping[str, Mapping[str, str]],
    by_stock: Mapping[str, set[str]],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    leader_clock: dict[str, tuple[str, list[str]]] = {}
    for item in leaders:
        timing = times.get(item["code"], {})
        sectors = sorted(by_stock.get(item["code"], set()))
        rows.append({
            "time": timing.get("first", "未获取"),
            "event_type": "异动启动",
            "subject": f"{item['name']}({item['code']})",
            "sector": "、".join(sectors[:2]) or "未获取",
            "evidence": f"日内涨幅 {_pct(item['change_pct'])}，成交额 {_amount(item['amount'])}",
            "cause": "分钟先后关系待与同板块跟随股核对",
            "result": f"{timing.get('state', item['status'])}；最后涨停时间 {timing.get('last', '未获取')}",
        })
        if timing.get("first"):
            leader_clock[item["code"]] = (timing["first"], sectors)
    for item in broken:
        timing = times.get(item["code"], {})
        sectors = sorted(by_stock.get(item["code"], set()))
        rows.append({
            "time": timing.get("first", "未获取"),
            "event_type": "分歧转弱",
            "subject": f"{item['name']}({item['code']})",
            "sector": "、".join(sectors[:2]) or "未获取",
            "evidence": f"最高价触及涨停价，收盘涨幅 {_pct(item['change_pct'])}，成交额 {_amount(item['amount'])}",
            "cause": "同板块跟随与炸板先后时间未获取",
            "result": f"{timing.get('state', '炸板')}；最后触及涨停 {timing.get('last', '未获取')}",
        })
    for item in smashers:
        timing = times.get(item["code"], {})
        sectors = sorted(by_stock.get(item["code"], set()))
        rows.append({
            "time": timing.get("first", "未获取"),
            "event_type": "拖累候选",
            "subject": f"{item['name']}({item['code']})",
            "sector": "、".join(sectors[:2]) or "未获取",
            "evidence": f"收盘涨幅 {_pct(item['change_pct'])}，成交额 {_amount(item['amount'])}",
            "cause": "仅识别走弱对象，板块拖累因果未获取",
            "result": timing.get("state", "日线走弱"),
        })
    rows.sort(key=lambda item: (item["time"] == "未获取", item["time"]))
    for row in rows:
        if not row["cause"].startswith("分钟先后关系待"):
            continue
        candidates = []
        for code, (clock, sectors) in leader_clock.items():
            if clock <= row["time"]:
                continue
            if set(sectors) & set(part.strip() for part in row["sector"].split("、")):
                candidates.append(clock)
        if candidates:
            row["cause"] = (
                f"同板块后续出现涨停跟随，最早 {min(candidates)}；"
                "仅证明时间先后，不足以单独证明因果"
            )
    return rows


def _fund_section(
    engine: Any,
    target: date,
    names: Mapping[str, str],
    by_stock: Mapping[str, set[str]],
) -> dict[str, Any]:
    frame = _read_frame(
        engine,
        text(
            "SELECT stock_code, main_net_inflow FROM sm_stock_capital_flow_daily "
            "WHERE trade_date=:d"
        ),
        {"d": target.isoformat()},
    )
    inflow: list[dict[str, Any]] = []
    outflow: list[dict[str, Any]] = []
    if not frame.empty:
        frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
        frame["main_net_inflow"] = pd.to_numeric(frame["main_net_inflow"], errors="coerce")
        frame = frame.dropna(subset=["main_net_inflow"])

        def convert(row: Mapping[str, Any]) -> dict[str, Any]:
            code = str(row["stock_code"])
            return {
                "code": code,
                "name": names.get(code, code),
                "value": _number(row["main_net_inflow"]),
                "sector": "、".join(sorted(by_stock.get(code, set()))[:2]) or "未获取",
            }

        inflow = [
            convert(row)
            for row in frame.sort_values("main_net_inflow", ascending=False)
            .head(5)
            .to_dict(orient="records")
        ]
        outflow = [
            convert(row)
            for row in frame.sort_values("main_net_inflow")
            .head(5)
            .to_dict(orient="records")
        ]
    north = None
    north_frame = _read_frame(
        engine,
        text(
            "SELECT net_hgt, net_sgt, net_tgt FROM st_north_flow_daily "
            "WHERE trade_date=:d LIMIT 1"
        ),
        {"d": target.isoformat()},
    )
    if not north_frame.empty:
        row = north_frame.iloc[0]
        direct_total = row.get("net_tgt")
        north = (
            _number(direct_total)
            if direct_total is not None and pd.notna(direct_total)
            else _number(row.get("net_hgt")) + _number(row.get("net_sgt"))
        )
        if north == 0:
            north = None

    longhubang: list[dict[str, Any]] = []
    dragon_frame = _read_frame(
        engine,
        text(
            "SELECT stock_code, a_net_amount, reason FROM st_a_list_daily "
            "WHERE trade_date=:d ORDER BY ABS(a_net_amount) DESC LIMIT 8"
        ),
        {"d": target.isoformat()},
    )
    if not dragon_frame.empty:
        for row in dragon_frame.to_dict(orient="records"):
            code = str(row.get("stock_code") or "").zfill(6)
            longhubang.append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "value": _number(row.get("a_net_amount")),
                    "reason": _plain(row.get("reason")),
                }
            )

    notices = _read_frame(
        engine,
        text(
            """
            SELECT stock_code, title, display_time
            FROM si_notice_eastmoney
            WHERE notice_date=:d AND association_validated=1
            ORDER BY display_time DESC
            LIMIT 12
            """
        ),
        {"d": target.isoformat()},
    )
    notice_items = []
    if not notices.empty:
        for row in notices.to_dict(orient="records"):
            title = _plain(row.get("title"))
            status = (
                "风险词"
                if any(word in title for word in NOTICE_RISK_WORDS)
                else "催化词"
                if any(word in title for word in NOTICE_CATALYST_WORDS)
                else "中性"
            )
            code = str(row.get("stock_code") or "").zfill(6)
            notice_items.append({
                "code": code,
                "name": names.get(code, code),
                "title": title,
                "status": status,
                "time": _clock(row.get("display_time")),
            })
    return {
        "inflow": inflow,
        "outflow": outflow,
        "north": north,
        "longhubang": longhubang,
        "notices": notice_items,
    }


def _watch_pool(
    bars: pd.DataFrame,
    *,
    sectors: list[dict[str, Any]],
    by_sector: Mapping[str, set[str]],
    names: Mapping[str, str],
) -> list[dict[str, Any]]:
    if bars.empty:
        return []
    frame = bars.copy()
    frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
    frame["name"] = frame["stock_code"].map(names).fillna(frame["stock_code"])
    frame["pct"] = (
        pd.to_numeric(frame["close"], errors="coerce")
        / pd.to_numeric(frame["pre_close"], errors="coerce")
        - 1.0
    ) * 100.0
    frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce")
    if "turnover_ratio" in frame:
        frame["turnover_ratio"] = pd.to_numeric(frame["turnover_ratio"], errors="coerce")
    frame = frame[
        frame["pct"].notna()
        & frame["amount"].gt(0)
        & ~frame["name"].astype(str).str.contains("ST|退", regex=True, na=False)
    ]
    by_code = frame.set_index("stock_code", drop=False)
    selected_codes: set[str] = set()
    output: list[dict[str, Any]] = []
    for sector in sectors:
        members = by_sector.get(sector["name"], set())
        if not members:
            continue
        candidates = by_code[
            by_code.index.isin(members) & ~by_code.index.isin(selected_codes)
        ].copy()
        if candidates.empty:
            continue
        candidates["limit_pct"] = [
            _limit_pct(code, name)
            for code, name in zip(candidates["stock_code"], candidates["name"])
        ]
        candidates["is_limit"] = candidates["pct"] >= candidates["limit_pct"] - 0.15
        candidates = candidates.sort_values(
            ["is_limit", "pct", "amount"],
            ascending=[False, False, False],
        )
        stocks = []
        for row in candidates.head(4).to_dict(orient="records"):
            code = str(row["stock_code"])
            if code in selected_codes:
                continue
            selected_codes.add(code)
            turnover = (
                f"{_number(row.get('turnover_ratio')):.2f}%"
                if pd.notna(row.get("turnover_ratio"))
                else "未获取"
            )
            stocks.append({
                "code": code,
                "name": name_for(row, names),
                "evidence": (
                    f"涨跌幅 {_pct(row['pct'])}；成交额 {_amount(row['amount'])}；"
                    f"换手率 {turnover}"
                ),
                "observation": (
                    "观察竞价承接、开盘后量能能否延续"
                    if not bool(row["is_limit"])
                    else "观察开板后回封强度与板块跟随"
                ),
                "risk": (
                    "高位连板波动"
                    if bool(row["is_limit"])
                    else "板块分歧或成交额萎缩"
                ),
                "status": "已获取",
            })
            if len(stocks) >= 3:
                break
        if stocks:
            output.append({
                "sector": sector["name"],
                "catalyst": (
                    f"板块涨幅 {_pct(sector['change_pct'])}，"
                    f"热度值 {_number(sector['hot_value']):.1f}"
                ),
                "stocks": stocks,
            })
        if len(output) >= 5:
            break
    return output


def name_for(row: Mapping[str, Any], names: Mapping[str, str]) -> str:
    code = str(row.get("stock_code") or "")
    return str(row.get("name") or names.get(code, code))


def _mainline_rows(
    sectors: list[dict[str, Any]],
    *,
    bars: pd.DataFrame,
    by_sector: Mapping[str, set[str]],
    names: Mapping[str, str],
    previous_top_names: set[str],
) -> list[dict[str, Any]]:
    if bars.empty:
        return []
    frame = bars.copy()
    frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
    frame["pct"] = (
        pd.to_numeric(frame["close"], errors="coerce")
        / pd.to_numeric(frame["pre_close"], errors="coerce")
        - 1.0
    ) * 100.0
    frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce")
    frame["limit_pct"] = [
        _limit_pct(code, names.get(code, ""))
        for code in frame["stock_code"]
    ]
    frame["is_limit"] = frame["pct"] >= frame["limit_pct"] - 0.15
    by_code = frame.set_index("stock_code", drop=False)
    rows: list[dict[str, Any]] = []
    for sector in sectors[:8]:
        members = by_sector.get(sector["name"], set())
        if not members:
            rows.append({
                **sector,
                "reason": "板块内部映射未获取",
                "leader": "未获取",
                "limit_up_count": None,
                "persistence": "未获取",
                "status": "映射未获取",
            })
            continue
        subset = by_code[by_code.index.isin(members)]
        limit_count = int(subset["is_limit"].sum()) if not subset.empty else 0
        leader = None
        if not subset.empty:
            lead = subset.sort_values(["is_limit", "amount"], ascending=[False, False]).iloc[0]
            leader = f"{names.get(str(lead['stock_code']), str(lead['stock_code']))}({lead['stock_code']}) {_pct(lead['pct'])}"
        rows.append({
            **sector,
            "reason": (
                f"板块{pct_text(sector['change_pct'])}；"
                f"成分股涨停 {limit_count} 只；领涨股 {leader or '未获取'}"
            ),
            "leader": leader or "未获取",
            "limit_up_count": limit_count,
            "persistence": (
                "延续"
                if sector["name"] in previous_top_names
                else "新启动"
            ),
            "status": "已获取",
        })
    return rows


def pct_text(value: Any) -> str:
    return _pct(value)


def _render_timeline(rows: list[Mapping[str, str]]) -> str:
    lines = [
        "| 时间 | 事件类型 | 触发个股 | 带动/拖累板块 | 数据依据 | 因果链 | 结果 |",
        "|---|---|---|---|---|---|---|",
    ]
    if not rows:
        lines.append(
            "| 未获取 | 未获取 | 未获取 | 未获取 | 分时事件与分钟数据均未获取 | 未获取 | 未获取 |"
        )
        return "\n".join(lines)
    for row in rows[:20]:
        lines.append(
            "| "
            + " | ".join(
                _plain(row.get(key))
                for key in ("time", "event_type", "subject", "sector", "evidence", "cause", "result")
            )
            + " |"
        )
    return "\n".join(lines)


def render_six_step(payload: Mapping[str, Any]) -> str:
    market = payload.get("market") or {}
    breadth = payload.get("breadth") or {}
    board = payload.get("board") or {}
    indices = payload.get("indices") or []
    index_lines = payload.get("index_lines") or []
    sectors = payload.get("sectors") or {}
    mainlines = payload.get("mainlines") or []
    leaders = payload.get("leaders") or []
    leaders_detail = payload.get("leader_details") or {}
    smashers = payload.get("smashers") or []
    timeline = payload.get("timeline") or []
    flows = payload.get("flows") or {}
    watch = payload.get("watch_pool") or []
    cycle = payload.get("cycle") or {}
    notes = payload.get("notes") or []

    tone = breadth.get("status", "未获取")
    cycle_name = cycle.get("cycle", "未获取")
    median = market.get("return_median_pct")
    up_cov = market.get("up_coverage")
    amount_change = market.get("amount_change_pct")
    amount = market.get("total_amount")
    lines = [f"# A股收盘复盘｜{payload.get('date', '未获取')}", "", "## 一、今日基调", ""]
    if index_lines:
        lines.extend(f"- {line}" for line in index_lines)
    lines.extend([
        f"- 成交与广度：两市成交额 {_amount(amount)}，较前一交易日 {_pct(amount_change)}；"
        f"上涨家数占比 {(_number(up_cov) * 100):.2f}%，个股涨跌幅中位数 {_pct(median)}。",
        f"- 情绪数据：涨停 {board.get('limit_up_count', '未获取')} 家、跌停 {board.get('limit_down_count', '未获取')} 家，"
            f"炸板 {board.get('broken_board_count', '未获取')} 家，炸板率 "
            f"{_rate(board.get('broken_rate'))}，"
        f"最高连板 {board.get('max_boards', '未获取')} 板。",
        f"- 一句话：今天是**{tone}**，情绪阶段为**{_plain(cycle_name)}**。",
        "",
        "## 二、主线与板块",
        "",
    ])
    top = sectors.get("top", [])
    weak = sectors.get("weak", [])
    lines.append(
        "- 涨幅前列："
        + ("、".join(f"{item['name']} {_pct(item['change_pct'])}" for item in top[:5]) or "未获取")
    )
    lines.append(
        "- 跌幅前列："
        + ("、".join(f"{item['name']} {_pct(item['change_pct'])}" for item in weak[:5]) or "未获取")
    )
    mapped_mainlines = [
        item for item in mainlines if item.get("status") == "已获取"
    ]
    if mapped_mainlines:
        for item in mapped_mainlines[:5]:
            lines.append(
                f"- {item['name']}：{_plain(item.get('reason'))}；"
                f"持续性 {_plain(item.get('persistence'))}。"
            )
        drivers = [item["name"] for item in mapped_mainlines[:5]]
        if drivers:
            lines.append(
                "- 盘面归因：上涨贡献集中在"
                + "、".join(drivers)
                + "及其前排个股。当前数据只能确认价格、成交额和板块内部跟随，"
                "不能据此识别具体资金身份或单条新闻的因果。"
            )
    else:
        lines.append("- 主线与板块驱动：未获取")
    lines.extend(["", "## 三、龙头与个股", ""])
    if leaders:
        for item in leaders[:5]:
            detail = leaders_detail.get(item["code"], {})
            lines.append(
                f"- {item['name']}（{item['code']}）：涨幅 {_pct(item['change_pct'])}，"
                f"成交额 {_amount(item['amount'])}，板块 "
                f"{'、'.join(detail.get('sectors', [])) or '未获取'}；"
                f"首次触及涨停 {detail.get('first', '未获取')}，"
                f"最后涨停 {detail.get('last', '未获取')}，状态 {detail.get('state', item['status'])}。"
            )
    else:
        lines.append("- 龙头与涨停个股：未获取")
    if smashers:
        lines.append(
            "- 日线拖累候选："
            + "、".join(
                f"{item['name']}({item['code']}) {_pct(item['change_pct'])}"
                f"[{'、'.join(item.get('sectors', [])) or '板块未获取'}]"
                for item in smashers[:5]
            )
            + "。仅能确认跌幅与成交额，具体砸盘时点以分时证据为准。"
        )
    lines.extend(["", "## 四、盘中时间轴（核心）", "", _render_timeline(timeline), ""])
    timed_timeline = [
        item for item in timeline if _plain(item.get("time")) != "未获取"
    ]
    if timed_timeline:
        lines.append(
            "- 分水岭：按最早可验证事件为 "
            + _plain(timed_timeline[0].get("time"))
            + " 的 "
            + _plain(timed_timeline[0].get("subject"))
            + "；后续因果只采用有分钟先后与板块联动证据的事件。"
        )
    else:
        lines.append("- 分水岭：未获取")
    lines.extend(["", "## 五、资金与消息", ""])
    if flows.get("inflow"):
        lines.append(
            "- 个股主力净流入靠前："
            + "、".join(
                f"{item['name']}({item['code']}) {item['value'] / 1e8:+.2f}亿元"
                for item in flows["inflow"][:5]
            )
        )
    else:
        lines.append("- 个股主力净流入：未获取")
    if flows.get("outflow"):
        lines.append(
            "- 个股主力净流出靠前："
            + "、".join(
                f"{item['name']}({item['code']}) {item['value'] / 1e8:+.2f}亿元"
                for item in flows["outflow"][:5]
            )
        )
    else:
        lines.append("- 个股主力净流出：未获取")
    if flows.get("north") is not None:
        lines.append(
            f"- 北向净额：{_number(flows.get('north')) / 1e8:+.2f}亿元"
            "（同花顺收盘净额口径）；ETF 净流入/流出：未获取。"
        )
    else:
        lines.append("- 北向净额：未获取（公开源未披露有效净额）；ETF 净流入/流出：未获取。")
    if flows.get("longhubang"):
        lines.append(
            "- 龙虎榜净额靠前："
            + "、".join(
                f"{item['name']}({item['code']}) "
                f"{item['value'] / 1e8:+.2f}亿元"
                for item in flows["longhubang"][:5]
            )
            + "。"
        )
    else:
        lines.append("- 龙虎榜：未获取（当日榜单可能尚未发布）。")
    if flows.get("notices"):
        lines.append(
            "- 盘后公告："
            + "；".join(
                f"{item['name']}({item['code']}) {item['title']}[{item['status']}]"
                for item in flows["notices"][:5]
            )
        )
    else:
        lines.append("- 盘后公告：未获取")
    lines.extend(["", "## 六、明日推演", ""])
    valid_indices = [item for item in indices if item.get("status") == "已获取"]
    if valid_indices:
        for item in valid_indices:
            lines.append(
                f"- {item['name']}：支撑 {item.get('support') or '未获取'}，"
                f"压力 {item.get('resistance') or '未获取'}。"
            )
    else:
        lines.append("- 指数关键支撑/压力：未获取（目标日指数数据缺失或过期）")
    lines.append(
        "- 可能轮动方向："
        + ("、".join(item["name"] for item in mapped_mainlines[:5]) or "未获取")
        + "。"
    )
    risks = []
    if _number(board.get("broken_rate")) >= 30:
        risks.append("炸板率偏高")
    if _number(board.get("max_boards")) >= 5:
        risks.append("高位连板波动")
    if _number(amount_change) < 0:
        risks.append("成交额萎缩")
    if flows.get("notices"):
        risks.extend(
            f"{item['name']}公告风险"
            for item in flows["notices"]
            if item["status"] == "风险词"
        )
    lines.append("- 需要回避的风险：" + ("、".join(dict.fromkeys(risks)) or "未获取"))
    lines.extend(["", "### 明日观察池", ""])
    if watch:
        lines.extend([
            "| 板块 | 催化 | 观察个股（代码+名称） | 入选依据 | 观察点 | 风险 | 数据状态 |",
            "|---|---|---|---|---|---|---|",
        ])
        for sector in watch[:5]:
            for stock in sector["stocks"][:4]:
                lines.append(
                    f"| {_plain(sector['sector'])} | {_plain(sector['catalyst'])} | "
                    f"{_plain(stock['code'])} {_plain(stock['name'])} | "
                    f"{_plain(stock['evidence'])} | {_plain(stock['observation'])} | "
                    f"{_plain(stock['risk'])} | {_plain(stock['status'])} |"
                )
        lines.append("- 以上个股均为【观察池，不构成推荐】。")
    else:
        lines.append("- 今日无高确定性板块映射，观察池为空。")
    if notes:
        lines.append("- 数据口径：" + "；".join(dict.fromkeys(str(item) for item in notes if item)) + "。")
    lines.extend(["", "## 七、一句话总结", ""])
    first = timed_timeline[0] if timed_timeline else None
    primary_mainline = (
        mapped_mainlines[0]["name"]
        if mapped_mainlines
        else mainlines[0]["name"]
        if mainlines
        else "主线"
    )
    if first:
        lines.append(
            f"今天最重要的分水岭是 {_plain(first.get('time'))} 的 {_plain(first.get('subject'))}，"
            f"明天优先核对 {primary_mainline} 的承接、"
            f"炸板率与成交额是否同步改善。"
        )
    else:
        lines.append(
            f"今天缺少可验证的分钟级分水岭，明天优先核对"
            f"{primary_mainline}的承接、炸板率与成交额是否同步改善。"
        )
    lines.extend(["", "以上为信息整理，不构成投资建议。"])
    return "\n".join(lines).strip()


def build_six_step_review(
    engine: Any,
    date_str: str,
    *,
    digest: Mapping[str, Any],
    target_bars: pd.DataFrame,
    previous_bars: pd.DataFrame,
    history_bars: pd.DataFrame | None = None,
) -> str:
    """Load optional attribution inputs and render the six-step review."""

    target = date.fromisoformat(date_str)
    quality = dict(digest.get("quality_json") or {})
    previous_text = str((quality.get("source_dates") or {}).get("previous_bars") or "")
    try:
        previous = date.fromisoformat(previous_text)
    except ValueError:
        previous = target - timedelta(days=1)
    market = dict(digest.get("market_structure_json") or {})
    names = _stock_names(engine)
    index_lines, indices = _index_lines(engine, target)
    sector_data = _sector_rank(engine, target, previous)
    leaders, broken = _daily_leaders(
        target_bars,
        history_bars if isinstance(history_bars, pd.DataFrame) else pd.DataFrame(),
        names,
    )
    smashers = []
    if isinstance(target_bars, pd.DataFrame) and not target_bars.empty:
        loser_frame = target_bars.copy()
        loser_frame["stock_code"] = (
            loser_frame["stock_code"].astype(str).str.zfill(6)
        )
        loser_frame["pct"] = (
            pd.to_numeric(loser_frame["close"], errors="coerce")
            / pd.to_numeric(loser_frame["pre_close"], errors="coerce")
            - 1.0
        ) * 100.0
        loser_frame["amount"] = pd.to_numeric(
            loser_frame["amount"], errors="coerce"
        )
        for row in (
            loser_frame.sort_values(
                ["pct", "amount"], ascending=[True, False]
            )
            .head(8)
            .to_dict(orient="records")
        ):
            code = str(row["stock_code"])
            smashers.append({
                "code": code,
                "name": names.get(code, code),
                "change_pct": _number(row["pct"]),
                "amount": _number(row["amount"]),
                "pre_close": _number(row["pre_close"]),
                "status": "走弱",
            })
    all_codes = {
        str(item["stock_code"])
        for item in target_bars.to_dict(orient="records")
        if str(item.get("stock_code") or "").strip()
    } if isinstance(target_bars, pd.DataFrame) and not target_bars.empty else set()
    candidate_sectors = sector_data["positive"][:30]
    sector_names = [item["name"] for item in candidate_sectors]
    leader_codes = [
        item["code"]
        for item in leaders[:10] + broken[:10] + smashers[:8]
    ]
    by_stock, watch_by_sector, membership_source = _memberships(
        engine,
        sector_names=sector_names,
        sector_codes={
            item["concept_code"]: item["name"]
            for item in candidate_sectors
            if item.get("concept_code")
        },
        stock_codes=leader_codes,
        target=target,
    )
    for name, codes in watch_by_sector.items():
        for code in codes:
            by_stock[code].add(name)
    if not all_codes:
        all_codes = set(by_stock)
    for item in smashers:
        item["sectors"] = sorted(by_stock.get(item["code"], set()))[:2]
    timing_items = {
        item["code"]: item for item in leaders[:8] + broken[:8] + smashers[:8]
    }
    minute_times = _minute_times(engine, target, list(timing_items.values()))
    event_rows = _event_rows(engine, target)
    if not event_rows:
        event_rows = _minute_event_rows(
            leaders,
            broken,
            smashers,
            minute_times,
            by_stock,
        )
    leader_details = {}
    for item in leaders:
        sectors = sorted(by_stock.get(item["code"], set()))
        leader_details[item["code"]] = {
            "sectors": sectors[:2],
            **minute_times.get(item["code"], {}),
        }
    mainlines = _mainline_rows(
        sector_data["top"],
        bars=target_bars,
        by_sector=watch_by_sector,
        names=names,
        previous_top_names=sector_data["previous_top_names"],
    )
    watch = _watch_pool(
        target_bars,
        sectors=candidate_sectors,
        by_sector=watch_by_sector,
        names=names,
    )
    flows = _fund_section(engine, target, names, by_stock)
    tone, cycle = _tone_and_cycle(target_bars, previous_bars)
    quality_notes = []
    if sector_data["status"] != "available":
        quality_notes.append("板块热度未获取")
    if not watch_by_sector:
        quality_notes.append("板块成员映射未获取")
    elif membership_source:
        quality_notes.append(f"板块成员：{membership_source}")
    if not minute_times:
        quality_notes.append("目标日分钟数据未获取")
    if not any(item.get("status") == "已获取" for item in indices):
        quality_notes.append("五大指数目标日数据未获取")
    payload = {
        "date": date_str,
        "market": market,
        "breadth": {"status": tone},
        "board": _board_metrics(target_bars, names, leaders, broken),
        "cycle": {"cycle": cycle},
        "index_lines": index_lines,
        "indices": indices,
        "sectors": sector_data,
        "mainlines": mainlines,
        "leaders": leaders,
        "leader_details": leader_details,
        "smashers": smashers,
        "timeline": event_rows,
        "flows": flows,
        "watch_pool": watch,
        "notes": quality_notes,
    }
    return render_six_step(payload)


def _board_metrics(
    bars: pd.DataFrame,
    names: Mapping[str, str],
    leaders: list[dict[str, Any]],
    broken: list[dict[str, Any]],
) -> dict[str, Any]:
    if bars.empty:
        return {
            "limit_up_count": None,
            "limit_down_count": None,
            "broken_board_count": None,
            "broken_rate": None,
            "max_boards": None,
        }
    frame = bars.copy()
    frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
    frame["pct"] = (
        pd.to_numeric(frame["close"], errors="coerce")
        / pd.to_numeric(frame["pre_close"], errors="coerce")
        - 1.0
    ) * 100.0
    limit_up = 0
    limit_down = 0
    broken_count = 0
    for row in frame.to_dict(orient="records"):
        code = str(row["stock_code"])
        threshold = _limit_pct(code, names.get(code, ""))
        value = _number(row["pct"])
        if value >= threshold - 0.15:
            limit_up += 1
        elif value <= -threshold + 0.15:
            limit_down += 1
        elif (
            _number(row.get("high")) >= _limit_price(_number(row.get("pre_close")), threshold) - 0.005
            and value < threshold - 0.15
        ):
            broken_count += 1
    touched = limit_up + broken_count
    return {
        "limit_up_count": limit_up,
        "limit_down_count": limit_down,
        "broken_board_count": broken_count,
        "broken_rate": round(broken_count / touched * 100, 2) if touched else None,
        "max_boards": max((item.get("boards", 1) for item in leaders), default=0),
    }


def _sentiment_label(bars: pd.DataFrame) -> str:
    if bars.empty:
        return "未获取"
    frame = bars.copy()
    frame["pct"] = (
        pd.to_numeric(frame["close"], errors="coerce")
        / pd.to_numeric(frame["pre_close"], errors="coerce")
        - 1.0
    ) * 100.0
    up = float(frame["pct"].gt(0).mean() * 100)
    if up >= 60:
        return "普涨"
    if up >= 45:
        return "结构性/分化"
    if up >= 30:
        return "偏弱"
    return "退潮/冰点"


def _tone_and_cycle(
    bars: pd.DataFrame,
    previous_bars: pd.DataFrame,
) -> tuple[str, str]:
    if bars.empty:
        return "未获取", "未获取"
    current = bars.copy()
    current["pct"] = (
        pd.to_numeric(current["close"], errors="coerce")
        / pd.to_numeric(current["pre_close"], errors="coerce")
        - 1.0
    ) * 100.0
    current["pct"] = current["pct"].where(
        current["pct"].notna() & (current["pct"] > -90)
    )
    up_ratio = float(current["pct"].gt(0).mean() * 100)
    previous_up = None
    if isinstance(previous_bars, pd.DataFrame) and not previous_bars.empty:
        previous = previous_bars.copy()
        previous["pct"] = (
            pd.to_numeric(previous["close"], errors="coerce")
            / pd.to_numeric(previous["pre_close"], errors="coerce")
            - 1.0
        ) * 100.0
        previous["pct"] = previous["pct"].where(
            previous["pct"].notna() & (previous["pct"] > -90)
        )
        if previous["pct"].notna().any():
            previous_up = float(previous["pct"].gt(0).mean() * 100)

    if up_ratio >= 60:
        tone = "普涨"
    elif up_ratio >= 45:
        tone = "结构性"
    elif up_ratio >= 30:
        tone = "偏弱"
    else:
        tone = "退潮"

    if previous_up is not None and up_ratio <= previous_up - 8:
        cycle = "退潮"
    elif previous_up is not None and up_ratio >= previous_up + 8:
        cycle = "修复"
    elif up_ratio >= 65:
        cycle = "发酵"
    elif up_ratio >= 45:
        cycle = "分歧"
    elif up_ratio >= 30:
        cycle = "偏弱"
    else:
        cycle = "冰点"
    return tone, cycle


__all__ = ["build_six_step_review", "render_six_step"]

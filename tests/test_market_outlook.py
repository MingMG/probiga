from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine, text

from server.api.routers.market_outlook import build_market_outlook


def _database(tmp_path):
    engine = create_engine(
        f"sqlite:///{(tmp_path / 'market-outlook.db').as_posix()}",
        future=True,
    )
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE si_trade_calendar "
            "(trade_date DATE, trade_status INTEGER)"
        ))
        connection.execute(text(
            "CREATE TABLE sm_stock_kline ("
            "stock_code TEXT, trade_date DATE, k_type INTEGER, "
            "adjust_type INTEGER, change_pct REAL, amount REAL, "
            "close REAL, pre_close REAL)"
        ))
        connection.execute(text(
            "CREATE TABLE st_recommended_stocks ("
            "stock_code TEXT, short_name TEXT, pick_date DATE, "
            "publication_status TEXT, sources TEXT, "
            "short_term_score REAL, capital_score REAL, technical REAL, "
            "signal_status TEXT, signal_reason TEXT, "
            "entry_price_low REAL, entry_price_high REAL, "
            "stop_loss_price REAL, take_profit_1 REAL, "
            "risk_reward_ratio REAL, entry_conditions_json TEXT, "
            "invalidation_reason TEXT)"
        ))
        connection.execute(text(
            "CREATE TABLE st_recommended_run_history ("
            "run_uid TEXT, trade_date DATE, status TEXT, total INTEGER, "
            "passed INTEGER, executable_count INTEGER, "
            "canonical_pool_sha256 TEXT, build_sha TEXT, published_at TEXT)"
        ))
        connection.execute(text(
            "INSERT INTO si_trade_calendar VALUES "
            "('2026-10-22',1),('2026-10-23',1),('2026-10-26',1)"
        ))

        previous = []
        current = []
        for index in range(100):
            code = f"{index + 1:06d}"
            previous.append((
                code, "2026-10-22", 1, 0,
                -3.0 if index >= 5 else 0.5,
                1_000_000_000, 10.0, 10.3,
            ))
            current.append((
                code, "2026-10-23", 1, 0,
                9.9 if index < 80 else (1.0 if index < 85 else -1.0),
                1_500_000_000, 11.0, 10.0,
            ))
        previous.extend([
            ("603278", "2026-10-22", 1, 0, 10.0, 684_825_820, 14.51, 13.19),
            ("002724", "2026-10-22", 1, 0, 10.0, 1_271_013_146, 11.21, 10.19),
        ])
        current.extend([
            ("603278", "2026-10-23", 1, 0, 9.99, 1_663_228_698, 15.96, 14.51),
            ("002724", "2026-10-23", 1, 0, 9.99, 1_727_203_725, 12.33, 11.21),
        ])
        connection.execute(
            text("INSERT INTO sm_stock_kline VALUES "
                 "(:stock_code,:trade_date,:k_type,:adjust_type,"
                 ":change_pct,:amount,:close,:pre_close)"),
            [
                {
                    "stock_code": row[0],
                    "trade_date": row[1],
                    "k_type": row[2],
                    "adjust_type": row[3],
                    "change_pct": row[4],
                    "amount": row[5],
                    "close": row[6],
                    "pre_close": row[7],
                }
                for row in previous + current
            ],
        )

        def event(code, role, low_return, rebound, first_limit, vwap):
            payload = {
                "status": "AVAILABLE",
                "source": "sina_public_1m",
                "session_low_return_pct": low_return,
                "rebound_from_low_pct": rebound,
                "first_limit_time": first_limit,
                "last_limit_time": "2026-10-23T15:00:00",
                "preclose_reclaim_time": "2026-10-23T09:49:00",
                "open_board_count": 0,
                "vwap": vwap,
            }
            return json.dumps([
                f"昨日{first_limit[11:16]}首次封板，次日回踩不破再观察",
                "intraday_event=" + json.dumps(payload, ensure_ascii=False),
            ], ensure_ascii=False)

        connection.execute(text(
            "INSERT INTO st_recommended_run_history VALUES "
            "(:run_uid,'2026-10-23','done',102,2,0,:pool_sha,:build_sha,"
            "'2026-10-25 22:00:00')"
        ), {
            "run_uid": "a" * 32,
            "pool_sha": "b" * 64,
            "build_sha": "c" * 40,
        })
        connection.execute(text(
            "INSERT INTO st_recommended_stocks VALUES "
            "(:code,:name,'2026-10-23','ACTIVE',"
            "'fast_eod+mode:PANIC_REBOUND+tactical',80.9,90.1,100,"
            "'WATCH',:reason,14.37,14.80,14.07,15.75,2.26,"
            ":conditions,'跌破止损或龙头炸板')"
        ), [
            {
                "code": "603278",
                "name": "大业股份",
                "reason": "PANIC_REBOUND 龙头封板：13:10首次封板",
                "conditions": event(
                    "603278", "龙头封板", -8.82, 20.63,
                    "2026-10-23T13:10:00", 14.565,
                ),
            },
            {
                "code": "002724",
                "name": "海洋王",
                "reason": "PANIC_REBOUND 尾盘跟随封板：14:26首次封板",
                "conditions": event(
                    "002724", "尾盘跟随封板", -9.99, 22.20,
                    "2026-10-23T14:26:00", 10.850,
                ),
            },
        ])
    return engine


def test_market_outlook_builds_scenarios_and_core_stock_plan(tmp_path):
    engine = _database(tmp_path)

    payload = build_market_outlook(engine, trade_date="2026-10-23")

    assert payload["status"] == "READY"
    assert payload["next_trade_date"] == "2026-10-26"
    assert payload["market_mode"] == "PANIC_REBOUND"
    assert sum(item["probability_pct"] for item in payload["scenarios"]) == 100
    assert [item["stock_code"] for item in payload["stocks"]] == [
        "603278",
        "002724",
    ]
    assert payload["stocks"][0]["next_limit_up_price"] == 17.56
    assert payload["stocks"][1]["next_limit_up_price"] == 13.56
    assert payload["stocks"][0]["entry_price_low"] == 14.37
    assert payload["session_timeline"][0]["time"] == "09:25"


def test_market_outlook_blocks_without_active_pool(tmp_path):
    engine = create_engine(
        f"sqlite:///{(tmp_path / 'empty-outlook.db').as_posix()}",
        future=True,
    )
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE st_recommended_stocks "
            "(stock_code TEXT, pick_date DATE, publication_status TEXT)"
        ))

    payload = build_market_outlook(engine)

    assert payload["status"] == "DATA_BLOCKED"
    assert payload["scenarios"] == []
    assert payload["stocks"] == []

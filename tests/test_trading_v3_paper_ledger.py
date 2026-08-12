from __future__ import annotations

from server.api.routers import trading_v3
from server.trading_v2 import repository as v2_repository


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _Connection:
    def execute(self, statement, params):
        sql = str(statement)
        if "FROM st_sim_position" in sql:
            return _Rows(
                [
                    {
                        "id": 1,
                        "stock_code": "688059",
                        "short_name": "华锐精密",
                        "strategy_type": "live_event",
                        "buy_price": 95.53,
                        "buy_shares": 1000,
                        "buy_date": "2026-08-12",
                        "buy_time": "09:31:00",
                        "buy_reason": "test fill",
                        "status": "holding",
                    }
                ]
            )
        return _Rows([])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class _Engine:
    def connect(self):
        return _Connection()


def test_paper_ledger_uses_real_v2_read_repository(monkeypatch):
    monkeypatch.setattr(trading_v3, "get_engine", lambda: _Engine())
    monkeypatch.setattr(
        v2_repository.TradingV2ReadRepository,
        "account",
        lambda self, account_id: {
            "account_id": account_id,
            "initial_cash": 1_000_000,
            "cash_balance": 900_000,
        },
    )
    monkeypatch.setattr(
        v2_repository.TradingV2ReadRepository,
        "positions",
        lambda self, account_id: [],
    )
    monkeypatch.setattr(
        v2_repository.TradingV2ReadRepository,
        "orders",
        lambda self, account_id, limit: [],
    )

    result = trading_v3.paper_ledger(account_id="paper-main-v2", limit=20)

    assert result["status"] == "ok"
    assert result["data"]["summary"]["legacy_position_count"] == 1
    assert result["data"]["positions"][0]["stock_code"] == "688059"
    assert result["data"]["positions"][0]["ledger_source"] == "LEGACY_EVENT_SIM"
    assert result["data"]["real_trading_enabled"] is False

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
        if "FROM sm_stock_current c" in sql:
            assert params == {"quote_code_0": "688059"}
            return _Rows(
                [
                    {
                        "stock_code": "688059",
                        "short_name": "华锐精密",
                        "price": 99.80,
                        "snapshot_at": "2026-08-12 15:00:00",
                        "data_source": "GJ_BIG_QMT_INNER",
                    }
                ]
            )
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
    assert result["data"]["positions"][0]["current_price"] == 99.8
    assert result["data"]["positions"][0]["short_name"] == "华锐精密"
    assert result["data"]["positions"][0]["market_value"] == 99_800.0
    assert result["data"]["positions"][0]["unrealized_pnl"] == 4_270.0
    assert result["data"]["positions"][0]["unrealized_pnl_pct"] == 4.47
    assert result["data"]["summary"]["total_unrealized_pnl"] == 4_270.0
    assert result["data"]["summary"]["position_lot_count"] == 1
    assert result["data"]["positions"][0]["position_lot_count"] == 1
    assert result["data"]["real_trading_enabled"] is False


def test_paper_ledger_merges_same_stock_with_weighted_cost(monkeypatch):
    monkeypatch.setattr(trading_v3, "get_engine", lambda: _Engine())
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "account", lambda self, account_id: {})
    monkeypatch.setattr(
        v2_repository.TradingV2ReadRepository,
        "positions",
        lambda self, account_id: [
            {
                "stock_code": "688059",
                "short_name": "华锐精密",
                "position_state": "HOLDING",
                "quantity": 400,
                "remaining_quantity": 400,
                "sellable_quantity": 400,
                "cost_price": 100.0,
            }
        ],
    )
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "orders", lambda self, account_id, limit: [])

    result = trading_v3.paper_ledger(account_id="paper-main-v2", limit=20)
    data = result["data"]
    assert data["summary"]["position_count"] == 1
    assert data["summary"]["position_lot_count"] == 2
    assert len(data["positions"]) == 1
    position = data["positions"][0]
    assert position["stock_code"] == "688059"
    assert position["quantity"] == 1400
    assert position["sellable_quantity"] == 400
    assert position["cost_price"] == 96.8071
    assert position["current_price"] == 99.8
    assert position["market_value"] == 139_720.0
    assert position["unrealized_pnl"] == 4_190.0
    assert position["unrealized_pnl_pct"] == 3.09
    assert position["position_lot_count"] == 2
    assert position["ledger_source"] == "MERGED_LEDGER"
    assert position["ledger_sources"] == ["V2_CANONICAL", "LEGACY_EVENT_SIM"]
    assert data["merge_policy"] == "READ_ONLY_GROUP_BY_STOCK_CODE_WEIGHTED_COST"

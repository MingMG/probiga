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
        if "SUM(profit) AS realized_profit" in sql:
            return _Rows([{"realized_profit": 0}])
        if "FROM st_sim_risk_budget" in sql:
            return _Rows([{"initial_capital": 1_000_000}])
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
            "account_name": "20万元V2主模拟账户",
            "initial_cash": 1_000_000,
            "cash_balance": 900_000,
            "latest_equity": {
                "trade_date": "2026-08-12",
                "cash_balance": 900_000,
                "market_value": 2_500,
                "total_equity": 902_500,
            },
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
    assert result["data"]["summary"]["canonical_initial_cash"] == 1_000_000.0
    assert result["data"]["summary"]["canonical_cash_balance"] == 900_000.0
    assert result["data"]["summary"]["canonical_market_value"] == 2_500.0
    assert result["data"]["summary"]["canonical_total_equity"] == 902_500.0
    assert result["data"]["summary"]["canonical_equity_trade_date"] == "2026-08-12"
    assert result["data"]["summary"]["canonical_account_scope"] == "V2_CANONICAL_ONLY"
    assert result["data"]["summary"]["display_account_scope"] == "LEGACY_EVENT_SIM_ACTIVE"
    assert result["data"]["summary"]["legacy_initial_cash"] == 1_000_000.0
    assert result["data"]["summary"]["legacy_cash_balance"] == 904_470.0
    assert result["data"]["summary"]["legacy_market_value"] == 99_800.0
    assert result["data"]["summary"]["legacy_total_equity"] == 1_004_270.0
    assert result["data"]["summary"]["display_cash_balance"] == 904_470.0
    assert result["data"]["summary"]["display_total_equity"] == 1_004_270.0
    assert result["data"]["positions"][0]["position_lot_count"] == 1
    assert result["data"]["positions"][0]["buy_at"] == "2026-08-12 09:31:00"
    assert result["data"]["positions"][0]["lot_details"][0]["quantity"] == 1000
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


def test_paper_ledger_keeps_today_closed_position_for_same_day_review(monkeypatch):
    today = trading_v3.date.today().isoformat()

    class _SoldTodayConnection(_Connection):
        def execute(self, statement, params):
            sql = str(statement)
            if "SUM(profit) AS realized_profit" in sql:
                return _Rows([{"realized_profit": 125.5}])
            if "FROM st_sim_position" in sql and "status = 'holding'" in sql:
                return _Rows([])
            if "FROM st_sim_position" in sql and "status = 'sold'" in sql:
                return _Rows(
                    [
                        {
                            "id": 8,
                            "stock_code": "600030",
                            "short_name": "中信证券",
                            "strategy_type": "live_event",
                            "buy_price": 28.71,
                            "buy_shares": 500,
                            "buy_date": today,
                            "buy_time": "09:36:10",
                            "buy_reason": "盘中确认后成交",
                            "status": "sold",
                            "sell_price": 29.18,
                            "sell_date": today,
                            "sell_time": "14:42:05",
                            "sell_reason": "达到动态退出条件",
                            "profit": 125.5,
                            "profit_rate": 1.64,
                        }
                    ]
                )
            if "FROM st_sim_risk_budget" in sql:
                return _Rows([{"initial_capital": 1_000_000}])
            return _Rows([])

    class _SoldTodayEngine:
        def connect(self):
            return _SoldTodayConnection()

    monkeypatch.setattr(trading_v3, "get_engine", lambda: _SoldTodayEngine())
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "account", lambda self, account_id: {})
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "positions", lambda self, account_id: [])
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "orders", lambda self, account_id, limit: [])
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "fills", lambda self, account_id, limit: [])

    data = trading_v3.paper_ledger(account_id="paper-main-v2", limit=20)["data"]

    assert data["positions"] == []
    assert data["summary"]["position_count"] == 0
    assert data["summary"]["today_sold_count"] == 1
    assert data["summary"]["today_closed_position_count"] == 1
    closed = data["today_closed_positions"][0]
    assert closed["stock_code"] == "600030"
    assert closed["position_state"] == "SOLD_TODAY"
    assert closed["buy_at"] == f"{today} 09:36:10"
    assert closed["sell_at"] == f"{today} 14:42:05"
    assert closed["sell_price"] == 29.18
    assert closed["sold_quantity_today"] == 500
    assert closed["realized_pnl"] == 125.5


def test_paper_ledger_excludes_closed_position_after_sale_day(monkeypatch):
    class _OldSoldConnection(_Connection):
        def execute(self, statement, params):
            sql = str(statement)
            if "FROM st_sim_position" in sql and "status = 'holding'" in sql:
                return _Rows([])
            if "FROM st_sim_position" in sql and "status = 'sold'" in sql:
                return _Rows(
                    [
                        {
                            "stock_code": "600030",
                            "status": "sold",
                            "sell_date": "2026-08-01",
                            "sell_time": "14:42:05",
                        }
                    ]
                )
            return super().execute(statement, params)

    class _OldSoldEngine:
        def connect(self):
            return _OldSoldConnection()

    monkeypatch.setattr(trading_v3, "get_engine", lambda: _OldSoldEngine())
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "account", lambda self, account_id: {})
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "positions", lambda self, account_id: [])
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "orders", lambda self, account_id, limit: [])
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "fills", lambda self, account_id, limit: [])

    data = trading_v3.paper_ledger(account_id="paper-main-v2", limit=20)["data"]
    assert data["today_closed_positions"] == []
    assert data["summary"]["today_sold_count"] == 0


def test_paper_ledger_counts_pending_buy_when_remaining_quantity_is_zero(monkeypatch):
    class _PendingConnection(_Connection):
        def execute(self, statement, params):
            if "FROM st_sim_position" in str(statement) and "status = 'holding'" in str(statement):
                return _Rows([])
            if "FROM st_sim_order" in str(statement):
                return _Rows(
                    [
                        {
                            "id": 9,
                            "side": "BUY",
                            "status": "PENDING",
                            "requested_shares": 100,
                            "filled_shares": 0,
                            "remaining_shares": 0,
                            "limit_price": 10,
                        }
                    ]
                )
            return super().execute(statement, params)

    class _PendingEngine:
        def connect(self):
            return _PendingConnection()

    monkeypatch.setattr(trading_v3, "get_engine", lambda: _PendingEngine())
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "account", lambda self, account_id: {})
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "positions", lambda self, account_id: [])
    monkeypatch.setattr(v2_repository.TradingV2ReadRepository, "orders", lambda self, account_id, limit: [])

    summary = trading_v3.paper_ledger(account_id="paper-main-v2", limit=20)["data"]["summary"]
    assert summary["display_account_scope"] == "LEGACY_EVENT_SIM_ACTIVE"
    assert summary["legacy_pending_buy_amount"] == 1_000.0
    assert summary["display_cash_balance"] == 999_000.0
    assert summary["display_total_equity"] == 1_000_000.0

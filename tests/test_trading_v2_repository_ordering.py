from server.trading_v2.repository import TradingV2ReadRepository


class RecordingRepository(TradingV2ReadRepository):
    def __init__(self):
        self.sql_calls = []

    def _one(self, sql, params=None):
        self.sql_calls.append(sql)
        return None


def test_latest_snapshot_uses_creation_time_for_same_decision_time():
    repository = RecordingRepository()

    repository.latest_snapshot()

    assert "decision_at DESC, created_at DESC, snapshot_id DESC" in repository.sql_calls[-1]


def test_latest_regime_uses_worker_start_time_for_recomputed_decision():
    repository = RecordingRepository()

    repository.latest_regime()

    assert "decision_at DESC, started_at DESC, run_uid DESC" in repository.sql_calls[-1]


class DecisionHistoryRepository(TradingV2ReadRepository):
    def __init__(self):
        self.sql = ""
        self.params = {}

    def _all(self, sql, params=None):
        self.sql = sql
        self.params = params or {}
        return []


def test_decision_history_is_persisted_only_and_ordered_by_batch_time():
    repository = DecisionHistoryRepository()

    result = repository.decision_runs(trade_date="2026-07-24", limit=50)

    assert result == []
    assert "r.trade_date = :trade_date" in repository.sql
    assert "r.decision_at DESC" in repository.sql
    assert "st_strategy_signal_v2" in repository.sql
    assert repository.params == {
        "trade_date": "2026-07-24",
        "limit": 50,
    }


class SecurityNameRepository(TradingV2ReadRepository):
    def __init__(self):
        pass

    def _security_name_map(self, codes):
        assert sorted(codes) == ["000001", "511880"]
        return {"000001": "平安银行", "511880": "银华日利ETF"}


def test_security_rows_are_enriched_with_chinese_names():
    repository = SecurityNameRepository()

    rows = repository._enrich_security_rows(
        [{"stock_code": "000001"}, {"etf_code": "511880"}]
    )

    assert rows[0]["short_name"] == "平安银行"
    assert rows[1]["short_name"] == "银华日利ETF"


class TomorrowPendingRepository(TradingV2ReadRepository):
    def __init__(self):
        self.pending_query = ""

    def latest_regime(self):
        return {
            "run_uid": "run-new",
            "trade_date": "2026-07-27",
            "market_regime": "PANIC_RECOVERY",
            "status": "COMPLETED",
        }

    def current_plan(self, _account_id):
        return {
            "run_uid": "run-new",
            "positions": [],
            "target_cash": "151107.80",
            "target_risk_asset_weight": "0.244461",
            "worst_case_loss": "2877.10",
            "rejected_candidates": [],
        }

    def candidates(self, **_kwargs):
        return []

    def _one(self, sql, params=None):
        if "MIN(trade_date)" in sql:
            return {"trade_date": "2026-07-28"}
        return None

    def _all(self, sql, params=None):
        if "FROM st_order_v2 o" not in sql:
            return []
        self.pending_query = sql
        assert params == {
            "account_id": "paper-main-v2",
            "execution_date": "2026-07-28",
        }
        return [
            {
                "order_id": "order-1",
                "stock_code": "002326",
                "short_name": "永太科技",
                "target_quantity": 800,
                "target_weight": "0.079316",
                "strategy_version": "sector_preheat_v1.4.0",
                "theme_code": "CONCEPT:新材料概念",
                "order_status": "RISK_APPROVED",
                "limit_price": "19.829",
                "initial_stop": "18.662",
                "earliest_at": "2026-07-28 09:31:00",
                "expires_at": "2026-07-28 15:00:00",
            }
        ]


def test_tomorrow_action_keeps_approved_pending_orders_visible():
    repository = TomorrowPendingRepository()

    result = repository.tomorrow_action("paper-main-v2")

    assert result["action"] == "BUY"
    assert result["pending_order_count"] == 1
    assert result["positions"][0]["stock_code"] == "002326"
    assert result["positions"][0]["short_name"] == "永太科技"
    assert result["positions"][0]["order_status"] == "RISK_APPROVED"
    assert result["positions"][0]["target_weight"].startswith("0.079316")
    assert "RISK_APPROVED" in repository.pending_query

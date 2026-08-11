from pathlib import Path
import re

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.routers import trading_v2
from server.trading_v2.repository import V2_TABLES


ROOT = Path(__file__).resolve().parents[1]


class FakeRepository:
    snapshot = {
        "snapshot_id": "snapshot-1",
        "data_snapshot_hash": "a" * 64,
        "code_commit_sha": "b" * 64,
        "quality_status": "BLOCK",
        "blocked_capabilities": ["QMT_DAILY_KLINE_NOT_ATTESTED"],
    }

    def table_readiness(self):
        return {name: True for name in V2_TABLES}

    def latest_snapshot(self):
        return self.snapshot

    def execution_capability(self, _capability_code):
        return {
            "capability_code": "B-003_RELIABLE_LEVEL1_BID_ASK",
            "status": "BLOCK",
            "protocol_version": "level1_continuity_v2.0.0",
        }

    def fee_profile_confirmation(self, _fee_profile_version):
        return {
            "fee_profile_version": "guojin_fee_v2.0.0",
            "profile_count": 2,
            "confirmed_profile_count": 2,
            "confirmed_required_type_count": 2,
        }

    def latest_regime(self):
        return {
            "run_uid": "run-1",
            "market_regime": "DATA_BLOCKED",
            "market_regime_version": "market_regime_v2.0.0",
            "status": "BLOCKED",
            "result_hash": "c" * 64,
            "code_commit_sha": "b" * 64,
        }

    def strategies(self):
        return [
            {
                "strategy_id": "short_term",
                "lifecycle_status": "PAPER_TRIAL",
            }
        ]

    def candidates(self, **_kwargs):
        return [
            {
                "stock_code": "000001",
                "short_name": "平安银行",
                "competition_status": "BLOCKED",
            }
        ]

    def decision_runs(self, **_kwargs):
        return [
            {
                "run_uid": "run-12345",
                "trade_date": "2026-07-24",
                "decision_at": "2026-07-24 15:20:00",
                "status": "COMPLETED",
                "signal_count": 1,
            }
        ]

    def account(self, _account_id):
        return {
            "account_id": "paper-main-v2",
            "status": "ACTIVE",
            "fee_profile_version": "guojin_fee_v2.0.0",
            "instrument_rule_version": "paper_instrument_qmt_v2.1.0",
            "real_trading_enabled": 0,
            "latest_reconciliation": {"status": "PASS"},
        }

    def decision_run(self, run_uid):
        return {"run_uid": run_uid, "status": "BLOCKED"}

    def current_plan(self, _account_id):
        return {"positions": [], "target_cash": "200000.00"}

    def positions(self, _account_id):
        return []

    def orders(self, _account_id, _limit=200):
        return []

    def fills(self, _account_id, _limit=200):
        return []

    def cash_ledger(self, _account_id, _limit=500):
        return []

    def reconciliations(self, _account_id, _limit=100):
        return [{"status": "PASS"}]

    def daily_reports(self, _limit=100):
        return []

    def job(self, job_id):
        return {"job_id": job_id, "status": "COMPLETED"}

    def backtest(self, backtest_uid):
        return {
            "backtest_uid": backtest_uid,
            "status": "COMPLETED",
            "gate_status": "BLOCK",
        }

    def worker_heartbeats(self):
        return [{"worker_name": "trading-v2-job-worker", "status": "IDLE"}]

    def tomorrow_action(self, _account_id):
        return {
            "source_trade_date": "2026-07-24",
            "execution_trade_date": "2026-07-27",
            "market_regime": "EXTREME",
            "run_uid": "run-1",
            "run_status": "COMPLETED",
            "action": "NO_BUY",
            "positions": [],
            "target_cash": "200000.00",
            "target_risk_asset_weight": "0",
            "worst_case_loss": "0",
            "rejected_candidate_count": 33,
            "watch_candidates": [],
        }

    def etf_forward_summary(self, _limit):
        return {
            "status": "waiting_first_forward_close",
            "strategies": [
                {
                    "strategy_version": "etf_trend_risk_v2.0.0",
                    "forward_start_date": "2026-07-27",
                    "status": "registered",
                    "config": {"cold_start": {"511880": 1.0}},
                }
            ],
            "observations": [],
            "observation_count": 0,
            "data": {
                "row_count": 100,
                "symbol_count": 6,
                "latest_trade_date": "2026-07-24",
                "validated_rows": 100,
            },
            "task": {
                "task_type": "etf_forward_daily",
                "cron_time": "15:20",
                "enabled": 1,
            },
            "backfill": "prohibited",
            "automatic_order_submission": False,
            "security_names": {"511880": "银华日利ETF"},
        }

    def operations_summary(self):
        return {
            "tasks": [{"task_type": "etf_forward_daily", "enabled": 1}],
            "backtests": [],
            "jobs": [],
            "workers": self.worker_heartbeats(),
            "real_trading_guards": [
                {
                    "trigger_name": "trg_trade_account_v2_real_disabled_bi",
                },
                {
                    "trigger_name": "trg_trade_account_v2_real_disabled_bu",
                },
            ],
            "running_backtest_count": 0,
        }


def _client(monkeypatch):
    monkeypatch.setattr(trading_v2, "_repo", lambda: FakeRepository())
    monkeypatch.setattr(
        trading_v2,
        "load_qmt_kline_attestation_status",
        lambda limit=3: {
            "status": "complete",
            "runs": [
                {
                    "status": "COMPLETED",
                    "target_rows": 100,
                    "matched_rows": 100,
                    "missing_qmt_rows": 0,
                    "mismatched_rows": 0,
                    "coverage_pct": 100.0,
                }
            ],
        },
    )
    monkeypatch.setattr(
        trading_v2,
        "load_membership_snapshot_history",
        lambda **kwargs: {
            "status": "ok",
            "snapshot_date": "2026-07-24",
            "member_type": kwargs["member_type"],
            "runs": [{"snapshot_date": "2026-07-24"}],
            "data": [],
        },
    )
    app = FastAPI()
    app.include_router(trading_v2.router, prefix="/api")
    return TestClient(app)


def test_readiness_is_blocked_and_has_required_envelope(monkeypatch):
    response = _client(monkeypatch).get("/api/v2/system/readiness")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "blocked"
    assert payload["trace_id"]
    assert payload["generated_at"]
    assert payload["data_snapshot_id"] == "snapshot-1"
    assert payload["data_snapshot_hash"] == "a" * 64
    assert payload["code_commit_sha"] == "b" * 64
    assert payload["config_version"] == "portfolio_policy_v2.1.0-paper"
    assert payload["data"]["real_trading_enabled"] is False
    assert "QMT_DAILY_KLINE_NOT_ATTESTED" in payload["data"]["blocks"]
    assert "B-001_ACTUAL_BROKER_FEES" not in payload["data"]["blocks"]
    assert "B-002_ACCOUNT_INSTRUMENT_PERMISSIONS" not in payload["data"]["blocks"]
    assert "B-003_RELIABLE_LEVEL1_BID_ASK" not in payload["data"]["blocks"]
    assert "B-001_ACTUAL_BROKER_FEES" not in payload["data"]["real_trading_blocks"]
    assert "B-002_ACCOUNT_INSTRUMENT_PERMISSIONS" in payload["data"]["real_trading_blocks"]
    assert "B-003_RELIABLE_LEVEL1_BID_ASK" in payload["data"]["real_trading_blocks"]
    assert (
        payload["data"]["fee_confirmation"][
            "confirmed_required_type_count"
        ]
        == 2
    )


def test_v2_read_endpoints_return_only_snapshots(monkeypatch):
    client = _client(monkeypatch)
    paths = [
        "/api/v2/market-regime/latest",
        "/api/v2/strategies",
        "/api/v2/decision-runs",
        "/api/v2/candidates",
        "/api/v2/decision-runs/run-12345",
        "/api/v2/accounts/paper-main-v2",
        "/api/v2/accounts/paper-main-v2/plan",
        "/api/v2/accounts/paper-main-v2/positions",
        "/api/v2/accounts/paper-main-v2/orders",
        "/api/v2/accounts/paper-main-v2/fills",
        "/api/v2/accounts/paper-main-v2/cash-ledger",
        "/api/v2/accounts/paper-main-v2/reconciliation",
        "/api/v2/reports/daily",
        "/api/v2/jobs/job-12345",
        "/api/v2/research/backtests/backtest-12345",
        "/api/v2/system/workers",
        "/api/v2/operations/tomorrow",
        "/api/v2/research/etf-forward",
        "/api/v2/system/data-evidence",
        "/api/v2/system/operations",
    ]
    for path in paths:
        response = client.get(path)
        assert response.status_code == 200, path
        assert "data_snapshot_hash" in response.json()


def test_v2_candidates_include_chinese_security_name(monkeypatch):
    response = _client(monkeypatch).get("/api/v2/candidates")
    assert response.status_code == 200
    assert response.json()["data"][0]["short_name"] == "平安银行"


def test_v2_candidate_buy_label_requires_all_canonical_gates():
    row = {
        "stock_code": "000001",
        "action": "BUY",
        "competition_status": "ELIGIBLE",
        "rejection_code": None,
        "raw_features": {
            "source_recommend_status": "ALLOW",
            "source_signal_status": "BUY_READY",
            "source_chase_risk_status": "ALLOW",
            "source_ordinary_buy_eligible": True,
        },
    }

    allowed = trading_v2._candidate_display_projection(row)
    assert allowed["action"] == "BUY_READY"
    assert allowed["new_buy_eligible"] is True

    research = trading_v2._candidate_display_projection({
        **row,
        "competition_status": "RESEARCH_ONLY",
        "raw_features": {
            **row["raw_features"],
            "source_signal_status": "WATCH",
        },
    })
    assert research["source_action"] == "BUY"
    assert research["action"] == "RESEARCH_ONLY"
    assert research["new_buy_eligible"] is False


def test_v2_candidate_sell_is_not_blocked_by_new_buy_projection():
    projected = trading_v2._candidate_display_projection({
        "stock_code": "000001",
        "action": "SELL",
        "competition_status": "REJECTED",
        "raw_features": {},
    })

    assert projected["action"] == "SELL"
    assert projected["display_action"] == "SELL"
    assert projected["new_buy_eligible"] is False


def test_v2_decision_history_can_filter_persisted_batches_by_trade_date(monkeypatch):
    response = _client(monkeypatch).get(
        "/api/v2/decision-runs?trade_date=2026-07-24&limit=50"
    )
    assert response.status_code == 200
    assert response.json()["data"][0]["run_uid"] == "run-12345"


def test_v2_decision_history_rejects_invalid_trade_date(monkeypatch):
    response = _client(monkeypatch).get(
        "/api/v2/decision-runs?trade_date=2026-7-24"
    )
    assert response.status_code == 422


def test_get_router_has_no_strategy_recalculation_or_ddl():
    source = (ROOT / "server" / "api" / "routers" / "trading_v2.py").read_text(
        encoding="utf-8"
    )
    assert "build_strategy_center_snapshot" not in source
    assert "CREATE TABLE" not in source
    assert "run_v2_migrations" not in source


def test_v2_page_exposes_all_required_information_areas():
    html = (ROOT / "server" / "static" / "trading-v2.html").read_text(
        encoding="utf-8"
    )
    for label in (
        "系统可信度",
        "明日动作",
        "今日机会",
        "组合计划",
        "当前持仓",
        "订单与成交",
        "ETF 前向",
        "数据证据",
        "运行状态",
        "复盘与验收",
        "策略实验室",
    ):
        assert label in html
    assert "真实交易关闭" in html


def test_v2_page_script_references_existing_elements_and_views():
    html = (ROOT / "server" / "static" / "trading-v2.html").read_text(
        encoding="utf-8"
    )
    script = (
        ROOT / "server" / "static" / "js" / "trading-v2.js"
    ).read_text(encoding="utf-8")
    html_ids = set(re.findall(r'\bid="([^"]+)"', html))
    script_ids = set(re.findall(r"\bel\('([^']+)'\)", script))
    assert script_ids <= html_ids
    view_names = set(re.findall(r'data-view="([^"]+)"', html))
    for view_name in view_names:
        assert f"view-{view_name}" in html_ids
    assert "securityLink" in script
    assert "买入信号尚未确认" in script
    assert "/?tab=stock-list&stock_code=" in script
    assert "candidateDate" in html_ids
    assert "candidateRun" in html_ids
    assert "/decision-runs?limit=500" in script


def test_main_page_supports_stock_detail_deep_link():
    script = (ROOT / "server" / "static" / "js" / "app.js").read_text(
        encoding="utf-8"
    )
    assert "linkedStockCodeFromLocation" in script
    assert "window.openStockDetail(linkedStockCode)" in script


def test_core_migration_contains_all_required_v2_tables():
    source = (ROOT / "server" / "db" / "migrations_v2.py").read_text(
        encoding="utf-8"
    )
    for table in V2_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in source

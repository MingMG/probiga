from unittest.mock import patch

from server.api.routers import health


def test_combine_qmt_table_status_uses_error_warn_empty_ok_order():
    assert health._combine_qmt_table_status({"status": "ok", "total_rows": 1}) == "ok"
    assert health._combine_qmt_table_status({"status": "ok", "total_rows": 0}) == "warn"
    assert health._combine_qmt_table_status({"status": "warn", "total_rows": 1}) == "warn"
    assert health._combine_qmt_table_status({"status": "error", "total_rows": 1}) == "error"


def test_table_freshness_uses_bounded_snapshot_range():
    executed: list[str] = []

    class Result:
        def __init__(self, row=None, scalar_value=None):
            self.row = row
            self.scalar_value = scalar_value

        def mappings(self):
            return self

        def first(self):
            return self.row

        def scalar(self):
            return self.scalar_value

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, statement, _params):
            sql = str(statement)
            executed.append(sql)
            if "information_schema.TABLES" in sql:
                return Result(scalar_value=10000)
            return Result(
                row={
                    "latest_snapshot_at": None,
                    "today_rows": 5526,
                    "today_symbols": 5526,
                }
            )

    class Engine:
        @staticmethod
        def connect():
            return Connection()

    with patch("server.api.routers.health.get_current_engine", return_value=Engine()):
        result = health._table_freshness(
            "sm_rt_quote_snapshot",
            "stock_code",
            fresh_window_seconds=300,
        )

    assert result["today_rows"] == 5526
    assert "DATE(snapshot_at)" not in executed[0]
    assert "snapshot_at >= :today_start" in executed[0]


def test_qmt_bridge_reports_external_collector_ok_when_tables_are_fresh():
    runtime = {
        "enabled": False,
        "poll_seconds": 5,
        "idle_sleep_seconds": 30,
        "trading_hours_only": True,
        "candidate_limit": 60,
    }

    with patch("server.api.routers.health._get_qmt_live_runtime_config", return_value=runtime), \
         patch("server.api.routers.health.get_gj_qmt_config", return_value={"ping_timeout": 1}), \
         patch("server.api.routers.health._format_mysql_target", return_value={}), \
         patch("server.api.routers.health._is_trading_time", return_value=False), \
         patch("server.api.routers.health._table_freshness", side_effect=[
             {"status": "ok", "table": "sm_stock_current", "total_rows": 5000},
             {"status": "ok", "table": "sm_index_current", "total_rows": 50},
         ]), \
         patch("integrations.qmt.bridge.is_configured", return_value=False), \
         patch("integrations.qmt.diagnostics.diagnostics") as diagnostics:
        result = health.health_qmt_bridge()

    assert result["status"] == "ok"
    assert result["collector_mode"] == "external_windows_collector"
    assert "local Windows QMT collector" in result["status_reason"]
    diagnostics.assert_not_called()


def test_qmt_bridge_warns_when_external_collector_tables_are_stale():
    runtime = {
        "enabled": False,
        "poll_seconds": 5,
        "idle_sleep_seconds": 30,
        "trading_hours_only": True,
        "candidate_limit": 60,
    }

    with patch("server.api.routers.health._get_qmt_live_runtime_config", return_value=runtime), \
         patch("server.api.routers.health._format_mysql_target", return_value={}), \
         patch("server.api.routers.health._is_trading_time", return_value=True), \
         patch("server.api.routers.health._table_freshness", side_effect=[
             {"status": "warn", "table": "sm_stock_current", "total_rows": 5000},
             {"status": "ok", "table": "sm_index_current", "total_rows": 50},
         ]), \
         patch("integrations.qmt.bridge.is_configured", return_value=False):
        result = health.health_qmt_bridge()

    assert result["status"] == "warn"
    assert result["collector_mode"] == "external_windows_collector"
    assert "stale snapshots" in result["status_reason"]


def test_qmt_capabilities_skips_worker_when_runtime_disabled_and_sdk_missing():
    runtime = {
        "enabled": False,
        "poll_seconds": 5,
        "idle_sleep_seconds": 30,
        "trading_hours_only": True,
        "candidate_limit": 60,
    }

    with patch("server.api.routers.health._get_qmt_live_runtime_config", return_value=runtime), \
         patch("integrations.qmt.bridge.is_configured", return_value=False), \
         patch("integrations.qmt.diagnostics.capabilities") as capabilities:
        result = health.health_qmt_capabilities()

    assert result["status"] == "disabled"
    assert "国金QMT" in result["reason"]
    assert result["rows"] == []
    capabilities.assert_not_called()

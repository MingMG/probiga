import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

from server.api.routers import trading_v3


class _ContextRepository:
    def __init__(self, run):
        self.run = run
        self.requested = None

    def latest_run_metadata(self, trade_date=None):
        self.requested = trade_date
        return self.run


def _analysis_history_engine(*, status, error="", message=""):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE st_recommended_run_history ("
            "id INTEGER PRIMARY KEY, run_uid TEXT, trade_date DATE, "
            "status TEXT, progress_percent INTEGER, message TEXT, error TEXT, "
            "started_at DATETIME, finished_at DATETIME, "
            "duration_seconds INTEGER, updated_at DATETIME)"
        ))
        connection.execute(
            text(
                "INSERT INTO st_recommended_run_history VALUES ("
                "1, 'be2570dff8a742e8b49542bbfcd4de34', '2026-08-31', "
                ":status, 5, :message, :error, '2026-08-31 23:31:24', "
                "'2026-09-01 00:10:57', 2373, '2026-09-01 00:10:57')"
            ),
            {"status": status, "message": message, "error": error},
        )
    return engine


def test_decision_context_does_not_render_blocked_run_as_empty(monkeypatch):
    repository = _ContextRepository(
        {
            "run_uid": "run-blocked-20260814",
            "trade_date": date(2026, 8, 14),
            "decision_at": datetime(2026, 8, 14, 15, 20),
            "status": "BLOCKED",
            "dominant_regime": "DATA_BLOCKED",
            "lifecycle_status": "PAPER_TRIAL",
            "target_count": 0,
            "portfolio": {"targets": []},
        }
    )
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    payload = trading_v3.decision_context(date(2026, 8, 14))

    assert payload["status"] == "blocked"
    assert payload["data"]["data_status"] == "BLOCKED"
    assert payload["data"]["decision_status"] == "BLOCKED"
    assert payload["data"]["decision_status"] != "EMPTY"
    assert payload["data"]["paper_order_authority"] == "NONE"
    assert payload["data"]["real_order_authority"] == "DISABLED"
    assert repository.requested == date(2026, 8, 14)


def test_decision_context_exposes_upstream_kline_data_block(monkeypatch):
    engine = _analysis_history_engine(
        status="error",
        error=(
            "DATA_BLOCKED: KLINE_FEATURE_QUERY_CONNECTION_LOST; "
            "trade_date=2026-08-31; stage=date_chunk_2_of_18"
        ),
        message="盘前预演失败",
    )
    repository = _ContextRepository(None)
    repository.engine = engine
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    payload = trading_v3.decision_context(date(2026, 8, 31))

    assert payload["status"] == "blocked"
    assert payload["data"]["data_status"] == "DATA_BLOCKED"
    assert payload["data"]["decision_status"] == "BLOCKED"
    assert payload["data"]["run_uid"] is None
    assert payload["data"]["upstream_run_uid"] == (
        "be2570dff8a742e8b49542bbfcd4de34"
    )
    assert "数据库连接中断" in payload["data"]["data_blocked_reason"]
    assert "KLINE_FEATURE_QUERY_CONNECTION_LOST" in (
        payload["data"]["reason_codes"]
    )
    assert payload["data"]["paper_order_authority"] == "NONE"


def test_legacy_lost_connection_is_classified_as_kline_data_block():
    reason_code, reason = trading_v3._analysis_data_block_reason(
        "(pymysql.err.OperationalError) "
        "(2013, 'Lost connection to MySQL server during query')"
    )

    assert reason_code == "KLINE_FEATURE_QUERY_CONNECTION_LOST"
    assert "数据库连接中断" in reason


def test_decision_context_exposes_running_analysis_progress(monkeypatch):
    engine = _analysis_history_engine(
        status="running",
        message="分批读取日K特征 2/18 (2026-04-28 至 2026-05-07)",
    )
    repository = _ContextRepository(None)
    repository.engine = engine
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    payload = trading_v3.decision_context(date(2026, 8, 31))

    assert payload["status"] == "loading"
    assert payload["data"]["data_status"] == "LOADING"
    assert payload["data"]["decision_status"] == "LOADING"
    assert payload["data"]["analysis_progress_percent"] == 5
    assert "分批读取日K特征" in payload["data"]["data_blocked_reason"]


def test_decision_context_uses_empty_only_for_valid_completed_run(monkeypatch):
    repository = _ContextRepository(
        {
            "run_uid": "run-empty-20260814",
            "trade_date": date(2026, 8, 14),
            "decision_at": datetime(2026, 8, 14, 15, 20),
            "status": "COMPLETED",
            "dominant_regime": "RANGE",
            "lifecycle_status": "PAPER_TRIAL",
            "target_count": 0,
            "decision_integrity_verified": True,
            "decision_integrity_reason": "",
            "portfolio": {
                "targets": [],
                "decision_snapshot": {"manifest_hash": "a" * 64},
                "decision_truth": {
                    "schema_version": "probiga.trading-v3.decision-truth.v1",
                    "execution_authority": "V2_CANONICAL_LEDGER",
                    "order_authority": False,
                    "real_order_allowed": False,
                },
            },
        }
    )
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    payload = trading_v3.decision_context(date(2026, 8, 14))

    assert payload["status"] == "empty"
    assert payload["data"]["data_status"] == "READY"
    assert payload["data"]["decision_status"] == "EMPTY"
    assert payload["data"]["requested_date"] == "2026-08-14"
    assert payload["data"]["data_date"] == "2026-08-14"
    assert payload["data"]["run_uid"] == "run-empty-20260814"


def test_decision_context_does_not_render_cancelled_run_as_empty(monkeypatch):
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    repository = _ContextRepository(
        {
            "run_uid": "run-cancelled",
            "trade_date": today,
            "decision_at": datetime.combine(today, datetime.min.time()),
            "status": "CANCELLED",
            "dominant_regime": "RANGE",
            "lifecycle_status": "PAPER_TRIAL",
            "target_count": 0,
            "portfolio": {"targets": []},
        }
    )
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    payload = trading_v3.decision_context(today)

    assert payload["status"] == "unavailable"
    assert payload["data"]["data_status"] == "UNAVAILABLE"
    assert payload["data"]["decision_status"] == "UNAVAILABLE"
    assert "DECISION_RUN_STATUS_UNTRUSTED" in payload["data"]["reason_codes"]
    assert "VALID_RUN_WITHOUT_TARGETS" not in payload["data"]["reason_codes"]


def test_unknown_terminal_run_statuses_never_project_ready_or_empty():
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()

    for run_status in ("UNKNOWN", "SKIPPED", "CANCELLED"):
        projected = trading_v3._decision_context_projection(
            {
                "run_uid": f"run-{run_status.lower()}",
                "trade_date": today,
                "decision_at": datetime.combine(
                    today,
                    datetime.min.time(),
                ),
                "status": run_status,
                "dominant_regime": "RANGE",
                "lifecycle_status": "PAPER_TRIAL",
                "target_count": 0,
                "decision_integrity_verified": True,
                "decision_integrity_reason": "",
                "portfolio": {
                    "decision_snapshot": {"manifest_hash": "a" * 64},
                    "decision_truth": {
                        "schema_version": (
                            "probiga.trading-v3.decision-truth.v1"
                        ),
                        "execution_authority": "V2_CANONICAL_LEDGER",
                        "order_authority": False,
                        "real_order_allowed": False,
                    },
                },
            },
            requested_date=today,
        )

        assert projected["data_status"] == "UNAVAILABLE"
        assert projected["decision_status"] == "UNAVAILABLE"
        assert projected["paper_order_authority"] == "NONE"
        assert "DECISION_RUN_STATUS_UNTRUSTED" in projected[
            "reason_codes"
        ]
        assert "VALID_RUN_WITHOUT_TARGETS" not in projected[
            "reason_codes"
        ]


def test_completed_run_without_verified_truth_is_unavailable(monkeypatch):
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    repository = _ContextRepository(
        {
            "run_uid": "run-unverified",
            "trade_date": today,
            "decision_at": datetime.combine(today, datetime.min.time()),
            "status": "COMPLETED",
            "dominant_regime": "RANGE",
            "lifecycle_status": "PAPER_TRIAL",
            "target_count": 0,
            "decision_integrity_verified": False,
            "decision_integrity_reason": "RESULT_HASH_MISMATCH",
            "portfolio": {"targets": []},
        }
    )
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    payload = trading_v3.decision_context(today)

    assert payload["status"] == "unavailable"
    assert payload["data"]["decision_status"] == "UNAVAILABLE"
    assert payload["data"]["reason_codes"].count(
        "DECISION_TRUTH_UNVERIFIED"
    ) == 1
    assert "VALID_RUN_WITHOUT_TARGETS" not in payload["data"]["reason_codes"]


def test_decision_context_keeps_processing_run_in_loading_state(monkeypatch):
    repository = _ContextRepository(
        {
            "run_uid": "run-processing-20260814",
            "trade_date": date(2026, 8, 14),
            "decision_at": datetime(2026, 8, 14, 15, 20),
            "status": "PROCESSING",
            "dominant_regime": "RANGE",
            "lifecycle_status": "PAPER_TRIAL",
            "target_count": 2,
            "portfolio": {"targets": [{"stock_code": "000001"}]},
        }
    )
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    payload = trading_v3.decision_context(date(2026, 8, 14))

    assert payload["status"] == "loading"
    assert payload["data"]["data_status"] == "LOADING"
    assert payload["data"]["decision_status"] == "LOADING"
    assert payload["data"]["actionable_output_allowed"] is False


def test_decision_context_is_unavailable_when_requested_run_is_missing(
    monkeypatch,
):
    repository = _ContextRepository(None)
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    payload = trading_v3.decision_context(date(2026, 8, 12))

    assert payload["status"] == "unavailable"
    assert payload["data"]["decision_status"] == "UNAVAILABLE"
    assert payload["data"]["reason_codes"] == ["DECISION_RUN_NOT_FOUND"]
    assert payload["data"]["requested_date"] == "2026-08-12"


def test_decision_context_exposes_authority_axes_without_order_permission():
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    projected = trading_v3._decision_context_projection(
        {
            "run_uid": "run-ready-20260814",
            "trade_date": today,
            "decision_at": f"{today} 15:20:00",
            "status": "COMPLETED",
            "dominant_regime": "TREND_UP",
            "lifecycle_status": "PAPER_TRIAL",
            "target_count": 2,
            "decision_integrity_verified": True,
            "decision_integrity_reason": "",
            "portfolio": {
                "targets": [],
                "decision_snapshot": {"manifest_hash": "a" * 64},
                "decision_truth": {
                    "schema_version": (
                        "probiga.trading-v3.decision-truth.v1"
                    ),
                    "run_status": "COMPLETED",
                    "actionable_status": "PAPER_ACTIONABLE",
                    "paper_order_authority": "V2_GATED",
                    "execution_authority": "V2_CANONICAL_LEDGER",
                    "order_authority": False,
                    "real_order_allowed": False,
                },
            },
        },
        requested_date=None,
    )

    assert projected["decision_status"] == "CANDIDATE_AVAILABLE"
    assert projected["decision_scope"] == "INTERNAL_PAPER_TRIAL"
    assert projected["ranking_authority"] == "V3_READ_MODEL"
    assert projected["execution_authority"] == "V2_CANONICAL_LEDGER"
    assert projected["paper_order_authority"] == "V2_GATED"
    assert projected["order_authority"] is False
    assert projected["real_order_allowed"] is False
    assert projected["actionable_output_allowed"] is False
    assert projected["historical_read_only"] is False


def test_research_lifecycle_never_advertises_v2_paper_review_authority():
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    projected = trading_v3._decision_context_projection(
        {
            "run_uid": "run-research",
            "trade_date": today,
            "decision_at": f"{today} 15:20:00",
            "status": "COMPLETED",
            "dominant_regime": "TREND_UP",
            "lifecycle_status": "RESEARCH_ONLY",
            "target_count": 1,
            "portfolio": {
                "decision_snapshot": {"manifest_hash": "a" * 64},
                "decision_truth": {
                    "schema_version": (
                        "probiga.trading-v3.decision-truth.v1"
                    ),
                    "run_status": "COMPLETED",
                    "actionable_status": "PAPER_ACTIONABLE",
                    "paper_order_authority": "V2_GATED",
                    "execution_authority": "V2_CANONICAL_LEDGER",
                    "order_authority": False,
                    "real_order_allowed": False,
                },
            },
        },
        requested_date=None,
    )

    assert projected["decision_scope"] == "RESEARCH_ONLY"
    assert projected["paper_order_authority"] == "NONE"
    assert "RESEARCH_LIFECYCLE" in projected["reason_codes"]


def test_historical_context_is_research_only_and_has_no_paper_authority():
    projected = trading_v3._decision_context_projection(
        {
            "run_uid": "run-history-20260805",
            "trade_date": "2026-08-05",
            "decision_at": "2026-08-05 16:05:00",
            "status": "COMPLETED",
            "dominant_regime": "TREND_UP",
            "lifecycle_status": "PAPER_TRIAL",
            "target_count": 2,
            "portfolio": {"targets": []},
        },
        requested_date=date(2026, 8, 5),
    )

    assert projected["historical_read_only"] is True
    assert projected["decision_scope"] == "RESEARCH_ONLY"
    assert projected["paper_order_authority"] == "NONE"
    assert "HISTORICAL_CONTEXT_READ_ONLY" in projected["reason_codes"]


def test_decision_context_uses_snapshot_knowledge_and_feature_times():
    projected = trading_v3._decision_context_projection(
        {
            "run_uid": "run-ready-20260814",
            "trade_date": "2026-08-14",
            "decision_at": "2026-08-14 15:20:00",
            "status": "COMPLETED",
            "dominant_regime": "TREND_UP",
            "lifecycle_status": "PAPER_TRIAL",
            "target_count": 1,
            "portfolio": {
                "targets": [],
                "decision_truth": {
                    "actionable_status": "PAPER_ACTIONABLE",
                },
                "decision_snapshot": {
                    "decision_at": "2026-08-14 15:19:30",
                    "knowledge_cutoff_at": "2026-08-14 15:19:30",
                    "feature_time": "2026-08-14 15:00:00",
                    "manifest_hash": "a" * 64,
                },
            },
        },
        requested_date=None,
    )

    assert projected["decision_at"].startswith("2026-08-14T15:19:30")
    assert projected["knowledge_cutoff_at"].startswith("2026-08-14T15:19:30")
    assert projected["evidence_as_of"].startswith("2026-08-14T15:00:00")
    assert projected["actionable_status"] == "PAPER_ACTIONABLE"
    assert projected["snapshot_manifest_hash"] == "a" * 64


def test_decision_lineage_joins_target_risk_order_fill_and_lot(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    ddl = (
        "CREATE TABLE st_decision_run_v3 (run_uid TEXT, trade_date TEXT, "
        "decision_at TEXT, status TEXT, dominant_regime TEXT, "
        "target_count INTEGER, result_hash TEXT)",
        "CREATE TABLE st_target_portfolio_v3 (run_uid TEXT, rank_no INTEGER, "
        "stock_code TEXT, short_name TEXT, target_weight REAL, "
        "target_value REAL, target_quantity INTEGER, "
        "primary_strategy_key TEXT, strategy_keys_json TEXT, "
        "theme_codes_json TEXT, reason TEXT)",
        "CREATE TABLE st_trade_intent_v2 (intent_id TEXT, account_id TEXT, "
        "decision_run_uid TEXT, strategy_version TEXT, stock_code TEXT, "
        "action TEXT, current_quantity INTEGER, target_quantity INTEGER, "
        "target_weight REAL, earliest_at TEXT, expires_at TEXT, "
        "limit_price REAL, worst_price REAL, initial_stop REAL, "
        "protective_stop REAL, invalidation_condition TEXT, reason_code TEXT, "
        "evidence_json TEXT, intent_version INTEGER, idempotency_key TEXT, "
        "created_at TEXT)",
        "CREATE TABLE st_risk_decision_v2 (intent_id TEXT, "
        "decision_status TEXT, requested_quantity INTEGER, "
        "approved_quantity INTEGER, trade_risk REAL, post_single_weight REAL, "
        "post_total_weight REAL, post_theme_weight REAL, post_open_risk REAL, "
        "post_cash REAL, checks_json TEXT, first_failure TEXT, "
        "decision_hash TEXT)",
        "CREATE TABLE st_order_v2 (order_id TEXT, account_id TEXT, "
        "intent_id TEXT, stock_code TEXT, side TEXT, order_type TEXT, "
        "limit_price REAL, quantity INTEGER, filled_quantity INTEGER, "
        "status TEXT, waiting_reason TEXT, earliest_at TEXT, expires_at TEXT, "
        "idempotency_key TEXT, created_at TEXT, updated_at TEXT)",
        "CREATE TABLE st_fill_v2 (fill_id TEXT, order_id TEXT, account_id TEXT, "
        "stock_code TEXT, side TEXT, quantity INTEGER, price REAL, "
        "gross_amount REAL, fee_amount REAL, net_cash_amount REAL, "
        "quote_event_id TEXT, match_event_id TEXT, idempotency_key TEXT, "
        "filled_at TEXT, created_at TEXT)",
        "CREATE TABLE st_position_lot_v2 (lot_id TEXT, account_id TEXT, "
        "stock_code TEXT, strategy_version TEXT, opened_fill_id TEXT, "
        "opened_trade_date TEXT, settlement_date TEXT, "
        "original_quantity INTEGER, remaining_quantity INTEGER, "
        "cost_price REAL, allocated_buy_fee REAL, position_state TEXT, "
        "approved_target_quantity INTEGER, add_count INTEGER, "
        "initial_stop REAL, protective_stop REAL, "
        "invalidation_condition TEXT, version INTEGER, created_at TEXT, "
        "closed_at TEXT)",
        "CREATE TABLE st_trade_event_v2 (event_id TEXT, trace_id TEXT, "
        "account_id TEXT, event_type TEXT, entity_type TEXT, entity_id TEXT, "
        "event_payload_json TEXT, payload_hash TEXT, occurred_at TEXT, "
        "created_at TEXT)",
    )
    valid_close_payload = {
        "lot_close_allocations": [
            {
                "lot_id": "lot-1",
                "stock_code": "000001",
                "consumed_quantity": 200,
                "remaining_quantity_before": 500,
                "remaining_quantity_after": 300,
            }
        ]
    }
    with engine.begin() as connection:
        for statement in ddl:
            connection.execute(text(statement))
        connection.execute(
            text(
                "INSERT INTO st_decision_run_v3 VALUES "
                "('run-lineage-1','2026-08-14','2026-08-14 15:20:00',"
                "'COMPLETED','RANGE',1,'hash')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_target_portfolio_v3 VALUES "
                "('run-lineage-1',1,'000001','平安银行',0.05,5000,500,"
                "'right_side_trend','[\"right_side_trend\"]','[]','test')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_trade_intent_v2 VALUES "
                "('intent-1','paper-main-v2','run-lineage-1','v3',"
                "'000001','BUY',0,500,0.05,'2026-08-15','2026-08-15',"
                "10,10.1,9,9,'break','READY','{}',1,'ik',"
                "'2026-08-14 15:21:00')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_risk_decision_v2 VALUES "
                "('intent-1','APPROVED',500,500,500,0.05,0.05,0.05,500,"
                "195000,'{}',NULL,'risk-hash')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_order_v2 VALUES "
                "('order-1','paper-main-v2','intent-1','000001','BUY','LIMIT',"
                "10,500,500,'FILLED',NULL,'2026-08-15','2026-08-15','ok',"
                "'2026-08-15 09:30:00','2026-08-15 09:31:00')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_fill_v2 VALUES "
                "('fill-1','order-1','paper-main-v2','000001','BUY',500,10,"
                "5000,5,-5005,'q','m','fill-key','2026-08-15 09:31:00',"
                "'2026-08-15 09:31:00')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_position_lot_v2 VALUES "
                "('lot-1','paper-main-v2','000001','v3','fill-1','2026-08-15',"
                "'2026-08-18',500,500,10,5,'PROBE',500,0,9,9,'break',1,"
                "'2026-08-15 09:31:00',NULL)"
            )
        )

    repository = type("Repository", (), {"engine": engine})()
    monkeypatch.setattr(trading_v3, "_repo", lambda: repository)

    payload = trading_v3.decision_lineage("run-lineage-1")

    assert payload["data"]["summary"] == {
        "target_count": 1,
        "intent_count": 1,
        "exit_intent_count": 0,
        "approved_intent_count": 1,
        "order_count": 1,
        "fill_count": 1,
        "lot_close_allocation_count": 0,
        "lot_close_evidence_status": "NO_SELL_FILL",
        "open_lot_count": 1,
    }
    assert payload["data"]["intents"][0]["checks"] == {}
    assert payload["data"]["lots"][0]["lot_id"] == "lot-1"

    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO st_trade_intent_v2 VALUES "
                "('intent-exit','paper-main-v2','run-lineage-1','v3',"
                "'000001','EXIT',500,0,0,'2026-08-18','2026-08-18',"
                "9.8,9.7,9,9,'break','TREND_INVALIDATED','{}',1,"
                "'exit-ik','2026-08-18 09:30:00')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_order_v2 VALUES "
                "('order-exit','paper-main-v2','intent-exit','000001',"
                "'SELL','LIMIT',9.8,200,200,'FILLED',NULL,'2026-08-18',"
                "'2026-08-18','exit-ok','2026-08-18 09:30:00',"
                "'2026-08-18 09:31:00')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_fill_v2 VALUES "
                "('fill-exit','order-exit','paper-main-v2','000001','SELL',"
                "200,9.8,1960,2,1958,'q2','m2','fill-exit-key',"
                "'2026-08-18 09:31:00','2026-08-18 09:31:00')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_trade_event_v2 VALUES "
                "('event-exit','trace-exit','paper-main-v2',"
                "'PAPER_FILL_APPLIED','FILL','fill-exit',:payload,"
                ":payload_hash,'2026-08-18 09:31:00',"
                "'2026-08-18 09:31:00')"
            ),
            {
                "payload": json.dumps(
                    valid_close_payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "payload_hash": trading_v3._canonical_payload_hash(
                    valid_close_payload
                ),
            },
        )

    exit_payload = trading_v3.decision_lineage("run-lineage-1")
    exit_summary = exit_payload["data"]["summary"]
    assert exit_summary["exit_intent_count"] == 1
    assert exit_summary["fill_count"] == 2
    assert exit_summary["lot_close_allocation_count"] == 1
    assert exit_summary["lot_close_evidence_status"] == "COMPLETE"
    assert exit_payload["data"]["lot_close_allocations"][0][
        "consumed_quantity"
    ] == 200
    assert exit_payload["data"]["fill_event_evidence"][0][
        "payload_hash_verified"
    ] is True

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE st_trade_event_v2 SET payload_hash = :payload_hash "
                "WHERE event_id = 'event-exit'"
            ),
            {"payload_hash": "0" * 64},
        )
    hash_mismatch = trading_v3.decision_lineage("run-lineage-1")["data"]
    assert hash_mismatch["lot_close_evidence"]["status"] == "INCOMPLETE"
    assert "EVENT_PAYLOAD_HASH_MISMATCH" in (
        hash_mismatch["lot_close_evidence"]["reason_codes"]
    )

    duplicate_payload = {
        "lot_close_allocations": [
            {"lot_id": "lot-1", "consumed_quantity": 100},
            {"lot_id": "lot-1", "consumed_quantity": 100},
        ]
    }
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE st_trade_event_v2 "
                "SET event_payload_json = :payload, payload_hash = :payload_hash "
                "WHERE event_id = 'event-exit'"
            ),
            {
                "payload": json.dumps(
                    duplicate_payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "payload_hash": trading_v3._canonical_payload_hash(
                    duplicate_payload
                ),
            },
        )
    duplicate = trading_v3.decision_lineage("run-lineage-1")["data"]
    assert duplicate["lot_close_evidence"]["status"] == "INCOMPLETE"
    assert "LOT_CLOSE_ALLOCATION_DUPLICATE_LOT" in (
        duplicate["lot_close_evidence"]["reason_codes"]
    )

    nonpositive_payload = {
        "lot_close_allocations": [
            {"lot_id": "lot-1", "consumed_quantity": 0}
        ]
    }
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE st_trade_event_v2 "
                "SET event_payload_json = :payload, payload_hash = :payload_hash "
                "WHERE event_id = 'event-exit'"
            ),
            {
                "payload": json.dumps(
                    nonpositive_payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "payload_hash": trading_v3._canonical_payload_hash(
                    nonpositive_payload
                ),
            },
        )
    nonpositive = trading_v3.decision_lineage("run-lineage-1")["data"]
    assert nonpositive["lot_close_evidence"]["status"] == "INCOMPLETE"
    assert "LOT_CLOSE_ALLOCATION_QUANTITY_NOT_POSITIVE" in (
        nonpositive["lot_close_evidence"]["reason_codes"]
    )

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE st_trade_event_v2 "
                "SET event_payload_json = '{', payload_hash = :payload_hash "
                "WHERE event_id = 'event-exit'"
            ),
            {"payload_hash": "0" * 64},
        )
    malformed = trading_v3.decision_lineage("run-lineage-1")["data"]
    assert malformed["lot_close_evidence"]["status"] == "INCOMPLETE"
    assert "EVENT_PAYLOAD_JSON_INVALID" in (
        malformed["lot_close_evidence"]["reason_codes"]
    )


def test_manual_action_job_tracks_the_exact_scheduler_history_row(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE st_scheduled_task_history ("
                "run_uid TEXT, task_id INTEGER, task_name TEXT, task_type TEXT, "
                "run_at TEXT, finished_at TEXT, status TEXT, duration INTEGER, "
                "exit_code INTEGER, output TEXT, trigger_source TEXT)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_scheduled_task_history VALUES ("
                "'0123456789abcdef0123456789abcdef',72,'manual decision',"
                "'trading_v3_close_decision','2026-08-16 10:00:00',"
                "'2026-08-16 10:00:12','completed',12,0,'ok','manual')"
            )
        )
    monkeypatch.setattr(trading_v3, "get_engine", lambda: engine)

    payload = trading_v3.manual_action_job(
        "0123456789abcdef0123456789abcdef"
    )

    assert payload["status"] == "ok"
    assert payload["data"]["state"] == "succeeded"
    assert payload["data"]["terminal"] is True
    assert payload["data"]["job_id"] == "0123456789abcdef0123456789abcdef"


def test_manual_action_job_treats_scheduler_timeout_as_terminal_failure(
    monkeypatch,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE st_scheduled_task_history ("
                "run_uid TEXT, task_id INTEGER, task_name TEXT, task_type TEXT, "
                "run_at TEXT, finished_at TEXT, status TEXT, duration INTEGER, "
                "exit_code INTEGER, output TEXT, trigger_source TEXT)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_scheduled_task_history VALUES ("
                "'abcdef0123456789abcdef0123456789',72,'manual decision',"
                "'trading_v3_close_decision','2026-08-16 10:00:00',"
                "'2026-08-16 10:30:00','timeout',1800,124,'timed out','manual')"
            )
        )
    monkeypatch.setattr(trading_v3, "get_engine", lambda: engine)

    payload = trading_v3.manual_action_job(
        "abcdef0123456789abcdef0123456789"
    )

    assert payload["status"] == "failed"
    assert payload["data"]["state"] == "failed"
    assert payload["data"]["terminal"] is True
    assert payload["data"]["status"] == "timeout"


def test_historical_overview_and_portfolio_share_the_requested_run(monkeypatch):
    selected = date(2026, 8, 13)

    class Repository:
        def latest_run_metadata(self, trade_date=None):
            assert trade_date == selected
            return {
                "run_uid": "run-history-20260813",
                "trade_date": selected,
                "decision_at": datetime(2026, 8, 13, 15, 20),
                "portfolio": {"targets": []},
            }

        def stock_pool(self, *, trade_date=None):
            assert trade_date == selected
            return {
                "run_uid": "run-history-20260813",
                "trade_date": "2026-08-13",
                "items": [
                    {
                        "stock_code": "000001",
                        "stock_name": "平安银行",
                        "target": {
                            "target_weight": 0.05,
                            "target_quantity": 500,
                        },
                    }
                ],
            }

    monkeypatch.setattr(trading_v3, "_repo", Repository)

    overview = trading_v3.overview(compact=False, trade_date=selected)
    portfolio = trading_v3.latest_portfolio(trade_date=selected)

    assert overview["data"]["run"]["run_uid"] == "run-history-20260813"
    assert overview["data"]["positions"] == []
    assert portfolio["data"][0]["run_uid"] == "run-history-20260813"
    assert portfolio["data"][0]["decision_scope"] == "RESEARCH_ONLY"
    assert portfolio["data"][0]["new_buy_eligible"] is False

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _function_source(relative_path: str, function_name: str) -> str:
    path = ROOT / relative_path
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"missing function {function_name} in {relative_path}")


def test_strategy_finance_and_notice_readers_have_no_mutable_table_fallback():
    finance_readers = (
        ("biz/analysis/sync_analysis_fast.py", "load_finance"),
        ("server/trading_v2/candidate_context.py", "_load_finance"),
        ("server/trading_v3/daily_features.py", "_load_finance"),
        ("server/api/routers/screener.py", "_enrich_selector_evidence"),
    )
    notice_readers = (
        ("biz/analysis/sync_analysis_fast.py", "load_notice_features"),
        ("server/trading_v2/candidate_context.py", "_load_notices"),
        ("server/trading_v3/daily_features.py", "_load_recent_notices"),
    )
    for path, name in finance_readers:
        body = _function_source(path, name)
        assert "load_finance_facts" in body
        assert "si_stock_finance" not in body
        if name == "_enrich_selector_evidence":
            assert "load_event_facts" in body
            assert "pit_strategy_status" in body
            assert "analysis_data_quality_flags" in body
            assert "pit_score_binding_verified" in body
    for path, name in notice_readers:
        body = _function_source(path, name)
        assert "load_event_facts" in body
        assert "si_notice_eastmoney" not in body


def test_v3_industry_reader_requires_explicit_research_for_last_known_fallback():
    body = _function_source(
        "server/trading_v3/daily_features.py", "_load_industries"
    )
    assert "allow_research_last_known: bool = False" in body
    assert "if not source_date and allow_research_last_known:" in body
    assert "RETROSPECTIVE_RESEARCH_LAST_KNOWN_QMT_SNAPSHOT" in body
    assert "si_industry_sw" not in body
    assert "st_strategy_industry_history" not in body


def test_batch_analysis_does_not_score_mutable_flash_news():
    body = _function_source(
        "biz/analysis/sync_analysis_fast.py", "_prepare_batch_outputs"
    )
    assert "load_news_features(" not in body
    assert 'news = pd.DataFrame({"stock_code": []})' in body
    v2_body = _function_source(
        "server/trading_v2/candidate_context.py", "_load_news"
    )
    assert "FROM st_news_flash" not in v2_body
    assert '"DATA_BLOCKED"' in v2_body


def test_unified_stock_analysis_forces_strategy_safe_data_loader_path():
    body = _function_source(
        "server/engine/stock_analysis_engine.py", "analyze"
    )
    assert "strategy_context=True" in body
    assert "pit_common_cutoff" in body
    assert "fact_cutoff_at=fact_cutoff_at" in body
    assert "PIT_DATA_BLOCKED" in body

    cached = _function_source(
        "server/engine/stock_analysis_engine.py", "analyze_with_cache"
    )
    assert '"status": "SUSPENDED"' in cached
    assert "PIT_DATA_BLOCKED" in cached

    v2 = _function_source(
        "server/trading_v2/candidate_context.py", "apply_candidate_context"
    )
    assert 'signal["signal_status"] = "BLOCKED"' in v2
    assert "pit_context_blocked" in v2

    v3 = _function_source(
        "server/trading_v3/daily_features.py", "load_daily_feature_universe"
    )
    assert 'item["entry_eligible"] = 0.0' in v3
    assert "pit_strategy_eligible" in v3


def test_finance_is_pit_first_but_eastmoney_notice_is_display_only():
    finance = _function_source(
        "biz/stock_finance/sync_finance.py", "upsert_finance"
    )
    notice = _function_source("biz/notice/sync_notice_em.py", "upsert_rows")
    assert finance.index("append_finance_revision(") < finance.index(
        "conn.execute(sql, params)"
    )
    assert "append_event_revision(" not in notice
    assert "append_source_coverage(" not in notice
    assert "conn.execute(UPSERT_SQL, payload)" in notice


def test_current_industry_cache_cannot_enter_strategy_or_rank_consumers():
    consumers = (
        ("biz/analysis/sync_analysis_fast.py", "_load_sector_industry_memberships"),
        ("biz/analysis/sync_analysis_fast.py", "load_sector_rotation_features"),
        ("server/api/routers/screener.py", "_run_intraday_sector"),
        ("biz/market_radar/core.py", "_load_sector_metadata"),
        ("biz/premarket/theme_forecast.py", "_load_forecast_inputs"),
    )
    for path, name in consumers:
        assert "FROM si_industry_sw" not in _function_source(path, name)

    v3 = _function_source(
        "server/trading_v3/daily_features.py",
        "load_daily_feature_universe",
    )
    assert 'item.get("industry_pit_status") == PIT_AVAILABLE' in v3
    assert 'item["entry_eligible"] = 0.0' in v3


def test_mutable_news_is_display_only_and_cannot_authorize_funding():
    context = _function_source(
        "server/trading_v3/context.py", "load_asof_context"
    )
    assert "allow_legacy_display: bool = False" in context
    assert '"context_evidence_status": "DATA_BLOCKED"' in context
    assert '"funding_eligible": False' in context

    commentary = _function_source(
        "server/api/routers/commentary.py", "_assess_one"
    )
    assert "news_count=0" in commentary
    assert '"decision_scope": "RESEARCH_DISPLAY_ONLY"' in commentary
    assert '"order_authority": False' in commentary

    entry_policy = _function_source(
        "server/trading_v2/legacy_execution_policy.py",
        "sector_entry_wait_reason",
    )
    assert "FROM sm_market_radar_sector" not in entry_policy
    assert "WAIT_SECTOR_CONFIRMATION" in entry_policy


def test_screener_persisted_scores_require_matching_revision_evidence():
    body = _function_source(
        "server/api/routers/screener.py", "_analysis_pit_binding"
    )
    assert "finance_revision_id=" in body
    assert "finance_content_hash=" in body
    assert "event_revision_ids" in body
    assert "event_content_hashes" in body


def test_privileged_deployment_preflights_and_creates_pit_schema():
    preflight = _function_source(
        "tools/prepare_strategy_governance_schema.py", "_preflight_schema"
    )
    cutover = _function_source(
        "tools/prepare_strategy_governance_schema.py", "_cutover_schema"
    )
    triggers = _function_source(
        "tools/prepare_strategy_governance_schema.py", "_non_v3_trigger_contracts"
    )
    assert "preflight_pit_fact_schema" in preflight
    assert "ensure_pit_fact_schema" in cutover
    assert "pit_fact_schema_health" in cutover
    assert "PIT_FACT_TRIGGER_STATEMENTS" in triggers


def test_strategy_data_loader_ignores_mutable_reference_tables():
    body = _function_source(
        "server/engine/data_loader.py", "load_full_data"
    )
    assert "if strategy_context:" in body
    assert "LEGACY_CURRENT_REFERENCE_INPUTS_IGNORED" in body
    assert "holder_rows = [] if strategy_context" in body
    assert "hot_rank_rows = [] if strategy_context" in body
    assert "lifting_rows = [] if strategy_context" in body
    assert "mine_rows = [] if strategy_context" in body
    engine = _function_source(
        "server/engine/stock_analysis_engine.py", "analyze"
    )
    assert "reference_status != PIT_AVAILABLE" in engine

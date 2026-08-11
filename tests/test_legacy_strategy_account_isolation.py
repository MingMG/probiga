from __future__ import annotations

from datetime import date

import pytest

from server.trading_v2 import planner, position_monitor
from server.trading_v2.legacy_strategy_account_boundary import (
    LegacyStrategyAccountIsolationError,
    require_legacy_strategy_account,
)
from server.trading_v3 import paper_execution


class _UnexpectedDatabaseUse:
    def begin(self):
        raise AssertionError("database transaction must not be opened")

    def execute(self, *_args, **_kwargs):
        raise AssertionError("SQL must not be issued")


class _ReachedLegacyImplementation(RuntimeError):
    pass


@pytest.mark.parametrize(
    "account_id",
    (
        "paper-v4",
        "paper-v4-campaign-001",
        "PAPER-V4-CAMPAIGN-001",
        "  paper-v4-campaign-001  ",
    ),
)
def test_boundary_reserves_v4_namespace_under_database_collation_variants(
    account_id: str,
) -> None:
    with pytest.raises(LegacyStrategyAccountIsolationError):
        require_legacy_strategy_account(account_id, entrypoint="test")


def test_boundary_keeps_legacy_and_v3_namespaces() -> None:

    require_legacy_strategy_account(
        "paper-main-v2",
        entrypoint="test",
    )
    require_legacy_strategy_account(
        "paper-v3-next",
        entrypoint="test",
    )


def test_v4_account_cannot_enter_v2_planner_or_issue_sql() -> None:
    with pytest.raises(LegacyStrategyAccountIsolationError):
        planner.persist_portfolio_competition(
            _UnexpectedDatabaseUse(),
            run_uid="run-v4",
            trade_date=date(2026, 8, 3),
            account={"account_id": "paper-v4-campaign-001"},
            market_regime="NORMAL",
            candidates=[],
        )


@pytest.mark.parametrize("account_id", ["paper-main-v2", "paper-v3-next"])
def test_non_v4_accounts_still_reach_v2_planner(
    monkeypatch: pytest.MonkeyPatch,
    account_id: str,
) -> None:
    def reached():
        raise _ReachedLegacyImplementation

    monkeypatch.setattr(planner, "load_portfolio_policy", reached)
    with pytest.raises(_ReachedLegacyImplementation):
        planner.persist_portfolio_competition(
            _UnexpectedDatabaseUse(),
            run_uid="run-legacy",
            trade_date=date(2026, 8, 3),
            account={"account_id": account_id},
            market_regime="NORMAL",
            candidates=[],
        )


def test_v4_account_cannot_enter_v2_position_monitor_or_open_transaction() -> None:
    with pytest.raises(LegacyStrategyAccountIsolationError):
        position_monitor.monitor_positions(
            _UnexpectedDatabaseUse(),
            trade_date=date(2026, 8, 3),
            run_uid="run-v4",
            account_id="paper-v4-campaign-001",
        )


@pytest.mark.parametrize("account_id", ["paper-main-v2", "paper-v3-next"])
def test_non_v4_accounts_still_reach_v2_position_monitor(
    account_id: str,
) -> None:
    with pytest.raises(AssertionError, match="transaction"):
        position_monitor.monitor_positions(
            _UnexpectedDatabaseUse(),
            trade_date=date(2026, 8, 3),
            run_uid="run-legacy",
            account_id=account_id,
        )


@pytest.mark.parametrize(
    "entrypoint",
    [
        paper_execution.freeze_pending_v3_buys,
        paper_execution.materialize_internal_paper_orders,
    ],
)
def test_v4_account_cannot_enter_v3_legacy_paper_execution(
    entrypoint,
) -> None:
    kwargs = {"account_id": "paper-v4-campaign-001"}
    if entrypoint is paper_execution.materialize_internal_paper_orders:
        kwargs["run_uid"] = "run-v4"
    with pytest.raises(LegacyStrategyAccountIsolationError):
        entrypoint(_UnexpectedDatabaseUse(), **kwargs)


@pytest.mark.parametrize("account_id", ["paper-main-v2", "paper-v3-next"])
def test_non_v4_accounts_still_reach_v3_freeze(
    account_id: str,
) -> None:
    with pytest.raises(AssertionError, match="transaction"):
        paper_execution.freeze_pending_v3_buys(
            _UnexpectedDatabaseUse(),
            account_id=account_id,
        )


@pytest.mark.parametrize("account_id", ["paper-main-v2", "paper-v3-next"])
def test_non_v4_accounts_still_reach_v3_materialization(
    monkeypatch: pytest.MonkeyPatch,
    account_id: str,
) -> None:
    def reached():
        raise _ReachedLegacyImplementation

    monkeypatch.setattr(paper_execution, "load_v3_config", reached)
    with pytest.raises(_ReachedLegacyImplementation):
        paper_execution.materialize_internal_paper_orders(
            _UnexpectedDatabaseUse(),
            run_uid="run-legacy",
            account_id=account_id,
        )

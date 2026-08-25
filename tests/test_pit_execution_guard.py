from server.common.pit_execution_guard import (
    build_open_execution_receipt,
    daily_bar_execution_disposition,
    nonlinear_impact_rate,
    participation_capped_quantity,
    validate_open_execution_receipt,
)


def _bar(**changes):
    result = {
        "open": 10.0,
        "high": 10.5,
        "low": 9.8,
        "close": 10.2,
        "pre_close": 10.0,
        "volume": 1_000_000,
        "amount": 10_000_000,
    }
    result.update(changes)
    return result


def test_locked_limit_guards_cover_st_and_wider_boards_without_name_guessing():
    st_up = daily_bar_execution_disposition(
        _bar(open=10.5, high=10.5, low=10.5, close=10.5), side="BUY",
    )
    wide_down = daily_bar_execution_disposition(
        _bar(open=8.0, high=8.0, low=8.0, close=8.0), side="SELL",
    )

    assert st_up == {
        "status": "KNOWN_UNFILLED",
        "reason": "LOCKED_LIMIT_UP",
        "executable": False,
    }
    assert wide_down["reason"] == "LOCKED_LIMIT_DOWN"
    assert wide_down["executable"] is False


def test_missing_or_suspended_bar_never_becomes_an_execution():
    assert daily_bar_execution_disposition(None, side="BUY")["status"] == "DATA_BLOCKED"
    suspended = daily_bar_execution_disposition(
        _bar(volume=0, amount=0), side="SELL",
    )
    assert suspended["status"] == "KNOWN_UNFILLED"
    assert suspended["executable"] is False


def test_capacity_is_board_lot_bounded_and_impact_is_nonlinear():
    result = participation_capped_quantity(
        desired_notional_cny=1_000_000,
        price=10,
        daily_amount_cny=2_000_000,
        maximum_participation_rate=0.05,
    )

    assert result["quantity"] == 10_000
    assert result["reason"] == "CAPACITY_CAPPED"
    assert result["participation_rate"] == 0.05
    assert nonlinear_impact_rate(
        participation_rate=0.0125,
        maximum_participation_rate=0.05,
        base_slippage_rate=0.001,
    ) == 0.0005


def test_open_execution_receipt_is_content_addressed_and_bound_to_session():
    receipt = build_open_execution_receipt(
        stock_code="000001",
        trade_date="2026-08-24",
        execution_price=10.02,
        observed_at="2026-08-24T09:31:00",
        source_provider="QMT_FIRST_1MIN",
        source_payload_hash="a" * 64,
    )

    verified = validate_open_execution_receipt(
        receipt,
        stock_code="000001",
        trade_date="2026-08-24",
        daily_open_price=10.0,
    )
    tampered = dict(receipt)
    tampered["execution_price"] = 10.03

    assert verified["valid"] is True
    assert verified["receipt_hash"] == receipt["receipt_hash"]
    assert validate_open_execution_receipt(
        tampered,
        stock_code="000001",
        trade_date="2026-08-24",
        daily_open_price=10.0,
    )["valid"] is False


def test_missing_open_receipt_never_becomes_funded_execution():
    result = validate_open_execution_receipt(
        None,
        stock_code="000001",
        trade_date="2026-08-24",
        daily_open_price=10.0,
    )

    assert result == {
        "valid": False,
        "reason": "MISSING_IMMUTABLE_OPEN_RECEIPT",
    }

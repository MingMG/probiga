"""Shared fail-closed safety contracts for strategy governance payloads."""

from __future__ import annotations

from typing import Any


REAL_ORDER_AUTHORITY_FIELDS = frozenset({
    "automatic_real_order_submission",
    "real_order_authority",
    "real_order_submission_enabled",
    "real_order_submission",
    "real_orders_allowed",
    "real_trading_enabled",
    "order_authority",
})


def assert_real_order_authority_closed(
    value: Any, *, path: str = "result", error_type: type[Exception] = ValueError,
) -> None:
    """Require every recognized real-order authority field to be exactly false."""

    if isinstance(value, dict):
        for raw_name, item in value.items():
            name = str(raw_name)
            if name in REAL_ORDER_AUTHORITY_FIELDS and item is not False:
                raise error_type(f"{path}.{name}未显式关闭真实下单权限")
            assert_real_order_authority_closed(
                item, path=f"{path}.{name}", error_type=error_type,
            )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            assert_real_order_authority_closed(
                item, path=f"{path}[{index}]", error_type=error_type,
            )


def real_order_authority_is_closed(value: Any) -> bool:
    try:
        assert_real_order_authority_closed(value)
    except ValueError:
        return False
    return True


__all__ = [
    "REAL_ORDER_AUTHORITY_FIELDS",
    "assert_real_order_authority_closed",
    "real_order_authority_is_closed",
]

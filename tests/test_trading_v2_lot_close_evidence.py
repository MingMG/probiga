from datetime import date, datetime

from server.trading_v2.execution import _consume_sell_lots


class _Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _Connection:
    def __init__(self):
        self.updates = []

    def execute(self, statement, params=None):
        sql = str(statement)
        if "SELECT lot_id, remaining_quantity" in sql:
            return _Result([
                {"lot_id": "lot-old", "remaining_quantity": 100},
                {"lot_id": "lot-new", "remaining_quantity": 200},
            ])
        if "UPDATE st_position_lot_v2" in sql:
            self.updates.append(dict(params or {}))
        return _Result()


def test_sell_lot_consumption_returns_exact_fifo_close_allocations():
    connection = _Connection()

    allocations = _consume_sell_lots(
        connection,
        account_id="paper-main-v2",
        stock_code="000001",
        trade_date=date(2026, 8, 18),
        quantity=250,
        now=datetime(2026, 8, 18, 9, 31),
    )

    assert allocations == [
        {
            "lot_id": "lot-old",
            "stock_code": "000001",
            "consumed_quantity": 100,
            "remaining_before": 100,
            "remaining_after": 0,
        },
        {
            "lot_id": "lot-new",
            "stock_code": "000001",
            "consumed_quantity": 150,
            "remaining_before": 200,
            "remaining_after": 50,
        },
    ]
    assert [item["remaining"] for item in connection.updates] == [0, 50]

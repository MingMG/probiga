from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pymysql as mysql_driver
from pymysql.connections import Connection as MySQLConnection
from pymysql.cursors import Cursor
from sqlalchemy.dialects.mysql import pymysql

from server.trading_v2.public_quote_failover import _persist_result


def test_full_market_quote_publish_uses_real_driver_batches_in_one_transaction():
    # Exercise the actual driver parser and escaping, replacing only the
    # network operation. PyMySQL 1.2 rejects literal VALUES items for batching.
    driver = MySQLConnection(defer_connect=True, charset="utf8mb4")
    driver.server_status = 0
    cursor = Cursor(driver)
    sent = []
    cursor._query = lambda sql: sent.append(sql) or 1
    connection = MagicMock()

    def execute(statement, payload):
        sql = str(statement.compile(dialect=pymysql.dialect(dbapi=mysql_driver)))
        if isinstance(payload, list):
            return cursor.executemany(sql, payload)
        return cursor.execute(sql, payload)

    connection.execute.side_effect = execute
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = connection
    quote_at = datetime(2026, 9, 10, 10, 57, 31)
    rows = [
        {
            "stock_code": f"{index:06d}",
            "short_name": "测试'股票",
            "price": 10.1,
            "pre_close": 10.0,
            "change_pct": 1.0,
            "volume": 100.0,
            "amount": 1010.0,
            "source_count": 2,
            "provider_mask": "sina,tencent",
            "price_deviation_pct": 0.0,
        }
        for index in range(501)
    ]
    result = {
        "rows": rows,
        "quality_status": "PASS",
        "expected_count": len(rows),
        "observed_count": len(rows),
        "coverage": 1.0,
        "provider_count": 2,
        "minimum_sources_per_symbol": 2,
        "agreement_ratio": 1.0,
        "maximum_price_deviation_pct": 0.0,
        "maximum_source_latency_seconds": 1.0,
        "evidence": ["two-source agreement"],
    }

    _persist_result(
        engine, now=quote_at, config={}, provider_status={}, result=result,
    )

    engine.begin.assert_called_once_with()
    assert len(sent) == 3  # Two bounded quote INSERTs and their receipt.
    statements = [
        bytes(sql).decode("utf8") if isinstance(sql, (bytes, bytearray)) else sql
        for sql in sent
    ]
    assert "st_public_quote_current_v2" in statements[0]
    assert "st_public_quote_current_v2" in statements[1]
    assert "st_public_quote_receipt_v2" in statements[2]
    batches = [call.args[1] for call in connection.execute.call_args_list[:2]]
    assert [len(batch) for batch in batches] == [500, 1]
    assert all(row["quality_status"] == "PASS" for batch in batches for row in batch)
    assert all(row["quote_at"] == quote_at for batch in batches for row in batch)
    assert connection.execute.call_args_list[-1].args[1]["quote_at"] == quote_at

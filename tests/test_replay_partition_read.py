"""Real database budget failures; synthetic market values remain in tests."""
import json
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from server.engine.strategy_governance import _digest
from server.engine.strategy_rule_backtest import PART_ENTITY, _partitions, partition_key


def test_partition_directory_then_primary_keys_survive_real_database_budget(monkeypatch):
    engine = create_engine('sqlite://')
    state, proposal = {'run_id': '1' * 64}, {'rule_hash': '2' * 64}
    expected = {}
    with engine.begin() as c:
        c.execute(text('PRAGMA case_sensitive_like=ON'))
        # WITHOUT ROWID mirrors InnoDB's primary key in a secondary index.
        c.execute(text('''CREATE TABLE st_strategy_governance_audit(
            audit_id TEXT PRIMARY KEY,entity_type TEXT,entity_key TEXT,created_at TEXT,
            payload_json TEXT,audit_hash TEXT,after_json TEXT) WITHOUT ROWID'''))
        c.execute(text('CREATE INDEX by_entity ON st_strategy_governance_audit(entity_type,entity_key,created_at)'))
        for i in range(79):
            day = (date(2026, 5, 28) + timedelta(days=i)).isoformat()
            value = {'day': day, 'status': 'COMPLETE', 'signals': {'fixture': [
                {'stock_code': '000001', 'score': i, 'signal_known_at': day + ' 18:00:00'}]}}
            payload = {'after': value}
            c.execute(text('INSERT INTO st_strategy_governance_audit VALUES(:id,:e,:k,:d,:p,:h,:a)'),
                      {'id': f'{i:032x}', 'e': PART_ENTITY, 'k': partition_key(state, proposal, 'S', day),
                       'd': day, 'p': json.dumps(payload), 'h': _digest(payload), 'a': json.dumps(value)})
            expected[day] = value
    with engine.connect() as c:
        driver = c.connection.driver_connection
        predicate = ' FROM st_strategy_governance_audit WHERE entity_type=:e AND entity_key LIKE :p ORDER BY entity_key '
        params = {'e': PART_ENTITY, 'p': state['run_id'][:24] + ':' + proposal['rule_hash'][:24] + ':S:%'}
        old = text('SELECT payload_json,audit_hash,after_json' + predicate + 'LIMIT 256')
        directory = text('SELECT audit_id,entity_key' + predicate + 'LIMIT 257')
        def steps(statement):
            count = [0]
            def progress():
                count[0] += 1
                return 0
            driver.set_progress_handler(progress, 1)
            c.execute(statement, params).all()
            driver.set_progress_handler(None, 0)
            return count[0]
        cheap, expensive = steps(directory), steps(old)
        assert cheap < expensive
        limit = (cheap + expensive) // 2
        batches = []
        def budget(_c, _cursor, statement, parameters, _context, _many):
            used = 0
            if 'WHERE audit_id IN' in statement:
                batches.append(len(parameters))
            def progress():
                nonlocal used
                used += 1
                return int(used > limit)
            driver.set_progress_handler(progress, 1)
        event.listen(c, 'before_cursor_execute', budget)
        try:
            with pytest.raises(OperationalError, match='interrupted'):
                c.execute(old, params).all()
            assert _partitions(c, state, proposal, 'S') == expected
        finally:
            driver.set_progress_handler(None, 0)
            event.remove(c, 'before_cursor_execute', budget)
        assert batches and max(batches) <= 4
        changed = {**next(iter(expected.values())), 'status': 'ALTERED'}
        c.execute(text('UPDATE st_strategy_governance_audit SET after_json=:value WHERE audit_id=:id'),
                  {'value': json.dumps(changed), 'id': '0' * 32})
        with pytest.raises(ValueError, match='审计哈希'):
            _partitions(c, state, proposal, 'S')
        from server.engine import strategy_rule_backtest
        ticks = iter((0, 121))
        monkeypatch.setattr(strategy_rule_backtest.time, 'monotonic', lambda: next(ticks))
        with pytest.raises(TimeoutError, match='120秒总时限'):
            _partitions(c, state, proposal, 'S')
    engine.dispose()

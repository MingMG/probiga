from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text

from server.common import strategy_audit_heads
from server.common.strategy_audit_heads import latest_head, recent_heads


@pytest.fixture
def journal():
    engine = create_engine('sqlite://')
    with engine.begin() as connection:
        connection.execute(text('''CREATE TABLE st_strategy_governance_audit (
            audit_id TEXT PRIMARY KEY,entity_type TEXT,entity_key TEXT,created_at TEXT,action TEXT)'''))
        connection.execute(text('CREATE INDEX journal_entity ON st_strategy_governance_audit(entity_type,entity_key,created_at)'))
    yield engine
    engine.dispose()


def test_recent_metadata_keeps_all_experiments_and_same_time_ties(journal):
    records = []
    start = datetime(2026, 11, 20)
    for number in range(46):
        key = format(number, '064x')
        for sequence in range(4):
            records.append({'entity': 'NATIVE_REPLAY', 'key': key,
                'at': str(start + timedelta(days=number % 7, seconds=sequence // 2)),
                'action': f'CHECKPOINT_{sequence:08d}', 'id': f'{number:04d}-{sequence}'})
    records.append({**records[3], 'id': 'zz-final-tie'})
    with journal.begin() as connection:
        connection.execute(text('''INSERT INTO st_strategy_governance_audit VALUES (:id,:entity,:key,:at,:action)'''), records)
        connection.execute(text("INSERT INTO st_strategy_governance_audit VALUES ('other','OTHER','other','2099-01-01','new')"))
    expected = {}
    for row in records:
        if row['key'] not in expected or (row['at'], row['action'], row['id']) > (
                expected[row['key']]['at'], expected[row['key']]['action'], expected[row['key']]['id']):
            expected[row['key']] = row
    with journal.connect() as connection:
        for key, row in expected.items():
            head = latest_head(connection, 'NATIVE_REPLAY', key)
            assert (head['latest_at'], head['action'], head['audit_id']) == (row['at'], row['action'], row['id'])
        for limit in (8, 12, 46):
            actual = recent_heads(connection, 'NATIVE_REPLAY', limit)
            ordered = sorted(expected.values(), key=lambda row: (row['at'], row['key']), reverse=True)[:limit]
            assert [(row['entity_key'], row['audit_id']) for row in actual] == [(row['key'], row['id']) for row in ordered]
        assert latest_head(connection, 'NATIVE_REPLAY', 'absent') is None
        assert recent_heads(connection, 'ABSENT', 12) == []


def test_head_read_deadline_never_returns_a_partial_directory(journal):
    with journal.connect() as connection:
        with pytest.raises(TimeoutError, match='HEAD_READ_DEADLINE'):
            recent_heads(connection, 'NATIVE_REPLAY', 12, deadline=1)


@pytest.mark.parametrize('limit', [8, 600])
def test_recent_heads_covers_multiple_pages_with_bounded_round_trip_time(journal, monkeypatch, limit):
    records = []
    # Winning experiments straddle both page boundaries and the final page.
    winners = (1, 255, 256, 511, 512, 599)
    for number in range(600):
        key = format(number, '064x')
        for sequence in range(2):
            records.append({'id': f'{number:04d}-{sequence}', 'entity': 'NATIVE_REPLAY',
                'key': key, 'at': f'2026-11-20 00:00:0{sequence}',
                'action': f'CHECKPOINT_{sequence:08d}'})
        if number in winners:
            for suffix in ('a', 'z'):
                records.append({'id': f'{number:04d}-{suffix}', 'entity': 'NATIVE_REPLAY',
                    'key': key, 'at': f'2026-11-21 00:00:0{winners.index(number)}',
                    'action': 'CHECKPOINT_00000009'})
    with journal.begin() as connection:
        connection.execute(text('''INSERT INTO st_strategy_governance_audit
            VALUES (:id,:entity,:key,:at,:action)'''), records)
        connection.execute(text("INSERT INTO st_strategy_governance_audit VALUES ('other','OTHER','other','2099-01-01','new')"))
    expected = {}
    for row in records:
        prior = expected.get(row['key'])
        if prior is None or (row['at'], row['action'], row['id']) > (
                prior['at'], prior['action'], prior['id']):
            expected[row['key']] = row
    ordered = sorted(expected.values(), key=lambda row: (row['at'], row['key']), reverse=True)[:limit]

    clock = SimpleNamespace(now=1000.0)
    monkeypatch.setattr(strategy_audit_heads, 'time', SimpleNamespace(monotonic=lambda: clock.now))
    # Model time spent on each actual SQL round trip, without sleeping or
    # prescribing which SQL statements the implementation must issue.
    def round_trip(*_args):
        clock.now += 0.02
    with journal.connect() as connection:
        event.listen(connection, 'before_cursor_execute', round_trip)
        try:
            actual = recent_heads(connection, 'NATIVE_REPLAY', limit, deadline=1008.0)
        finally:
            event.remove(connection, 'before_cursor_execute', round_trip)
    assert clock.now < 1008.0
    assert [(row['entity_key'], row['latest_at'], row['action'], row['audit_id']) for row in actual] == [
        (row['key'], row['at'], row['action'], row['id']) for row in ordered]

"""Evidence and freshness tests for the read-only acquisition monitor."""
import csv
import io
from datetime import date, datetime, timedelta

import pytest

from server.api import data_monitor as dm


DAY = '2026-09-18'
CURRENT = datetime(2026, 9, 21, 19)


def group(code, n=1, valid=1, unique=1, bad=0, times=''):
    return dict(code=code, n=n, valid_n=valid, unique_n=unique, bad=bad, valid_times=times)


def test_identity_substitution_cannot_pass_equal_row_count():
    result = dm.assess({'A', 'B'}, [group('A'), group('C')])
    assert result['status'] == 'partial'
    assert result['missing_count'] == 1
    assert result['invalid_count'] == 1
    assert {m['stock_code'] for m in result['missing']} == {'B', 'C'}


def test_duplicates_and_invalid_rows_never_pass():
    assert dm.assess({'A'}, [group('A', n=2)])['status'] == 'partial'
    assert dm.assess({'A'}, [group('A', bad=1)])['status'] == 'partial'
    assert dm.assess({'A'}, [])['status'] == 'missing'
    assert dm.assess(None, [group('A')])['status'] == 'unknown'
    assert dm.assess(set(), [])['status'] == 'unknown'


def test_optional_suspended_flow_is_not_unexpected():
    result = dm.assess({'A'}, [group('A'), group('S')], allowed={'A', 'S'})
    assert result['status'] == 'full'
    assert result['expected_count'] == result['actual_count'] == 1


def test_missing_details_are_bounded_without_losing_total():
    expected = {f'{value:06d}' for value in range(500)}
    result = dm.assess(expected, [], missing_limit=dm.MAX_DETAILS)
    assert len(result['missing']) == dm.MAX_DETAILS
    assert result['missing_total'] == 500
    assert result['missing_count'] == 500


def test_daily_failure_does_not_block_independent_source_dataset():
    observer = object.__new__(dm.Observer)

    def fail_daily(*_args, **_kwargs):
        raise RuntimeError('daily unavailable')

    observer.daily_context = fail_daily
    observer.source_partition = lambda *_args: {
        'status': 'full',
        'reason': 'verified',
        'missing': [],
        'missing_total': 0,
    }
    result = observer.inspect_day(
        DAY, CURRENT, datasets={'daily', 'hot'}, missing_limit=0
    )
    assert result['daily']['status'] == 'unknown'
    assert result['hot']['status'] == 'full'


def test_native_minute_grid_requires_every_slot_and_reports_lunch_ranges():
    grid = list(dm.minute_time_grid())
    assert len(grid) == 241
    present = grid[:100]
    result = dm.assess({'A'}, [group('A', n=100, unique=100, valid=100, times=','.join(present))], slots=241, expected_slots=grid)
    assert result['status'] == 'partial'
    assert result['missing_count'] == 141
    assert result['missing'][0]['missing_times'] == ['11:10–11:30', '13:01–15:00']


def test_calendar_requires_authoritative_record_not_weekday():
    calendar = dm.calendar_map([{'trade_date': DAY, 'trade_status': 1}, {'trade_date': DAY, 'trade_status': 0}])
    assert calendar[DAY] is None
    assert calendar.get('2026-09-21') is None
    assert dm.due_at(dm.BY_KEY['minute_flow'], DAY) == datetime(2026, 9, 19)
    with pytest.raises(ValueError):
        dm.date_range(date(2025, 1, 1), date(2026, 1, 2))


def test_ths_hot_rank_is_an_independent_monitored_dataset():
    spec = dm.BY_KEY['hot_ths']
    assert spec.name == '同花顺热门榜单'
    assert spec.table == 'st_hot_rank_ths'
    assert spec.date_column == 'snapshot_date'
    assert spec.task_type == 'hot_rank_ths'
    assert spec.deadline == '17:12'


class Queue:
    def __init__(self):
        self.jobs = []

    def submit(self, fn):
        self.jobs.append(fn)

    def run(self):
        while self.jobs:
            self.jobs.pop(0)()


class Observer:
    calendar_rows = {DAY: 1, '2026-09-19': 0}
    visits = []

    def calendar(self, start, end):
        return self.calendar_rows

    def tasks(self, start, end, current):
        return []

    def gaps(self, day, dataset):
        return []

    def inspect_day(self, day, current, *, datasets=None,
                    missing_limit=dm.MAX_DETAILS):
        self.visits.append(day)
        specs = [s for s in dm.DATASETS if datasets is None or s.key in datasets]
        limit = 250 if missing_limit is None else min(250, missing_limit)
        return {s.key: dict(dm.blank_cell(s, day, 'full', 'verified'), checked_at=dm.iso(current),
                            missing=[dict(stock_code=str(i), missing_count=1, reason='missing') for i in range(limit)],
                            missing_total=250) for s in specs}


def monitor():
    queue = Queue()
    clock = [CURRENT]
    result = dm.Monitor(Observer, clock=lambda: clock[0], executor=queue)
    return result, queue, clock


def test_unchecked_rest_and_unmapped_dates_remain_distinct():
    m, q, clock = monitor()
    result = m.overview(date(2026, 9, 18), date(2026, 9, 20))
    assert [c['status'] for c in result['datasets'][0]['days']] == ['unknown', 'closed', 'unknown']
    assert result['summary']['complete_days'] == 0
    q.run()
    result = m.overview(date(2026, 9, 18), date(2026, 9, 20))
    assert [c['status'] for c in result['datasets'][0]['days']] == ['full', 'closed', 'unknown']
    assert result['summary']['due_days'] == 1


def test_expiration_and_manual_recheck_remove_green_immediately():
    m, q, clock = monitor()
    m.enqueue(DAY)
    q.run()
    assert m.detail('daily', date.fromisoformat(DAY))['status'] == 'full'
    assert m.detail('daily', date.fromisoformat(DAY), recheck=True)['status'] == 'unknown'
    assert m.overview(date.fromisoformat(DAY), date.fromisoformat(DAY))['datasets'][0]['days'][0]['status'] == 'unknown'
    q.run()
    clock[0] += timedelta(seconds=dm.CACHE_SECONDS)
    assert m.detail('daily', date.fromisoformat(DAY))['stale'] is True
    with pytest.raises(ValueError):
        m.export('daily', date.fromisoformat(DAY))


def test_export_preserves_all_rows_with_one_targeted_database_scan():
    m, q, clock = monitor()
    m.enqueue(DAY)
    q.run()
    assert 'exports' not in m.cache[DAY]
    assert len(m.detail('daily', date.fromisoformat(DAY))['missing']) == 200
    before = len(Observer.visits)
    output = m.export('daily', date.fromisoformat(DAY))
    assert output.startswith('\ufeff')
    assert len(list(csv.reader(io.StringIO(output)))) == 251
    assert len(Observer.visits) == before + 1


def test_priority_recheck_overtakes_queued_history():
    m, q, clock = monitor()
    Observer.visits = []
    m.enqueue('2026-09-16')
    m.enqueue('2026-09-17')
    m.enqueue(DAY, priority=True)
    assert len(q.jobs) == 1
    q.run()
    assert Observer.visits == [DAY, '2026-09-16', '2026-09-17']


def test_expired_pending_partition_gets_rechecked_at_deadline():
    m, q, clock = monitor()
    clock[0] = datetime(2026, 9, 18, 15, 30)
    data = {s.key: dm.blank_cell(s, DAY, 'pending', 'not due') for s in dm.DATASETS}
    m.cache[DAY] = {'at': clock[0], 'data': data}
    clock[0] += timedelta(minutes=6)
    m.enqueue(DAY)
    assert DAY in m.pending


def test_failed_inspection_never_leaves_old_green():
    class Failed(Observer):
        def inspect_day(self, *args):
            raise RuntimeError('private connection detail')
    m, q, clock = monitor()
    m.observer_factory = Failed
    m.enqueue(DAY)
    q.run()
    result = m.detail('daily', date.fromisoformat(DAY))
    assert result['status'] == 'unknown'
    assert 'private connection' not in result['reason']


def test_shutdown_cancels_history_and_stops_at_read_boundary():
    from threading import Event
    started = Event()
    class Waiting(Observer):
        def inspect_day(self, day, current):
            started.set()
            assert self.stop_event.wait(timeout=3)
            return super().inspect_day(day, current)
    m = dm.Monitor(Waiting, clock=lambda: CURRENT)
    m.enqueue(DAY)
    assert started.wait(timeout=3)
    m.enqueue('2026-09-17')
    m.close()
    m.enqueue('2026-09-16')
    assert not m.pending
    assert not m.active
    assert '2026-09-17' not in m.cache


def test_task_running_requires_fresh_scheduler_heartbeat_and_target_date():
    observer = object.__new__(dm.Observer)
    row = dict(id=1, task_name='repair', task_type='qmt_stock_daily_canonical', status='running',
               run_at=CURRENT-timedelta(minutes=1), finished_at=None, heartbeat_at=CURRENT,
               output='{"trade_date":"2026-09-18"}')
    observer.read = lambda *args: [row]
    assert observer.tasks(DAY, DAY, CURRENT)[0]['status'] == 'running'
    assert observer.tasks(DAY, DAY, CURRENT)[0]['target_dates'] == [DAY]
    row['heartbeat_at'] = CURRENT-timedelta(minutes=4)
    assert observer.tasks(DAY, DAY, CURRENT)[0]['status'] == 'unknown'
    row['output'] = ''
    assert observer.tasks(DAY, DAY, CURRENT)[0]['target_dates'] == []


def test_api_rejects_unbounded_range_and_unknown_dataset(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from server.api.routers.datasource import router
    m, q, clock = monitor()
    monkeypatch.setattr(dm, 'get_monitor', lambda: m)
    app = FastAPI()
    app.include_router(router, prefix='/api')
    client = TestClient(app)
    assert client.get('/api/datasource/monitor?start_date=2020-01-01&end_date=2026-01-01').status_code == 422
    assert client.get('/api/datasource/monitor/detail?dataset=invalid&trade_date='+DAY).status_code == 422
    assert client.get('/api/datasource/monitor/gaps.csv?dataset=daily&trade_date='+DAY).status_code == 409
    q.run()
    response = client.get('/api/datasource/monitor?start_date='+DAY+'&end_date='+DAY)
    assert response.status_code == 200
    assert response.headers['cache-control'] == 'no-store'
    assert response.json()['datasets'][0]['days'][0]['status'] == 'full'

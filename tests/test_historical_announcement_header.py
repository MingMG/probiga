from time import monotonic

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from server.common.historical_announcement_read import _reconstruction_counts


def test_header_inventory_respects_query_budget_and_preserves_all_counts():
    engine = create_engine('sqlite://')
    with engine.begin() as c:
        c.execute(text('CREATE TABLE st_pit_source_coverage(fact_kind TEXT,source TEXT,batch_id TEXT,stock_code TEXT,result_count INT)'))
        c.execute(text('CREATE INDEX inventory ON st_pit_source_coverage(fact_kind,source,batch_id,stock_code)'))
        c.execute(text("INSERT INTO st_pit_source_coverage VALUES('event','cninfo.announcement',:batch,:code,:count)"),
                  [{'batch': batch, 'code': f'{i:06d}', 'count': i % 7}
                   for batch in ['selected', 'unrelated'] for i in range(5558)])
    with engine.connect() as c:
        driver = c.connection.driver_connection
        def budget(_c, _cursor, statement, _parameters, _context, _many):
            if statement.startswith('SELECT'):
                used = 0
                def progress():
                    nonlocal used
                    used += 1000
                    return int(used > 10000)
                driver.set_progress_handler(progress, 1000)
        event.listen(c, 'before_cursor_execute', budget)
        try:
            with pytest.raises(OperationalError, match='interrupted'):
                c.execute(text("SELECT result_count FROM st_pit_source_coverage WHERE fact_kind='event' AND source='cninfo.announcement' AND batch_id='selected' LIMIT 10001")).all()
            counts = _reconstruction_counts(c, source='cninfo.announcement', batch_id='selected', deadline=monotonic() + 30)
            assert counts == [i % 7 for i in range(5558)]
        finally:
            driver.set_progress_handler(None, 0)
            event.remove(c, 'before_cursor_execute', budget)
        with pytest.raises(ValueError, match='out of bounds'):
            _reconstruction_counts(c, source='cninfo.announcement', batch_id='missing', deadline=monotonic() + 30)


def test_header_inventory_does_not_silently_truncate_oversized_batch():
    engine = create_engine('sqlite://')
    with engine.begin() as c:
        c.execute(text('CREATE TABLE st_pit_source_coverage(fact_kind TEXT,source TEXT,batch_id TEXT,stock_code TEXT,result_count INT)'))
        c.execute(text("INSERT INTO st_pit_source_coverage VALUES('event','cninfo.announcement','oversized',:code,0)"),
                  [{'code': f'{i:06d}'} for i in range(10001)])
        with pytest.raises(ValueError, match='out of bounds'):
            _reconstruction_counts(c, source='cninfo.announcement', batch_id='oversized', deadline=monotonic() + 30)

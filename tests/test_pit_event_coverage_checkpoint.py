import sqlite3

from server.common.pit_event_coverage_checkpoint import EventCoverageCheckpoint


def test_progress_counts_only_committed_new_revisions(tmp_path):
    path = tmp_path / 'private' / 'coverage.sqlite'
    checkpoint = EventCoverageCheckpoint(path, 'namespace')
    checkpoint.save([('scope', 3, 'first')])
    assert checkpoint.committed_revisions == 3
    checkpoint.save([('scope', 3, 'first')])
    assert checkpoint.committed_revisions == 3
    resumed = EventCoverageCheckpoint(path, 'namespace')
    assert resumed.load(['scope']) == {'scope': (3, 'first')}
    resumed.save([('scope', 4, 'next')])
    assert resumed.committed_revisions == 1
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TRIGGER reject_update BEFORE UPDATE ON verified_event_prefix "
                           "BEGIN SELECT RAISE(ABORT, 'controlled disk failure'); END")
    resumed.save([('scope', 5, 'not committed')])
    assert not resumed.available and resumed.committed_revisions == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute('SELECT revision_count FROM verified_event_prefix').fetchone() == (4,)

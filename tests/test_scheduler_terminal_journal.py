from pathlib import Path
import os

import pytest

from server.common import scheduler_terminal_journal as journal


@pytest.fixture
def store(tmp_path,monkeypatch):
    monkeypatch.setattr(journal,"_FAILED_CLOSES",[])
    root=tmp_path/"jobs"
    root.mkdir(mode=0o700)
    return journal.TerminalJournal(root)


def test_exact_replay_preserves_original_inode_and_bytes(store):
    value={"model":"original","unicode":"原件"}
    raw=store.preserve("a"*32,"OBSERVED",value)
    path=store.root/("a"*32)/"OBSERVED"
    before=path.stat()
    assert store.preserve("a"*32,"OBSERVED",value)==raw==store.read("a"*32,"OBSERVED")
    assert path.stat().st_ino==before.st_ino
    with pytest.raises(journal.TerminalJournalError):
        store.preserve("a"*32,"OBSERVED",{"model":"replacement"})
    assert path.read_bytes()==raw


def test_partial_write_is_kept_and_never_replaced(store,monkeypatch):
    write=journal.os.write
    calls=[]
    def partial(fd,raw):
        if calls:
            raise OSError("MODEL write failed")
        calls.append(fd)
        return write(fd,raw[:3])
    monkeypatch.setattr(journal.os,"write",partial)
    with pytest.raises(OSError):
        store.preserve("a"*32,"OBSERVED",{"model":"original"})
    path=store.root/("a"*32)/"OBSERVED"
    assert path.read_bytes()==b'{"m'
    monkeypatch.setattr(journal.os,"write",write)
    with pytest.raises(journal.TerminalJournalError):
        store.preserve("a"*32,"OBSERVED",{"model":"original"})
    assert path.read_bytes()==b'{"m'


def test_full_write_fsync_failure_can_only_resync_same_original(store,monkeypatch):
    sync=journal.os.fsync
    def failed(fd):
        raise OSError("MODEL fsync")
    monkeypatch.setattr(journal.os,"fsync",failed)
    with pytest.raises(OSError):
        store.preserve("a"*32,"OBSERVED",{"model":"original"})
    path=store.root/("a"*32)/"OBSERVED"
    original=path.read_bytes()
    inode=path.stat().st_ino
    monkeypatch.setattr(journal.os,"fsync",sync)
    assert store.preserve("a"*32,"OBSERVED",{"model":"original"})==original
    assert path.stat().st_ino==inode


def test_failed_close_quarantines_original_and_blocks_new_allocation(store,monkeypatch):
    close=journal.os.close
    observed=[]
    def failed(fd):
        observed.append(fd)
        raise OSError("MODEL close failure")
    monkeypatch.setattr(journal.os,"close",failed)
    with pytest.raises(OSError):
        store.preserve("a"*32,"OBSERVED",{"model":"original"})
    assert journal._FAILED_CLOSES==observed and len(observed)==1
    monkeypatch.setattr(journal.os,"close",close)
    with pytest.raises(journal.TerminalJournalError):
        store.preserve("b"*32,"OBSERVED",{"model":"different"})
    with pytest.raises(journal.TerminalJournalError):
        journal.TerminalJournal(store.root.parent)
    close(observed[0])  # Test-only cleanup of the exact still-open synthetic fd.


@pytest.mark.parametrize("uid,name",[("../foreign","OBSERVED"),("A"*32,"OBSERVED"),
    ("a"*32,"UNKNOWN"),("a"*32,"PREPARED."+"A"*64)])
def test_closed_names(store,uid,name):
    with pytest.raises(journal.TerminalJournalError):
        store.preserve(uid,name,{"model":1})


def test_unknown_namespace_stops_inventory_without_deleting(store):
    store.preserve("a"*32,"OBSERVED",{"model":1})
    path=store.root/("a"*32)/"FOREIGN"
    path.write_bytes(b"original unknown")
    with pytest.raises(journal.TerminalJournalError):
        list(store.records("a"*32,""))
    assert path.read_bytes()==b"original unknown"


def test_hardlink_is_not_an_original(store,tmp_path):
    store.preserve("a"*32,"OBSERVED",{"model":1})
    path=store.root/("a"*32)/"OBSERVED"
    os.link(path,tmp_path/"alias")
    with pytest.raises(journal.TerminalJournalError):
        store.read("a"*32,"OBSERVED")


def test_oversize_refused_before_create(store,monkeypatch):
    monkeypatch.setattr(journal,"MAX_BYTES",16)
    with pytest.raises(journal.TerminalJournalError):
        store.preserve("a"*32,"OBSERVED",{"model":"x"*40})
    assert not (store.root/("a"*32)).exists()

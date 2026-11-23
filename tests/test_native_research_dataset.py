from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine

from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from server.engine import native_research_dataset as storage


@pytest.fixture
def dataset(tmp_path):
    engine=create_engine('sqlite://')
    privileged_migrate_formal_analysis_schema(engine)
    yield storage.PreparedDataset(tmp_path),FormalAnalysisStore(engine)
    engine.dispose()


def test_typed_handoff_and_warm_read_never_query_formal_storage(dataset,monkeypatch):
    local,store=dataset
    value={'date':date(2026,9,7),'time':datetime(2026,9,7,18),'price':Decimal('10.123456'),
        'members':('000001','000002'),'empty':[],'set':{'bank'}}
    reference=local.put(store,'a'*64,'native_prepared','identity',value)
    local.pin('b'*64,[reference])
    monkeypatch.setattr(store,'load_artifacts',lambda *_:pytest.fail('warm inputs were retransmitted'))
    assert local.prepare(store,[reference])==0
    assert local.get(reference)==value


def test_missing_and_corrupt_local_content_require_the_same_formal_hash(dataset):
    local,store=dataset
    reference=local.put(store,'a'*64,'native_prepared','identity',{'known_at':datetime(2026,9,7,18)})
    path=local.root/(reference['sha256']+'.json')
    for corruption in ('missing','tampered'):
        if corruption=='missing':path.unlink()
        else:path.write_bytes(b'wrong bytes')
        with pytest.raises(storage.PreparedInputMissing) as exc:local.get(reference)
        assert exc.value.content_id==reference['sha256']
        assert local.prepare(store,[reference])==1
        assert local.get(reference)=={'known_at':datetime(2026,9,7,18)}


def test_feature_collections_split_without_losing_types_or_single_field(dataset,monkeypatch):
    local,store=dataset
    monkeypatch.setattr(storage,'MAX_BLOCK_BYTES',1600)
    value={'stocks':[{'code':str(index).zfill(6),'cutoff':date(2026,9,7),'features':list(range(30))} for index in range(100)]}
    references={}
    tree=local.put_tree(store,'a'*64,'native_prepared','features',value,references=references)
    assert len(references)>2
    assert local.get_tree(tree)==value


def test_active_workset_is_never_evicted_to_hide_quota_failure(dataset,monkeypatch):
    local,store=dataset
    monkeypatch.setattr(storage,'MAX_BLOCK_BYTES',1024)
    local.quota_bytes=1200
    first=local.put(store,'a'*64,'native_prepared','one','x'*700)
    local.pin('b'*64,[first])
    with pytest.raises(RuntimeError,match='QUOTA'):
        local.put(store,'a'*64,'native_prepared','two','y'*700)
    assert local.get(first)=='x'*700


def test_cold_restore_obeys_formal_byte_budget_for_large_valid_blocks(dataset):
    local,store=dataset
    references=[local.put(store,'a'*64,'native_prepared',str(index),character*(11*1024*1024))
                for index,character in enumerate(('a','b','c'))]
    for reference in references:(local.root/(reference['sha256']+'.json')).unlink()
    assert local.prepare(store,references)==3
    assert all(len(local.get(reference))==11*1024*1024 for reference in references)


def test_restart_removes_only_terminal_or_orphan_pins(dataset):
    local,store=dataset
    reference=local.put(store,'a'*64,'native_prepared','one',{'value':1})
    local.pin('b'*64,[reference]);local.pin('c'*64,[reference])
    local.reconcile_pins(['c'*64])
    assert not (local.root/(('b'*64)+'.pin')).exists()
    assert (local.root/(('c'*64)+'.pin')).exists()

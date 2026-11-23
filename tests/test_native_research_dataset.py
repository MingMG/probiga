from datetime import date, datetime
from decimal import Decimal
import hashlib

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


def test_warm_preparation_does_not_decode_a_window_that_will_be_read_next(dataset,monkeypatch):
    local,store=dataset
    value=[{'stock_code':str(i).zfill(6),'day':date(2026,9,7),'close':Decimal('12.3456')}
           for i in range(1200)]
    references={}
    tree=local.put_tree(store,'a'*64,'native_prepared','bars',value,references=references)
    with monkeypatch.context() as guarded:
        guarded.setattr(storage.json,'loads',lambda *_args,**_kwargs:pytest.fail('warm prepare decoded the window'))
        assert local.prepare(store,list(references.values()))==0
    with monkeypatch.context() as guarded:
        guarded.setattr(storage,'_bytes',lambda *_:pytest.fail('warm read serialized the window again'))
        assert local.get_tree(tree)==value


@pytest.mark.parametrize('raw',[
    b'{',
    b'[]',
    b'{"schema":"other","value":1}',
    b'{"schema":"probiga.prepared-native-value.v1"}',
    b'{"schema":"probiga.prepared-native-value.v1","value":1,"value":2}',
    b'{"schema":"probiga.prepared-native-value.v1","value":NaN}',
    b'{"schema":"probiga.prepared-native-value.v1","value":Infinity}',
    b'{"schema":"probiga.prepared-native-value.v1","value":-Infinity}',
    b'{"schema":"probiga.prepared-native-value.v1","value":1e9999}',
    b'{"schema":"probiga.prepared-native-value.v1","value":["unknown",1]}',
    b'{"schema":"probiga.prepared-native-value.v1","value":["mapping",[["x",1],["x",2]]]}',
    b'{"schema":"probiga.prepared-native-value.v1","value":1,"extra":NaN}',
    b'{"schema":"probiga.prepared-native-value.v1","value":1,"extra":1e9999}',
])
def test_exact_hash_does_not_allow_invalid_json_or_typed_values(dataset,raw):
    local,_=dataset
    digest=hashlib.sha256(raw).hexdigest()
    (local.root/(digest+'.json')).write_bytes(raw)
    reference={'schema':storage.SCHEMA,'sha256':digest,'bytes':len(raw)}
    with pytest.raises(ValueError):local.get(reference)


def test_same_length_corruption_is_restored_by_exact_hash(dataset):
    local,store=dataset
    value={'price':Decimal('12.3456'),'day':date(2026,9,7)}
    reference=local.put(store,'a'*64,'native_prepared','bars',value)
    path=local.root/(reference['sha256']+'.json')
    raw=path.read_bytes();corrupt=raw.replace(b'12.3456',b'12.3457');assert len(corrupt)==len(raw) and corrupt!=raw
    path.write_bytes(corrupt)
    assert local.prepare(store,[reference])==1
    assert path.read_bytes()==raw and local.get(reference)==value

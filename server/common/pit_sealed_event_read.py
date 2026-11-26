"""Materialize the exact validated event scope with durable issuer checkpoints."""
from dataclasses import asdict
import json
from pathlib import Path
from time import monotonic

from server.common import pit_facts as pit
from server.common.formal_source_identity import frozen_source_sha256

ISSUER_BATCH_SIZE = 400


def event_fact_manifest(batch, selection):
    """Encode the established PIT event v1 identity for a materialized scope."""
    proof=selection.selection
    manifest={'schema':'probiga.pit-event-selection.v1','fact_cutoff_at':batch.fact_cutoff_at,
        'decision_at':batch.decision_at,'start_date':proof.window_start.isoformat(),
        'end_date':proof.window_end.isoformat(),
        'facts':{code:{'status':batch.status_by_code.get(code),'reason':batch.reason_by_code.get(code),
            'revision_ids':[row.get('event_revision_id') for row in batch.facts.get(code,[])],
            'coverage':batch.coverage_by_code.get(code)} for code in sorted(batch.status_by_code)},
        'qmt_event_batch':dict(proof.batch)}
    return pit.canonical_hash(manifest)


def load_sealed_event_facts(engine, *, selection, codes, formal_store, read_progress=None):
    requested = sorted({str(code).zfill(6) for code in codes})
    proof = selection.selection
    if (not 0<len(requested)<=10000 or proof.engine is not engine or not set(requested) <= proof.members.keys()
            or not proof.batch):
        raise ValueError('PIT_SEALED_EVENT_SCOPE_DIFFERS')
    decision = pit._dt_text(proof.decision_at)
    cutoff = pit._dt_text(selection.fact_cutoff_at)
    generation = pit.canonical_hash({
        'schema':'probiga.sealed-event-facts.v1', 'decision_at':decision, 'fact_cutoff_at':cutoff,
        'window_start':proof.window_start, 'window_end':proof.window_end, 'batch':dict(proof.batch),
        'members':{code:proof.members[code][0]['coverage_id'] for code in requested},
        'bindings':selection.signed_historical_bindings,
        'reader_sha256':frozen_source_sha256(Path(pit.__file__)),
        'materializer_sha256':frozen_source_sha256(Path(__file__)),
    })
    fields = ('facts','coverage_by_code','status_by_code','reason_by_code')
    merged = {field:{} for field in fields}
    deadline=monotonic()+600
    for offset in range(0,len(requested),ISSUER_BATCH_SIZE):
        if monotonic()>deadline:raise TimeoutError('PIT_SEALED_EVENT_MATERIALIZATION_DEADLINE')
        chunk = requested[offset:offset+ISSUER_BATCH_SIZE]
        key = pit.canonical_hash({'codes':chunk})
        retained = formal_store.get(generation,'event_issuer_facts',key)
        fresh = retained is None
        if fresh:
            part = pit.load_event_facts(engine,codes=chunk,decision_at=proof.decision_at,
                fact_cutoff_at=selection.fact_cutoff_at,start_date=proof.window_start,end_date=proof.window_end,
                require_qmt_complete_batch=True,historical_selection=pit.subset_historical_event_read(selection,chunk))
            if part.global_status != pit.PIT_AVAILABLE:
                raise RuntimeError('PIT_SEALED_EVENT_READ_FAILED:'+next(iter(part.reason_by_code.values()),part.global_status))
        else:
            part = pit.PITFactBatch(**json.loads(retained['ordered_json']))
        if (part.table_name != pit.EVENT_REVISION_TABLE or part.global_status != pit.PIT_AVAILABLE
                or part.decision_at != decision or part.fact_cutoff_at != cutoff
                or set(part.status_by_code) != set(chunk) or set(part.reason_by_code) != set(chunk)
                or not part.facts.keys() <= set(chunk) or not part.coverage_by_code.keys() <= set(chunk)
                or not pit._HASH_RE.fullmatch(part.manifest_hash)):
            raise ValueError('PIT_SEALED_EVENT_CHECKPOINT_DIFFERS')
        if fresh:
            formal_store.put(generation,'event_issuer_facts',key,
                {'ordered_json':json.dumps(pit._json_safe(asdict(part)),ensure_ascii=False,separators=(',',':'),allow_nan=False)})
        for field in fields:
            merged[field].update(getattr(part,field))
        if read_progress is not None:
            read_progress(offset+len(chunk),len(requested),generation,fresh)
    batch=pit.PITFactBatch(**merged,decision_at=decision,
        fact_cutoff_at=cutoff,table_name=pit.EVENT_REVISION_TABLE,global_status=pit.PIT_AVAILABLE)
    batch.manifest_hash=event_fact_manifest(batch,selection)
    return batch

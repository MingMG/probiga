"""Reuse verified, immutable historical source selections across worker steps."""
import hashlib
from pathlib import Path
from types import MappingProxyType

from server.common import pit_facts as pit
from server.common.formal_analysis_store import FormalAnalysisStore
from server.common.qmt_announcement_pit import HistoricalAnnouncementSelection, _assert_pit_fact_schema_prepared


def snapshot_identity(*, day, decision, identity_hash, finance_seal, codes):
    from server.common import historical_announcement_read, pit_sealed_finance_read
    return pit.canonical_hash({'schema':'probiga.native-fact-selection.v1', 'day':day,
        'decision':str(decision), 'identity':identity_hash, 'finance_seal':finance_seal,
        'codes':sorted(codes), 'readers':{Path(m.__file__).name:hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()
            for m in (pit,historical_announcement_read,pit_sealed_finance_read)},
        'codec':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})


def save_snapshot(engine, generation, inputs):
    finance, events = inputs.finance, inputs.events
    selected = events.selection
    if (finance.engine is not engine or selected.engine is not engine
            or inputs.cutoff.get('status')!=pit.PIT_AVAILABLE or not selected.members
            or not selected.members.keys()<=finance.members.keys()
            or finance.decision_at!=selected.decision_at or finance.fact_cutoff_at!=events.fact_cutoff_at
            or finance.as_of_date!=selected.window_end):
        raise ValueError('native historical selection scope differs')
    if type(selected) not in (HistoricalAnnouncementSelection, pit._LiveEventSelection) or events.signed_historical_bindings is not None:
        raise ValueError('unsupported native historical selection')
    codes = sorted(finance.members.keys() | selected.members.keys())
    chunks = {}
    for offset in range(0,len(codes),400):
        group=codes[offset:offset+400]
        chunks[str(offset)] = pit._json_safe({
            'finance':{code:finance.members[code] for code in group if code in finance.members},
            'events':{code:list(selected.members[code]) for code in group if code in selected.members}})
    store=FormalAnalysisStore(engine)
    chunks=store.put_many(generation,'native_fact_members',chunks)
    metadata={'cutoff':inputs.cutoff, 'chunks':chunks,
        'finance':{key:getattr(finance,key) for key in ('decision_at','fact_cutoff_at','as_of_date','seal_id','batch_root_sha256','exclusions')},
        'event':{key:getattr(selected,key) for key in ('decision_at','window_start','window_end','batch')},
        'event_kind':type(selected).__name__, 'event_cutoff':events.fact_cutoff_at,
        'finance_count':len(finance.members), 'event_count':len(selected.members)}
    store.put(generation,'native_fact_snapshot','complete',pit._json_safe(metadata))


def load_snapshot(engine, generation):
    store=FormalAnalysisStore(engine)
    header=store.get(generation,'native_fact_snapshot','complete')
    if header is None:return None
    _assert_pit_fact_schema_prepared(engine)
    if not 1<=len(header['chunks'])<=25:raise ValueError('native fact snapshot chunk count differs')
    artifacts=store.load_artifacts(list(header['chunks'].values()))
    finance,events={},{}
    for key in sorted(header['chunks'],key=int):
        part=artifacts[header['chunks'][key]]
        if (set(finance)&part['finance'].keys() or set(events)&part['events'].keys()
                or len(part['finance'].keys() | part['events'].keys())>400):
            raise ValueError('native fact snapshot membership differs')
        finance.update({code:MappingProxyType(row) for code,row in part['finance'].items()})
        events.update({code:(MappingProxyType(row),head) for code,(row,head) in part['events'].items()})
    if len(finance)!=header['finance_count'] or len(events)!=header['event_count']:
        raise ValueError('native fact snapshot coverage differs')
    data=dict(header['finance'])
    data.update(engine=engine,members=MappingProxyType(finance),
        decision_at=pit.normalize_decision_at(data['decision_at']),
        fact_cutoff_at=pit.normalize_decision_at(data['fact_cutoff_at']),
        as_of_date=pit._date_value(data['as_of_date'],required=True),
        exclusions=MappingProxyType({code:MappingProxyType(row) for code,row in data['exclusions'].items()}))
    sealed=pit.SealedFinanceInputs(**data)
    data=dict(header['event'])
    kind={'HistoricalAnnouncementSelection':HistoricalAnnouncementSelection,'_LiveEventSelection':pit._LiveEventSelection}[header['event_kind']]
    selected=kind(engine=engine,decision_at=pit.normalize_decision_at(data['decision_at']),
        window_start=pit._date_value(data['window_start'],required=True),
        window_end=pit._date_value(data['window_end'],required=True),
        batch=MappingProxyType(data['batch']),members=MappingProxyType(events))
    from server.trading_v3.daily_features import DailyFactInputs
    return DailyFactInputs(header['cutoff'],sealed,
        pit.HistoricalEventRead(selected,pit.normalize_decision_at(header['event_cutoff'])))

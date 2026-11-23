"""Explicit, verified local handoff of formal research inputs to computation.

Only PREPARE uses the formal store. Calculation reads exact content IDs locally;
it cannot fall back to the database or substitute a newer observation.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile

SCHEMA = 'probiga.prepared-native-value.v1'
MAX_BLOCK_BYTES = 15 * 1024 * 1024
DEFAULT_QUOTA_BYTES = 2 * 1024 * 1024 * 1024
_SHA = re.compile(r'[a-f0-9]{64}')


class PreparedInputMissing(RuntimeError):
    def __init__(self, content_id):
        self.content_id = content_id
        super().__init__('PREPARE_REQUIRED:' + content_id)


def encode(value):
    """A closed typed JSON format; dates must not silently become strings."""
    kind = type(value)
    if value is None or kind in (str, int, bool):
        return value
    if kind is float:
        if not math.isfinite(value):
            raise ValueError('prepared input contains a nonfinite number')
        return value
    if kind in (datetime, date, Decimal):
        return [kind.__name__, str(value)]
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ValueError('prepared input keys must be strings')
        return ['mapping', [[key, encode(value[key])] for key in sorted(value)]]
    if kind in (list, tuple, set, frozenset):
        items = sorted(value) if kind in (set, frozenset) else value
        return [kind.__name__, [encode(item) for item in items]]
    raise ValueError('unsupported prepared input type: ' + kind.__name__)


def decode(value):
    if type(value) is not list:
        if type(value) not in (str, int, float, bool, type(None)):
            raise ValueError('invalid prepared scalar')
        if type(value) is float and not math.isfinite(value):
            raise ValueError('invalid prepared number')
        return value
    if len(value) != 2:
        raise ValueError('invalid prepared type framing')
    kind, data = value
    if kind == 'mapping':
        if (type(data) is not list or any(type(pair) is not list or len(pair) != 2
                or type(pair[0]) is not str for pair in data)
                or [pair[0] for pair in data] != sorted({pair[0] for pair in data})):
            raise ValueError('invalid prepared mapping')
        return {key: decode(child) for key, child in data}
    if kind in ('list', 'tuple', 'set', 'frozenset') and type(data) is list:
        return {'list': list, 'tuple': tuple, 'set': set, 'frozenset': frozenset}[kind](decode(item) for item in data)
    if kind in ('date', 'datetime', 'Decimal') and type(data) is str:
        return {'date': date.fromisoformat, 'datetime': datetime.fromisoformat, 'Decimal': Decimal}[kind](data)
    raise ValueError('invalid prepared type')


def _bytes(payload):
    raw=bytearray()
    encoder=json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
    for part in encoder.iterencode(payload):
        value=part.encode('utf-8')
        if len(raw)+len(value)>MAX_BLOCK_BYTES:
            raise ValueError('prepared input block exceeds byte limit')
        raw.extend(value)
    if not raw:
        raise ValueError('prepared input block exceeds byte limit')
    return bytes(raw)


class PreparedDataset:
    def __init__(self, root=None, *, quota_bytes=DEFAULT_QUOTA_BYTES):
        configured = root or os.environ.get('PROBIGA_NATIVE_DATASET_ROOT')
        if not configured:
            raise RuntimeError('PREPARE_DIRECTORY_NOT_CONFIGURED')
        self.root = Path(configured).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.quota_bytes = int(quota_bytes)
        if self.quota_bytes < MAX_BLOCK_BYTES:
            raise ValueError('prepared dataset quota is too small')

    def _path(self, digest, suffix='.json'):
        if type(digest) is not str or _SHA.fullmatch(digest) is None:
            raise ValueError('invalid prepared content identity')
        return self.root / (digest + suffix)

    def _atomic(self, destination, raw):
        handle, temporary = tempfile.mkstemp(prefix='.prepare-', dir=self.root)
        try:
            with os.fdopen(handle, 'wb') as output:
                output.write(raw)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, destination)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def pin(self, experiment_id, references):
        identities = sorted({ref['sha256'] for ref in references})
        for digest in identities:
            self._path(digest)
        self._atomic(self._path(experiment_id, '.pin'), _bytes(identities))

    def release(self, experiment_id):
        self._path(experiment_id, '.pin').unlink(missing_ok=True)

    def reconcile_pins(self, active_experiments):
        active=set(active_experiments)
        for path in self.root.glob('*.pin'):
            if path.stem not in active:
                self.release(path.stem)

    def _space(self, size, incoming):
        pinned = {incoming}
        for path in self.root.glob('*.pin'):
            pinned.update(json.loads(path.read_text(encoding='utf-8')))
        blocks = sorted(self.root.glob('*.json'), key=lambda path: path.stat().st_mtime_ns)
        used = sum(path.stat().st_size for path in blocks)
        for path in blocks:
            if used + size <= self.quota_bytes:
                break
            if path.stem not in pinned:
                length = path.stat().st_size
                path.unlink()
                used -= length
        if used + size > self.quota_bytes:
            raise RuntimeError('PREPARE_DISK_QUOTA_EXCEEDED: active inputs remain pinned')

    def _install(self, reference, payload):
        raw = _bytes(payload)
        if len(raw) != reference['bytes'] or hashlib.sha256(raw).hexdigest() != reference['sha256']:
            raise ValueError('formal prepared content differs: ' + reference['sha256'])
        path = self._path(reference['sha256'])
        if not path.exists():
            self._space(len(raw), reference['sha256'])
        self._atomic(path, raw)

    def put(self, formal_store, generation, stage, key, value):
        payload = {'schema': SCHEMA, 'value': encode(value)}
        raw = _bytes(payload)
        digest = formal_store.put(generation, stage, key, payload)
        reference = {'sha256': digest, 'bytes': len(raw), 'schema': SCHEMA}
        self._install(reference, payload)
        return reference

    def get(self, reference):
        path = self._path(reference['sha256'])
        if (reference.get('schema') != SCHEMA or type(reference.get('bytes')) is not int
                or not 0 < reference['bytes'] <= MAX_BLOCK_BYTES):
            raise ValueError('prepared reference contract differs')
        try:
            if path.stat().st_size != reference['bytes']:
                raise PreparedInputMissing(reference['sha256'])
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != reference['sha256']:
                raise PreparedInputMissing(reference['sha256'])
        except FileNotFoundError:
            raise PreparedInputMissing(reference['sha256']) from None
        payload = json.loads(raw)
        if payload.get('schema') != SCHEMA or _bytes(payload) != raw:
            raise ValueError('prepared input schema or canonical encoding differs')
        return decode(payload['value'])

    def prepare(self, formal_store, references):
        """Restore exact blocks only; no selection by date, version or latest."""
        missing = []
        for reference in references:
            try:
                self.get(reference)
            except PreparedInputMissing:
                missing.append(reference)
        from server.common.formal_analysis_store import FORMAL_ANALYSIS_BATCH_MAX_BYTES
        def install(chunk):
            values = formal_store.load_artifacts([item['sha256'] for item in chunk])
            for reference in chunk:
                self._install(reference, values[reference['sha256']])
                self.get(reference)
        chunk=[];size=0
        for reference in missing:
            if chunk and (len(chunk)>=8 or size+reference['bytes']>FORMAL_ANALYSIS_BATCH_MAX_BYTES):
                install(chunk);chunk=[];size=0
            chunk.append(reference);size+=reference['bytes']
        if chunk:install(chunk)
        return len(missing)

    def put_tree(self, formal_store, generation, stage, key, value, *, references):
        """Bound memory and publish shards through the formal bulk API."""
        from server.common.formal_analysis_store import FORMAL_ANALYSIS_BATCH_MAX_BYTES
        pending={};nodes={};sizes={};pending_bytes=0

        def flush():
            nonlocal pending_bytes
            if not pending:return
            digests=formal_store.put_many(generation,stage,pending)
            for name,payload in pending.items():
                reference={'sha256':digests[name],'bytes':sizes[name],'schema':SCHEMA}
                self._install(reference,payload)
                references[reference['sha256']]=reference
                nodes[name]['reference']=reference
            pending.clear();nodes.clear();sizes.clear();pending_bytes=0

        def collect(name,item):
            nonlocal pending_bytes
            kind=type(item)
            if kind in (dict,list) and len(item)>400:
                keys=sorted(item) if kind is dict else None
                groups=({key:item[key] for key in keys[offset:offset+400]} if keys is not None else item[offset:offset+400]
                        for offset in range(0,len(item),400))
                return {'kind':'mapping' if keys is not None else 'list',
                    'children':[collect(name+':'+str(index),part) for index,part in enumerate(groups)]}
            if kind is dict and any(type(child) in (dict,list) and len(child)>400 for child in item.values()):
                return {'kind':'mapping','children':[{'kind':'field','name':field,
                    'child':collect(name+':'+str(index),item[field])} for index,field in enumerate(sorted(item))]}
            payload={'schema':SCHEMA,'value':encode(item)}
            try:size=len(_bytes(payload))
            except ValueError as exc:
                if str(exc)!='prepared input block exceeds byte limit' or kind not in (dict,list):raise
                if kind is dict and len(item)==1:
                    field,child=next(iter(item.items()))
                    return {'kind':'field','name':field,'child':collect(name+':field',child)}
                if len(item)<2:raise
                middle=len(item)//2
                if kind is dict:
                    keys=sorted(item)
                    parts=[{field:item[field] for field in group} for group in (keys[:middle],keys[middle:])]
                else:parts=[item[:middle],item[middle:]]
                return {'kind':'mapping' if kind is dict else 'list',
                    'children':[collect(name+':'+str(index),part) for index,part in enumerate(parts)]}
            if pending and (len(pending)>=256 or pending_bytes+size>FORMAL_ANALYSIS_BATCH_MAX_BYTES):flush()
            node={'kind':'value'}
            pending[name]=payload;nodes[name]=node;sizes[name]=size;pending_bytes+=size
            return node

        tree=collect(key,value)
        flush()
        return tree

    def get_tree(self, tree):
        if tree['kind']=='field':
            return {tree['name']:self.get_tree(tree['child'])}
        if tree['kind'] == 'value':
            return self.get(tree['reference'])
        if tree['kind'] == 'mapping':
            merged = {}
            for child in tree['children']:
                value = self.get_tree(child)
                if not isinstance(value, dict) or merged.keys() & value.keys():
                    raise ValueError('prepared mapping shards overlap or differ')
                merged.update(value)
            return merged
        if tree['kind'] == 'list':
            result = []
            for child in tree['children']:
                value = self.get_tree(child)
                if not isinstance(value, list):
                    raise ValueError('prepared list shard differs')
                result.extend(value)
            return result
        raise ValueError('prepared shard kind differs')

    def tree_references(self,tree):
        if tree['kind']=='value':
            reference=tree['reference']
            return {reference['sha256']:reference}
        children=[tree['child']] if tree['kind']=='field' else tree['children']
        references={}
        for child in children:references.update(self.tree_references(child))
        return references

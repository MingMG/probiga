"""Batch replay of an immutable checkpoint generation for one worker step."""
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from itertools import islice
from time import monotonic


_reads = ContextVar('formal_checkpoint_reads', default=None)


@contextmanager
def prefetch_checkpoint_reads(store, engine, generation, stage):
    # A market scope has fewer than 512 ten-issuer checkpoints. Two batches
    # keep the existing 32 MiB per-batch payload limit and bound retained data.
    from server.common.formal_analysis_store import FORMAL_ANALYSIS_BATCH_MAX_MEMBERS

    deadline = monotonic() + 45
    keys = list(islice(store.list_members(generation, stage), 2 * FORMAL_ANALYSIS_BATCH_MAX_MEMBERS))
    values = {}
    for offset in range(0, len(keys), FORMAL_ANALYSIS_BATCH_MAX_MEMBERS):
        if monotonic() >= deadline:
            raise TimeoutError('FORMAL_CHECKPOINT_PREFETCH_DEADLINE')
        values.update(store.get_many(generation, stage, keys[offset:offset + FORMAL_ANALYSIS_BATCH_MAX_MEMBERS]))
    if monotonic() >= deadline:
        raise TimeoutError('FORMAL_CHECKPOINT_PREFETCH_DEADLINE')
    token = _reads.set((engine, generation, stage, values))
    try:
        yield
    finally:
        _reads.reset(token)


def read_prefetched_checkpoint(bind, generation, stage, key):
    scope = _reads.get()
    if scope is not None and bind is scope[0] and (generation, stage) == scope[1:3]:
        return deepcopy(scope[3].get(key))
    return None

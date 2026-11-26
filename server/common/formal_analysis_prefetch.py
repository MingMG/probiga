"""Batch replay of an immutable checkpoint generation for one worker step."""
from contextlib import contextmanager
from contextvars import ContextVar
from itertools import islice
import logging
from time import monotonic

from sqlalchemy.exc import DBAPIError


_reads = ContextVar('formal_checkpoint_reads', default=None)
logger = logging.getLogger(__name__)


class CheckpointPrefetchDeferred(RuntimeError):
    """A bounded refill advanced the durable cache and needs another slice."""


@contextmanager
def prefetch_checkpoint_reads(store, engine, generation, stage, *, dataset, deadline, progress):
    from server.common.batch_db import _transient_db_errno
    from server.common.analysis_pool_receipt import canonical_sha256
    from server.common.formal_analysis_store import _LOAD_MAX_ARTIFACTS

    members = dict(islice(store.list_members(generation, stage).items(), 512))
    digests = list(dict.fromkeys(members.values()))
    scope = canonical_sha256([generation, digests])
    missing = [digest for digest in digests if dataset.read_formal_artifact(digest) is None]
    initial = len(digests) - len(missing)
    finished = initial
    for offset in range(0, len(missing), _LOAD_MAX_ARTIFACTS):
        if monotonic() >= deadline:
            if finished > initial:
                raise CheckpointPrefetchDeferred('财务断点缓存已保存，继续读取剩余分片')
            raise TimeoutError('FORMAL_CHECKPOINT_PREFETCH_DEADLINE')
        batch = missing[offset:offset + _LOAD_MAX_ARTIFACTS]
        started = monotonic()
        try:
            fetched = store.load_artifacts(batch)
        except DBAPIError as exc:
            logger.warning('Checkpoint body read failed: stage=%s cached=%d/%d errno=%s',
                           stage, finished, len(digests), _transient_db_errno(exc))
            if finished > initial and _transient_db_errno(exc) in (2003, 2006, 2013, 3024):
                raise CheckpointPrefetchDeferred('财务断点缓存已保存，数据库读取中断') from exc
            raise
        elapsed = monotonic() - started
        if elapsed > 1:
            logger.warning('Slow checkpoint body read: stage=%s offset=%d artifacts=%d seconds=%.2f',
                           stage, offset, len(batch), elapsed)
        dataset.retain_formal_artifacts(fetched)
        finished += len(fetched)
        progress(finished, len(digests), scope, True)
    token = _reads.set((engine, generation, stage, members, dataset))
    try:
        yield
    finally:
        _reads.reset(token)


def read_prefetched_checkpoint(bind, generation, stage, key):
    scope = _reads.get()
    if scope is not None and bind is scope[0] and (generation, stage) == scope[1:3]:
        digest = scope[3].get(key)
        if digest is not None:
            return scope[4].read_formal_artifact(digest)
    return None

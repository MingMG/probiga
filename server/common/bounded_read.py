"""Read exact ID batches within a shared deadline without dropping split IDs."""

import logging
from time import monotonic


timing = logging.getLogger("server.native_replay.timing")


def read_id_batches(ids, fetch, *, deadline, retryable, logger, stage, size=160, splits=2):
    if size < 1 or splits < 0:
        raise ValueError("invalid bounded read configuration")

    def read(batch, remaining_splits):
        started = monotonic()
        if started >= deadline:
            logger.warning("%s batch=%d elapsed=0s outcome=deadline failed_ids=%s",
                           stage, len(batch), list(batch))
            raise TimeoutError(f"{stage}: total read deadline")
        try:
            rows = fetch(batch)
        except Exception as exc:
            can_split = remaining_splits > 0 and len(batch) > 1 and retryable(exc)
            if not can_split:
                logger.warning("%s batch=%d elapsed=%.3fs outcome=failed failed_ids=%s",
                               stage, len(batch), monotonic() - started, list(batch))
                raise
            logger.warning("%s batch=%d elapsed=%.3fs outcome=split", stage, len(batch),
                           monotonic() - started)
            middle = len(batch) // 2
            return read(batch[:middle], remaining_splits - 1) + read(batch[middle:], remaining_splits - 1)
        elapsed = monotonic() - started
        if monotonic() >= deadline:
            logger.warning("%s batch=%d elapsed=%.3fs outcome=deadline failed_ids=%s",
                           stage, len(batch), elapsed, list(batch))
            raise TimeoutError(f"{stage}: total read deadline")
        timing.info("%s batch=%d elapsed=%.3fs rows=%d slow=%s", stage, len(batch),
                    elapsed, len(rows), elapsed > 1)
        return rows

    result = []
    for offset in range(0, len(ids), size):
        result.extend(read(ids[offset:offset + size], splits))
    return result


def read_keyset_pages(fetch, *, key, deadline, retryable, logger, stage, max_rows, size=160):
    """Read an ordered source without offsets or an unbounded ID directory."""
    if size < 1 or max_rows < 1:
        raise ValueError('invalid bounded page configuration')
    cursor, result, seen = None, [], set()
    while True:
        for attempt in range(3):
            started = monotonic()
            if started >= deadline:
                raise TimeoutError(stage + ': total read deadline')
            try:
                page = fetch(cursor, size)
                break
            except Exception as exc:
                from server.common.batch_db import _transient_db_errno
                logger.warning('%s batch=%d elapsed=%.3fs error_type=%s errno=%s outcome=failed',
                               stage, size, monotonic()-started, type(exc).__name__, _transient_db_errno(exc))
                if attempt == 2 or not retryable(exc):
                    raise
                size = max(1, size // 2)
        timing.info('%s batch=%d elapsed=%.3fs rows=%d completed=%d total_limit=%d',
                    stage, size, monotonic()-started, len(page), len(result)+len(page), max_rows)
        if monotonic() >= deadline:
            raise TimeoutError(stage + ': total read deadline')
        keys = [key(row) for row in page]
        if len(page) > size or len(set(keys)) != len(keys) or seen.intersection(keys):
            raise ValueError(stage + ': repeated or oversized source page')
        seen.update(keys)
        result.extend(page)
        if len(result) > max_rows:
            raise ValueError(stage + ': source coverage exceeds declared row count')
        if len(page) < size:
            return result
        cursor = keys[-1]

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

"""Read exact ID batches within a shared deadline without dropping split IDs."""

from time import monotonic


def read_id_batches(ids, fetch, *, size, deadline, retryable, logger, stage, splits=2):
    if size < 1 or splits < 0:
        raise ValueError("invalid bounded read configuration")

    def read(batch, remaining_splits):
        started = monotonic()
        if started >= deadline:
            raise TimeoutError(f"{stage}: total read deadline")
        try:
            rows = fetch(batch)
        except Exception as exc:
            can_split = remaining_splits > 0 and len(batch) > 1 and retryable(exc)
            logger.warning("%s batch=%d elapsed=%.3fs outcome=%s", stage, len(batch),
                           monotonic() - started, "split" if can_split else "failed")
            if not can_split:
                raise
            middle = len(batch) // 2
            return read(batch[:middle], remaining_splits - 1) + read(batch[middle:], remaining_splits - 1)
        logger.info("%s batch=%d elapsed=%.3fs rows=%d", stage, len(batch),
                    monotonic() - started, len(rows))
        return rows

    result = []
    for offset in range(0, len(ids), size):
        result.extend(read(ids[offset:offset + size], splits))
    return result

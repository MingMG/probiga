"""Read journal metadata before fetching a single immutable audit body."""
import time

from sqlalchemy import text


def _budget(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('STRATEGY_AUDIT_HEAD_READ_DEADLINE')
    return max(1, min(4000, int(remaining * 1000)))


def latest_head(connection, entity, key, *, locking=False, latest_at=None, deadline=None):
    deadline = time.monotonic() + 8 if deadline is None else deadline
    suffix = ' FOR UPDATE' if locking and connection.dialect.name == 'mysql' else ''
    parameters = {'entity': entity, 'key': key}
    if latest_at is None:
        latest_at = connection.execute(text(f'''SELECT /*+ MAX_EXECUTION_TIME({_budget(deadline)}) */ created_at
            FROM st_strategy_governance_audit WHERE entity_type=:entity AND entity_key=:key
            ORDER BY created_at DESC LIMIT 1''' + suffix), parameters).scalar()
    if latest_at is None:
        return None
    row = connection.execute(text(f'''SELECT /*+ MAX_EXECUTION_TIME({_budget(deadline)}) */ audit_id,action
        FROM st_strategy_governance_audit WHERE entity_type=:entity AND entity_key=:key AND created_at=:at
        ORDER BY action DESC,audit_id DESC LIMIT 1''' + suffix), {**parameters, 'at': latest_at}).mappings().first()
    return {'entity_key': key, 'latest_at': latest_at, **row} if row else None


def recent_heads(connection, entity, limit, *, deadline=None):
    deadline = time.monotonic() + 8 if deadline is None else deadline
    keys = []
    cursor = ''
    while True:
        key = connection.execute(text(f'''SELECT /*+ MAX_EXECUTION_TIME({_budget(deadline)}) */ entity_key
            FROM st_strategy_governance_audit WHERE entity_type=:entity AND entity_key>:cursor
            ORDER BY entity_key LIMIT 1'''), {'entity': entity, 'cursor': cursor}).scalar()
        if key is None:
            break
        keys.append(key)
        cursor = key
    def batch(rows, sequence=False):
        # Eight constant seeks avoid the measured optimizer blow-up of wider UNIONs.
        parameters = {'entity': entity}
        queries = []
        for index, row in enumerate(rows):
            key = row['entity_key'] if sequence else row
            parameters['key_' + str(index)] = key
            fields = ',action,audit_id' if sequence else ''
            scope = f' AND created_at=:at_{index}' if sequence else ''
            order = 'action DESC,audit_id DESC' if sequence else 'created_at DESC'
            if sequence:
                parameters['at_' + str(index)] = row['latest_at']
            hint = f'/*+ MAX_EXECUTION_TIME({_budget(deadline)}) */ ' if index == 0 else ''
            query = f'''SELECT {hint}:key_{index} AS entity_key,created_at AS latest_at{fields}
                FROM st_strategy_governance_audit WHERE entity_type=:entity AND entity_key=:key_{index}{scope}
                ORDER BY {order} LIMIT 1'''
            queries.append('(' + query + ')' if connection.dialect.name == 'mysql' else 'SELECT * FROM (' + query + ')')
        return [dict(row) for row in connection.execute(text(' UNION ALL '.join(queries)), parameters).mappings()]
    times = [row for offset in range(0, len(keys), 8) for row in batch(keys[offset:offset + 8])]
    if len(times) != len(keys):
        raise ValueError('STRATEGY_AUDIT_HEAD_COVERAGE_DIFFERS')
    chosen = sorted(times, key=lambda row: (row['latest_at'], row['entity_key']), reverse=True)[:limit]
    result = [row for offset in range(0, len(chosen), 8) for row in batch(chosen[offset:offset + 8], sequence=True)]
    if len(result) != len(chosen):
        raise ValueError('STRATEGY_AUDIT_HEAD_COVERAGE_DIFFERS')
    return sorted(result, key=lambda row: (row['latest_at'], row['entity_key']), reverse=True)

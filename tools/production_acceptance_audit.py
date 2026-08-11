# -*- coding: utf-8 -*-
"""Read-only production acceptance evidence collector.

The script uploads a short-lived Python probe to the production host, runs it
with the production virtual environment and PYTHONPATH, removes the probe, and
prints machine-readable JSON.  It never updates business tables.
"""
from __future__ import annotations

import argparse
import json
import posixpath
import shlex
import uuid
from pathlib import Path
from typing import Any

import paramiko

from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_pythonpath,
    remote_root,
)


INVENTORY_PROBE = r'''
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import bindparam, text

from server.common.batch_db import create_batch_engine
from server.common.minute_data import get_minute_engine
from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS


def stringify(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


engine = create_batch_engine()
with engine.connect() as conn:
    columns = [
        row[0]
        for row in conn.execute(
            text(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='st_scheduled_tasks' "
                "ORDER BY ORDINAL_POSITION"
            )
        ).fetchall()
    ]
    wanted = [
        name for name in (
            'id', 'task_name', 'task_type', 'group_name', 'script_path',
            'script_args', 'cron_time', 'interval_minutes', 'enabled',
            'last_run_at', 'last_triggered_at', 'last_run_status',
            'last_run_duration', 'updated_at', 'description'
        ) if name in columns
    ]
    rows = conn.execute(
        text(
            "SELECT " + ", ".join('`' + name + '`' for name in wanted) +
            " FROM st_scheduled_tasks ORDER BY enabled DESC, sort_order, id"
        )
    ).mappings().all()
    task_rows = [
        {key: stringify(value) for key, value in dict(row).items()}
        for row in rows
    ]

requirements = []
for task_type, items in sorted(TASK_OUTPUT_REQUIREMENTS.items()):
    for item in items:
        requirements.append({
            'task_type': task_type,
            'table': item.table,
            'min_rows': item.min_rows,
            'date_col': item.date_col,
            'target': item.target,
            'ready_time': item.ready_time,
            'distinct_col': item.distinct_col,
            'min_distinct': item.min_distinct,
            'where_sql': item.where_sql,
            'freshness_col': item.freshness_col,
            'require_fresh': item.require_fresh,
        })

print(json.dumps({
    'collected_at': datetime.now().isoformat(timespec='seconds'),
    'scheduler_columns': columns,
    'tasks': task_rows,
    'requirements': requirements,
}, ensure_ascii=False, default=str))
'''


SCHEMA_PROBE = r'''
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import bindparam, text

from server.common.batch_db import create_batch_engine
from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS


extra_tables = {
    'si_notice_eastmoney', 'st_news_flash', 'sm_rt_quote_snapshot',
    'stock_analysis_result', 'st_recommended_stocks',
    'sm_sim_trade_order', 'sm_sim_trade_position', 'sm_sim_trade_account',
    'st_scheduler_runtime', 'sys_data_coverage', 'sys_data_gap',
}
tables = extra_tables | {
    requirement.table
    for requirements in TASK_OUTPUT_REQUIREMENTS.values()
    for requirement in requirements
}
engine = create_batch_engine()
with engine.connect() as conn:
    rows = conn.execute(
        text(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, "
            "COLUMN_KEY, ORDINAL_POSITION FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN :tables "
            "ORDER BY TABLE_NAME, ORDINAL_POSITION"
        ).bindparams(bindparam('tables', expanding=True)),
        {'tables': sorted(tables)},
    ).mappings().all()
    indexes = conn.execute(
        text(
            "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
            "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME IN :tables ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
        ).bindparams(bindparam('tables', expanding=True)),
        {'tables': sorted(tables)},
    ).mappings().all()

schemas = {}
for row in rows:
    item = {key: (str(value) if value is not None else None) for key, value in dict(row).items()}
    schemas.setdefault(item.pop('TABLE_NAME'), []).append(item)
index_map = {}
for row in indexes:
    item = {key: (str(value) if value is not None else None) for key, value in dict(row).items()}
    index_map.setdefault(item.pop('TABLE_NAME'), []).append(item)

print(json.dumps({
    'collected_at': datetime.now().isoformat(timespec='seconds'),
    'schemas': schemas,
    'indexes': index_map,
    'missing_tables': sorted(tables - set(schemas)),
}, ensure_ascii=False))
'''


RUNTIME_PROBE = r'''
from __future__ import annotations

import json
import subprocess
from datetime import datetime

from sqlalchemy import text

from server.common.batch_db import create_batch_engine
from server.common.minute_data import get_minute_engine


def run(command):
    completed = subprocess.run(command, text=True, capture_output=True, check=False, timeout=20)
    return {
        'returncode': completed.returncode,
        'stdout': completed.stdout.strip(),
        'stderr': completed.stderr.strip(),
    }


engine = create_batch_engine()
with engine.connect() as conn:
    active_tasks = [dict(row) for row in conn.execute(text(
        "SELECT id, task_name, task_type, enabled, last_run_at, last_triggered_at, "
        "last_run_status, last_run_duration, RIGHT(COALESCE(last_run_output,''), 5000) AS output_tail "
        "FROM st_scheduled_tasks WHERE enabled=1 AND "
        "(last_run_status IN ('running','failed','timeout','stopped') OR id IN (36,44)) "
        "ORDER BY id"
    )).mappings().all()]
    heartbeat = conn.execute(text(
        "SELECT instance_id, mode, host_name, pid, started_at, heartbeat_at, "
        "TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) AS heartbeat_age_seconds, "
        "poll_seconds, max_concurrent_tasks FROM st_scheduler_runtime "
        "ORDER BY heartbeat_at DESC LIMIT 1"
    )).mappings().first()
    processlist = [dict(row) for row in conn.execute(text(
        "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 500) AS INFO "
        "FROM information_schema.PROCESSLIST WHERE COMMAND <> 'Sleep' ORDER BY TIME DESC"
    )).mappings().all()]

minute_engine = get_minute_engine()
with minute_engine.connect() as conn:
    kline_processlist = [dict(row) for row in conn.execute(text(
        "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 1000) AS INFO "
        "FROM information_schema.PROCESSLIST WHERE COMMAND <> 'Sleep' ORDER BY TIME DESC"
    )).mappings().all()]

payload = {
    'collected_at': datetime.now().isoformat(timespec='seconds'),
    'active_tasks': active_tasks,
    'heartbeat': dict(heartbeat or {}),
    'mysql_processlist': processlist,
    'minute_mysql_processlist': kline_processlist,
    'services': run(['systemctl', 'is-active', 'probiga', 'probiga-scheduler', 'mysql']),
    'scheduler_limits': run([
        'systemctl', 'show', 'probiga-scheduler',
        '--property=MemoryCurrent,MemoryHigh,MemoryMax,MemorySwapMax,CPUQuotaPerSecUSec,TasksCurrent,TasksMax'
    ]),
    'memory': run(['free', '-m']),
    'uptime': run(['uptime']),
    'processes': run(['ps', '-eo', 'pid,ppid,etimes,pcpu,pmem,rss,stat,cmd', '--sort=-rss']),
    'scheduler_journal': run([
        'journalctl', '-u', 'probiga-scheduler', '--since', '2026-07-18 18:40:00',
        '--no-pager', '-n', '300'
    ]),
}
print(json.dumps(payload, ensure_ascii=False, default=str))
'''


ROUTING_PROBE = r'''
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text

from server.common.batch_db import create_batch_engine
from server.common.kline_data import get_kline_engine
from server.common.minute_data import get_minute_engine


def inspect(label, engine):
    with engine.connect() as conn:
        identity = conn.execute(text(
            "SELECT DATABASE() AS database_name, @@hostname AS server_name, @@port AS server_port"
        )).mappings().first()
        tables = {}
        for table, date_column, code_column in (
            ('sm_stock_kline', 'trade_date', 'stock_code'),
            ('sm_stock_minute', 'trade_date', 'stock_code'),
            ('sm_index_kline', 'trade_date', 'index_code'),
            ('sm_index_minute', 'trade_date', 'index_code'),
        ):
            exists = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table"
            ), {'table': table}).scalar()
            if not exists:
                tables[table] = {'exists': False}
                continue
            row = conn.execute(text(
                f"SELECT latest.latest_date, COUNT(*) AS latest_rows, "
                f"COUNT(DISTINCT data.`{code_column}`) AS latest_codes FROM `{table}` data "
                f"JOIN (SELECT MAX(`{date_column}`) AS latest_date FROM `{table}`) latest "
                f"ON data.`{date_column}`=latest.latest_date GROUP BY latest.latest_date"
            )).mappings().first()
            tables[table] = {'exists': True, **dict(row or {})}
    return {'label': label, 'identity': dict(identity or {}), 'tables': tables}


engines = [
    ('main', create_batch_engine()),
    ('kline', get_kline_engine()),
    ('minute', get_minute_engine()),
]
print(json.dumps({
    'collected_at': datetime.now().isoformat(timespec='seconds'),
    'routes': [inspect(label, engine) for label, engine in engines],
}, ensure_ascii=False, default=str))
'''


HISTORY_SCHEMA_PROBE = r'''
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import bindparam, text

from server.common.kline_data import get_kline_engine


tables = ('sm_stock_kline', 'sm_stock_minute', 'sm_index_kline', 'sm_index_minute')
engine = get_kline_engine()
with engine.connect() as conn:
    columns = conn.execute(text(
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, ORDINAL_POSITION "
        "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN :tables "
        "ORDER BY TABLE_NAME, ORDINAL_POSITION"
    ).bindparams(bindparam('tables', expanding=True)), {'tables': tables}).mappings().all()
    indexes = conn.execute(text(
        "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
        "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN :tables "
        "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
    ).bindparams(bindparam('tables', expanding=True)), {'tables': tables}).mappings().all()
print(json.dumps({
    'collected_at': datetime.now().isoformat(timespec='seconds'),
    'columns': [dict(row) for row in columns],
    'indexes': [dict(row) for row in indexes],
}, ensure_ascii=False, default=str))
'''


QMT_ACCEPTANCE_PROBE = r'''
from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime

from sqlalchemy import text

from server.common.batch_db import create_batch_engine
from server.common.kline_data import get_kline_engine


def plain(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def one(engine, name, sql, params=None):
    started = time.monotonic()
    with engine.connect() as conn:
        row = conn.execute(text(sql), params or {}).mappings().first()
    return {
        'name': name,
        'seconds': round(time.monotonic() - started, 3),
        'metrics': {key: plain(value) for key, value in dict(row or {}).items()},
    }


main = create_batch_engine()
history = get_kline_engine()
checks = []

checks.append(one(main, 'stock_pool', """
    SELECT COUNT(*) AS rows, COUNT(DISTINCT stock_code) AS codes,
           SUM(stock_code LIKE '910%') AS bad_910,
           SUM(stock_code LIKE '810%') AS bad_810_convertible_bonds,
           SUM(stock_code LIKE '899%') AS bad_899_indexes,
           SUM(stock_code NOT REGEXP '^[0-9]{6}$') AS invalid_codes,
           SUM(short_name IS NULL OR TRIM(short_name)='') AS blank_names
    FROM si_all_code
"""))
checks.append(one(main, 'stock_current', """
    SELECT COUNT(*) AS rows, COUNT(DISTINCT stock_code) AS codes,
           MIN(snapshot_at) AS min_snapshot_at, MAX(snapshot_at) AS max_snapshot_at,
           SUM(price IS NULL OR price <= 0) AS bad_price,
           SUM(volume < 0 OR amount < 0) AS bad_volume_amount
    FROM sm_stock_current
"""))
checks.append(one(main, 'index_pool', """
    SELECT COUNT(*) AS rows, COUNT(DISTINCT index_code) AS codes,
           SUM(name IS NULL OR TRIM(name)='') AS blank_names
    FROM si_all_index_code
"""))
checks.append(one(main, 'index_constituents', """
    SELECT COUNT(*) AS rows, COUNT(DISTINCT c.index_code) AS indexes,
           COUNT(DISTINCT c.stock_code) AS stocks,
           SUM(c.stock_code NOT REGEXP '^[0-9]{6}$') AS invalid_codes,
           SUM(p.stock_code IS NULL) AS outside_stock_pool
    FROM si_index_constituent c LEFT JOIN si_all_code p ON p.stock_code=c.stock_code
"""))
checks.append(one(main, 'concept_reference', """
    SELECT (SELECT COUNT(*) FROM si_concept_code_east) AS concepts,
           (SELECT COUNT(*) FROM si_concept_constituent_east) AS memberships,
           (SELECT COUNT(DISTINCT concept_code) FROM si_concept_constituent_east) AS concepts_with_members,
           (SELECT SUM(c.stock_code LIKE '910%' OR p.stock_code IS NULL)
              FROM si_concept_constituent_east c
              LEFT JOIN si_all_code p ON p.stock_code=c.stock_code) AS invalid_members
"""))
checks.append(one(main, 'index_current', """
    SELECT COUNT(*) AS rows, COUNT(DISTINCT index_code) AS codes,
           MIN(trade_date) AS min_trade_date, MAX(trade_date) AS max_trade_date,
           SUM(price IS NULL OR price <= 0) AS bad_price,
           SUM(index_code NOT IN (SELECT index_code FROM si_all_index_code)) AS outside_pool
    FROM sm_index_current
"""))

checks.append(one(history, 'stock_daily_latest', """
    SELECT MAX(trade_date) AS trade_date, COUNT(*) AS rows,
           COUNT(DISTINCT stock_code) AS codes,
           SUM(data_source='gj_qmt') AS qmt_rows,
           SUM(stock_code LIKE '810%' OR stock_code LIKE '899%') AS non_equity_codes,
           SUM(open <= 0 OR close <= 0 OR high < GREATEST(open, close) OR low > LEAST(open, close)) AS bad_ohlc,
           SUM(volume < 0 OR amount < 0) AS bad_volume_amount,
           SUM(pre_close IS NULL OR pre_close <= 0 OR `change` IS NULL OR change_pct IS NULL) AS missing_change_fields,
           COUNT(*) - COUNT(DISTINCT stock_code, trade_date, k_type, adjust_type) AS duplicate_rows,
           MIN(etl_sync_at) AS first_sync, MAX(etl_sync_at) AS last_sync
    FROM sm_stock_kline
    WHERE k_type=1 AND adjust_type=0
      AND trade_date=(SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type=1 AND adjust_type=0)
"""))
checks.append(one(history, 'stock_minute_latest', """
    SELECT MAX(trade_date) AS trade_date, COUNT(*) AS rows,
           COUNT(DISTINCT stock_code) AS codes,
           COUNT(DISTINCT stock_code, trade_time) AS unique_keys,
           SUM(stock_code LIKE '810%' OR stock_code LIKE '899%') AS non_equity_rows,
           MIN(TIME(trade_time)) AS first_bar, MAX(TIME(trade_time)) AS last_bar,
           SUM(TIME(trade_time)='09:30:00') AS bars_0930,
           SUM(price IS NULL OR price <= 0 OR volume < 0 OR amount < 0) AS bad_rows,
           MIN(etl_sync_at) AS first_sync, MAX(etl_sync_at) AS last_sync
    FROM sm_stock_minute
    WHERE trade_date=(SELECT MAX(trade_date) FROM sm_stock_minute)
"""))
checks.append(one(history, 'stock_minute_bars_per_code', """
    SELECT MIN(bar_count) AS min_bars, MAX(bar_count) AS max_bars,
           SUM(bar_count <> 240) AS codes_not_240, COUNT(*) AS codes
    FROM (
      SELECT stock_code, COUNT(*) AS bar_count FROM sm_stock_minute
      WHERE trade_date=(SELECT MAX(trade_date) FROM sm_stock_minute)
      GROUP BY stock_code
    ) bars
"""))
checks.append(one(history, 'stock_daily_minute_reconciliation', """
    SELECT COUNT(*) AS compared,
           SUM(ABS(m.minute_volume-d.volume) > 0.5) AS volume_mismatches,
           SUM(ABS(last_bar.price-d.close) > 0.000001) AS close_mismatches,
           SUM(ABS(m.minute_amount-d.amount) > 0.05) AS amount_abs_mismatches,
           SUM(ABS(m.minute_amount-d.amount) > GREATEST(0.05, ABS(d.amount)*0.00001)) AS amount_relative_mismatches,
           SUM(ABS(m.minute_volume-d.volume) > GREATEST(100, ABS(d.volume)*0.001)) AS volume_over_0_1pct,
           SUM(ABS(m.minute_volume-d.volume) > GREATEST(100, ABS(d.volume)*0.01)) AS volume_over_1pct,
           SUM(ABS(m.minute_amount-d.amount) > GREATEST(100, ABS(d.amount)*0.001)) AS amount_over_0_1pct,
           SUM(ABS(m.minute_amount-d.amount) > GREATEST(100, ABS(d.amount)*0.01)) AS amount_over_1pct,
           MAX(ABS(m.minute_volume-d.volume)/NULLIF(ABS(d.volume),0)) AS max_volume_relative_diff,
           MAX(ABS(m.minute_amount-d.amount)) AS max_amount_abs_diff,
           MAX(ABS(m.minute_amount-d.amount)/NULLIF(ABS(d.amount),0)) AS max_amount_relative_diff
    FROM sm_stock_kline d
    JOIN (
      SELECT stock_code, SUM(volume) AS minute_volume, SUM(amount) AS minute_amount,
             MAX(trade_time) AS max_trade_time
      FROM sm_stock_minute
      WHERE trade_date=(SELECT MAX(trade_date) FROM sm_stock_minute)
      GROUP BY stock_code
    ) m ON m.stock_code=d.stock_code
    JOIN sm_stock_minute last_bar
      ON last_bar.stock_code=m.stock_code AND last_bar.trade_time=m.max_trade_time
    WHERE d.k_type=1 AND d.adjust_type=0
      AND d.trade_date=(SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type=1 AND adjust_type=0)
"""))
checks.append(one(history, 'stock_sample_000001', """
    SELECT d.trade_date, d.open, d.high, d.low, d.close, d.volume AS daily_volume,
           d.amount AS daily_amount, m.bars, m.minute_volume, m.minute_amount,
           last_bar.price AS minute_close, d.data_source
    FROM sm_stock_kline d
    JOIN (
      SELECT stock_code, COUNT(*) AS bars, SUM(volume) AS minute_volume,
             SUM(amount) AS minute_amount, MAX(trade_time) AS max_trade_time
      FROM sm_stock_minute WHERE stock_code='000001'
        AND trade_date=(SELECT MAX(trade_date) FROM sm_stock_minute)
      GROUP BY stock_code
    ) m ON m.stock_code=d.stock_code
    JOIN sm_stock_minute last_bar ON last_bar.stock_code=m.stock_code AND last_bar.trade_time=m.max_trade_time
    WHERE d.stock_code='000001' AND d.k_type=1 AND d.adjust_type=0
      AND d.trade_date=(SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type=1 AND adjust_type=0)
"""))
checks.append(one(history, 'index_kline', """
    SELECT COUNT(*) AS rows, COUNT(DISTINCT index_code) AS codes,
           MIN(trade_date) AS first_date, MAX(trade_date) AS last_date,
           SUM(open <= 0 OR close <= 0 OR high < GREATEST(open, close) OR low > LEAST(open, close)) AS bad_ohlc,
           COUNT(*) - COUNT(DISTINCT index_code, trade_date, k_type) AS duplicate_rows,
           COUNT(DISTINCT CASE WHEN trade_date=(SELECT MAX(trade_date) FROM sm_index_kline) THEN index_code END) AS latest_codes
    FROM sm_index_kline
"""))
checks.append(one(history, 'index_minute_latest', """
    SELECT MAX(trade_date) AS trade_date, COUNT(*) AS rows,
           COUNT(DISTINCT index_code) AS codes,
           COUNT(DISTINCT index_code, trade_time) AS unique_keys,
           MIN(TIME(trade_time)) AS first_bar, MAX(TIME(trade_time)) AS last_bar,
           SUM(TIME(trade_time)='09:30:00') AS bars_0930,
           SUM(price IS NULL OR price <= 0 OR volume < 0 OR amount < 0) AS bad_rows
    FROM sm_index_minute
    WHERE trade_date=(SELECT MAX(trade_date) FROM sm_index_minute)
"""))
checks.append(one(history, 'index_minute_bars_per_code', """
    SELECT MIN(bar_count) AS min_bars, MAX(bar_count) AS max_bars,
           SUM(bar_count <> 240) AS codes_not_240, COUNT(*) AS codes
    FROM (
      SELECT index_code, COUNT(*) AS bar_count FROM sm_index_minute
      WHERE trade_date=(SELECT MAX(trade_date) FROM sm_index_minute)
      GROUP BY index_code
    ) bars
"""))

with history.connect() as conn:
    daily_rows = conn.execute(text("""
        SELECT stock_code FROM sm_stock_kline
        WHERE k_type=1 AND adjust_type=0
          AND trade_date=(SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type=1 AND adjust_type=0)
    """)).fetchall()
with main.connect() as conn:
    latest_flow_date = conn.execute(text("SELECT DATE(MAX(trade_time)) FROM sm_stock_capital_flow_min")).scalar()
    flow_rows = conn.execute(text("""
        SELECT DISTINCT stock_code FROM sm_stock_capital_flow_min
        WHERE trade_time >= :d AND trade_time < DATE_ADD(:d, INTERVAL 1 DAY)
    """), {'d': latest_flow_date}).fetchall() if latest_flow_date else []
    pool_rows = conn.execute(text("SELECT stock_code, exchange FROM si_all_code")).fetchall()
daily_codes = {str(row[0]).zfill(6) for row in daily_rows}
flow_codes = {str(row[0]).zfill(6) for row in flow_rows}
exchange_map = {str(row[0]).zfill(6): str(row[1] or '') for row in pool_rows}
missing_flow = sorted(daily_codes - flow_codes)
missing_by_exchange = {}
daily_by_exchange = {}
flow_by_exchange = {}
for code in daily_codes:
    exchange = exchange_map.get(code, 'UNKNOWN') or 'UNKNOWN'
    daily_by_exchange[exchange] = daily_by_exchange.get(exchange, 0) + 1
    if code in flow_codes:
        flow_by_exchange[exchange] = flow_by_exchange.get(exchange, 0) + 1
for code in missing_flow:
    exchange = exchange_map.get(code, 'UNKNOWN') or 'UNKNOWN'
    missing_by_exchange[exchange] = missing_by_exchange.get(exchange, 0) + 1
checks.append({
    'name': 'stock_minute_flow_provider_coverage',
    'seconds': 0,
    'metrics': {
        'flow_trade_date': plain(latest_flow_date),
        'daily_codes': len(daily_codes),
        'flow_codes': len(flow_codes & daily_codes),
        'coverage': round(len(flow_codes & daily_codes) / max(len(daily_codes), 1), 6),
        'daily_by_exchange': daily_by_exchange,
        'flow_by_exchange': flow_by_exchange,
        'missing_by_exchange': missing_by_exchange,
        'missing_sample': ','.join(missing_flow[:30]),
    },
})

service = subprocess.run(
    ['systemctl', 'is-active', 'probiga', 'probiga-scheduler', 'mysql'],
    text=True, capture_output=True, check=False, timeout=15,
)
limits = subprocess.run(
    ['systemctl', 'show', 'probiga-scheduler',
     '--property=MemoryCurrent,MemoryPeak,MemoryHigh,MemoryMax,CPUQuotaPerSecUSec,TasksCurrent'],
    text=True, capture_output=True, check=False, timeout=15,
)
print(json.dumps({
    'collected_at': datetime.now().isoformat(timespec='seconds'),
    'checks': checks,
    'services': service.stdout.strip().splitlines(),
    'scheduler_limits': limits.stdout.strip().splitlines(),
}, ensure_ascii=False, default=str))
'''


ACTIVE_PROBE = r'''
from __future__ import annotations

import json
import subprocess
from datetime import datetime

from sqlalchemy import text

from server.common.batch_db import create_batch_engine


engine = create_batch_engine()
with engine.connect() as conn:
    tasks = [dict(row) for row in conn.execute(text(
        "SELECT id, task_name, task_type, last_run_at, last_run_status, last_run_duration, "
        "RIGHT(COALESCE(last_run_output,''), 1000) AS output_tail "
        "FROM st_scheduled_tasks WHERE enabled=1 AND last_run_status='running' ORDER BY id"
    )).mappings().all()]
    heartbeat = dict(conn.execute(text(
        "SELECT instance_id, pid, heartbeat_at, TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) AS age_seconds "
        "FROM st_scheduler_runtime ORDER BY heartbeat_at DESC LIMIT 1"
    )).mappings().first() or {})

service = subprocess.run(
    ['systemctl', 'is-active', 'probiga', 'probiga-scheduler'],
    text=True, capture_output=True, check=False, timeout=15,
)
memory = subprocess.run(
    ['systemctl', 'show', 'probiga-scheduler', '--property=MemoryCurrent,MemoryPeak,TasksCurrent'],
    text=True, capture_output=True, check=False, timeout=15,
)
scheduler_pid = str(heartbeat.get('pid') or '')
children = subprocess.run(
    ['ps', '-o', 'pid,ppid,etime,pcpu,rss,stat,wchan:24,cmd', '--ppid', scheduler_pid],
    text=True, capture_output=True, check=False, timeout=15,
) if scheduler_pid.isdigit() else None
print(json.dumps({
    'collected_at': datetime.now().isoformat(timespec='seconds'),
    'running_tasks': tasks,
    'heartbeat': heartbeat,
    'services': service.stdout.strip().splitlines(),
    'scheduler_memory': memory.stdout.strip().splitlines(),
    'scheduler_children': children.stdout.strip().splitlines() if children else [],
}, ensure_ascii=False, default=str))
'''


DATABASE_PROBE = r'''
from __future__ import annotations

import json
import math
import time
from datetime import datetime

from sqlalchemy import text

from server.common.batch_db import create_batch_engine, quote_identifier, routed_read_engine
from server.common.kline_data import get_kline_engine
from server.common.minute_data import get_minute_engine
from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS


def plain(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def execute_one(target_engine, sql, params=None):
    started = time.monotonic()
    with target_engine.connect() as conn:
        row = conn.execute(text(sql), params or {}).mappings().first()
    return {
        'metrics': {key: plain(value) for key, value in dict(row or {}).items()},
        'seconds': round(time.monotonic() - started, 3),
    }


def add_check(checks, name, status, metrics, seconds=0.0, note=''):
    checks.append({
        'name': name,
        'status': status,
        'metrics': {key: plain(value) for key, value in metrics.items()},
        'seconds': round(float(seconds), 3),
        'note': note,
    })


engine = create_batch_engine()
kline_engine = get_kline_engine()
minute_engine = get_minute_engine()
checks = []

with kline_engine.connect() as conn:
    reference_trade_date_value = conn.execute(text(
        "SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type=1 AND adjust_type=0"
    )).scalar()
reference_trade_date = (
    str(reference_trade_date_value)[:10] if reference_trade_date_value else ''
)

with engine.connect() as conn:
    enabled_types = {
        str(row[0])
        for row in conn.execute(text("SELECT task_type FROM st_scheduled_tasks WHERE enabled=1")).fetchall()
    }

profiles = []
seen = set()
for task_type, requirements in sorted(TASK_OUTPUT_REQUIREMENTS.items()):
    for requirement in requirements:
        key = (requirement.table, requirement.date_col, requirement.distinct_col, requirement.where_sql)
        if key in seen:
            continue
        seen.add(key)
        profile = {
            'task_types': sorted({
                candidate_type
                for candidate_type, candidate_requirements in TASK_OUTPUT_REQUIREMENTS.items()
                if any((item.table, item.date_col, item.distinct_col, item.where_sql) == key for item in candidate_requirements)
            }),
            'active': any(candidate in enabled_types for candidate in TASK_OUTPUT_REQUIREMENTS if any(
                (item.table, item.date_col, item.distinct_col, item.where_sql) == key
                for item in TASK_OUTPUT_REQUIREMENTS[candidate]
            )),
            'table': requirement.table,
            'date_col': requirement.date_col,
            'distinct_col': requirement.distinct_col,
            'where_sql': requirement.where_sql,
            'min_rows': requirement.min_rows,
            'min_distinct': requirement.min_distinct,
            'require_fresh': requirement.require_fresh,
            'expected_date': reference_trade_date if requirement.require_fresh and requirement.date_col else None,
        }
        target_engine = routed_read_engine(f"SELECT * FROM {requirement.table}", engine)
        table = quote_identifier(requirement.table)
        where = f" WHERE ({requirement.where_sql})" if requirement.where_sql else ""
        started = time.monotonic()
        try:
            if requirement.date_col:
                date_col = quote_identifier(requirement.date_col)
                with target_engine.connect() as conn:
                    latest = conn.execute(text(f"SELECT DATE(MAX({date_col})) FROM {table}{where}")).scalar()
                    latest_s = str(latest)[:10] if latest else ''
                    params = {'latest': latest_s}
                    parts = [f"{date_col} >= :latest", f"{date_col} < DATE_ADD(:latest, INTERVAL 1 DAY)"]
                    if requirement.where_sql:
                        parts.insert(0, f"({requirement.where_sql})")
                    latest_where = " WHERE " + " AND ".join(parts)
                    count_sql = f"SELECT COUNT(*) AS rows"
                    if requirement.distinct_col:
                        distinct_col = quote_identifier(requirement.distinct_col)
                        count_sql += f", COUNT(DISTINCT {distinct_col}) AS distinct_rows, SUM({distinct_col} IS NULL OR {distinct_col}='') AS null_keys"
                    count_sql += f" FROM {table}{latest_where}"
                    metrics = dict(conn.execute(text(count_sql), params).mappings().first() or {})
                    duplicate_groups = None
                    if requirement.distinct_col:
                        duplicate_groups = conn.execute(text(
                            f"SELECT COUNT(*) FROM (SELECT {distinct_col} FROM {table}{latest_where} "
                            f"GROUP BY {distinct_col} HAVING COUNT(*) > 1) d"
                        ), params).scalar()
                    profile.update({
                        'latest_date': latest_s,
                        'latest_rows': int(metrics.get('rows') or 0),
                        'latest_distinct': int(metrics.get('distinct_rows') or 0) if requirement.distinct_col else None,
                        'null_keys': int(metrics.get('null_keys') or 0) if requirement.distinct_col else None,
                        'duplicate_groups': int(duplicate_groups or 0) if requirement.distinct_col else None,
                    })
            else:
                with target_engine.connect() as conn:
                    count_sql = "SELECT COUNT(*) AS rows"
                    if requirement.distinct_col:
                        distinct_col = quote_identifier(requirement.distinct_col)
                        count_sql += f", COUNT(DISTINCT {distinct_col}) AS distinct_rows, SUM({distinct_col} IS NULL OR {distinct_col}='') AS null_keys"
                    count_sql += f" FROM {table}{where}"
                    metrics = dict(conn.execute(text(count_sql)).mappings().first() or {})
                    profile.update({
                        'latest_date': None,
                        'latest_rows': int(metrics.get('rows') or 0),
                        'latest_distinct': int(metrics.get('distinct_rows') or 0) if requirement.distinct_col else None,
                        'null_keys': int(metrics.get('null_keys') or 0) if requirement.distinct_col else None,
                        'duplicate_groups': None,
                    })
            rows_ok = int(profile.get('latest_rows') or 0) >= int(requirement.min_rows or 0)
            distinct_ok = not requirement.min_distinct or int(profile.get('latest_distinct') or 0) >= int(requirement.min_distinct)
            freshness_required = bool(
                profile['active'] and requirement.require_fresh and requirement.date_col
            )
            freshness_ok = (
                not freshness_required
                or bool(profile.get('latest_date'))
                and str(profile.get('latest_date')) >= reference_trade_date
            )
            profile['integrity_status'] = 'PASS' if rows_ok and distinct_ok else 'FAIL'
            profile['freshness_status'] = (
                'PASS' if freshness_required and freshness_ok
                else 'FAIL' if freshness_required
                else 'NOT_REQUIRED'
            )
            profile['status'] = 'PASS' if rows_ok and distinct_ok and freshness_ok else 'FAIL'
        except Exception as exc:
            profile.update({'status': 'ERROR', 'error': str(exc)})
        profile['seconds'] = round(time.monotonic() - started, 3)
        profiles.append(profile)

# Core master-data integrity.
result = execute_one(engine, """
    SELECT COUNT(*) AS total_rows, COUNT(DISTINCT stock_code) AS distinct_codes,
           SUM(stock_code IS NULL OR stock_code NOT REGEXP '^[0-9]{6}$') AS invalid_codes,
           SUM(stock_code LIKE '810%') AS bad_810_convertible_bonds,
           SUM(stock_code LIKE '899%') AS bad_899_indexes,
           SUM(short_name IS NULL OR TRIM(short_name)='') AS blank_names,
           SUM(exchange IS NULL OR TRIM(exchange)='') AS blank_exchange
    FROM si_all_code
""")
m = result['metrics']
add_check(checks, 'stock_universe_integrity', 'PASS' if not any(int(m.get(k) or 0) for k in ('invalid_codes','bad_810_convertible_bonds','bad_899_indexes','blank_names','blank_exchange')) and int(m.get('total_rows') or 0) == int(m.get('distinct_codes') or 0) else 'FAIL', m, result['seconds'])

# Daily K-line correctness and date freshness.
result = execute_one(kline_engine, """
    SELECT MAX(trade_date) AS latest_trade_date,
           COUNT(*) AS rows,
           COUNT(DISTINCT stock_code) AS stocks,
           SUM(open IS NULL OR close IS NULL OR high IS NULL OR low IS NULL) AS null_ohlc,
           SUM(stock_code LIKE '810%' OR stock_code LIKE '899%') AS non_equity_codes,
           SUM(pre_close IS NULL OR pre_close <= 0 OR `change` IS NULL OR change_pct IS NULL) AS missing_change_fields,
           SUM(open <= 0 OR close <= 0 OR high <= 0 OR low <= 0) AS nonpositive_ohlc,
           SUM(high < GREATEST(open, close, low) OR low > LEAST(open, close, high)) AS invalid_ohlc,
           SUM(volume < 0 OR amount < 0) AS negative_volume_amount,
           SUM(pre_close > 0 AND ABS(change_pct - ((close / pre_close - 1) * 100)) > 0.08) AS change_pct_mismatch
    FROM sm_stock_kline
    WHERE k_type=1 AND adjust_type=0
      AND trade_date=(SELECT MAX(trade_date) FROM sm_stock_kline WHERE k_type=1 AND adjust_type=0)
""")
m = result['metrics']
kline_bad = sum(int(m.get(k) or 0) for k in ('null_ohlc','non_equity_codes','nonpositive_ohlc','invalid_ohlc','negative_volume_amount'))
change_coverage = 1.0 - int(m.get('missing_change_fields') or 0) / max(int(m.get('rows') or 0), 1)
m['change_field_coverage'] = round(change_coverage, 6)
add_check(checks, 'daily_kline_business_rules', 'PASS' if kline_bad == 0 and int(m.get('stocks') or 0) >= 5000 and change_coverage >= 0.98 and int(m.get('change_pct_mismatch') or 0) <= 5 else 'FAIL', m, result['seconds'])
latest_trade_date = str(m.get('latest_trade_date') or '')[:10]
expected_stocks = int(m.get('stocks') or 0)

result = execute_one(engine, """
    SELECT :d AS latest_trade_date, COUNT(*) AS rows,
           COUNT(DISTINCT stock_code) AS stocks,
           SUM(stock_code IS NULL OR stock_code NOT REGEXP '^[0-9]{6}$') AS invalid_codes,
           SUM(main_net_inflow IS NULL OR max_net_inflow IS NULL OR lg_net_inflow IS NULL OR mid_net_inflow IS NULL OR sm_net_inflow IS NULL) AS null_flow_fields
    FROM sm_stock_capital_flow_daily
    WHERE trade_date=:d
""", {'d': latest_trade_date})
m = result['metrics']
with kline_engine.connect() as conn:
    target_flow_codes = {
        str(row[0]).zfill(6)
        for row in conn.execute(text("""
            SELECT DISTINCT stock_code FROM sm_stock_kline
            WHERE trade_date=:d AND k_type=1 AND adjust_type=0
              AND stock_code NOT LIKE '810%%' AND stock_code NOT LIKE '899%%'
        """), {'d': latest_trade_date}).fetchall()
    }
with engine.connect() as conn:
    actual_flow_codes = {
        str(row[0]).zfill(6)
        for row in conn.execute(text(
            "SELECT stock_code FROM sm_stock_capital_flow_daily WHERE trade_date=:d"
        ), {'d': latest_trade_date}).fetchall()
    }
    flow_exchange_map = {
        str(row[0]).zfill(6): str(row[1] or 'UNKNOWN')
        for row in conn.execute(text("SELECT stock_code, exchange FROM si_all_code")).fetchall()
    }
supported_flow_codes = {
    code for code in target_flow_codes
    if flow_exchange_map.get(code, 'UNKNOWN') in {'SH', 'SZ', 'BJ'}
}
unsupported_flow_codes = sorted(target_flow_codes - supported_flow_codes)
missing_daily_flow = sorted(supported_flow_codes - actual_flow_codes)
outside_daily_flow = sorted(actual_flow_codes - supported_flow_codes)
missing_daily_by_exchange = {}
for code in missing_daily_flow:
    exchange = flow_exchange_map.get(code, 'UNKNOWN') or 'UNKNOWN'
    missing_daily_by_exchange[exchange] = missing_daily_by_exchange.get(exchange, 0) + 1
m.update({
    'target_stocks': len(supported_flow_codes),
    'matched_stocks': len(supported_flow_codes & actual_flow_codes),
    'coverage_ratio': round(len(supported_flow_codes & actual_flow_codes) / max(len(supported_flow_codes), 1), 6),
    'unsupported_stocks': len(unsupported_flow_codes),
    'unsupported_by_exchange': {
        exchange: sum(1 for code in unsupported_flow_codes if flow_exchange_map.get(code, 'UNKNOWN') == exchange)
        for exchange in sorted({flow_exchange_map.get(code, 'UNKNOWN') for code in unsupported_flow_codes})
    },
    'missing_stocks': len(missing_daily_flow),
    'outside_stocks': len(outside_daily_flow),
    'missing_by_exchange': missing_daily_by_exchange,
    'missing_sample': ','.join(missing_daily_flow[:30]),
    'outside_sample': ','.join(outside_daily_flow[:30]),
})
daily_flow_exact = (
    supported_flow_codes == actual_flow_codes
    and not int(m.get('invalid_codes') or 0)
    and not int(m.get('null_flow_fields') or 0)
)
add_check(
    checks,
    'daily_capital_flow_integrity',
    'PASS' if daily_flow_exact else 'FAIL',
    m,
    result['seconds'],
    note='PASS requires exact SH/SZ/BJ coverage from the Eastmoney batch endpoint; non-equity codes are never synthesized.',
)

flow_minute_date_result = execute_one(engine, """
    SELECT DATE(MAX(trade_time)) AS latest_minute_flow_date
    FROM sm_stock_capital_flow_min
""")
latest_minute_flow_date = str(flow_minute_date_result['metrics'].get('latest_minute_flow_date') or '')[:10]
result = execute_one(engine, """
    SELECT COUNT(*) AS compared,
           SUM(
               ABS(d.main_net_inflow-m.main_net_inflow) > 500 OR
               ABS(d.max_net_inflow-m.max_net_inflow) > 500 OR
               ABS(d.lg_net_inflow-m.lg_net_inflow) > 500 OR
               ABS(d.mid_net_inflow-m.mid_net_inflow) > 500 OR
               ABS(d.sm_net_inflow-m.sm_net_inflow) > 500
           ) AS rows_over_500_abs,
           MAX(GREATEST(
               ABS(d.main_net_inflow-m.main_net_inflow),
               ABS(d.max_net_inflow-m.max_net_inflow),
               ABS(d.lg_net_inflow-m.lg_net_inflow),
               ABS(d.mid_net_inflow-m.mid_net_inflow),
               ABS(d.sm_net_inflow-m.sm_net_inflow)
           )) AS max_field_abs_diff,
           SUM(ABS(d.main_net_inflow-(d.max_net_inflow+d.lg_net_inflow)) > 1) AS daily_main_identity_mismatch,
           SUM(ABS(m.main_net_inflow-(m.max_net_inflow+m.lg_net_inflow)) > 1) AS minute_main_identity_mismatch
    FROM sm_stock_capital_flow_daily d
    JOIN (
        SELECT f.stock_code, f.main_net_inflow, f.max_net_inflow,
               f.lg_net_inflow, f.mid_net_inflow, f.sm_net_inflow
        FROM sm_stock_capital_flow_min f
        JOIN (
            SELECT stock_code, MAX(trade_time) AS last_time
            FROM sm_stock_capital_flow_min
            WHERE trade_time >= :d AND trade_time < DATE_ADD(:d, INTERVAL 1 DAY)
            GROUP BY stock_code
        ) x ON x.stock_code=f.stock_code AND x.last_time=f.trade_time
    ) m ON m.stock_code=d.stock_code
    WHERE d.trade_date=:d
""", {'d': latest_trade_date})
m = result['metrics']
m['latest_minute_flow_date'] = latest_minute_flow_date
daily_minute_flow_exact = (
    int(m.get('compared') or 0) >= int(len(supported_flow_codes) * 0.99)
    and not int(m.get('rows_over_500_abs') or 0)
    and not int(m.get('daily_main_identity_mismatch') or 0)
    and not int(m.get('minute_main_identity_mismatch') or 0)
)
same_flow_day_available = latest_minute_flow_date == latest_trade_date
add_check(
    checks,
    'daily_flow_vs_minute_close_accuracy',
    'PASS' if (daily_minute_flow_exact or not same_flow_day_available) else 'FAIL',
    m,
    result['seconds'] + flow_minute_date_result['seconds'],
    note=(
        'Same-day Eastmoney daily/minute values agree within 500 CNY.'
        if same_flow_day_available
        else 'Skipped because the minute-flow table intentionally retains the newer intraday day; the historical range was audited separately.'
    ),
)

result = execute_one(engine, """
    SELECT DATE(MAX(snapshot_at)) AS latest_date, MAX(snapshot_at) AS latest_snapshot,
           COUNT(*) AS rows, COUNT(DISTINCT stock_code) AS stocks,
           SUM(stock_code LIKE '810%' OR stock_code LIKE '899%') AS non_equity_rows,
           SUM(price IS NULL OR price <= 0) AS bad_price,
           SUM(volume < 0 OR amount < 0) AS negative_volume_amount,
           SUM(stock_code IS NULL OR stock_code NOT REGEXP '^[0-9]{6}$') AS invalid_codes
    FROM sm_stock_current
""")
m = result['metrics']
add_check(checks, 'current_quote_integrity', 'PASS' if int(m.get('stocks') or 0) >= int(expected_stocks * 0.95) and not sum(int(m.get(k) or 0) for k in ('non_equity_rows','bad_price','negative_volume_amount','invalid_codes')) else 'FAIL', m, result['seconds'])

result = execute_one(minute_engine, """
    SELECT MAX(trade_date) AS latest_trade_date, COUNT(*) AS rows,
           COUNT(DISTINCT stock_code) AS stocks,
           SUM(stock_code LIKE '810%' OR stock_code LIKE '899%') AS non_equity_rows,
           SUM(price IS NULL OR price <= 0) AS bad_price,
           SUM(volume < 0 OR amount < 0) AS negative_volume_amount,
           SUM(TIME(trade_time) < '09:25:00' OR TIME(trade_time) > '15:05:00' OR (TIME(trade_time) > '11:35:00' AND TIME(trade_time) < '12:55:00')) AS outside_market_window
    FROM sm_stock_minute
    WHERE trade_date=(SELECT MAX(trade_date) FROM sm_stock_minute)
""")
m = result['metrics']
minute_stocks = int(m.get('stocks') or 0)
minute_ratio = round(minute_stocks / max(expected_stocks, 1), 4)
m['coverage_ratio'] = minute_ratio
add_check(checks, 'stock_minute_integrity_and_coverage', 'PASS' if minute_ratio >= 0.95 and not sum(int(m.get(k) or 0) for k in ('non_equity_rows','bad_price','negative_volume_amount','outside_market_window')) else 'WARN' if minute_ratio >= 0.70 else 'FAIL', m, result['seconds'], note='PASS requires 95% full-market coverage; configured operational floor is 70%.')

result = execute_one(engine, """
    SELECT DATE(MAX(trade_time)) AS latest_trade_date, COUNT(*) AS rows,
           COUNT(DISTINCT stock_code) AS stocks,
           SUM(stock_code LIKE '810%' OR stock_code LIKE '899%') AS non_equity_rows,
           SUM(main_net_inflow IS NULL OR max_net_inflow IS NULL OR lg_net_inflow IS NULL OR mid_net_inflow IS NULL OR sm_net_inflow IS NULL) AS null_flow_fields,
           SUM(TIME(trade_time) < '09:25:00' OR TIME(trade_time) > '15:05:00' OR (TIME(trade_time) > '11:35:00' AND TIME(trade_time) < '12:55:00')) AS outside_market_window
    FROM sm_stock_capital_flow_min
    WHERE trade_time >= (SELECT DATE(MAX(trade_time)) FROM sm_stock_capital_flow_min)
      AND trade_time < DATE_ADD((SELECT DATE(MAX(trade_time)) FROM sm_stock_capital_flow_min), INTERVAL 1 DAY)
""")
m = result['metrics']
flow_min_stocks = int(m.get('stocks') or 0)
flow_min_ratio = round(flow_min_stocks / max(expected_stocks, 1), 4)
m['coverage_ratio'] = flow_min_ratio
add_check(checks, 'minute_capital_flow_integrity_and_coverage', 'PASS' if flow_min_ratio >= 0.95 and not sum(int(m.get(k) or 0) for k in ('non_equity_rows','null_flow_fields','outside_market_window')) else 'WARN' if flow_min_ratio >= 0.50 else 'FAIL', m, result['seconds'], note='PASS requires 95% full-market coverage; configured operational floor is 50%.')

# Cross-table price consistency using small full-market maps, not a cross-database join.
with kline_engine.connect() as conn:
    kline_rows = conn.execute(text("""
        SELECT stock_code, close, volume, amount FROM sm_stock_kline
        WHERE k_type=1 AND adjust_type=0 AND trade_date=:d
    """), {'d': latest_trade_date}).fetchall()
with minute_engine.connect() as conn:
    minute_rows = conn.execute(text("""
        SELECT m.stock_code, m.price
        FROM sm_stock_minute m
        JOIN (
          SELECT stock_code, MAX(trade_time) AS max_time
          FROM sm_stock_minute WHERE trade_date=:d GROUP BY stock_code
        ) x ON x.stock_code=m.stock_code AND x.max_time=m.trade_time
        WHERE m.trade_date=:d
    """), {'d': latest_trade_date}).fetchall()
    latest_minute_date = conn.execute(
        text("SELECT MAX(trade_date) FROM sm_stock_minute")
    ).scalar()
    latest_minute_codes = {
        str(row[0]).zfill(6)
        for row in conn.execute(
            text(
                "SELECT DISTINCT stock_code FROM sm_stock_minute "
                "WHERE trade_date=:minute_date"
            ),
            {'minute_date': latest_minute_date},
        ).fetchall()
    }
with engine.connect() as conn:
    current_rows = conn.execute(
        text("SELECT stock_code, price, snapshot_at FROM sm_stock_current")
    ).fetchall()
close_map = {
    str(row[0]).zfill(6): float(row[1])
    for row in kline_rows
    if row[1] is not None and float(row[1]) > 0
}
minute_codes = {str(row[0]).zfill(6) for row in minute_rows}
missing_minute = sorted(set(close_map) - latest_minute_codes)
kline_activity = {
    str(row[0]).zfill(6): (float(row[2] or 0), float(row[3] or 0))
    for row in kline_rows
}
missing_no_trade = [
    code for code in missing_minute
    if kline_activity.get(code, (0.0, 0.0))[0] <= 0 and kline_activity.get(code, (0.0, 0.0))[1] <= 0
]
add_check(checks, 'minute_daily_universe_semantic_coverage', 'PASS' if len(missing_minute) <= max(10, int(expected_stocks * 0.01)) else 'FAIL', {
    'daily_universe': len(close_map),
    'minute_present': len(latest_minute_codes & set(close_map)),
    'latest_minute_date': latest_minute_date,
    'missing': len(missing_minute),
    'missing_no_trade': len(missing_no_trade),
    'missing_codes': ','.join(missing_minute[:30]),
}, note='PASS allows at most 1% provider gaps; no-trade symbols are reported separately and no bars are synthesized.')

def compare_prices(rows, label, tolerance):
    compared = mismatch = 0
    max_diff = 0.0
    for code, value in rows:
        key = str(code).zfill(6)
        if key not in close_map or value is None or float(value) <= 0:
            continue
        diff = abs(float(value) / close_map[key] - 1.0)
        compared += 1
        max_diff = max(max_diff, diff)
        if diff > tolerance:
            mismatch += 1
    ratio = mismatch / max(compared, 1)
    add_check(checks, label, 'PASS' if compared >= 3000 and ratio <= 0.01 else 'FAIL', {
        'compared': compared,
        'mismatches': mismatch,
        'mismatch_ratio': round(ratio, 6),
        'max_relative_diff': round(max_diff, 6),
        'tolerance': tolerance,
    })

current_snapshot_dates = {
    str(row[2])[:10] for row in current_rows if len(row) > 2 and row[2] is not None
}
if current_snapshot_dates == {latest_trade_date}:
    compare_prices(
        [(row[0], row[1]) for row in current_rows],
        'current_vs_daily_close',
        0.005,
    )
else:
    add_check(checks, 'current_vs_daily_close', 'PASS', {
        'compared': 0,
        'current_snapshot_dates': sorted(current_snapshot_dates),
        'daily_close_date': latest_trade_date,
        'skipped_cross_date_comparison': True,
    }, note='Current quotes and daily close are from different sessions; a price-equality check would be invalid.')
compare_prices(minute_rows, 'minute_last_vs_daily_close', 0.005)

for table_name, code_col in (
    ('sm_index_current', 'index_code'),
    ('sm_concept_east_current', 'index_code'),
    ('sm_concept_ths_current', 'index_code'),
):
    result = execute_one(engine, f"""
        SELECT COUNT(*) AS rows, COUNT(DISTINCT {code_col}) AS distinct_codes,
               SUM(price IS NULL OR price <= 0) AS bad_price,
               SUM(high < GREATEST(open, price, low) OR low > LEAST(open, price, high)) AS invalid_ohlc,
               SUM(volume < 0 OR amount < 0) AS negative_volume_amount
        FROM {table_name}
    """)
    mm = result['metrics']
    status = 'PASS' if int(mm.get('distinct_codes') or 0) >= 50 and not sum(int(mm.get(k) or 0) for k in ('bad_price','invalid_ohlc','negative_volume_amount')) else 'FAIL'
    add_check(checks, f'{table_name}_integrity', status, mm, result['seconds'])

result = execute_one(engine, """
    SELECT DATE(MAX(snapshot_at)) AS latest_date, COUNT(*) AS rows,
           COUNT(DISTINCT index_code) AS concepts,
           SUM(index_code IS NULL OR TRIM(index_code)='') AS blank_codes,
           SUM(index_name IS NULL OR TRIM(index_name)='') AS blank_names,
           SUM(ABS(main_net_inflow - (COALESCE(max_net_inflow,0)+COALESCE(lg_net_inflow,0))) > 0.01) AS main_flow_mismatch
    FROM sm_concept_capital_flow_east
""")
m = result['metrics']
add_check(checks, 'concept_capital_flow_integrity', 'PASS' if int(m.get('concepts') or 0) >= 100 and not sum(int(m.get(k) or 0) for k in ('blank_codes','blank_names','main_flow_mismatch')) else 'FAIL', m, result['seconds'])

for table_name, date_col, rank_col in (
    ('st_hot_rank_ths', 'snapshot_date', 'rank'),
    ('st_hot_pop_rank_east', 'snapshot_date', 'rank'),
    ('st_hot_rank_xq', 'snapshot_date', 'rank'),
    ('st_hot_rank_sina', 'snapshot_date', 'rank'),
    ('st_hot_rank_fused', 'snapshot_date', 'fused_rank'),
):
    rank_identifier = quote_identifier(rank_col)
    result = execute_one(engine, f"""
        SELECT MAX({date_col}) AS latest_date, COUNT(*) AS rows,
               COUNT(DISTINCT stock_code) AS stocks,
               SUM(stock_code IS NULL OR stock_code NOT REGEXP '^[0-9]{{6}}$') AS invalid_codes,
               SUM({rank_identifier} IS NULL OR {rank_identifier} < 1 OR {rank_identifier} > 100) AS invalid_rank
        FROM {table_name}
        WHERE {date_col}=(SELECT MAX({date_col}) FROM {table_name})
    """)
    mm = result['metrics']
    status = 'PASS' if int(mm.get('stocks') or 0) >= 20 and not int(mm.get('invalid_codes') or 0) and not int(mm.get('invalid_rank') or 0) else 'FAIL'
    add_check(checks, f'{table_name}_rank_integrity', status, mm, result['seconds'])

result = execute_one(engine, """
    SELECT COUNT(*) AS total_rows,
           SUM(publish_time > DATE_ADD(NOW(), INTERVAL 5 MINUTE)) AS future_rows,
           SUM((title IS NULL OR TRIM(title)='') AND (content IS NULL OR TRIM(content)='')) AS blank_items,
           COUNT(DISTINCT source) AS sources,
           SUM(publish_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)) AS rows_7d
    FROM st_news_flash
""")
m = result['metrics']
add_check(checks, 'news_integrity', 'PASS' if int(m.get('sources') or 0) >= 3 and int(m.get('rows_7d') or 0) >= 100 and not int(m.get('future_rows') or 0) and not int(m.get('blank_items') or 0) else 'FAIL', m, result['seconds'])

result = execute_one(engine, """
    SELECT COUNT(*) AS total_rows, MAX(notice_date) AS latest_notice,
           SUM(stock_code IS NULL OR stock_code NOT REGEXP '^[0-9]{6}$') AS invalid_codes,
           SUM(title IS NULL OR TRIM(title)='') AS blank_titles,
           SUM(notice_date > DATE_ADD(NOW(), INTERVAL 5 MINUTE)) AS future_rows,
           SUM(notice_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)) AS rows_7d
    FROM si_notice_eastmoney
""")
m = result['metrics']
add_check(checks, 'notice_integrity', 'PASS' if int(m.get('rows_7d') or 0) >= 1000 and not sum(int(m.get(k) or 0) for k in ('invalid_codes','blank_titles','future_rows')) else 'FAIL', m, result['seconds'])

result = execute_one(engine, """
    SELECT MAX(analysis_date) AS latest_date, COUNT(*) AS rows,
           COUNT(DISTINCT stock_code) AS stocks,
           SUM(stock_code IS NULL OR stock_code NOT REGEXP '^[0-9]{6}$') AS invalid_codes,
           SUM(long_term_score < 0 OR long_term_score > 100 OR short_term_score < 0 OR short_term_score > 100 OR data_quality_score < 0 OR data_quality_score > 100) AS invalid_scores
    FROM stock_analysis_result
    WHERE analysis_date=(SELECT MAX(analysis_date) FROM stock_analysis_result)
""")
m = result['metrics']
add_check(checks, 'analysis_result_integrity', 'PASS' if int(m.get('stocks') or 0) >= 5000 and not int(m.get('invalid_codes') or 0) and not int(m.get('invalid_scores') or 0) else 'FAIL', m, result['seconds'])

result = execute_one(engine, """
    SELECT MAX(pick_date) AS latest_date, COUNT(*) AS rows,
           COUNT(DISTINCT stock_code) AS stocks,
           SUM(stock_code IS NULL OR stock_code NOT REGEXP '^[0-9]{6}$') AS invalid_codes,
           SUM(ai_score < 0 OR ai_score > 100 OR final_trade_score < 0 OR final_trade_score > 100 OR position_weight < 0 OR position_weight > 100) AS invalid_scores,
           SUM(entry_price_low IS NOT NULL AND entry_price_high IS NOT NULL AND entry_price_low > entry_price_high) AS invalid_entry_range,
           SUM(stop_loss_price IS NOT NULL AND entry_price_low IS NOT NULL AND stop_loss_price >= entry_price_low) AS invalid_stop_loss,
           SUM(take_profit_1 IS NOT NULL AND entry_price_high IS NOT NULL AND take_profit_1 <= entry_price_high) AS invalid_take_profit
    FROM st_recommended_stocks
    WHERE pick_date=(SELECT MAX(pick_date) FROM st_recommended_stocks)
""")
m = result['metrics']
add_check(checks, 'recommendation_boundary_values', 'PASS' if int(m.get('rows') or 0) >= 1 and not sum(int(m.get(k) or 0) for k in ('invalid_codes','invalid_scores','invalid_entry_range','invalid_stop_loss','invalid_take_profit')) else 'FAIL', m, result['seconds'])

result = execute_one(engine, """
    SELECT MAX(trade_date) AS latest_date, COUNT(*) AS rows,
           SUM(up_cnt + down_cnt > total OR sideline_cnt > total) AS count_boundary_mismatch,
           SUM(total < 0 OR up_cnt < 0 OR down_cnt < 0 OR sideline_cnt < 0 OR total_amount < 0) AS negative_values
    FROM sm_market_overview_daily
    WHERE trade_date=(SELECT MAX(trade_date) FROM sm_market_overview_daily)
""")
m = result['metrics']
add_check(checks, 'market_overview_arithmetic', 'PASS' if int(m.get('rows') or 0) == 1 and not int(m.get('count_boundary_mismatch') or 0) and not int(m.get('negative_values') or 0) else 'FAIL', m, result['seconds'], note='sideline_cnt is an abs(change_pct)<1 subset, not an additive partition.')

result = execute_one(engine, """
    SELECT COUNT(*) AS enabled_tasks,
           SUM(last_run_status = 'running') AS running_tasks,
           SUM(
             last_run_status IN ('failed','timeout','stopped')
             OR (last_run_status = 'running' AND last_run_at < DATE_SUB(NOW(), INTERVAL 2 HOUR))
             OR last_run_status IS NULL
             OR TRIM(last_run_status)=''
           ) AS unhealthy_tasks,
           GROUP_CONCAT(
             CASE WHEN last_run_status IN ('failed','timeout','stopped')
                       OR (last_run_status = 'running' AND last_run_at < DATE_SUB(NOW(), INTERVAL 2 HOUR))
                       OR last_run_status IS NULL
                       OR TRIM(last_run_status)=''
                  THEN CONCAT(id, ':', task_type, ':', COALESCE(NULLIF(TRIM(last_run_status),''), 'NULL')) END
             ORDER BY id SEPARATOR ';'
           ) AS unhealthy_task_details
    FROM st_scheduled_tasks WHERE enabled=1
""")
m = result['metrics']
add_check(
    checks,
    'scheduler_enabled_task_health',
    'PASS' if int(m.get('enabled_tasks') or 0) > 0 and int(m.get('unhealthy_tasks') or 0) == 0 else 'FAIL',
    m,
    result['seconds'],
    note='A currently running task is healthy unless it has remained running for more than two hours.',
)

result = execute_one(engine, """
    SELECT COUNT(*) AS stage_tables,
           SUM(CREATE_TIME < DATE_SUB(NOW(), INTERVAL 2 HOUR)) AS stale_stage_tables
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME REGEXP '_stage_[0-9a-f]+$'
""")
minute_stage_result = execute_one(minute_engine, """
    SELECT COUNT(*) AS minute_stage_tables,
           SUM(CREATE_TIME < DATE_SUB(NOW(), INTERVAL 2 HOUR)) AS stale_minute_stage_tables
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME REGEXP '_stage_[0-9a-f]+$'
""")
m = result['metrics']
m.update(minute_stage_result['metrics'])
add_check(
    checks,
    'temporary_stage_cleanup',
    'PASS' if not int(m.get('stale_stage_tables') or 0) and not int(m.get('stale_minute_stage_tables') or 0) else 'FAIL',
    m,
    result['seconds'] + minute_stage_result['seconds'],
    note='Fresh stage tables belong to active atomic collectors; only stages older than two hours are abandoned.',
)

print(json.dumps({
    'collected_at': datetime.now().isoformat(timespec='seconds'),
    'latest_trade_date': latest_trade_date,
    'expected_stocks': expected_stocks,
    'profiles': profiles,
    'checks': checks,
}, ensure_ascii=False, default=str))
'''


API_PROBE = r'''
from __future__ import annotations

import json
import time
from datetime import datetime
from urllib.request import Request, urlopen


BASE = "http://127.0.0.1:8000"
CASES = [
    ("health", "/api/health", 0),
    ("health_runtime", "/api/health/runtime", 0),
    ("health_schema", "/api/health/schema", 0),
    ("intraday_readiness", "/api/health/intraday-readiness", 0),
    ("latest_trade_date", "/api/hot-data/latest-trade-date", 0),
    ("market_clock", "/api/hot-data/market-clock", 0),
    ("fused_rank", "/api/hot-data/fused?top=100", 20),
    ("ths_rank", "/api/hot-data/rank-ths?top=100", 20),
    ("east_rank", "/api/hot-data/pop-rank-east?top=100", 20),
    ("xq_rank", "/api/hot-data/rank-xq?top=100", 20),
    ("capital_flow", "/api/hot-data/capital-flow?top=100", 20),
    ("concept_rank", "/api/hot-data/concept-ths?snapshot_date=2026-07-19", 20),
    ("news_history", "/api/hot-data/news-history?limit=50", 50),
    ("stock_notices", "/api/hot-data/stock-notices?limit=50", 50),
    ("stock_minute", "/api/hot-data/stock-minute?stock_code=000001&trade_date=2026-07-17", 200),
    ("stock_list", "/api/hot-data/stock-list?page=1&page_size=20", 20),
    ("stock_detail", "/api/hot-data/stock-detail?stock_code=000001&trade_date=2026-07-17", 0),
    ("analysis_result", "/api/hot-data/analysis-result?page=1&page_size=20", 20),
    ("recommendations", "/api/hot-data/recommended-stocks?trade_date=2026-07-17", 1),
    ("market_sentiment", "/api/hot-data/market-sentiment?date=2026-07-17", 0),
    ("sector_movement", "/api/sector/movement?group_by=all", 0),
    ("monitor_data", "/api/monitor/data?date=2026-07-17", 0),
]


def count_payload(payload):
    if not isinstance(payload, dict):
        return 0
    data = payload.get("data")
    if isinstance(data, list):
        return len(data)
    total = payload.get("total")
    try:
        return int(total or 0)
    except Exception:
        return 0


results = []
for name, path, minimum in CASES:
    started = time.monotonic()
    item = {"name": name, "path": path, "minimum": minimum}
    try:
        request = Request(BASE + path, headers={"Accept": "application/json", "User-Agent": "ProBigA-Acceptance/1.0"})
        with urlopen(request, timeout=45) as response:
            raw = response.read()
            item["http_status"] = int(response.status)
            item["server_elapsed_ms"] = response.headers.get("X-ProBigA-Elapsed-Ms", "")
        payload = json.loads(raw.decode("utf-8"))
        count = count_payload(payload)
        item.update({
            "count": count,
            "payload_status": payload.get("status") if isinstance(payload, dict) else "",
            "date": payload.get("date", "") if isinstance(payload, dict) else "",
            "source": payload.get("source", "") if isinstance(payload, dict) else "",
            "error": payload.get("error", "") if isinstance(payload, dict) else "",
            "keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
        })
        item["status"] = "PASS" if item["http_status"] == 200 and not item["error"] and count >= minimum else "FAIL"
    except Exception as exc:
        item.update({"status": "ERROR", "error": str(exc), "count": 0})
    item["elapsed_ms"] = round((time.monotonic() - started) * 1000, 2)
    results.append(item)

print(json.dumps({
    "collected_at": datetime.now().isoformat(timespec="seconds"),
    "base_url": BASE,
    "passed": sum(item["status"] == "PASS" for item in results),
    "failed": sum(item["status"] != "PASS" for item in results),
    "results": results,
}, ensure_ascii=False, default=str))
'''


def _connect() -> paramiko.SSHClient:
    remote_pythonpath(remote_root())
    client = production_ssh_client(paramiko)
    client.connect(**production_ssh_connect_kwargs())
    return client


def _run_probe(client: paramiko.SSHClient, source: str, *, timeout: int) -> dict[str, Any]:
    root = remote_root()
    pythonpath = remote_pythonpath(root)
    remote_path = posixpath.join("/tmp", f"probiga_acceptance_{uuid.uuid4().hex}.py")
    sftp = client.open_sftp()
    try:
        with sftp.open(remote_path, "w") as handle:
            handle.write(source)
    finally:
        sftp.close()

    command = (
        f"cd {shlex.quote(root)} && "
        f"PYTHONPATH={shlex.quote(pythonpath)} "
        f"{shlex.quote(posixpath.join(root, 'venv/bin/python'))} "
        f"{shlex.quote(remote_path)}"
    )
    try:
        _stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        out = stdout.read().decode("utf-8", errors="replace").strip()
        err = stderr.read().decode("utf-8", errors="replace").strip()
        status = stdout.channel.recv_exit_status()
        if status != 0:
            raise RuntimeError(f"production probe failed ({status}): {err[-4000:]}")
        if not out:
            raise RuntimeError(f"production probe returned no output: {err[-4000:]}")
        payload = json.loads(out.splitlines()[-1])
        if err:
            payload["stderr_tail"] = err[-2000:]
        return payload
    finally:
        cleanup = client.open_sftp()
        try:
            cleanup.remove(remote_path)
        except FileNotFoundError:
            pass
        finally:
            cleanup.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect read-only production acceptance evidence")
    parser.add_argument("--phase", choices=("inventory", "schema", "runtime", "active", "routing", "history-schema", "qmt", "database", "api"), default="inventory")
    parser.add_argument("--output", default="", help="Optional local JSON output path")
    parser.add_argument("--timeout", type=int, default=120)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    probe = {
        "inventory": INVENTORY_PROBE,
        "schema": SCHEMA_PROBE,
        "runtime": RUNTIME_PROBE,
        "active": ACTIVE_PROBE,
        "routing": ROUTING_PROBE,
        "history-schema": HISTORY_SCHEMA_PROBE,
        "qmt": QMT_ACCEPTANCE_PROBE,
        "database": DATABASE_PROBE,
        "api": API_PROBE,
    }[args.phase]
    client = _connect()
    try:
        payload = _run_probe(client, probe, timeout=max(30, args.timeout))
    finally:
        client.close()

    rendered = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    if args.output:
        path = Path(args.output).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

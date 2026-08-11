"""Deploy today's changes to server via paramiko."""
import os
import sys
from pathlib import Path

import paramiko

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_root,
)

LOCAL = os.fspath(ROOT)
LEGACY_DEPLOY_OVERRIDE_ENV = "PROBIGA_ALLOW_LEGACY_DEPLOY"
LEGACY_DEPLOY_OVERRIDE_SENTINEL = (
    "I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES"
)

# Modified files
files = [
    '.env.example',
    'biz/analysis/sync_sim_trade.py',
    'biz/early_briefing/generate.py',
    'biz/evening_review/generate.py',
    'biz/news/sync_news.py',
    'biz/notice/sync_notice_em.py',
    'biz/review/generate.py',
    'biz/sentiment/sync_sentiment.py',
    'biz/stock_finance/sync_finance.py',
    'biz/stock_info/sync_all_code_incremental.py',
    'biz/stock_info/sync_stock_holder.py',
    'biz/stock_info/sync_stock_info.py',
    'biz/stock_market/realtime_quotes.py',
    'biz/stock_market/sina_kline_fetch.py',
    'biz/stock_market/stock_kline_akshare.py',
    'biz/stock_market/sync_stock_market.py',
    'biz/stock_market/sync_stock_snapshot.py',
    'requirements-platform.txt',
    'scripts/sync_realtime_quotes.py',
    'server/api/main.py',
    'server/api/routers/_engine.py',
    'server/api/routers/health.py',
    'server/api/routers/hot_data.py',
    'server/api/routers/scheduler.py',
    'server/api/routers/sim_trade.py',
    'server/api/scheduler_runtime.py',
    'server/common/batch_db.py',
    'server/common/config.py',
    'server/common/mysql_lock.py',
    'server/common/process_env.py',
    'server/common/scheduler_args.py',
    'server/common/scheduler_runner.py',
    'server/common/scheduler_tasks.py',
    'server/common/scheduler_validation.py',
    'server/db/data_integrity.py',
    'server/db/migrations.py',
    'server/engine/sim_trade_engine.py',
    'server/static/js/app.js',
    'tools/crawl_minute_kline.py',
    'tools/crawl_realtime_batch.py',
    'tools/fetch_hot_concept_ths_daily.py',
    'tools/fetch_hot_rank_ths.py',
    'tools/fetch_hot_rank_xq.py',
    'tools/fetch_sm_stock_capital_flow_daily.py',
    'tools/repair_data_integrity.py',
    'tools/run_single_table.py',
    'tools/sync_capital_flow_push2delay.py',
    'tools/sync_concept_ths.py',
]

# New untracked files
new_files = [
    'biz/analysis/sync_analysis_fast.py',
    'tools/data_quality_check.py',
    'tools/ensure_quality_gate.py',
    'tools/fetch_sm_stock_kline_daily.py',
]

all_files = files + new_files


def _require_legacy_deploy_override() -> None:
    if os.environ.get(LEGACY_DEPLOY_OVERRIDE_ENV) != LEGACY_DEPLOY_OVERRIDE_SENTINEL:
        raise SystemExit(
            "Legacy deploy blocked. Set "
            f"{LEGACY_DEPLOY_OVERRIDE_ENV}={LEGACY_DEPLOY_OVERRIDE_SENTINEL} "
            "to override."
        )


def main() -> None:
    _require_legacy_deploy_override()
    remote = remote_root()
    # Connect
    print('Connecting to server...')
    client = production_ssh_client(paramiko)
    sftp = None
    success = 0
    fail = 0
    try:
        client.connect(**production_ssh_connect_kwargs(timeout=30))
        sftp = client.open_sftp()

        # Upload
        for f in all_files:
            local_path = os.path.join(LOCAL, f)
            remote_path = f'{remote}/{f}'
            remote_dir = os.path.dirname(remote_path)
            try:
                # Create remote directory
                stdin, stdout, stderr = client.exec_command(f'mkdir -p {remote_dir}', timeout=30)
                stdout.channel.recv_exit_status()
                # Upload
                sftp.put(local_path, remote_path)
                print(f'  OK: {f}')
                success += 1
            except Exception as e:
                print(f'  FAIL: {f} - {e}')
                fail += 1

        # Install new dependencies
        print('\nInstalling dependencies...')
        stdin, stdout, stderr = client.exec_command(
            f'cd {remote} && source venv/bin/activate && pip install -r requirements-platform.txt -q 2>&1 | tail -5',
            timeout=300,
        )
        print(stdout.read().decode())

        # Restart service
        print('Restarting probiga service...')
        stdin, stdout, stderr = client.exec_command(
            'systemctl restart probiga && sleep 2 && systemctl is-active probiga',
            timeout=60,
        )
        output = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        print(f'Service status: {output}')
        if err:
            print(f'Stderr: {err}')

        # Check status
        stdin, stdout, stderr = client.exec_command(
            'systemctl status probiga --no-pager -l | head -10',
            timeout=30,
        )
        print(stdout.read().decode())
    finally:
        if sftp is not None:
            sftp.close()
        client.close()
    print(f'\nDone! Uploaded {success} files, {fail} failed.')


if __name__ == "__main__":
    main()

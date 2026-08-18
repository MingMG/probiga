from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_layer4_workflow_is_manual_protected_and_sha_pinned() -> None:
    path = ROOT / ".github" / "workflows" / "layer4-maintenance.yml"
    workflow = path.read_text(encoding="utf-8")
    assert "  group: probiga-production-deploy" in workflow
    assert "workflow_dispatch:" in workflow
    assert "\n  push:" not in workflow
    assert "\n  pull_request:" not in workflow
    assert 'test "$REQUESTED_SHA" = "$GITHUB_SHA"' in workflow
    assert 'test "$GITHUB_REF" = refs/heads/main' in workflow
    assert "actions/checkout@v4" not in workflow
    assert "appleboy/ssh-action@v1.0.3" not in workflow
    assert "environment: production" in workflow
    assert "SERVER_HOST_FINGERPRINT" in workflow
    assert 'test "$SERVER_USER" != root' in workflow
    assert 'active_code="/opt/ProBigA-releases/$PROBIGA_EXPECTED_GIT_SHA"' in workflow
    assert "test -L /opt/ProBigA-current" in workflow
    assert (
        'test "$(readlink -f /opt/ProBigA-current)" = "$active_code"'
        in workflow
    )
    assert "exec sudo -n /usr/bin/env --" in workflow
    assert "cd /opt/ProBigA" not in workflow


def test_layer4_workflow_has_separate_migrate_recovery_activation_acks() -> None:
    workflow = (
        ROOT / ".github" / "workflows" / "layer4-maintenance.yml"
    ).read_text(encoding="utf-8")
    assert "I_CONFIRM_LAYER4_PRODUCTION_MIGRATION" in workflow
    assert "I_CONFIRM_LAYER4_FORWARD_RECOVERY" in workflow
    assert "I_CONFIRM_LAYER4_SHADOW_WRITERS_ACTIVATION" in workflow
    assert "activate:true" not in workflow
    assert "register" not in workflow.casefold()
    assert "pin-model" not in workflow.casefold()


def test_remote_maintenance_orders_every_fail_closed_gate_before_apply() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    fence = script.index("tools/add_trading_v3_tasks.py --fence-only")
    stop = script.index("sudo systemctl disable --now probiga-scheduler", fence)
    heartbeat = script.index("wait-writers", stop)
    hold = script.index("hold-lock", heartbeat)
    audit = script.index("\ndba_audit\n", hold)
    backup = script.index("--no-data --routines --events --triggers", audit)
    plan = script.index("PLAN_ARGS=(migration-plan)", backup)
    second_audit = script.index("\n  dba_audit\n", plan)
    apply = script.index("tools/migrate_trading_v3.py \\", second_audit)
    verify = script.index("verify-migrations", apply)
    stage = script.index("tools/add_trading_v3_tasks.py --writer-fence", verify)
    restart = script.index("sudo systemctl start probiga", stage)
    assert fence < stop < heartbeat < hold < audit < backup
    assert backup < plan < second_audit < apply < verify < stage < restart


def test_remote_maintenance_has_dba_backup_receipt_and_recovery_contracts() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    assert "information_schema.innodb_trx" in script
    assert "performance_schema.metadata_locks" in script
    assert "IS_USED_LOCK('probiga:trading_v3:maintenance')" in script
    assert "IS_USED_LOCK('probiga:trading_v3_schema')" in script
    assert "sudo -n \"$MYSQL_BIN\"" in script
    assert "sudo -n \"$MYSQLDUMP_BIN\"" in script
    assert "--result-file=\"$BACKUP_FILE\"" in script
    assert "BACKUP_SHA256" in script
    assert "probiga.layer4-maintenance-receipt.v1" in script
    assert 'return 2' in script[script.index("die() {") : script.index("[[ \"$EXPECTED_SHA\"")]
    recovery = script[
        script.index("failure_recovery() {") :
        script.index("trap 'failure_recovery $?' ERR")
    ]
    assert recovery.index("--fence-only") < recovery.index(
        "release_maintenance_lock"
    )
    assert "FORWARD_RECOVERY_REQUIRED" in script
    assert "--fence-only" in script
    assert "model_gate_modified\": False" in script
    assert "order_authority\": False" in script
    assert "down migration" in script


def test_remote_maintenance_shares_deploy_lock_and_immutable_runtime() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    assert "CODE_RELEASE_ROOT=/opt/ProBigA-releases" in script
    assert "CURRENT_RELEASE_LINK=/opt/ProBigA-current" in script
    assert "RELEASE_VENV_ROOT=/var/lib/probiga/release-venvs" in script
    assert "DEPLOY_LOCK_ROOT=/run/probiga" in script
    assert (
        'DEPLOY_LOCK_FILE="$DEPLOY_LOCK_ROOT/production-deploy.lock"'
        in script
    )
    assert "exec 9>\"$DEPLOY_LOCK_FILE\"" in script
    assert "flock -n 9" in script
    assert "root:root:600" in script
    assert ".probiga_deploy_lock" not in script
    assert 'PROBIGA_CODE_ROOT="$ROOT"' in script
    assert 'PYTHONPATH="$ADATA_SOURCE:$ROOT"' in script
    assert '"$RELEASE_VENV/bin/python" -P "$ROOT/$entrypoint"' in script
    assert '"$ROOT/.release_venvs' not in script


def test_remote_maintenance_never_lifts_model_or_order_gates() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    lowered = script.casefold()
    assert "register_horizon" not in lowered
    assert "pin_horizon" not in lowered
    assert "order_authority\": true" not in lowered
    assert "real_order_allowed\": true" not in lowered
    assert "tools/add_trading_v3_tasks.py --activate-layer4" in script

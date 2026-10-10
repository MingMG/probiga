from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_layer4_github_workflow_is_retired() -> None:
    path = ROOT / ".github" / "workflows" / "layer4-maintenance.yml"
    assert not path.exists()
    assert not (ROOT / ".github" / "workflows" / "deploy.yml").exists()


def test_layer4_manual_script_is_activation_only() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    assert "I_CONFIRM_LAYER4_SHADOW_WRITERS_ACTIVATION" in script
    assert "I_CONFIRM_LAYER4_PRODUCTION_MIGRATION" not in script
    assert "I_CONFIRM_LAYER4_FORWARD_RECOVERY" not in script
    assert 'test "$PHASE" = activate' in script
    assert "schema migration is owned by the production deploy pipeline" in script
    assert "activate:true" not in script
    assert "register_horizon" not in script.casefold()
    assert "pin_horizon" not in script.casefold()


def test_remote_activation_orders_every_fail_closed_gate_before_apply() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    topology = script.index("assert-exclusive-writer")
    fence = script.index("tools/add_trading_v3_tasks.py --fence-only", topology)
    fence_state = script.index("--expected fenced", fence)
    stop = script.index("sudo systemctl disable --now probiga-scheduler", fence_state)
    heartbeat = script.index("wait-writers", stop)
    hold = script.index("hold-lock", heartbeat)
    verify = script.index("verify-migrations", hold)
    activate = script.index("tools/add_trading_v3_tasks.py --activate-layer4", verify)
    state = script.index("--expected enabled", activate)
    restart = script.index("sudo systemctl start probiga", state)
    final_topology = script.index("assert-exclusive-writer", restart)
    assert topology < fence < fence_state < stop < heartbeat < hold < verify
    assert verify < activate < state < restart < final_topology


def test_remote_activation_has_single_schema_owner_and_recovery_contracts() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    assert "information_schema.innodb_trx" not in script
    assert "performance_schema.metadata_locks" not in script
    assert "MYSQL_BIN" not in script
    assert "MYSQLDUMP_BIN" not in script
    assert "tools/migrate_trading_v3.py" not in script
    assert "--no-data" not in script
    assert "probiga.layer4-activation-receipt.v2" in script
    assert 'return 2' in script[script.index("die() {") : script.index("[[ \"$EXPECTED_SHA\"")]
    recovery = script[
        script.index("failure_recovery() {") :
        script.index("trap 'failure_recovery $?' ERR")
    ]
    assert recovery.index("--fence-only") < recovery.index(
        "release_maintenance_lock"
    )
    assert recovery.index("--expected fenced") < recovery.index(
        "release_maintenance_lock"
    )
    assert 'recovery_fence_succeeded=1' in recovery
    assert '[ "$recovery_fence_succeeded" -eq 1 ]' in recovery
    assert "WRITER_EXECUTION_BLOCKED_ON_FAILURE=1" in recovery
    assert "disable --now probiga-scheduler" in recovery
    assert "TASK_FENCE_RECOVERY_REQUIRED" in recovery
    assert (
        'elif [ "$WRITER_EXECUTION_BLOCKED_ON_FAILURE" -ne 1 ]'
        in recovery
    )
    assert "SERVICE_RECOVERY_REQUIRED" in script
    assert '"$SERVICES_STOPPED" -eq 1 ]' in script
    assert "sudo systemctl enable --now probiga" in recovery
    assert "sudo systemctl enable --now probiga-scheduler" in recovery
    assert "--fence-only" in script
    assert "model_gate_modified\": False" in script
    assert "order_authority\": False" in script
    assert "production_deploy.sh" in script
    assert "migration-plan" not in script


def test_remote_maintenance_shares_deploy_lock_and_immutable_runtime() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    assert "CODE_RELEASE_ROOT=/opt/ProBigA-releases" in script
    assert "CURRENT_RELEASE_LINK=/opt/ProBigA-current" in script
    assert "RELEASE_VENV_ROOT=/var/lib/probiga/release-venvs" in script
    assert "ADATA_RUNTIME_ROOT=/var/lib/probiga/release-sources/adata" in script
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
    assert 'ADATA_SOURCE="$ADATA_RUNTIME_ROOT/$ADATA_SHA-$ADATA_TREE_SHA256"' in script
    assert 'a.get("source_dir")' not in script
    assert '"$ADATA_SOURCE/.probiga-adata.gitsha"' in script
    assert '"$ADATA_SOURCE/.probiga-adata.tree.sha256"' in script
    assert 'sudo -u "$SERVICE_USER" test ! -w "$ADATA_SOURCE"' in script
    assert 'PYTHONPATH="$ADATA_SOURCE:$ROOT"' in script
    assert '"$RELEASE_VENV/bin/python" -P "$ROOT/$entrypoint"' in script
    assert '"$ROOT/.release_venvs' not in script


def test_remote_maintenance_gives_only_lock_ipc_to_service_user() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    assert 'LOCK_IPC_DIR="$RUN_DIR/maintenance-lock-ipc"' in script
    assert 'READY_FILE="$LOCK_IPC_DIR/maintenance-lock.ready.json"' in script
    assert 'RELEASE_FILE="$LOCK_IPC_DIR/maintenance-lock.release"' in script
    assert 'chown root:"$SERVICE_GROUP" "$RUN_DIR"' in script
    assert 'chmod 0710 "$RUN_DIR"' in script
    assert '"root:$SERVICE_GROUP:710"' in script
    install = script.index(
        'install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0700 '
        '"$LOCK_IPC_DIR"'
    )
    hold = script.index("hold-lock", install)
    assert install < hold
    assert 'stat -c \'%U:%G:%a\' "$LOCK_IPC_DIR"' in script
    assert '"$SERVICE_USER:$SERVICE_GROUP:700"' in script
    assert 'chown -R "$SERVICE_USER" "$RUN_DIR"' not in script


def test_remote_maintenance_bounds_full_deep_health_retries() -> None:
    script = (ROOT / "deploy" / "layer4_maintenance.sh").read_text(
        encoding="utf-8"
    )
    assert "HEALTH_ATTEMPT_TIMEOUT_SECONDS=120" in script
    assert "HEALTH_RETRY_MAX_SECONDS=360" in script
    assert "HEALTH_RETRY_DELAY_SECONDS=2" in script
    assert script.count('HEALTH_JSON="$(read_deep_health)"') == 2
    health_helper = script[
        script.index("read_deep_health() {") :
        script.index('HEALTH_JSON="$(read_deep_health)"')
    ]
    assert "deadline=$((SECONDS + HEALTH_RETRY_MAX_SECONDS))" in health_helper
    assert "while (( (remaining = deadline - SECONDS) > 0 )); do" in health_helper
    assert 'attempt_timeout="$HEALTH_ATTEMPT_TIMEOUT_SECONDS"' in health_helper
    assert '--max-time "$attempt_timeout"' in health_helper
    assert 'sleep "$HEALTH_RETRY_DELAY_SECONDS"' in health_helper
    assert "--retry " not in health_helper
    assert "return \"$last_status\"" in health_helper
    assert "http://127.0.0.1/api/health" in health_helper


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

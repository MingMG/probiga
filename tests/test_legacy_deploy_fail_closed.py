from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEPLOY_ROOT = ROOT / "deploy"
OVERRIDE_ENV = "PROBIGA_ALLOW_LEGACY_DEPLOY"
OVERRIDE_SENTINEL = "I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES"

LEGACY_ENTRYPOINTS = (
    "deploy.bat",
    "deploy.ps1",
    "deploy.sh",
    "deploy_today.py",
    "publish_to_cloud.bat",
    "restart_probiga.sh",
    "upload_and_restart.bat",
    "upload_and_restart.ps1",
    "upload_capital_flow_fixes.py",
    "upload_portfolio_profit_fix.bat",
    "upload_portfolio_profit_fix.ps1",
)

LEGACY_TOOL_ENTRYPOINTS = (
    "tools/_upload_sync_sm.py",
    "tools/_run_kline_incremental.py",
    "tools/_kill_and_restart.py",
    "tools/_run_plate_sync.py",
)

ALL_LEGACY_ENTRYPOINTS = tuple(
    f"deploy/{name}" for name in LEGACY_ENTRYPOINTS
) + LEGACY_TOOL_ENTRYPOINTS

PARAMIKO_ENTRYPOINTS = (
    "deploy/deploy_today.py",
    "deploy/upload_capital_flow_fixes.py",
    *LEGACY_TOOL_ENTRYPOINTS,
)

STRICT_PRODUCTION_PARAMIKO_ENTRYPOINTS = PARAMIKO_ENTRYPOINTS + (
    "tools/_fix_kline_run.py",
    "tools/_install_akshare.py",
    "tools/_kill_all_sync.py",
    "tools/_kill_and_check.py",
    "tools/_kill_and_wait.py",
    "tools/_restart_probiga.py",
    "tools/_run_akshare_fill.py",
    "tools/_ssh_runner.py",
    "tools/_start_concept_east_sync.py",
    "tools/_trigger_concept_sync.py",
    "tools/_verify_fused_api.py",
    "tools/production_acceptance_audit.py",
    "tools/promote_etf_forward_to_production.py",
    "tools/promote_etf_history_to_production.py",
    "tools/promote_qmt_membership_to_production.py",
    "tools/provision_ai_bridge_token.py",
    "tools/run_production_acceptance_job.py",
    "tools/run_remote_qmt_tunnel.py",
    "tools/verify_trading_v3_production.py",
)

DATABASE_AUTOADD_EXEMPTIONS = {
    "tools/_fix_notice_ddl.py",
    "tools/run_remote_mysql_tunnel.py",
    "tools/verify_real_trading_database_guards.py",
}

REMOTE_SHELL_DEPLOY_ENTRYPOINTS = (
    "deploy/deploy.ps1",
    "deploy/publish_to_cloud.bat",
    "deploy/upload_and_restart.bat",
    "deploy/upload_and_restart.ps1",
    "deploy/upload_portfolio_profit_fix.bat",
    "deploy/upload_portfolio_profit_fix.ps1",
)

FIRST_REMOTE_OR_MUTATING_TOKEN = {
    ".bat": ("powershell ", "tar ", "scp ", "ssh "),
    ".ps1": ("tar ", "scp ", "ssh "),
    ".sh": ("apt ", "yum ", "systemctl "),
    ".py": (" = production_ssh_client(",),
}

FAIL_CLOSED_TOKEN = {
    ".bat": "if not ",
    ".ps1": " -cne ",
    ".sh": " != ",
    ".py": " != legacy_deploy_override_sentinel",
}

STOP_TOKEN = {
    ".bat": "exit /b 64",
    ".ps1": "throw ",
    ".sh": "exit 64",
    ".py": "raise systemexit(",
}


def _source(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8").lower()


def test_every_legacy_deploy_or_upload_entrypoint_is_listed() -> None:
    discovered = {
        path.name
        for path in DEPLOY_ROOT.iterdir()
        if path.is_file()
        and path.suffix.lower() in {".bat", ".ps1", ".py", ".sh"}
        and path.name.startswith(("deploy", "publish", "restart", "upload"))
    }

    assert discovered == set(LEGACY_ENTRYPOINTS)


def test_legacy_entrypoints_require_exact_override_before_side_effects() -> None:
    for name in ALL_LEGACY_ENTRYPOINTS:
        source = _source(name)
        suffix = Path(name).suffix.lower()
        guard_at = source.index(OVERRIDE_SENTINEL.lower())

        assert OVERRIDE_ENV.lower() in source, name
        assert "legacy deploy blocked" in source, name
        assert FAIL_CLOSED_TOKEN[suffix] in source, name
        assert STOP_TOKEN[suffix] in source, name

        side_effect_positions = [
            source.index(token)
            for token in FIRST_REMOTE_OR_MUTATING_TOKEN[suffix]
            if token in source
        ]
        assert side_effect_positions, name
        assert guard_at < min(side_effect_positions), name


def test_paramiko_entrypoints_reject_unknown_host_keys() -> None:
    for name in PARAMIKO_ENTRYPOINTS:
        source = _source(name)
        main_source = source[source.index("def main()") :]

        assert "autoaddpolicy" not in source, name
        assert "production_ssh_client" in source, name
        assert "production_ssh_connect_kwargs" in source, name
        assert main_source.index("_require_legacy_deploy_override()") < (
            main_source.index("production_ssh_client(")
        ), name


def test_non_database_production_paramiko_entrypoints_use_strict_helper() -> None:
    for name in STRICT_PRODUCTION_PARAMIKO_ENTRYPOINTS:
        source = _source(name)
        assert "autoaddpolicy" not in source, name
        assert "production_ssh_client" in source, name
        assert "production_ssh_connect_kwargs" in source, name


def test_autoadd_policy_is_limited_to_deferred_database_tools() -> None:
    discovered = {
        path.relative_to(ROOT).as_posix()
        for base in (ROOT / "tools", ROOT / "deploy")
        for path in base.rglob("*.py")
        if "autoaddpolicy" in path.read_text(encoding="utf-8").lower()
    }
    assert discovered == DATABASE_AUTOADD_EXEMPTIONS


def test_remote_shell_deploys_require_named_non_root_user() -> None:
    for name in REMOTE_SHELL_DEPLOY_ENTRYPOINTS:
        source = _source(name)
        assert "root production deploy is forbidden" in source, name
        assert "remote_ssh_user=root" not in source, name
        assert 'else { "root" }' not in source, name
        assert "probiga_ssh_known_hosts" in source, name
        assert "probiga_remote_ssh_key_file" in source, name
        assert "stricthostkeychecking=yes" in source, name
        assert "passwordauthentication=no" in source, name
        assert "batchmode=yes" in source, name


def test_legacy_code_deploy_never_uploads_runtime_cache() -> None:
    assert "data/east_sector_heat_cache.json" not in _source(
        "deploy/deploy_today.py"
    )


from __future__ import annotations

import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT = ROOT / "deploy" / "production_deploy.sh"


def _normalized_shell(source: str) -> str:
    """Join shell continuations so fail-closed checks can be matched exactly."""

    return re.sub(r"[ \t]*\\\r?\n[ \t]*", " ", source)


def _function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?ms)^{re.escape(name)}\(\) \{{\n(?P<body>.*?)^[ \t]*\}}\s*$",
        source,
    )
    assert match is not None, f"missing shell function: {name}"
    return _normalized_shell(match.group("body"))


def _runtime_verifier() -> str:
    return _function_body(
        DEPLOY_SCRIPT.read_text(encoding="utf-8"),
        "controlled_guard_verify_restored_runtime",
    )


def _assert_exact_process_environment_check(block: str, identity: str) -> None:
    expected = (
        f'grep -zFx -- "{identity}" "/proc/$pid/environ" >/dev/null || return 1'
    )
    assert expected in block, f"missing fail-closed process identity check: {identity}"


def _assert_exact_unit_exec_start_check(
    block: str,
    *,
    exec_start_variable: str,
    identity: str,
) -> None:
    expected = (
        f"printf '%s' \"${exec_start_variable}\" | grep -F -- "
        f'"{identity}" >/dev/null || return 1'
    )
    assert expected in block, f"missing fail-closed unit identity check: {identity}"


def _assert_fail_closed_identity_loop(
    block: str,
    *,
    identity_variable: str,
    identity: str,
    source: str | None,
    exact: bool,
) -> None:
    assert f'"{identity}"' in block
    grep_mode = "-zFx" if exact else "-F"
    source_argument = "" if source is None else f' "{source}"'
    expected = (
        f'grep {grep_mode} -- "${identity_variable}"{source_argument} '
        ">/dev/null || return 1"
    )
    assert expected in block, f"identity loop is not fail-closed for: {identity}"


def test_deferred_runtime_allowlist_comes_from_active_main_process_attestation() -> None:
    body = _runtime_verifier()
    main_process = body.index('if [ "$main_active" = active ]; then')
    deferred_attestation = body.index(
        'if [ "$main_active" = active ] && grep -zFx -- '
        "'PROBIGA_STRATEGY_GOVERNANCE_MODE=DEFERRED_DB' "
        '"/proc/$main_pid/environ" >/dev/null; then'
    )
    scheduler_identity = body.index(
        'scheduler_exec_start="$(systemctl show -p ExecStart --value '
        'probiga-scheduler)"'
    )

    assert main_process < deferred_attestation < scheduler_identity
    attestation = body[deferred_attestation:scheduler_identity]
    for proof in (
        "PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=//p",
        "PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT=//p",
        'test "$deferred_expected_sha" != "$expected_sha" || return 1',
        'test "$deferred_code_root" = "$CODE_RELEASE_ROOT/$deferred_expected_sha" || return 1',
        'git -C "$deferred_code_root" rev-parse HEAD',
        'test "$(stat -c \'%U:%G\' "$deferred_code_root")" = root:root || return 1',
        'test "$(<"$deferred_venv/.probiga.gitsha")" = "$deferred_expected_sha" || return 1',
        ".release-tree.sha256",
        ".adapter-registry-seal.sha256",
    ):
        assert proof in attestation

    # Merely restoring an inactive API must never authorize an older auxiliary
    # runtime: the allowlist exists only when the live API process attests it.
    inactive_main = body[body.index("else", main_process):deferred_attestation]
    assert 'main_pid="$pid"' not in inactive_main
    assert 'PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=//p' not in inactive_main


def test_inactive_scheduler_unit_is_attested_before_active_state_branch() -> None:
    body = _runtime_verifier()
    scheduler_loaded = body.index('if [ "$scheduler_load" = loaded ]; then')
    scheduler_exec_start = body.index(
        'scheduler_exec_start="$(systemctl show -p ExecStart --value '
        'probiga-scheduler)"',
        scheduler_loaded,
    )
    scheduler_active = body.index(
        'if [ "$scheduler_active" = active ]; then',
        scheduler_exec_start,
    )
    scheduler_inactive = body.index(
        'test "$scheduler_active" = inactive || return 1',
        scheduler_active,
    )

    assert scheduler_loaded < scheduler_exec_start < scheduler_active < scheduler_inactive
    unit_attestation = body[scheduler_exec_start:scheduler_active]
    for accepted_command in (
        '"$python_path -P $code_root/tools/run_scheduler_daemon.py"',
        '"$deferred_python_path -P '
        '$deferred_code_root/tools/run_scheduler_daemon.py"',
    ):
        assert accepted_command in unit_attestation
    for identity in (
        "PROBIGA_EXPECTED_GIT_SHA=$scheduler_expected_sha",
        "PROBIGA_CODE_ROOT=$scheduler_code_root",
        "PROBIGA_RELEASE_TREE_SHA256=$scheduler_release_tree_sha",
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="
        "$scheduler_adapter_registry_seal_sha",
        "PYTHONPATH=$scheduler_adata_source:$scheduler_code_root",
    ):
        _assert_fail_closed_identity_loop(
            unit_attestation,
            identity_variable="scheduler_identity",
            identity=identity,
            source=None,
            exact=False,
        )


@pytest.mark.parametrize(
    "identity",
    (
        "PROBIGA_EXPECTED_GIT_SHA=$scheduler_expected_sha",
        "PROBIGA_CODE_ROOT=$scheduler_code_root",
        "PROBIGA_RELEASE_TREE_SHA256=$scheduler_release_tree_sha",
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="
        "$scheduler_adapter_registry_seal_sha",
        "PYTHONPATH=$scheduler_adata_source:$scheduler_code_root",
    ),
)
def test_active_scheduler_rejects_each_process_identity_mismatch(identity: str) -> None:
    body = _runtime_verifier()
    scheduler_active = body.index('if [ "$scheduler_active" = active ]; then')
    ai_service = body.index('if [ "$ai_service_load" = loaded ]; then')
    active_process = body[scheduler_active:ai_service]

    _assert_fail_closed_identity_loop(
        active_process,
        identity_variable="scheduler_identity",
        identity=identity,
        source="/proc/$pid/environ",
        exact=True,
    )


@pytest.mark.parametrize(
    "identity",
    (
        "PROBIGA_EXPECTED_GIT_SHA=$ai_expected_sha",
        "PROBIGA_CODE_ROOT=$ai_code_root",
        "PROBIGA_RELEASE_TREE_SHA256=$ai_release_tree_sha",
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="
        "$ai_adapter_registry_seal_sha",
        "PYTHONPATH=$ai_adata_source:$ai_code_root",
    ),
)
def test_ai_unit_rejects_each_configured_identity_mismatch(identity: str) -> None:
    body = _runtime_verifier()
    ai_service = body.index('if [ "$ai_service_load" = loaded ]; then')
    ai_active = body.index('if [ "$ai_service_active" = active ]; then', ai_service)
    unit_attestation = body[ai_service:ai_active]

    _assert_exact_unit_exec_start_check(
        unit_attestation,
        exec_start_variable="ai_exec_start",
        identity=identity,
    )


@pytest.mark.parametrize(
    "identity",
    (
        "PROBIGA_EXPECTED_GIT_SHA=$ai_expected_sha",
        "PROBIGA_CODE_ROOT=$ai_code_root",
        "PROBIGA_RELEASE_TREE_SHA256=$ai_release_tree_sha",
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256="
        "$ai_adapter_registry_seal_sha",
        "PYTHONPATH=$ai_adata_source:$ai_code_root",
    ),
)
def test_active_ai_worker_rejects_each_process_identity_mismatch(identity: str) -> None:
    body = _runtime_verifier()
    ai_active = body.index('if [ "$ai_service_active" = active ]; then')
    ai_inactive = body.index(
        'test "$ai_service_active" = inactive || return 1',
        ai_active,
    )
    active_process = body[ai_active:ai_inactive]

    _assert_exact_process_environment_check(active_process, identity)

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_workflow_uses_fixed_root_owned_deploy_broker() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
        encoding="utf-8"
    )

    assert "sudo -n /usr/local/sbin/probiga-production-deploy" in workflow
    assert "bash \"$DEPLOY_BOOTSTRAP\"" not in workflow


def test_broker_restricts_caller_remote_and_revision() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )

    assert "SUDO_USER" in broker
    assert "probiga-deploy" in broker
    assert "https://github.com/MingMG/probiga.git" in broker
    assert "refs/heads/main" in broker
    assert "requested revision is not the current trusted main revision" in broker
    assert "safe.directory" in broker
    assert "resolved requirements digest differs" in broker

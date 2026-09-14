"""Execute the deployed resource policy, including effective-value rejection."""
import subprocess

import pytest

from test_production_deploy_recovery_state_machine import (
    ROOT, _bash, _function, _shell_function_bodies,
)


def test_scheduler_resources_escape_reclaiming_ancestor_and_check_effective_limits(tmp_path):
    bash = _bash()
    if not bash:
        pytest.skip("bash is required")
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = _shell_function_bodies(source)
    script = "set -eu\n" + "\n".join(
        _function(name, bodies[name])
        for name in ("write_scheduler_resources", "assert_scheduler_resources")
    ) + r'''
write_scheduler_resources resources.conf
grep -Fx 'Slice=system.slice' resources.conf
grep -Fx 'MemoryHigh=1792M' resources.conf
grep -Fx 'MemoryMax=2048M' resources.conf
grep -Fx 'MemorySwapMax=512M' resources.conf
slice=system.slice
high=1879048192
max=2147483648
swap=536870912
systemctl() {
  case "$*" in
    *--property=Slice*) echo "$slice";;
    *--property=MemoryHigh*) echo "$high";;
    *--property=MemoryMax*) echo "$max";;
    *--property=MemorySwapMax*) echo "$swap";;
    *) return 99;;
  esac
}
assert_scheduler_resources
slice=probiga-heavy.slice
if assert_scheduler_resources; then exit 10; fi
slice=system.slice
high=943718400
if assert_scheduler_resources; then exit 11; fi
high=1879048192
max=infinity
if assert_scheduler_resources; then exit 12; fi
max=2147483648
swap=infinity
if assert_scheduler_resources; then exit 13; fi
'''
    path = tmp_path / "check.sh"
    path.write_text(script, encoding="utf-8", newline="\n")
    result = subprocess.run([bash, str(path)], cwd=tmp_path, capture_output=True,
                            text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_resource_file_is_in_existing_activation_transaction():
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    broker = (ROOT / "deploy/production_deploy_root.sh").read_text(encoding="utf-8")
    path = "/etc/systemd/system/probiga-scheduler.service.d/release.conf"
    for script in (source, broker):
        paths = script.split("ACTIVATION_UNIT_PATHS=(", 1)[1].split(")", 1)[0]
        assert path in paths
    bodies = _shell_function_bodies(source)
    assert 'source="$PREPARED_SCHEDULER_RESOURCES"' in bodies["activation_snapshot_append_new_record"]
    assert '"$SCHEDULER_RESOURCE_DROPIN"' in bodies["install_prepared_dropins"]
    assert 'assert_scheduler_resources || return 1' in bodies["prepared_active_runtime_matches_current_request"]

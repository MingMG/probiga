"""Actual embedded broker policy against private POSIX inode/permission trees.

No database, package install, service identity switch or production state is
needed. Race injections perform real rename/link/write operations before calling
the original syscall; they never supply invented stat results.
"""

import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SUBTREE = "exposure-event-risk-watch-v1"
BATCH = "exposure-risk-" + "a" * 48


def _body(name):
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    return source.split(name + "() {", 1)[1].split("\n}\n", 1)[0]


def _script(name):
    return re.search(r"<<'PY'[^\n]*\n(.*?)\nPY", _body(name), re.S).group(1)


class BrokerExposurePolicyWiring(unittest.TestCase):
    def test_one_validator_is_bounded_and_wired_to_live_and_quiescent_passes(self):
        compile(_script("validate_exposure_risk_job_checkpoint_tree"), "broker-checkpoints", "exec")
        prepare = _body("prepare_probiga_job_log_root")
        migrate = _body("migrate_probiga_job_log_legacy_modes")
        self.assertEqual(prepare.count("validate_exposure_risk_job_checkpoint_tree live || return 2"), 2)
        self.assertEqual(migrate.count("validate_exposure_risk_job_checkpoint_tree quiescent || return 2"), 2)
        self.assertIn("--kill-after=2s 30s", _body("validate_exposure_risk_job_checkpoint_tree"))
        self.assertIn("stage=exposure_risk_checkpoint_validation", _body("validate_exposure_risk_job_checkpoint_tree"))


@unittest.skipUnless(os.name == "posix" and os.open in os.supports_dir_fd,
                     "Linux nofollow/dirfd/uid/gid acceptance requires POSIX")
class BrokerExposurePhysicalPolicy(unittest.TestCase):
    def setUp(self):
        self.private = tempfile.TemporaryDirectory(prefix="probiga-risk-policy-")
        self.base = Path(self.private.name)
        self.jobs = self.base / "jobs"
        self.jobs.mkdir(mode=0o700)
        self.risk = self.jobs / SUBTREE
        self.risk.mkdir(mode=0o755)
        self.batch = self.risk / BATCH
        self.batch.mkdir(mode=0o755)
        self.results = self.batch / "results"
        self.results.mkdir(mode=0o755)
        self._file(self.batch / "manifest.json", b'{"partial-original')
        self._file(self.batch / "prepared.json", b"")
        self._file(self.batch / "complete.json", b"not-a-business-success")
        self._file(self.results / "000001.json", b"original-result")
        self.external = self.base / "external"
        self._file(self.external, b"must not follow or modify")

    def tearDown(self):
        self.private.cleanup()

    def _file(self, path, content=b"interrupted checkpoint"):
        path.write_bytes(content)
        path.chmod(0o600)
        return path

    def _run(self, phase="quiescent", *, script=None, injection=""):
        source = script or _script("validate_exposure_risk_job_checkpoint_tree")
        if injection:
            source = injection + "\nexec(compile(" + repr(source) + ", 'actual-broker-validator', 'exec'))\n"
        arguments = [sys.executable, "-I", "-", str(self.jobs), str(os.geteuid()), str(os.getegid())]
        # The existing flat-log passes accept three arguments, while the
        # fixed subtree validator additionally binds its operational phase.
        if script is None:
            arguments.append(phase)
        return subprocess.run(
            arguments,
            input=source, text=True, capture_output=True, timeout=5,
        )

    def _accepted(self, phase="quiescent", **kwargs):
        result = self._run(phase, **kwargs)
        self.assertEqual(result.returncode, 0, result.stderr)

    def _rejected(self, **kwargs):
        for phase in ("live", "quiescent"):
            with self.subTest(phase=phase):
                result = self._run(phase, **kwargs)
                self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.external.read_bytes(), b"must not follow or modify")

    def test_exact_partial_files_and_strict_killed_writer_temps_are_preserved(self):
        self._file(self.batch / ".manifest.json.abc_1234.tmp", b"partial manifest")
        self._file(self.batch / ".prepared.json.z1234567.tmp", b"")
        self._file(self.results / ".600000.json.a1b2c3d4.tmp", b"partial result")
        original = {path.relative_to(self.jobs).as_posix():
                    (path.read_bytes(), path.stat().st_ino, path.stat().st_mode)
                    for path in self.jobs.rglob("*") if path.is_file()}
        self._accepted("live")
        self._accepted("quiescent")
        current = {path.relative_to(self.jobs).as_posix():
                   (path.read_bytes(), path.stat().st_ino, path.stat().st_mode)
                   for path in self.jobs.rglob("*") if path.is_file()}
        self.assertEqual(current, original)

    def test_absent_and_empty_and_creation_with_only_valid_temp(self):
        for path in self.results.iterdir():
            path.unlink()
        self.results.rmdir()
        for path in self.batch.iterdir():
            path.unlink()
        self.batch.rmdir()
        self._accepted()
        self.batch.mkdir(mode=0o700)
        self._file(self.batch / ".manifest.json.12345678.tmp")
        self._accepted()
        (self.batch / ".manifest.json.12345678.tmp").unlink()
        self.batch.rmdir()
        self.risk.rmdir()
        self._accepted()

    def test_formal_directory_modes_allowed_but_no_group_or_other_write(self):
        for mode in (0o700, 0o750, 0o755):
            with self.subTest(mode=oct(mode)):
                for path in (self.risk, self.batch, self.results):
                    path.chmod(mode)
                self._accepted()
        for path in (self.risk, self.batch, self.results):
            for mode in (0o777, 0o775, 0o707, 0o500):
                path.chmod(mode)
                self._rejected()
                path.chmod(0o755)

    def test_unknown_batch_files_and_general_announcement_features_not_allowed(self):
        for parent, name in (
            (self.risk, "exposure-risk-" + "b" * 47),
            (self.risk, "exposure-risk-" + "B" * 48),
            (self.batch, "failure.json"),
            (self.batch, "invalidated-results"),
            (self.batch, "arbitrary.json"),
            (self.batch, ".manifest.json.ABC12345.tmp"),
            (self.batch, ".manifest.json.abc1234.tmp"),
            (self.results, "not-a-stock.json"),
            (self.results, ".000001.json.abc12345.json"),
            (self.results, "000001.reason.123456789abcdef0.json"),
        ):
            with self.subTest(name=name):
                unknown = self._file(parent / name)
                self._rejected()
                unknown.unlink()

    def test_nofollow_links_hardlinks_fifo_and_wrong_file_mode_rejected(self):
        checkpoint = self.results / "000001.json"
        checkpoint.chmod(0o644)
        self._rejected()
        checkpoint.unlink()
        checkpoint.symlink_to(self.external)
        self._rejected()
        checkpoint.unlink()
        os.link(self.external, checkpoint)
        self._rejected()
        checkpoint.unlink()
        os.mkfifo(checkpoint, mode=0o600)
        self._rejected()
        checkpoint.unlink()
        checkpoint.mkdir(mode=0o700)
        self._rejected()
        checkpoint.rmdir()
        self.results.rmdir()
        self.results.symlink_to(self.base, target_is_directory=True)
        self._rejected()

    def test_expected_owner_and_group_drift_rejected(self):
        # No root privilege needed: pass a deliberately wrong expected identity
        # while the actual inode retains the caller's uid/gid.
        source = _script("validate_exposure_risk_job_checkpoint_tree")
        for uid, gid in ((os.geteuid() + 1, os.getegid()), (os.geteuid(), os.getegid() + 1)):
            result = subprocess.run(
                [sys.executable, "-I", "-", str(self.jobs), str(uid), str(gid), "quiescent"],
                input=source, text=True, capture_output=True, timeout=5,
            )
            self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.external.read_bytes(), b"must not follow or modify")

    def test_real_atomic_file_replace_live_allowed_quiescent_rejected(self):
        injection = """
import os
original_open = os.open
replaced = False
def real_open_with_one_atomic_replace(name, flags, mode=0o777, *, dir_fd=None):
    global replaced
    if name == '000001.json' and dir_fd is not None and not replaced:
        replaced = True
        temporary = '.000001.json.abc12345.tmp'
        writer = original_open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600, dir_fd=dir_fd)
        os.write(writer, b'new real checkpoint inode')
        os.fsync(writer)
        os.close(writer)
        os.replace(temporary, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
    return original_open(name, flags, mode, dir_fd=dir_fd)
os.open = real_open_with_one_atomic_replace
"""
        self._accepted("live", injection=injection)
        result = self._run("quiescent", injection=injection)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("changed while quiescent", result.stderr)

    def test_new_real_leaf_during_enumeration_live_allowed_quiescent_rejected(self):
        injection = """
import os
original_listdir = os.listdir
added = False
def real_listdir_with_new_leaf(fd):
    global added
    names = original_listdir(fd)
    if not added and os.readlink('/proc/self/fd/' + str(fd)).endswith('/results'):
        added = True
        writer = os.open('600000.json', os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600, dir_fd=fd)
        os.write(writer, b'new actual leaf')
        os.close(writer)
    return names
os.listdir = real_listdir_with_new_leaf
"""
        self._accepted("live", injection=injection)
        (self.results / "600000.json").unlink()
        result = self._run("quiescent", injection=injection)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("namespace changed while quiescent", result.stderr)

    def test_exact_temp_rename_live_allowed_but_not_named_file_disappearance(self):
        temporary = self._file(self.results / ".000001.json.abc12345.tmp", b"real temporary")
        injection = """
import os
original_lstat = os.lstat
moved = False
def real_lstat_with_temp_publication(name, *, dir_fd=None):
    global moved
    if name == '.000001.json.abc12345.tmp' and dir_fd is not None and not moved:
        moved = True
        os.replace(name, '000001.json', src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
    return original_lstat(name, dir_fd=dir_fd)
os.lstat = real_lstat_with_temp_publication
"""
        self._accepted("live", injection=injection)
        self.assertFalse(temporary.exists())
        self.assertEqual((self.results / "000001.json").read_bytes(), b"real temporary")
        injection = """
import os
original_lstat = os.lstat
removed = False
def real_lstat_with_named_file_removal(name, *, dir_fd=None):
    global removed
    if name == '000001.json' and dir_fd is not None and not removed:
        removed = True
        os.unlink(name, dir_fd=dir_fd)
    return original_lstat(name, dir_fd=dir_fd)
os.lstat = real_lstat_with_named_file_removal
"""
        result = self._run("live", injection=injection)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("named file disappeared", result.stderr)

    def test_real_directory_swap_rejected_even_during_live_phase(self):
        injection = """
import os
original_open = os.open
swapped = False
def real_open_with_directory_swap(name, flags, mode=0o777, *, dir_fd=None):
    global swapped
    if name == 'results' and dir_fd is not None and not swapped:
        swapped = True
        os.rename(name, 'moved-original', src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        os.mkdir(name, mode=0o755, dir_fd=dir_fd)
    return original_open(name, flags, mode, dir_fd=dir_fd)
os.open = real_open_with_directory_swap
"""
        result = self._run("live", injection=injection)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("directory changed", result.stderr)
        self.assertEqual((self.batch / "moved-original/000001.json").read_bytes(), b"original-result")

    def test_prepare_and_quiescent_flat_pass_and_final_find_accept_only_fixed_subtree(self):
        for name in ("prepare_probiga_job_log_root", "migrate_probiga_job_log_legacy_modes"):
            self._accepted(script=_script(name))
        # Execute the actual final find expression extracted from the broker,
        # with private paths and the caller's real account, not a rewritten rule.
        import pwd
        body = _body("migrate_probiga_job_log_legacy_modes")
        start = body.index('  unsafe_entry="$(find -P ')
        end = body.index('  if [ -n "$unsafe_entry" ];', start)
        program = body[start:end] + '\nprintf "%s" "$unsafe_entry"\n'
        environment = dict(os.environ, PROBIGA_JOB_LOG_ROOT=str(self.jobs),
                           SERVICE_USER=pwd.getpwuid(os.geteuid()).pw_name)
        result = subprocess.run(["bash", "-c", program], env=environment,
                                text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        arbitrary = self.jobs / "arbitrary-directory"
        arbitrary.mkdir(mode=0o700)
        for name in ("prepare_probiga_job_log_root", "migrate_probiga_job_log_legacy_modes"):
            result = self._run(script=_script(name))
            self.assertNotEqual(result.returncode, 0)
        result = subprocess.run(["bash", "-c", program], env=environment,
                                text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("arbitrary-directory", result.stdout)


if __name__ == "__main__":
    unittest.main()

"""Real private Linux inode races against the embedded broker validator.

Hooks mutate actual private files and then delegate to the original syscall.
No stat results, validator helpers, database, services or production paths are
mocked.  The marker proves each intended race was actually injected.
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


def _validator():
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = source.split("validate_exposure_risk_job_checkpoint_tree() {", 1)[1]
    return re.search(r"<<'PY'[^\n]*\n(.*?)\nPY", body, re.S).group(1)


class BrokerExposureRaceSource(unittest.TestCase):
    def test_actual_embedded_validator_compiles(self):
        compile(_validator(), "actual-broker-validator", "exec")


@unittest.skipUnless(
    sys.platform.startswith("linux") and os.open in os.supports_dir_fd,
    "actual Linux inode, proc-fd and nofollow acceptance requires Linux",
)
class BrokerExposureLateInodeRaces(unittest.TestCase):
    def setUp(self):
        self.private = tempfile.TemporaryDirectory(prefix="probiga-risk-races-")
        self.base = Path(self.private.name)
        self.jobs = self.base / "jobs"
        self.jobs.mkdir(mode=0o700)
        self.risk = self.jobs / SUBTREE
        self.risk.mkdir(mode=0o755)
        self.batch = self.risk / BATCH
        self.batch.mkdir(mode=0o755)
        self.results = self.batch / "results"
        self.results.mkdir(mode=0o755)
        self._file(self.batch / "manifest.json", b"partial manifest")
        self._file(self.results / "000001.json", b"original result")
        self.external = self._file(self.base / "external", b"untouched outside proof")

    def tearDown(self):
        self.private.cleanup()

    def _file(self, path, content):
        path.write_bytes(content)
        path.chmod(0o600)
        return path

    def _run(self, phase, injection):
        source = injection + "\nexec(compile(" + repr(_validator())
        source += ", 'actual-broker-validator', 'exec'))\n"
        return subprocess.run(
            [sys.executable, "-I", "-", str(self.jobs), str(os.geteuid()),
             str(os.getegid()), phase, str(self.external)],
            input=source, text=True, capture_output=True, timeout=8,
        )

    def _assert_injected_and_rejected(self, phase, injection):
        result = self._run(phase, injection)
        self.assertIn("RACE_INJECTED", result.stdout, result.stderr)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("exposure risk checkpoint", result.stderr)
        self.assertEqual(self.external.read_bytes(), b"untouched outside proof")

    def test_live_canonical_removed_after_leaf_validation_is_rejected(self):
        injection = r'''
import os
import sys
sys.argv.pop()  # The validator consumes only its fixed four CLI arguments.
original_listdir = os.listdir
results_calls = 0
def remove_checked_canonical_before_current_names(fd):
    global results_calls
    if os.readlink('/proc/self/fd/' + str(fd)).endswith('/results'):
        results_calls += 1
        if results_calls == 2:
            os.unlink('000001.json', dir_fd=fd)
            print('RACE_INJECTED', flush=True)
    return original_listdir(fd)
os.listdir = remove_checked_canonical_before_current_names
'''
        self._assert_injected_and_rejected("live", injection)

    def test_live_unknown_added_after_last_current_names_is_rejected(self):
        injection = r'''
import os
import sys
sys.argv.pop()
original_listdir = os.listdir
results_calls = 0
def add_unknown_after_second_pass_current_names(fd):
    global results_calls
    names = original_listdir(fd)
    if os.readlink('/proc/self/fd/' + str(fd)).endswith('/results'):
        results_calls += 1
        if results_calls == 4:
            writer = os.open('unrecognized-late-entry',
                             os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600, dir_fd=fd)
            os.write(writer, b'not an approved checkpoint leaf')
            os.close(writer)
            print('RACE_INJECTED', flush=True)
    return names
os.listdir = add_unknown_after_second_pass_current_names
'''
        self._assert_injected_and_rejected("live", injection)

    def test_live_tail_removal_cannot_be_forgotten_by_a_new_stability_round(self):
        injection = r'''
import os
import sys
sys.argv.pop()
original_listdir = os.listdir
results_calls = 0
def remove_known_leaf_after_current_names_were_read(fd):
    global results_calls
    names = original_listdir(fd)
    if os.readlink('/proc/self/fd/' + str(fd)).endswith('/results'):
        results_calls += 1
        if results_calls == 4:
            os.unlink('000001.json', dir_fd=fd)
            print('RACE_INJECTED', flush=True)
    return names
os.listdir = remove_known_leaf_after_current_names_were_read
'''
        self._assert_injected_and_rejected("live", injection)

    def test_live_known_canonical_must_survive_between_global_passes(self):
        injection = r'''
import os
import sys
sys.argv.pop()
original_listdir = os.listdir
results_calls = 0
def remove_known_leaf_before_second_pass_names(fd):
    global results_calls
    if os.readlink('/proc/self/fd/' + str(fd)).endswith('/results'):
        results_calls += 1
        if results_calls == 3:
            os.unlink('000001.json', dir_fd=fd)
            print('RACE_INJECTED', flush=True)
    return original_listdir(fd)
os.listdir = remove_known_leaf_before_second_pass_names
'''
        self._assert_injected_and_rejected("live", injection)

    def test_live_same_named_safe_directory_cannot_change_between_passes(self):
        injection = r'''
import os
import sys
from pathlib import Path
sys.argv.pop()
original_lstat = os.lstat
root_checks = 0
def replace_same_named_tree_before_second_pass(name, *, dir_fd=None):
    global root_checks
    if name == 'exposure-event-risk-watch-v1' and dir_fd is not None:
        root_checks += 1
        if root_checks == 4:
            retained = str(Path(sys.argv[1]).parent / 'retained-risk-original')
            os.rename(name, retained, src_dir_fd=dir_fd)
            os.mkdir(name, mode=0o755, dir_fd=dir_fd)
            risk = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                           dir_fd=dir_fd)
            batch = results = None
            try:
                batch_name = 'exposure-risk-' + 'a' * 48
                os.mkdir(batch_name, mode=0o755, dir_fd=risk)
                batch = os.open(batch_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=risk)
                os.mkdir('results', mode=0o755, dir_fd=batch)
                results = os.open('results', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                  dir_fd=batch)
                for parent, leaf in ((batch, 'manifest.json'), (results, '000001.json')):
                    writer = os.open(leaf, os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                                     0o600, dir_fd=parent)
                    os.write(writer, b'same approved names but a replaced directory')
                    os.close(writer)
            finally:
                if results is not None:
                    os.close(results)
                if batch is not None:
                    os.close(batch)
                os.close(risk)
            print('RACE_INJECTED', flush=True)
    return original_lstat(name, dir_fd=dir_fd)
os.lstat = replace_same_named_tree_before_second_pass
'''
        self._assert_injected_and_rejected("live", injection)
        self.assertEqual((self.base / "retained-risk-original" / BATCH /
                          "results/000001.json").read_bytes(), b"original result")

    def _absent_risk_created_after_last_absence_check_is_rejected(self, phase):
        (self.results / "000001.json").unlink()
        self.results.rmdir()
        (self.batch / "manifest.json").unlink()
        self.batch.rmdir()
        self.risk.rmdir()
        injection = r'''
import os
import sys
sys.argv.pop()
original_lstat = os.lstat
absence_checks = 0
def create_tree_after_second_proven_absence(name, *, dir_fd=None):
    global absence_checks
    try:
        return original_lstat(name, dir_fd=dir_fd)
    except FileNotFoundError:
        if name == 'exposure-event-risk-watch-v1' and dir_fd is not None:
            absence_checks += 1
            if absence_checks == 2:
                os.mkdir(name, mode=0o755, dir_fd=dir_fd)
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=dir_fd)
                try:
                    writer = os.open('not-an-approved-batch',
                                     os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600,
                                     dir_fd=child)
                    os.write(writer, b'never inspected by the old root snapshot')
                    os.close(writer)
                finally:
                    os.close(child)
                print('RACE_INJECTED', flush=True)
        raise
os.lstat = create_tree_after_second_proven_absence
'''
        self._assert_injected_and_rejected(phase, injection)

    def test_quiescent_absent_risk_created_after_last_absence_check_is_rejected(self):
        self._absent_risk_created_after_last_absence_check_is_rejected("quiescent")

    def test_live_absent_risk_created_after_last_absence_check_is_rejected(self):
        self._absent_risk_created_after_last_absence_check_is_rejected("live")

    def test_live_successor_must_remain_bound_to_current_named_inode(self):
        injection = r'''
import os
import sys
external = sys.argv.pop()
original_open = os.open
canonical_opens = 0
def replace_held_successor_before_final_named_identity(name, flags, mode=0o777,
                                                      *, dir_fd=None):
    global canonical_opens
    descriptor = original_open(name, flags, mode, dir_fd=dir_fd)
    if name == '000001.json' and dir_fd is not None:
        canonical_opens += 1
        if canonical_opens == 2:
            # Keep the already observed inode singly linked outside the
            # inspected tree after replacement.  This real adversarial race
            # reaches successor binding instead of failing on nlink==0 first.
            os.link(name, external + '.prior', src_dir_fd=dir_fd)
            temporary = '.000001.json.abc12345.tmp'
            writer = original_open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                                   0o600, dir_fd=dir_fd)
            os.write(writer, b'legal real successor')
            os.close(writer)
            os.replace(temporary, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        elif canonical_opens == 3:
            os.link(name, external + '.successor', src_dir_fd=dir_fd)
            os.unlink(name, dir_fd=dir_fd)
            os.symlink(external, name, dir_fd=dir_fd)
            print('RACE_INJECTED', flush=True)
    return descriptor
os.open = replace_held_successor_before_final_named_identity
'''
        self._assert_injected_and_rejected("live", injection)

    def test_live_unlinked_held_inode_is_rechecked_not_authorized(self):
        injection = r'''
import os
import sys
sys.argv.pop()
original_open = os.open
replaced = False
def publish_after_open_before_fstat(name, flags, mode=0o777, *, dir_fd=None):
    global replaced
    descriptor = original_open(name, flags, mode, dir_fd=dir_fd)
    if name == '000001.json' and dir_fd is not None and not replaced:
        replaced = True
        temporary = '.000001.json.abc12345.tmp'
        writer = original_open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                               0o600, dir_fd=dir_fd)
        os.write(writer, b'legitimate successor of an unlinked held inode')
        os.close(writer)
        os.replace(temporary, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        assert os.fstat(descriptor).st_nlink == 0
        print('RACE_INJECTED', flush=True)
    return descriptor
os.open = publish_after_open_before_fstat
'''
        result = self._run("live", injection)
        self.assertIn("RACE_INJECTED", result.stdout, result.stderr)
        self.assertEqual(result.returncode, 0, result.stderr)
        current = self.results / "000001.json"
        self.assertEqual(current.read_bytes(),
                         b"legitimate successor of an unlinked held inode")
        self.assertEqual(current.stat().st_nlink, 1)
        self.assertEqual(current.stat().st_mode & 0o7777, 0o600)
        self.assertEqual(self.external.read_bytes(), b"untouched outside proof")
        self._assert_injected_and_rejected("quiescent", injection)


if __name__ == "__main__":
    unittest.main()

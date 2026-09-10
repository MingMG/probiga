"""Verify or install the merged release's hash-locked Windows MyQuant runtime.

The updater invokes installation only after its authorized scheduler quiescence.
This tool never starts/stops QMT, accesses market data, or writes the database.
"""
from __future__ import annotations

import argparse
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import re
import struct
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
LOCK_PATH = ROOT / "deploy" / "qmt_myquant_requirements.lock"
SCHEMA = "probiga.qmt-myquant-runtime.v1"
_IMPORT_SUCCESS = b"PROBIGA_MYQUANT_IMPORT_READY"
_REQUIREMENT = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9._-]*)==([A-Za-z0-9][A-Za-z0-9._+!-]*)"
    r"((?:\s+--hash=sha256:[0-9a-f]{64})+)"
)


class RuntimeNotReady(RuntimeError):
    """Only fixed, credential-free reason codes may be exposed by the CLI."""


def _name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def parse_lock(content: str) -> dict[str, str]:
    requirements: dict[str, str] = {}
    pending = ""
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        pending += (" " if pending else "") + line.removesuffix("\\").strip()
        if line.endswith("\\"):
            continue
        match = _REQUIREMENT.fullmatch(pending)
        if match is None:
            raise RuntimeNotReady("MYQUANT_LOCK_INVALID")
        name = _name(match[1])
        if name in requirements:
            raise RuntimeNotReady("MYQUANT_LOCK_DUPLICATE")
        requirements[name] = match[2]
        pending = ""
    if pending or not {"gm", "numpy", "pandas"} <= requirements.keys():
        raise RuntimeNotReady("MYQUANT_LOCK_INCOMPLETE")
    return requirements


def validate_runtime(expected_build_sha: str) -> None:
    if (os.name != "nt" or sys.implementation.name != "cpython"
            or sys.version_info[:2] != (3, 13) or struct.calcsize("P") != 8
            or platform.machine().lower() not in {"amd64", "x86_64"}):
        raise RuntimeNotReady("MYQUANT_RUNTIME_PLATFORM_DIFFERS")
    runtime = ROOT / "runtime" / "qmt-py313"
    for path in (ROOT, ROOT / "runtime", runtime, runtime / "Scripts",
                 runtime / "Scripts" / "python.exe", LOCK_PATH.parent, LOCK_PATH):
        if (not path.exists() or path.is_symlink()
                or getattr(path, "is_junction", lambda: False)()):
            raise RuntimeNotReady("MYQUANT_RUNTIME_PATH_INVALID")
    if (Path(sys.prefix).resolve() != runtime.resolve()
            or Path(sys.executable).resolve() != (runtime / "Scripts" / "python.exe").resolve()):
        raise RuntimeNotReady("MYQUANT_RUNTIME_PATH_DIFFERS")
    if re.fullmatch(r"[0-9a-f]{40}", expected_build_sha) is None:
        raise RuntimeNotReady("MYQUANT_BUILD_INVALID")
    result = subprocess.run(
        ["git", "--no-replace-objects", "-C", str(ROOT), "rev-parse", "HEAD"],
        capture_output=True, text=True, timeout=15, check=False,
    )
    if result.returncode or result.stdout.strip() != expected_build_sha:
        raise RuntimeNotReady("MYQUANT_BUILD_DIFFERS")


def version_mismatches(requirements: dict[str, str]) -> list[str]:
    installed: dict[str, list[str]] = {}
    for distribution in metadata.distributions():
        name = _name(distribution.metadata.get("Name") or "")
        if name in requirements:
            installed.setdefault(name, []).append(distribution.version)
    return sorted(name for name, version in requirements.items()
                  if installed.get(name) != [version])


def verify_import() -> None:
    result = subprocess.run(
        [sys.executable, "-I", "-c",
         "import gm.api; print('PROBIGA_MYQUANT_IMPORT_READY', flush=True)"],
        capture_output=True, timeout=60, check=False,
    )
    # gm registers an atexit handler calling os._exit(0). A failed import can
    # therefore report exit 0; require proof emitted after import completes.
    if result.returncode or result.stdout.strip() != _IMPORT_SUCCESS:
        raise RuntimeNotReady("MYQUANT_SDK_IMPORT_FAILED")


def package_source() -> list[str]:
    """Use a complete fixed local wheelhouse, otherwise the official index."""
    wheelhouse = ROOT / "runtime" / "myquant-wheels"
    if not wheelhouse.exists():
        return ["--index-url", "https://pypi.org/simple"]
    if (not wheelhouse.is_dir() or wheelhouse.is_symlink()
            or getattr(wheelhouse, "is_junction", lambda: False)()):
        raise RuntimeNotReady("MYQUANT_WHEELHOUSE_PATH_INVALID")
    wheel_hashes: set[str] = set()
    for path in wheelhouse.iterdir():
        if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
            raise RuntimeNotReady("MYQUANT_WHEELHOUSE_PATH_INVALID")
        if path.is_file() and path.suffix == ".whl":
            with path.open("rb") as stream:
                wheel_hashes.add(hashlib.file_digest(stream, "sha256").hexdigest())
    lock = re.sub(r"\\\s*\n", " ", LOCK_PATH.read_text(encoding="utf-8"))
    requirements = [line.strip() for line in lock.splitlines()
                    if line.strip() and not line.lstrip().startswith("#")]
    if all(set(re.findall(r"--hash=sha256:([0-9a-f]{64})", line)) & wheel_hashes
           for line in requirements):
        return ["--no-index", "--find-links", str(wheelhouse)]
    return ["--index-url", "https://pypi.org/simple"]


def install_locked_dependencies() -> None:
    result = subprocess.run(
        [sys.executable, "-I", "-m", "pip", "--isolated", "install",
         *package_source(), "--require-hashes",
         "--only-binary=:all:", "--no-input", "--disable-pip-version-check",
         "--timeout", "30", "--retries", "2", "-r", str(LOCK_PATH)],
        capture_output=True, timeout=900, check=False,
    )
    if result.returncode:
        raise RuntimeNotReady("MYQUANT_LOCKED_INSTALL_FAILED")


def ensure_runtime(expected_build_sha: str, *, install: bool = False) -> dict:
    validate_runtime(expected_build_sha)
    lock_bytes = LOCK_PATH.read_bytes()
    requirements = parse_lock(lock_bytes.decode("utf-8"))
    mismatch = version_mismatches(requirements)
    installed = False
    if mismatch and install:
        install_locked_dependencies()
        installed = True
        if LOCK_PATH.read_bytes() != lock_bytes:
            raise RuntimeNotReady("MYQUANT_LOCK_CHANGED")
        # A new metadata scan observes the packages written by the pip child.
        mismatch = version_mismatches(requirements)
        if mismatch:
            raise RuntimeNotReady("MYQUANT_LOCKED_VERSIONS_DIFFER")
    result = {
        "schema": SCHEMA,
        "mode": "install" if install else "check",
        "build_sha": expected_build_sha,
        "lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
        "status": "NEEDS_INSTALL" if mismatch else "READY",
        "mismatched_packages": mismatch,
        "package_count": len(requirements),
        "installed": installed,
        "database_writes": False,
        "qmt_calls": False,
    }
    if not mismatch:
        verify_import()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-build-sha", required=True)
    parser.add_argument("--install", action="store_true")
    args = parser.parse_args()
    try:
        result = ensure_runtime(args.expected_build_sha, install=args.install)
    except Exception as exc:
        # pip/index/HTTP errors may include configured credentials. Never echo
        # child output or arbitrary exception text into scheduler release logs.
        result = {
            "schema": SCHEMA, "status": "BLOCKED",
            "reason_code": str(exc) if isinstance(exc, RuntimeNotReady)
            else ("MYQUANT_RUNTIME_TIMEOUT" if isinstance(exc, subprocess.TimeoutExpired)
                  else "MYQUANT_RUNTIME_CHECK_FAILED"),
            "database_writes": False, "qmt_calls": False,
        }
        print(json.dumps(result, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 4 if result["status"] == "NEEDS_INSTALL" else 0


if __name__ == "__main__":
    raise SystemExit(main())

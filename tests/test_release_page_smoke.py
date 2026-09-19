"""Execute the production Bash page probes against sealed local fixtures.

Only curl is substituted; parsing, account contracts, cmp and grep run as in
the release script. No production service or network request is contacted.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _bash() -> str | None:
    discovered = shutil.which("bash")
    if discovered:
        return discovered
    git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
    return str(git_bash) if git_bash.is_file() else None


def _function(name: str) -> str:
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    match = re.search(rf"(?ms)^{re.escape(name)}\(\) \{{\n.*?^\}}\s*$", source)
    assert match is not None, f"production shell function missing: {name}"
    return match.group(0)


def _quote(value: str | Path) -> str:
    return shlex.quote(value.as_posix() if isinstance(value, Path) else value)


def _run(script: str) -> subprocess.CompletedProcess[str]:
    bash = _bash()
    if not bash:
        pytest.skip("Bash unavailable")
    return subprocess.run(
        [bash, "--noprofile", "--norc", "-s"],
        input=script,
        cwd=ROOT,
        # Git Bash otherwise rewrites the URL path into C:/Program Files/Git/...
        # when passing it to native Windows Python. Filesystem paths still need
        # normal conversion for the real mktemp outputs used by the probe.
        env={**os.environ, "MSYS2_ARG_CONV_EXCL": "/static/"},
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )


def _page_helper(page: Path, expected: str) -> subprocess.CompletedProcess[str]:
    return _run(
        "set -euo pipefail\n"
        f"BOOTSTRAP_PYTHON={_quote(Path(sys.executable))}\n"
        + _function("release_page_asset_url")
        + f"\nrelease_page_asset_url {_quote(page)} {_quote(expected)}\n"
    )


@pytest.mark.parametrize(
    ("markup", "expected", "url"),
    [
        ('<script src="/static/js/login.js?v=2"></script>', "/static/js/login.js", "/static/js/login.js?v=2"),
        ('<script src="/static/js/login.js?v=123"></script>', "/static/js/login.js", "/static/js/login.js?v=123"),
        ("<SCRIPT defer SRC='/static/js/login.js?v=release-42'></SCRIPT>", "/static/js/login.js", "/static/js/login.js?v=release-42"),
        ('<link rel="stylesheet" href="/static/css/style.css?v=48">', "/static/css/style.css", "/static/css/style.css?v=48"),
        ('<script src="https://cdn.example/chart.js"></script><script src="/static/js/app.js?v=129"></script>', "/static/js/app.js", "/static/js/app.js?v=129"),
    ],
)
def test_release_page_asset_url_reads_actual_versioned_asset(
    tmp_path: Path, markup: str, expected: str, url: str,
) -> None:
    page = tmp_path / "page.html"
    page.write_text(markup, encoding="utf-8")
    result = _page_helper(page, expected)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == url


@pytest.mark.parametrize(
    "markup",
    [
        "<html><body>No script</body></html>",
        '<!-- <script src="/static/js/login.js?v=2"></script> -->',
        '<script src="https://evil.example/static/js/login.js?v=2"></script>',
        '<script src="//evil.example/static/js/login.js?v=2"></script>',
        '<script src="/static/js/login.js?v=2"></script><script src="/static/js/login.js?v=2"></script>',
        '<script src="/static/js/login.js?v=2" src="/static/js/login.js?v=3"></script>',
        '<script src="/static/js/../js/login.js?v=2"></script>',
        '<script src="/static/js/%2e%2e/js/login.js?v=2"></script>',
        '<script src="/static/js/login.js"></script>',
        '<script src="/static/js/login.js?v="></script>',
        '<script src="/static/js/login.js?v=%20"></script>',
        '<script src="/static/js/login.js?v=2&amp;v=3"></script>',
        '<link rel="preload" href="/static/js/login.js?v=2">',
    ],
)
def test_release_page_asset_url_rejects_unproven_references(tmp_path: Path, markup: str) -> None:
    page = tmp_path / "page.html"
    page.write_text(markup, encoding="utf-8")
    result = _page_helper(page, "/static/js/login.js")
    assert result.returncode != 0, result.stdout + result.stderr
    assert not result.stdout.strip(), "invalid HTML must not produce a usable release URL"


def _login_fixture(
    tmp_path: Path,
    *,
    version: str = "2",
    page_markup: str | None = None,
    page_drift: bool = False,
    script_drift: bool = False,
    negative_http: str = "401",
    negative_error: str = "invalid_credentials",
    status_overrides: dict | None = None,
    missing_api_marker: str | None = None,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    prepared = tmp_path / "prepared"
    static = prepared / "server/static"
    (static / "js").mkdir(parents=True)
    fixture = tmp_path / "responses"
    fixture.mkdir()
    page_text = (ROOT / "server/static/login.html").read_text(encoding="utf-8")
    page_text = re.sub(r"/static/js/login\.js\?v=[^\"']+", f"/static/js/login.js?v={version}", page_text)
    if page_markup is not None:
        page_text = page_markup
    script_bytes = (ROOT / "server/static/js/login.js").read_bytes()
    if missing_api_marker is not None:
        marker = missing_api_marker.encode("utf-8")
        assert marker in script_bytes
        script_bytes = script_bytes.replace(marker, b"removed_api_marker")
    (static / "login.html").write_text(page_text, encoding="utf-8")
    (static / "js/login.js").write_bytes(script_bytes)
    (fixture / "page.html").write_bytes((static / "login.html").read_bytes() + (b"\n<!-- drift -->" if page_drift else b""))
    (fixture / "script.js").write_bytes(script_bytes + (b"\n// drift" if script_drift else b""))
    status = {
        "status": "ok", "required": True, "authenticated": False,
        "user_initialized": True, "user_count": 1, "registration_open": False,
    }
    status.update(status_overrides or {})
    (fixture / "status.json").write_text(json.dumps(status), encoding="utf-8")
    (fixture / "negative.json").write_text(json.dumps({
        "status": "error", "error": negative_error, "authenticated": False,
    }), encoding="utf-8")
    trace = tmp_path / "curl-urls.txt"
    shell = "set -euo pipefail\n" + "\n".join([
        f"BOOTSTRAP_PYTHON={_quote(Path(sys.executable))}",
        f"PREPARED_CODE_ROOT={_quote(prepared)}",
        f"FIXTURE_ROOT={_quote(fixture)}",
        f"CURL_TRACE={_quote(trace)}",
        f"EXPECTED_ASSET_URL={_quote('http://127.0.0.1/static/js/login.js?v=' + version)}",
        f"NEGATIVE_HTTP={_quote(negative_http)}",
        # Keep real mktemp/cmp/grep; isolate all ephemeral outputs to this test.
        f"TMPDIR={_quote(tmp_path)}",
        "export TMPDIR",
    ]) + r"""
curl() {
  local output='' url='' write_out=''
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --output) output="$2"; shift 2 ;;
      --write-out) write_out="$2"; shift 2 ;;
      http://*|https://*) url="$1"; shift ;;
      *) shift ;;
    esac
  done
  printf '%s\n' "$url" >> "$CURL_TRACE"
  test -n "$output" || return 72
  case "$url" in
    http://127.0.0.1/api/auth/status) cp "$FIXTURE_ROOT/status.json" "$output" ;;
    http://127.0.0.1/api/auth/login)
      cp "$FIXTURE_ROOT/negative.json" "$output"
      test "$write_out" = '%{http_code}' || return 73
      printf '%s' "$NEGATIVE_HTTP"
      ;;
    http://127.0.0.1/login) cp "$FIXTURE_ROOT/page.html" "$output" ;;
    "$EXPECTED_ASSET_URL") cp "$FIXTURE_ROOT/script.js" "$output" ;;
    *) printf 'unexpected static URL: %s\n' "$url" >&2; return 74 ;;
  esac
}
"""
    shell += _function("release_page_asset_url") + "\n"
    shell += _function("verify_account_login_api_and_page_smoke") + "\n"
    shell += "verify_account_login_api_and_page_smoke " + "a" * 40 + "\n"
    result = _run(shell)
    urls = trace.read_text(encoding="utf-8").splitlines() if trace.exists() else []
    return result, urls


@pytest.mark.parametrize("version", ["2", "123"])
def test_account_login_smoke_requests_the_prepared_version_and_compares_bytes(tmp_path: Path, version: str) -> None:
    result, urls = _login_fixture(tmp_path, version=version)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Account login API and page smoke passed" in result.stdout
    assert f"http://127.0.0.1/static/js/login.js?v={version}" in urls
    assert "http://127.0.0.1/static/js/login.js" not in urls


@pytest.mark.parametrize("drift", ["page_drift", "script_drift"])
def test_account_login_smoke_rejects_wrong_release_bytes(tmp_path: Path, drift: str) -> None:
    result, _ = _login_fixture(tmp_path, **{drift: True})
    assert result.returncode != 0, result.stdout + result.stderr
    assert "Account login page/static release smoke failed" in result.stderr


@pytest.mark.parametrize(
    "markup",
    [
        '<html><body id="authForm"></body></html>',
        '<script src="https://evil.example/static/js/login.js?v=2"></script>',
        '<script src="/static/js/login.js?v=2"></script><script src="/static/js/login.js?v=2"></script>',
    ],
)
def test_account_login_smoke_rejects_missing_external_or_duplicate_script(tmp_path: Path, markup: str) -> None:
    result, urls = _login_fixture(tmp_path, page_markup=markup)
    assert result.returncode != 0, result.stdout + result.stderr
    assert not any("/static/js/login.js" in url for url in urls), "unproven script must not be requested"


@pytest.mark.parametrize(
    ("http", "error"), [("200", "invalid_credentials"), ("403", "invalid_credentials"), ("401", "internal_error")],
)
def test_account_login_smoke_preserves_negative_login_contract(tmp_path: Path, http: str, error: str) -> None:
    result, urls = _login_fixture(tmp_path, negative_http=http, negative_error=error)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "Account login API and page smoke passed" not in result.stdout
    assert "http://127.0.0.1/login" not in urls, "bad authentication contract must fail before static checks"


@pytest.mark.parametrize("overrides", [{"required": False}, {"user_count": 0}])
def test_account_login_smoke_requires_an_initialized_protected_account(tmp_path: Path, overrides: dict) -> None:
    result, urls = _login_fixture(tmp_path, status_overrides=overrides)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "status_or_initialized_account" in result.stderr
    assert "http://127.0.0.1/login" not in urls


@pytest.mark.parametrize("marker", ["fetch('/api/auth/' + mode", "fetch('/api/auth/status'"])
def test_account_login_smoke_keeps_login_api_checks_even_when_asset_bytes_match(tmp_path: Path, marker: str) -> None:
    result, urls = _login_fixture(tmp_path, missing_api_marker=marker)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "http://127.0.0.1/static/js/login.js?v=2" in urls
    assert "Account login page/static release smoke failed" in result.stderr

from __future__ import annotations

from pathlib import Path

from tools.scan_tracked_secrets import scan_paths, scan_text, tracked_paths


def test_reports_location_and_type_without_secret_value() -> None:
    secret = "not-for-output"
    findings = scan_text(
        "tools/example.py",
        f'DATABASE_URL = "mysql+pymysql://app:{secret}@db:3306/app"',
    )

    rendered = [f"{item.path}:{item.line}:{item.kind}" for item in findings]

    assert rendered == ["tools/example.py:1:DB_URL_PASSWORD"]
    assert secret not in repr(findings)
    assert secret not in "\n".join(rendered)


def test_reports_literal_password_assignment() -> None:
    findings = scan_text(
        "deploy/legacy.ps1",
        '\n'.join(
            [
                '$sshPassword = "not-for-output"',
                'password="also-not-for-output"',
                'PASS="still-not-for-output"',
            ]
        ),
    )

    assert [(item.line, item.kind) for item in findings] == [
        (1, "PASSWORD_LITERAL"),
        (2, "PASSWORD_LITERAL"),
        (3, "PASSWORD_LITERAL"),
    ]


def test_ignores_environment_lookup_and_empty_default() -> None:
    source = "\n".join(
        [
            'password = os.environ.get("PROBIGA_REMOTE_SSH_PASSWORD", "")',
            'mysql_url = os.environ.get("MYSQL_URL")',
        ]
    )

    assert scan_text("tools/safe.py", source) == []


def test_allows_only_known_password_toggle_element_ids() -> None:
    safe = '<button data-toggle-password="login-password">show</button>'
    unsafe = '<button data-toggle-password="embedded-secret">show</button>'

    assert scan_text("server/static/login.html", safe) == []
    assert [item.kind for item in scan_text("server/static/login.html", unsafe)] == [
        "PASSWORD_LITERAL"
    ]


def test_scans_javascript_but_allows_known_ui_password_and_pass_status_idioms() -> None:
    assert [
        item.kind
        for item in scan_text(
            "server/static/js/new-feature.js",
            'const config = {mysql_password: "not-for-output"};',
        )
    ] == ["PASSWORD_LITERAL"]
    assert scan_text(
        "server/static/js/login.js",
        "target.type = showing ? 'password' : 'text';",
    ) == []
    assert scan_text(
        "server/static/js/status.js",
        "const labels = {PASS: '通过', PASSED: '通过'};",
    ) == []


def test_excludes_only_reviewed_fixture_and_public_env_example() -> None:
    fake = 'DATABASE_URL="mysql+pymysql://fake:fake@localhost/fake"'

    assert [item.kind for item in scan_text("tests/test_config.py", fake)] == [
        "DB_URL_PASSWORD"
    ]
    assert scan_text("tests/fixtures/config.py", fake) == []
    assert scan_text(".env.example", fake) == []


def test_new_test_files_are_scanned_unless_explicitly_reviewed() -> None:
    fake = 'DATABASE_URL="mysql+pymysql://fake:fake@localhost/fake"'

    assert [
        item.kind
        for item in scan_text("tests/test_new_unreviewed_tool.py", fake)
    ] == ["DB_URL_PASSWORD"]


def test_scans_documentation_and_configuration_text() -> None:
    fake = "mysql+pymysql://fake:fake@localhost/fake"

    assert [(item.line, item.kind) for item in scan_text("docs/example.md", fake)] == [
        (1, "DB_URL_PASSWORD")
    ]
    assert [(item.line, item.kind) for item in scan_text("config/runtime.json", fake)] == [
        (1, "DB_URL_PASSWORD")
    ]


def test_scans_quoted_json_credential_keys() -> None:
    source = '\n'.join(
        [
            '{"mysql_password": "not-for-output"}',
            '{"server": "203.0.113.10"}',
        ]
    )

    assert [(item.line, item.kind) for item in scan_text("config/runtime.json", source)] == [
        (1, "PASSWORD_LITERAL"),
        (2, "REMOTE_IDENTITY_LITERAL"),
    ]


def test_reports_fixed_remote_ssh_identity_but_not_loopback() -> None:
    source = "\n".join(
        [
            'SERVER = "203.0.113.10"',
            'command = "ssh root@203.0.113.10 uptime"',
            'local_host = "127.0.0.1"',
        ]
    )

    assert [(item.line, item.kind) for item in scan_text("deploy/old.py", source)] == [
        (1, "REMOTE_IDENTITY_LITERAL"),
        (2, "REMOTE_IDENTITY_LITERAL"),
    ]


def test_tracked_source_has_no_embedded_credentials() -> None:
    root = Path(__file__).resolve().parents[1]

    assert scan_paths(root, tracked_paths(root)) == []

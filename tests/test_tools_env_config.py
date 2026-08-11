# -*- coding: utf-8 -*-
from tools.env_config import load_project_env


def test_load_project_env_preserves_existing_values_and_parses_quotes(tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "# ignored",
                "MYSQL_URL=mysql://from-file",
                "EXISTING=from-file",
                "QUOTED='hello world'",
                'DOUBLE_QUOTED="hello again"',
                "export EXPORTED=enabled",
                "=ignored",
            ]
        ),
        encoding="utf-8",
    )
    environ = {"EXISTING": "from-env"}

    load_project_env(env_path, environ=environ)

    assert environ["MYSQL_URL"] == "mysql://from-file"
    assert environ["EXISTING"] == "from-env"
    assert environ["QUOTED"] == "hello world"
    assert environ["DOUBLE_QUOTED"] == "hello again"
    assert environ["EXPORTED"] == "enabled"
    assert "" not in environ

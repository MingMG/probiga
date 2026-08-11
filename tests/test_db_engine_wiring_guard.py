from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PACKAGES = ("biz", "tools", "scripts")
PRODUCTION_PACKAGES = ("biz", "tools", "scripts", "deploy", "integrations", "server")
SIDE_EFFECT_SCRIPT_PACKAGES = ("biz", "tools", "scripts", "deploy")
ENGINE_FACTORY_ALLOWED = {"server/common/engine_factory.py"}
# Offline upgrade auditors intentionally use option files and direct PyMySQL
# connections so source/target credentials and TLS identities stay independent
# from the application's runtime URL.  They are not API, scheduler or worker
# entry points; keep the exception exact so new runtime bypasses still fail.
MYSQL_DBAPI_ALLOWED = {
    "tools/audit_mysql55_to_mysql84_schema.py",
    "tools/mysql55_to_mysql84_data_manifest.py",
}
PANDAS_SQL_ALLOWED = {"server/common/batch_db.py"}
MYSQL_URL_RESOLUTION_ALLOWED = {"tools/env_config.py", "deploy/env_config.py"}
ENV_COPY_ALLOWED = {
    "server/common/process_env.py",
}
DOTENV_LOADING_ALLOWED = {
    "server/common/config.py",
    "tools/env_config.py",
}
REMOTE_DEFAULT_ALLOWED = {
    "server/common/config.py",
    "tools/remote_support.py",
}
SERVER_POPEN_ALLOWED = {
    "server/api/scheduler_runtime.py",
    "server/api/routers/deploy.py",
}
HTTP_CLIENT_NAME_FALSE_POSITIVES = {
    "tools/archive_guojin_qmt_probe.py",
}


def _py_files(packages: tuple[str, ...] = PRODUCTION_PACKAGES):
    for package in packages:
        for path in (ROOT / package).rglob("*.py"):
            yield path


def _rel(path: Path) -> str:
    return str(path.relative_to(ROOT)).replace("\\", "/")


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _subprocess_call_name(node: ast.Call) -> str | None:
    if not isinstance(node.func, ast.Attribute):
        return None
    if isinstance(node.func.value, ast.Name) and node.func.value.id == "subprocess":
        return node.func.attr
    return None


def _is_http_client_constructor(node: ast.Call) -> bool:
    if not isinstance(node.func, ast.Attribute):
        return False
    if not isinstance(node.func.value, ast.Name):
        return False
    return (
        node.func.value.id,
        node.func.attr,
    ) in {("requests", "Session"), ("httpx", "Client"), ("httpx", "AsyncClient")}


def _http_client_aliases(tree: ast.AST) -> set[str]:
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            if _is_http_client_constructor(node.value):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        aliases.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.value, ast.Call):
            if _is_http_client_constructor(node.value) and isinstance(node.target, ast.Name):
                aliases.add(node.target.id)
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                if not isinstance(item.context_expr, ast.Call):
                    continue
                if _is_http_client_constructor(item.context_expr) and isinstance(item.optional_vars, ast.Name):
                    aliases.add(item.optional_vars.id)
    return aliases


def test_business_and_tool_scripts_do_not_create_sqlalchemy_engines_directly():
    offenders: list[str] = []
    for package in SCRIPT_PACKAGES:
        for path in (ROOT / package).rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "create_engine(" in text or "from sqlalchemy import create_engine" in text:
                offenders.append(str(path.relative_to(ROOT)).replace("\\", "/"))

    assert offenders == []


def test_production_code_creates_sqlalchemy_engines_only_in_factory():
    offenders: list[str] = []
    for path in _py_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "create_engine(" in text or "from sqlalchemy import create_engine" in text:
            rel = _rel(path)
            if rel not in ENGINE_FACTORY_ALLOWED:
                offenders.append(rel)

    assert offenders == []


def test_production_code_does_not_create_engines_at_import_time():
    offenders: list[str] = []
    engine_helpers = {"create_tool_engine", "create_batch_engine"}
    skipped_top_level_nodes = (
        ast.Import,
        ast.ImportFrom,
        ast.FunctionDef,
        ast.AsyncFunctionDef,
        ast.ClassDef,
    )

    for path in _py_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(text)
        for node in tree.body:
            if isinstance(node, skipped_top_level_nodes):
                continue
            for child in ast.walk(node):
                if isinstance(child, ast.Call) and _call_name(child) in engine_helpers:
                    offenders.append(f"{_rel(path)}:{child.lineno}")

    assert offenders == []


def test_scripts_do_not_run_external_side_effects_at_import_time():
    offenders: list[str] = []
    side_effect_calls = {
        "run",
        "Popen",
        "system",
        "SSHClient",
        "urlopen",
        "exec_command",
        "open_sftp",
        "put",
    }
    skipped_top_level_nodes = (
        ast.Import,
        ast.ImportFrom,
        ast.FunctionDef,
        ast.AsyncFunctionDef,
        ast.ClassDef,
    )

    for path in _py_files(SIDE_EFFECT_SCRIPT_PACKAGES):
        text = path.read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(text)
        for node in tree.body:
            if isinstance(node, skipped_top_level_nodes):
                continue
            for child in ast.walk(node):
                if isinstance(child, ast.Call) and _call_name(child) in side_effect_calls:
                    offenders.append(f"{_rel(path)}:{child.lineno}")

    assert offenders == []


def test_create_tool_engine_delegates_to_batch_helper():
    from unittest.mock import patch

    from tools import env_config

    engine = object()
    with patch("tools.env_config.create_batch_engine", return_value=engine) as create_batch_engine:
        assert env_config.create_tool_engine("mysql://example", future=True) is engine

    create_batch_engine.assert_called_once_with("mysql://example", future=True)


def test_resolve_tool_mysql_url_prefers_environment():
    import os
    from unittest.mock import patch

    from tools import env_config

    with patch.dict(os.environ, {"MYSQL_URL": "mysql://env"}):
        assert env_config.resolve_tool_mysql_url() == "mysql://env"


def test_resolve_tool_mysql_url_falls_back_to_required_config():
    import os
    from unittest.mock import patch

    from tools import env_config

    with patch.dict(os.environ, {"MYSQL_URL": ""}), patch(
        "tools.env_config.require_mysql_url",
        return_value="mysql://config",
    ):
        assert env_config.resolve_tool_mysql_url() == "mysql://config"


def test_production_scripts_use_shared_mysql_url_resolver():
    offenders: list[str] = []
    for path in _py_files():
        rel = _rel(path)
        if rel in MYSQL_URL_RESOLUTION_ALLOWED:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "require_mysql_url" in text:
            offenders.append(rel)

    assert offenders == []


def test_business_and_tool_scripts_use_shared_sql_read_helper():
    offenders: list[str] = []
    for package in SCRIPT_PACKAGES:
        for path in (ROOT / package).rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "pd.read_sql(" in text or "pd.read_sql_query(" in text:
                offenders.append(str(path.relative_to(ROOT)).replace("\\", "/"))

    assert offenders == []


def test_production_code_uses_shared_pandas_sql_helpers():
    offenders: list[str] = []
    for path in _py_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "pd.read_sql(" in text or "pd.read_sql_query(" in text or ".to_sql(" in text:
            rel = _rel(path)
            if rel not in PANDAS_SQL_ALLOWED:
                offenders.append(rel)

    assert offenders == []


def test_business_and_tool_scripts_use_shared_sql_write_helper():
    offenders: list[str] = []
    for package in (*SCRIPT_PACKAGES, "server/api/routers"):
        for path in (ROOT / package).rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if ".to_sql(" in text:
                offenders.append(str(path.relative_to(ROOT)).replace("\\", "/"))

    assert offenders == []


def test_business_and_tool_scripts_do_not_use_api_router_engine():
    offenders: list[str] = []
    for package in SCRIPT_PACKAGES:
        for path in (ROOT / package).rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if "server.api.routers._engine" in text:
                offenders.append(str(path.relative_to(ROOT)).replace("\\", "/"))

    assert offenders == []


def test_production_code_does_not_write_scheduler_table_directly():
    offenders: list[str] = []
    needles = (
        "UPDATE st_scheduled_tasks",
        "INSERT INTO st_scheduled_tasks",
        "ALTER TABLE st_scheduled_tasks",
        "UPDATE `st_scheduled_tasks`",
        "INSERT INTO `st_scheduled_tasks`",
        "ALTER TABLE `st_scheduled_tasks`",
    )
    for path in _py_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(needle in text for needle in needles):
            offenders.append(_rel(path))

    assert offenders == []


def test_production_code_uses_sqlalchemy_for_mysql_connections():
    offenders: list[str] = []
    needles = ("pymysql.connect(", "mysql.connector.connect(")
    for path in _py_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(needle in text for needle in needles):
            rel = _rel(path)
            if rel not in MYSQL_DBAPI_ALLOWED:
                offenders.append(rel)

    assert offenders == []


def test_production_code_uses_shared_child_env_helper():
    offenders: list[str] = []
    for path in _py_files():
        rel = _rel(path)
        if rel in ENV_COPY_ALLOWED:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "os.environ.copy()" in text:
            offenders.append(rel)

    assert offenders == []


def test_production_code_does_not_use_shell_wrappers_for_subprocesses():
    offenders: list[str] = []
    needles = ("os.system(", "shell=True")
    for path in _py_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(needle in text for needle in needles):
            offenders.append(_rel(path))

    assert offenders == []


def test_production_code_has_no_local_machine_absolute_paths():
    offenders: list[str] = []
    needles = ("C:\\Users\\Administrator", "E:\\My Code\\ProBigA")
    for path in _py_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(needle in text for needle in needles):
            offenders.append(_rel(path))

    assert offenders == []


def test_production_scripts_have_no_local_machine_absolute_paths():
    offenders: list[str] = []
    needles = ("C:\\Users\\Administrator", "E:\\My Code\\ProBigA", "E:/probiga_dump.sql")
    suffixes = {".py", ".ps1", ".bat", ".cmd"}
    for package in PRODUCTION_PACKAGES:
        for path in (ROOT / package).rglob("*"):
            if path.suffix.lower() not in suffixes:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if any(needle in text for needle in needles):
                offenders.append(_rel(path))

    assert offenders == []


def test_server_subprocess_run_calls_have_explicit_bounded_timeout():
    offenders: list[str] = []
    names = {"run", "call", "check_call", "check_output"}
    for path in (ROOT / "server").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or _subprocess_call_name(node) not in names:
                continue
            timeout_kw = next((kw for kw in node.keywords if kw.arg == "timeout"), None)
            if timeout_kw is None or (
                isinstance(timeout_kw.value, ast.Constant) and timeout_kw.value.value is None
            ):
                offenders.append(f"{_rel(path)}:{node.lineno}")

    assert offenders == []


def test_production_subprocess_run_calls_have_explicit_bounded_timeout():
    offenders: list[str] = []
    names = {"run", "call", "check_call", "check_output"}
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or _subprocess_call_name(node) not in names:
                continue
            timeout_kw = next((kw for kw in node.keywords if kw.arg == "timeout"), None)
            if timeout_kw is None or (
                isinstance(timeout_kw.value, ast.Constant) and timeout_kw.value.value is None
            ):
                offenders.append(f"{_rel(path)}:{node.lineno}")

    assert offenders == []


def test_remote_ssh_exec_commands_have_explicit_timeout():
    offenders: list[str] = []
    for package in ("tools", "deploy"):
        for path in (ROOT / package).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                if node.func.attr != "exec_command":
                    continue
                owner = node.func.value.id if isinstance(node.func.value, ast.Name) else ""
                if owner in {"ssh", "client"} and not any(kw.arg == "timeout" for kw in node.keywords):
                    offenders.append(f"{_rel(path)}:{node.lineno}")

    assert offenders == []


def test_remote_ssh_connect_calls_use_shared_kwargs():
    offenders: list[str] = []
    for package in ("tools", "deploy"):
        for path in (ROOT / package).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                if node.func.attr != "connect":
                    continue
                owner = node.func.value.id if isinstance(node.func.value, ast.Name) else ""
                if owner not in {"ssh", "client"}:
                    continue
                has_shared_kwargs = any(
                    kw.arg is None
                    and isinstance(kw.value, ast.Call)
                    and _call_name(kw.value) == "ssh_connect_kwargs"
                    for kw in node.keywords
                )
                if not has_shared_kwargs:
                    offenders.append(f"{_rel(path)}:{node.lineno}")

    assert offenders == []


def test_production_http_calls_have_explicit_timeout():
    offenders: list[str] = []
    network_names = {"get", "post", "put", "delete", "request"}
    direct_owners = {"requests", "httpx"}
    common_client_names = {"client", "session", "SESSION"}

    for path in _py_files():
        rel = _rel(path)
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        http_client_aliases = _http_client_aliases(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in network_names:
                continue
            owner = node.func.value
            owner_name = owner.id if isinstance(owner, ast.Name) else ""
            inline_client = isinstance(owner, ast.Call) and _is_http_client_constructor(owner)
            if (
                owner_name in direct_owners
                or owner_name in http_client_aliases
                or inline_client
                or (owner_name in common_client_names and rel not in HTTP_CLIENT_NAME_FALSE_POSITIVES)
            ):
                if not any(kw.arg == "timeout" for kw in node.keywords):
                    offenders.append(f"{rel}:{node.lineno}")

    assert offenders == []


def test_production_httpx_clients_have_explicit_timeout():
    offenders: list[str] = []
    for path in _py_files():
        rel = _rel(path)
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if not (isinstance(node.func.value, ast.Name) and node.func.value.id == "httpx"):
                continue
            if node.func.attr not in {"Client", "AsyncClient"}:
                continue
            if not any(kw.arg == "timeout" for kw in node.keywords):
                offenders.append(f"{rel}:{node.lineno}")

    assert offenders == []


def test_server_popen_usage_stays_in_lifecycle_managed_modules():
    offenders: list[str] = []
    for path in (ROOT / "server").rglob("*.py"):
        rel = _rel(path)
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _subprocess_call_name(node) == "Popen":
                if rel not in SERVER_POPEN_ALLOWED:
                    offenders.append(f"{rel}:{node.lineno}")

    assert offenders == []


def test_scripts_use_shared_dotenv_loader():
    offenders: list[str] = []
    needles = (
        "def _load_env",
        "def load_env",
        "def _load_project_env",
        'env_path = ROOT / ".env"',
    )
    for path in _py_files():
        rel = _rel(path)
        if rel in DOTENV_LOADING_ALLOWED:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(needle in text for needle in needles):
            offenders.append(rel)

    assert offenders == []


def test_remote_scripts_use_shared_remote_pythonpath_helper():
    offenders: list[str] = []
    for path in (ROOT / "tools").glob("_*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "{root}:{root}/adata" in text or "PYTHONPATH={root}:" in text:
            offenders.append(_rel(path))

    assert offenders == []


def test_remote_defaults_are_centralized():
    offenders: list[str] = []
    needles = ("47.113.123.190", "/opt/ProBigA")
    suffixes = {".py", ".ps1", ".bat", ".cmd"}
    for package in PRODUCTION_PACKAGES:
        for path in (ROOT / package).rglob("*"):
            if path.suffix.lower() not in suffixes:
                continue
            rel = _rel(path)
            if rel in REMOTE_DEFAULT_ALLOWED:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if any(needle in text for needle in needles):
                offenders.append(rel)

    assert offenders == []


def test_production_python_has_no_bare_except_handlers():
    offenders: list[str] = []
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler) and node.type is None:
                offenders.append(f"{_rel(path)}:{node.lineno}")

    assert offenders == []


def test_production_python_does_not_use_eval_or_exec():
    offenders: list[str] = []
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"eval", "exec"}:
                offenders.append(f"{_rel(path)}:{node.lineno}:{node.func.id}")

    assert offenders == []


def test_production_python_has_no_exact_exception_pass_handlers():
    offenders: list[str] = []
    for path in _py_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler):
                continue
            if not (isinstance(node.type, ast.Name) and node.type.id == "Exception"):
                continue
            if len(node.body) == 1 and isinstance(node.body[0], ast.Pass):
                offenders.append(f"{_rel(path)}:{node.lineno}")

    assert offenders == []


def test_hot_data_exception_fallbacks_are_observable():
    offenders: list[str] = []
    path = ROOT / "server" / "api" / "routers" / "hot_data.py"
    tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        if not (isinstance(node.type, ast.Name) and node.type.id == "Exception"):
            continue
        if len(node.body) == 1 and isinstance(node.body[0], ast.Pass):
            offenders.append(f"{_rel(path)}:{node.lineno}")

    assert offenders == []

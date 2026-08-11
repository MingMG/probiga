"""Static SQL boundary for the clean-room V4 control plane.

The V4 runtime may persist decision-control facts only.  Account, cash,
position, order, fill and risk-ledger tables remain owned by the canonical V2
ledger and are intentionally absent from this allowlist.
"""

from __future__ import annotations

import ast
import hashlib
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
V4_ROOT = PROJECT_ROOT / "server" / "trading_v4"
V4_MIGRATION = PROJECT_ROOT / "server" / "db" / "migrations_v4.py"

ALLOWED_V4_TABLES = frozenset(
    {
        "schema_migration_v4",
        "st_decision_channel_head_v4",
        "st_decision_context_v4",
        "st_decision_run_v4",
        "st_job_claim_token_v4",
        "st_job_run_v4",
        "st_runtime_control_transition_v4",
        "st_runtime_control_v4",
        "st_source_watermark_v4",
        "st_data_source_certification_v4",
        "st_factor_definition_v4",
        "st_entity_feature_snapshot_v4",
    }
)
ALLOWED_METADATA_TABLES = frozenset(
    {
        "information_schema.columns",
        "information_schema.key_column_usage",
        "information_schema.referential_constraints",
        "information_schema.schemata",
        "information_schema.statistics",
        "information_schema.tables",
        "information_schema.triggers",
    }
)
_TABLE_REFERENCE = re.compile(
    r"\b(?:"
    r"DELETE\s+FROM|FROM|JOIN|INSERT\s+INTO|REPLACE\s+INTO|"
    r"REFERENCES|UPDATE(?!\s+ON\b)|"
    r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|"
    r"DROP\s+(?:TEMPORARY\s+)?TABLE(?:\s+IF\s+EXISTS)?|"
    r"TRUNCATE(?:\s+TABLE)?|ALTER\s+TABLE|RENAME\s+TABLE"
    r")\s+`?([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)?)`?",
    flags=re.IGNORECASE,
)
_SQL_STATEMENT = re.compile(
    r"\b(?:SELECT\b|INSERT\s+INTO\b|UPDATE\s+[A-Za-z0-9_`.]"
    r"+\s+SET\b|DELETE\s+FROM\b|CREATE\s+TABLE\b|DROP\b|"
    r"TRUNCATE\b|ALTER\b|RENAME\b|REPLACE\b)",
    flags=re.IGNORECASE,
)
_DESTRUCTIVE_SQL = re.compile(
    r"(?:\A|;)"
    r"(?:\s|/\*.*?\*/|--[^\r\n]*(?:\r?\n|$))*"
    r"(?P<verb>DROP|TRUNCATE|ALTER|RENAME|REPLACE)\b",
    flags=re.IGNORECASE | re.DOTALL,
)
_SAFE_ASSIGNMENT_FRAGMENT = re.compile(
    r"\A[A-Za-z_][A-Za-z0-9_]*\s*=\s*(?:"
    r":[A-Za-z_][A-Za-z0-9_]*|"
    r"COALESCE\(\s*[A-Za-z_][A-Za-z0-9_]*\s*,\s*"
    r":[A-Za-z_][A-Za-z0-9_]*\s*\)"
    r")\Z",
    flags=re.IGNORECASE,
)
_ALLOWED_LOCK_SUFFIXES = frozenset({"", " FOR UPDATE"})
_FROZEN_V4_EXPAND_ALTER_HASHES = frozenset(
    {
        "6c8b80c5e6f1e11337c51f3c4e61472836b9420330a740a4be768bb1e436073e",
        "cbb0d0d9cbbadf156d41fabb58102d79b708301c3187f017928765c8a996d404",
        "eeee0e351f70fb7b6a1b08f6ea1ca02d2622effdd3625e6efcbccbafec9a60f5",
        "1df74227b83569c6f6e8f4af316dbc3117b9866961a0fdf3f3f9c6ae2e974848",
        "f00a4169035f437cdcb92119b6a54604101295a908068b2527b4a004109904b2",
        "8dfc8d4e862c53b71c36712bb92069b55a6134c97506ebc2846605aacf6097dc",
        "080a2afe1a0230bcf5e78b1d09055607949aced4c6cd189e9ab72f2cecf36f3b",
    }
)


def _python_files() -> tuple[Path, ...]:
    return tuple(sorted((*V4_ROOT.rglob("*.py"), V4_MIGRATION)))


def _static_string(
    node: ast.AST,
    bindings: dict[str, str] | None = None,
) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and bindings is not None:
        return bindings.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_string(node.left, bindings)
        right = _static_string(node.right, bindings)
        return None if left is None or right is None else left + right
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
                continue
            if (
                isinstance(value, ast.FormattedValue)
                and value.conversion == -1
                and value.format_spec is None
            ):
                rendered = _static_string(value.value, bindings)
                if rendered is not None:
                    parts.append(rendered)
                    continue
            return None
        return "".join(parts)
    return None


def _module_static_bindings(tree: ast.Module) -> dict[str, str]:
    """Resolve only immutable-looking top-level string assignments."""

    bindings: dict[str, str] = {}
    for node in tree.body:
        target: ast.AST | None = None
        value: ast.AST | None = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            value = node.value
        if not isinstance(target, ast.Name) or value is None:
            continue
        rendered = _static_string(value, bindings)
        if rendered is not None:
            bindings[target.id] = rendered
    return bindings


def _static_strings(tree: ast.AST) -> tuple[tuple[int, str], ...]:
    values = {
        (node.lineno, rendered)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Constant, ast.BinOp, ast.JoinedStr))
        and (rendered := _static_string(node)) is not None
    }
    return tuple(sorted(values))


def _tree_sql_table_references(
    tree: ast.AST,
) -> tuple[tuple[int, str], ...]:
    references: set[tuple[int, str]] = set()
    for line_number, value in _static_strings(tree):
        if _SQL_STATEMENT.search(value) is None:
            continue
        references.update(
            (line_number, match.group(1).casefold())
            for match in _TABLE_REFERENCE.finditer(value)
        )
    return tuple(sorted(references))


def _literal_sql_table_references(path: Path) -> tuple[tuple[int, str], ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return _tree_sql_table_references(tree)


def _tree_destructive_sql(
    tree: ast.AST,
    *,
    allowed_expand_alter_hashes: frozenset[str] = frozenset(),
) -> tuple[tuple[int, str], ...]:
    violations: set[tuple[int, str]] = set()
    for line_number, value in _static_strings(tree):
        for match in _DESTRUCTIVE_SQL.finditer(value):
            fingerprint = hashlib.sha256(
                " ".join(value.casefold().split()).encode("utf-8")
            ).hexdigest()
            if (
                match.group("verb").upper() == "ALTER"
                and fingerprint in allowed_expand_alter_hashes
            ):
                continue
            violations.add((line_number, match.group("verb").upper()))
    return tuple(sorted(violations))


def _concat_parts(node: ast.AST) -> list[ast.AST]:
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _concat_parts(node.left) + _concat_parts(node.right)
    return [node]


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _scope_walk(scope: ast.AST):
    """Walk one lexical scope without borrowing facts from nested scopes."""

    stack = list(reversed(list(ast.iter_child_nodes(scope))))
    while stack:
        node = stack.pop()
        if isinstance(
            node,
            (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef),
        ):
            continue
        yield node
        stack.extend(reversed(list(ast.iter_child_nodes(node))))


def _target_names(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, (ast.Tuple, ast.List)):
        return {
            name
            for item in node.elts
            for name in _target_names(item)
        }
    if isinstance(node, ast.Starred):
        return _target_names(node.value)
    if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
        return {node.value.id}
    return set()


def _scope_static_bindings(
    scope: ast.AST,
    module_bindings: dict[str, str],
) -> dict[str, str]:
    bindings = dict(module_bindings)
    ambiguous: set[str] = set()
    if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
        arguments = (
            *scope.args.posonlyargs,
            *scope.args.args,
            *scope.args.kwonlyargs,
        )
        ambiguous.update(argument.arg for argument in arguments)
        if scope.args.vararg is not None:
            ambiguous.add(scope.args.vararg.arg)
        if scope.args.kwarg is not None:
            ambiguous.add(scope.args.kwarg.arg)
    for node in _scope_walk(scope):
        targets: set[str] = set()
        value: ast.AST | None = None
        if isinstance(node, ast.Assign):
            targets = {
                name
                for target in node.targets
                for name in _target_names(target)
            }
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = _target_names(node.target)
            value = node.value
        elif isinstance(node, (ast.AugAssign, ast.NamedExpr)):
            targets = _target_names(node.target)
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            targets = _target_names(node.target)
        if not targets:
            continue
        rendered = (
            _static_string(value, bindings)
            if value is not None and len(targets) == 1
            else None
        )
        for name in targets:
            if name in ambiguous or rendered is None:
                ambiguous.add(name)
                bindings.pop(name, None)
            elif name in bindings and bindings[name] != rendered:
                ambiguous.add(name)
                bindings.pop(name, None)
            else:
                bindings[name] = rendered
    for name in ambiguous:
        bindings.pop(name, None)
    return bindings


def _static_string_choices(
    node: ast.AST,
    bindings: dict[str, str],
) -> frozenset[str] | None:
    rendered = _static_string(node, bindings)
    if rendered is not None:
        return frozenset({rendered})
    if isinstance(node, ast.IfExp):
        body = _static_string_choices(node.body, bindings)
        otherwise = _static_string_choices(node.orelse, bindings)
        if body is not None and otherwise is not None:
            return body | otherwise
    return None


def _for_update_helper_is_restricted(
    tree: ast.Module,
    bindings: dict[str, str],
) -> bool:
    helpers = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_for_update"
    ]
    if len(helpers) != 1:
        return False
    returns = [
        node
        for node in _scope_walk(helpers[0])
        if isinstance(node, ast.Return) and node.value is not None
    ]
    if not returns:
        return False
    choices: set[str] = set()
    for node in returns:
        rendered = _static_string_choices(node.value, bindings)
        if rendered is None:
            return False
        choices.update(rendered)
    return bool(choices) and choices <= _ALLOWED_LOCK_SUFFIXES


def _is_restricted_lock_expression(
    node: ast.AST,
    *,
    helper_is_restricted: bool,
    bindings: dict[str, str],
) -> bool:
    rendered = _static_string(node, bindings)
    if rendered is not None:
        return rendered in _ALLOWED_LOCK_SUFFIXES
    if isinstance(node, ast.IfExp):
        return _is_restricted_lock_expression(
            node.body,
            helper_is_restricted=helper_is_restricted,
            bindings=bindings,
        ) and _is_restricted_lock_expression(
            node.orelse,
            helper_is_restricted=helper_is_restricted,
            bindings=bindings,
        )
    return bool(
        helper_is_restricted
        and isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "self"
        and node.func.attr == "_for_update"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "connection"
        and not node.keywords
    )


def _approved_lock_suffix_names(
    scope: ast.AST,
    *,
    helper_is_restricted: bool,
    bindings: dict[str, str],
) -> frozenset[str]:
    writes: dict[str, list[ast.AST | None]] = {}
    for node in _scope_walk(scope):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    writes.setdefault(target.id, []).append(node.value)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            writes.setdefault(node.target.id, []).append(node.value)
        elif isinstance(node, (ast.AugAssign, ast.NamedExpr)) and isinstance(
            node.target,
            ast.Name,
        ):
            writes.setdefault(node.target.id, []).append(None)
    return frozenset(
        name
        for name, values in writes.items()
        if values
        and all(
            value is not None
            and _is_restricted_lock_expression(
                value,
                helper_is_restricted=helper_is_restricted,
                bindings=bindings,
            )
            for value in values
        )
        and any(
            value is not None
            and _static_string(value, bindings) is None
            for value in values
        )
    )


def _safe_assignment_values(
    node: ast.AST,
    bindings: dict[str, str],
) -> bool:
    if not isinstance(node, (ast.List, ast.Tuple)):
        return False
    values = [_static_string(item, bindings) for item in node.elts]
    return bool(values) and all(
        value is not None and _SAFE_ASSIGNMENT_FRAGMENT.fullmatch(value)
        for value in values
    )


def _approved_assignment_join_names(
    scope: ast.AST,
    bindings: dict[str, str],
) -> frozenset[str]:
    nodes = tuple(_scope_walk(scope))
    parents = {
        id(child): parent
        for parent in nodes
        for child in ast.iter_child_nodes(parent)
    }
    candidates = {
        call.args[0].id
        for call in nodes
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and _static_string(call.func.value, bindings) == ", "
        and call.func.attr == "join"
        and len(call.args) == 1
        and isinstance(call.args[0], ast.Name)
    }
    approved: set[str] = set()
    for name in candidates:
        initialized = False
        valid = True
        for node in nodes:
            if isinstance(node, ast.Assign) and any(
                name in _target_names(target) for target in node.targets
            ):
                if (
                    initialized
                    or len(node.targets) != 1
                    or not isinstance(node.targets[0], ast.Name)
                    or not _safe_assignment_values(node.value, bindings)
                ):
                    valid = False
                initialized = True
            elif isinstance(node, ast.AnnAssign) and name in _target_names(
                node.target
            ):
                if initialized or not _safe_assignment_values(
                    node.value,
                    bindings,
                ):
                    valid = False
                initialized = True
            elif isinstance(node, (ast.AugAssign, ast.NamedExpr)) and name in (
                _target_names(node.target)
            ):
                valid = False
            elif isinstance(node, (ast.For, ast.AsyncFor)) and name in (
                _target_names(node.target)
            ):
                valid = False
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == name
            ):
                if node.func.attr == "append":
                    valid = valid and len(node.args) == 1 and bool(
                        (value := _static_string(node.args[0], bindings))
                        is not None
                        and _SAFE_ASSIGNMENT_FRAGMENT.fullmatch(value)
                    )
                elif node.func.attr == "extend":
                    valid = valid and len(node.args) == 1 and (
                        _safe_assignment_values(node.args[0], bindings)
                    )
                else:
                    valid = False
            elif (
                isinstance(node, ast.Name)
                and node.id == name
                and isinstance(node.ctx, ast.Load)
            ):
                parent = parents.get(id(node))
                grandparent = parents.get(id(parent)) if parent is not None else None
                mutation = (
                    isinstance(parent, ast.Attribute)
                    and parent.value is node
                    and isinstance(grandparent, ast.Call)
                    and grandparent.func is parent
                    and parent.attr in {"append", "extend"}
                )
                joined = (
                    isinstance(parent, ast.Call)
                    and node in parent.args
                    and isinstance(parent.func, ast.Attribute)
                    and parent.func.attr == "join"
                    and _static_string(parent.func.value, bindings) == ", "
                )
                if not mutation and not joined:
                    valid = False
        if initialized and valid:
            approved.add(name)
    return frozenset(approved)


def _scope_write_nodes(scope: ast.AST, name: str) -> tuple[ast.AST, ...]:
    writes: list[ast.AST] = []
    for node in _scope_walk(scope):
        targets: tuple[ast.AST, ...] = ()
        if isinstance(node, ast.Assign):
            targets = tuple(node.targets)
        elif isinstance(node, ast.AnnAssign):
            targets = (node.target,)
        elif isinstance(node, (ast.AugAssign, ast.NamedExpr)):
            targets = (node.target,)
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            targets = (node.target,)
        if any(name in _target_names(target) for target in targets):
            writes.append(node)
    return tuple(writes)


def _migration_statement_loop_is_restricted(scope: ast.AST) -> bool:
    migration_writes = _scope_write_nodes(scope, "migration")
    statements_writes = _scope_write_nodes(scope, "statements")
    statement_writes = _scope_write_nodes(scope, "statement")
    if not (
        len(migration_writes) == 1
        and len(statements_writes) == 1
        and len(statement_writes) == 1
    ):
        return False
    migration_loop = migration_writes[0]
    statements_assignment = statements_writes[0]
    statement_loop = statement_writes[0]
    if not (
        isinstance(migration_loop, (ast.For, ast.AsyncFor))
        and isinstance(migration_loop.target, ast.Name)
        and migration_loop.target.id == "migration"
        and isinstance(migration_loop.iter, ast.Name)
        and migration_loop.iter.id == "MIGRATIONS"
        and isinstance(statements_assignment, ast.Assign)
        and len(statements_assignment.targets) == 1
        and isinstance(statements_assignment.targets[0], ast.Name)
        and isinstance(statements_assignment.value, ast.Call)
        and isinstance(statements_assignment.value.func, ast.Name)
        and statements_assignment.value.func.id == "tuple"
        and len(statements_assignment.value.args) == 1
        and isinstance(statements_assignment.value.args[0], ast.Subscript)
        and isinstance(statements_assignment.value.args[0].value, ast.Name)
        and statements_assignment.value.args[0].value.id == "migration"
        and _static_string(statements_assignment.value.args[0].slice)
        == "statements"
        and isinstance(statement_loop, (ast.For, ast.AsyncFor))
        and isinstance(statement_loop.target, ast.Name)
        and statement_loop.target.id == "statement"
        and isinstance(statement_loop.iter, ast.Name)
        and statement_loop.iter.id == "statements"
    ):
        return False
    return statement_loop in migration_loop.body


def _trusted_dynamic_text_names(
    scope: ast.AST,
    function_name: str,
) -> frozenset[str]:
    if function_name in {
        "_idempotent_insert",
        "_execute_mysql_regexp_compatible_statement",
    } and isinstance(
        scope,
        (ast.FunctionDef, ast.AsyncFunctionDef),
    ):
        parameters = {
            argument.arg
            for argument in (
                *scope.args.posonlyargs,
                *scope.args.args,
                *scope.args.kwonlyargs,
            )
        }
        if "statement" in parameters and not _scope_write_nodes(
            scope,
            "statement",
        ):
            return frozenset({"statement"})
    if (
        function_name == "_run_v4_migrations_unlocked"
        and _migration_statement_loop_is_restricted(scope)
    ):
        return frozenset({"statement"})
    return frozenset()


def _sql_text_argument_is_static(
    node: ast.AST,
    *,
    bindings: dict[str, str] | None = None,
    approved_lock_suffixes: frozenset[str] = frozenset(),
    approved_assignment_joins: frozenset[str] = frozenset(),
) -> bool:
    bindings = bindings or {}
    if _static_string(node, bindings) is not None:
        return True
    parts = _concat_parts(node)
    if not parts:
        return False
    first = _static_string(parts[0], bindings)
    if (
        first is None
        or _SQL_STATEMENT.search(first) is None
        or _TABLE_REFERENCE.search(first) is None
    ):
        return False
    for part in parts[1:]:
        if _static_string(part, bindings) is not None:
            continue
        if isinstance(part, ast.Name) and part.id in approved_lock_suffixes:
            continue
        approved_assignment_join = (
            isinstance(part, ast.Call)
            and isinstance(part.func, ast.Attribute)
            and part.func.attr == "join"
            and _static_string(part.func.value, bindings) == ", "
            and len(part.args) == 1
            and isinstance(part.args[0], ast.Name)
            and part.args[0].id in approved_assignment_joins
        )
        if not approved_assignment_join:
            return False
    return True


def _tree_dynamic_runtime_sql(
    tree: ast.Module,
) -> tuple[tuple[int, str], ...]:
    violations: list[tuple[int, str]] = []
    module_bindings = _module_static_bindings(tree)
    text_names = {"text"}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and (
            node.module or ""
        ).startswith("sqlalchemy"):
            text_names.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "text"
            )
    helper_is_restricted = _for_update_helper_is_restricted(
        tree,
        module_bindings,
    )
    state: list[
        tuple[
            str,
            dict[str, str],
            frozenset[str],
            frozenset[str],
            frozenset[str],
            frozenset[str],
        ]
    ] = []

    def is_text_call(node: ast.AST) -> bool:
        return bool(
            isinstance(node, ast.Call)
            and (
                (
                    isinstance(node.func, ast.Name)
                    and node.func.id in text_names
                )
                or (
                    isinstance(node.func, ast.Attribute)
                    and node.func.attr == "text"
                )
            )
        )

    def trusted_dynamic_parameter(
        trusted_names: frozenset[str],
        node: ast.AST,
    ) -> bool:
        return bool(
            isinstance(node, ast.Name)
            and node.id in trusted_names
        )

    def text_argument_is_approved(
        function_name: str,
        argument: ast.AST,
        bindings: dict[str, str],
        lock_suffixes: frozenset[str],
        assignment_joins: frozenset[str],
        trusted_names: frozenset[str],
    ) -> bool:
        mysql_regexp_compatibility_boundary = bool(
            isinstance(argument, ast.Call)
            and _call_name(argument.func)
            == "_mysql_regexp_compatible_statement"
            and len(argument.args) == 2
            and isinstance(argument.args[0], ast.Name)
            and argument.args[0].id == "connection"
            and trusted_dynamic_parameter(
                trusted_names,
                argument.args[1],
            )
            and not argument.keywords
        )
        return (
            trusted_dynamic_parameter(trusted_names, argument)
            or mysql_regexp_compatibility_boundary
            or (
            _sql_text_argument_is_static(
                argument,
                bindings=bindings,
                approved_lock_suffixes=lock_suffixes,
                approved_assignment_joins=assignment_joins,
            )
            )
        )

    def approved_text_variables(
        scope: ast.AST,
        function_name: str,
        bindings: dict[str, str],
        lock_suffixes: frozenset[str],
        assignment_joins: frozenset[str],
        trusted_names: frozenset[str],
    ) -> frozenset[str]:
        writes: dict[str, list[ast.AST | None]] = {}
        for node in _scope_walk(scope):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        writes.setdefault(target.id, []).append(node.value)
            elif isinstance(node, ast.AnnAssign) and isinstance(
                node.target,
                ast.Name,
            ):
                writes.setdefault(node.target.id, []).append(node.value)
            elif isinstance(node, (ast.AugAssign, ast.NamedExpr)) and isinstance(
                node.target,
                ast.Name,
            ):
                writes.setdefault(node.target.id, []).append(None)
        return frozenset(
            name
            for name, values in writes.items()
            if values
            and all(
                value is not None
                and is_text_call(value)
                and bool(value.args)
                and text_argument_is_approved(
                    function_name,
                    value.args[0],
                    bindings,
                    lock_suffixes,
                    assignment_joins,
                    trusted_names,
                )
                for value in values
            )
        )

    def push_scope(node: ast.AST, function_name: str) -> None:
        bindings = _scope_static_bindings(node, module_bindings)
        lock_suffixes = _approved_lock_suffix_names(
            node,
            helper_is_restricted=helper_is_restricted,
            bindings=bindings,
        )
        assignment_joins = _approved_assignment_join_names(node, bindings)
        trusted_names = _trusted_dynamic_text_names(node, function_name)
        safe_text_variables = approved_text_variables(
            node,
            function_name,
            bindings,
            lock_suffixes,
            assignment_joins,
            trusted_names,
        )
        state.append(
            (
                function_name,
                bindings,
                lock_suffixes,
                assignment_joins,
                safe_text_variables,
                trusted_names,
            )
        )

    class Visitor(ast.NodeVisitor):
        def visit_Module(self, node: ast.Module) -> None:
            push_scope(node, "")
            self.generic_visit(node)
            state.pop()

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            push_scope(node, node.name)
            self.generic_visit(node)
            state.pop()

        visit_AsyncFunctionDef = visit_FunctionDef

        def visit_Call(self, node: ast.Call) -> None:
            (
                function_name,
                bindings,
                lock_suffixes,
                assignment_joins,
                safe_text_variables,
                trusted_names,
            ) = state[-1]
            called_name = _call_name(node.func)
            if is_text_call(node) and node.args:
                if not text_argument_is_approved(
                    function_name,
                    node.args[0],
                    bindings,
                    lock_suffixes,
                    assignment_joins,
                    trusted_names,
                ):
                    violations.append((node.lineno, "dynamic text() SQL"))
            if called_name == "_idempotent_insert":
                statement_argument = (
                    node.args[1] if len(node.args) >= 2 else None
                )
                if statement_argument is None:
                    statement_argument = next(
                        (
                            keyword.value
                            for keyword in node.keywords
                            if keyword.arg == "statement"
                        ),
                        None,
                    )
                if statement_argument is None or _static_string(
                    statement_argument,
                    bindings,
                ) is None:
                    violations.append(
                        (node.lineno, "dynamic idempotent INSERT SQL")
                    )
            if called_name == "_execute_mysql_regexp_compatible_statement":
                statement_argument = (
                    node.args[1] if len(node.args) >= 2 else None
                )
                if statement_argument is None:
                    statement_argument = next(
                        (
                            keyword.value
                            for keyword in node.keywords
                            if keyword.arg == "statement"
                        ),
                        None,
                    )
                if statement_argument is None or _static_string(
                    statement_argument,
                    bindings,
                ) is None:
                    violations.append(
                        (node.lineno, "dynamic MySQL REGEXP compatibility SQL")
                    )
            if called_name == "exec_driver_sql":
                violations.append((node.lineno, "exec_driver_sql() is forbidden"))
            elif called_name in {"execute", "executemany"} and node.args:
                statement = node.args[0]
                approved_statement = is_text_call(statement) or (
                    isinstance(statement, ast.Name)
                    and statement.id in safe_text_variables
                )
                if not approved_statement:
                    violations.append(
                        (node.lineno, f"raw/unchecked {called_name}() SQL")
                    )
            self.generic_visit(node)

    Visitor().visit(tree)
    return tuple(sorted(set(violations)))


def _dynamic_runtime_sql(path: Path) -> tuple[tuple[int, str], ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return _tree_dynamic_runtime_sql(tree)


def test_sql_scanner_reassembles_split_table_names():
    tree = ast.parse(
        "SQL = 'SELECT * FROM ' + 'st_position_' + 'state_v3'\n"
    )
    assert (1, "st_position_state_v3") in _tree_sql_table_references(tree)


def test_sql_scanner_distinguishes_update_dml_from_trigger_event_syntax():
    tree = ast.parse(
        "TRIGGER = 'CREATE TRIGGER guard BEFORE UPDATE ON "
        "st_decision_context_v4 FOR EACH ROW BEGIN SELECT 1; END'\n"
        "DML = 'UPDATE st_runtime_control_v4 SET version = 2'\n"
    )
    references = _tree_sql_table_references(tree)
    assert (1, "on") not in references
    assert (1, "st_decision_context_v4") not in references
    assert (2, "st_runtime_control_v4") in references


def test_sql_guard_rejects_every_destructive_statement_family():
    sources = {
        "DROP": "text('DR' + 'OP TABLE st_decision_context_v4')\n",
        "TRUNCATE": "text('TRUNCATE TABLE st_decision_context_v4')\n",
        "ALTER": "text('ALTER TABLE st_decision_context_v4 ADD x INT')\n",
        "RENAME": (
            "text('RENAME TABLE st_decision_context_v4 "
            "TO st_decision_context_v4_old')\n"
        ),
        "REPLACE": (
            "text('REPLACE INTO st_decision_context_v4 (context_id) "
            "VALUES (1)')\n"
        ),
    }
    for expected_verb, source in sources.items():
        assert any(
            verb == expected_verb
            for _, verb in _tree_destructive_sql(ast.parse(source))
        ), expected_verb


def test_sql_entry_point_guard_rejects_raw_and_driver_sql():
    sources = (
        "connection.execute('SELECT * FROM st_decision_context_v4')\n",
        "sql = 'SELECT * FROM st_decision_context_v4'\nconnection.execute(sql)\n",
        "connection.exec_driver_sql('SELECT 1')\n",
        "sa.text('SELECT * FROM ' + table_name)\n",
    )
    for source in sources:
        assert _tree_dynamic_runtime_sql(ast.parse(source)), source


def test_sql_guard_does_not_allow_an_arbitrary_suffix_variable():
    tree = ast.parse(
        "def query(connection, suffix):\n"
        "    return connection.execute(\n"
        "        text('SELECT * FROM st_decision_context_v4' + suffix)\n"
        "    )\n"
    )
    assert any(
        reason == "dynamic text() SQL"
        for _, reason in _tree_dynamic_runtime_sql(tree)
    )


def test_sql_guard_allows_only_the_restricted_for_update_suffix():
    tree = ast.parse(
        "class Repository:\n"
        "    @staticmethod\n"
        "    def _for_update(connection):\n"
        "        return ' FOR UPDATE' if connection else ''\n"
        "\n"
        "    def query(self, connection, for_update=False):\n"
        "        suffix = self._for_update(connection) if for_update else ''\n"
        "        return connection.execute(\n"
        "            text('SELECT * FROM st_decision_context_v4' + suffix)\n"
        "        )\n"
    )
    assert not _tree_dynamic_runtime_sql(tree)


def test_v4_sql_references_only_explicit_control_plane_tables():
    allowed = ALLOWED_V4_TABLES | ALLOWED_METADATA_TABLES
    violations = [
        f"{path.relative_to(PROJECT_ROOT)}:{line_number} -> {table_name}"
        for path in _python_files()
        for line_number, table_name in _literal_sql_table_references(path)
        if table_name not in allowed
    ]
    assert not violations, "V4 SQL crossed its table allowlist:\n" + "\n".join(
        violations
    )


def test_v4_application_runtime_does_not_query_schema_metadata():
    violations = [
        f"{path.relative_to(PROJECT_ROOT)}:{line_number} -> {table_name}"
        for path in sorted(V4_ROOT.rglob("*.py"))
        for line_number, table_name in _literal_sql_table_references(path)
        if table_name in ALLOWED_METADATA_TABLES
    ]
    assert not violations, "schema inspection leaked into V4 runtime:\n" + "\n".join(
        violations
    )


def test_v4_sql_is_expand_only_and_contains_no_destructive_statement():
    violations = [
        f"{path.relative_to(PROJECT_ROOT)}:{line_number} -> {verb}"
        for path in _python_files()
        for line_number, verb in _tree_destructive_sql(
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path)),
            allowed_expand_alter_hashes=(
                _FROZEN_V4_EXPAND_ALTER_HASHES
                if path == V4_MIGRATION
                else frozenset()
            ),
        )
    ]
    assert not violations, "destructive V4 SQL detected:\n" + "\n".join(
        violations
    )


def test_v4_runtime_sql_does_not_build_table_names_dynamically():
    violations = [
        f"{path.relative_to(PROJECT_ROOT)}:{line_number} -> {reason}"
        for path in _python_files()
        for line_number, reason in _dynamic_runtime_sql(path)
    ]
    assert not violations, "dynamic V4 SQL detected:\n" + "\n".join(
        violations
    )

    synthetic = ast.parse("text('SELECT * FROM ' + table_name)\n")
    expression = synthetic.body[0].value
    assert isinstance(expression, ast.Call)
    assert not _sql_text_argument_is_static(expression.args[0])


def test_mysql_regexp_compatibility_boundary_requires_static_callers():
    static_call = ast.parse(
        "_execute_mysql_regexp_compatible_statement("
        "connection, 'SELECT COUNT(*) FROM st_job_run_v4')\n"
    )
    assert not _tree_dynamic_runtime_sql(static_call)

    dynamic_call = ast.parse(
        "_execute_mysql_regexp_compatible_statement(connection, user_sql)\n"
    )
    assert (
        1,
        "dynamic MySQL REGEXP compatibility SQL",
    ) in _tree_dynamic_runtime_sql(dynamic_call)

    bypass = ast.parse(
        "def unsafe(connection, user_sql):\n"
        "    connection.execute(text("
        "_mysql_regexp_compatible_statement(connection, user_sql)))\n"
    )
    assert (2, "dynamic text() SQL") in _tree_dynamic_runtime_sql(bypass)


def test_v4_allowlist_contains_no_mechanical_ledger_tables():
    forbidden_fragments = (
        "account",
        "cash",
        "fill",
        "lot",
        "order",
        "position",
        "risk_ledger",
    )
    assert not {
        table_name
        for table_name in ALLOWED_V4_TABLES
        if any(fragment in table_name for fragment in forbidden_fragments)
    }

# -*- coding: utf-8 -*-
"""Code-owned execution adapters for dynamically registered strategies.

Database registration is intentionally not enough to make a strategy
executable.  A dynamic strategy must bind its immutable version to a deployed
adapter artifact and cost model, and that exact adapter must be present in this
process.  The registry has no fixed strategy-count limit; new code-owned
adapters can be registered without changing the governance catalogue.
"""
from __future__ import annotations

import hashlib
import builtins
import copy
import dis
import inspect
import json
import marshal
import os
from pathlib import Path
import re
import subprocess
import sys
import sysconfig
import threading
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from types import CodeType, FunctionType, MappingProxyType, ModuleType
from typing import Any, Callable, Iterable, Mapping

from sqlalchemy import text


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_ADAPTER_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{2,79}$")
_ADAPTER_VERSION_PATTERN = re.compile(r"^[0-9A-Za-z][0-9A-Za-z_.:-]{0,159}$")
_COST_FIELDS = (
    "commission_pct",
    "stamp_tax_pct",
    "slippage_pct",
    "transfer_fee_pct",
)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEPENDENCY_LOCK_RELATIVE_PATH = Path("deploy/production_requirements.lock")
_SAFE_BUILTIN_GLOBALS = frozenset({
    "abs", "all", "any", "bool", "dict", "enumerate", "filter", "float",
    "frozenset", "int", "isinstance", "len", "list", "map", "max", "min",
    "range", "reversed", "round", "set", "sorted", "str", "sum", "tuple",
    "zip",
})
_HELPER_SAFE_BUILTIN_GLOBALS = _SAFE_BUILTIN_GLOBALS | frozenset({
    "Exception", "RuntimeError", "TypeError", "ValueError",
})
_FORBIDDEN_GLOBAL_OPS = frozenset({
    "STORE_GLOBAL", "DELETE_GLOBAL", "STORE_DEREF", "DELETE_DEREF",
    "IMPORT_NAME", "IMPORT_FROM",
})
_AUDIT_RECEIPT_FIELDS = frozenset({
    "run_uid",
    "completed_at",
    "receipt_hash",
    "generated_at",
    "cache_status",
    "candidate_run_uid",
    "candidate_completed_at",
    "candidate_receipt_hash",
    "candidate_run_receipt",
    "completed_run_uid",
    "persisted_run_uid",
})
_CANDIDATE_CONTEXT_FIELDS = frozenset({
    "trade_date", "recommendation_rows", "market", "configs", "metrics",
})


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def normalize_cost_model(value: Any) -> dict[str, Any]:
    """Return the exact immutable cost model accepted for dynamic funding."""

    if not isinstance(value, dict):
        raise ValueError("动态策略必须声明不可变成本模型")
    unknown = sorted(set(value) - {
        "model_key", "currency", "round_trip_cost_pct", *_COST_FIELDS,
    })
    if unknown:
        raise ValueError("成本模型包含不受支持字段：" + "、".join(unknown))
    model_key = str(value.get("model_key") or "").strip()
    currency = str(value.get("currency") or "CNY").strip().upper()
    if not _ADAPTER_KEY_PATTERN.fullmatch(model_key):
        raise ValueError("成本模型代码无效")
    if currency != "CNY":
        raise ValueError("当前策略治理只接受CNY成本模型")
    result: dict[str, Any] = {"model_key": model_key, "currency": currency}
    for field in _COST_FIELDS:
        raw = value.get(field)
        if isinstance(raw, bool):
            raise ValueError(f"成本模型字段{field}无效")
        try:
            number = float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"成本模型字段{field}无效") from exc
        if not 0.0 <= number <= 10.0:
            raise ValueError(f"成本模型字段{field}必须在0至10之间")
        result[field] = round(number, 8)
    result["round_trip_cost_pct"] = round(
        result["commission_pct"] * 2
        + result["stamp_tax_pct"]
        + result["slippage_pct"] * 2
        + result["transfer_fee_pct"] * 2,
        8,
    )
    if value.get("round_trip_cost_pct") is not None:
        try:
            declared_round_trip = round(
                float(value.get("round_trip_cost_pct")), 8
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("成本模型往返成本汇总无效") from exc
        if declared_round_trip != result["round_trip_cost_pct"]:
            raise ValueError("成本模型往返成本汇总与明细不一致")
    return result


def normalize_execution_binding(
    value: Any, *, strategy_version: str,
) -> dict[str, Any]:
    """Validate and hash a version-bound dynamic adapter declaration."""

    if not isinstance(value, dict):
        raise ValueError("动态策略未声明执行适配器绑定")
    adapter_key = str(value.get("adapter_key") or "").strip().lower()
    adapter_version = str(value.get("adapter_version") or "").strip()
    bound_version = str(value.get("strategy_version") or "").strip()
    artifact_sha256 = str(value.get("artifact_sha256") or "").strip()
    if not _ADAPTER_KEY_PATTERN.fullmatch(adapter_key):
        raise ValueError("执行适配器代码无效")
    if not _ADAPTER_VERSION_PATTERN.fullmatch(adapter_version):
        raise ValueError("执行适配器版本无效")
    if bound_version != str(strategy_version or ""):
        raise ValueError("执行适配器未绑定当前策略版本")
    if not _SHA256_PATTERN.fullmatch(artifact_sha256):
        raise ValueError("执行适配器制品SHA必须是64位小写sha256")
    cost_model = normalize_cost_model(value.get("cost_model"))
    cost_model_hash = _digest(cost_model)
    normalized = {
        "adapter_key": adapter_key,
        "adapter_version": adapter_version,
        "strategy_version": bound_version,
        "artifact_sha256": artifact_sha256,
        "cost_model": cost_model,
        "cost_model_hash": cost_model_hash,
    }
    normalized["execution_binding_hash"] = _digest(normalized)
    return normalized


AdapterValidator = Callable[[Mapping[str, Any]], tuple[bool, str]]
CandidateBuilder = Callable[
    [Mapping[str, Any], Mapping[str, Any]], "CandidateBatch"
]


def _deployment_mode() -> str:
    return os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower()


def _trusted_release_root() -> Path:
    """Return the canonical, non-symlink code root used for adapter trust."""

    configured = str(os.environ.get("PROBIGA_CODE_ROOT") or "").strip()
    root = Path(configured) if configured else _PROJECT_ROOT
    try:
        absolute = root.absolute()
        resolved = root.resolve(strict=True)
    except OSError as exc:
        raise ValueError("执行适配器可信发布根不存在") from exc
    if absolute != resolved:
        raise ValueError("执行适配器可信发布根不能是符号链接")
    if _deployment_mode() == "production" and not configured:
        raise ValueError("生产执行适配器必须显式声明PROBIGA_CODE_ROOT")
    return resolved


def _trusted_regular_file(path: Path, root: Path, *, label: str) -> Path:
    """Resolve one immutable release file without following any symlink."""

    try:
        absolute = path.absolute()
        relative = absolute.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ValueError(f"{label}不在可信发布根内") from exc
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{label}路径包含符号链接")
    try:
        resolved = absolute.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"{label}不存在") from exc
    if resolved != absolute or not resolved.is_file():
        raise ValueError(f"{label}不是可信普通文件")
    return resolved


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _release_tree_sha256(root: Path) -> str:
    declared = str(
        os.environ.get("PROBIGA_RELEASE_TREE_SHA256") or ""
    ).strip().lower()
    if declared:
        if not _SHA256_PATTERN.fullmatch(declared):
            raise ValueError("发布树SHA必须是64位小写sha256")
        return declared
    if _deployment_mode() == "production":
        raise ValueError("生产执行适配器缺少可信发布树SHA")
    try:
        tree = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD^{tree}"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError("无法确定执行适配器发布树身份") from exc
    if not re.fullmatch(r"[0-9a-f]{40,64}", tree):
        raise ValueError("执行适配器发布树身份无效")
    return _digest({"kind": "git-tree", "tree": tree})


def _python_abi_contract() -> dict[str, str]:
    return {
        "implementation": sys.implementation.name,
        "cache_tag": str(sys.implementation.cache_tag or ""),
        "version": ".".join(str(item) for item in sys.version_info[:3]),
        "abiflags": str(getattr(sys, "abiflags", "")),
        "soabi": str(sysconfig.get_config_var("SOABI") or ""),
    }


def _immutable_global(value: Any) -> bool:
    if value is None or isinstance(value, (str, bytes, int, float, bool)):
        return True
    if isinstance(value, (tuple, frozenset)):
        return all(_immutable_global(item) for item in value)
    return False


def _module_dependency_contract(
    module: ModuleType, root: Path,
) -> dict[str, str] | None:
    raw_path = str(getattr(module, "__file__", "") or "")
    if not raw_path:
        return None
    path = Path(raw_path)
    try:
        path.absolute().relative_to(root)
    except ValueError:
        # Third-party and stdlib code are frozen by the dependency lock and ABI.
        return None
    trusted = _trusted_regular_file(path, root, label="项目依赖模块")
    return {
        "module": str(module.__name__),
        "relative_path": trusted.relative_to(root).as_posix(),
        "file_sha256": _file_sha256(trusted),
    }


def _immutable_runtime_value_contract(value: Any) -> Any:
    """Return a deterministic typed contract for immutable runtime values."""

    if value is None:
        return {"type": "none", "value": None}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, (str, bytes, int, float)):
        encoded = value.hex() if isinstance(value, bytes) else value
        return {"type": type(value).__name__, "value": encoded}
    if isinstance(value, tuple):
        return {
            "type": "tuple",
            "items": [_immutable_runtime_value_contract(item) for item in value],
        }
    if isinstance(value, frozenset):
        items = [_immutable_runtime_value_contract(item) for item in value]
        items.sort(key=_digest)
        return {"type": "frozenset", "items": items}
    raise ValueError("执行适配器函数依赖包含可变默认值或闭包状态")


def _function_runtime_dependency_contract(
    global_name: str,
    dependency: FunctionType,
    root: Path,
    *,
    ancestry: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Bind the exact helper symbol and code, not only its containing file."""

    if dependency.__closure__:
        raise ValueError(f"执行适配器helper包含闭包状态：{global_name}")
    module = inspect.getmodule(dependency)
    name = str(dependency.__name__ or "")
    qualname = str(dependency.__qualname__ or "")
    if (
        not isinstance(module, ModuleType)
        or not name
        or str(dependency.__module__ or "") != str(module.__name__)
        or module.__dict__.get(name) is not dependency
    ):
        raise ValueError(f"执行适配器helper不是声明模块的真实导出：{global_name}")
    module_contract = _module_dependency_contract(module, root)
    identity = f"{module.__name__}:{qualname}"
    if identity in ancestry:
        return {
            "global_name": global_name,
            "kind": "function_cycle_ref",
            "identity": identity,
        }
    nested_ancestry = (*ancestry, identity)
    runtime_dependencies: dict[str, dict[str, Any]] = {}
    for instruction in _code_instructions(dependency.__code__):
        if instruction.opname in _FORBIDDEN_GLOBAL_OPS:
            raise ValueError(
                f"执行适配器helper包含全局写入或运行期导入：{global_name}"
            )
        if instruction.opname not in {"LOAD_GLOBAL", "LOAD_NAME"}:
            continue
        nested_name = str(instruction.argval or "")
        if nested_name in runtime_dependencies:
            continue
        if nested_name in _HELPER_SAFE_BUILTIN_GLOBALS:
            runtime_dependencies[nested_name] = {
                "global_name": nested_name, "kind": "safe_builtin",
            }
            continue
        if nested_name in builtins.__dict__:
            raise ValueError(
                f"执行适配器helper LOAD_GLOBAL不在白名单：{nested_name}"
            )
        if nested_name not in dependency.__globals__:
            raise ValueError(
                f"执行适配器helper全局依赖缺失：{nested_name}"
            )
        nested = dependency.__globals__[nested_name]
        if isinstance(nested, ModuleType):
            runtime_dependencies[nested_name] = {
                "global_name": nested_name,
                "kind": "module",
                "module": str(nested.__name__),
                "module_file": _module_dependency_contract(nested, root),
            }
        elif type(nested) is FunctionType:
            runtime_dependencies[nested_name] = (
                _function_runtime_dependency_contract(
                    nested_name,
                    nested,
                    root,
                    ancestry=nested_ancestry,
                )
            )
        elif isinstance(nested, type):
            runtime_dependencies[nested_name] = (
                _class_runtime_dependency_contract(nested_name, nested, root)
            )
        elif nested.__class__.__module__ == "typing":
            runtime_dependencies[nested_name] = {
                "global_name": nested_name,
                "kind": "typing_symbol",
                "symbol": str(nested),
            }
        elif isinstance(nested, re.Pattern):
            runtime_dependencies[nested_name] = {
                "global_name": nested_name,
                "kind": "regex_pattern",
                "pattern": nested.pattern,
                "flags": int(nested.flags),
            }
        elif _immutable_global(nested):
            runtime_dependencies[nested_name] = {
                "global_name": nested_name,
                "kind": "immutable",
                "value": _immutable_runtime_value_contract(nested),
            }
        else:
            raise ValueError(
                f"执行适配器helper拒绝可变或不透明全局："
                f"{global_name}.{nested_name}"
            )
    return {
        "global_name": global_name,
        "kind": "function",
        "module": str(module.__name__),
        "name": name,
        "qualname": qualname,
        "code_marshaled_sha256": hashlib.sha256(
            marshal.dumps(dependency.__code__)
        ).hexdigest(),
        "defaults": _immutable_runtime_value_contract(
            tuple(dependency.__defaults__ or ())
        ),
        "kwdefaults": [
            {
                "name": str(key),
                "value": _immutable_runtime_value_contract(value),
            }
            for key, value in sorted((dependency.__kwdefaults__ or {}).items())
        ],
        "module_file": module_contract,
        "runtime_dependencies": [
            runtime_dependencies[key] for key in sorted(runtime_dependencies)
        ],
    }


def _class_runtime_dependency_contract(
    global_name: str, dependency: type, root: Path,
) -> dict[str, Any]:
    """Bind a referenced class identity plus all directly callable methods."""

    module = inspect.getmodule(dependency)
    if not isinstance(module, ModuleType):
        raise ValueError(f"执行适配器class依赖模块无效：{global_name}")
    methods: list[dict[str, str]] = []
    for name, raw in sorted(vars(dependency).items()):
        value = raw.__func__ if isinstance(raw, (staticmethod, classmethod)) else raw
        if isinstance(value, property):
            functions = [value.fget, value.fset, value.fdel]
        else:
            functions = [value]
        for function in functions:
            if type(function) is FunctionType:
                methods.append({
                    "name": str(name),
                    "qualname": str(function.__qualname__ or ""),
                    "code_marshaled_sha256": hashlib.sha256(
                        marshal.dumps(function.__code__)
                    ).hexdigest(),
                })
    return {
        "global_name": global_name,
        "kind": "class",
        "module": str(module.__name__),
        "name": str(dependency.__name__ or ""),
        "qualname": str(dependency.__qualname__ or ""),
        "methods": methods,
        "module_file": _module_dependency_contract(module, root),
    }


def _code_instructions(code: CodeType) -> Iterable[dis.Instruction]:
    """Yield bytecode from the function and every nested code object."""

    yield from dis.get_instructions(code)
    for constant in code.co_consts:
        if isinstance(constant, CodeType):
            yield from _code_instructions(constant)


def _callable_code_contract(
    value: Callable[..., Any] | None,
) -> dict[str, Any]:
    """Validate a named module-level pure function and bind all code inputs."""

    if value is None:
        return {"callable": None}
    if type(value) is not FunctionType:
        raise ValueError("执行适配器实现必须是模块级types.FunctionType纯函数")
    name = str(value.__name__ or "")
    qualname = str(value.__qualname__ or "")
    if (
        not name
        or name == "<lambda>"
        or qualname != name
        or "<locals>" in qualname
    ):
        raise ValueError("执行适配器实现必须是具名模块级函数，拒绝lambda或嵌套函数")
    if inspect.ismethod(value) or value.__closure__:
        raise ValueError("执行适配器实现不能是绑定方法或闭包")
    if value.__defaults__ or value.__kwdefaults__:
        raise ValueError("执行适配器实现不能使用默认参数")
    module = inspect.getmodule(value)
    if (
        not isinstance(module, ModuleType)
        or module.__dict__.get(name) is not value
        or str(value.__module__ or "") != str(module.__name__)
    ):
        raise ValueError("执行适配器函数必须由其声明模块以同名导出")
    root = _trusted_release_root()
    module_path = _trusted_regular_file(
        Path(str(module.__file__ or "")), root, label="执行适配器模块",
    )
    dependencies: dict[str, dict[str, Any]] = {}
    for instruction in _code_instructions(value.__code__):
        if instruction.opname in _FORBIDDEN_GLOBAL_OPS:
            raise ValueError("执行适配器纯函数包含全局写入或运行期导入")
        if instruction.opname not in {"LOAD_GLOBAL", "LOAD_NAME"}:
            continue
        global_name = str(instruction.argval or "")
        if global_name in _SAFE_BUILTIN_GLOBALS:
            continue
        if global_name in builtins.__dict__:
            raise ValueError(f"执行适配器LOAD_GLOBAL不在白名单：{global_name}")
        if global_name not in value.__globals__:
            raise ValueError(f"执行适配器全局依赖缺失：{global_name}")
        dependency = value.__globals__[global_name]
        if isinstance(dependency, ModuleType):
            raise ValueError(
                f"执行适配器拒绝可变模块全局：{global_name}"
            )
        elif type(dependency) is FunctionType:
            dependencies[global_name] = _function_runtime_dependency_contract(
                global_name, dependency, root,
            )
        elif isinstance(dependency, type):
            dependencies[global_name] = _class_runtime_dependency_contract(
                global_name, dependency, root,
            )
        elif _immutable_global(dependency):
            dependencies[global_name] = {
                "global_name": global_name,
                "kind": "immutable",
                "value": _immutable_runtime_value_contract(dependency),
            }
        else:
            raise ValueError(f"执行适配器拒绝可变或不透明全局：{global_name}")
    lock_path = _trusted_regular_file(
        root / _DEPENDENCY_LOCK_RELATIVE_PATH,
        root,
        label="生产依赖锁",
    )
    return {
        "module": str(module.__name__),
        "qualname": qualname,
        "module_relative_path": module_path.relative_to(root).as_posix(),
        "module_file_sha256": _file_sha256(module_path),
        "code_marshaled_sha256": hashlib.sha256(
            marshal.dumps(value.__code__)
        ).hexdigest(),
        "project_dependencies": [
            dependencies[key] for key in sorted(dependencies)
        ],
        "dependency_lock_relative_path": (
            _DEPENDENCY_LOCK_RELATIVE_PATH.as_posix()
        ),
        "dependency_lock_sha256": _file_sha256(lock_path),
        "release_tree_sha256": _release_tree_sha256(root),
        "python_abi": _python_abi_contract(),
    }


def _callable_code_fingerprint(value: Callable[..., Any] | None) -> str:
    return _digest(_callable_code_contract(value))


def compute_strategy_execution_adapter_artifact_sha256(
    *,
    adapter_key: str,
    adapter_version: str,
    evaluator_types: Iterable[str],
    candidate_builder: CandidateBuilder,
    validator: AdapterValidator | None = None,
) -> str:
    """Recompute the immutable artifact identity from deployed code."""

    return _digest({
        "schema": "probiga.strategy-adapter-code-artifact.v2",
        "adapter_key": str(adapter_key),
        "adapter_version": str(adapter_version),
        "evaluator_types": sorted(str(item) for item in evaluator_types),
        "candidate_builder_code_hash": _callable_code_fingerprint(
            candidate_builder
        ),
        "validator_code_hash": _callable_code_fingerprint(validator),
    })


def _candidate_input_contract(
    strategy: Mapping[str, Any], context: Mapping[str, Any],
) -> dict[str, Any]:
    config = strategy.get("evaluator_config")
    config = config if isinstance(config, Mapping) else {}
    binding = normalize_execution_binding(
        config.get("execution_adapter"),
        strategy_version=str(strategy.get("current_version") or ""),
    )
    runtime_context = _candidate_runtime_context(context)
    trade_date = str(runtime_context.get("trade_date") or "")[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", trade_date):
        raise ValueError("执行适配器候选批次缺少有效交易日")
    strategy_key = str(strategy.get("strategy_key") or "")
    version_hash = str(strategy.get("version_hash") or "")
    if not strategy_key or not _SHA256_PATTERN.fullmatch(version_hash):
        raise ValueError("动态候选缺少策略代码或不可变版本哈希")
    return {
        "schema": "probiga.strategy-candidate-input.v1",
        "trade_date": trade_date,
        "strategy_key": strategy_key,
        "strategy_version": str(strategy.get("current_version") or ""),
        "strategy_version_hash": version_hash,
        "execution_binding_hash": binding["execution_binding_hash"],
        "adapter_artifact_sha256": binding["artifact_sha256"],
        "cost_model_hash": binding["cost_model_hash"],
        "recommendation_rows_hash": _digest(
            runtime_context["recommendation_rows"]
        ),
        "market_hash": _digest(runtime_context["market"]),
        "configs_hash": _digest(runtime_context["configs"]),
        "metrics_hash": _digest(runtime_context["metrics"]),
    }


def _stable_candidate_runtime_value(value: Any) -> Any:
    """Project facts while removing per-attempt and cache metadata."""

    if isinstance(value, Mapping):
        return {
            str(key): _stable_candidate_runtime_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in _AUDIT_RECEIPT_FIELDS
        }
    if isinstance(value, tuple):
        return tuple(_stable_candidate_runtime_value(item) for item in value)
    if isinstance(value, list):
        return [_stable_candidate_runtime_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return tuple(sorted(
            (_stable_candidate_runtime_value(item) for item in value),
            key=lambda item: json.dumps(
                item, ensure_ascii=False, sort_keys=True, default=str,
            ),
        ))
    return copy.deepcopy(value)


def _candidate_runtime_context(context: Mapping[str, Any]) -> dict[str, Any]:
    """Return the exact stable context exposed to adapter code and hashing."""

    if not isinstance(context, Mapping):
        raise ValueError("执行适配器运行输入必须是对象")
    unknown = sorted(
        str(key) for key in context if str(key) not in _CANDIDATE_CONTEXT_FIELDS
    )
    if unknown:
        raise ValueError("执行适配器运行输入包含未声明字段：" + "、".join(unknown))
    recommendation_rows = context.get("recommendation_rows") or ()
    if not isinstance(recommendation_rows, (list, tuple)) or not all(
        isinstance(item, Mapping) for item in recommendation_rows
    ):
        raise ValueError("执行适配器推荐明细必须是对象列表")
    for field in ("market", "configs", "metrics"):
        if not isinstance(context.get(field) or {}, Mapping):
            raise ValueError(f"执行适配器{field}输入必须是对象")
    return {
        "trade_date": str(context.get("trade_date") or "")[:10],
        "recommendation_rows": tuple(
            _stable_candidate_runtime_value(item) for item in recommendation_rows
        ),
        "market": _stable_candidate_runtime_value(context.get("market") or {}),
        "configs": _stable_candidate_runtime_value(context.get("configs") or {}),
        "metrics": _stable_candidate_runtime_value(context.get("metrics") or {}),
    }


def _reject_candidate_output_audit_fields(value: Any, *, path: str) -> None:
    """Reject attempt/cache metadata anywhere in stable candidate output."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key)
            if name in _AUDIT_RECEIPT_FIELDS:
                raise ValueError(
                    f"执行适配器候选稳定输出包含审计字段：{path}.{name}"
                )
            _reject_candidate_output_audit_fields(
                item, path=f"{path}.{name}",
            )
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_candidate_output_audit_fields(
                item, path=f"{path}[{index}]",
            )


@dataclass(frozen=True)
class CandidateBatch:
    """An adapter-authored batch whose hashes are independently recomputed."""

    strategy_key: str
    strategy_version: str
    strategy_version_hash: str
    execution_binding_hash: str
    adapter_artifact_sha256: str
    cost_model_hash: str
    trade_date: str
    input_hash: str
    output_hash: str
    stable_result_hash: str
    run_uid: str
    completed_at: str
    candidates: tuple[Mapping[str, Any], ...]


def create_candidate_batch(
    strategy: Mapping[str, Any],
    context: Mapping[str, Any],
    candidates: Iterable[Mapping[str, Any]],
    *,
    run_uid: str | None = None,
    completed_at: str | None = None,
) -> CandidateBatch:
    """Create the strict batch envelope used by code-owned adapters."""

    input_contract = _candidate_input_contract(strategy, context)
    effective_completed_at = str(
        completed_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )
    rows_list: list[dict[str, Any]] = []
    for raw in candidates:
        if not isinstance(raw, Mapping):
            raise ValueError("执行适配器候选必须是对象")
        row = dict(raw)
        _reject_candidate_output_audit_fields(row, path="candidate")
        row.setdefault("trade_date", input_contract["trade_date"])
        row.setdefault("data_date", input_contract["trade_date"])
        rows_list.append(row)
    rows = tuple(rows_list)
    output_payload = {
        "schema": "probiga.strategy-candidate-output.v1",
        "trade_date": input_contract["trade_date"],
        "strategy_key": input_contract["strategy_key"],
        "strategy_version": input_contract["strategy_version"],
        "execution_binding_hash": input_contract["execution_binding_hash"],
        "candidates": rows,
    }
    input_hash = _digest(input_contract)
    output_hash = _digest(output_payload)
    stable_result_hash = _digest({
        "schema": "probiga.strategy-candidate-stable-result.v1",
        "strategy_key": input_contract["strategy_key"],
        "strategy_version": input_contract["strategy_version"],
        "execution_binding_hash": input_contract["execution_binding_hash"],
        "trade_date": input_contract["trade_date"],
        "input_hash": input_hash,
        "output_hash": output_hash,
        "candidate_count": len(rows),
    })
    return CandidateBatch(
        strategy_key=input_contract["strategy_key"],
        strategy_version=input_contract["strategy_version"],
        strategy_version_hash=input_contract["strategy_version_hash"],
        execution_binding_hash=input_contract["execution_binding_hash"],
        adapter_artifact_sha256=input_contract["adapter_artifact_sha256"],
        cost_model_hash=input_contract["cost_model_hash"],
        trade_date=input_contract["trade_date"],
        input_hash=input_hash,
        output_hash=output_hash,
        stable_result_hash=stable_result_hash,
        run_uid=str(run_uid or uuid.uuid4().hex),
        completed_at=effective_completed_at,
        candidates=rows,
    )


@dataclass(frozen=True)
class StrategyExecutionAdapter:
    """A deployed, code-owned adapter implementation."""

    adapter_key: str
    adapter_version: str
    artifact_sha256: str
    evaluator_types: frozenset[str]
    candidate_builder: CandidateBuilder
    validator: AdapterValidator | None = None

    def __post_init__(self) -> None:
        if not _ADAPTER_KEY_PATTERN.fullmatch(self.adapter_key):
            raise ValueError("执行适配器代码无效")
        if not _ADAPTER_VERSION_PATTERN.fullmatch(self.adapter_version):
            raise ValueError("执行适配器版本无效")
        if not _SHA256_PATTERN.fullmatch(self.artifact_sha256):
            raise ValueError("执行适配器制品SHA必须是64位小写sha256")
        if not self.evaluator_types or not all(
            isinstance(item, str) and item for item in self.evaluator_types
        ):
            raise ValueError("执行适配器必须声明可识别的评估器类型")
        if not callable(self.candidate_builder):
            raise ValueError("执行适配器必须提供候选生成器")
        if self.validator is not None and not callable(self.validator):
            raise ValueError("执行适配器校验器无效")
        computed_artifact = (
            compute_strategy_execution_adapter_artifact_sha256(
                adapter_key=self.adapter_key,
                adapter_version=self.adapter_version,
                evaluator_types=self.evaluator_types,
                candidate_builder=self.candidate_builder,
                validator=self.validator,
            )
        )
        if self.artifact_sha256 != computed_artifact:
            raise ValueError("执行适配器制品SHA不是部署代码的可复算指纹")


_REGISTRY_LOCK = threading.RLock()
_REGISTRY: dict[tuple[str, str], StrategyExecutionAdapter] = {}
_REGISTRY_SEALED = False
_REGISTRY_SEAL_HASH = ""

# Code-owned startup manifest. New dynamic strategies are added by deploying a
# new immutable adapter here; database registration alone never grants code
# execution or funding eligibility. The tuple has no fixed cardinality.
_TRUSTED_STARTUP_ADAPTERS: tuple[StrategyExecutionAdapter, ...] = ()


def seal_strategy_execution_adapter_registry() -> str:
    """Permanently freeze this process registry before runtime execution."""

    global _REGISTRY_SEALED, _REGISTRY_SEAL_HASH
    with _REGISTRY_LOCK:
        payload = {
            "schema": "probiga.strategy-adapter-registry-seal.v1",
            "adapters": [
                {
                    "adapter_key": key[0],
                    "adapter_version": key[1],
                    "artifact_sha256": adapter.artifact_sha256,
                    "evaluator_types": sorted(adapter.evaluator_types),
                }
                for key, adapter in sorted(_REGISTRY.items())
            ],
        }
        seal_hash = _digest(payload)
        if _deployment_mode() == "production":
            expected = str(
                os.environ.get(
                    "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256"
                ) or ""
            ).strip().lower()
            if (
                not _SHA256_PATTERN.fullmatch(expected)
                or expected != seal_hash
            ):
                raise RuntimeError("生产执行适配器注册表封印未通过发布清单校验")
        if _REGISTRY_SEALED and seal_hash != _REGISTRY_SEAL_HASH:
            raise RuntimeError("执行适配器注册表封印后发生变化")
        _REGISTRY_SEALED = True
        _REGISTRY_SEAL_HASH = seal_hash
        return seal_hash


def bootstrap_strategy_execution_adapter_registry() -> dict[str, Any]:
    """Register the code manifest and explicitly seal every production process."""

    declared = {
        (adapter.adapter_key, adapter.adapter_version): adapter
        for adapter in _TRUSTED_STARTUP_ADAPTERS
    }
    if len(declared) != len(_TRUSTED_STARTUP_ADAPTERS):
        raise RuntimeError("可信执行适配器启动清单包含重复身份")
    with _REGISTRY_LOCK:
        undeclared = sorted(set(_REGISTRY) - set(declared))
    if _deployment_mode() == "production" and undeclared:
        raise RuntimeError("执行适配器注册表包含启动清单之外的实现")
    for adapter in _TRUSTED_STARTUP_ADAPTERS:
        register_strategy_execution_adapter(adapter)
    if _deployment_mode() == "production":
        seal_strategy_execution_adapter_registry()
    return strategy_execution_adapter_capabilities()


def _seal_production_registry() -> None:
    """Production must be sealed explicitly by trusted startup code."""

    if _deployment_mode() == "production" and not _REGISTRY_SEALED:
        raise RuntimeError("生产执行适配器注册表尚未显式封印")


def register_strategy_execution_adapter(
    adapter: StrategyExecutionAdapter,
) -> None:
    """Register one immutable deployed adapter; conflicting replacement fails."""

    if not isinstance(adapter, StrategyExecutionAdapter):
        raise TypeError("adapter must be StrategyExecutionAdapter")
    identity = (adapter.adapter_key, adapter.adapter_version)
    with _REGISTRY_LOCK:
        if _REGISTRY_SEALED:
            raise RuntimeError("执行适配器注册表已封印，禁止运行期新增或替换")
        existing = _REGISTRY.get(identity)
        if existing is not None and existing != adapter:
            raise ValueError("同一执行适配器版本不可被不同实现覆盖")
        _REGISTRY[identity] = adapter


def unregister_strategy_execution_adapter(
    adapter_key: str, adapter_version: str, *, explicit_test_mode: bool = False,
) -> None:
    """Remove a test adapter only through an explicit non-production path."""

    if (
        explicit_test_mode is not True
        or os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower()
        == "production"
    ):
        raise RuntimeError("注销执行适配器仅允许显式测试模式")
    with _REGISTRY_LOCK:
        if _REGISTRY_SEALED:
            raise RuntimeError("执行适配器注册表已封印，禁止注销")
        _REGISTRY.pop((str(adapter_key), str(adapter_version)), None)


def deployed_strategy_execution_adapters(
) -> Mapping[tuple[str, str], StrategyExecutionAdapter]:
    with _REGISTRY_LOCK:
        return MappingProxyType(dict(_REGISTRY))


def strategy_execution_adapter_capabilities() -> dict[str, Any]:
    """Expose code-owned adapter/evaluator capabilities for registration UIs."""

    with _REGISTRY_LOCK:
        adapters = [
            {
                "adapter_key": adapter.adapter_key,
                "adapter_version": adapter.adapter_version,
                "artifact_sha256": adapter.artifact_sha256,
                "evaluator_types": sorted(adapter.evaluator_types),
                "status_label": (
                    "已部署并显式封印"
                    if _REGISTRY_SEALED
                    else "已部署，等待显式封印"
                ),
            }
            for _identity, adapter in sorted(_REGISTRY.items())
        ]
        return {
            "schema": "probiga.strategy-adapter-capabilities.v1",
            "deployment_mode": _deployment_mode() or "development",
            "registry_sealed": _REGISTRY_SEALED,
            "registry_seal_hash": _REGISTRY_SEAL_HASH,
            "production_execution_ready": bool(
                _deployment_mode() != "production" or _REGISTRY_SEALED
            ),
            "evaluator_types": sorted({
                evaluator
                for adapter in _REGISTRY.values()
                for evaluator in adapter.evaluator_types
            }),
            "adapters": adapters,
            "funding_pipeline_ready": False,
        }


def strategy_execution_adapter_status(
    strategy: Mapping[str, Any],
) -> dict[str, Any]:
    """Return fail-closed execution capability for one registry version."""

    source_kind = str(strategy.get("source_kind") or "")
    evaluator_type = str(strategy.get("evaluator_type") or "")
    strategy_key = str(strategy.get("strategy_key") or "")
    strategy_version = str(strategy.get("current_version") or "")
    integrity_valid = strategy.get("version_integrity_valid") is True
    enabled = strategy.get("enabled") is True
    lifecycle = str(strategy.get("current_status") or "")

    builtin = {
        "immutable_manifest": ("manifest_score_adapter", "manifest-pipeline-v1"),
        "immutable_v3_sleeve": ("v3_primary_sleeve_adapter", "v3-sleeve-pipeline-v1"),
    }.get(source_kind)
    if builtin is not None:
        executable = (
            integrity_valid
            and enabled
            and lifecycle not in {"RETIRED", "SUSPENDED"}
            and evaluator_type == builtin[0]
        )
        reason = (
            "内置执行适配器已部署并绑定不可变策略版本"
            if executable else "执行适配器无效：内置版本类型或内容哈希不匹配"
        )
        return {
            "executable": executable,
            "status": "READY" if executable else "INVALID",
            "status_label": "执行适配器已就绪" if executable else "执行适配器无效",
            "reason": reason,
            "adapter_key": builtin[0],
            "adapter_version": builtin[1],
            "artifact_sha256": str(strategy.get("version_hash") or ""),
            "cost_model_hash": str(strategy.get("version_hash") or ""),
            "execution_binding_hash": str(strategy.get("version_hash") or ""),
            "strategy_version": strategy_version,
            "candidate_builder_deployed": source_kind == "immutable_manifest",
        }

    config = strategy.get("evaluator_config")
    config = config if isinstance(config, dict) else {}
    try:
        binding = normalize_execution_binding(
            config.get("execution_adapter"), strategy_version=strategy_version,
        )
    except ValueError as exc:
        return {
            "executable": False,
            "status": "UNDEPLOYED_OR_INVALID",
            "status_label": "执行适配器未部署/无效",
            "reason": "执行适配器未部署/无效：" + str(exc),
            "adapter_key": "",
            "adapter_version": "",
            "artifact_sha256": "",
            "cost_model_hash": "",
            "execution_binding_hash": "",
            "strategy_version": strategy_version,
            "candidate_builder_deployed": False,
        }
    with _REGISTRY_LOCK:
        adapter = _REGISTRY.get(
            (binding["adapter_key"], binding["adapter_version"])
        )
    reasons: list[str] = []
    if _deployment_mode() == "production" and not _REGISTRY_SEALED:
        reasons.append("生产执行适配器注册表尚未由可信启动流程显式封印")
    if not enabled:
        reasons.append("策略已禁用，禁止执行")
    if lifecycle == "RETIRED":
        reasons.append("策略已淘汰，禁止执行")
    elif lifecycle == "SUSPENDED":
        reasons.append("策略已暂停，仅允许独立诊断，不进入候选或资金池")
    if not integrity_valid:
        reasons.append("不可变策略版本内容哈希校验失败")
    if adapter is None:
        reasons.append("对应执行适配器未部署")
    else:
        try:
            recomputed_artifact = (
                compute_strategy_execution_adapter_artifact_sha256(
                    adapter_key=adapter.adapter_key,
                    adapter_version=adapter.adapter_version,
                    evaluator_types=adapter.evaluator_types,
                    candidate_builder=adapter.candidate_builder,
                    validator=adapter.validator,
                )
            )
        except ValueError as exc:
            recomputed_artifact = ""
            reasons.append(str(exc))
        if recomputed_artifact != adapter.artifact_sha256:
            reasons.append("已部署适配器代码指纹在注册后发生变化")
        if adapter.artifact_sha256 != binding["artifact_sha256"]:
            reasons.append("已部署适配器制品SHA与策略版本绑定不一致")
        if evaluator_type not in adapter.evaluator_types:
            reasons.append("执行适配器无法识别该评估器类型")
        # Never invoke adapter-owned code after a hard trust/lifecycle failure.
        # The validator receives a detached copy and must remain pure, so it
        # cannot rewrite source_kind/lifecycle fields used by later funding gates.
        if adapter.validator is not None and not reasons:
            validator_input = copy.deepcopy(dict(strategy))
            validator_input_hash = _digest(validator_input)
            try:
                valid, validator_reason = adapter.validator(validator_input)
            except Exception as exc:  # pragma: no cover - defensive isolation
                valid, validator_reason = False, f"适配器校验异常：{type(exc).__name__}"
            if _digest(validator_input) != validator_input_hash:
                valid = False
                validator_reason = "执行适配器校验器修改了只读策略输入"
            if valid is not True:
                reasons.append(str(validator_reason or "执行适配器校验未通过"))
    executable = not reasons
    return {
        "executable": executable,
        "status": (
            "RESEARCH_READY" if executable else "UNDEPLOYED_OR_INVALID"
        ),
        "status_label": (
            "影子候选执行就绪（未接通资金证据）"
            if executable else "执行适配器未部署/无效"
        ),
        "reason": (
            "执行适配器代码指纹、策略版本和成本模型绑定通过；"
            "仅允许影子候选，完整资金证据链尚未部署"
            if executable else "执行适配器未部署/无效：" + "；".join(reasons)
        ),
        **{
            key: binding[key]
            for key in (
                "adapter_key", "adapter_version", "artifact_sha256",
                "cost_model_hash", "execution_binding_hash",
                "strategy_version",
            )
        },
        "cost_model": binding["cost_model"],
        "candidate_builder_deployed": bool(adapter),
        # Dynamic candidates can run in research/shadow mode.  They do not
        # become capital-eligible until an exact-bound intent/order/fill
        # forward ledger is deployed and independently verified.
        "funding_pipeline_ready": False,
        "funding_status": "SHADOW_LEDGER_NOT_DEPLOYED",
        "registry_sealed": _REGISTRY_SEALED,
        "registry_seal_hash": _REGISTRY_SEAL_HASH,
        "production_seal_required": _deployment_mode() == "production",
    }


def execute_dynamic_adapter_candidate_batch(
    strategy: Mapping[str, Any], context: Mapping[str, Any],
) -> dict[str, Any]:
    """Run an adapter and independently verify its batch and run receipt."""

    _seal_production_registry()
    if strategy.get("enabled") is not True:
        raise ValueError("策略已禁用，禁止执行动态适配器")
    if str(strategy.get("current_status") or "") == "RETIRED":
        raise ValueError("策略已淘汰，禁止执行动态适配器")
    status = strategy_execution_adapter_status(strategy)
    if status["executable"] is not True:
        raise ValueError(str(status.get("reason") or "执行适配器不可执行"))
    with _REGISTRY_LOCK:
        adapter = _REGISTRY.get(
            (status["adapter_key"], status["adapter_version"])
        )
    if adapter is None:
        raise ValueError("对应执行适配器未部署")
    strategy_input = copy.deepcopy(dict(strategy))
    context_input = _candidate_runtime_context(context)
    strategy_input_hash = _digest(strategy_input)
    context_input_hash = _digest(context_input)
    batch = adapter.candidate_builder(strategy_input, context_input)
    if (
        _digest(strategy_input) != strategy_input_hash
        or _digest(context_input) != context_input_hash
    ):
        raise ValueError("执行适配器纯函数修改了只读策略或运行输入")
    if not isinstance(batch, CandidateBatch):
        raise ValueError("执行适配器必须返回CandidateBatch，None或裸列表均无效")
    input_contract = _candidate_input_contract(strategy, context)
    expected_identity = {
        "strategy_key": input_contract["strategy_key"],
        "strategy_version": input_contract["strategy_version"],
        "strategy_version_hash": input_contract["strategy_version_hash"],
        "execution_binding_hash": status["execution_binding_hash"],
        "adapter_artifact_sha256": status["artifact_sha256"],
        "cost_model_hash": status["cost_model_hash"],
        "trade_date": input_contract["trade_date"],
    }
    for key, expected in expected_identity.items():
        if str(getattr(batch, key, "")) != str(expected):
            raise ValueError(f"CandidateBatch身份字段{key}与运行策略不一致")
    expected_input_hash = _digest(input_contract)
    if batch.input_hash != expected_input_hash:
        raise ValueError("CandidateBatch输入哈希与本次运行输入不一致")
    if not re.fullmatch(r"[0-9a-f]{32}", batch.run_uid):
        raise ValueError("CandidateBatch运行编号无效")
    try:
        completed_at = datetime.fromisoformat(
            batch.completed_at.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("CandidateBatch完成时间无效") from exc
    if completed_at.tzinfo is None:
        raise ValueError("CandidateBatch完成时间必须带时区")
    completed_utc = completed_at.astimezone(timezone.utc)
    if completed_utc > datetime.now(timezone.utc) + timedelta(minutes=5):
        raise ValueError("CandidateBatch完成时间不能晚于当前可信时钟")
    if completed_at.date() < date.fromisoformat(input_contract["trade_date"]):
        raise ValueError("CandidateBatch完成时间早于候选交易日")
    raw_rows = batch.candidates
    output_payload = {
        "schema": "probiga.strategy-candidate-output.v1",
        "trade_date": input_contract["trade_date"],
        "strategy_key": input_contract["strategy_key"],
        "strategy_version": input_contract["strategy_version"],
        "execution_binding_hash": status["execution_binding_hash"],
        "candidates": raw_rows,
    }
    expected_output_hash = _digest(output_payload)
    if batch.output_hash != expected_output_hash:
        raise ValueError("CandidateBatch输出哈希与实际候选不一致")
    expected_stable_result_hash = _digest({
        "schema": "probiga.strategy-candidate-stable-result.v1",
        "strategy_key": input_contract["strategy_key"],
        "strategy_version": input_contract["strategy_version"],
        "execution_binding_hash": status["execution_binding_hash"],
        "trade_date": input_contract["trade_date"],
        "input_hash": expected_input_hash,
        "output_hash": expected_output_hash,
        "candidate_count": len(raw_rows),
    })
    if batch.stable_result_hash != expected_stable_result_hash:
        raise ValueError("CandidateBatch稳定结果哈希无效")
    result: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            raise ValueError("执行适配器候选必须是对象")
        row = dict(raw)
        _reject_candidate_output_audit_fields(row, path="candidate")
        code = str(row.get("stock_code") or "").strip().zfill(6)
        if not re.fullmatch(r"[0-9]{6}", code):
            raise ValueError("执行适配器候选证券代码无效")
        if code in seen_codes:
            raise ValueError("CandidateBatch包含重复证券代码")
        seen_codes.add(code)
        if (
            str(row.get("trade_date") or "") != input_contract["trade_date"]
            or str(row.get("data_date") or "") != input_contract["trade_date"]
        ):
            raise ValueError("执行适配器候选逐行交易日或数据日越界")
        declared_version = str(row.get("strategy_version") or "")
        if declared_version != str(strategy.get("current_version") or ""):
            raise ValueError("执行适配器候选混入其他策略版本")
        row_identity = {
            "strategy_key": str(row.get("strategy_key") or ""),
            "strategy_version_hash": str(
                row.get("strategy_version_hash") or ""
            ),
            "execution_binding_hash": str(
                row.get("execution_binding_hash") or ""
            ),
            "adapter_artifact_sha256": str(
                row.get("adapter_artifact_sha256") or ""
            ),
            "cost_model_hash": str(row.get("cost_model_hash") or ""),
        }
        for key, expected in expected_identity.items():
            if key in {"strategy_version", "trade_date"}:
                continue
            if row_identity.get(key) != str(expected):
                raise ValueError(f"执行适配器候选身份字段{key}缺失或不一致")
        result.append({
            **row,
            "stock_code": code,
            "strategy_key": str(strategy.get("strategy_key") or ""),
            "strategy_name": str(strategy.get("strategy_name") or ""),
            "strategy_version": declared_version,
            "adapter_key": status["adapter_key"],
            "adapter_version": status["adapter_version"],
            "adapter_artifact_sha256": status["artifact_sha256"],
            "cost_model_hash": status["cost_model_hash"],
            "execution_binding_hash": status["execution_binding_hash"],
            "adapter_mode": "dynamic_execution_adapter",
            "model_version": declared_version,
            "candidate_completed_at": batch.completed_at,
        })
    receipt_payload = {
        "schema": "probiga.strategy-candidate-run-receipt.v2",
        **expected_identity,
        "adapter_key": status["adapter_key"],
        "adapter_version": status["adapter_version"],
        "run_uid": batch.run_uid,
        "completed_at": batch.completed_at,
        "status": "COMPLETED",
        "input_hash": expected_input_hash,
        "output_hash": expected_output_hash,
        "stable_result_hash": expected_stable_result_hash,
        "candidate_count": len(result),
        "candidate_identity": sorted(seen_codes),
    }
    receipt = {
        **receipt_payload,
        "receipt_hash": _digest(receipt_payload),
    }
    for row in result:
        row["candidate_run_uid"] = batch.run_uid
        row["candidate_receipt_hash"] = receipt["receipt_hash"]
        row["candidate_input_hash"] = expected_input_hash
        row["candidate_output_hash"] = expected_output_hash
        row["candidate_stable_result_hash"] = expected_stable_result_hash
    return {"signals": result, "receipt": receipt}


def build_dynamic_adapter_signals(
    strategy: Mapping[str, Any], context: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Compatibility view returning signals from a verified CandidateBatch."""

    return execute_dynamic_adapter_candidate_batch(strategy, context)["signals"]


_RUN_RECEIPT_KEYS = frozenset({
    "schema", "strategy_key", "strategy_version", "strategy_version_hash",
    "execution_binding_hash", "adapter_artifact_sha256", "cost_model_hash",
    "trade_date", "adapter_key", "adapter_version", "run_uid",
    "completed_at", "status", "input_hash", "output_hash",
    "stable_result_hash", "candidate_count", "candidate_identity",
    "receipt_hash",
})


def validate_strategy_adapter_run_receipt(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the exact append-only dynamic adapter run-receipt envelope."""

    if not isinstance(value, Mapping):
        raise ValueError("动态适配器运行回执必须是对象")
    receipt = dict(value)
    if set(receipt) != _RUN_RECEIPT_KEYS:
        raise ValueError("动态适配器运行回执字段集合不精确")
    receipt_hash = str(receipt.pop("receipt_hash") or "")
    hashes = (
        "strategy_version_hash", "execution_binding_hash",
        "adapter_artifact_sha256", "cost_model_hash", "input_hash",
        "output_hash", "stable_result_hash",
    )
    candidate_identity = receipt.get("candidate_identity")
    if (
        receipt.get("schema") != "probiga.strategy-candidate-run-receipt.v2"
        or receipt.get("status") != "COMPLETED"
        or not re.fullmatch(r"[0-9a-f]{32}", str(receipt.get("run_uid") or ""))
        or not str(receipt.get("strategy_key") or "")
        or len(str(receipt.get("strategy_key") or "")) > 80
        or not _ADAPTER_VERSION_PATTERN.fullmatch(
            str(receipt.get("strategy_version") or "")
        )
        or not _ADAPTER_KEY_PATTERN.fullmatch(
            str(receipt.get("adapter_key") or "")
        )
        or not _ADAPTER_VERSION_PATTERN.fullmatch(
            str(receipt.get("adapter_version") or "")
        )
        or not all(_SHA256_PATTERN.fullmatch(str(receipt.get(key) or "")) for key in hashes)
        or not _SHA256_PATTERN.fullmatch(receipt_hash)
        or _digest(receipt) != receipt_hash
        or type(receipt.get("candidate_count")) is not int
        or int(receipt.get("candidate_count") or 0) < 0
        or not isinstance(candidate_identity, list)
        or len(candidate_identity or [])
        != int(receipt.get("candidate_count") or 0)
        or list(candidate_identity or [])
        != sorted(set(str(item) for item in candidate_identity or []))
        or not all(
            isinstance(item, str) and re.fullmatch(r"[0-9]{6}", item)
            for item in candidate_identity or []
        )
    ):
        raise ValueError("动态适配器运行回执内容或哈希无效")
    try:
        completed = datetime.fromisoformat(
            str(receipt.get("completed_at") or "").replace("Z", "+00:00")
        )
        raw_trade_date = str(receipt.get("trade_date") or "")
        trade_day = date.fromisoformat(raw_trade_date)
    except ValueError as exc:
        raise ValueError("动态适配器运行回执时间无效") from exc
    if (
        trade_day.isoformat() != raw_trade_date
        or completed.tzinfo is None
        or completed.date() < trade_day
        or completed.astimezone(timezone.utc)
        > datetime.now(timezone.utc) + timedelta(minutes=5)
    ):
        raise ValueError("动态适配器运行回执时间越界")
    return {**receipt, "receipt_hash": receipt_hash}


def persist_strategy_adapter_run_receipt(
    connection: Any, receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Append one verified receipt, including a zero-candidate completed run."""

    normalized = validate_strategy_adapter_run_receipt(receipt)
    completed_at_db = datetime.fromisoformat(
        str(normalized["completed_at"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc).replace(tzinfo=None)
    result = connection.execute(text("""
        INSERT INTO st_strategy_adapter_run_receipt
        (run_uid, strategy_key, strategy_version, strategy_version_hash,
         execution_binding_hash, adapter_artifact_sha256, cost_model_hash,
         adapter_key, adapter_version, trade_date, completed_at, status,
         input_hash, output_hash, stable_result_hash, candidate_count,
         candidate_identity_json, receipt_json, receipt_hash)
        VALUES
        (:run_uid, :strategy_key, :strategy_version, :strategy_version_hash,
         :execution_binding_hash, :adapter_artifact_sha256, :cost_model_hash,
         :adapter_key, :adapter_version, :trade_date, :completed_at, 'COMPLETED',
         :input_hash, :output_hash, :stable_result_hash, :candidate_count,
         :candidate_identity_json, :receipt_json, :receipt_hash)
    """), {
        **normalized,
        "completed_at": completed_at_db,
        "candidate_identity_json": json.dumps(
            normalized["candidate_identity"], ensure_ascii=False,
            separators=(",", ":"),
        ),
        "receipt_json": json.dumps(
            normalized, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        ),
    })
    if getattr(result, "rowcount", 1) != 1:
        raise RuntimeError("动态适配器运行回执未能持久化")
    return normalized


def verify_persisted_strategy_adapter_run_receipt(
    receipt: Mapping[str, Any], persisted_row: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare an audit envelope with one independently loaded DB fact."""

    normalized = validate_strategy_adapter_run_receipt(receipt)
    if not isinstance(persisted_row, Mapping):
        raise ValueError("找不到持久化动态适配器运行回执")
    stored = dict(persisted_row)
    stored_receipt = stored.get("receipt_json")
    if isinstance(stored_receipt, str):
        try:
            stored_receipt = json.loads(stored_receipt)
        except json.JSONDecodeError as exc:
            raise ValueError("持久化动态适配器运行回执JSON无效") from exc
    verified = validate_strategy_adapter_run_receipt(stored_receipt)
    for field in (
        "run_uid", "strategy_key", "strategy_version",
        "strategy_version_hash", "execution_binding_hash",
        "adapter_artifact_sha256", "cost_model_hash", "adapter_key",
        "adapter_version", "trade_date", "status", "input_hash",
        "output_hash", "stable_result_hash", "candidate_count",
        "receipt_hash",
    ):
        if str(stored.get(field) or "") != str(verified.get(field) or ""):
            raise ValueError(f"持久化动态适配器运行回执列{field}漂移")
        if str(normalized.get(field) or "") != str(verified.get(field) or ""):
            raise ValueError(f"动态适配器运行回执字段{field}不匹配")
    stored_identity = stored.get("candidate_identity_json")
    if isinstance(stored_identity, str):
        try:
            stored_identity = json.loads(stored_identity)
        except json.JSONDecodeError as exc:
            raise ValueError("持久化动态适配器候选身份JSON无效") from exc
    if stored_identity != verified["candidate_identity"]:
        raise ValueError("持久化动态适配器候选身份漂移")
    stored_completed = stored.get("completed_at")
    if isinstance(stored_completed, str):
        try:
            stored_completed = datetime.fromisoformat(
                stored_completed.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError("持久化动态适配器完成时间无效") from exc
    if not isinstance(stored_completed, datetime):
        raise ValueError("持久化动态适配器完成时间无效")
    if stored_completed.tzinfo is not None:
        stored_completed = stored_completed.astimezone(timezone.utc).replace(
            tzinfo=None
        )
    expected_completed = datetime.fromisoformat(
        str(verified["completed_at"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc).replace(tzinfo=None)
    if stored_completed != expected_completed:
        raise ValueError("持久化动态适配器完成时间漂移")
    return verified


def verify_dynamic_shadow_ledger_chain(
    strategy: Mapping[str, Any],
    *,
    candidate_receipt: Mapping[str, Any],
    intent: Mapping[str, Any],
    order: Mapping[str, Any],
    fill: Mapping[str, Any],
    forward_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify a candidate→paper fill→forward-evidence chain exactly.

    This contract is deliberately independent from capital eligibility.  A
    persistent producer/consumer must be deployed before governance may set
    ``funding_pipeline_ready``; until then dynamic strategies remain SHADOW.
    """

    # A caller-provided hash chain is not an authoritative ledger.  Only the
    # candidate run receipt now has an append-only producer/table.  Until the
    # intent/order/fill/evidence producers and their DB foreign-key chain are
    # deployed, this API must reject even internally self-consistent objects.
    validate_strategy_adapter_run_receipt(candidate_receipt)
    raise RuntimeError(
        "动态影子资金链尚未部署持久化intent/order/fill/evidence事实；"
        "调用方自造哈希链不能取得资金资格"
    )


__all__ = [
    "CandidateBatch",
    "StrategyExecutionAdapter",
    "build_dynamic_adapter_signals",
    "compute_strategy_execution_adapter_artifact_sha256",
    "create_candidate_batch",
    "deployed_strategy_execution_adapters",
    "execute_dynamic_adapter_candidate_batch",
    "normalize_cost_model",
    "normalize_execution_binding",
    "register_strategy_execution_adapter",
    "bootstrap_strategy_execution_adapter_registry",
    "persist_strategy_adapter_run_receipt",
    "seal_strategy_execution_adapter_registry",
    "strategy_execution_adapter_capabilities",
    "strategy_execution_adapter_status",
    "unregister_strategy_execution_adapter",
    "verify_dynamic_shadow_ledger_chain",
    "validate_strategy_adapter_run_receipt",
    "verify_persisted_strategy_adapter_run_receipt",
]

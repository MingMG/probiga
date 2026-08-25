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
import logging
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
from decimal import Decimal
from types import (
    CodeType,
    FunctionType,
    MappingProxyType,
    MethodType,
    ModuleType,
)
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
_FORBIDDEN_SIDE_EFFECT_OPS = frozenset({
    "STORE_ATTR", "DELETE_ATTR",
})
_REFLECTION_ATTRIBUTE_OPS = frozenset({
    "LOAD_ATTR", "LOAD_METHOD", "LOAD_SUPER_ATTR", "STORE_ATTR",
    "DELETE_ATTR",
})
_FORBIDDEN_REFLECTION_ATTRIBUTES = frozenset({
    # Frame/traceback/generator attributes are not consistently dunder-named,
    # but all of them can recover a globals/builtins dictionary.
    "ag_frame", "cr_frame", "f_back", "f_builtins", "f_code",
    "f_globals", "f_locals", "func_dict", "func_globals", "gi_frame",
    "im_func", "im_self", "mro", "tb_frame",
})
_TRUSTED_FRAMEWORK_PRIMITIVES = frozenset({"create_candidate_batch"})
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
_LOGGER = logging.getLogger(__name__)


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
    # Exact types matter here.  A subclass instance can override attribute or
    # iteration behavior and turn an apparently immutable default/global into
    # an object-capability escape.
    if value is None or type(value) in {str, bytes, int, float, bool}:
        return True
    if type(value) in {tuple, frozenset}:
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
    if type(value) is bool:
        return {"type": "bool", "value": value}
    if type(value) in {str, bytes, int, float}:
        encoded = value.hex() if type(value) is bytes else value
        return {"type": type(value).__name__, "value": encoded}
    if type(value) is tuple:
        return {
            "type": "tuple",
            "items": [_immutable_runtime_value_contract(item) for item in value],
        }
    if type(value) is frozenset:
        items = [_immutable_runtime_value_contract(item) for item in value]
        items.sort(key=_digest)
        return {"type": "frozenset", "items": items}
    raise ValueError("执行适配器函数依赖包含可变默认值或闭包状态")


def _reflection_attribute_forbidden(name: str) -> bool:
    normalized = str(name or "")
    return (
        normalized in _FORBIDDEN_REFLECTION_ATTRIBUTES
        or (normalized.startswith("__") and normalized.endswith("__"))
    )


def _validate_no_reflection_escape(
    instruction: dis.Instruction,
    *,
    label: str,
) -> None:
    """Reject bytecode that can recover globals, builtins, frames or classes."""

    if instruction.opname not in _REFLECTION_ATTRIBUTE_OPS:
        return
    attribute = str(instruction.argval or "")
    if _reflection_attribute_forbidden(attribute):
        raise ValueError(
            f"{label}包含禁止的反射属性访问：{attribute}"
        )


def _function_runtime_dependency_contract(
    global_name: str,
    dependency: FunctionType,
    root: Path,
    *,
    ancestry: tuple[str, ...] = (),
    require_module_export: bool = True,
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
        or (
            require_module_export
            and module.__dict__.get(name) is not dependency
        )
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
        _validate_no_reflection_escape(
            instruction,
            label=f"执行适配器helper {global_name}",
        )
        if instruction.opname in _FORBIDDEN_GLOBAL_OPS:
            raise ValueError(
                f"执行适配器helper包含全局写入或运行期导入：{global_name}"
            )
        if instruction.opname in _FORBIDDEN_SIDE_EFFECT_OPS:
            raise ValueError(
                f"执行适配器helper包含对象属性副作用：{global_name}"
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
            raise ValueError(
                f"执行适配器helper拒绝模块依赖："
                f"{global_name}.{nested_name}"
            )
        elif isinstance(nested, MethodType):
            raise ValueError(
                f"执行适配器helper拒绝绑定方法依赖："
                f"{global_name}.{nested_name}"
            )
        elif type(nested) is FunctionType:
            if (
                nested.__module__ == __name__
                and nested.__name__ in _TRUSTED_FRAMEWORK_PRIMITIVES
                and globals().get(nested.__name__) is nested
            ):
                runtime_dependencies[nested_name] = {
                    "global_name": nested_name,
                    "kind": "trusted_framework_primitive",
                    "module": __name__,
                    "name": nested.__name__,
                    "code_marshaled_sha256": hashlib.sha256(
                        marshal.dumps(nested.__code__)
                    ).hexdigest(),
                    "module_file": _module_dependency_contract(
                        sys.modules[__name__], root,
                    ),
                }
            else:
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
                _class_runtime_dependency_contract(
                    nested_name, nested, root, ancestry=nested_ancestry,
                )
            )
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
    global_name: str,
    dependency: type,
    root: Path,
    *,
    ancestry: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Recursively validate a class, its state, bases and callable methods."""

    module = inspect.getmodule(dependency)
    dependency_name = str(dependency.__name__ or "")
    qualname = str(dependency.__qualname__ or "")
    if (
        not isinstance(module, ModuleType)
        or not dependency_name
        or str(dependency.__module__ or "") != str(module.__name__)
        or module.__dict__.get(dependency_name) is not dependency
    ):
        raise ValueError(f"执行适配器class依赖模块无效：{global_name}")
    identity = f"class:{module.__name__}:{qualname}"
    if identity in ancestry:
        return {
            "global_name": global_name,
            "kind": "class_cycle_ref",
            "identity": identity,
        }
    nested_ancestry = (*ancestry, identity)
    class_values: list[dict[str, Any]] = []
    methods: list[dict[str, Any]] = []
    for name, raw in sorted(vars(dependency).items()):
        if name in {
            "__dict__", "__weakref__", "__module__", "__doc__",
            "__annotations__", "__qualname__",
        }:
            continue
        raw_type = type(raw)
        descriptor = raw_type in {staticmethod, classmethod, property}
        if raw_type in {staticmethod, classmethod}:
            functions = [raw.__func__]
        elif raw_type is property:
            functions = [raw.fget, raw.fset, raw.fdel]
        else:
            functions = [raw]
        found_function = False
        for function in functions:
            if function is None:
                continue
            if descriptor and type(function) is not FunctionType:
                raise ValueError(
                    f"执行适配器class描述符必须包装纯Python函数："
                    f"{global_name}.{name}"
                )
            if type(function) is FunctionType:
                found_function = True
                methods.append(
                    _function_runtime_dependency_contract(
                        f"{global_name}.{name}",
                        function,
                        root,
                        ancestry=nested_ancestry,
                        require_module_export=False,
                    )
                )
        if found_function or descriptor:
            continue
        if isinstance(raw, MethodType):
            raise ValueError(
                f"执行适配器class拒绝绑定方法类属性："
                f"{global_name}.{name}"
            )
        if _immutable_global(raw):
            class_values.append({
                "name": str(name),
                "value": _immutable_runtime_value_contract(raw),
            })
            continue
        raise ValueError(
            f"执行适配器class拒绝可变或不透明类属性："
            f"{global_name}.{name}"
        )
    bases: list[dict[str, Any]] = []
    for base in dependency.__bases__:
        if base in {object, type}:
            continue
        bases.append(
            _class_runtime_dependency_contract(
                f"{global_name}.__base__", base, root,
                ancestry=nested_ancestry,
            )
        )
    metaclass = type(dependency)
    metaclass_contract = None
    if metaclass is not type:
        metaclass_contract = _class_runtime_dependency_contract(
            f"{global_name}.__metaclass__",
            metaclass,
            root,
            ancestry=nested_ancestry,
        )
    return {
        "global_name": global_name,
        "kind": "class",
        "module": str(module.__name__),
        "name": str(dependency.__name__ or ""),
        "qualname": qualname,
        "bases": bases,
        "metaclass": metaclass_contract,
        "class_values": class_values,
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
        _validate_no_reflection_escape(
            instruction,
            label="执行适配器纯函数",
        )
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
        elif isinstance(dependency, MethodType):
            raise ValueError(
                f"执行适配器拒绝绑定方法全局：{global_name}"
            )
        elif type(dependency) is FunctionType:
            if (
                dependency.__module__ == __name__
                and dependency.__name__ in _TRUSTED_FRAMEWORK_PRIMITIVES
                and globals().get(dependency.__name__) is dependency
            ):
                dependencies[global_name] = {
                    "global_name": global_name,
                    "kind": "trusted_framework_primitive",
                    "module": __name__,
                    "name": dependency.__name__,
                    "code_marshaled_sha256": hashlib.sha256(
                        marshal.dumps(dependency.__code__)
                    ).hexdigest(),
                    "module_file": _module_dependency_contract(
                        sys.modules[__name__], root,
                    ),
                }
            else:
                dependencies[global_name] = (
                    _function_runtime_dependency_contract(
                        global_name, dependency, root,
                    )
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


_DYNAMIC_LEDGER_IDENTITY_FIELDS = (
    "strategy_key",
    "strategy_version",
    "strategy_version_hash",
    "execution_binding_hash",
)


def _dynamic_ledger_identity(
    *,
    strategy_key: Any,
    strategy_version: Any,
    strategy_version_hash: Any,
    execution_binding_hash: Any,
) -> tuple[str, str, str, str]:
    return (
        str(strategy_key or ""),
        str(strategy_version or ""),
        str(strategy_version_hash or ""),
        str(execution_binding_hash or ""),
    )


def _dynamic_ledger_failure(
    identity: tuple[str, str, str, str],
    exc: Exception,
    *,
    schema_readable: bool,
) -> dict[str, Any]:
    error = _dynamic_ledger_error_detail(exc)
    return {
        "schema": "probiga.dynamic-shadow-ledger-readiness.v1",
        **dict(zip(_DYNAMIC_LEDGER_IDENTITY_FIELDS, identity)),
        "status": "INVALID" if schema_readable else "UNAVAILABLE_OR_INVALID",
        "schema_readable": schema_readable,
        "shadow_trial_producer_ready": False,
        "funding_pipeline_ready": False,
        "verified_forward_evidence_ready": False,
        "plan_count": 0,
        "pending_plan_count": 0,
        "verified_chain_count": 0,
        "invalid_chain_count": 1,
        "invalid_chains": [{
            "plan_id": "",
            **error,
        }],
        "ledger_hash": "",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _dynamic_ledger_error_detail(exc: Exception) -> dict[str, str]:
    """Keep explicit adapter input errors actionable; redact infrastructure."""

    if type(exc) is ValueError:
        return {
            "error_type": "ValueError",
            "error_code": "DYNAMIC_ADAPTER_INPUT_INVALID",
            "reason": str(exc),
            "incident_id": "",
        }
    incident_id = uuid.uuid4().hex
    _LOGGER.error(
        "dynamic adapter ledger incident_id=%s error_type=%s",
        incident_id,
        type(exc).__name__,
    )
    return {
        "error_type": type(exc).__name__,
        "error_code": "DYNAMIC_ADAPTER_LEDGER_INTERNAL_FAILURE",
        "reason": "动态策略账本验证发生内部错误，请按事件编号排查",
        "incident_id": incident_id,
    }


def _mapping_rows(
    connection: Any,
    statement: str,
    params: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            text(statement), dict(params or {}),
        ).mappings().all()
    ]


def _bounded_mapping_rows(
    connection: Any,
    statement: str,
    *,
    maximum_rows: int,
    label: str,
) -> list[dict[str, Any]]:
    """Read a complete authoritative relation without a silent LIMIT cut."""

    if maximum_rows < 0:
        raise RuntimeError(f"{label}扫描边界无效")
    rows = _mapping_rows(
        connection,
        statement + " LIMIT :_authoritative_scan_limit",
        {"_authoritative_scan_limit": maximum_rows + 1},
    )
    if len(rows) > maximum_rows:
        raise RuntimeError(f"{label}超出权威计数边界，拒绝截断")
    return rows


def _authoritatively_bounded_mapping_rows(
    connection: Any,
    *,
    count_statement: str,
    statement: str,
    label: str,
) -> list[dict[str, Any]]:
    """Read one whole relation against an explicit authoritative COUNT."""

    count = int(connection.execute(text(count_statement)).scalar_one())
    rows = _bounded_mapping_rows(
        connection,
        statement,
        maximum_rows=count,
        label=label,
    )
    if len(rows) != count:
        raise RuntimeError(f"{label}权威计数发生漂移")
    return rows


_CURRENT_DYNAMIC_PLAN_FROM = """
    FROM st_dynamic_shadow_trial_plan p
    JOIN st_strategy_registry sr
      ON sr.strategy_key=p.strategy_key
     AND sr.current_version=p.strategy_version
    JOIN st_strategy_version sv
      ON sv.strategy_key=sr.strategy_key
     AND sv.version=sr.current_version
    WHERE sv.source_kind='runtime_registry'
"""

_ALL_DYNAMIC_PLAN_FROM = """
    FROM st_dynamic_shadow_trial_plan p
"""


_DYNAMIC_LEDGER_STRUCTURE_PROBE = """
    SELECT p.plan_id, p.strategy_version_hash, p.execution_binding_hash,
           c.chain_id, c.risk_decision_fact_hash,
           b.binding_id, r.run_uid, cf.candidate_hash,
           i.intent_id, d.decision_hash, eo.order_id, ef.fill_id,
           e.evidence_id, a.allocation_id, xo.order_id, xf.fill_id,
           h.snapshot_id, h.row_hash
    FROM st_dynamic_shadow_trial_plan p
    LEFT JOIN st_dynamic_shadow_trial_chain c ON c.plan_id=p.plan_id
    LEFT JOIN st_dynamic_shadow_trial_exit_binding b ON b.chain_id=c.chain_id
    LEFT JOIN st_strategy_adapter_run_receipt r
      ON r.run_uid=p.candidate_run_uid
    LEFT JOIN st_strategy_adapter_candidate_fact cf
      ON cf.candidate_run_uid=p.candidate_run_uid
     AND cf.stock_code=p.stock_code
    LEFT JOIN st_trade_intent_v2 i ON i.intent_id=c.source_intent_id
    LEFT JOIN st_risk_decision_v2 d ON d.intent_id=i.intent_id
    LEFT JOIN st_order_v2 eo ON eo.order_id=c.entry_order_id
    LEFT JOIN st_fill_v2 ef ON ef.fill_id=c.entry_fill_id
    LEFT JOIN st_forward_trade_evidence_v3 e
      ON e.evidence_id=c.forward_evidence_id
    LEFT JOIN st_forward_exit_allocation_v3 a
      ON a.allocation_id=b.allocation_id
    LEFT JOIN st_order_v2 xo ON xo.order_id=b.exit_order_id
    LEFT JOIN st_fill_v2 xf ON xf.fill_id=b.exit_fill_id
    LEFT JOIN st_strategy_industry_history h
      ON h.trade_date=p.trade_date AND h.stock_code=p.stock_code
    WHERE 1=0
"""


def _rows_by_unique_key(
    rows: Iterable[Mapping[str, Any]],
    key: str,
    *,
    label: str,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        identity = str(row.get(key) or "")
        if not identity or identity in result:
            raise RuntimeError(f"{label}身份缺失或重复")
        result[identity] = row
    return result


def _rows_grouped(
    rows: Iterable[Mapping[str, Any]],
    key: str,
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        result.setdefault(str(row.get(key) or ""), []).append(row)
    return result


def _verify_prefetched_industry_fact(
    row: Mapping[str, Any],
    *,
    trade_date: str,
    stock_code: str,
) -> dict[str, Any]:
    from server.engine import dynamic_shadow_ledger as ledger

    payload = {
        "snapshot_id": str(row.get("snapshot_id") or ""),
        "trade_date": str(row.get("trade_date") or "")[:10],
        "as_of_exclusive": ledger._iso_second(row.get("as_of_exclusive")),
        "stock_code": str(row.get("stock_code") or ""),
        "industry_name": str(row.get("industry_name") or ""),
        "industry_type": str(row.get("industry_type") or ""),
        "source_system": str(row.get("source_system") or ""),
        "source_fact_id": str(row.get("source_fact_id") or ""),
        "source_effective_at": ledger._iso_second(
            row.get("source_effective_at")
        ),
        "source_etl_sync_at": ledger._iso_second(
            row.get("source_etl_sync_at")
        ),
    }
    expected_hash = ledger._digest(payload)
    if (
        not _SHA256_PATTERN.fullmatch(payload["snapshot_id"])
        or expected_hash != str(row.get("row_hash") or "")
        or payload["trade_date"] != str(trade_date)[:10]
        or payload["stock_code"] != str(stock_code)
        or not payload["industry_name"]
        or not payload["industry_type"]
    ):
        raise RuntimeError("动态模拟链行业历史事实校验失败")
    return {**payload, "row_hash": expected_hash}


def _verify_prefetched_candidate_run(
    run_uid: str,
    *,
    receipt_rows: Mapping[str, Mapping[str, Any]],
    candidate_rows: Mapping[str, list[dict[str, Any]]],
    receipt_cache: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    from server.engine import dynamic_shadow_ledger as ledger

    if run_uid not in receipt_cache:
        stored_receipt = receipt_rows.get(run_uid)
        if stored_receipt is None:
            raise RuntimeError("动态模拟计划缺少候选运行回执")
        raw_receipt = ledger._strict_json(
            stored_receipt.get("receipt_json"),
            label="动态候选运行回执",
            expected=dict,
        )
        receipt = verify_persisted_strategy_adapter_run_receipt(
            raw_receipt, stored_receipt,
        )
        raw_facts = sorted(
            candidate_rows.get(run_uid, []),
            key=lambda item: (
                int(item.get("candidate_index") or 0),
                str(item.get("stock_code") or ""),
            ),
        )
        if len(raw_facts) != int(receipt.get("candidate_count") or 0):
            raise RuntimeError("候选事实未完整覆盖动态运行回执")
        candidates: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []
        for expected_index, fact_row in enumerate(raw_facts):
            candidate = ledger._strict_json(
                fact_row.get("candidate_json"),
                label="持久化动态候选事实",
                expected=dict,
            )
            payload = ledger._candidate_fact_payload(
                receipt=receipt,
                candidate_index=expected_index,
                candidate=candidate,
            )
            candidate_hash = ledger._digest(payload)
            if (
                int(fact_row.get("candidate_index") or 0) != expected_index
                or str(fact_row.get("candidate_run_uid") or "") != run_uid
                or str(fact_row.get("stock_code") or "")
                != payload["stock_code"]
                or str(fact_row.get("trade_date") or "")[:10]
                != str(receipt.get("trade_date") or "")
                or str(fact_row.get("candidate_hash") or "")
                != candidate_hash
            ):
                raise RuntimeError("持久化动态候选事实身份或哈希无效")
            candidates.append(candidate)
            facts.append({**payload, "candidate_hash": candidate_hash})
        ledger._validate_candidate_facts(receipt, candidates)
        receipt_cache[run_uid] = (receipt, facts)
    return receipt_cache[run_uid]


def _verify_prefetched_plan(
    row: Mapping[str, Any],
    *,
    receipt_rows: Mapping[str, Mapping[str, Any]],
    candidate_rows: Mapping[str, list[dict[str, Any]]],
    receipt_cache: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]],
) -> dict[str, Any]:
    from server.engine import dynamic_shadow_ledger as ledger

    plan_id = str(row.get("plan_id") or "")
    run_uid = str(row.get("candidate_run_uid") or "")
    if (
        int(row.get("automatic_real_order_submission") or 0) != 0
        or int(row.get("real_order_authority") or 0) != 0
        or str(row.get("account_id") or "") != ledger.INTERNAL_PAPER_ACCOUNT_ID
        or str(row.get("plan_status") or "") != "PLANNED_SHADOW_TRIAL"
        or not 1 <= int(row.get("maximum_target_bp") or 0) <= 100
    ):
        raise RuntimeError("动态模拟计划权限、账户、状态或仓位上限无效")
    receipt, facts = _verify_prefetched_candidate_run(
        run_uid,
        receipt_rows=receipt_rows,
        candidate_rows=candidate_rows,
        receipt_cache=receipt_cache,
    )
    if str(row.get("candidate_receipt_hash") or "") != str(
        receipt.get("receipt_hash") or ""
    ):
        raise RuntimeError("动态模拟计划与候选回执哈希不一致")
    for plan_field, receipt_field in (
        ("strategy_key", "strategy_key"),
        ("strategy_version", "strategy_version"),
        ("strategy_version_hash", "strategy_version_hash"),
        ("execution_binding_hash", "execution_binding_hash"),
    ):
        if str(row.get(plan_field) or "") != str(
            receipt.get(receipt_field) or ""
        ):
            raise RuntimeError(f"动态模拟计划字段{plan_field}与回执漂移")
    matches = [
        fact for fact in facts
        if str(fact.get("stock_code") or "") == str(row.get("stock_code") or "")
    ]
    if len(matches) != 1:
        raise RuntimeError("动态模拟计划缺少唯一原始候选事实")
    candidate_fact = matches[0]
    if str(row.get("candidate_fact_hash") or "") != str(
        candidate_fact.get("candidate_hash") or ""
    ):
        raise RuntimeError("动态模拟计划候选事实哈希漂移")
    stored_signal = ledger._strict_json(
        row.get("candidate_signal_json"),
        label="动态模拟计划候选信封",
        expected=dict,
    )
    expected_signal = ledger._candidate_signal_contract(
        candidate_fact["candidate"], receipt, candidate_fact,
    )
    if (
        stored_signal != expected_signal
        or str(row.get("candidate_signal_hash") or "")
        != ledger._digest(expected_signal)
    ):
        raise RuntimeError("动态模拟计划候选信封校验失败")
    payload = ledger._plan_payload(row)
    stored_payload = ledger._strict_json(
        row.get("plan_payload_json"),
        label="动态模拟计划载荷",
        expected=dict,
    )
    plan_hash = ledger._digest(payload)
    if (
        payload != stored_payload
        or plan_hash != str(row.get("plan_hash") or "")
        or ledger._expected_plan_id(payload) != plan_id
        or payload["stock_code"] != expected_signal["stock_code"]
        or payload["trade_date"] != str(receipt.get("trade_date") or "")
    ):
        raise RuntimeError("动态模拟计划身份或哈希校验失败")
    return {
        **payload,
        "plan_id": plan_id,
        "plan_hash": plan_hash,
        "candidate_signal": expected_signal,
        "candidate_fact": candidate_fact,
        "candidate_receipt": receipt,
    }


def _verify_prefetched_bootstrap_contract(
    plan: Mapping[str, Any],
    *,
    intent: Mapping[str, Any],
    risk: Mapping[str, Any],
    evidence: Mapping[str, Any],
    industry_rows: Mapping[tuple[str, str], list[dict[str, Any]]],
) -> dict[str, Any]:
    from server.engine import dynamic_shadow_ledger as ledger

    intent_evidence = ledger._strict_json(
        intent.get("evidence_json"),
        label="动态模拟V2意图证据",
        expected=dict,
    )
    ledger._no_real_authority(intent_evidence, path="intent.evidence")
    authorization = intent_evidence.get("dynamic_shadow_bootstrap")
    risk_binding = intent_evidence.get("dynamic_shadow_risk")
    if not isinstance(authorization, Mapping):
        governance_receipt = intent_evidence.get("strategy_governance")
        if not isinstance(governance_receipt, Mapping):
            raise RuntimeError("动态模拟意图缺少治理计划回执")
        observed_receipt = json.loads(
            ledger._canonical_json(governance_receipt)
        )
        receipt_hash = str(observed_receipt.pop("receipt_hash", ""))
        if (
            observed_receipt.get("schema")
            != "probiga.governance-paper-buy-receipt.v1"
            or not _SHA256_PATTERN.fullmatch(receipt_hash)
            or ledger._digest(observed_receipt) != receipt_hash
            or observed_receipt.get("new_buy_allowed") is not True
            or observed_receipt.get("exit_always_allowed") is not True
            or observed_receipt.get("real_order_authority") is not False
            or str(observed_receipt.get("stock_code") or "")
            != str(plan["stock_code"])
            or str(observed_receipt.get("strategy_key") or "")
            != str(plan["strategy_key"])
            or str(observed_receipt.get("strategy_version") or "")
            != str(plan["strategy_version"])
            or str(observed_receipt.get("strategy_version_hash") or "")
            != str(plan["strategy_version_hash"])
            or str(observed_receipt.get("strategy_source_kind") or "")
            != "runtime_registry"
            or str(observed_receipt.get("trade_date") or "")
            != str(plan["trade_date"])
            or not 1 <= int(observed_receipt.get("target_bp") or 0)
            <= int(plan["maximum_target_bp"])
        ):
            raise RuntimeError("动态模拟意图治理计划回执校验失败")
        return intent_evidence
    if not isinstance(risk_binding, Mapping):
        raise RuntimeError("bootstrap动态模拟意图缺少风险绑定")
    observed_auth = json.loads(ledger._canonical_json(authorization))
    authorization_hash = str(observed_auth.pop("authorization_hash", ""))
    industry_key = (str(plan["trade_date"]), str(plan["stock_code"]))
    matching_industries = industry_rows.get(industry_key, [])
    if len(matching_industries) != 1:
        raise RuntimeError("动态模拟计划缺少唯一目标日行业历史事实")
    industry = _verify_prefetched_industry_fact(
        matching_industries[0],
        trade_date=str(plan["trade_date"]),
        stock_code=str(plan["stock_code"]),
    )
    expected_auth = {
        "schema": ledger.BOOTSTRAP_AUTHORIZATION_SCHEMA,
        "plan_id": plan["plan_id"],
        "plan_hash": plan["plan_hash"],
        "candidate_run_uid": plan["candidate_run_uid"],
        "candidate_receipt_hash": plan["candidate_receipt_hash"],
        "candidate_fact_hash": plan["candidate_fact_hash"],
        "candidate_signal_hash": plan["candidate_signal_hash"],
        "strategy_key": plan["strategy_key"],
        "strategy_version": plan["strategy_version"],
        "strategy_version_hash": plan["strategy_version_hash"],
        "execution_binding_hash": plan["execution_binding_hash"],
        "trade_date": plan["trade_date"],
        "stock_code": plan["stock_code"],
        "account_id": plan["account_id"],
        "maximum_target_bp": int(plan["maximum_target_bp"]),
        "industry_snapshot_id": industry["snapshot_id"],
        "industry_row_hash": industry["row_hash"],
        "industry_name": industry["industry_name"],
        "industry_type": industry["industry_type"],
        "shadow_forecast_id": ledger._digest({
            "schema": "probiga.dynamic-shadow-bootstrap-forecast.v1",
            "plan_id": plan["plan_id"],
            "plan_hash": plan["plan_hash"],
        }),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    if (
        observed_auth != expected_auth
        or not _SHA256_PATTERN.fullmatch(authorization_hash)
        or ledger._digest(observed_auth) != authorization_hash
    ):
        raise RuntimeError("动态模拟计划授权身份或行业绑定校验失败")

    observed_risk = json.loads(ledger._canonical_json(risk_binding))
    binding_hash = str(observed_risk.pop("binding_hash", ""))
    decision_payload = observed_risk.get("decision_payload")
    if (
        set(observed_risk) != {
            "schema", "decision_payload", "decision_hash",
            "automatic_real_order_submission", "real_order_authority",
        }
        or observed_risk.get("schema") != ledger.BOOTSTRAP_RISK_SCHEMA
        or not isinstance(decision_payload, Mapping)
        or not _SHA256_PATTERN.fullmatch(binding_hash)
        or ledger._digest(observed_risk) != binding_hash
        or observed_risk.get("automatic_real_order_submission") is not False
        or observed_risk.get("real_order_authority") is not False
    ):
        raise RuntimeError("动态模拟风险绑定身份、权限或哈希校验失败")
    payload = json.loads(ledger._canonical_json(decision_payload))
    decision_hash = ledger._digest(payload)
    expected_payload_keys = {
        "schema", "plan_id", "authorization_hash", "authorization",
        "intent_id", "account_id", "strategy_key", "strategy_version",
        "trade_date", "execution_date", "stock_code",
        "industry_snapshot_id", "industry_row_hash", "industry_name",
        "equity_cny", "reference_price", "worst_price", "initial_stop",
        "maximum_target_bp", "requested_quantity", "approved_quantity",
        "current_code_value", "current_total_value",
        "current_industry_value", "current_open_risk_cny",
        "current_daily_buy_turnover_cny", "available_cash_cny",
        "live_position_count", "limits", "decision_status", "checks",
        "first_failure", "trade_risk", "post_single_weight",
        "post_total_weight", "post_theme_weight", "post_open_risk_cny",
        "post_cash", "post_turnover_weight",
        "automatic_real_order_submission", "real_order_authority",
    }
    required_checks = {
        "CASH_AVAILABLE", "SINGLE_POSITION_CAP", "TOTAL_RISK_ASSET_CAP",
        "THEME_EXPOSURE_CAP", "OPEN_RISK_CAP", "DAILY_TURNOVER_CAP",
        "LIVE_POSITION_CAP", "REAL_TRADING_DISABLED",
    }
    checks = ledger._strict_json(
        risk.get("checks_json"),
        label="动态模拟V2风险检查",
        expected=dict,
    )
    if (
        set(payload) != expected_payload_keys
        or payload.get("schema")
        != "probiga.dynamic-shadow-bootstrap-risk-decision.v1"
        or str(payload.get("plan_id") or "") != str(plan["plan_id"])
        or str(payload.get("authorization_hash") or "") != authorization_hash
        or payload.get("authorization") != dict(authorization)
        or str(payload.get("intent_id") or "")
        != str(intent.get("intent_id") or "")
        or str(payload.get("account_id") or "") != str(plan["account_id"])
        or str(payload.get("strategy_key") or "") != str(plan["strategy_key"])
        or str(payload.get("strategy_version") or "")
        != str(plan["strategy_version"])
        or str(payload.get("stock_code") or "") != str(plan["stock_code"])
        or str(payload.get("industry_snapshot_id") or "")
        != industry["snapshot_id"]
        or str(payload.get("industry_row_hash") or "") != industry["row_hash"]
        or str(observed_risk.get("decision_hash") or "") != decision_hash
        or str(risk.get("decision_hash") or "") != decision_hash
        or str(payload.get("decision_status") or "") != "APPROVED"
        or str(risk.get("decision_status") or "") != "APPROVED"
        or int(risk.get("requested_quantity") or 0) <= 0
        or int(risk.get("approved_quantity") or 0)
        != int(risk.get("requested_quantity") or 0)
        or int(payload.get("requested_quantity") or 0)
        != int(risk.get("requested_quantity") or 0)
        or int(payload.get("approved_quantity") or 0)
        != int(risk.get("approved_quantity") or 0)
        or int(payload.get("maximum_target_bp") or 0)
        != int(plan["maximum_target_bp"])
        or checks != payload.get("checks")
        or set(checks) != required_checks
        or any(value is not True for value in checks.values())
        or str(risk.get("first_failure") or "")
        != str(payload.get("first_failure") or "")
        or payload.get("automatic_real_order_submission") is not False
        or payload.get("real_order_authority") is not False
    ):
        raise RuntimeError("动态模拟V2风险决策与冻结绑定校验失败")
    for row_field, payload_field in (
        ("trade_risk", "trade_risk"),
        ("post_single_weight", "post_single_weight"),
        ("post_total_weight", "post_total_weight"),
        ("post_theme_weight", "post_theme_weight"),
        ("post_open_risk", "post_open_risk_cny"),
        ("post_cash", "post_cash"),
    ):
        if Decimal(str(risk.get(row_field) or 0)) != Decimal(
            str(payload.get(payload_field) or 0)
        ):
            raise RuntimeError("动态模拟V2风险数值与冻结绑定漂移")
    equity = Decimal(str(payload.get("equity_cny") or 0))
    worst_price = Decimal(str(payload.get("worst_price") or 0))
    requested = Decimal(int(risk.get("requested_quantity") or 0))
    limits = payload.get("limits")
    if (
        equity <= 0
        or worst_price <= 0
        or not isinstance(limits, Mapping)
        or requested * worst_price / equity
        > Decimal(int(plan["maximum_target_bp"])) / Decimal(10000)
        or Decimal(str(payload.get("post_single_weight") or 0))
        > Decimal(str(limits.get("maximum_single_weight") or 0))
        or Decimal(str(payload.get("post_total_weight") or 0))
        > Decimal(str(limits.get("maximum_total_weight") or 0))
        or Decimal(str(payload.get("post_theme_weight") or 0))
        > Decimal(str(limits.get("maximum_industry_weight") or 0))
        or Decimal(str(payload.get("post_open_risk_cny") or 0)) / equity
        > Decimal(str(limits.get("maximum_open_risk_weight") or 0))
        or Decimal(str(payload.get("post_turnover_weight") or 0))
        > Decimal(str(limits.get("maximum_daily_buy_turnover_weight") or 0))
    ):
        raise RuntimeError("动态模拟风险上限复算失败")
    return intent_evidence


def _verify_prefetched_chain(
    plan: Mapping[str, Any],
    chain: Mapping[str, Any],
    *,
    intents: Mapping[str, Mapping[str, Any]],
    risks: Mapping[str, Mapping[str, Any]],
    entry_orders: Mapping[str, Mapping[str, Any]],
    entry_fills: Mapping[str, Mapping[str, Any]],
    evidences: Mapping[str, Mapping[str, Any]],
    bindings_by_chain: Mapping[str, list[dict[str, Any]]],
    allocations: Mapping[str, Mapping[str, Any]],
    exit_orders: Mapping[str, Mapping[str, Any]],
    exit_fills: Mapping[str, Mapping[str, Any]],
    industry_rows: Mapping[tuple[str, str], list[dict[str, Any]]],
) -> dict[str, Any]:
    from server.engine import dynamic_shadow_ledger as ledger

    if (
        int(chain.get("automatic_real_order_submission") or 0) != 0
        or int(chain.get("real_order_authority") or 0) != 0
    ):
        raise RuntimeError("动态模拟完整链错误声明真实下单权限")
    intent = dict(intents[str(chain.get("source_intent_id") or "")])
    risk = dict(risks[str(intent.get("intent_id") or "")])
    entry_order = dict(entry_orders[str(chain.get("entry_order_id") or "")])
    entry_fill = dict(entry_fills[str(chain.get("entry_fill_id") or "")])
    evidence = dict(evidences[str(chain.get("forward_evidence_id") or "")])
    exits: list[dict[str, Any]] = []
    stored_bindings = sorted(
        bindings_by_chain.get(str(chain.get("chain_id") or ""), []),
        key=lambda item: (
            str(item.get("allocation_id") or ""),
            str(item.get("binding_id") or ""),
        ),
    )
    for binding in stored_bindings:
        exits.append({
            "allocation": dict(
                allocations[str(binding.get("allocation_id") or "")]
            ),
            "order": dict(exit_orders[str(binding.get("exit_order_id") or "")]),
            "fill": dict(exit_fills[str(binding.get("exit_fill_id") or "")]),
        })
    facts = {
        "evidence": evidence,
        "intent": intent,
        "risk_decision": risk,
        "entry_order": entry_order,
        "entry_fill": entry_fill,
        "exits": exits,
    }
    account = ledger.INTERNAL_PAPER_ACCOUNT_ID
    stock_code = str(plan["stock_code"])
    if any(
        str(item.get("account_id") or "") != account
        or str(item.get("stock_code") or "") != stock_code
        for item in (intent, entry_order, entry_fill, evidence)
    ):
        raise RuntimeError("动态模拟链账户或证券身份不一致")
    if (
        str(intent.get("intent_id") or "")
        != str(evidence.get("source_intent_id") or "")
        or str(intent.get("decision_run_uid") or "")
        != str(evidence.get("source_run_uid") or "")
        or str(intent.get("action") or "").upper() != "BUY"
        or str(intent.get("reason_code") or "")
        not in ledger.EXECUTED_INTENT_REASONS
        or str(entry_order.get("intent_id") or "")
        != str(intent.get("intent_id") or "")
        or str(entry_order.get("order_id") or "")
        != str(evidence.get("entry_order_id") or "")
        or str(entry_order.get("side") or "").upper() != "BUY"
        or str(entry_order.get("status") or "").upper() != "FILLED"
        or int(entry_order.get("quantity") or 0)
        != int(entry_order.get("filled_quantity") or -1)
        or str(entry_fill.get("order_id") or "")
        != str(entry_order.get("order_id") or "")
        or str(entry_fill.get("fill_id") or "")
        != str(evidence.get("entry_fill_id") or "")
        or str(entry_fill.get("side") or "").upper() != "BUY"
        or int(entry_fill.get("quantity") or 0)
        != int(evidence.get("entry_quantity") or -1)
        or str(evidence.get("strategy_key") or "") != str(plan["strategy_key"])
        or str(evidence.get("strategy_version") or "")
        != str(plan["strategy_version"])
        or str(evidence.get("sample_owner_role") or "") != "PRIMARY"
        or str(evidence.get("attribution_status") or "")
        != "VERIFIED_SNAPSHOT"
        or str(evidence.get("attribution_version") or "")
        != ledger.ATTRIBUTION_VERSION
        or str(evidence.get("evidence_kind") or "") != "EXECUTED_PAPER"
        or str(evidence.get("protocol_version") or "")
        != ledger.EXECUTED_FORWARD_PROTOCOL
        or str(evidence.get("evidence_status") or "") != "MATURED"
        or int(evidence.get("closed_quantity") or 0)
        != int(evidence.get("entry_quantity") or -1)
    ):
        raise RuntimeError("动态模拟V2/V3事实关系校验失败")
    intent_evidence = _verify_prefetched_bootstrap_contract(
        plan,
        intent=intent,
        risk=risk,
        evidence=evidence,
        industry_rows=industry_rows,
    )
    is_bootstrap = isinstance(
        intent_evidence.get("dynamic_shadow_bootstrap"), Mapping,
    )
    supporting_keys = ledger._strict_json(
        evidence.get("supporting_strategy_keys_json"),
        label="动态模拟证据归属集合",
        expected=list,
    )
    expected_ownership_hash = hashlib.sha256(
        (
            f"{evidence.get('source_run_uid')}|"
            f"{evidence.get('source_forecast_id')}|"
            f"{evidence.get('stock_code')}|"
            f"{evidence.get('strategy_key')}|"
            f"{evidence.get('strategy_version')}"
        ).encode("utf-8")
    ).hexdigest()
    if (
        supporting_keys != [plan["strategy_key"]]
        or str(intent_evidence.get("primary_strategy_key") or "")
        != str(plan["strategy_key"])
        or str(intent_evidence.get("primary_strategy_version") or "")
        != str(plan["strategy_version"])
        or str(intent_evidence.get("primary_forecast_id") or "")
        != str(evidence.get("source_forecast_id") or "")
        or str(intent_evidence.get("run_uid") or "")
        != str(evidence.get("source_run_uid") or "")
        or str(intent_evidence.get("ownership_hash") or "")
        != expected_ownership_hash
        or str(evidence.get("ownership_hash") or "")
        != expected_ownership_hash
        or (
            is_bootstrap
            and str(intent.get("strategy_version") or "")
            != str(plan["strategy_version"])
        )
        or (
            is_bootstrap
            and int(entry_order.get("quantity") or 0)
            != int(risk.get("approved_quantity") or 0)
        )
    ):
        raise RuntimeError("动态模拟链策略归属、版本或风险数量绑定失败")
    if not exits:
        raise RuntimeError("成熟动态模拟链缺少FIFO退出分配")
    allocated_quantity = 0
    exit_order_ids: list[str] = []
    exit_fill_ids: list[str] = []
    for item in exits:
        allocation = item["allocation"]
        order = item["order"]
        fill = item["fill"]
        if (
            str(allocation.get("evidence_id") or "")
            != str(evidence.get("evidence_id") or "")
            or str(allocation.get("attribution_status") or "") != "ATTRIBUTED"
            or str(allocation.get("account_id") or "") != account
            or str(allocation.get("stock_code") or "") != stock_code
            or str(allocation.get("entry_fill_id") or "")
            != str(entry_fill.get("fill_id") or "")
            or str(allocation.get("allocation_protocol_version") or "")
            != ledger.EXIT_ALLOCATION_PROTOCOL
            or str(order.get("order_id") or "")
            != str(allocation.get("exit_order_id") or "")
            or str(order.get("side") or "").upper() != "SELL"
            or str(order.get("status") or "").upper() != "FILLED"
            or str(fill.get("fill_id") or "")
            != str(allocation.get("exit_fill_id") or "")
            or str(fill.get("order_id") or "") != str(order.get("order_id") or "")
            or str(fill.get("side") or "").upper() != "SELL"
            or int(allocation.get("allocated_quantity") or 0) <= 0
        ):
            raise RuntimeError("动态模拟FIFO退出事实关系校验失败")
        allocated_quantity += int(allocation["allocated_quantity"])
        exit_order_ids.append(str(order["order_id"]))
        exit_fill_ids.append(str(fill["fill_id"]))
    if (
        allocated_quantity != int(evidence.get("closed_quantity") or 0)
        or sorted(set(exit_order_ids)) != sorted(set(
            str(item) for item in ledger._strict_json(
                evidence.get("exit_order_ids_json"),
                label="动态模拟退出订单集合",
                expected=list,
            )
        ))
        or sorted(set(exit_fill_ids)) != sorted(set(
            str(item) for item in ledger._strict_json(
                evidence.get("exit_fill_ids_json"),
                label="动态模拟退出成交集合",
                expected=list,
            )
        ))
    ):
        raise RuntimeError("动态模拟FIFO退出数量或身份集合不守恒")

    expected = ledger._chain_contract(plan, facts)
    payload = expected["chain_payload"]
    stored_payload = ledger._strict_json(
        chain.get("chain_payload_json"),
        label="动态模拟完整链载荷",
        expected=dict,
    )
    scalar_fields = (
        "plan_id", "source_intent_id", "entry_order_id", "entry_fill_id",
        "forward_evidence_id", "intent_fact_hash", "risk_decision_fact_hash",
        "entry_order_fact_hash", "entry_fill_fact_hash",
        "forward_evidence_fact_hash", "exit_set_hash",
    )
    if (
        str(chain.get("chain_id") or "") != expected["chain_id"]
        or str(chain.get("chain_hash") or "") != expected["chain_hash"]
        or int(chain.get("exit_binding_count") or 0)
        != len(expected["exit_rows"])
        or stored_payload != payload
        or any(
            str(chain.get(field) or "") != str(payload.get(field) or "")
            for field in scalar_fields
        )
        or len(stored_bindings) != len(expected["exit_rows"])
    ):
        raise RuntimeError("动态模拟完整链身份或事实哈希复算失败")
    for stored, wanted in zip(stored_bindings, expected["exit_rows"]):
        wanted_payload = {
            key: wanted[key]
            for key in (
                "schema", "chain_id", "allocation_id", "exit_order_id",
                "exit_fill_id", "allocation_fact_hash",
                "exit_order_fact_hash", "exit_fill_fact_hash",
                "real_order_authority",
            )
        }
        if (
            int(stored.get("real_order_authority") or 0) != 0
            or ledger._strict_json(
                stored.get("binding_payload_json"),
                label="动态模拟退出绑定载荷",
                expected=dict,
            ) != wanted_payload
            or any(
                str(stored.get(field) or "") != str(wanted.get(field) or "")
                for field in (
                    "binding_id", "chain_id", "allocation_id",
                    "exit_order_id", "exit_fill_id", "allocation_fact_hash",
                    "exit_order_fact_hash", "exit_fill_fact_hash",
                    "binding_hash",
                )
            )
        ):
            raise RuntimeError("动态模拟退出绑定哈希复算失败")
    return {
        "plan_id": plan["plan_id"],
        "chain_hash": expected["chain_hash"],
        "real_order_authority": False,
        "automatic_real_order_submission": False,
    }


def batch_verify_all_strategy_adapter_candidate_facts(
    connection: Any,
) -> dict[str, dict[str, Any]]:
    """Replay every persisted candidate run with a constant SQL query count."""

    facts = _authoritatively_bounded_mapping_rows(
        connection,
        count_statement=(
            "SELECT COUNT(*) FROM st_strategy_adapter_candidate_fact"
        ),
        statement=(
            "SELECT * FROM st_strategy_adapter_candidate_fact "
            "ORDER BY candidate_run_uid, candidate_index, stock_code"
        ),
        label="全部动态候选原始事实",
    )
    receipts = _authoritatively_bounded_mapping_rows(
        connection,
        count_statement=(
            "SELECT COUNT(*) FROM st_strategy_adapter_run_receipt r "
            "WHERE EXISTS (SELECT 1 "
            "FROM st_strategy_adapter_candidate_fact cf "
            "WHERE cf.candidate_run_uid=r.run_uid)"
        ),
        statement=(
            "SELECT r.* FROM st_strategy_adapter_run_receipt r "
            "WHERE EXISTS (SELECT 1 "
            "FROM st_strategy_adapter_candidate_fact cf "
            "WHERE cf.candidate_run_uid=r.run_uid) ORDER BY r.run_uid"
        ),
        label="全部动态候选运行回执",
    )
    facts_by_run = _rows_grouped(facts, "candidate_run_uid")
    receipts_by_run = _rows_by_unique_key(
        receipts, "run_uid", label="全部动态候选运行回执",
    )
    if set(receipts_by_run) != set(facts_by_run):
        raise RuntimeError("动态候选事实与运行回执身份集合不一致")
    cache: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]] = {}
    result: dict[str, dict[str, Any]] = {}
    for run_uid in sorted(facts_by_run):
        receipt, verified_facts = _verify_prefetched_candidate_run(
            run_uid,
            receipt_rows=receipts_by_run,
            candidate_rows=facts_by_run,
            receipt_cache=cache,
        )
        result[run_uid] = {
            "candidate_receipt": receipt,
            "candidate_fact_count": len(verified_facts),
            "candidate_fact_set_hash": _digest([
                {
                    "candidate_index": fact["candidate_index"],
                    "candidate_hash": fact["candidate_hash"],
                }
                for fact in verified_facts
            ]),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    return result


def batch_dynamic_shadow_ledger_readiness(
    connection: Any,
    identities: Iterable[tuple[str, str, str, str]],
    *,
    include_historical: bool = False,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    """Build an exact-version readiness index with O(1) SQL query count.

    The strategy inventory is not capped.  Every relation scan is bounded by
    an authoritative count read from the same connection and fails if that
    boundary would truncate rows.  SQL count therefore stays constant as the
    number of strategies grows; plans and facts are indexed once in memory.
    ``include_historical`` is reserved for production integrity acceptance and
    deliberately removes the current-registry filter without limiting history.
    """

    normalized = sorted(set(tuple(map(str, identity)) for identity in identities))
    if not normalized:
        return {}
    if any(
        len(identity) != 4
        or not identity[0]
        or not identity[1]
        or not _SHA256_PATTERN.fullmatch(identity[2])
        or not _SHA256_PATTERN.fullmatch(identity[3])
        for identity in normalized
    ):
        exc = ValueError("动态模拟链批量查询身份不完整")
        return {
            identity: _dynamic_ledger_failure(
                identity, exc, schema_readable=False,
            )
            for identity in normalized
        }
    schema_readable = False
    try:
        _mapping_rows(connection, _DYNAMIC_LEDGER_STRUCTURE_PROBE)
        schema_readable = True
        plan_from = (
            _ALL_DYNAMIC_PLAN_FROM
            if include_historical
            else _CURRENT_DYNAMIC_PLAN_FROM
        )
        scope_label = "全部历史动态策略模拟计划" if include_historical else (
            "当前动态策略模拟计划"
        )
        plans = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement="SELECT COUNT(*) " + plan_from,
            statement=(
                "SELECT p.* " + plan_from
                + " ORDER BY p.strategy_key, p.strategy_version, p.plan_id"
            ),
            label=scope_label,
        )
        identity_set = set(normalized)
        scoped_plans = [
            row for row in plans
            if _dynamic_ledger_identity(
                strategy_key=row.get("strategy_key"),
                strategy_version=row.get("strategy_version"),
                strategy_version_hash=row.get("strategy_version_hash"),
                execution_binding_hash=row.get("execution_binding_hash"),
            ) in identity_set
        ]
        if not scoped_plans:
            empty_result: dict[
                tuple[str, str, str, str], dict[str, Any]
            ] = {}
            for identity in normalized:
                contract = {
                    **dict(zip(_DYNAMIC_LEDGER_IDENTITY_FIELDS, identity)),
                    "plan_ids": [],
                    "pending_plan_ids": [],
                    "verified_chain_hashes": [],
                    "invalid_plan_ids": [],
                }
                empty_result[identity] = {
                    "schema": "probiga.dynamic-shadow-ledger-readiness.v1",
                    "status": "VERIFIED_EMPTY",
                    **dict(zip(_DYNAMIC_LEDGER_IDENTITY_FIELDS, identity)),
                    "schema_readable": True,
                    "shadow_trial_producer_ready": True,
                    "funding_pipeline_ready": False,
                    "verified_forward_evidence_ready": False,
                    "plan_count": 0,
                    "pending_plan_count": 0,
                    "verified_chain_count": 0,
                    "invalid_chain_count": 0,
                    "invalid_chains": [],
                    "ledger_hash": _digest(contract),
                    "automatic_real_order_submission": False,
                    "real_order_authority": False,
                }
            return empty_result
        plan_exists = """
            EXISTS (
                SELECT 1
                FROM st_dynamic_shadow_trial_plan p
                WHERE 1=1
        """ if include_historical else """
            EXISTS (
                SELECT 1
                FROM st_dynamic_shadow_trial_plan p
                JOIN st_strategy_registry sr
                  ON sr.strategy_key=p.strategy_key
                 AND sr.current_version=p.strategy_version
                JOIN st_strategy_version sv
                  ON sv.strategy_key=sr.strategy_key
                 AND sv.version=sr.current_version
                WHERE sv.source_kind='runtime_registry'
        """
        receipts_where = (
            " WHERE " + plan_exists + " AND p.candidate_run_uid=r.run_uid)"
        )
        receipts = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement=(
                "SELECT COUNT(*) FROM st_strategy_adapter_run_receipt r"
                + receipts_where
            ),
            statement=(
                "SELECT r.* FROM st_strategy_adapter_run_receipt r"
                + receipts_where + " ORDER BY r.run_uid"
            ),
            label="动态候选运行回执",
        )
        candidates_where = (
            " WHERE " + plan_exists
            + " AND p.candidate_run_uid=cf.candidate_run_uid)"
        )
        candidates = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement=(
                "SELECT COUNT(*) FROM st_strategy_adapter_candidate_fact cf"
                + candidates_where
            ),
            statement=(
                "SELECT cf.* FROM st_strategy_adapter_candidate_fact cf"
                + candidates_where
                + " ORDER BY cf.candidate_run_uid, cf.candidate_index, cf.stock_code"
            ),
            label="动态候选原始事实",
        )
        chains_where = " WHERE " + plan_exists + " AND p.plan_id=c.plan_id)"
        chains = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement=(
                "SELECT COUNT(*) FROM st_dynamic_shadow_trial_chain c"
                + chains_where
            ),
            statement=(
                "SELECT c.* FROM st_dynamic_shadow_trial_chain c"
                + chains_where + " ORDER BY c.plan_id"
            ),
            label="动态模拟完整链",
        )
        chain_plan_exists = """
            EXISTS (
                SELECT 1
                FROM st_dynamic_shadow_trial_chain c
                JOIN st_dynamic_shadow_trial_plan p ON p.plan_id=c.plan_id
                WHERE 1=1
        """ if include_historical else """
            EXISTS (
                SELECT 1
                FROM st_dynamic_shadow_trial_chain c
                JOIN st_dynamic_shadow_trial_plan p ON p.plan_id=c.plan_id
                JOIN st_strategy_registry sr
                  ON sr.strategy_key=p.strategy_key
                 AND sr.current_version=p.strategy_version
                JOIN st_strategy_version sv
                  ON sv.strategy_key=sr.strategy_key
                 AND sv.version=sr.current_version
                WHERE sv.source_kind='runtime_registry'
        """
        intents_where = (
            " WHERE " + chain_plan_exists + " AND c.source_intent_id=i.intent_id)"
        )
        intents = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement="SELECT COUNT(*) FROM st_trade_intent_v2 i" + intents_where,
            statement=(
                "SELECT i.* FROM st_trade_intent_v2 i" + intents_where
                + " ORDER BY i.intent_id"
            ),
            label="动态模拟V2意图",
        )
        risks_where = (
            " WHERE " + chain_plan_exists + " AND c.source_intent_id=d.intent_id)"
        )
        risks = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement="SELECT COUNT(*) FROM st_risk_decision_v2 d" + risks_where,
            statement=(
                "SELECT d.* FROM st_risk_decision_v2 d" + risks_where
                + " ORDER BY d.intent_id"
            ),
            label="动态模拟V2风险决策",
        )
        entry_orders_where = (
            " WHERE " + chain_plan_exists + " AND c.entry_order_id=o.order_id)"
        )
        entry_orders = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement="SELECT COUNT(*) FROM st_order_v2 o" + entry_orders_where,
            statement=(
                "SELECT o.* FROM st_order_v2 o" + entry_orders_where
                + " ORDER BY o.order_id"
            ),
            label="动态模拟V2买入订单",
        )
        entry_fills_where = (
            " WHERE " + chain_plan_exists + " AND c.entry_fill_id=f.fill_id)"
        )
        entry_fills = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement="SELECT COUNT(*) FROM st_fill_v2 f" + entry_fills_where,
            statement=(
                "SELECT f.* FROM st_fill_v2 f" + entry_fills_where
                + " ORDER BY f.fill_id"
            ),
            label="动态模拟V2买入成交",
        )
        evidences_where = (
            " WHERE " + chain_plan_exists
            + " AND c.forward_evidence_id=e.evidence_id)"
        )
        evidences = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement=(
                "SELECT COUNT(*) FROM st_forward_trade_evidence_v3 e"
                + evidences_where
            ),
            statement=(
                "SELECT e.* FROM st_forward_trade_evidence_v3 e"
                + evidences_where + " ORDER BY e.evidence_id"
            ),
            label="动态模拟V3成熟证据",
        )
        industries_where = (
            " WHERE " + plan_exists
            + " AND p.trade_date=h.trade_date AND p.stock_code=h.stock_code)"
        )
        industries = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement=(
                "SELECT COUNT(*) FROM st_strategy_industry_history h"
                + industries_where
            ),
            statement=(
                "SELECT h.* FROM st_strategy_industry_history h"
                + industries_where
                + " ORDER BY h.trade_date, h.stock_code, h.snapshot_id"
            ),
            label="动态模拟目标日行业事实",
        )
        binding_exists = """
            EXISTS (
                SELECT 1
                FROM st_dynamic_shadow_trial_chain c
                JOIN st_dynamic_shadow_trial_plan p ON p.plan_id=c.plan_id
                WHERE 1=1
        """ if include_historical else """
            EXISTS (
                SELECT 1
                FROM st_dynamic_shadow_trial_chain c
                JOIN st_dynamic_shadow_trial_plan p ON p.plan_id=c.plan_id
                JOIN st_strategy_registry sr
                  ON sr.strategy_key=p.strategy_key
                 AND sr.current_version=p.strategy_version
                JOIN st_strategy_version sv
                  ON sv.strategy_key=sr.strategy_key
                 AND sv.version=sr.current_version
                WHERE sv.source_kind='runtime_registry'
        """
        bindings_where = " WHERE " + binding_exists + " AND c.chain_id=b.chain_id)"
        bindings = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement=(
                "SELECT COUNT(*) FROM st_dynamic_shadow_trial_exit_binding b"
                + bindings_where
            ),
            statement=(
                "SELECT b.* FROM st_dynamic_shadow_trial_exit_binding b"
                + bindings_where
                + " ORDER BY b.chain_id, b.allocation_id, b.binding_id"
            ),
            label="动态模拟FIFO退出绑定",
        )
        exit_binding_exists = """
            EXISTS (
                SELECT 1
                FROM st_dynamic_shadow_trial_exit_binding b
                JOIN st_dynamic_shadow_trial_chain c ON c.chain_id=b.chain_id
                JOIN st_dynamic_shadow_trial_plan p ON p.plan_id=c.plan_id
                WHERE 1=1
        """ if include_historical else """
            EXISTS (
                SELECT 1
                FROM st_dynamic_shadow_trial_exit_binding b
                JOIN st_dynamic_shadow_trial_chain c ON c.chain_id=b.chain_id
                JOIN st_dynamic_shadow_trial_plan p ON p.plan_id=c.plan_id
                JOIN st_strategy_registry sr
                  ON sr.strategy_key=p.strategy_key
                 AND sr.current_version=p.strategy_version
                JOIN st_strategy_version sv
                  ON sv.strategy_key=sr.strategy_key
                 AND sv.version=sr.current_version
                WHERE sv.source_kind='runtime_registry'
        """
        allocations_where = (
            " WHERE " + exit_binding_exists
            + " AND b.allocation_id=a.allocation_id)"
        )
        allocations = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement=(
                "SELECT COUNT(*) FROM st_forward_exit_allocation_v3 a"
                + allocations_where
            ),
            statement=(
                "SELECT a.* FROM st_forward_exit_allocation_v3 a"
                + allocations_where + " ORDER BY a.allocation_id"
            ),
            label="动态模拟V3退出分配",
        )
        exit_orders_where = (
            " WHERE " + exit_binding_exists + " AND b.exit_order_id=o.order_id)"
        )
        exit_orders = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement="SELECT COUNT(*) FROM st_order_v2 o" + exit_orders_where,
            statement=(
                "SELECT o.* FROM st_order_v2 o" + exit_orders_where
                + " ORDER BY o.order_id"
            ),
            label="动态模拟V2退出订单",
        )
        exit_fills_where = (
            " WHERE " + exit_binding_exists + " AND b.exit_fill_id=f.fill_id)"
        )
        exit_fills = _authoritatively_bounded_mapping_rows(
            connection,
            count_statement="SELECT COUNT(*) FROM st_fill_v2 f" + exit_fills_where,
            statement=(
                "SELECT f.* FROM st_fill_v2 f" + exit_fills_where
                + " ORDER BY f.fill_id"
            ),
            label="动态模拟V2退出成交",
        )

        receipts_by_run = _rows_by_unique_key(
            receipts, "run_uid", label="动态候选运行回执",
        )
        candidates_by_run = _rows_grouped(candidates, "candidate_run_uid")
        chains_by_plan = _rows_grouped(chains, "plan_id")
        intents_by_id = _rows_by_unique_key(
            intents, "intent_id", label="动态模拟V2意图",
        )
        risks_by_intent = _rows_by_unique_key(
            risks, "intent_id", label="动态模拟V2风险决策",
        )
        entry_orders_by_id = _rows_by_unique_key(
            entry_orders, "order_id", label="动态模拟V2买入订单",
        )
        entry_fills_by_id = _rows_by_unique_key(
            entry_fills, "fill_id", label="动态模拟V2买入成交",
        )
        evidences_by_id = _rows_by_unique_key(
            evidences, "evidence_id", label="动态模拟V3成熟证据",
        )
        bindings_by_chain = _rows_grouped(bindings, "chain_id")
        allocations_by_id = _rows_by_unique_key(
            allocations, "allocation_id", label="动态模拟V3退出分配",
        )
        exit_orders_by_id = _rows_by_unique_key(
            exit_orders, "order_id", label="动态模拟V2退出订单",
        )
        exit_fills_by_id = _rows_by_unique_key(
            exit_fills, "fill_id", label="动态模拟V2退出成交",
        )
        industries_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for industry in industries:
            industries_by_key.setdefault((
                str(industry.get("trade_date") or "")[:10],
                str(industry.get("stock_code") or ""),
            ), []).append(industry)

        plans_by_identity: dict[
            tuple[str, str, str, str], list[dict[str, Any]]
        ] = {identity: [] for identity in normalized}
        for plan in scoped_plans:
            plans_by_identity[_dynamic_ledger_identity(
                strategy_key=plan.get("strategy_key"),
                strategy_version=plan.get("strategy_version"),
                strategy_version_hash=plan.get("strategy_version_hash"),
                execution_binding_hash=plan.get("execution_binding_hash"),
            )].append(plan)
        receipt_cache: dict[
            str, tuple[dict[str, Any], list[dict[str, Any]]]
        ] = {}
        result: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for identity in normalized:
            identity_plans = sorted(
                plans_by_identity[identity],
                key=lambda item: str(item.get("plan_id") or ""),
            )
            pending: list[str] = []
            verified: list[dict[str, Any]] = []
            invalid: list[dict[str, str]] = []
            for raw_plan in identity_plans:
                plan_id = str(raw_plan.get("plan_id") or "")
                try:
                    plan = _verify_prefetched_plan(
                        raw_plan,
                        receipt_rows=receipts_by_run,
                        candidate_rows=candidates_by_run,
                        receipt_cache=receipt_cache,
                    )
                    exact = _dynamic_ledger_identity(
                        strategy_key=plan.get("strategy_key"),
                        strategy_version=plan.get("strategy_version"),
                        strategy_version_hash=plan.get("strategy_version_hash"),
                        execution_binding_hash=plan.get("execution_binding_hash"),
                    )
                    if exact != identity:
                        raise RuntimeError("动态模拟计划越出精确版本边界")
                    matching_chains = chains_by_plan.get(plan_id, [])
                    if not matching_chains:
                        pending.append(plan_id)
                    elif len(matching_chains) == 1:
                        verified.append(_verify_prefetched_chain(
                            plan,
                            matching_chains[0],
                            intents=intents_by_id,
                            risks=risks_by_intent,
                            entry_orders=entry_orders_by_id,
                            entry_fills=entry_fills_by_id,
                            evidences=evidences_by_id,
                            bindings_by_chain=bindings_by_chain,
                            allocations=allocations_by_id,
                            exit_orders=exit_orders_by_id,
                            exit_fills=exit_fills_by_id,
                            industry_rows=industries_by_key,
                        ))
                    else:
                        raise RuntimeError("同一动态模拟计划存在多条完整链")
                except Exception as exc:
                    invalid.append({
                        "plan_id": plan_id,
                        **_dynamic_ledger_error_detail(exc),
                    })
            producer_ready = not invalid
            funding_ready = bool(verified) and producer_ready
            status = (
                "INVALID" if invalid
                else "VERIFIED" if verified
                else "VERIFIED_EMPTY" if not identity_plans
                else "VERIFIED_PENDING"
            )
            contract = {
                **dict(zip(_DYNAMIC_LEDGER_IDENTITY_FIELDS, identity)),
                "plan_ids": [
                    str(item.get("plan_id") or "") for item in identity_plans
                ],
                "pending_plan_ids": pending,
                "verified_chain_hashes": sorted(
                    str(item.get("chain_hash") or "") for item in verified
                ),
                "invalid_plan_ids": sorted(
                    item["plan_id"] for item in invalid
                ),
            }
            result[identity] = {
                "schema": "probiga.dynamic-shadow-ledger-readiness.v1",
                "status": status,
                **dict(zip(_DYNAMIC_LEDGER_IDENTITY_FIELDS, identity)),
                "schema_readable": True,
                "shadow_trial_producer_ready": producer_ready,
                "funding_pipeline_ready": funding_ready,
                "verified_forward_evidence_ready": funding_ready,
                "plan_count": len(identity_plans),
                "pending_plan_count": len(pending),
                "verified_chain_count": len(verified),
                "invalid_chain_count": len(invalid),
                "invalid_chains": invalid,
                "ledger_hash": _digest(contract),
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }
        return result
    except Exception as exc:
        return {
            identity: _dynamic_ledger_failure(
                identity, exc, schema_readable=schema_readable,
            )
            for identity in normalized
        }


_LEDGER_READINESS_UNSET = object()


def _with_dynamic_ledger_status(
    base: Mapping[str, Any],
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    """Merge structural adapter health with one exact-version ledger state."""

    result = dict(base)
    ledger_status = str(readiness.get("status") or "UNAVAILABLE_OR_INVALID")
    funding_ready = bool(
        result.get("executable") is True
        and readiness.get("funding_pipeline_ready") is True
    )
    structure_ready = bool(
        result.get("executable") is True
        and readiness.get("schema_readable") is True
        and readiness.get("shadow_trial_producer_ready") is True
    )
    if ledger_status == "VERIFIED" and funding_ready:
        status_label = "执行适配器与模拟链成熟证据已就绪"
        funding_reason = "模拟链成熟证据已通过精确版本与底层事实复算"
        evidence_state = "MATURED"
    elif ledger_status == "VERIFIED_EMPTY" and structure_ready:
        status_label = "模拟链结构已就绪，证据积累中"
        funding_reason = "模拟链结构已就绪，当前版本尚无影子试验；证据积累中"
        evidence_state = "EMPTY_ACCUMULATING"
    elif ledger_status == "VERIFIED_PENDING" and structure_ready:
        status_label = "模拟链结构已就绪，证据积累中"
        funding_reason = "模拟链结构已就绪，当前版本影子试验尚未形成成熟闭环证据"
        evidence_state = "PENDING_MATURITY"
    else:
        status_label = "模拟链校验失败"
        invalid = readiness.get("invalid_chains")
        first_reason = ""
        if isinstance(invalid, list) and invalid and isinstance(invalid[0], Mapping):
            first_reason = str(invalid[0].get("reason") or "")
        funding_reason = "模拟链校验失败" + (
            "：" + first_reason if first_reason else ""
        )
        evidence_state = "INVALID"
    result.update({
        "status_label": status_label,
        "reason": (
            "执行适配器代码指纹、策略版本和成本模型绑定通过；"
            + funding_reason
            + "；真实下单始终关闭"
        ),
        "funding_pipeline_ready": funding_ready,
        "paper_chain_structure_ready": structure_ready,
        "shadow_trial_producer_ready": structure_ready,
        "funding_status": ledger_status,
        "funding_evidence_state": evidence_state,
        "funding_pipeline_reason": funding_reason,
        "funding_ledger_hash": str(readiness.get("ledger_hash") or ""),
        "verified_forward_evidence_ready": bool(
            funding_ready
            and readiness.get("verified_forward_evidence_ready") is True
        ),
        "verified_forward_chain_count": int(
            readiness.get("verified_chain_count") or 0
        ),
        "pending_shadow_plan_count": int(
            readiness.get("pending_plan_count") or 0
        ),
        "invalid_forward_chain_count": int(
            readiness.get("invalid_chain_count") or 0
        ),
        "real_order_submission_enabled": False,
        "automatic_real_order_submission": False,
    })
    return result


def strategy_execution_adapter_capabilities(
    *,
    registry_rows: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Expose code and batch-ledger capabilities without recursive scans.

    Governance callers that already own a registry snapshot pass it here, so
    rendering capabilities cannot trigger a second registry/ledger scan.
    Standalone API/startup callers omit it and load exactly one snapshot.
    """

    # Never hold the code registry lock across database work.  ``load_registry``
    # validates adapters from this immutable snapshot and performs one batched
    # ledger scan for every exact current runtime version.
    with _REGISTRY_LOCK:
        deployment_mode = _deployment_mode() or "development"
        registry_sealed = _REGISTRY_SEALED
        registry_seal_hash = _REGISTRY_SEAL_HASH
        registry_snapshot = dict(_REGISTRY)
    adapters = [
        {
            "adapter_key": adapter.adapter_key,
            "adapter_version": adapter.adapter_version,
            "artifact_sha256": adapter.artifact_sha256,
            "evaluator_types": sorted(adapter.evaluator_types),
            "status_label": (
                "已部署并显式封印"
                if registry_sealed
                else "已部署，等待显式封印"
            ),
            "real_order_submission_enabled": False,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        for _identity, adapter in sorted(registry_snapshot.items())
    ]
    registry_integrity_ready = bool(
        registry_sealed
        and _SHA256_PATTERN.fullmatch(registry_seal_hash)
    )
    adapter_configured = bool(adapters)
    candidate_execution_ready = bool(
        adapter_configured
        and (
            deployment_mode != "production"
            or registry_integrity_ready
        )
    )
    dynamic_versions: list[dict[str, Any]] = []
    capability_ledger_error = ""
    try:
        if registry_rows is None:
            from server.engine.strategy_governance import load_registry

            effective_registry_rows = load_registry()
        else:
            effective_registry_rows = [dict(row) for row in registry_rows]
        for row in effective_registry_rows:
            if str(row.get("source_kind") or "") != "runtime_registry":
                continue
            status = row.get("execution_adapter")
            status = status if isinstance(status, Mapping) else {}
            if status.get("executable") is not True:
                continue
            dynamic_versions.append({
                "strategy_key": str(row.get("strategy_key") or ""),
                "strategy_version": str(row.get("current_version") or ""),
                "strategy_version_hash": str(row.get("version_hash") or ""),
                "execution_binding_hash": str(
                    status.get("execution_binding_hash") or ""
                ),
                "funding_status": str(status.get("funding_status") or ""),
                "funding_evidence_state": str(
                    status.get("funding_evidence_state") or ""
                ),
                "paper_chain_structure_ready": (
                    status.get("paper_chain_structure_ready") is True
                ),
                "funding_pipeline_ready": (
                    status.get("funding_pipeline_ready") is True
                ),
                "funding_ledger_hash": str(
                    status.get("funding_ledger_hash") or ""
                ),
                "verified_forward_chain_count": int(
                    status.get("verified_forward_chain_count") or 0
                ),
                "pending_shadow_plan_count": int(
                    status.get("pending_shadow_plan_count") or 0
                ),
                "real_order_submission_enabled": False,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            })
    except Exception as exc:
        capability_ledger_error = type(exc).__name__
    structure_ready = bool(
        candidate_execution_ready
        and any(
            row["paper_chain_structure_ready"] is True
            for row in dynamic_versions
        )
    )
    funding_pipeline_ready = bool(
        candidate_execution_ready
        and any(
            row["funding_pipeline_ready"] is True
            for row in dynamic_versions
        )
    )
    governance_paper_execution_ready = bool(
        candidate_execution_ready and funding_pipeline_ready
    )
    return {
        "schema": "probiga.strategy-adapter-capabilities.v1",
        "deployment_mode": deployment_mode,
        "registry_sealed": registry_sealed,
        "registry_seal_hash": registry_seal_hash,
        "registry_integrity_ready": registry_integrity_ready,
        "adapter_configured": adapter_configured,
        "candidate_execution_ready": candidate_execution_ready,
        "adapter_count": len(adapters),
        "evaluator_types": sorted({
            evaluator
            for adapter in registry_snapshot.values()
            for evaluator in adapter.evaluator_types
        }),
        "adapters": adapters,
        "dynamic_version_count": len(dynamic_versions),
        "structure_ready_dynamic_version_count": sum(
            row["paper_chain_structure_ready"] is True
            for row in dynamic_versions
        ),
        "funding_ready_dynamic_version_count": sum(
            row["funding_pipeline_ready"] is True
            for row in dynamic_versions
        ),
        "accumulating_dynamic_version_count": sum(
            row["funding_evidence_state"] in {
                "EMPTY_ACCUMULATING", "PENDING_MATURITY",
            }
            for row in dynamic_versions
        ),
        "invalid_dynamic_version_count": sum(
            row["funding_evidence_state"] == "INVALID"
            for row in dynamic_versions
        ),
        "dynamic_version_readiness": dynamic_versions,
        "capability_ledger_error": capability_ledger_error,
        "paper_chain_structure_ready": structure_ready,
        "funding_pipeline_ready": funding_pipeline_ready,
        "governance_paper_execution_ready": governance_paper_execution_ready,
        "production_execution_ready": governance_paper_execution_ready,
        "real_order_submission_enabled": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def strategy_execution_adapter_status(
    strategy: Mapping[str, Any],
    *,
    ledger_readiness: Mapping[str, Any] | None | object = (
        _LEDGER_READINESS_UNSET
    ),
) -> dict[str, Any]:
    """Return structural and exact-ledger health for one registry version.

    ``ledger_readiness=None`` is the structure-only mode used by
    ``load_registry`` before its single batch scan.  Omitting the keyword keeps
    standalone callers compatible by resolving just this identity directly.
    """

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
            # Built-ins use the immutable manifest/V3 sleeve funding path; the
            # dynamic shadow ledger is deliberately not applicable to them.
            "funding_pipeline_ready": executable,
            "paper_chain_structure_ready": executable,
            "shadow_trial_producer_ready": False,
            "funding_status": "NOT_APPLICABLE_BUILTIN",
            "funding_evidence_state": "BUILTIN_VERSION_BOUND_PATH",
            "funding_pipeline_reason": (
                "内置策略使用既有版本绑定模拟执行与资金证据路径"
                if executable else reason
            ),
            "funding_ledger_hash": str(strategy.get("version_hash") or ""),
            "verified_forward_evidence_ready": executable,
            "verified_forward_chain_count": 0,
            "pending_shadow_plan_count": 0,
            "invalid_forward_chain_count": 0 if executable else 1,
            "real_order_submission_enabled": False,
            "automatic_real_order_submission": False,
        }

    config = strategy.get("evaluator_config")
    config = config if isinstance(config, dict) else {}
    binding_input = config.get("execution_adapter")
    try:
        binding = normalize_execution_binding(
            binding_input, strategy_version=strategy_version,
        )
    except ValueError as exc:
        undeployed = not isinstance(binding_input, Mapping)
        label = "执行适配器未部署" if undeployed else "执行适配器校验失败"
        return {
            "executable": False,
            "status": "UNDEPLOYED" if undeployed else "INVALID",
            "status_label": label,
            "reason": label + "：" + str(exc),
            "adapter_key": "",
            "adapter_version": "",
            "artifact_sha256": "",
            "cost_model_hash": "",
            "execution_binding_hash": "",
            "strategy_version": strategy_version,
            "candidate_builder_deployed": False,
            "funding_pipeline_ready": False,
            "paper_chain_structure_ready": False,
            "shadow_trial_producer_ready": False,
            "funding_status": (
                "NOT_APPLICABLE_UNDEPLOYED_ADAPTER"
                if undeployed else "NOT_APPLICABLE_INVALID_ADAPTER"
            ),
            "funding_evidence_state": (
                "ADAPTER_UNAVAILABLE" if undeployed else "ADAPTER_INVALID"
            ),
            "funding_pipeline_reason": label,
            "funding_ledger_hash": "",
            "verified_forward_evidence_ready": False,
            "verified_forward_chain_count": 0,
            "pending_shadow_plan_count": 0,
            "invalid_forward_chain_count": 0,
            "real_order_submission_enabled": False,
            "automatic_real_order_submission": False,
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
    if adapter is None:
        blocked_status = "UNDEPLOYED"
        blocked_label = "执行适配器未部署"
    elif not enabled or lifecycle in {"RETIRED", "SUSPENDED"}:
        blocked_status = "LIFECYCLE_BLOCKED"
        blocked_label = "策略当前不允许执行"
    else:
        blocked_status = "INVALID"
        blocked_label = "执行适配器校验失败"
    base = {
        "executable": executable,
        # Preserve the public machine enum; detailed ledger state is carried by
        # funding_status/funding_evidence_state rather than overloading it.
        "status": "RESEARCH_READY" if executable else blocked_status,
        "status_label": (
            "执行适配器结构已就绪" if executable else blocked_label
        ),
        "reason": (
            "执行适配器代码指纹、策略版本和成本模型绑定通过"
            if executable else blocked_label + "：" + "；".join(reasons)
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
        "funding_pipeline_ready": False,
        "paper_chain_structure_ready": False,
        "shadow_trial_producer_ready": False,
        "funding_status": (
            "STRUCTURE_NOT_EVALUATED" if executable
            else (
                "NOT_APPLICABLE_UNDEPLOYED_ADAPTER"
                if blocked_status == "UNDEPLOYED"
                else "NOT_APPLICABLE_BLOCKED_ADAPTER"
            )
        ),
        "funding_evidence_state": (
            "STRUCTURE_NOT_EVALUATED" if executable
            else (
                "ADAPTER_UNAVAILABLE"
                if blocked_status == "UNDEPLOYED"
                else "ADAPTER_BLOCKED"
            )
        ),
        "funding_pipeline_reason": (
            "模拟链结构等待批量校验" if executable else blocked_label
        ),
        "funding_ledger_hash": "",
        "verified_forward_evidence_ready": False,
        "verified_forward_chain_count": 0,
        "pending_shadow_plan_count": 0,
        "invalid_forward_chain_count": 0,
        "registry_sealed": _REGISTRY_SEALED,
        "registry_seal_hash": _REGISTRY_SEAL_HASH,
        "production_seal_required": _deployment_mode() == "production",
        "real_order_submission_enabled": False,
        "automatic_real_order_submission": False,
    }
    if not executable or ledger_readiness is None:
        return base
    if ledger_readiness is _LEDGER_READINESS_UNSET:
        from server.engine.dynamic_shadow_ledger import (
            dynamic_shadow_ledger_readiness,
        )

        resolved_readiness: Mapping[str, Any] = dynamic_shadow_ledger_readiness(
            strategy_key=strategy_key,
            strategy_version=strategy_version,
            strategy_version_hash=str(strategy.get("version_hash") or ""),
            execution_binding_hash=str(
                binding.get("execution_binding_hash") or ""
            ),
        )
    elif isinstance(ledger_readiness, Mapping):
        resolved_readiness = ledger_readiness
    else:
        raise TypeError("ledger_readiness必须是映射、None或省略")
    expected_identity = _dynamic_ledger_identity(
        strategy_key=strategy_key,
        strategy_version=strategy_version,
        strategy_version_hash=str(strategy.get("version_hash") or ""),
        execution_binding_hash=binding.get("execution_binding_hash"),
    )
    observed_identity = _dynamic_ledger_identity(
        strategy_key=resolved_readiness.get("strategy_key"),
        strategy_version=resolved_readiness.get("strategy_version"),
        strategy_version_hash=resolved_readiness.get("strategy_version_hash"),
        execution_binding_hash=resolved_readiness.get("execution_binding_hash"),
    )
    if observed_identity != expected_identity:
        resolved_readiness = _dynamic_ledger_failure(
            expected_identity,
            ValueError("批量模拟链就绪度越出精确策略版本边界"),
            schema_readable=False,
        )
    return _with_dynamic_ledger_status(base, resolved_readiness)


def execute_dynamic_adapter_candidate_batch(
    strategy: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    adapter_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run an adapter and independently verify its batch and run receipt."""

    _seal_production_registry()
    if strategy.get("enabled") is not True:
        raise ValueError("策略已禁用，禁止执行动态适配器")
    if str(strategy.get("current_status") or "") == "RETIRED":
        raise ValueError("策略已淘汰，禁止执行动态适配器")
    if adapter_status is None:
        status = strategy_execution_adapter_status(strategy)
    else:
        # ``load_registry`` has already resolved all current runtime identities
        # with one batch ledger scan. Recheck code/version structure without a
        # database read, then require the supplied status to be that identity.
        status = strategy_execution_adapter_status(
            strategy, ledger_readiness=None,
        )
        identity_fields = (
            "adapter_key", "adapter_version", "artifact_sha256",
            "cost_model_hash", "execution_binding_hash", "strategy_version",
        )
        if (
            adapter_status.get("executable") is not True
            or any(
                str(adapter_status.get(field) or "")
                != str(status.get(field) or "")
                for field in identity_fields
            )
        ):
            raise ValueError("批量执行适配器状态与精确策略版本身份不一致")
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
    return {
        "signals": result,
        "receipt": receipt,
        # Exact pre-enrichment rows are persisted separately so a later paper
        # trial can replay the receipt output_hash instead of trusting a
        # caller-provided signal with the same stock code.
        "candidate_facts": [copy.deepcopy(dict(row)) for row in raw_rows],
    }


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
    intent: Mapping[str, Any] | None = None,
    order: Mapping[str, Any] | None = None,
    fill: Mapping[str, Any] | None = None,
    forward_evidence: Mapping[str, Any] | None = None,
    connection: Any | None = None,
    plan_id: str = "",
) -> dict[str, Any]:
    """Verify a candidate→paper fill→forward-evidence chain exactly.

    This contract is deliberately independent from capital eligibility.  Only
    the persistent candidate/plan/V2/V3 ledger path can prove a completed
    chain; caller-provided dictionaries never grant funding readiness.
    """

    normalized_receipt = validate_strategy_adapter_run_receipt(
        candidate_receipt
    )
    if connection is not None and str(plan_id or ""):
        from server.engine.dynamic_shadow_ledger import (
            verify_dynamic_shadow_trial,
        )

        verified = verify_dynamic_shadow_trial(connection, str(plan_id))
        if (
            str(verified.get("strategy_key") or "")
            != str(strategy.get("strategy_key") or "")
            or str(verified.get("strategy_version") or "")
            != str(strategy.get("current_version") or "")
            or str(verified.get("candidate_run_uid") or "")
            != str(normalized_receipt.get("run_uid") or "")
            or str(verified.get("candidate_receipt_hash") or "")
            != str(normalized_receipt.get("receipt_hash") or "")
        ):
            raise ValueError("动态影子资金链与策略或候选回执身份不一致")
        return verified
    # A caller-provided hash chain is not an authoritative ledger.  The
    # dictionaries are retained only for API compatibility and diagnostics;
    # funding proof must be replayed from the FK-bound V2/V3 database facts.
    raise RuntimeError(
        "调用方自造资金链不是可信事实；必须从持久化FK绑定的"
        "V2/V3模拟账本复算；"
        "调用方自造intent/order/fill/evidence哈希链不能取得资金资格"
    )


__all__ = [
    "CandidateBatch",
    "StrategyExecutionAdapter",
    "batch_dynamic_shadow_ledger_readiness",
    "batch_verify_all_strategy_adapter_candidate_facts",
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

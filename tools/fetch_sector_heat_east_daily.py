#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同步东方财富行业板块热度到 st_hot_concept_ths_daily。

plate_type 说明：
  3 = 东财一级行业（由二级行业按 EAST_INDUSTRY_MAP 聚合）
  4 = 东财二级行业（按 EAST_INDUSTRY_MAP 固定表头聚合后的二级行业）
"""

import argparse
import ast
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, time as datetime_time
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from tools.env_config import create_tool_engine, resolve_tool_mysql_url
from server.common.authoritative_market_clock import authoritative_closed_trade_date

CACHE_FILE = ROOT / "runtime" / "cache" / "east_sector_heat_cache.json"
FORMAL_RESULT_SCHEMA = "probiga.sector-heat-east-result.v1"
FORMAL_SOURCE = "eastmoney.push2.industry"
SECTOR_HEAT_CLOSE_READY_TIME = datetime_time(15, 10)

from server.common.process_env import build_child_env


class SectorHeatContractError(RuntimeError):
    """Raised before publication when the exact sector snapshot is incomplete."""


def _canonical_hash(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _canonical_number(value: object) -> str | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SectorHeatContractError(f"invalid numeric sector value: {value!r}") from exc
    if not math.isfinite(number):
        raise SectorHeatContractError(f"non-finite sector value: {value!r}")
    return f"{number:.6f}"


def canonical_sector_rows(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return the stable business representation used by the formal receipt."""

    canonical = [
        {
            "snapshot_date": str(row.get("snapshot_date") or "")[:10],
            "plate_type": int(row.get("plate_type") or 0),
            "rank": int(row.get("rank") or 0),
            "concept_code": str(row.get("concept_code") or "").strip(),
            "concept_name": str(row.get("concept_name") or "").strip(),
            "change_pct": _canonical_number(row.get("change_pct")),
            "hot_value": _canonical_number(row.get("hot_value")),
            "hot_tag": str(row.get("hot_tag") or "").strip(),
        }
        for row in rows
    ]
    return sorted(
        canonical,
        key=lambda row: (
            row["plate_type"],
            row["rank"],
            row["concept_code"],
            row["concept_name"],
        ),
    )


def sector_heat_row_hash(rows: list[Mapping[str, Any]]) -> str:
    return _canonical_hash(canonical_sector_rows(rows))


def _with_receipt_id(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["receipt_id"] = _canonical_hash(result)
    return result

EASTMONEY_CLIST_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
EASTMONEY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://data.eastmoney.com/",
}


def _warn(message: str, exc: Exception) -> None:
    print(f"[WARN] {message}: {exc}", file=sys.stderr)


def _node_binary_candidates() -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(path: str | os.PathLike[str] | None) -> None:
        if not path:
            return
        value = str(path).strip()
        if not value:
            return
        try:
            resolved = str(Path(value).expanduser())
        except Exception:
            resolved = value
        key = os.path.normcase(os.path.abspath(resolved))
        if key in seen or not os.path.exists(resolved):
            return
        seen.add(key)
        candidates.append(resolved)

    add(os.environ.get("NODE_BINARY", "").strip())
    add(os.environ.get("PROBIGA_NODE_BINARY", "").strip())
    add(shutil.which("node"))
    add(shutil.which("node.exe"))

    for env_name in ("CODEX_PRIMARY_RUNTIME", "CODEX_RUNTIME_DIR"):
        root = os.environ.get(env_name, "").strip()
        if root:
            add(Path(root) / "dependencies" / "node" / "bin" / "node")
            add(Path(root) / "dependencies" / "node" / "bin" / "node.exe")

    cache_root = Path.home() / ".cache" / "codex-runtimes"
    if cache_root.is_dir():
        for pattern in (
            "codex-primary-runtime/dependencies/node/bin/node",
            "codex-primary-runtime/dependencies/node/bin/node.exe",
            "*/dependencies/node/bin/node",
            "*/dependencies/node/bin/node.exe",
        ):
            for path in cache_root.glob(pattern):
                add(path)

    return candidates


def _fetch_json(url: str, timeout: int = 30, retries: int = 5) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers=EASTMONEY_HEADERS, method="GET")
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8", errors="replace"))
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.2 * attempt)

    for node_exe in _node_binary_candidates():
        node_script = r"""
const url = process.env.EASTMONEY_URL;
const timeoutMs = Number(process.env.EASTMONEY_TIMEOUT_MS || 30000);
const transport = require(url.startsWith('https:') ? 'https' : 'http');
const req = transport.get(url, {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://data.eastmoney.com/'
  }
}, (resp) => {
  let body = '';
  resp.setEncoding('utf8');
  resp.on('data', (chunk) => { body += chunk; });
  resp.on('error', fail);
  resp.on('end', () => {
    clearTimeout(timer);
    if (resp.statusCode < 200 || resp.statusCode >= 300) {
      console.error(`HTTP ${resp.statusCode}: ${body.slice(0, 500)}`);
      process.exit(2);
    }
    process.stdout.write(body);
  });
});
const timer = setTimeout(() => req.destroy(new Error('request deadline exceeded')), timeoutMs);
function fail(err) {
  clearTimeout(timer);
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
}
req.on('error', fail);
"""
        env = build_child_env(ROOT)
        env["EASTMONEY_URL"] = url
        env["EASTMONEY_TIMEOUT_MS"] = str(timeout * 1000)
        for attempt in range(1, retries + 1):
            try:
                proc = subprocess.run(
                    [node_exe, "-e", node_script],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=env,
                    cwd=str(ROOT),
                    timeout=timeout + 10,
                )
                if proc.returncode == 0 and proc.stdout.strip():
                    return json.loads(proc.stdout)
                last_error = RuntimeError((proc.stderr or proc.stdout or "node fetch failed").strip())
            except Exception as exc:
                last_error = exc
            if attempt < retries:
                time.sleep(1.2 * attempt)

    raise RuntimeError(f"东财请求失败: {last_error}")


def _cache_rows(db_rows: list[dict], snapshot_date: str, requested_date: str) -> None:
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "snapshot_date": snapshot_date,
            "requested_date": requested_date,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "db_rows": [
                {
                    **row,
                    "etl_sync_at": row["etl_sync_at"].isoformat(timespec="seconds")
                    if isinstance(row.get("etl_sync_at"), datetime)
                    else row.get("etl_sync_at"),
                }
                for row in db_rows
            ],
        }
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        _warn("failed to save east sector heat cache", exc)


def _load_cached_rows() -> dict | None:
    if not CACHE_FILE.is_file():
        return None
    try:
        payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        db_rows = []
        for row in payload.get("db_rows") or []:
            item = dict(row)
            etl_sync_at = item.get("etl_sync_at")
            if isinstance(etl_sync_at, str) and etl_sync_at:
                try:
                    item["etl_sync_at"] = datetime.fromisoformat(etl_sync_at)
                except ValueError:
                    item["etl_sync_at"] = datetime.now().replace(microsecond=0)
            db_rows.append(item)
        if not db_rows:
            return None
        return {
            "snapshot_date": str(payload.get("snapshot_date") or ""),
            "requested_date": str(payload.get("requested_date") or ""),
            "db_rows": db_rows,
        }
    except Exception as exc:
        _warn("failed to load east sector heat cache", exc)
        return None


def _industry_map() -> dict[str, list[str]]:
    try:
        source = (ROOT / "server" / "api" / "routers" / "hot_data.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.Assign):
                if any(isinstance(t, ast.Name) and t.id == "EAST_INDUSTRY_MAP" for t in node.targets):
                    value = ast.literal_eval(node.value)
                    return value if isinstance(value, dict) else {}
    except Exception as exc:
        _warn("failed to load EAST_INDUSTRY_MAP", exc)
        return {}
    return {}


def _to_float(value, default=None):
    if value in (None, "", "-"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _norm_name(name: str) -> str:
    s = str(name or "").strip()
    for suffix in ("行业", "板块"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    for token in ("Ⅰ", "Ⅱ", "Ⅲ", "II", "III", "（", "）", "(", ")", " ", "\t"):
        s = s.replace(token, "")
    return s.upper()


def _stable_l1_code(name: str) -> str:
    digest = hashlib.md5(name.encode("utf-8")).hexdigest()[:10]
    return f"EML1_{digest}"


def _stable_l2_code(name: str) -> str:
    digest = hashlib.md5(name.encode("utf-8")).hexdigest()[:10]
    return f"EML2_{digest}"


def _east_level_suffix(name: str) -> int:
    s = str(name or "").strip().upper()
    if s.endswith(("Ⅰ", "I")):
        return 1
    if s.endswith(("Ⅱ", "II")):
        return 2
    if s.endswith(("Ⅲ", "III")):
        return 3
    return 0


def _direct_row_score(row: dict, fixed_name: str, level: int) -> tuple[int, float]:
    raw_name = str(row.get("concept_name") or "").strip()
    suffix = _east_level_suffix(raw_name)
    hot_value = float(row.get("hot_value") or 0.0)
    if level == 1:
        if raw_name == fixed_name:
            return (5, hot_value)
        if suffix == 1:
            return (4, hot_value)
        if suffix == 0:
            return (3, hot_value)
        return (1, hot_value)

    if suffix == 2:
        return (5, hot_value)
    if raw_name == fixed_name:
        return (4, hot_value)
    if suffix == 0:
        return (3, hot_value)
    if suffix == 3:
        return (2, hot_value)
    return (1, hot_value)


def _pick_direct_row(rows_by_norm: dict[str, list[dict]], fixed_name: str, level: int) -> dict | None:
    candidates = rows_by_norm.get(_norm_name(fixed_name), [])
    if not candidates:
        return None
    return max(candidates, key=lambda row: _direct_row_score(row, fixed_name, level))


def _weighted_change(rows: list[dict]) -> float | None:
    total_weight = 0.0
    weighted = 0.0
    for row in rows:
        change_pct = row.get("change_pct")
        if change_pct is None:
            continue
        weight = max(float(row.get("hot_value") or 0.0), 1.0)
        weighted += float(change_pct) * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return round(weighted / total_weight, 4)


def _aggregate_rows(fixed_name: str, rows: list[dict], code_prefix: str, hot_tag: str) -> dict:
    best = max(rows, key=lambda row: float(row.get("hot_value") or 0.0)) if rows else {}
    hot_value = sum(float(row.get("hot_value") or 0.0) for row in rows)
    code = str(best.get("concept_code") or "").strip()[:32]
    if not code:
        code = _stable_l1_code(fixed_name) if code_prefix == "EML1" else _stable_l2_code(fixed_name)
    return {
        "rank": 0,
        "concept_code": code,
        "concept_name": fixed_name[:64],
        "change_pct": _weighted_change(rows),
        "hot_value": round(hot_value, 4),
        "hot_tag": hot_tag,
    }


def _row_from_direct(fixed_name: str, row: dict, hot_tag: str) -> dict:
    item = dict(row)
    item["rank"] = 0
    item["concept_name"] = fixed_name[:64]
    item["hot_tag"] = hot_tag
    return item


def _rerank_by_hot_value(rows: list[dict]) -> list[dict]:
    rows.sort(key=lambda r: float(r.get("hot_value") or 0.0), reverse=True)
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def _build_fixed_industry_rows(raw_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    mapping = _industry_map()
    if not mapping:
        return _aggregate_primary_fallback(raw_rows), raw_rows

    rows_by_norm: dict[str, list[dict]] = {}
    for row in raw_rows:
        name = str(row.get("concept_name") or "").strip()
        if not name:
            continue
        rows_by_norm.setdefault(_norm_name(name), []).append(row)

    rows_l2: list[dict] = []
    rows_l2_by_parent: dict[str, list[dict]] = {}
    for primary, children in mapping.items():
        rows_l2_by_parent[primary] = []
        for child in children:
            direct = _pick_direct_row(rows_by_norm, child, 2)
            if direct:
                row = _row_from_direct(child, direct, "东财二级行业成交额")
            else:
                child_norm = _norm_name(child)
                fallback = [
                    raw
                    for raw in raw_rows
                    if child_norm and child_norm in _norm_name(str(raw.get("concept_name") or ""))
                ]
                if fallback:
                    row = _aggregate_rows(child, fallback, "EML2", f"东财二级行业成交额/{len(fallback)}条明细")
                else:
                    row = {
                        "rank": 0,
                        "concept_code": _stable_l2_code(child),
                        "concept_name": child[:64],
                        "change_pct": None,
                        "hot_value": 0.0,
                        "hot_tag": "东财二级行业无匹配明细",
                    }
            rows_l2.append(row)
            rows_l2_by_parent[primary].append(row)

    rows_l1: list[dict] = []
    for primary, children in mapping.items():
        direct = _pick_direct_row(rows_by_norm, primary, 1)
        if direct:
            row = _row_from_direct(primary, direct, "东财一级行业成交额")
        else:
            row = _aggregate_rows(primary, rows_l2_by_parent.get(primary, []), "EML1", f"东财一级行业成交额/{len(children)}个二级")
        rows_l1.append(row)

    return _rerank_by_hot_value(rows_l1), _rerank_by_hot_value(rows_l2)


def _match_primary(name: str, mapping: dict[str, list[str]]) -> str:
    if not mapping:
        return name

    norm = _norm_name(name)
    exact = {}
    for primary, children in mapping.items():
        exact.setdefault(_norm_name(primary), primary)
        for child in children:
            exact.setdefault(_norm_name(child), primary)

    if norm in exact:
        return exact[norm]

    for primary, children in mapping.items():
        for child in children:
            child_norm = _norm_name(child)
            if child_norm and (child_norm in norm or norm in child_norm):
                return primary

    return name


def _fetch_eastmoney_industries() -> list[dict]:
    rows: list[dict] = []
    seen_codes: set[str] = set()
    page = 1
    page_size = 100

    while page <= 20:
        params = {
            "pn": page,
            "pz": page_size,
            "po": "1",
            "np": "1",
            "fltt": "2",
            "invt": "2",
            "fid": "f6",
            "fs": "m:90+t:2",
            "fields": "f12,f14,f3,f6,f62,f124",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        }
        url = EASTMONEY_CLIST_URL + "?" + urlencode(params)
        payload = _fetch_json(url)
        data = (payload.get("data") or {})
        diff = data.get("diff") or []
        if not diff:
            break

        for item in diff:
            code = str(item.get("f12") or "").strip()
            name = str(item.get("f14") or "").strip()
            if not code or not name or code in seen_codes:
                continue
            amount = _to_float(item.get("f6"))
            if amount is None or amount <= 0:
                continue
            seen_codes.add(code)
            trade_date_hint = ""
            ts = _to_float(item.get("f124"))
            if ts and ts > 0:
                try:
                    trade_date_hint = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d")
                except (OSError, OverflowError, ValueError):
                    trade_date_hint = ""
            rows.append(
                {
                    "rank": len(rows) + 1,
                    "concept_code": code[:32],
                    "concept_name": name[:64],
                    "change_pct": _to_float(item.get("f3")),
                    "hot_value": float(amount),
                    "hot_tag": "东财成交额",
                    "_trade_date_hint": trade_date_hint,
                }
            )

        if len(diff) < page_size:
            break
        page += 1

    rows.sort(key=lambda r: float(r.get("hot_value") or 0), reverse=True)
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def _fetch_industry_kline(code: str, end_date: str, limit: int = 120) -> list[dict]:
    params = {
        "secid": f"90.{code}",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "end": end_date.replace("-", ""),
        "lmt": str(limit),
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    }
    url = EASTMONEY_KLINE_URL + "?" + urlencode(params)
    payload = _fetch_json(url)

    klines = ((payload.get("data") or {}).get("klines") or [])
    rows = []
    for line in klines:
        parts = str(line).split(",")
        if len(parts) < 9:
            continue
        rows.append(
            {
                "trade_date": parts[0],
                "close": _to_float(parts[2]),
                "volume": _to_float(parts[5], 0.0) or 0.0,
                "amount": _to_float(parts[6], 0.0) or 0.0,
                "change_pct": _to_float(parts[8]),
            }
        )
    return rows


def _latest_east_trade_date(rows_l2: list[dict], end_date: str | None = None) -> str:
    if not rows_l2:
        return end_date or datetime.now().strftime("%Y-%m-%d")
    target = end_date or datetime.now().strftime("%Y-%m-%d")
    try:
        klines = _fetch_industry_kline(str(rows_l2[0]["concept_code"]), target, limit=10)
        dates = [r["trade_date"] for r in klines if r.get("trade_date") and r["trade_date"] <= target]
        return dates[-1] if dates else target
    except Exception as exc:
        _warn("failed to resolve latest east trade date", exc)
        return target


def _apply_historical_values(rows_l2: list[dict], snapshot_date: str) -> list[dict]:
    updated = []
    failures: list[tuple[str, Exception]] = []
    for row in rows_l2:
        concept_code = str(row["concept_code"])
        try:
            klines = _fetch_industry_kline(concept_code, snapshot_date)
        except Exception as exc:
            failures.append((concept_code, exc))
            klines = []
        matched = None
        for kline in reversed(klines):
            if kline.get("trade_date") and kline["trade_date"] <= snapshot_date:
                matched = kline
                break
        if not matched:
            continue
        item = dict(row)
        item["change_pct"] = matched.get("change_pct")
        item["hot_value"] = float(matched.get("amount") or matched.get("volume") or row.get("hot_value") or 0)
        updated.append(item)

    if failures:
        sample = "; ".join(f"{code}: {exc}" for code, exc in failures[:3])
        if len(failures) > 3:
            sample = f"{sample}; ... +{len(failures) - 3} more"
        _warn(f"failed to fetch historical industry kline for {len(failures)} rows", RuntimeError(sample))

    updated.sort(key=lambda r: float(r.get("hot_value") or 0), reverse=True)
    for idx, row in enumerate(updated, start=1):
        row["rank"] = idx
    return updated


def _aggregate_primary_fallback(rows_l2: list[dict]) -> list[dict]:
    mapping = _industry_map()
    grouped: dict[str, dict] = {}

    for row in rows_l2:
        primary = _match_primary(str(row["concept_name"]), mapping)
        hot_value = float(row.get("hot_value") or 0.0)
        weight = max(hot_value, 1.0)
        bucket = grouped.setdefault(
            primary,
            {
                "hot_value": 0.0,
                "weighted_change": 0.0,
                "weight": 0.0,
                "children": 0,
            },
        )
        bucket["hot_value"] += hot_value
        bucket["children"] += 1
        if row.get("change_pct") is not None:
            bucket["weighted_change"] += float(row["change_pct"]) * weight
            bucket["weight"] += weight

    rows_l1 = []
    for rank, (name, bucket) in enumerate(
        sorted(grouped.items(), key=lambda item: item[1]["hot_value"], reverse=True),
        start=1,
    ):
        change_pct = None
        if bucket["weight"] > 0:
            change_pct = round(bucket["weighted_change"] / bucket["weight"], 4)
        rows_l1.append(
            {
                "rank": rank,
                "concept_code": _stable_l1_code(name),
                "concept_name": name[:64],
                "change_pct": change_pct,
                "hot_value": round(bucket["hot_value"], 4),
                "hot_tag": f"东财一级行业/{bucket['children']}个二级",
            }
        )
    return rows_l1


def _build_primary_rows(rows_l2: list[dict]) -> list[dict]:
    mapping = _industry_map()
    by_name = {_norm_name(str(row.get("concept_name") or "")): row for row in rows_l2}
    rows_l1: list[dict] = []

    for primary in mapping:
        direct = by_name.get(_norm_name(primary))
        if not direct:
            continue
        row = dict(direct)
        row["concept_name"] = primary[:64]
        row["hot_tag"] = "东财一级行业成交额"
        rows_l1.append(row)

    if not rows_l1:
        return _aggregate_primary_fallback(rows_l2)

    rows_l1.sort(key=lambda r: float(r.get("hot_value") or 0), reverse=True)
    for idx, row in enumerate(rows_l1, start=1):
        row["rank"] = idx
    return rows_l1


def validate_formal_sector_rows(
    rows: list[Mapping[str, Any]],
    *,
    target_date: str,
    raw_count: int,
) -> dict[str, Any]:
    """Require the complete fixed Eastmoney L1/L2 industry inventory."""

    mapping = _industry_map()
    if not mapping:
        raise SectorHeatContractError("EAST_INDUSTRY_MAP is empty")
    expected_names = {
        3: set(mapping),
        4: {child for children in mapping.values() for child in children},
    }
    if raw_count < len(expected_names[4]):
        raise SectorHeatContractError(
            "Eastmoney raw industry inventory is incomplete: "
            f"raw={raw_count} expected_at_least={len(expected_names[4])}"
        )

    canonical = canonical_sector_rows(rows)
    if not canonical:
        raise SectorHeatContractError("formal sector snapshot is empty")
    if any(row["snapshot_date"] != target_date for row in canonical):
        observed = sorted({row["snapshot_date"] for row in canonical})
        raise SectorHeatContractError(
            f"formal sector target date differs: expected={target_date} observed={observed}"
        )

    counts: dict[str, int] = {}
    for plate_type in (3, 4):
        plate_rows = [row for row in canonical if row["plate_type"] == plate_type]
        expected = expected_names[plate_type]
        names = {row["concept_name"] for row in plate_rows}
        codes = {row["concept_code"] for row in plate_rows}
        ranks = {row["rank"] for row in plate_rows}
        if len(plate_rows) != len(expected) or names != expected:
            raise SectorHeatContractError(
                f"plate_type={plate_type} fixed inventory differs: "
                f"expected={len(expected)} actual={len(plate_rows)} "
                f"missing={sorted(expected - names)[:10]} "
                f"unexpected={sorted(names - expected)[:10]}"
            )
        if "" in codes or len(codes) != len(expected):
            raise SectorHeatContractError(
                f"plate_type={plate_type} code inventory is empty or duplicated"
            )
        if ranks != set(range(1, len(expected) + 1)):
            raise SectorHeatContractError(
                f"plate_type={plate_type} rank inventory is incomplete"
            )
        for row in plate_rows:
            hot_value = float(row["hot_value"] or 0.0)
            if (
                hot_value <= 0
                or row["change_pct"] is None
                or not row["hot_tag"]
                or "无匹配" in row["hot_tag"]
            ):
                raise SectorHeatContractError(
                    "formal sector row lacks provider evidence: "
                    f"plate_type={plate_type} concept={row['concept_name']}"
                )
        counts[f"plate_type_{plate_type}"] = len(plate_rows)

    if len(canonical) != sum(counts.values()):
        bad_types = sorted({row["plate_type"] for row in canonical} - {3, 4})
        raise SectorHeatContractError(f"unexpected sector plate types: {bad_types}")
    return {
        "raw_count": raw_count,
        "expected_l1_count": len(expected_names[3]),
        "expected_l2_count": len(expected_names[4]),
        "l1_count": counts["plate_type_3"],
        "l2_count": counts["plate_type_4"],
        "coverage": 1.0,
        "row_count": len(canonical),
        "row_hash": _canonical_hash(canonical),
    }


def _select_sector_rows(connection: Any, target_date: str) -> list[dict[str, Any]]:
    from sqlalchemy import text

    return [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT snapshot_date, plate_type, `rank`, concept_code, "
                "concept_name, change_pct, hot_value, hot_tag "
                "FROM st_hot_concept_ths_daily "
                "WHERE snapshot_date=:snapshot_date AND plate_type IN (3,4) "
                "ORDER BY plate_type, `rank`, concept_code"
            ),
            {"snapshot_date": target_date},
        ).mappings()
    ]


def _readback_sector_rows(engine: Any, target_date: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return _select_sector_rows(connection, target_date)


def resolve_formal_sector_target_date(engine: Any, *, now: datetime | None = None) -> str:
    """Resolve the latest exchange session closed for this provider at 15:10."""

    target = str(
        authoritative_closed_trade_date(
            engine,
            now=now,
            close_ready_time=SECTOR_HEAT_CLOSE_READY_TIME,
        )
        or ""
    )[:10]
    try:
        parsed = datetime.strptime(target, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SectorHeatContractError(
            f"cannot resolve formal sector target date from si_trade_calendar: {target!r}"
        ) from exc
    if parsed.isoformat() != target:
        raise SectorHeatContractError(
            f"non-canonical formal sector target date from si_trade_calendar: {target!r}"
        )
    return target


def _formal_sector_receipt(
    *,
    status: str,
    requested_date: str,
    data_date: str,
    started_at: datetime,
    finished_at: datetime,
    published: bool,
    evidence: Mapping[str, Any] | None = None,
    error: BaseException | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": FORMAL_RESULT_SCHEMA,
        "status": status,
        "source": FORMAL_SOURCE,
        "requested_date": requested_date,
        "data_date": data_date,
        "published": bool(published),
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "evidence": dict(evidence or {}),
    }
    if error is not None:
        payload["error_type"] = type(error).__name__
        payload["error"] = str(error)[:1000]
    return _with_receipt_id(payload)


def _progress(message: str, *, formal: bool) -> None:
    print(message, file=sys.stderr if formal else sys.stdout, flush=True)


def fetch_sector_heat_east_daily(
    snapshot_date: str,
    dry_run: bool = False,
    *,
    formal: bool = False,
    diagnostic_cache: bool = False,
    engine: Any | None = None,
    now: datetime | None = None,
) -> dict:
    started_at = (now or datetime.now()).replace(microsecond=0)
    requested_date = snapshot_date
    raw_count = 0
    if diagnostic_cache and (formal or not dry_run):
        raise SectorHeatContractError(
            "diagnostic cache is allowed only with --dry-run and without --formal"
        )
    try:
        parsed_target = datetime.strptime(requested_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SectorHeatContractError(
            f"invalid requested sector date: {requested_date!r}"
        ) from exc
    if parsed_target.isoformat() != requested_date:
        raise SectorHeatContractError(f"non-canonical requested sector date: {requested_date!r}")

    _progress(f"开始同步东财板块热度，请求日期: {requested_date}", formal=formal)
    try:
        raw_rows = _fetch_eastmoney_industries()
        raw_count = len(raw_rows)
        if not raw_rows:
            raise RuntimeError("未获取到东财行业板块数据")

        current_date_hints = sorted({r.get("_trade_date_hint") for r in raw_rows if r.get("_trade_date_hint")})
        current_snapshot_date = current_date_hints[-1] if current_date_hints else _latest_east_trade_date(raw_rows)
        if requested_date >= current_snapshot_date:
            snapshot_date = current_snapshot_date
        else:
            snapshot_date = _latest_east_trade_date(raw_rows, requested_date)

        if snapshot_date < current_snapshot_date:
            historical_rows = _apply_historical_values(raw_rows, snapshot_date)
            min_required = max(20, int(len(raw_rows) * 0.6))
            if len(historical_rows) < min_required:
                raise RuntimeError(f"历史日期 {snapshot_date} 仅补到 {len(historical_rows)} 条，低于最低要求 {min_required}")
            raw_rows = historical_rows

        rows_l1, rows_l2 = _build_fixed_industry_rows(raw_rows)
        captured_at = (now or datetime.now()).replace(microsecond=0)
        db_rows = []
        for plate_type, rows in ((3, rows_l1), (4, rows_l2)):
            for row in rows:
                db_rows.append(
                    {
                        "snapshot_date": snapshot_date,
                        "plate_type": plate_type,
                        "rank": row["rank"],
                        "concept_code": row["concept_code"],
                        "concept_name": row["concept_name"],
                        "change_pct": row["change_pct"],
                        "hot_value": row["hot_value"],
                        "hot_tag": row["hot_tag"],
                        "etl_sync_at": captured_at,
                    }
                )
    except Exception as exc:
        cached = _load_cached_rows() if diagnostic_cache else None
        if not cached:
            raise SectorHeatContractError(
                f"Eastmoney sector fetch failed without admissible cache: {exc}"
            ) from exc
        db_rows = cached["db_rows"]
        snapshot_date = cached["snapshot_date"] or requested_date
        rows_l1 = [r for r in db_rows if int(r.get("plate_type") or 0) == 3]
        rows_l2 = [r for r in db_rows if int(r.get("plate_type") or 0) == 4]
        evidence = {
            "cache_snapshot_date": snapshot_date,
            "row_count": len(db_rows),
            "row_hash": sector_heat_row_hash(db_rows),
        }
        return _formal_sector_receipt(
            status="DIAGNOSTIC_CACHE",
            requested_date=requested_date,
            data_date=snapshot_date,
            started_at=started_at,
            finished_at=(now or datetime.now()).replace(microsecond=0),
            published=False,
            evidence=evidence,
            error=exc,
        )

    if formal and snapshot_date != requested_date:
        raise SectorHeatContractError(
            "formal sector source date differs from target: "
            f"requested={requested_date} source={snapshot_date}"
        )
    evidence = validate_formal_sector_rows(
        db_rows,
        target_date=snapshot_date,
        raw_count=raw_count,
    ) if formal else {
        "raw_count": raw_count,
        "l1_count": len(rows_l1),
        "l2_count": len(rows_l2),
        "row_count": len(db_rows),
        "row_hash": sector_heat_row_hash(db_rows),
    }

    if diagnostic_cache:
        _cache_rows(db_rows, snapshot_date, requested_date)

    if dry_run:
        _progress(
            f"干跑完成: 快照日期 {snapshot_date}, 东财一级行业 {len(rows_l1)} 条, "
            f"东财二级行业 {len(rows_l2)} 条",
            formal=formal,
        )
        if formal:
            return _formal_sector_receipt(
                status="VALIDATED",
                requested_date=requested_date,
                data_date=snapshot_date,
                started_at=started_at,
                finished_at=(now or datetime.now()).replace(microsecond=0),
                published=False,
                evidence=evidence,
            )
        print(f"RAW={raw_count}")
        print(f"DATE={snapshot_date}")
        print(f"SYNCED={len(db_rows)}")
        return {"synced": len(db_rows), "raw_count": raw_count, "l1_count": len(rows_l1), "l2_count": len(rows_l2), "date": snapshot_date}

    from sqlalchemy import text

    engine = engine or create_tool_engine(resolve_tool_mysql_url())
    insert_sql = text(
        "INSERT INTO st_hot_concept_ths_daily "
        "(snapshot_date, plate_type, `rank`, concept_code, concept_name, change_pct, hot_value, hot_tag, etl_sync_at) "
        "VALUES (:snapshot_date, :plate_type, :rank, :concept_code, :concept_name, :change_pct, :hot_value, :hot_tag, :etl_sync_at)"
    )

    persisted_evidence: dict[str, Any] | None = None
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM st_hot_concept_ths_daily WHERE snapshot_date = :d AND plate_type IN (3, 4)"),
            {"d": snapshot_date},
        )
        inserted = conn.execute(insert_sql, db_rows)
        if inserted.rowcount is not None and inserted.rowcount >= 0 and int(inserted.rowcount) != len(db_rows):
            raise SectorHeatContractError(
                f"sector publication row count differs: expected={len(db_rows)} actual={inserted.rowcount}"
            )
        if formal:
            persisted = _select_sector_rows(conn, snapshot_date)
            persisted_evidence = validate_formal_sector_rows(
                persisted,
                target_date=snapshot_date,
                raw_count=raw_count,
            )
            if persisted_evidence != evidence:
                raise SectorHeatContractError(
                    "persisted formal sector snapshot differs from collected receipt"
                )

    if formal:
        if persisted_evidence is None:  # pragma: no cover - defensive contract guard
            raise SectorHeatContractError("formal sector readback evidence is missing")
        return _formal_sector_receipt(
            status="PASS",
            requested_date=requested_date,
            data_date=snapshot_date,
            started_at=started_at,
            finished_at=(now or datetime.now()).replace(microsecond=0),
            published=True,
            evidence=persisted_evidence,
        )

    print(f"写入完成: 快照日期 {snapshot_date}, 东财一级行业 {len(rows_l1)} 条, 东财二级行业 {len(rows_l2)} 条")
    print(f"RAW={raw_count}")
    print(f"DATE={snapshot_date}")
    print(f"SYNCED={len(db_rows)}")
    return {"synced": len(db_rows), "raw_count": raw_count, "l1_count": len(rows_l1), "l2_count": len(rows_l2), "date": snapshot_date}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="同步东财板块热度（写入 st_hot_concept_ths_daily 的 plate_type=3/4）")
    parser.add_argument("date", nargs="?", default="", help="快照日期，格式：YYYY-MM-DD；正式模式默认从交易日历解析")
    parser.add_argument("--dry-run", action="store_true", help="只抓取并聚合，不写入数据库")
    parser.add_argument("--formal", action="store_true", help="启用精确目标日、完整目录和写后回读合同")
    parser.add_argument(
        "--diagnostic-cache",
        action="store_true",
        help="仅诊断干跑时允许读取缓存；缓存永不写正式表",
    )
    parser.add_argument("--json", action="store_true", help="输出唯一机器结果 JSON")
    args = parser.parse_args(argv)

    started_at = datetime.now().replace(microsecond=0)
    engine = None
    requested_date = str(args.date or "").strip()
    try:
        if args.formal and not requested_date:
            engine = create_tool_engine(resolve_tool_mysql_url())
            requested_date = resolve_formal_sector_target_date(engine, now=started_at)
        if not requested_date:
            raise SectorHeatContractError("sector target date is required")
        result = fetch_sector_heat_east_daily(
            requested_date,
            dry_run=args.dry_run,
            formal=args.formal,
            diagnostic_cache=args.diagnostic_cache,
            engine=engine,
            now=started_at,
        )
    except Exception as exc:
        result = _formal_sector_receipt(
            status="FAILED",
            requested_date=requested_date,
            data_date="",
            started_at=started_at,
            finished_at=datetime.now().replace(microsecond=0),
            published=False,
            error=exc,
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
        return 1

    if args.formal or args.diagnostic_cache or args.json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    if args.formal:
        return 0 if result.get("status") in {"PASS", "VALIDATED"} else 1
    if args.diagnostic_cache:
        return 0
    return 0 if int(result.get("synced") or 0) > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

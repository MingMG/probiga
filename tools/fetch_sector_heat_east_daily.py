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
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

DEFAULT_MYSQL_URL = "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4"
CACHE_FILE = ROOT / "data" / "east_sector_heat_cache.json"

EASTMONEY_CLIST_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
EASTMONEY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://data.eastmoney.com/",
}


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

    node_candidates: list[str] = []
    env_node = os.environ.get("NODE_BINARY", "").strip()
    if env_node:
        node_candidates.append(env_node)
    codex_node = r"C:\Users\Administrator\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe"
    if os.path.exists(codex_node):
        node_candidates.append(codex_node)
    sys_node = shutil.which("node")
    if sys_node:
        node_candidates.append(sys_node)
    for node_exe in node_candidates:
        node_script = r"""
const url = process.env.EASTMONEY_URL;
const timeoutMs = Number(process.env.EASTMONEY_TIMEOUT_MS || 30000);
const controller = new AbortController();
const timer = setTimeout(() => controller.abort(), timeoutMs);
fetch(url, {
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://data.eastmoney.com/'
  },
  signal: controller.signal
}).then(async (resp) => {
  clearTimeout(timer);
  const text = await resp.text();
  if (!resp.ok) {
    console.error(`HTTP ${resp.status}: ${text.slice(0, 500)}`);
    process.exit(2);
  }
  process.stdout.write(text);
}).catch((err) => {
  clearTimeout(timer);
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});
"""
        env = os.environ.copy()
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


def _mysql_url() -> str:
    return os.environ.get("MYSQL_URL", DEFAULT_MYSQL_URL)


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
        CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


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
    except Exception:
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
    except Exception:
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
    except Exception:
        return target


def _apply_historical_values(rows_l2: list[dict], snapshot_date: str) -> list[dict]:
    updated = []
    for row in rows_l2:
        try:
            klines = _fetch_industry_kline(str(row["concept_code"]), snapshot_date)
        except Exception:
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


def fetch_sector_heat_east_daily(snapshot_date: str, dry_run: bool = False) -> dict:
    requested_date = snapshot_date
    raw_count = 0
    print(f"开始同步东财板块热度，请求日期: {requested_date}")
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
        now = datetime.now().replace(microsecond=0)
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
                        "etl_sync_at": now,
                    }
                )
    except Exception as exc:
        cached = _load_cached_rows()
        if not cached:
            print(f"未获取到东财行业板块数据: {exc}")
            print("SYNCED=0")
            return {"synced": 0, "l1_count": 0, "l2_count": 0}
        db_rows = cached["db_rows"]
        snapshot_date = cached["snapshot_date"] or requested_date
        rows_l1 = [r for r in db_rows if int(r.get("plate_type") or 0) == 3]
        rows_l2 = [r for r in db_rows if int(r.get("plate_type") or 0) == 4]
        print(f"东财抓取失败，使用本地缓存快照 {snapshot_date}: {exc}")

    _cache_rows(db_rows, snapshot_date, requested_date)

    if dry_run:
        print(f"干跑完成: 快照日期 {snapshot_date}, 东财一级行业 {len(rows_l1)} 条, 东财二级行业 {len(rows_l2)} 条")
        print(f"RAW={raw_count}")
        print(f"DATE={snapshot_date}")
        print(f"SYNCED={len(db_rows)}")
        return {"synced": len(db_rows), "raw_count": raw_count, "l1_count": len(rows_l1), "l2_count": len(rows_l2), "date": snapshot_date}

    from sqlalchemy import create_engine, text

    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    insert_sql = text(
        "INSERT INTO st_hot_concept_ths_daily "
        "(snapshot_date, plate_type, rank, concept_code, concept_name, change_pct, hot_value, hot_tag, etl_sync_at) "
        "VALUES (:snapshot_date, :plate_type, :rank, :concept_code, :concept_name, :change_pct, :hot_value, :hot_tag, :etl_sync_at)"
    )

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM st_hot_concept_ths_daily WHERE snapshot_date = :d AND plate_type IN (3, 4)"),
            {"d": snapshot_date},
        )
        conn.execute(insert_sql, db_rows)

    print(f"写入完成: 快照日期 {snapshot_date}, 东财一级行业 {len(rows_l1)} 条, 东财二级行业 {len(rows_l2)} 条")
    print(f"RAW={raw_count}")
    print(f"DATE={snapshot_date}")
    print(f"SYNCED={len(db_rows)}")
    return {"synced": len(db_rows), "raw_count": raw_count, "l1_count": len(rows_l1), "l2_count": len(rows_l2), "date": snapshot_date}


def main():
    parser = argparse.ArgumentParser(description="同步东财板块热度（写入 st_hot_concept_ths_daily 的 plate_type=3/4）")
    parser.add_argument("date", help="快照日期，格式：YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="只抓取并聚合，不写入数据库")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        raise SystemExit(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}")

    result = fetch_sector_heat_east_daily(args.date, dry_run=args.dry_run)
    if result.get("synced", 0) <= 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

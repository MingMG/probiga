# -*- coding: utf-8 -*-
"""
新浪财经 A 股日 K（hisdata_klc2/klc_kl.js），解密通过 Node 执行 vendor/sina_kline_decoder.js（来自 AkShare hk_js_decode）。

需本机已安装 Node.js：优先 PATH 中的 node；也可设环境变量 SM_NODE_BIN 指向 node.exe 绝对路径。
无需 akshare、无需 py_mini_racer。
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

logger = logging.getLogger("sync_stock_market")

_VEND = Path(__file__).resolve().parent / "vendor"
_DECODER_JS = _VEND / "sina_kline_decoder.js"
_RUNNER_JS = _VEND / "sina_decode_run.js"

SINA_HIST_URL = "https://finance.sina.com.cn/realstock/company/{}/hisdata_klc2/klc_kl.js"
SINA_AMOUNT_URL = (
    "https://stock.finance.sina.com.cn/stock/api/jsonp.php/"
    "var%20KKE_ShareAmount_{}=/StockService.getAmountBySymbol?_=20&symbol={}"
)
SINA_HFQ_URL = "https://finance.sina.com.cn/realstock/company/{}/hfq.js"
SINA_QFQ_URL = "https://finance.sina.com.cn/realstock/company/{}/qfq.js"

_SINA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/",
    "Accept": "*/*",
}

# 模块级 Session，复用 TCP 连接，避免每次请求都重新建连
_SESSION = requests.Session()
_SESSION.headers.update(_SINA_HEADERS)
_SESSION.trust_env = False
# 连接池适配器：每个 host 最多 8 个连接，自动重试 3 次
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=8, pool_maxsize=8, max_retries=requests.adapters.Retry(
        total=3, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    ),
)
_SESSION.mount("https://", _adapter)
_SESSION.mount("http://", _adapter)


def _naive_normalize_index(dti: pd.DatetimeIndex | pd.Index) -> pd.DatetimeIndex:
    """统一为无时区日历日，避免 K 线与股本/复权因子 merge 时 tz-naive 与 tz-aware 冲突。"""
    dti = pd.DatetimeIndex(pd.to_datetime(dti, errors="coerce"))
    if dti.tz is not None:
        dti = dti.tz_convert("Asia/Shanghai")
        dti = pd.DatetimeIndex([ts.replace(tzinfo=None) for ts in dti])
    return dti.normalize()


def _expand_node_hint(raw: str) -> str:
    s = os.path.expandvars(os.path.expanduser(raw.strip().strip('"')))
    return s.strip().strip('"')


def _resolve_node_executable(expanded: str) -> Optional[str]:
    """把用户输入解析为存在的 node.exe 路径。"""
    if not expanded:
        return None
    p = Path(expanded)
    if p.is_file():
        return str(p.resolve())
    if p.is_dir():
        for name in ("node.exe", "node"):
            cand = p / name
            if cand.is_file():
                return str(cand.resolve())
    if sys.platform == "win32" and not expanded.lower().endswith(".exe"):
        pe = Path(expanded + ".exe")
        if pe.is_file():
            return str(pe.resolve())
    return None


def _node_path_winreg() -> Optional[str]:
    if sys.platform != "win32":
        return None
    try:
        import winreg
    except ImportError:
        return None
    for hive, root in (
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\node.exe"),
        (winreg.HKEY_CURRENT_USER, r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\node.exe"),
    ):
        try:
            with winreg.OpenKey(hive, root) as k:
                path, _ = winreg.QueryValueEx(k, "")
                got = _resolve_node_executable(_expand_node_hint(str(path)))
                if got:
                    return got
        except OSError:
            continue
    return None


def _node_path_cmd_where() -> Optional[str]:
    """部分环境下 Python 的 PATH 比 cmd 少，用 where 再试一次（仅 Windows）。"""
    if sys.platform != "win32":
        return None
    try:
        r = subprocess.run(
            ["cmd.exe", "/d", "/s", "/c", "where node"],
            capture_output=True,
            text=True,
            timeout=8,
            encoding="utf-8",
            errors="replace",
        )
    except OSError:
        return None
    if r.returncode != 0 or not (r.stdout or "").strip():
        return None
    first = (r.stdout or "").strip().splitlines()[0].strip()
    return _resolve_node_executable(first)


def _node_path() -> Optional[str]:
    """定位 node：SM_NODE_BIN / NODE_BINARY → PATH → where → 注册表 App Paths → 常见目录。"""
    for key in ("SM_NODE_BIN", "NODE_BINARY"):
        raw = (os.environ.get(key) or "").strip()
        if not raw:
            continue
        expanded = _expand_node_hint(raw)
        got = _resolve_node_executable(expanded)
        if got:
            return got
        logger.warning(
            "环境变量 %s=%r 展开为 %r，但未找到可执行文件；将尝试其它方式定位 node",
            key,
            raw,
            expanded,
        )
    for name in ("node", "node.exe"):
        w = shutil.which(name)
        if w:
            got = _resolve_node_executable(w)
            if got:
                return got
    got = _node_path_cmd_where()
    if got:
        return got
    got = _node_path_winreg()
    if got:
        return got
    if sys.platform == "win32":
        roots: list[Path] = []
        for ev in ("ProgramFiles", "ProgramFiles(x86)"):
            v = os.environ.get(ev)
            if v:
                roots.append(Path(v) / "nodejs")
        roots.append(Path(r"C:\Program Files\nodejs"))
        roots.append(Path(r"C:\Program Files (x86)\nodejs"))
        la = os.environ.get("LOCALAPPDATA", "").strip()
        if la:
            roots.append(Path(la) / "Programs" / "nodejs")
        for root in roots:
            for exe in ("node.exe", "node"):
                cand = root / exe
                if cand.is_file():
                    return str(cand.resolve())
    return None


def _decode_payload_with_node(payload: str) -> list[dict[str, Any]]:
    node = _node_path()
    if not node:
        hint = (os.environ.get("SM_NODE_BIN") or os.environ.get("NODE_BINARY") or "").strip()
        extra = ""
        if hint:
            extra = f" 当前 SM_NODE_BIN/NODE_BINARY 展开后仍无效: {_expand_node_hint(hint)!r}。"
        raise RuntimeError(
            "新浪日 K 需要 Node.js 执行解密：请安装 https://nodejs.org/ 并加入 PATH；"
            "或在 PowerShell 中设置 SM_NODE_BIN 为实际存在的 node.exe，例如 "
            r'(Get-Command node).Source 或 "C:\Program Files\nodejs\node.exe"。'
            + extra
            + " 也可设 SM_STOCK_KLINE_ENGINE=east 改走东财接口。"
        )
    if not _DECODER_JS.is_file() or not _RUNNER_JS.is_file():
        raise RuntimeError(
            f"缺少解密资源文件：{_DECODER_JS} / {_RUNNER_JS}。"
            "请在项目根执行: python tools/extract_sina_kline_js.py"
        )
    proc = subprocess.run(
        [node, str(_RUNNER_JS), str(_DECODER_JS)],
        input=payload.encode("utf-8"),
        capture_output=True,
        timeout=120,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or b"").decode("utf-8", errors="replace")
        raise RuntimeError(f"Node 解密失败 (exit {proc.returncode}): {err[:2000]}")
    out = proc.stdout.decode("utf-8").strip()
    if not out:
        return []
    return json.loads(out)


def _parse_hist_payload(raw_text: str) -> str:
    # 与 akshare.stock_zh_a_sina.stock_zh_a_daily 一致：取第一个 = 之后、分号前的密文
    part = raw_text.split("=", 1)[1].split(";", 1)[0].replace('"', "")
    return part.strip()


def _amount_json_from_text(text: str) -> list[Any]:
    lo = text.find("[")
    hi = text.rfind("]")
    if lo < 0 or hi < lo:
        return []
    blob = text[lo : hi + 1]
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        return json.loads(blob.replace("'", '"'))


def _eval_factor_data(res_text: str) -> Any:
    """东财式 hfq/qfq.js 返回 var xxx=({\"data\":...});"""
    blob = res_text.split("=", 1)[1].split("\n", 1)[0].strip().rstrip(";")
    if blob.endswith(";"):
        blob = blob[:-1]
    return eval(blob, {"__builtins__": {}}, {})  # noqa: S307


def fetch_sina_a_daily_kline(
    sina_symbol: str,
    start_date: str,
    end_date: str,
    adjust: str,
    *,
    timeout: float = 45.0,
) -> Optional[pd.DataFrame]:
    """
    返回与 akshare.stock_zh_a_daily(adjust='') 接近的 DataFrame：列 date, open, high, low, close, volume, amount, outstanding_share, turnover
    start_date/end_date: YYYYMMDD
    """
    if adjust not in ("", "qfq", "hfq"):
        raise ValueError(f"adjust 须为 ''|qfq|hfq，收到: {adjust!r}")

    r = _SESSION.get(SINA_HIST_URL.format(sina_symbol), timeout=timeout)
    r.raise_for_status()
    payload = _parse_hist_payload(r.text)
    dict_list = _decode_payload_with_node(payload)
    if not dict_list:
        return None
    data_df = pd.DataFrame(dict_list)
    data_df.index = _naive_normalize_index(pd.to_datetime(data_df["date"], errors="coerce"))
    data_df = data_df.drop(columns=["date", "postVol", "postAmt"], errors="ignore")
    data_df = data_df.rename(columns={"prevclose": "pre_close"})
    data_df = data_df[~data_df.index.duplicated(keep="last")]
    data_df = data_df.astype(float)

    r2 = _SESSION.get(
        SINA_AMOUNT_URL.format(sina_symbol, sina_symbol),
        timeout=timeout,
    )
    r2.raise_for_status()
    amount_data_json = _amount_json_from_text(r2.text)
    amount_data_df = pd.DataFrame(amount_data_json)
    if amount_data_df.empty:
        amount_data_df["outstanding_share"] = pd.NA
    else:
        amount_data_df.columns = ["date", "outstanding_share"]
        amount_data_df.index = _naive_normalize_index(pd.to_datetime(amount_data_df["date"], errors="coerce"))
        del amount_data_df["date"]
        amount_data_df = amount_data_df[~amount_data_df.index.duplicated(keep="last")]
        amount_data_df = amount_data_df.reindex(data_df.index)
    temp_df = pd.merge(data_df, amount_data_df, left_index=True, right_index=True, how="outer")
    raw_pre_close = temp_df["pre_close"].copy() if "pre_close" in temp_df.columns else None
    temp_df.ffill(inplace=True)
    if raw_pre_close is not None:
        temp_df["pre_close"] = raw_pre_close
    temp_df = temp_df.astype(float)
    temp_df["outstanding_share"] = temp_df["outstanding_share"] * 10000
    temp_df["turnover"] = temp_df["volume"] / temp_df["outstanding_share"]

    if adjust == "":
        d0 = pd.to_datetime(start_date, format="%Y%m%d")
        d1 = pd.to_datetime(end_date, format="%Y%m%d")
        temp_df = temp_df.loc[d0:d1]
        temp_df.drop_duplicates(
            subset=["open", "high", "low", "close", "volume", "amount"],
            inplace=True,
        )
        temp_df["open"] = round(temp_df["open"], 2)
        temp_df["high"] = round(temp_df["high"], 2)
        temp_df["low"] = round(temp_df["low"], 2)
        temp_df["close"] = round(temp_df["close"], 2)
        temp_df.dropna(subset=["open", "high", "low", "close", "volume", "amount"], inplace=True)
        temp_df.drop_duplicates(inplace=True)
        temp_df.reset_index(inplace=True)
        if "date" not in temp_df.columns and len(temp_df.columns):
            temp_df.rename(columns={temp_df.columns[0]: "date"}, inplace=True)
        temp_df["date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.normalize()
        return temp_df

    temp_df = temp_df.drop(columns=["pre_close"], errors="ignore")

    if adjust == "hfq":
        res = _SESSION.get(SINA_HFQ_URL.format(sina_symbol), timeout=timeout)
        res.raise_for_status()
        hfq_factor_df = pd.DataFrame(_eval_factor_data(res.text)["data"])
        hfq_factor_df.columns = ["date", "hfq_factor"]
        hfq_factor_df.index = _naive_normalize_index(pd.to_datetime(hfq_factor_df["date"], errors="coerce"))
        del hfq_factor_df["date"]
        temp_df = pd.merge(temp_df, hfq_factor_df, left_index=True, right_index=True, how="outer")
        temp_df.ffill(inplace=True)
        temp_df = temp_df.astype(float)
        temp_df.dropna(inplace=True)
        temp_df.drop_duplicates(
            subset=["open", "high", "low", "close", "volume", "amount"],
            inplace=True,
        )
        temp_df["open"] = temp_df["open"] * temp_df["hfq_factor"]
        temp_df["high"] = temp_df["high"] * temp_df["hfq_factor"]
        temp_df["close"] = temp_df["close"] * temp_df["hfq_factor"]
        temp_df["low"] = temp_df["low"] * temp_df["hfq_factor"]
        temp_df = temp_df.iloc[:, :-1]
        d0 = pd.to_datetime(start_date, format="%Y%m%d")
        d1 = pd.to_datetime(end_date, format="%Y%m%d")
        temp_df = temp_df.loc[d0:d1]
        temp_df["open"] = round(temp_df["open"], 2)
        temp_df["high"] = round(temp_df["high"], 2)
        temp_df["low"] = round(temp_df["low"], 2)
        temp_df["close"] = round(temp_df["close"], 2)
        temp_df.dropna(inplace=True)
        temp_df.reset_index(inplace=True)
        if "date" not in temp_df.columns and len(temp_df.columns):
            temp_df.rename(columns={temp_df.columns[0]: "date"}, inplace=True)
        temp_df["date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.normalize()
        return temp_df

    if adjust == "qfq":
        res = _SESSION.get(SINA_QFQ_URL.format(sina_symbol), timeout=timeout)
        res.raise_for_status()
        qfq_factor_df = pd.DataFrame(_eval_factor_data(res.text)["data"])
        qfq_factor_df.columns = ["date", "qfq_factor"]
        qfq_factor_df.index = _naive_normalize_index(pd.to_datetime(qfq_factor_df["date"], errors="coerce"))
        del qfq_factor_df["date"]
        temp_df = pd.merge(temp_df, qfq_factor_df, left_index=True, right_index=True, how="outer")
        temp_df.ffill(inplace=True)
        temp_df = temp_df.astype(float)
        temp_df.dropna(inplace=True)
        temp_df.drop_duplicates(
            subset=["open", "high", "low", "close", "volume", "amount"],
            inplace=True,
        )
        temp_df["open"] = temp_df["open"] / temp_df["qfq_factor"]
        temp_df["high"] = temp_df["high"] / temp_df["qfq_factor"]
        temp_df["close"] = temp_df["close"] / temp_df["qfq_factor"]
        temp_df["low"] = temp_df["low"] / temp_df["qfq_factor"]
        temp_df = temp_df.iloc[:, :-1]
        d0 = pd.to_datetime(start_date, format="%Y%m%d")
        d1 = pd.to_datetime(end_date, format="%Y%m%d")
        temp_df = temp_df.loc[d0:d1]
        temp_df["open"] = round(temp_df["open"], 2)
        temp_df["high"] = round(temp_df["high"], 2)
        temp_df["low"] = round(temp_df["low"], 2)
        temp_df["close"] = round(temp_df["close"], 2)
        temp_df.dropna(inplace=True)
        temp_df.reset_index(inplace=True)
        if "date" not in temp_df.columns and len(temp_df.columns):
            temp_df.rename(columns={temp_df.columns[0]: "date"}, inplace=True)
        temp_df["date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.normalize()
        return temp_df

    return None

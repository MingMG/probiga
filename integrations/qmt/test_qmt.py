from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt import import_xtdata, iter_xtquant_import_paths, to_qmt_symbol


def _print_header(title: str) -> None:
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def check_xtquant_import():
    _print_header("[1] Import xtquant")
    try:
        xtdata = import_xtdata()
        print("OK: xtquant import succeeded")
        print(f"xtdata module: {getattr(xtdata, '__file__', '')}")
        return xtdata
    except Exception as exc:
        print(f"FAIL: {exc}")
        print("Searched paths:")
        for path in iter_xtquant_import_paths():
            print(f"  - {path}")
        return None


def check_connection(xtdata) -> bool:
    _print_header("[2] Connect miniQMT")
    try:
        xtdata.connect()
        print("OK: connected to miniQMT")
        return True
    except Exception as exc:
        print(f"FAIL: {exc}")
        return False


def test_get_instrument_list(xtdata):
    _print_header("[3] Load instrument list")
    try:
        instruments = xtdata.get_stock_list_in_sector("沪深A股")
        count = len(instruments) if instruments else 0
        print(f"OK: instrument count={count}")
        if instruments:
            print(f"Sample: {instruments[:5]}")
        return instruments
    except Exception as exc:
        print(f"FAIL: {exc}")
        return None


def test_subscribe_quote(xtdata, codes: list[str]) -> bool:
    _print_header("[4] Subscribe realtime quotes")
    qmt_codes = [to_qmt_symbol(code) for code in codes if to_qmt_symbol(code)]
    if not qmt_codes:
        print("FAIL: no valid stock codes")
        return False
    try:
        for code in qmt_codes:
            xtdata.subscribe_quote(code, period="1m", count=-1)
            print(f"Subscribed: {code}")
        time.sleep(2)
        return True
    except Exception as exc:
        print(f"FAIL: {exc}")
        return False


def test_get_market_data(xtdata, codes: list[str]):
    _print_header("[5] Fetch 1-minute bars")
    qmt_codes = [to_qmt_symbol(code) for code in codes if to_qmt_symbol(code)]
    if not qmt_codes:
        print("FAIL: no valid stock codes")
        return None
    try:
        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=qmt_codes,
            period="1m",
            count=10,
            dividend_type="none",
            fill_data=True,
        )
        if not isinstance(data, dict) or not data:
            print("WARN: no minute data returned")
            return None
        print(f"OK: received {len(data)} symbols")
        for code, frame in list(data.items())[:2]:
            print(f"{code}: rows={0 if frame is None else len(frame)}")
        return data
    except Exception as exc:
        print(f"FAIL: {exc}")
        return None


def test_get_full_tick(xtdata, codes: list[str]):
    _print_header("[6] Fetch full tick snapshot")
    qmt_codes = [to_qmt_symbol(code) for code in codes if to_qmt_symbol(code)]
    if not qmt_codes:
        print("FAIL: no valid stock codes")
        return None
    try:
        data = xtdata.get_full_tick(qmt_codes)
        if not isinstance(data, dict) or not data:
            print("WARN: no tick snapshot returned")
            return None
        print(f"OK: snapshot count={len(data)}")
        sample_code = next(iter(data))
        print(f"Sample {sample_code}: {list((data.get(sample_code) or {}).items())[:8]}")
        return data
    except Exception as exc:
        print(f"FAIL: {exc}")
        return None


def test_get_tick_data(xtdata, codes: list[str]):
    _print_header("[7] Fetch tick data")
    qmt_codes = [to_qmt_symbol(code) for code in codes if to_qmt_symbol(code)]
    if not qmt_codes:
        print("FAIL: no valid stock codes")
        return None
    try:
        for code in qmt_codes:
            xtdata.subscribe_quote(code, period="tick", count=-1)
        time.sleep(2)
        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=qmt_codes,
            period="tick",
            count=10,
            dividend_type="none",
            fill_data=True,
        )
        if not isinstance(data, dict) or not data:
            print("WARN: no tick detail returned")
            return None
        print(f"OK: tick payload count={len(data)}")
        return data
    except Exception as exc:
        print(f"FAIL: {exc}")
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="QMT connectivity test")
    parser.add_argument("--codes", default="000001,600519", help="comma-separated stock codes")
    args = parser.parse_args()

    codes = [item.strip() for item in args.codes.split(",") if item.strip()]
    print(f"QMT connectivity check at {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Test codes: {codes}")

    xtdata = check_xtquant_import()
    if xtdata is None:
        sys.exit(1)
    if not check_connection(xtdata):
        sys.exit(1)

    test_get_instrument_list(xtdata)
    test_subscribe_quote(xtdata, codes)
    test_get_market_data(xtdata, codes)
    test_get_full_tick(xtdata, codes)
    test_get_tick_data(xtdata, codes)

    _print_header("Done")
    print("If all steps passed, QMT can provide this project's market data inputs.")


if __name__ == "__main__":
    main()

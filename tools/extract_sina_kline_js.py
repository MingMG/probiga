# -*- coding: utf-8 -*-
"""从已安装的 akshare 抽取 hk_js_decode 到 biz/stock_market/vendor/sina_kline_decoder.js"""
from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    cons: Path | None = None
    try:
        import akshare

        cons = Path(akshare.__file__).resolve().parent / "stock" / "cons.py"
    except Exception:
        import site

        for base in site.getsitepackages():
            cand = Path(base) / "akshare" / "stock" / "cons.py"
            if cand.is_file():
                cons = cand
                break
    if cons is None or not cons.is_file():
        print("未找到 akshare/stock/cons.py，请先 pip install akshare。", file=sys.stderr)
        sys.exit(1)
    text = cons.read_text(encoding="utf-8")
    key = 'hk_js_decode = """'
    a = text.index(key) + len(key)
    b = text.index('"""', a)
    js = text[a:b].strip() + "\n"
    root = Path(__file__).resolve().parents[1]
    out = root / "biz" / "stock_market" / "vendor" / "sina_kline_decoder.js"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(js, encoding="utf-8")
    print("written:", out, "chars:", len(js))


if __name__ == "__main__":
    main()

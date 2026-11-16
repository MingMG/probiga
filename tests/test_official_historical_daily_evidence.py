from datetime import datetime
from hashlib import sha256

import pytest

from server.common.official_historical_daily_evidence import historical_daily_evidence


CUTOFF = datetime.fromisoformat("2026-11-16T08:44:11+08:00")


def _document(code, publication, number, text):
    source_text = f"证券代码：{code}\n{text}"
    return {
        "source_url": f"https://static.cninfo.com.cn/finalpage/{publication}/{number}.PDF",
        "observed_at": "2026-11-16T08:44:10+08:00",
        "source_text": source_text,
        "source_text_sha256": sha256(source_text.encode("utf-8")).hexdigest(),
        "official_document_sha256": sha256(f"{publication}:{number}".encode("utf-8")).hexdigest(),
    }


def _proof(documents, code, day="2026-10-22", cutoff=CUTOFF):
    return historical_daily_evidence(documents, stock_code=code, day=day, cutoff=cutoff)[0]


def test_voluntary_delisting_suspension_has_two_effective_endpoints():
    halt = _document(
        "000016、200016", "2026-09-04", 1,
        "关于本次终止上市事项的停牌公告\n"
        "公司股票（证券代码：000016、200016）将自2026年9月4日（星期五）开市起停牌，"
        "直至深交所在公告终止上市决定后对公司股票予以摘牌。",
    )
    delist = _document(
        "000016、200016", "2026-10-23", 2,
        "关于股票终止上市暨摘牌的公告\n深交所同意公司股票主动终止上市。"
        "摘牌日期：2026年10月23日。",
    )

    assert _proof([delist, halt], "000016") == "NO_TRADE"
    assert _proof([halt, delist], "000016", "2026-10-23") == "DELISTED"
    for missing_endpoint in ([halt], [delist]):
        with pytest.raises(ValueError, match="unproven"):
            _proof(missing_endpoint, "000016")
    with pytest.raises(ValueError, match="unproven"):
        _proof([halt, delist], "000016", "2026-09-03")

    wrong_issuer = _document("000017", "2026-10-23", 2, delist["source_text"].split("\n", 1)[1])
    with pytest.raises(ValueError, match="identity or time differs"):
        _proof([halt, wrong_issuer], "000016")
    with pytest.raises(ValueError, match="identity or time differs"):
        _proof([halt, delist], "000016", cutoff=datetime.fromisoformat("2026-11-16T08:44:09+08:00"))

    earlier_resume = _document(
        "000016", "2026-10-20", 3,
        "公司股票自2026年10月20日开市起复牌。",
    )
    with pytest.raises(ValueError, match="contradicted by a resumption"):
        _proof([halt, delist, earlier_resume], "000016")

    wrong_effective_day = _document(
        "000016", "2026-10-21", 4,
        "关于股票终止上市暨摘牌的公告\n深交所同意公司股票主动终止上市。"
        "摘牌日期：2026年10月23日。",
    )
    with pytest.raises(ValueError, match="interval is ambiguous"):
        _proof([halt, delist, wrong_effective_day], "000016")


def test_regulatory_suspension_ends_before_exact_delisting_board_resumption():
    halt = _document(
        "002731", "2026-09-07", 1,
        "关于公司股票将被终止上市暨继续停牌的风险提示公告\n"
        "公司股票已触及规范类退市情形，于2026年9月1日开市起停牌。"
        "公司股票（股票代码：002731）自2026年9月7日（星期一）起继续实施停牌。",
    )
    admission = _document(
        "002731", "2026-10-23", 2,
        "关于公司股票进入退市整理期交易首日的风险提示公告\n"
        "公司股票于2026年10月23日复牌并进入退市整理期。",
    )

    assert _proof([halt, admission], "002731") == "NO_TRADE"
    with pytest.raises(ValueError, match="resumed on target session"):
        _proof([halt, admission], "002731", "2026-10-23")
    with pytest.raises(ValueError, match="unproven"):
        _proof([halt, admission], "002731", "2026-10-26")
    with pytest.raises(ValueError, match="unproven"):
        _proof([halt], "002731")


def test_definitive_intraday_open_wording_closes_historical_halt_interval():
    resumption = _document(
        "301314", "2026-10-26", 1,
        "关于公司股票复牌的公告\n一、公司股票停牌情况\n"
        "公司股票自2026年10月13日（星期二）开市时起开始停牌。"
        "公司股票自2026年10月26日开市时起开始复牌。",
    )
    assert _proof([resumption], "301314", "2026-10-23") == "NO_TRADE"
    with pytest.raises(ValueError, match="resumed on target session"):
        _proof([resumption], "301314", "2026-10-26")

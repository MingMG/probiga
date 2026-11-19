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


@pytest.mark.parametrize("code,publication,statement,target", [
    ("300211", "2026-09-25", "公司股票将于2026年9月28日（星期一）开市起停牌一天，于2026年9月29日开市起复牌。", "2026-09-28"),
    ("300527", "2026-09-29", "公司股票将于2026年9月29日（星期二）停牌1天，2026年9月30日开市起复牌。", "2026-09-29"),
    ("600363", "2026-09-29", "公司股票将于2026年9月29日停牌一日，复牌后将被实施其他风险警示。", "2026-09-29"),
])
def test_historical_full_day_halt_is_an_exact_session(code, publication, statement, target):
    document = _document(code, publication, 1, statement)
    assert _proof([document], code, target) == "NO_TRADE"
    with pytest.raises(ValueError):
        _proof([document], code, "2026-09-30")
    conditional = _document(code, publication, 2, "若审批通过，" + statement)
    with pytest.raises(ValueError, match="unproven"):
        _proof([conditional], code, target)


@pytest.mark.parametrize("wording", ["上午开市起", ""])
def test_historical_halt_interval_accepts_definitive_resumption_wording(wording):
    document = _document("603400", "2026-09-30", 1,
        "关于公司股票复牌的公告。公司股票停牌情况：公司股票自2026年9月15日开始停牌。"
        f"公司股票于2026年9月30日{wording}复牌。")
    assert _proof([document], "603400", "2026-09-29") == "NO_TRADE"
    with pytest.raises(ValueError, match="resumed on target session"):
        _proof([document], "603400", "2026-09-30")


def test_official_delisting_halt_may_begin_next_session_after_publication():
    halt = _document("301139", "2026-08-29", 1,
        "关于公司股票停牌暨可能被终止上市的风险提示公告。公司股票自2026年8月31日起停牌。")
    resume = _document("301139", "2026-09-30", 2,
        "关于公司股票进入退市整理期交易首日的风险提示公告。公司股票于2026年9月30日复牌并进入退市整理期。")
    assert _proof([halt, resume], "301139", "2026-09-29") == "NO_TRADE"
    with pytest.raises(ValueError, match="unproven"):
        _proof([halt], "301139", "2026-09-29")


def test_merger_terminal_halt_requires_definitive_no_further_trading_statement():
    body = ("关于公司A股股票连续停牌直至终止上市、实施换股吸收合并的提示性公告。"
            "公司A股股票将自2026年9月15日（即A股异议股东现金选择权申报首日）开市起连续停牌，不再交易。")
    halt = _document("601059", "2026-09-12", 1, body)
    assert _proof([halt], "601059", "2026-09-28") == "NO_TRADE"
    for invalid in (body.replace("不再交易", "等待后续安排"), body.replace("公司A股股票将自", "预计公司A股股票将自")):
        with pytest.raises(ValueError, match="unproven"):
            _proof([_document("601059", "2026-09-12", 2, invalid)], "601059", "2026-09-28")
    resume = _document("601059", "2026-09-20", 3, "公司股票自2026年9月20日复牌。")
    with pytest.raises(ValueError, match="unproven"):
        _proof([halt, resume], "601059", "2026-09-28")

import pytest
from biz.stock_info import ths_catalog as ths


def detail(cid="300001", index="885001", total=2, name="WiFi 6"):
    return (f'<h3>{name}<span>{index}</span></h3><input id="clid" value="{index}">'
            f'<input id="requestQuery" value="code/{cid}"><dt>涨幅排名</dt><dd>1/{total}</dd>')


def pages():
    return {
        ths.CATALOG_URL: '<a href="/gn/detail/code/300001/">WiFi6</a>'
                        '<input id="gnSection" value=\'{"0":{"cid":"300001","platecode":"885001"}}\'>',
        ths.DETAIL_URL.format(cid="300001"): detail(),
        ths.DETAIL_URL.format(cid="300002"): detail("300002", "885002"),
        ths.STOCK_CONCEPT_URL.format(stock_code="600000"):
            '<title>浦发银行(600000) 概念题材</title><input id="stockCode" value="600000">'
            '<tr><td class="gnName" clid="885002">同名概念</td>'
            '<td><span class="retractCon" cid="300002"></span></td></tr>',
    }


def test_catalog_discovers_missing_native_index_without_joining_names():
    catalog, evidence = ths.collect_catalog(["600000"], fetch=pages().__getitem__)
    assert [(r["index_code"], r["concept_code"]) for r in catalog] == [("885001", "300001"), ("885002", "300002")]
    assert catalog[0]["name"] == "WiFi 6"
    assert evidence["complete"] and evidence["received_rows"] == evidence["expected_total"] == 2


def test_native_graph_is_merged_with_visible_links():
    content = pages()[ths.CATALOG_URL].replace('"cid":"300001"', '"cid":"300002"')
    assert ths.parse_catalog_page(content) == {"300001", "300002"}


@pytest.mark.parametrize("change", [
    lambda p: p.update({ths.STOCK_CONCEPT_URL.format(stock_code="600000"): '<title>wrong identity</title>'}),
    lambda p: p.update({ths.DETAIL_URL.format(cid="300002"): detail("300999", "885002")}),
    lambda p: p.update({ths.DETAIL_URL.format(cid="300002"): detail("300002", "885002", total=3)}),
])
def test_missing_identity_wrong_binding_or_changing_total_cannot_certify_catalog(change):
    source = pages()
    change(source)
    with pytest.raises(RuntimeError):
        ths.collect_catalog(["600000"], fetch=source.__getitem__)


def test_login_page_never_becomes_an_empty_catalog():
    with pytest.raises(RuntimeError, match="structure differs"):
        ths.parse_catalog_page('<title>登录</title>')

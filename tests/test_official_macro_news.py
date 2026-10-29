from datetime import datetime

from biz.news import official_macro


def test_fed_rss_becomes_structured_news_with_statement_summary(monkeypatch):
    rss = """<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel>
      <item>
        <title>Federal Reserve issues FOMC statement</title>
        <link>https://example.test/fomc</link>
        <description>Federal Reserve issues FOMC statement</description>
        <pubDate>Wed, 28 Oct 2026 18:00:00 GMT</pubDate>
      </item>
    </channel></rss>"""
    statement = """
    <html><body>
    The Committee decided to maintain the target range for the federal funds
    rate at 3-3/4 to 4 percent, in support of its mandate.
    Voting against the monetary policy action were two members, who preferred
    to raise the target range by 1/4 percentage point.
    </body></html>
    """

    monkeypatch.setattr(
        official_macro,
        "_read",
        lambda url, **_kwargs: rss if url.endswith(".xml") else statement,
    )

    items = official_macro.fetch_official_macro_news(
        now=datetime(2026, 10, 29, 8, 0),
    )

    assert len(items) == 1
    item = items[0]
    assert item["source"] == "federal_reserve"
    assert item["title"] == "美联储FOMC公布利率决议"
    assert item["publish_time"] == datetime(2026, 10, 29, 2, 0)
    assert "3.75—4 percent" in item["content"]
    assert "两名委员反对，主张加息25个基点" in item["content"]
    assert item["is_top"] is True

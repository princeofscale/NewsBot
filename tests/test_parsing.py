from pathlib import Path

import pytest
from pydantic import ValidationError

from newsbot.dedupe import same_event
from newsbot.parsing import canonicalize_url, parse_feed, parse_html
from newsbot.schemas import SourceInput, SourceKind

FIXTURES = Path(__file__).parent / "fixtures"


def test_rss_parser_and_canonical_url() -> None:
    articles = parse_feed((FIXTURES / "source_a.xml").read_text(), "https://a.example")
    assert len(articles) == 1
    assert articles[0].url == "https://a.example/news/embankment"
    assert "500 метров" in articles[0].text
    assert canonicalize_url("HTTPS://A.EXAMPLE//x/?utm_medium=x&b=2&a=1#top") == (
        "https://a.example/x?a=1&b=2"
    )


def test_html_parser() -> None:
    articles = parse_html(
        (FIXTURES / "source_b.html").read_text(),
        "https://b.example",
        {
            "item": "article.news",
            "title": "h2",
            "text": ".body",
            "link": "a.url",
            "date": "time",
            "image": "img",
        },
    )
    assert articles[0].url == "https://b.example/story/1"
    assert articles[0].image_url == "https://b.example/images/1.jpg"
    assert articles[0].published_at is not None


def test_event_match_requires_shared_action_context() -> None:
    assert same_event(
        "В Саратове открыли новый участок набережной",
        "Новый участок набережной открыли в Саратове",
    )
    assert not same_event(
        "В Саратове открыли новый участок набережной",
        "В Саратове организация обсудила школьное питание",
    )


def test_source_urls_reject_credentials_and_plain_http() -> None:
    with pytest.raises(ValidationError):
        SourceInput(
            name="unsafe",
            base_url="http://example.test",
            kind=SourceKind.RSS,
            feed_url="https://user:password@example.test/rss",
        )

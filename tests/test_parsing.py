from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from newsbot.dedupe import same_event
from newsbot.parsing import (
    canonicalize_url,
    parse_article_page,
    parse_datetime,
    parse_feed,
    parse_html,
)
from newsbot.schemas import CandidateArticle, SourceInput, SourceKind

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


def test_article_page_parser_prefers_full_text_and_russian_date() -> None:
    discovered = parse_feed((FIXTURES / "source_a.xml").read_text(), "https://a.example")[0]
    article = parse_article_page(
        (FIXTURES / "article_a.html").read_bytes(),
        "text/html; charset=utf-8",
        discovered,
        {},
    )
    assert "Ротонды" in article.text
    assert article.raw_content.startswith(b"<!doctype html>")
    assert article.published_at == datetime(2026, 7, 28, 8, 0, tzinfo=UTC)
    assert article.image_url == "https://a.example/images/embankment.jpg"


def test_article_page_uses_meta_charset_and_url_year() -> None:
    discovered = CandidateArticle(
        url="https://news.example/news/2026/07/28/item",
        title="Анонс",
        text="Кратко",
        raw_content=b"",
        content_type="text/html",
    )
    html = """
    <meta charset="windows-1251">
    <h1>Новость Саратова</h1>
    <p class="date">28 июля, 16:50</p>
    <article>Полный текст новости с точным адресом, временем, участниками,
    организациями и всеми важными подробностями произошедшего события.</article>
    """.encode("windows-1251")

    article = parse_article_page(
        html,
        "text/html",
        discovered,
        {"article_date": ".date"},
    )

    assert article.title == "Новость Саратова"
    assert article.published_at == datetime(2026, 7, 28, 12, 50, tzinfo=UTC)


@pytest.mark.parametrize("value", ["28 июля 2026, 18:30", "28.07.2026 18:30"])
def test_russian_dates_use_saratov_timezone(value: str) -> None:
    assert parse_datetime(value) == datetime(2026, 7, 28, 14, 30, tzinfo=UTC)


def test_event_match_requires_shared_action_context() -> None:
    assert same_event(
        "В Саратове открыли новый участок набережной",
        "Новый участок набережной открыли в Саратове",
    )
    assert not same_event(
        "В Саратове открыли новый участок набережной",
        "В Саратове организация обсудила школьное питание",
    )
    assert not same_event(
        "В Саратове отключат воду на улице Ленина",
        "В Саратове отключат воду на улице Московской",
    )
    assert not same_event(
        "ДТП произошло на Чернышевского",
        "ДТП произошло на Московской",
    )


def test_source_urls_reject_credentials_and_plain_http() -> None:
    with pytest.raises(ValidationError):
        SourceInput(
            name="unsafe",
            base_url="http://example.test",
            kind=SourceKind.RSS,
            feed_url="https://user:password@example.test/rss",
        )

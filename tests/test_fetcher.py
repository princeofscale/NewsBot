import httpx
import pytest

from newsbot.config import Settings
from newsbot.db_models import Source
from newsbot.fetcher import SourceFetcher
from newsbot.schemas import SourceKind


@pytest.mark.asyncio
async def test_fetcher_rejects_oversized_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 10_001)

    settings = Settings(
        fetch_retries=1,
        max_response_bytes=10_000,
        validate_public_source_ips=False,
    )
    fetcher = SourceFetcher(settings, httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    source = Source(
        name="large",
        base_url="https://large.example",
        kind=SourceKind.RSS,
        feed_url="https://large.example/rss",
    )
    with pytest.raises(RuntimeError, match="fetch failed"):
        await fetcher.fetch(source)


@pytest.mark.asyncio
async def test_broken_article_page_does_not_drop_other_articles() -> None:
    feed = """
    <rss version="2.0"><channel>
      <item><title>Первая</title><link>https://news.example/1</link>
        <description>Анонс первой новости</description></item>
      <item><title>Вторая</title><link>https://news.example/2</link>
        <description>Анонс второй новости</description></item>
    </channel></rss>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rss":
            return httpx.Response(200, text=feed)
        if request.url.path == "/1":
            return httpx.Response(
                200,
                text=(
                    "<article><p>Полный текст первой новости с датой, адресом, "
                    "участниками и важными подробностями события для публикации.</p></article>"
                ),
            )
        return httpx.Response(500)

    settings = Settings(fetch_retries=1, validate_public_source_ips=False)
    fetcher = SourceFetcher(
        settings,
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    source = Source(
        name="partial",
        base_url="https://news.example",
        kind=SourceKind.RSS,
        feed_url="https://news.example/rss",
    )

    result = await fetcher.fetch(source)

    assert [article.url for article in result.articles] == ["https://news.example/1"]
    assert result.article_errors == 1


@pytest.mark.asyncio
async def test_article_page_retries_404_with_trailing_slash() -> None:
    feed = """
    <rss version="2.0"><channel><item>
      <title>Новость</title><link>https://news.example/item</link>
      <description>Краткий анонс</description>
    </item></channel></rss>
    """
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path == "/rss":
            return httpx.Response(200, text=feed)
        if request.url.path == "/item":
            return httpx.Response(404)
        return httpx.Response(
            200,
            text=(
                "<article>Полный текст новости с датой, адресом, участниками "
                "и достаточным количеством важных подробностей события.</article>"
            ),
        )

    fetcher = SourceFetcher(
        Settings(fetch_retries=1, validate_public_source_ips=False),
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    source = Source(
        name="slash",
        base_url="https://news.example",
        kind=SourceKind.RSS,
        feed_url="https://news.example/rss",
    )

    result = await fetcher.fetch(source)

    assert seen == ["/rss", "/item", "/item/"]
    assert len(result.articles) == 1


@pytest.mark.asyncio
async def test_discovery_respects_source_item_limit() -> None:
    feed = "<rss><channel>" + "".join(
        f"<item><title>Новость {item}</title><link>https://news.example/{item}</link></item>"
        for item in range(4)
    ) + "</channel></rss>"

    fetcher = SourceFetcher(
        Settings(
            fetch_retries=1,
            max_items_per_source=2,
            validate_public_source_ips=False,
        ),
        httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, text=feed))
        ),
    )
    source = Source(
        name="limited",
        base_url="https://news.example",
        kind=SourceKind.RSS,
        feed_url="https://news.example/rss",
    )

    result = await fetcher.discover(source)

    assert result.discovered_count == 2
    assert [article.title for article in result.articles] == ["Новость 0", "Новость 1"]


@pytest.mark.asyncio
async def test_discovery_rejects_cross_domain_article_url() -> None:
    feed = (
        "<rss><channel><item><title>Чужая ссылка</title>"
        "<link>https://attacker.example/item</link></item></channel></rss>"
    )
    fetcher = SourceFetcher(
        Settings(fetch_retries=1, validate_public_source_ips=False),
        httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, text=feed))
        ),
    )
    source = Source(
        name="source",
        base_url="https://news.example",
        kind=SourceKind.RSS,
        feed_url="https://news.example/rss",
        origin_group="news.example",
    )

    result = await fetcher.fetch(source)

    assert result.articles == []
    assert result.article_errors == 1
    assert result.error_reasons == ("cross-domain article URL",)

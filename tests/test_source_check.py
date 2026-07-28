import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from newsbot.config import Settings
from newsbot.db_models import Source, SourceFetch
from newsbot.fetcher import SourceFetcher
from newsbot.schemas import SourceHealth, SourceKind
from newsbot.source_check import check_sources


@pytest.mark.asyncio
async def test_source_check_reports_and_persists_extraction_health(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    feed = (
        "<rss><channel><item><title>Новость</title>"
        "<link>https://source.example/item</link></item></channel></rss>"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rss":
            return httpx.Response(200, text=feed)
        return httpx.Response(
            200,
            text="<article><h1>Новость</h1><time datetime='2026-07-29T10:00:00+04:00'>"
            "</time><img src='/image.jpg'><p>"
            + "Полный проверяемый текст новости. " * 5
            + "</p></article>",
        )

    async with sessions.begin() as session:
        session.add(
            Source(
                name="Источник",
                base_url="https://source.example",
                kind=SourceKind.RSS,
                feed_url="https://source.example/rss",
                selectors={"article_date": "time", "article_image": "img"},
            )
        )
    fetcher = SourceFetcher(
        Settings(fetch_retries=1, validate_public_source_ips=False),
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    results = await check_sources(sessions, fetcher)

    assert results[0].state == SourceHealth.HEALTHY
    assert results[0].found == results[0].loaded == 1
    assert results[0].dates == results[0].texts == results[0].images == 1
    async with sessions() as session:
        source = await session.scalar(select(Source))
        source_fetch = await session.scalar(select(SourceFetch))
        assert source and source.health_state == SourceHealth.HEALTHY
        assert source_fetch and source_fetch.extraction_success_rate == 1

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from newsbot.config import Settings
from newsbot.db_models import Article, Claim, Draft, Event, EventArticle, PublicationJob, Source
from newsbot.fetcher import SourceFetcher
from newsbot.llm import DeterministicLLM
from newsbot.pipeline import Pipeline
from newsbot.schemas import CandidateArticle, ClaimPayload, EventState, Post, SourceKind

FIXTURES = Path(__file__).parent / "fixtures"


def transport() -> httpx.MockTransport:
    rss = (FIXTURES / "source_a.xml").read_text()
    listing = (FIXTURES / "source_b.html").read_text()
    article_a = (FIXTURES / "article_a.html").read_text()
    article_b = (FIXTURES / "article_b.html").read_text()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rss":
            return httpx.Response(200, text=rss, headers={"etag": '"v1"'})
        if request.url.path == "/news":
            return httpx.Response(200, text=listing)
        return httpx.Response(
            200,
            text=article_a if request.url.host == "a.example" else article_b,
            headers={"content-type": "text/html; charset=utf-8"},
        )

    return httpx.MockTransport(handler)


async def add_sources(sessions: async_sessionmaker[AsyncSession], official: bool = False) -> None:
    async with sessions.begin() as session:
        session.add_all(
            [
                Source(
                    name="A",
                    base_url="https://a.example",
                    kind=SourceKind.RSS,
                    feed_url="https://a.example/rss",
                    trust_level=0.85 if official else 0.7,
                    is_official=official,
                    min_interval_seconds=30,
                ),
                Source(
                    name="B",
                    base_url="https://b.example",
                    kind=SourceKind.HTML,
                    feed_url="https://b.example/news",
                    selectors={
                        "item": "article.news",
                        "title": "h2",
                        "text": ".body",
                        "link": "a.url",
                        "date": "time",
                        "image": "img",
                    },
                    trust_level=0.75,
                    min_interval_seconds=30,
                ),
            ]
        )


@pytest.mark.asyncio
async def test_vertical_slice_clusters_and_is_idempotent(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await add_sources(sessions)
    settings = Settings(
        database_url="sqlite+aiosqlite://",
        fetch_retries=1,
        validate_public_source_ips=False,
    )
    client = httpx.AsyncClient(transport=transport())
    pipeline = Pipeline(sessions, SourceFetcher(settings, client), DeterministicLLM(), settings)

    first = await pipeline.run_cycle()
    assert first.new_articles == 2
    assert first.events_created == 1
    assert first.drafts_created == 1

    async with sessions.begin() as session:
        sources = (await session.scalars(select(Source))).all()
        for source in sources:
            source.last_fetched_at = None
    second = await pipeline.run_cycle()
    assert second.new_articles == 0

    async with sessions() as session:
        assert await session.scalar(select(func.count(Article.id))) == 2
        assert await session.scalar(select(func.count(Event.id))) == 1
        event = await session.scalar(select(Event))
        assert event and event.state == EventState.READY
        draft = await session.scalar(select(Draft))
        assert draft and draft.validated
        assert await session.scalar(select(func.count(PublicationJob.id))) == 2


@pytest.mark.asyncio
async def test_changed_content_at_same_url_creates_revision_and_rechecks_event(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with sessions.begin() as session:
        source = Source(
            name="official",
            base_url="https://official.example",
            kind=SourceKind.RSS,
            feed_url="https://official.example/rss",
            trust_level=1,
            is_official=True,
        )
        session.add(source)
        await session.flush()
        source_id = source.id
    settings = Settings(validate_public_source_ips=False)
    pipeline = Pipeline(
        sessions,
        SourceFetcher(settings, httpx.AsyncClient()),
        DeterministicLLM(),
        settings,
    )
    base = {
        "url": "https://official.example/news/1",
        "title": "На улице Ленина отключат воду",
        "published_at": datetime(2026, 7, 28, 10, tzinfo=UTC),
        "content_type": "text/html",
    }
    await pipeline._ingest(
        source_id,
        CandidateArticle(
            **base,
            text="На улице Ленина отключат воду с 12:00 до 14:00.",
            raw_content="<p>На улице Ленина отключат воду с 12:00 до 14:00.</p>".encode(),
        ),
    )
    await pipeline._ingest(
        source_id,
        CandidateArticle(
            **base,
            text="На улице Ленина отключат воду с 13:00 до 15:00.",
            raw_content="<p>На улице Ленина отключат воду с 13:00 до 15:00.</p>".encode(),
        ),
    )
    async with sessions() as session:
        articles = list((await session.scalars(select(Article).order_by(Article.created_at))).all())
        assert len(articles) == 2
        assert articles[1].revision_of_id == articles[0].id
        assert await session.scalar(select(func.count(Event.id))) == 1
        assert await session.scalar(select(func.count(EventArticle.article_id))) == 2


class HallucinatingLLM(DeterministicLLM):
    async def extract_claims(self, article_id: UUID, title: str, text: str) -> list[ClaimPayload]:
        return [
            ClaimPayload(
                subject=title,
                predicate="Пострадали",
                claim="Пострадали 999 человек",
                numbers=["999"],
                source_article_id=article_id,
            )
        ]


def test_conflicting_numbers_from_independent_claims_are_rejected() -> None:
    claims = [
        ClaimPayload(
            subject="МЧС",
            predicate="сообщило о пострадавших",
            claim="Пострадал 1 человек",
            numbers=["1"],
            source_article_id=uuid4(),
        ),
        ClaimPayload(
            subject="МЧС",
            predicate="сообщило о пострадавших",
            claim="Пострадали 2 человека",
            numbers=["2"],
            source_article_id=uuid4(),
        ),
    ]
    assert Pipeline._claims_conflict(claims)


class FailsOnceLLM(DeterministicLLM):
    def __init__(self) -> None:
        self.failed = False

    async def extract_claims(self, article_id: UUID, title: str, text: str) -> list[ClaimPayload]:
        if not self.failed:
            self.failed = True
            raise ConnectionError("temporary")
        return await super().extract_claims(article_id, title, text)


class TamperingLLM(DeterministicLLM):
    async def create_post(
        self,
        event_id: UUID,
        claims: list[ClaimPayload],
        source_urls: list[str],
        confidence: float,
    ) -> Post:
        post = await super().create_post(event_id, claims, source_urls, confidence)
        return post.model_copy(
            update={
                "source_urls": ["https://attacker.invalid"],
                "confidence": 1.0,
            }
        )


@pytest.mark.asyncio
async def test_llm_cannot_replace_authoritative_post_metadata(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await add_sources(sessions)
    settings = Settings(fetch_retries=1, validate_public_source_ips=False)
    pipeline = Pipeline(
        sessions,
        SourceFetcher(settings, httpx.AsyncClient(transport=transport())),
        TamperingLLM(),
        settings,
    )
    await pipeline.run_cycle()
    async with sessions() as session:
        draft = await session.scalar(select(Draft))
        event = await session.scalar(select(Event))
        assert draft and event
        assert draft.source_urls != ["https://attacker.invalid"]
        assert draft.confidence == event.confidence


@pytest.mark.asyncio
async def test_transient_llm_failure_resumes_existing_article(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await add_sources(sessions)
    settings = Settings(
        database_url="sqlite+aiosqlite://",
        fetch_retries=1,
        validate_public_source_ips=False,
    )
    pipeline = Pipeline(
        sessions,
        SourceFetcher(settings, httpx.AsyncClient(transport=transport())),
        FailsOnceLLM(),
        settings,
    )
    first = await pipeline.run_cycle()
    assert first.errors == 1
    second = await pipeline.run_cycle()
    assert second.drafts_created == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count(Article.id))) == 2
        assert await session.scalar(select(func.count(Draft.id))) == 1


@pytest.mark.asyncio
async def test_unsupported_claim_blocks_publication(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await add_sources(sessions, official=True)
    settings = Settings(
        database_url="sqlite+aiosqlite://",
        fetch_retries=1,
        validate_public_source_ips=False,
    )
    pipeline = Pipeline(
        sessions,
        SourceFetcher(settings, httpx.AsyncClient(transport=transport())),
        HallucinatingLLM(),
        settings,
    )
    await pipeline.run_cycle()
    async with sessions() as session:
        assert (
            await session.scalar(select(func.count(Claim.id)).where(Claim.verified.is_(False))) == 2
        )
        assert await session.scalar(select(func.count(PublicationJob.id))) == 0


@pytest.mark.asyncio
async def test_stale_article_is_saved_but_not_clustered(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with sessions.begin() as session:
        source = Source(
            name="official",
            base_url="https://official.example",
            kind=SourceKind.RSS,
            feed_url="https://official.example/rss",
            trust_level=1,
            is_official=True,
        )
        session.add(source)
        await session.flush()
        source_id = source.id
    settings = Settings(validate_public_source_ips=False, max_article_age_hours=24)
    pipeline = Pipeline(
        sessions,
        SourceFetcher(settings, httpx.AsyncClient(transport=transport())),
        DeterministicLLM(),
        settings,
    )
    await pipeline._ingest(
        source_id,
        CandidateArticle(
            url="https://official.example/old",
            title="Старая новость",
            text="Событие произошло давно.",
            published_at=datetime(2020, 1, 1, tzinfo=UTC),
            raw_content="<p>Событие произошло давно.</p>",
            content_type="text/html",
        ),
    )
    async with sessions() as session:
        article = await session.scalar(select(Article))
        assert article and article.rejected_reason == "article is outside freshness window"
        assert await session.scalar(select(func.count(Event.id))) == 0

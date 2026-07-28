import asyncio
from dataclasses import asdict, dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from newsbot.db_models import Source, SourceFetch, utcnow
from newsbot.fetcher import SourceFetcher
from newsbot.schemas import SourceHealth


@dataclass(slots=True)
class SourceCheckResult:
    name: str
    found: int
    loaded: int
    titles: int
    dates: int
    texts: int
    images: int
    errors: int
    success_rate: float
    state: SourceHealth
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def classify_source_health(
    found: int, loaded: int, errors: int, titles: int, dates: int, texts: int
) -> SourceHealth:
    if found and not loaded:
        return SourceHealth.UNAVAILABLE
    if not found or errors or loaded / found < 0.9 or min(titles, dates, texts) < loaded:
        return SourceHealth.DEGRADED
    return SourceHealth.HEALTHY


async def _check_source(
    sessions: async_sessionmaker[AsyncSession],
    fetcher: SourceFetcher,
    source: Source,
) -> SourceCheckResult:
    try:
        async with asyncio.timeout(max(30, fetcher.settings.fetch_timeout_seconds * 2)):
            fetched = await fetcher.fetch(source)
    except Exception as error:
        reason = type(error).__name__
        async with sessions.begin() as session:
            stored = await session.get(Source, source.id)
            if stored:
                fetcher.apply_failure(stored)
                stored.health_state = SourceHealth.UNAVAILABLE
                stored.last_checked_at = utcnow()
                stored.last_error = reason
            session.add(
                SourceFetch(
                    source_id=source.id,
                    finished_at=utcnow(),
                    error=reason,
                    diagnostics=[reason],
                )
            )
        return SourceCheckResult(
            name=source.name,
            found=0,
            loaded=0,
            titles=0,
            dates=0,
            texts=0,
            images=0,
            errors=1,
            success_rate=0,
            state=SourceHealth.UNAVAILABLE,
            reasons=[reason],
        )

    loaded = len(fetched.articles)
    rate = loaded / fetched.discovered_count if fetched.discovered_count else 1.0
    titles = sum(bool(article.title) for article in fetched.articles)
    dates = sum(article.published_at is not None for article in fetched.articles)
    texts = sum(bool(article.text) for article in fetched.articles)
    images = sum(article.image_url is not None for article in fetched.articles)
    state = classify_source_health(
        fetched.discovered_count,
        loaded,
        fetched.article_errors,
        titles,
        dates,
        texts,
    )
    reasons = list(fetched.error_reasons)
    async with sessions.begin() as session:
        stored = await session.get(Source, source.id)
        if stored:
            stored.health_state = state
            stored.last_checked_at = utcnow()
            stored.last_error = ", ".join(reasons) or None
            if state == SourceHealth.HEALTHY:
                stored.last_success_at = utcnow()
                stored.consecutive_failures = 0
                stored.disabled_until = None
        session.add(
            SourceFetch(
                source_id=source.id,
                finished_at=utcnow(),
                status_code=fetched.status_code,
                item_count=fetched.discovered_count,
                loaded_count=loaded,
                extraction_success_rate=rate,
                diagnostics=reasons,
                error=", ".join(reasons) or None,
            )
        )
    return SourceCheckResult(
        name=source.name,
        found=fetched.discovered_count,
        loaded=loaded,
        titles=titles,
        dates=dates,
        texts=texts,
        images=images,
        errors=fetched.article_errors,
        success_rate=round(rate * 100, 1),
        state=state,
        reasons=reasons,
    )


async def check_sources(
    sessions: async_sessionmaker[AsyncSession], fetcher: SourceFetcher
) -> list[SourceCheckResult]:
    async with sessions() as session:
        sources = list(
            (
                await session.scalars(
                    select(Source).where(Source.enabled.is_(True)).order_by(Source.name)
                )
            ).all()
        )
    return list(
        await asyncio.gather(
            *(_check_source(sessions, fetcher, source) for source in sources)
        )
    )

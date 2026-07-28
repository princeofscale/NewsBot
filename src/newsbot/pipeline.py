import asyncio
import hashlib
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import monotonic
from typing import cast
from urllib.parse import urlsplit
from uuid import UUID
from weakref import WeakKeyDictionary

import structlog
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from newsbot.config import Settings
from newsbot.db_models import (
    Article,
    Claim,
    Draft,
    Event,
    EventArticle,
    PlatformPublication,
    PublicationJob,
    RawArticle,
    RuntimeControl,
    Source,
    SourceFetch,
    as_utc,
    utcnow,
)
from newsbot.dedupe import event_similarity, extract_addresses, extract_entities
from newsbot.fetcher import FetchResult, SourceFetcher
from newsbot.formatters import format_max, format_telegram
from newsbot.metrics import (
    EVENT_MERGES,
    EVENTS,
    FETCHED,
    OUTBOX,
    PROCESSED,
    PUBLISH_ERRORS,
    RISK_BLOCKED,
    SOURCE_ERRORS,
    STAGE_SECONDS,
)
from newsbot.parsing import text_hash
from newsbot.schemas import (
    ArticleState,
    CandidateArticle,
    ClaimPayload,
    EventState,
    JobOperation,
    JobState,
    LLMPort,
    Platform,
    Post,
    PublicationFinder,
    Publisher,
    SourceHealth,
)
from newsbot.source_check import classify_source_health

log = structlog.get_logger()
RISK_TERMS = {
    "обвин",
    "арест",
    "авари",
    "дтп",
    "преступ",
    "пропал",
    "разыски",
    "смерт",
    "убий",
    "пострад",
    "погиб",
    "медицин",
    "выбор",
    "чрезвычайн",
    "персональн",
}
SENSITIVE_DATA_RE = re.compile(
    r"(?:\b\d{3}-\d{3}-\d{3}\s?\d{2}\b|"
    r"\b(?:паспорт|снилс)\s*[:№]?\s*[\d -]{6,}\b|"
    r"\b(?:\+7|8)[\s()-]*\d{3}[\s()-]*\d{3}[\s-]*\d{2}[\s-]*\d{2}\b|"
    r"\b[\w.+-]+@[\w.-]+\.[a-zа-я]{2,}\b)",
    re.IGNORECASE,
)
CLICKBAIT_TERMS = {"шок", "сенсаци", "вы не поверите", "срочно!!!"}
CYCLE_LOCK_ID = 0x4E455753424F54
_PROCESS_LOCKS: WeakKeyDictionary[AsyncEngine, asyncio.Lock] = WeakKeyDictionary()
_PLATFORM_LOCKS: WeakKeyDictionary[AsyncEngine, dict[Platform, asyncio.Lock]] = (
    WeakKeyDictionary()
)


class CycleAlreadyRunning(RuntimeError):
    pass


@dataclass(slots=True)
class CycleResult:
    fetched: int = 0
    new_articles: int = 0
    events_created: int = 0
    drafts_created: int = 0
    errors: int = 0


class Pipeline:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        fetcher: SourceFetcher,
        llm: LLMPort,
        settings: Settings,
    ) -> None:
        self.sessions = sessions
        self.fetcher = fetcher
        self.llm = llm
        self.settings = settings

    async def close(self) -> None:
        await self.fetcher.close()
        close = getattr(self.llm, "close", None)
        if close:
            await close()

    async def run_cycle(self) -> CycleResult:
        engine = cast(AsyncEngine, self.sessions.kw["bind"])
        process_lock = _PROCESS_LOCKS.setdefault(engine, asyncio.Lock())
        if process_lock.locked():
            raise CycleAlreadyRunning("cycle_already_running")
        async with process_lock, asyncio.timeout(self.settings.cycle_timeout_seconds):
            if engine.dialect.name != "postgresql":
                return await self._run_cycle_unlocked()
            async with engine.connect() as connection:
                acquired = await connection.scalar(
                    text("SELECT pg_try_advisory_lock(:lock_id)"),
                    {"lock_id": CYCLE_LOCK_ID},
                )
                if not acquired:
                    raise CycleAlreadyRunning("cycle_already_running")
                try:
                    return await self._run_cycle_unlocked()
                finally:
                    await asyncio.shield(
                        connection.execute(
                            text("SELECT pg_advisory_unlock(:lock_id)"),
                            {"lock_id": CYCLE_LOCK_ID},
                        )
                    )

    async def _run_cycle_unlocked(self) -> CycleResult:
        started = monotonic()
        result = CycleResult()
        await self._resume_pending_articles(result)
        async with self.sessions() as session:
            sources = list(
                (
                    await session.scalars(
                        select(Source).where(Source.enabled.is_(True)).order_by(Source.name)
                    )
                ).all()
            )
        now = utcnow()
        due = [
            source
            for source in sources
            if (not source.last_fetched_at)
            or as_utc(source.last_fetched_at) + timedelta(seconds=source.min_interval_seconds)
            <= now
            if not source.disabled_until or as_utc(source.disabled_until) <= now
        ]
        discovered = await asyncio.gather(
            *(self.fetcher.discover(source) for source in due), return_exceptions=True
        )
        outcomes = await asyncio.gather(
            *(
                self._hydrate_discovery(source, discovery)
                for source, discovery in zip(due, discovered, strict=True)
            )
        )
        for source, outcome in zip(due, outcomes, strict=True):
            if isinstance(outcome, BaseException):
                result.errors += 1
                await self._record_failure(source.id, outcome)
                continue
            result.fetched += len(outcome.articles)
            result.errors += outcome.article_errors
            FETCHED.labels(source=source.name).inc(len(outcome.articles))
            try:
                await self._record_success(source.id, outcome, result)
            except Exception as error:
                result.errors += 1
                await log.aerror(
                    "source_processing_failed",
                    source_id=str(source.id),
                    error=type(error).__name__,
                )
        STAGE_SECONDS.labels(stage="cycle").observe(monotonic() - started)
        await self.expire_old_events()
        return result

    async def _hydrate_discovery(
        self, source: Source, discovery: FetchResult | BaseException
    ) -> FetchResult | BaseException:
        if isinstance(discovery, BaseException):
            return discovery
        try:
            candidates = await self._candidates_to_fetch(source.id, discovery.articles)
            hydrated = await self.fetcher.hydrate(source, candidates)
        except Exception as error:
            return error
        return FetchResult(
            articles=hydrated.articles,
            status_code=discovery.status_code,
            etag=discovery.etag,
            last_modified=discovery.last_modified,
            article_errors=discovery.article_errors + hydrated.article_errors,
            discovered_count=discovery.discovered_count,
            attempted_count=hydrated.attempted_count,
            error_reasons=discovery.error_reasons + hydrated.error_reasons,
        )

    async def _resume_pending_articles(self, result: CycleResult) -> None:
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(Article, EventArticle.event_id)
                    .join(EventArticle, EventArticle.article_id == Article.id)
                    .where(
                        or_(
                            Article.processing_state == ArticleState.NEW,
                            (
                                (Article.processing_state == ArticleState.FAILED)
                                & or_(
                                    Article.next_processing_at.is_(None),
                                    Article.next_processing_at <= utcnow(),
                                )
                            ),
                        ),
                        Article.processing_attempts
                        < self.settings.article_processing_max_attempts,
                    )
                    .order_by(Article.created_at)
                    .limit(100)
                )
            ).all()
        for article, event_id in rows:
            try:
                result.drafts_created += await self._finish_article(
                    article.id, event_id, article.title, article.text
                )
            except Exception:
                result.errors += 1
                await self._mark_article_failure(article.id, event_id)
                continue
            async with self.sessions.begin() as session:
                stored = await session.get(Article, article.id)
                if stored:
                    stored.processing_state = ArticleState.PROCESSED
                    stored.next_processing_at = None

    async def _mark_article_failure(self, article_id: UUID, event_id: UUID) -> None:
        async with self.sessions.begin() as session:
            article = await session.get(Article, article_id)
            if not article:
                return
            article.processing_attempts += 1
            if article.processing_attempts >= self.settings.article_processing_max_attempts:
                article.processing_state = ArticleState.PROCESSED
                article.rejected_reason = "article processing retry limit reached"
                event = await session.get(Event, event_id)
                if event:
                    event.state = EventState.COLLECTING
                    event.requires_manual_review = True
                    event.decision_reason = article.rejected_reason
                return
            article.processing_state = ArticleState.FAILED
            delay = self.settings.article_retry_base_seconds * 2 ** (
                article.processing_attempts - 1
            )
            article.next_processing_at = utcnow() + timedelta(
                seconds=delay + random.uniform(0, delay * 0.2)
            )

    async def _candidates_to_fetch(
        self, source_id: UUID, candidates: list[CandidateArticle]
    ) -> list[CandidateArticle]:
        if not candidates:
            return []
        cutoff = utcnow() - timedelta(hours=self.settings.max_revision_recheck_age_hours)
        urls = [candidate.url for candidate in candidates]
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(
                        Article.canonical_url,
                        func.max(Article.published_at),
                        func.max(Article.created_at),
                    )
                    .where(Article.source_id == source_id, Article.canonical_url.in_(urls))
                    .group_by(Article.canonical_url)
                )
            ).all()
        known = {
            url: as_utc(published_at or created_at)
            for url, published_at, created_at in rows
            if published_at or created_at
        }
        return [
            candidate
            for candidate in candidates
            if candidate.url not in known or known[candidate.url] >= cutoff
        ]

    async def _record_failure(self, source_id: UUID, error: BaseException) -> None:
        async with self.sessions.begin() as session:
            source = await session.get(Source, source_id)
            if not source:
                return
            self.fetcher.apply_failure(source)
            source.last_checked_at = utcnow()
            source.last_error = type(error).__name__
            source.health_state = SourceHealth.UNAVAILABLE
            session.add(
                SourceFetch(
                    source_id=source_id,
                    finished_at=utcnow(),
                    error=type(error).__name__,
                )
            )
        await log.awarning(
            "source_fetch_failed", source_id=str(source_id), error=type(error).__name__
        )
        SOURCE_ERRORS.labels(source=str(source_id), stage="fetch").inc()

    async def _record_success(
        self, source_id: UUID, outcome: FetchResult, result: CycleResult
    ) -> None:
        async with self.sessions.begin() as session:
            fetch = SourceFetch(
                source_id=source_id,
                finished_at=utcnow(),
                status_code=outcome.status_code,
                item_count=outcome.discovered_count,
                loaded_count=len(outcome.articles),
                extraction_success_rate=(
                    len(outcome.articles) / outcome.discovered_count
                    if outcome.discovered_count
                    else 1
                ),
                diagnostics=list(outcome.error_reasons),
                error=(
                    f"{outcome.article_errors} article page(s) failed"
                    if outcome.article_errors
                    else None
                ),
            )
            session.add(fetch)
            await session.flush()
            fetch_id = fetch.id
        failures = 0
        for candidate in outcome.articles:
            try:
                created, event_created, draft_created = await self._ingest(source_id, candidate)
            except Exception as error:
                failures += 1
                result.errors += 1
                await log.aerror(
                    "article_processing_failed",
                    source_id=str(source_id),
                    url=candidate.url,
                    error=type(error).__name__,
                )
                continue
            result.new_articles += created
            result.events_created += event_created
            result.drafts_created += draft_created
        async with self.sessions.begin() as session:
            source = await session.get(Source, source_id)
            stored_fetch = await session.get(SourceFetch, fetch_id)
            if stored_fetch and failures:
                stored_fetch.error = " ".join(
                    filter(
                        None,
                        [
                            stored_fetch.error,
                            f"{failures} article(s) failed processing",
                        ],
                    )
                )
            if source:
                source.etag = outcome.etag
                source.last_modified = outcome.last_modified
                source.last_fetched_at = utcnow()
                source.last_checked_at = utcnow()
                total_failures = failures + outcome.article_errors
                health = (
                    SourceHealth.HEALTHY
                    if outcome.status_code == 304 or not outcome.attempted_count
                    else classify_source_health(
                        outcome.attempted_count,
                        len(outcome.articles),
                        total_failures,
                        sum(bool(article.title) for article in outcome.articles),
                        sum(article.published_at is not None for article in outcome.articles),
                        sum(bool(article.text) for article in outcome.articles),
                    )
                )
                source.health_state = health
                if health != SourceHealth.HEALTHY:
                    source.last_error = (
                        stored_fetch.error
                        if stored_fetch and stored_fetch.error
                        else f"source health {health}"
                    )
                    self.fetcher.apply_failure(source)
                else:
                    source.health_state = SourceHealth.HEALTHY
                    if outcome.articles:
                        source.last_success_at = utcnow()
                    source.last_error = None
                    source.consecutive_failures = 0
                    source.disabled_until = None

    async def _ingest(self, source_id: UUID, candidate: CandidateArticle) -> tuple[int, int, int]:
        content_hash = text_hash(candidate.text)
        async with self.sessions.begin() as session:
            existing = await session.scalar(
                select(Article)
                .where(
                    Article.source_id == source_id,
                    Article.canonical_url == candidate.url,
                    Article.content_hash == content_hash,
                )
                .order_by(Article.created_at.desc())
            )
            event_id = (
                await session.scalar(
                    select(EventArticle.event_id).where(EventArticle.article_id == existing.id)
                )
                if existing
                else None
            )
            enriched = False
            if existing and not existing.published_at and candidate.published_at:
                existing.published_at = candidate.published_at
                enriched = as_utc(candidate.published_at) >= utcnow() - timedelta(
                    hours=self.settings.max_article_age_hours
                )
                if enriched and event_id:
                    event = await session.get(Event, event_id)
                    if event and not event.event_time:
                        event.event_time = candidate.published_at
            if existing and not existing.image_url and candidate.image_url:
                existing.image_url = candidate.image_url
            if existing and (existing.processing_state != ArticleState.PROCESSED or enriched):
                existing.processing_state = ArticleState.NEW
                existing_id = existing.id
            else:
                existing_id = None
            if not existing:
                event_id = None
            elif existing_id and not event_id:
                existing_id = None
            previous = await session.scalar(
                select(Article)
                .where(
                    Article.source_id == source_id,
                    Article.canonical_url == candidate.url,
                )
                .order_by(Article.created_at.desc())
                .limit(1)
            )
            previous_id = previous.id if previous and previous.id != existing_id else None
            previous_event_id = (
                await session.scalar(
                    select(EventArticle.event_id).where(EventArticle.article_id == previous_id)
                )
                if previous_id
                else None
            )
        if existing_id and event_id:
            assert existing is not None
            try:
                return (
                    0,
                    0,
                    await self._finish_article(
                        existing_id, event_id, existing.title, existing.text
                    ),
                )
            except Exception:
                await self._mark_article_failure(existing_id, event_id)
                raise
        if existing:
            return 0, 0, 0

        async with self.sessions.begin() as session:
            raw_payload = candidate.raw_content
            raw = RawArticle(
                source_id=source_id,
                canonical_url=candidate.url,
                content_type=candidate.content_type,
                payload=raw_payload,
                payload_hash=hashlib.sha256(raw_payload).hexdigest(),
            )
            session.add(raw)
            await session.flush()
            duplicate = await session.scalar(
                select(Article.id).where(Article.content_hash == content_hash).limit(1)
            )
            stale = bool(
                candidate.published_at
                and as_utc(candidate.published_at)
                < utcnow() - timedelta(hours=self.settings.max_article_age_hours)
            )
            article = Article(
                raw_article_id=raw.id,
                source_id=source_id,
                revision_of_id=previous_id,
                canonical_url=candidate.url,
                title=candidate.title,
                text=candidate.text,
                content_hash=content_hash,
                published_at=candidate.published_at,
                image_url=candidate.image_url,
                rejected_reason=(
                    "exact content duplicate"
                    if duplicate
                    else "article is outside freshness window"
                    if stale
                    else None
                ),
                processing_state=(
                    ArticleState.PROCESSED if duplicate or stale else ArticleState.NEW
                ),
            )
            session.add(article)
            await session.flush()
            article_id = article.id
            PROCESSED.inc()
            if duplicate or stale:
                return 1, 0, 0

            event = await session.get(Event, previous_event_id) if previous_event_id else None
            similarity = 1.0 if event else 0.0
            if not event:
                event, similarity = await self._find_event(session, article)
            event_created = 0
            if not event:
                event = Event(
                    title=article.title,
                    event_time=article.published_at,
                    expires_at=utcnow() + timedelta(hours=self.settings.collecting_ttl_hours),
                )
                session.add(event)
                await session.flush()
                event_created = 1
                EVENTS.inc()
            else:
                event.manually_approved = False
                EVENT_MERGES.inc()
            event_id = event.id
            session.add(
                EventArticle(event_id=event_id, article_id=article_id, similarity=similarity)
            )

        try:
            draft_created = await self._finish_article(
                article_id, event_id, candidate.title, candidate.text
            )
        except Exception:
            await self._mark_article_failure(article_id, event_id)
            raise
        return 1, event_created, draft_created

    async def _finish_article(self, article_id: UUID, event_id: UUID, title: str, text: str) -> int:
        async with self.sessions() as session:
            existing_claims = list(
                (
                    await session.scalars(
                        select(Claim).where(Claim.source_article_id == article_id)
                    )
                ).all()
            )
        extracted = (
            [ClaimPayload.model_validate(item, from_attributes=True) for item in existing_claims]
            if existing_claims
            else await self.llm.extract_claims(article_id, title, text)
        )
        async with self.sessions.begin() as session:
            event = await session.get(Event, event_id, with_for_update=True)
            if not event:
                return 0
            if not existing_claims:
                for item in extracted:
                    article = await session.get(Article, article_id)
                    supported = self._claim_supported(
                        item,
                        title,
                        text,
                        article.published_at if article else None,
                    )
                    session.add(
                        Claim(
                            event_id=event_id,
                            source_article_id=article_id,
                            subject=item.subject,
                            predicate=item.predicate,
                            location=item.location,
                            event_time=item.event_time,
                            numbers=item.numbers,
                            names=item.names,
                            claim=item.claim,
                            verified=supported,
                            verification_reason="claim grounded in source"
                            if supported
                            else "claim not grounded in source",
                        )
                    )
            await session.flush()
            draft_input = await self._prepare_draft(session, event)

        if not draft_input:
            async with self.sessions.begin() as session:
                article = await session.get(Article, article_id)
                if article:
                    article.processing_state = ArticleState.PROCESSED
            return 0
        claims, urls, confidence = draft_input
        generated = await self.llm.create_post(event_id, claims, urls, confidence)
        post = generated.model_copy(
            update={
                "event_id": event_id,
                "source_urls": urls[:3],
                "confidence": confidence,
            }
        )
        valid = (
            len(format_telegram(post)) <= 4000
            and len(format_max(post)) <= 4000
            and len(post.title) <= 120
            and not post.title.rstrip().endswith((".", "!", "?"))
            and not any(term in post.title.casefold() for term in CLICKBAIT_TERMS)
            and self._post_supported(post, claims, urls)
            and await self.llm.verify_post(post, claims)
        )
        async with self.sessions.begin() as session:
            event = await session.get(Event, event_id)
            article = await session.get(Article, article_id)
            if not event or not article:
                return 0
            latest = await session.scalar(
                select(Draft)
                .where(Draft.event_id == event_id)
                .order_by(Draft.version.desc())
                .limit(1)
            )
            if latest and (latest.title, latest.body, latest.source_urls) == (
                post.title,
                post.body,
                post.source_urls,
            ):
                article.processing_state = ArticleState.PROCESSED
                return 0
            if latest and latest.version >= self.settings.max_event_updates + 1:
                event.state = EventState.COLLECTING
                event.requires_manual_review = True
                event.decision_reason = "automatic event update limit reached"
                article.processing_state = ArticleState.PROCESSED
                return 0
            draft = Draft(
                event_id=event_id,
                title=post.title,
                body=post.body,
                source_urls=post.source_urls,
                confidence=post.confidence,
                validated=valid,
                validation_reason="deterministic grounding and constrained checks passed"
                if valid
                else "draft contains unsupported data",
                version=(latest.version + 1) if latest else 1,
            )
            session.add(draft)
            await session.flush()
            await session.execute(
                update(PublicationJob)
                .where(
                    PublicationJob.draft_id.in_(
                        select(Draft.id).where(
                            Draft.event_id == event_id,
                            Draft.id != draft.id,
                        )
                    ),
                    PublicationJob.state.in_([JobState.PENDING, JobState.RETRY]),
                )
                .values(
                    state=JobState.CANCELLED,
                    last_error="superseded by a newer draft",
                )
            )
            if not valid:
                event.state = EventState.COLLECTING
                event.requires_manual_review = True
                event.decision_reason = "LLM draft failed deterministic grounding"
                article.processing_state = ArticleState.PROCESSED
                return 1
            published_platforms = set(
                (
                    await session.scalars(
                        select(PlatformPublication.platform).where(
                            PlatformPublication.event_id == event_id,
                            PlatformPublication.deleted_at.is_(None),
                        )
                    )
                ).all()
            )
            has_publications = bool(published_platforms)
            event.state = EventState.PUBLISHED if has_publications else EventState.READY
            session.add_all(
                [
                    PublicationJob(
                        draft_id=draft.id,
                        platform=platform,
                        operation=(
                            JobOperation.EDIT
                            if platform == Platform.TELEGRAM
                            and Platform.TELEGRAM in published_platforms
                            else JobOperation.PUBLISH
                        ),
                    )
                    for platform in (Platform.TELEGRAM, Platform.MAX)
                ]
            )
            article.processing_state = ArticleState.PROCESSED
        return 1

    async def _find_event(
        self, session: AsyncSession, article: Article
    ) -> tuple[Event | None, float]:
        fetched_rows = (
            await session.execute(
                select(Event, Article)
                .join(EventArticle, EventArticle.event_id == Event.id)
                .join(Article, Article.id == EventArticle.article_id)
                .where(
                    Event.state.in_(
                        [
                            EventState.COLLECTING,
                            EventState.READY,
                            EventState.PUBLISHED,
                            EventState.UPDATED,
                        ]
                    ),
                    Event.created_at
                    >= utcnow() - timedelta(hours=self.settings.event_match_window_hours),
                )
                .order_by(Event.created_at.desc())
                .limit(200)
            )
        ).all()
        best: tuple[Event | None, float] = (None, 0.0)
        for event, representative in fetched_rows:
            score = event_similarity(
                article.title,
                representative.title,
                left_text=article.text,
                right_text=representative.text,
                left_time=as_utc(article.published_at) if article.published_at else None,
                right_time=(
                    as_utc(representative.published_at) if representative.published_at else None
                ),
            )
            if score > best[1]:
                best = event, score
        return best

    @staticmethod
    def _claim_supported(
        claim: ClaimPayload,
        title: str,
        source_text: str,
        published_at: datetime | None = None,
    ) -> bool:
        corpus = f"{title} {source_text}".casefold()
        values = claim.numbers + claim.names + claim.location
        claim_tokens = set(re.findall(r"\w{3,}", f"{claim.predicate} {claim.claim}".casefold()))
        time_supported = not claim.event_time or (
            published_at is not None
            and abs(as_utc(claim.event_time) - as_utc(published_at)) <= timedelta(hours=24)
        )
        covered = sum(token in corpus for token in claim_tokens)
        return (
            bool(claim_tokens)
            and covered / len(claim_tokens) >= 0.65
            and all(value.casefold() in corpus for value in values)
            and time_supported
        )

    @staticmethod
    def _post_supported(post: Post, claims: list[ClaimPayload], source_urls: list[str]) -> bool:
        corpus_text = " ".join(
            " ".join(
                [
                    claim.subject,
                    claim.predicate,
                    claim.claim,
                    *claim.location,
                    *claim.names,
                    *claim.numbers,
                ]
            )
            for claim in claims
        )
        corpus = corpus_text.casefold()
        tokens = set(re.findall(r"\w{3,}", f"{post.title} {post.body}".casefold()))
        numbers = re.findall(r"\b\d+(?:[.,]\d+)?\b", f"{post.title} {post.body}")
        post_text = f"{post.title} {post.body}"
        entities = extract_entities(post_text)
        addresses = extract_addresses(post_text)
        quotes = re.findall(r"[«\"]([^»\"]{3,})[»\"]", post_text)
        return (
            post.source_urls == source_urls[:3]
            and bool(tokens)
            and sum(token in corpus for token in tokens) / len(tokens) >= 0.65
            and all(number in corpus for number in numbers)
            and all(entity in corpus for entity in entities)
            and all(address in extract_addresses(corpus_text) for address in addresses)
            and all(quote.casefold() in corpus for quote in quotes)
        )

    async def _prepare_draft(
        self, session: AsyncSession, event: Event
    ) -> tuple[list[ClaimPayload], list[str], float] | None:
        fetched_rows = (
            await session.execute(
                select(Article, Source)
                .join(EventArticle, EventArticle.article_id == Article.id)
                .join(Source, Source.id == Article.source_id)
                .where(EventArticle.event_id == event.id)
            )
        ).all()
        latest: dict[tuple[UUID, str], tuple[Article, Source]] = {}
        for article, source in fetched_rows:
            key = (article.source_id, article.canonical_url)
            current = latest.get(key)
            if not current or article.created_at > current[0].created_at:
                latest[key] = (article, source)
        rows = list(latest.values())
        article_ids = [article.id for article, _ in rows]
        unverified = await session.scalar(
            select(func.count(Claim.id)).where(
                Claim.source_article_id.in_(article_ids),
                Claim.verified.is_(False),
            )
        )
        if unverified:
            event.state = EventState.COLLECTING
            event.decision_reason = "important claim values are not present in source text"
            event.requires_manual_review = True
            return None
        verified_article_ids = set(
            (
                await session.scalars(
                    select(Claim.source_article_id)
                    .where(
                        Claim.source_article_id.in_(article_ids),
                        Claim.verified.is_(True),
                    )
                    .distinct()
                )
            ).all()
        )
        rows = [(article, source) for article, source in rows if article.id in verified_article_ids]
        dated = [article.published_at for article, _ in rows if article.published_at]
        if not dated:
            event.state = EventState.COLLECTING
            event.decision_reason = "source publication time is unknown"
            return None
        sources = {
            source.origin_group or urlsplit(source.base_url).netloc: source for _, source in rows
        }
        official = any(
            source.is_official and source.trust_level >= 0.8 for source in sources.values()
        )
        reliable = [source for source in sources.values() if source.trust_level >= 0.6]
        risky = any(
            term in f"{article.title} {article.text}".casefold()
            for article, _ in rows
            for term in RISK_TERMS
        )
        sensitive = any(
            SENSITIVE_DATA_RE.search(f"{article.title} {article.text}") for article, _ in rows
        )
        if sensitive:
            event.state = EventState.COLLECTING
            event.decision_reason = "sensitive personal data requires manual review"
            event.requires_manual_review = True
            RISK_BLOCKED.inc()
            return None
        if risky and not event.manually_approved and not (official and len(reliable) >= 2):
            event.state = EventState.COLLECTING
            event.decision_reason = "risk topic requires official and independent confirmation"
            event.requires_manual_review = True
            RISK_BLOCKED.inc()
            return None
        if official:
            event.confidence = max(source.trust_level for source in sources.values())
            event.decision_reason = "high-trust official source"
        elif len(reliable) >= 2:
            event.confidence = min(
                0.95, sum(source.trust_level for source in reliable) / len(reliable)
            )
            event.decision_reason = "confirmed by two independent reliable source domains"
        else:
            event.state = EventState.COLLECTING
            event.decision_reason = "awaiting an official or second independent source"
            return None

        article_ids = [article.id for article, _ in rows]
        claims = [
            ClaimPayload.model_validate(item, from_attributes=True)
            for item in (
                await session.scalars(select(Claim).where(Claim.source_article_id.in_(article_ids)))
            ).all()
        ]
        if self._claims_conflict(claims):
            event.state = EventState.COLLECTING
            event.decision_reason = "independent source claims conflict"
            event.requires_manual_review = True
            return None
        event.requires_manual_review = False
        urls = [article.canonical_url for article, _ in rows]
        return claims, urls, event.confidence

    @staticmethod
    def _claims_conflict(claims: list[ClaimPayload]) -> bool:
        for index, left in enumerate(claims):
            left_tokens = set(re.findall(r"\w{4,}", left.predicate.casefold()))
            for right in claims[index + 1 :]:
                if left.source_article_id == right.source_article_id:
                    continue
                right_tokens = set(re.findall(r"\w{4,}", right.predicate.casefold()))
                overlap = left_tokens & right_tokens
                if not overlap or len(overlap) / min(len(left_tokens), len(right_tokens)) < 0.5:
                    continue
                if left.numbers and right.numbers and set(left.numbers).isdisjoint(right.numbers):
                    return True
                if (
                    left.event_time
                    and right.event_time
                    and abs(as_utc(left.event_time) - as_utc(right.event_time)) > timedelta(hours=1)
                ):
                    return True
                if (
                    left.location
                    and right.location
                    and set(map(str.casefold, left.location)).isdisjoint(
                        map(str.casefold, right.location)
                    )
                ):
                    return True
        return False

    async def expire_old_events(self) -> None:
        async with self.sessions.begin() as session:
            await session.execute(
                update(Event)
                .where(
                    Event.state == EventState.COLLECTING,
                    Event.expires_at < utcnow(),
                )
                .values(
                    state=EventState.REJECTED,
                    decision_reason="collecting TTL expired",
                )
            )

    async def reprocess_event(self, event_id: UUID) -> int:
        async with self.sessions() as session:
            article = await session.scalar(
                select(Article)
                .join(EventArticle, EventArticle.article_id == Article.id)
                .where(EventArticle.event_id == event_id)
                .order_by(Article.created_at.desc())
                .limit(1)
            )
        if not article:
            raise ValueError("event has no articles")
        return await self._finish_article(article.id, event_id, article.title, article.text)


def _post_from_draft(draft: Draft) -> Post:
    return Post.model_validate(draft, from_attributes=True)


class PublicationWorker:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        publishers: dict[Platform, Publisher],
        max_attempts: int = 5,
        batch_size: int = 100,
        retry_base_seconds: int = 30,
        sending_stale_seconds: int = 300,
        publish_timeout_seconds: float = 45,
        publication_min_interval_seconds: int = 0,
        reconciliation_absence_delay_seconds: int = 300,
    ) -> None:
        self.sessions = sessions
        self.publishers = publishers
        self.max_attempts = max_attempts
        self.batch_size = batch_size
        self.retry_base_seconds = retry_base_seconds
        self.sending_stale_seconds = sending_stale_seconds
        self.publish_timeout_seconds = publish_timeout_seconds
        self.publication_min_interval_seconds = publication_min_interval_seconds
        self.reconciliation_absence_delay_seconds = reconciliation_absence_delay_seconds

    async def run_once(self) -> int:
        now = utcnow()
        async with self.sessions() as session:
            control = await session.get(RuntimeControl, 1)
            if control and not control.publication_enabled:
                return 0
            enabled_platforms = [
                platform
                for platform, enabled in (
                    (Platform.TELEGRAM, not control or control.telegram_enabled),
                    (Platform.MAX, not control or control.max_enabled),
                )
                if enabled
            ]
        async with self.sessions.begin() as session:
            await session.execute(
                update(PublicationJob)
                .where(
                    PublicationJob.state == JobState.SENDING,
                    PublicationJob.updated_at < now - timedelta(seconds=self.sending_stale_seconds),
                )
                .values(
                    state=JobState.UNCERTAIN,
                    last_error="worker stopped during send; reconciliation required",
                )
            )
        async with self.sessions() as session:
            outbox_counts: dict[JobState, int] = {
                state: count
                for state, count in (
                    await session.execute(
                        select(PublicationJob.state, func.count(PublicationJob.id)).group_by(
                            PublicationJob.state
                        )
                    )
                ).all()
            }
            for state in JobState:
                OUTBOX.labels(state=state.value).set(outbox_counts.get(state, 0))
            job_ids = list(
                (
                    await session.scalars(
                        select(PublicationJob.id)
                        .where(
                            PublicationJob.state.in_([JobState.PENDING, JobState.RETRY]),
                            PublicationJob.platform.in_(enabled_platforms),
                            or_(
                                PublicationJob.next_attempt_at.is_(None),
                                PublicationJob.next_attempt_at <= now,
                            ),
                        )
                        .order_by(PublicationJob.created_at)
                        .limit(self.batch_size)
                    )
                ).all()
            )
        for job_id in job_ids:
            await self._publish(job_id)
        return len(job_ids)

    async def reconcile_uncertain(self) -> dict[str, int]:
        counts = {"published": 0, "retry": 0, "unsupported": 0, "deferred": 0}
        async with self.sessions() as session:
            job_ids = list(
                (
                    await session.scalars(
                        select(PublicationJob.id)
                        .where(PublicationJob.state == JobState.UNCERTAIN)
                        .order_by(PublicationJob.created_at)
                        .limit(self.batch_size)
                    )
                ).all()
            )
        for job_id in job_ids:
            async with self.sessions() as session:
                job = await session.get(PublicationJob, job_id)
                draft = await session.get(Draft, job.draft_id) if job else None
            if not job or not draft:
                continue
            if as_utc(job.updated_at) > utcnow() - timedelta(
                seconds=self.reconciliation_absence_delay_seconds
            ):
                counts["deferred"] += 1
                continue
            publisher = self.publishers.get(job.platform)
            if not isinstance(publisher, PublicationFinder):
                counts["unsupported"] += 1
                continue
            try:
                async with asyncio.timeout(self.publish_timeout_seconds):
                    external_id = await publisher.find_publication(_post_from_draft(draft))
            except Exception:
                counts["deferred"] += 1
                continue
            if external_id:
                await self.resolve_published(job_id, external_id)
                counts["published"] += 1
            else:
                await self.resolve_retry(job_id)
                counts["retry"] += 1
        return counts

    async def resolve_published(self, job_id: UUID, external_id: str) -> None:
        async with self.sessions.begin() as session:
            job = await session.get(PublicationJob, job_id)
            if not job or job.state != JobState.UNCERTAIN:
                raise ValueError("job is not UNCERTAIN")
            draft = await session.get(Draft, job.draft_id)
            if not draft:
                raise ValueError("draft not found")
            publication = await session.scalar(
                select(PlatformPublication).where(PlatformPublication.job_id == job.id)
            )
            if job.operation == JobOperation.EDIT:
                publication = await session.scalar(
                    select(PlatformPublication)
                    .where(
                        PlatformPublication.event_id == draft.event_id,
                        PlatformPublication.platform == job.platform,
                        PlatformPublication.deleted_at.is_(None),
                    )
                    .order_by(PlatformPublication.published_at.desc())
                    .limit(1)
                )
                if not publication:
                    raise ValueError("existing publication not found")
                publication.updated_at = utcnow()
            elif not publication:
                session.add(
                    PlatformPublication(
                        job_id=job.id,
                        event_id=draft.event_id,
                        platform=job.platform,
                        external_id=external_id,
                        published_at=utcnow(),
                        metadata_json={"reconciled": True},
                    )
                )
            job.state = JobState.PUBLISHED
            job.next_attempt_at = None
            job.last_error = None
            remaining = await session.scalar(
                select(func.count(PublicationJob.id)).where(
                    PublicationJob.draft_id == draft.id,
                    PublicationJob.id != job.id,
                    PublicationJob.state != JobState.PUBLISHED,
                )
            )
            if not remaining:
                event = await session.get(Event, draft.event_id)
                if event:
                    event.state = (
                        EventState.UPDATED
                        if job.operation == JobOperation.EDIT
                        else EventState.PUBLISHED
                    )

    async def resolve_retry(self, job_id: UUID) -> None:
        async with self.sessions.begin() as session:
            job = await session.get(PublicationJob, job_id)
            if not job or job.state != JobState.UNCERTAIN:
                raise ValueError("job is not UNCERTAIN")
            job.state = JobState.RETRY
            job.next_attempt_at = utcnow()
            job.last_error = "reconciliation found no matching publication"

    async def _publish(self, job_id: UUID) -> None:
        async with self.sessions() as session:
            platform = await session.scalar(
                select(PublicationJob.platform).where(PublicationJob.id == job_id)
            )
        if not platform:
            return
        engine = cast(AsyncEngine, self.sessions.kw["bind"])
        platform_enum = Platform(platform)
        lock = _PLATFORM_LOCKS.setdefault(engine, {}).setdefault(
            platform_enum, asyncio.Lock()
        )
        async with lock:
            if engine.dialect.name != "postgresql":
                await self._publish_locked(job_id)
                return
            lock_id = CYCLE_LOCK_ID + 1 + list(Platform).index(platform_enum)
            async with engine.connect() as connection:
                acquired = await connection.scalar(
                    text("SELECT pg_try_advisory_lock(:lock_id)"),
                    {"lock_id": lock_id},
                )
                if not acquired:
                    return
                try:
                    await self._publish_locked(job_id)
                finally:
                    await asyncio.shield(
                        connection.execute(
                            text("SELECT pg_advisory_unlock(:lock_id)"),
                            {"lock_id": lock_id},
                        )
                    )

    async def _publish_locked(self, job_id: UUID) -> None:
        async with self.sessions.begin() as session:
            pending_job = await session.get(PublicationJob, job_id)
            control = await session.get(RuntimeControl, 1)
            if not pending_job or (
                control
                and (
                    not control.publication_enabled
                    or (
                        pending_job.platform == Platform.TELEGRAM
                        and not control.telegram_enabled
                    )
                    or (pending_job.platform == Platform.MAX and not control.max_enabled)
                )
            ):
                return
            claimed = await session.scalar(
                update(PublicationJob)
                .where(
                    PublicationJob.id == job_id,
                    PublicationJob.state.in_([JobState.PENDING, JobState.RETRY]),
                )
                .values(
                    state=JobState.SENDING,
                    attempts=PublicationJob.attempts + 1,
                    updated_at=utcnow(),
                )
                .returning(PublicationJob.id)
            )
            if not claimed:
                return
            job = await session.get(PublicationJob, job_id)
            if not job:
                return
            draft = await session.get(Draft, job.draft_id)
            if not draft or not draft.validated:
                job.state = JobState.FAILED
                job.last_error = "missing or invalid draft"
                return
            post = _post_from_draft(draft)
            platform = job.platform
            operation = job.operation
            publication = await session.scalar(
                select(PlatformPublication)
                .where(
                    PlatformPublication.event_id == draft.event_id,
                    PlatformPublication.platform == platform,
                    PlatformPublication.deleted_at.is_(None),
                )
                .order_by(PlatformPublication.published_at.desc())
                .limit(1)
            )
            if operation == JobOperation.EDIT and not publication:
                job.state = JobState.FAILED
                job.last_error = "no existing platform publication to edit"
                return
            latest_publication_at = await session.scalar(
                select(func.max(PlatformPublication.published_at)).where(
                    PlatformPublication.platform == platform
                )
            )
            if (
                operation == JobOperation.PUBLISH
                and latest_publication_at
                and as_utc(latest_publication_at)
                + timedelta(seconds=self.publication_min_interval_seconds)
                > utcnow()
            ):
                job.state = JobState.RETRY
                job.next_attempt_at = as_utc(latest_publication_at) + timedelta(
                    seconds=self.publication_min_interval_seconds
                )
                job.attempts -= 1
                return

        try:
            async with asyncio.timeout(self.publish_timeout_seconds):
                if operation == JobOperation.EDIT:
                    assert publication is not None
                    await self.publishers[platform].edit(publication.external_id, post)
                    result = None
                else:
                    if platform == Platform.MAX and publication:
                        post = post.model_copy(
                            update={"title": f"Обновление: {post.title}"[:240]}
                        )
                    result = await self.publishers[platform].publish(post)
        except TimeoutError:
            PUBLISH_ERRORS.labels(platform=str(platform), kind="timeout").inc()
            async with self.sessions.begin() as session:
                job = await session.get(PublicationJob, job_id)
                if job:
                    job.state = JobState.UNCERTAIN
                    job.last_error = "timeout after send; reconciliation required"
            return
        except Exception as error:
            error_name = type(error).__name__.casefold()
            permanent = isinstance(
                error, (ValueError, PermissionError, NotImplementedError)
            ) or any(
                term in error_name
                for term in ("unauthorized", "forbidden", "authkey", "sessionrevoked")
            )
            PUBLISH_ERRORS.labels(
                platform=str(platform),
                kind="permanent" if permanent else "transient",
            ).inc()
            async with self.sessions.begin() as session:
                job = await session.get(PublicationJob, job_id)
                if job:
                    job.state = (
                        JobState.FAILED
                        if permanent
                        else JobState.RETRY
                        if job.attempts < self.max_attempts
                        else JobState.DEAD_LETTER
                    )
                    if job.state == JobState.RETRY:
                        delay = self.retry_base_seconds * 2 ** (job.attempts - 1)
                        retry_after = getattr(error, "retry_after", None) or getattr(
                            error, "value", None
                        )
                        if isinstance(retry_after, (int, float)):
                            delay = max(delay, retry_after)
                        job.next_attempt_at = utcnow() + timedelta(
                            seconds=delay + random.uniform(0, delay * 0.2)
                        )
                    job.last_error = type(error).__name__
            return

        async with self.sessions.begin() as session:
            job = await session.get(PublicationJob, job_id)
            if not job:
                return
            draft = await session.get(Draft, job.draft_id)
            if not draft:
                return
            if operation == JobOperation.EDIT:
                stored_publication = await session.scalar(
                    select(PlatformPublication).where(
                        PlatformPublication.event_id == draft.event_id,
                        PlatformPublication.platform == platform,
                        PlatformPublication.deleted_at.is_(None),
                    )
                )
                if stored_publication:
                    stored_publication.updated_at = utcnow()
            else:
                assert result is not None
                session.add(
                    PlatformPublication(
                        job_id=job.id,
                        event_id=draft.event_id,
                        platform=platform,
                        external_id=result.publication_id,
                        published_at=result.published_at,
                    )
                )
            job.state = JobState.PUBLISHED
            job.next_attempt_at = None
            remaining = await session.scalar(
                select(func.count(PublicationJob.id)).where(
                    PublicationJob.draft_id == draft.id,
                    PublicationJob.id != job.id,
                    PublicationJob.state != JobState.PUBLISHED,
                )
            )
            if not remaining:
                event = await session.get(Event, draft.event_id)
                if event:
                    event.state = (
                        EventState.UPDATED
                        if operation == JobOperation.EDIT or publication
                        else EventState.PUBLISHED
                    )

    async def edit_event(self, event_id: UUID, post: Post) -> dict[Platform, str]:
        async with self.sessions() as session:
            publications = list(
                (
                    await session.scalars(
                        select(PlatformPublication).where(PlatformPublication.event_id == event_id)
                    )
                ).all()
            )
        results: dict[Platform, str] = {}
        for publication in publications:
            try:
                await self.publishers[publication.platform].edit(publication.external_id, post)
            except Exception as error:
                results[publication.platform] = f"{type(error).__name__}: {error}"
                continue
            results[publication.platform] = "updated"
            async with self.sessions.begin() as session:
                stored = await session.get(PlatformPublication, publication.id)
                if stored:
                    stored.updated_at = utcnow()
        if publications and all(value == "updated" for value in results.values()):
            async with self.sessions.begin() as session:
                event = await session.get(Event, event_id)
                if event:
                    event.state = EventState.UPDATED
        return results

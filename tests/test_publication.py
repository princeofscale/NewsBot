import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from newsbot.db_models import AuditLog, Draft, Event, PlatformPublication, PublicationJob
from newsbot.pipeline import PublicationWorker
from newsbot.publishers import DryRunPublisher
from newsbot.schemas import (
    EventState,
    JobOperation,
    JobState,
    Platform,
    Post,
    PublicationResult,
)


class FailsOncePublisher(DryRunPublisher):
    def __init__(self) -> None:
        super().__init__(Platform.MAX)
        self.failed = False

    async def publish(self, post: Post) -> PublicationResult:
        if not self.failed:
            self.failed = True
            raise ConnectionError("temporary")
        return await super().publish(post)


class FailsAlwaysPublisher(DryRunPublisher):
    async def publish(self, post: Post) -> PublicationResult:
        raise ConnectionError("offline")


async def seed_jobs(sessions: async_sessionmaker[AsyncSession]) -> UUID:
    async with sessions.begin() as session:
        event = Event(title="Событие", state=EventState.READY, confidence=0.9)
        session.add(event)
        await session.flush()
        draft = Draft(
            event_id=event.id,
            title="Заголовок",
            body="Текст",
            source_urls=["https://example.test/news"],
            confidence=0.9,
            validated=True,
        )
        session.add(draft)
        await session.flush()
        session.add_all(
            [
                PublicationJob(draft_id=draft.id, platform=Platform.TELEGRAM),
                PublicationJob(draft_id=draft.id, platform=Platform.MAX),
            ]
        )
        return event.id


@pytest.mark.asyncio
async def test_partial_failure_retry_and_idempotency(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await seed_jobs(sessions)
    telegram = DryRunPublisher(Platform.TELEGRAM)
    max_publisher = FailsOncePublisher()
    worker = PublicationWorker(
        sessions,
        {Platform.TELEGRAM: telegram, Platform.MAX: max_publisher},
        retry_base_seconds=0,
    )

    assert await worker.run_once() == 2
    async with sessions() as session:
        states = set((await session.scalars(select(PublicationJob.state))).all())
        assert states == {JobState.PUBLISHED, JobState.RETRY}
        assert await session.scalar(select(func.count(PlatformPublication.id))) == 1

    assert await worker.run_once() == 1
    assert await worker.run_once() == 0
    async with sessions() as session:
        assert await session.scalar(select(func.count(PlatformPublication.id))) == 2
        event = await session.get(Event, event_id)
        assert event and event.state == EventState.PUBLISHED


@pytest.mark.asyncio
async def test_update_existing_publications(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await seed_jobs(sessions)
    publishers = {
        Platform.TELEGRAM: DryRunPublisher(Platform.TELEGRAM),
        Platform.MAX: DryRunPublisher(Platform.MAX),
    }
    worker = PublicationWorker(sessions, publishers, retry_base_seconds=0)
    await worker.run_once()
    post = Post(
        title="Обновлено",
        body="Новый текст",
        source_urls=["https://example.test/news"],
        event_id=event_id,
        confidence=0.95,
    )
    assert set((await worker.edit_event(event_id, post)).values()) == {"updated"}
    async with sessions() as session:
        event = await session.get(Event, event_id)
        assert event and event.state == EventState.UPDATED


@pytest.mark.asyncio
async def test_outbox_edits_telegram_and_publishes_prefixed_max_update(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await seed_jobs(sessions)
    telegram = DryRunPublisher(Platform.TELEGRAM)
    max_publisher = DryRunPublisher(Platform.MAX)
    worker = PublicationWorker(
        sessions,
        {Platform.TELEGRAM: telegram, Platform.MAX: max_publisher},
        retry_base_seconds=0,
    )
    await worker.run_once()
    async with sessions.begin() as session:
        draft = Draft(
            event_id=event_id,
            title="Уточнено время",
            body="Новый текст",
            source_urls=["https://example.test/news"],
            confidence=0.95,
            validated=True,
            version=2,
        )
        session.add(draft)
        await session.flush()
        session.add_all(
            [
                PublicationJob(
                    draft_id=draft.id,
                    platform=Platform.TELEGRAM,
                    operation=JobOperation.EDIT,
                ),
                PublicationJob(
                    draft_id=draft.id,
                    platform=Platform.MAX,
                    operation=JobOperation.PUBLISH,
                ),
            ]
        )

    await worker.run_once()

    assert len(telegram.posts) == 1
    assert len(max_publisher.posts) == 2
    assert any(post.title.startswith("Обновление:") for post in max_publisher.posts.values())
    async with sessions() as session:
        assert await session.scalar(select(func.count(PlatformPublication.id))) == 3
        event = await session.get(Event, event_id)
        assert event and event.state == EventState.UPDATED


class TimeoutPublisher(DryRunPublisher):
    async def publish(self, post: Post) -> PublicationResult:
        raise TimeoutError


class ReconcilingPublisher(DryRunPublisher):
    def __init__(self, publication_id: str | None) -> None:
        super().__init__(Platform.TELEGRAM)
        self.publication_id = publication_id

    async def find_publication(self, post: Post) -> str | None:
        return self.publication_id


@pytest.mark.asyncio
async def test_timeout_becomes_uncertain_not_duplicate_retry(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await seed_jobs(sessions)
    worker = PublicationWorker(
        sessions,
        {
            Platform.TELEGRAM: TimeoutPublisher(Platform.TELEGRAM),
            Platform.MAX: DryRunPublisher(Platform.MAX),
        },
        retry_base_seconds=0,
    )
    await worker.run_once()
    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count(PublicationJob.id)).where(
                    PublicationJob.state == JobState.UNCERTAIN
                )
            )
            == 1
        )
    assert await worker.run_once() == 0


@pytest.mark.asyncio
async def test_uncertain_reconciliation_records_found_message_and_retries_absent_one(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await seed_jobs(sessions)
    async with sessions.begin() as session:
        jobs = list((await session.scalars(select(PublicationJob))).all())
        for job in jobs:
            job.state = JobState.UNCERTAIN
    worker = PublicationWorker(
        sessions,
        {
            Platform.TELEGRAM: ReconcilingPublisher("telegram-42"),
            Platform.MAX: DryRunPublisher(Platform.MAX),
        },
        reconciliation_absence_delay_seconds=0,
    )

    assert await worker.reconcile_uncertain() == {
        "published": 1,
        "retry": 0,
        "unsupported": 1,
        "deferred": 0,
    }
    async with sessions() as session:
        telegram_job = await session.scalar(
            select(PublicationJob).where(PublicationJob.platform == Platform.TELEGRAM)
        )
        max_job = await session.scalar(
            select(PublicationJob).where(PublicationJob.platform == Platform.MAX)
        )
        publication = await session.scalar(select(PlatformPublication))
        assert telegram_job and telegram_job.state == JobState.PUBLISHED
        assert max_job and max_job.state == JobState.UNCERTAIN
        assert publication and publication.external_id == "telegram-42"

    await worker.resolve_retry(max_job.id)
    async with sessions() as session:
        max_job = await session.get(PublicationJob, max_job.id)
        assert max_job and max_job.state == JobState.RETRY


@pytest.mark.asyncio
async def test_concurrent_workers_claim_jobs_once(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await seed_jobs(sessions)
    telegram = DryRunPublisher(Platform.TELEGRAM)
    max_publisher = DryRunPublisher(Platform.MAX)
    worker = PublicationWorker(
        sessions,
        {Platform.TELEGRAM: telegram, Platform.MAX: max_publisher},
        retry_base_seconds=0,
    )
    await asyncio.gather(worker.run_once(), worker.run_once())
    assert len(telegram.posts) == 1
    assert len(max_publisher.posts) == 1


@pytest.mark.asyncio
async def test_stale_sending_job_becomes_uncertain(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await seed_jobs(sessions)
    async with sessions.begin() as session:
        job = await session.scalar(select(PublicationJob).limit(1))
        assert job
        job.state = JobState.SENDING
        job.updated_at = job.updated_at - timedelta(hours=1)
    worker = PublicationWorker(
        sessions,
        {
            Platform.TELEGRAM: DryRunPublisher(Platform.TELEGRAM),
            Platform.MAX: DryRunPublisher(Platform.MAX),
        },
        sending_stale_seconds=30,
    )
    await worker.run_once()
    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count(PublicationJob.id)).where(
                    PublicationJob.state == JobState.UNCERTAIN
                )
            )
            == 1
        )


@pytest.mark.asyncio
async def test_kill_switch_stops_outbox_and_is_audited(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    from newsbot.api import set_publication_enabled

    await seed_jobs(sessions)
    async with sessions() as session:
        await set_publication_enabled(False, session, None)
    worker = PublicationWorker(
        sessions,
        {
            Platform.TELEGRAM: DryRunPublisher(Platform.TELEGRAM),
            Platform.MAX: DryRunPublisher(Platform.MAX),
        },
    )

    assert await worker.run_once() == 0
    async with sessions() as session:
        assert await session.scalar(select(func.count(AuditLog.id))) == 1


@pytest.mark.asyncio
async def test_exhausted_transient_failure_moves_to_dead_letter(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await seed_jobs(sessions)
    worker = PublicationWorker(
        sessions,
        {
            Platform.TELEGRAM: FailsAlwaysPublisher(Platform.TELEGRAM),
            Platform.MAX: FailsAlwaysPublisher(Platform.MAX),
        },
        max_attempts=1,
    )

    await worker.run_once()

    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count(PublicationJob.id)).where(
                    PublicationJob.state == JobState.DEAD_LETTER
                )
            )
            == 2
        )

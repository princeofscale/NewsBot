import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from newsbot.db_models import Draft, Event, PlatformPublication, PublicationJob
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

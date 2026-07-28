import asyncio
import os

import httpx
import pytest
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from newsbot.config import Settings
from newsbot.fetcher import SourceFetcher
from newsbot.llm import DeterministicLLM
from newsbot.pipeline import CycleAlreadyRunning, CycleResult, Pipeline


@pytest.mark.asyncio
async def test_alembic_schema_has_expected_constraints() -> None:
    url = os.getenv("NEWSBOT_DATABASE_URL")
    if not url or not url.startswith("postgresql"):
        pytest.skip("PostgreSQL migration database is not configured")
    engine = create_async_engine(url)
    async with engine.connect() as connection:
        tables, article_constraints = await connection.run_sync(
            lambda sync: (
                set(inspect(sync).get_table_names()),
                {
                    item["name"]
                    for item in inspect(sync).get_unique_constraints("articles")
                },
            )
        )
    await engine.dispose()

    assert {"sources", "articles", "events", "publication_jobs"} <= tables
    assert "uq_article_source_url_content" in article_constraints


@pytest.mark.asyncio
async def test_postgres_advisory_lock_blocks_second_engine() -> None:
    url = os.getenv("NEWSBOT_TEST_DATABASE_URL")
    if not url or not url.startswith("postgresql"):
        pytest.skip("PostgreSQL integration database is not configured")
    engines = [create_async_engine(url), create_async_engine(url)]
    settings = Settings(validate_public_source_ips=False)
    pipelines = [
        Pipeline(
            async_sessionmaker(engine, expire_on_commit=False),
            SourceFetcher(settings, httpx.AsyncClient()),
            DeterministicLLM(),
            settings,
        )
        for engine in engines
    ]
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocking_cycle() -> CycleResult:
        started.set()
        await release.wait()
        return CycleResult()

    pipelines[0]._run_cycle_unlocked = blocking_cycle  # type: ignore[method-assign]
    first = asyncio.create_task(pipelines[0].run_cycle())
    await started.wait()
    with pytest.raises(CycleAlreadyRunning):
        await pipelines[1].run_cycle()
    release.set()
    await first
    for pipeline, engine in zip(pipelines, engines, strict=True):
        await pipeline.fetcher.close()
        await engine.dispose()

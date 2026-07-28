from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from newsbot.db_models import Source
from newsbot.schemas import SourceKind
from newsbot.source_config import load_sources, sync_sources


def test_real_source_config_has_five_https_sources() -> None:
    sources = load_sources(Path("config/sources.json"))
    assert len(sources) == 5
    assert len({source.name for source in sources}) == 5
    assert all(source.feed_url.scheme == "https" for source in sources)
    assert sum(source.is_official for source in sources) == 2


@pytest.mark.asyncio
async def test_source_sync_disables_entries_removed_from_config(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    configured = load_sources(Path("config/sources.json"))
    async with sessions() as session:
        assert await sync_sources(session, configured) == (5, 0, 0)
        session.add(
            Source(
                name="removed",
                base_url="https://removed.example",
                kind=SourceKind.RSS,
                feed_url="https://removed.example/rss",
            )
        )
        await session.commit()
        assert await sync_sources(session, configured) == (0, 5, 1)

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from newsbot.config import get_settings


def make_engine(url: str | None = None) -> AsyncEngine:
    return create_async_engine(url or get_settings().database_url, pool_pre_ping=True)


engine = make_engine()
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as session:
        yield session

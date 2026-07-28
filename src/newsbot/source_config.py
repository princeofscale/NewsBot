import json
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from newsbot.db_models import Source
from newsbot.schemas import SourceInput


def load_sources(path: Path) -> list[SourceInput]:
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError("source config must be a JSON array")
    return [SourceInput.model_validate(item) for item in data]


async def sync_sources(
    session: AsyncSession, configured: list[SourceInput]
) -> tuple[int, int, int]:
    created = updated = 0
    for item in configured:
        source = await session.scalar(select(Source).where(Source.name == item.name))
        values = {
            "base_url": str(item.base_url),
            "kind": item.kind,
            "feed_url": str(item.feed_url),
            "selectors": item.selectors,
            "trust_level": item.trust_level,
            "is_official": item.is_official,
            "origin_group": item.origin_group,
            "min_interval_seconds": item.min_interval_seconds,
            "enabled": item.enabled,
        }
        if source:
            for key, value in values.items():
                setattr(source, key, value)
            updated += 1
        else:
            session.add(Source(name=item.name, **values))
            created += 1
    names = [item.name for item in configured]
    disabled = (
        await session.execute(
            update(Source)
            .where(Source.name.not_in(names), Source.enabled.is_(True))
            .values(enabled=False)
            .returning(Source.id)
        )
    ).all()
    await session.commit()
    return created, updated, len(disabled)

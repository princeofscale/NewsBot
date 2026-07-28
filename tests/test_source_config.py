from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from newsbot.db_models import Source
from newsbot.parsing import parse_article_page
from newsbot.schemas import CandidateArticle, SourceKind
from newsbot.source_config import load_sources, sync_sources


def test_real_source_config_has_five_https_sources() -> None:
    sources = load_sources(Path("config/sources.json"))
    assert len(sources) == 5
    assert len({source.name for source in sources}) == 5
    assert all(source.feed_url.scheme == "https" for source in sources)
    assert sum(source.is_official for source in sources) == 2
    html_sources = [source for source in sources if source.kind == SourceKind.HTML]
    assert all(source.selectors.get("article_date") for source in html_sources)
    assert all(source.selectors.get("article_text") for source in html_sources)
    administration = next(source for source in sources if source.name == "Администрация Саратова")
    assert administration.selectors["article_text"] == ".news-item-text"


@pytest.mark.parametrize(
    ("name", "fixture"),
    [
        ("СарБК", "sarbc_article.html"),
        ("Взгляд-инфо", "vzsar_article.html"),
        ("СарИнформ", "sarinform_article.html"),
        ("Администрация Саратова", "saratovmer_article.html"),
        ("Регион 64", "region64_article.html"),
    ],
)
def test_sanitized_real_source_snapshots(name: str, fixture: str) -> None:
    source = next(item for item in load_sources(Path("config/sources.json")) if item.name == name)
    discovered = CandidateArticle(
        url=f"{source.base_url}news/test",
        title="Анонс",
        text="Анонс",
        raw_content=b"",
        content_type="text/html",
    )

    article = parse_article_page(
        (Path("tests/fixtures/real") / fixture).read_bytes(),
        "text/html; charset=utf-8",
        discovered,
        source.selectors,
    )

    assert article.title
    assert article.published_at
    assert article.image_url
    assert len(article.text) >= 80


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

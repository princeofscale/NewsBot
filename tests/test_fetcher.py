import httpx
import pytest

from newsbot.config import Settings
from newsbot.db_models import Source
from newsbot.fetcher import SourceFetcher
from newsbot.schemas import SourceKind


@pytest.mark.asyncio
async def test_fetcher_rejects_oversized_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 10_001)

    settings = Settings(
        fetch_retries=1,
        max_response_bytes=10_000,
        validate_public_source_ips=False,
    )
    fetcher = SourceFetcher(settings, httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    source = Source(
        name="large",
        base_url="https://large.example",
        kind=SourceKind.RSS,
        feed_url="https://large.example/rss",
    )
    with pytest.raises(RuntimeError, match="fetch failed"):
        await fetcher.fetch(source)

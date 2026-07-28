import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta

import httpx

from newsbot.config import Settings
from newsbot.db_models import Source, utcnow
from newsbot.parsing import decode_payload, parse_article_page, parse_source
from newsbot.schemas import CandidateArticle
from newsbot.security import validate_public_url


@dataclass(slots=True)
class FetchResult:
    articles: list[CandidateArticle]
    status_code: int
    etag: str | None = None
    last_modified: str | None = None
    article_errors: int = 0


Download = Callable[[str, dict[str, str]], Awaitable[tuple[httpx.Response, bytes]]]


class ArticlePageFetcher:
    def __init__(self, download: Download) -> None:
        self.download = download

    async def fetch(self, source: Source, discovered: CandidateArticle) -> CandidateArticle:
        try:
            response, payload = await self.download(discovered.url, {})
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 404 or discovered.url.endswith("/"):
                raise
            response, payload = await self.download(f"{discovered.url}/", {})
        return parse_article_page(
            payload,
            response.headers.get("content-type", "text/html"),
            discovered,
            source.selectors or {},
        )


class SourceFetcher:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        self.settings = settings
        self.client = client or httpx.AsyncClient(
            timeout=settings.fetch_timeout_seconds,
            headers={"User-Agent": settings.user_agent},
            follow_redirects=False,
        )
        self._semaphore = asyncio.Semaphore(settings.fetch_concurrency)
        self.article_pages = ArticlePageFetcher(self._download)

    async def close(self) -> None:
        await self.client.aclose()

    async def fetch(self, source: Source) -> FetchResult:
        now = utcnow()
        if source.disabled_until and source.disabled_until > now:
            raise RuntimeError(f"source circuit open until {source.disabled_until.isoformat()}")
        headers = {}
        if source.etag:
            headers["If-None-Match"] = source.etag
        if source.last_modified:
            headers["If-Modified-Since"] = source.last_modified

        last_error: Exception | None = None
        for attempt in range(self.settings.fetch_retries):
            try:
                response, payload = await self._download(source.feed_url, headers)
                if response.status_code == 304:
                    return FetchResult([], 304, source.etag, source.last_modified)
                discovered = parse_source(
                    source.kind,
                    decode_payload(payload, response.headers.get("content-type", "")),
                    source.base_url,
                    source.selectors or {},
                )
                discovered = list({article.url: article for article in discovered}.values())
                pages = await asyncio.gather(
                    *(self.article_pages.fetch(source, article) for article in discovered),
                    return_exceptions=True,
                )
                articles = [page for page in pages if isinstance(page, CandidateArticle)]
                return FetchResult(
                    articles,
                    response.status_code,
                    response.headers.get("etag"),
                    response.headers.get("last-modified"),
                    sum(isinstance(page, BaseException) for page in pages),
                )
            except (httpx.HTTPError, ValueError) as error:
                last_error = error
                if attempt + 1 < self.settings.fetch_retries:
                    delay = min(8.0, 0.5 * 2**attempt) + random.uniform(0, 0.25)
                    await asyncio.sleep(delay)
        raise RuntimeError(f"fetch failed after retries: {last_error}") from last_error

    async def _download(
        self, initial_url: str, headers: dict[str, str]
    ) -> tuple[httpx.Response, bytes]:
        url = initial_url
        for _ in range(6):
            if self.settings.validate_public_source_ips:
                await validate_public_url(url)
            async with self._semaphore, self.client.stream("GET", url, headers=headers) as response:
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        response.raise_for_status()
                    url = str(response.url.join(location))
                    continue
                response.raise_for_status()
                declared = response.headers.get("content-length")
                if declared and int(declared) > self.settings.max_response_bytes:
                    raise ValueError("source response exceeds configured size limit")
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self.settings.max_response_bytes:
                        raise ValueError("source response exceeds configured size limit")
                    chunks.append(chunk)
                return response, b"".join(chunks)
        raise ValueError("too many source redirects")

    def apply_failure(self, source: Source) -> None:
        source.consecutive_failures += 1
        if source.consecutive_failures >= self.settings.circuit_failure_threshold:
            source.disabled_until = utcnow() + timedelta(
                seconds=self.settings.circuit_cooldown_seconds
            )

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from time import monotonic
from urllib.parse import urlsplit

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
    discovered_count: int = 0
    attempted_count: int = 0
    error_reasons: tuple[str, ...] = ()


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
        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._domain_last_request: dict[str, float] = {}
        self.article_pages = ArticlePageFetcher(self._download)

    async def close(self) -> None:
        await self.client.aclose()

    async def discover(self, source: Source) -> FetchResult:
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
                allowed_domain = (
                    source.origin_group or urlsplit(source.base_url).hostname or ""
                ).casefold()
                rejected = [
                    article
                    for article in discovered
                    if not (
                        (urlsplit(article.url).hostname or "").casefold() == allowed_domain
                        or (urlsplit(article.url).hostname or "")
                        .casefold()
                        .endswith(f".{allowed_domain}")
                    )
                ]
                discovered = [article for article in discovered if article not in rejected]
                limited = discovered[: self.settings.max_items_per_source]
                return FetchResult(
                    limited,
                    response.status_code,
                    response.headers.get("etag"),
                    response.headers.get("last-modified"),
                    article_errors=len(rejected),
                    discovered_count=len(limited),
                    error_reasons=("cross-domain article URL",) if rejected else (),
                )
            except (httpx.HTTPError, ValueError) as error:
                last_error = error
                if attempt + 1 < self.settings.fetch_retries:
                    delay = min(8.0, 0.5 * 2**attempt) + random.uniform(0, 0.25)
                    await asyncio.sleep(delay)
        raise RuntimeError(f"fetch failed after retries: {last_error}") from last_error

    async def hydrate(
        self, source: Source, discovered: list[CandidateArticle]
    ) -> FetchResult:
        pages = await asyncio.gather(
            *(self.article_pages.fetch(source, article) for article in discovered),
            return_exceptions=True,
        )
        return FetchResult(
            [page for page in pages if isinstance(page, CandidateArticle)],
            200,
            article_errors=sum(isinstance(page, BaseException) for page in pages),
            discovered_count=len(discovered),
            attempted_count=len(discovered),
            error_reasons=tuple(
                type(page).__name__ for page in pages if isinstance(page, BaseException)
            ),
        )

    async def fetch(self, source: Source) -> FetchResult:
        discovered = await self.discover(source)
        if discovered.status_code == 304:
            return discovered
        hydrated = await self.hydrate(source, discovered.articles)
        hydrated.status_code = discovered.status_code
        hydrated.etag = discovered.etag
        hydrated.last_modified = discovered.last_modified
        hydrated.discovered_count = discovered.discovered_count
        hydrated.article_errors += discovered.article_errors
        hydrated.error_reasons = discovered.error_reasons + hydrated.error_reasons
        return hydrated

    async def _download(
        self, initial_url: str, headers: dict[str, str]
    ) -> tuple[httpx.Response, bytes]:
        url = initial_url
        for _ in range(6):
            if self.settings.validate_public_source_ips:
                await validate_public_url(url)
            domain = urlsplit(url).netloc.casefold()
            lock = self._domain_locks.setdefault(domain, asyncio.Lock())
            async with self._semaphore:
                async with lock:
                    elapsed = monotonic() - self._domain_last_request.get(domain, 0.0)
                    delay = self.settings.domain_request_interval_seconds - elapsed
                    if delay > 0:
                        await asyncio.sleep(delay)
                    self._domain_last_request[domain] = monotonic()
                response, payload = await self._read(url, headers)
            if response.is_redirect:
                location = response.headers.get("location")
                if not location:
                    response.raise_for_status()
                url = str(response.url.join(location))
                continue
            return response, payload
        raise ValueError("too many source redirects")

    async def _read(
        self, url: str, headers: dict[str, str]
    ) -> tuple[httpx.Response, bytes]:
        async with self.client.stream("GET", url, headers=headers) as response:
            if response.is_redirect:
                return response, b""
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

    def apply_failure(self, source: Source) -> None:
        source.consecutive_failures += 1
        if source.consecutive_failures >= self.settings.circuit_failure_threshold:
            source.disabled_until = utcnow() + timedelta(
                seconds=self.settings.circuit_cooldown_seconds
            )

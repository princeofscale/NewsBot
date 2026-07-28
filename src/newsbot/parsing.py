import hashlib
import re
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import feedparser  # type: ignore[import-untyped]
from bs4 import BeautifulSoup, Tag

from newsbot.schemas import CandidateArticle, SourceKind

TRACKING_PARAMS = {"fbclid", "gclid", "yclid"}


def canonicalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = urlencode(
        sorted(
            (key, value)
            for key, value in parse_qsl(parts.query, keep_blank_values=True)
            if not key.lower().startswith("utm_") and key.lower() not in TRACKING_PARAMS
        )
    )
    path = re.sub(r"/+", "/", parts.path).rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, query, ""))


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", BeautifulSoup(value, "html.parser").get_text(" ")).strip()


def text_hash(value: str) -> str:
    return hashlib.sha256(clean_text(value).casefold().encode()).hexdigest()


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_feed(payload: str, base_url: str) -> list[CandidateArticle]:
    feed = feedparser.parse(payload)
    result: list[CandidateArticle] = []
    for entry in feed.entries:
        content = entry.get("content") or []
        body = content[0].get("value", "") if content else entry.get("summary", "")
        text = clean_text(body)
        link = canonicalize_url(urljoin(base_url, entry.get("link", "")))
        if link and entry.get("title") and text:
            result.append(
                CandidateArticle(
                    url=link,
                    title=clean_text(entry.title),
                    text=text,
                    published_at=parse_datetime(entry.get("published") or entry.get("updated")),
                    raw_content=body,
                    content_type="application/rss+xml",
                )
            )
    return result


def _value(node: Tag, selector: str | None, attribute: str | None = None) -> str:
    target = node.select_one(selector) if selector else node
    if not target:
        return ""
    return str(target.get(attribute, "")) if attribute else target.get_text(" ", strip=True)


def parse_html(payload: str, base_url: str, selectors: dict[str, str]) -> list[CandidateArticle]:
    soup = BeautifulSoup(payload, "html.parser")
    item_selector = selectors.get("item", "article")
    result: list[CandidateArticle] = []
    for item in soup.select(item_selector):
        title = clean_text(_value(item, selectors.get("title", "h1, h2, h3")))
        text = clean_text(_value(item, selectors.get("text", ".article-body, .content, p")))
        link = _value(item, selectors.get("link", "a"), "href")
        image = _value(item, selectors.get("image", "img"), "src") or None
        if title and text and link:
            result.append(
                CandidateArticle(
                    url=canonicalize_url(urljoin(base_url, link)),
                    title=title,
                    text=text,
                    published_at=parse_datetime(
                        _value(item, selectors.get("date", "time"), "datetime")
                    ),
                    image_url=urljoin(base_url, image) if image else None,
                    raw_content=str(item),
                    content_type="text/html",
                )
            )
    return result


def parse_source(
    kind: SourceKind, payload: str, base_url: str, selectors: dict[str, str]
) -> list[CandidateArticle]:
    if kind == SourceKind.RSS:
        return parse_feed(payload, base_url)
    return parse_html(payload, base_url, selectors)

import hashlib
import json
import re
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import feedparser  # type: ignore[import-untyped]
from bs4 import BeautifulSoup, Tag

from newsbot.schemas import CandidateArticle, SourceKind

TRACKING_PARAMS = {"fbclid", "gclid", "yclid"}
SARATOV_TZ = ZoneInfo("Europe/Saratov")
RU_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


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


def decode_payload(payload: bytes, content_type: str) -> str:
    header_match = re.search(r"charset=([\w-]+)", content_type, re.I)
    charset = header_match.group(1) if header_match else None
    if not charset:
        payload_match = re.search(
            rb"(?:charset\s*=|encoding\s*=\s*)[\"']?([\w-]+)",
            payload[:4096],
            re.I,
        )
        charset = payload_match.group(1).decode("ascii") if payload_match else None
    return payload.decode(charset or "utf-8", errors="replace")


def text_hash(value: str) -> str:
    return hashlib.sha256(clean_text(value).casefold().encode()).hexdigest()


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    value = clean_text(value).strip(" ,.")
    russian = re.search(
        rf"(\d{{1,2}})\s+({'|'.join(RU_MONTHS)})\s+(\d{{4}})"
        r"(?:\s*(?:года?)?\s*,?\s*(\d{1,2}):(\d{2}))?",
        value.casefold(),
    )
    numeric = re.search(
        r"(\d{1,2})[./](\d{1,2})[./](\d{4})(?:\s*,?\s*(\d{1,2}):(\d{2}))?",
        value,
    )
    if russian or numeric:
        match = russian or numeric
        assert match is not None
        day, month_value, year, hour, minute = match.groups()
        month = RU_MONTHS[month_value] if russian else int(month_value)
        parsed = datetime(
            int(year),
            month,
            int(day),
            int(hour or 0),
            int(minute or 0),
            tzinfo=SARATOV_TZ,
        )
        return parsed.astimezone(UTC)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SARATOV_TZ)
    return parsed.astimezone(UTC)


def parse_feed(payload: str, base_url: str) -> list[CandidateArticle]:
    feed = feedparser.parse(payload)
    result: list[CandidateArticle] = []
    for entry in feed.entries:
        content = entry.get("content") or []
        body = content[0].get("value", "") if content else entry.get("summary", "")
        title = clean_text(entry.get("title", ""))
        text = clean_text(body) or title
        link = canonicalize_url(urljoin(base_url, entry.get("link", "")))
        if link and title:
            result.append(
                CandidateArticle(
                    url=link,
                    title=title,
                    text=text,
                    published_at=parse_datetime(entry.get("published") or entry.get("updated")),
                    image_url=(
                        str(entry.enclosures[0].get("href")) if entry.get("enclosures") else None
                    ),
                    raw_content=body.encode(),
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
                    raw_content=str(item).encode(),
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


def _json_ld_article(soup: BeautifulSoup) -> dict[str, object]:
    def find_article(value: object) -> dict[str, object] | None:
        if isinstance(value, dict):
            kind = value.get("@type")
            if kind in {"Article", "NewsArticle", "ReportageNewsArticle"}:
                return value
            for nested in value.values():
                found = find_article(nested)
                if found:
                    return found
        elif isinstance(value, list):
            for nested in value:
                found = find_article(nested)
                if found:
                    return found
        return None

    for node in soup.select('script[type="application/ld+json"]'):
        try:
            found = find_article(json.loads(node.get_text()))
        except (json.JSONDecodeError, TypeError):
            continue
        if found:
            return found
    return {}


def _meta(soup: BeautifulSoup, *names: str) -> str:
    for name in names:
        node = soup.select_one(f'meta[property="{name}"], meta[name="{name}"]')
        if node and node.get("content"):
            return str(node["content"])
    return ""


def _selected_text(soup: BeautifulSoup, selector: str | None) -> str:
    if selector:
        selected = clean_text(" ".join(str(node) for node in soup.select(selector)))
        if selected:
            return selected
    for fallback in (
        "[itemprop='articleBody']",
        ".article-body",
        ".news-detail",
        ".detail-text",
        ".news-text",
        "article",
    ):
        node = soup.select_one(fallback)
        text = clean_text(str(node)) if node else ""
        if len(text) >= 80:
            return text
    return clean_text(
        " ".join(
            str(node)
            for node in soup.select(
                "main p, [class*='detail'] p, [class*='article'] p, [class*='news'] p"
            )
        )
    )


def parse_article_page(
    payload: bytes,
    content_type: str,
    discovered: CandidateArticle,
    selectors: dict[str, str],
) -> CandidateArticle:
    html = decode_payload(payload, content_type)
    soup = BeautifulSoup(html, "html.parser")
    structured = _json_ld_article(soup)

    title_node = soup.select_one(selectors.get("article_title", "h1"))
    title = clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
    title = title or str(structured.get("headline") or "") or _meta(soup, "og:title")

    text = _selected_text(soup, selectors.get("article_text"))
    text = clean_text(str(structured.get("articleBody") or "")) or text
    if not text:
        raise ValueError("article page has no extractable text")

    date_selector = selectors.get("article_date")
    date_node = soup.select_one(date_selector) if date_selector else None
    date_value = (
        str(date_node.get("datetime") or date_node.get("content") or date_node.get_text(" "))
        if date_node
        else ""
    )
    date_value = (
        date_value
        or str(structured.get("datePublished") or "")
        or _meta(soup, "article:published_time", "datePublished", "date")
    )

    image_selector = selectors.get("article_image")
    image_node = soup.select_one(image_selector) if image_selector else None
    image = str(image_node.get("src") or image_node.get("content") or "") if image_node else ""
    structured_image = structured.get("image")
    if isinstance(structured_image, list):
        structured_image = structured_image[0] if structured_image else ""
    if isinstance(structured_image, dict):
        structured_image = structured_image.get("url", "")
    image = image or str(structured_image or "") or _meta(soup, "og:image")

    published_at = parse_datetime(date_value)
    url_date = re.search(r"/(\d{4})/(\d{1,2})/(\d{1,2})/", discovered.url)
    if not published_at and url_date:
        year, month, day = map(int, url_date.groups())
        clock = re.search(r"\b(\d{1,2}):(\d{2})\b", date_value)
        published_at = datetime(
            year,
            month,
            day,
            int(clock.group(1)) if clock else 0,
            int(clock.group(2)) if clock else 0,
            tzinfo=SARATOV_TZ,
        ).astimezone(UTC)

    return CandidateArticle(
        url=discovered.url,
        title=title or discovered.title,
        text=text,
        published_at=published_at or discovered.published_at,
        image_url=urljoin(discovered.url, image) if image else discovered.image_url,
        raw_content=payload,
        content_type=content_type,
    )

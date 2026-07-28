from datetime import datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, model_validator


class SourceKind(StrEnum):
    RSS = "RSS"
    HTML = "HTML"


class EventState(StrEnum):
    COLLECTING = "COLLECTING"
    READY = "READY"
    REJECTED = "REJECTED"
    PUBLISHED = "PUBLISHED"
    UPDATED = "UPDATED"
    FAILED = "FAILED"


class JobState(StrEnum):
    PENDING = "PENDING"
    SENDING = "SENDING"
    RETRY = "RETRY"
    PUBLISHED = "PUBLISHED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    UNCERTAIN = "UNCERTAIN"


class ArticleState(StrEnum):
    NEW = "NEW"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class JobOperation(StrEnum):
    PUBLISH = "PUBLISH"
    EDIT = "EDIT"


class Platform(StrEnum):
    TELEGRAM = "telegram"
    MAX = "max"


class SourceInput(BaseModel):
    name: str
    base_url: HttpUrl
    kind: SourceKind
    feed_url: HttpUrl
    selectors: dict[str, str] = Field(default_factory=dict)
    trust_level: float = Field(default=0.5, ge=0, le=1)
    is_official: bool = False
    origin_group: str | None = Field(default=None, max_length=200)
    min_interval_seconds: int = Field(default=300, ge=30)
    enabled: bool = True

    @model_validator(mode="after")
    def safe_urls(self) -> "SourceInput":
        for url in (self.base_url, self.feed_url):
            if url.scheme != "https" or url.username or url.password:
                raise ValueError("source URLs must use HTTPS and must not contain credentials")
        return self


class CandidateArticle(BaseModel):
    url: str
    title: str
    text: str
    published_at: datetime | None = None
    image_url: str | None = None
    raw_content: bytes
    content_type: str


class ClaimPayload(BaseModel):
    subject: str = Field(min_length=1)
    predicate: str = Field(min_length=1)
    location: list[str] = Field(default_factory=list)
    event_time: datetime | None = None
    numbers: list[str] = Field(default_factory=list)
    names: list[str] = Field(default_factory=list)
    claim: str = Field(min_length=1)
    source_article_id: UUID


class Post(BaseModel):
    title: str = Field(max_length=240)
    body: str
    source_urls: list[str] = Field(min_length=1, max_length=3)
    event_id: UUID
    confidence: float = Field(ge=0, le=1)

    @property
    def length(self) -> int:
        return len(self.title) + len(self.body) + sum(map(len, self.source_urls))


class PublicationResult(BaseModel):
    publication_id: str
    platform: Platform
    published_at: datetime


class Publisher(Protocol):
    async def publish(self, post: Post) -> PublicationResult: ...

    async def edit(self, publication_id: str, post: Post) -> None: ...

    async def delete(self, publication_id: str) -> None: ...


class LLMPort(Protocol):
    async def extract_claims(
        self, article_id: UUID, title: str, text: str
    ) -> list[ClaimPayload]: ...

    async def create_post(
        self, event_id: UUID, claims: list[ClaimPayload], source_urls: list[str], confidence: float
    ) -> Post: ...

    async def verify_post(self, post: Post, claims: list[ClaimPayload]) -> bool: ...

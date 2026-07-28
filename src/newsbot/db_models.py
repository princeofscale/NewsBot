import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from newsbot.schemas import (
    ArticleState,
    EventState,
    JobOperation,
    JobState,
    Platform,
    SourceKind,
)


def utcnow() -> datetime:
    return datetime.now(UTC)


def as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class Source(Base, TimestampMixin):
    __tablename__ = "sources"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200), unique=True)
    base_url: Mapped[str] = mapped_column(String(1000))
    kind: Mapped[SourceKind] = mapped_column(String(20))
    feed_url: Mapped[str] = mapped_column(String(1000))
    selectors: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    trust_level: Mapped[float] = mapped_column(Float, default=0.5)
    is_official: Mapped[bool] = mapped_column(Boolean, default=False)
    origin_group: Mapped[str | None] = mapped_column(String(200))
    min_interval_seconds: Mapped[int] = mapped_column(Integer, default=300)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    etag: Mapped[str | None] = mapped_column(String(500))
    last_modified: Mapped[str | None] = mapped_column(String(500))
    last_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    disabled_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SourceFetch(Base):
    __tablename__ = "source_fetches"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    source_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status_code: Mapped[int | None]
    item_count: Mapped[int] = mapped_column(default=0)
    error: Mapped[str | None] = mapped_column(Text)


class RawArticle(Base):
    __tablename__ = "raw_articles"
    __table_args__ = (UniqueConstraint("source_id", "canonical_url", name="uq_raw_source_url"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    source_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"))
    canonical_url: Mapped[str] = mapped_column(String(1500))
    content_type: Mapped[str] = mapped_column(String(200))
    payload: Mapped[bytes] = mapped_column(LargeBinary)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    payload_hash: Mapped[str] = mapped_column(String(64))


class Article(Base, TimestampMixin):
    __tablename__ = "articles"
    __table_args__ = (
        UniqueConstraint("canonical_url", name="uq_article_url"),
        Index("ix_article_content_hash", "content_hash"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    raw_article_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("raw_articles.id", ondelete="RESTRICT"), unique=True
    )
    source_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("sources.id", ondelete="RESTRICT"))
    canonical_url: Mapped[str] = mapped_column(String(1500))
    title: Mapped[str] = mapped_column(String(1000))
    text: Mapped[str] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    image_url: Mapped[str | None] = mapped_column(String(1500))
    rejected_reason: Mapped[str | None] = mapped_column(Text)
    processing_state: Mapped[ArticleState] = mapped_column(String(20), default=ArticleState.NEW)


class Event(Base, TimestampMixin):
    __tablename__ = "events"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    title: Mapped[str] = mapped_column(String(1000))
    state: Mapped[EventState] = mapped_column(String(20), default=EventState.COLLECTING)
    confidence: Mapped[float] = mapped_column(Float, default=0)
    decision_reason: Mapped[str] = mapped_column(Text, default="awaiting verification")
    event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    articles: Mapped[list["EventArticle"]] = relationship(back_populates="event")


class EventArticle(Base):
    __tablename__ = "event_articles"

    event_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("events.id", ondelete="CASCADE"), primary_key=True
    )
    article_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("articles.id", ondelete="CASCADE"), primary_key=True
    )
    similarity: Mapped[float] = mapped_column(Float)
    event: Mapped[Event] = relationship(back_populates="articles")


class Claim(Base):
    __tablename__ = "claims"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"))
    source_article_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("articles.id", ondelete="CASCADE")
    )
    subject: Mapped[str] = mapped_column(String(500))
    predicate: Mapped[str] = mapped_column(String(1000))
    location: Mapped[list[str]] = mapped_column(JSON, default=list)
    event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    numbers: Mapped[list[str]] = mapped_column(JSON, default=list)
    names: Mapped[list[str]] = mapped_column(JSON, default=list)
    claim: Mapped[str] = mapped_column(Text)
    verified: Mapped[bool] = mapped_column(Boolean, default=False)
    verification_reason: Mapped[str] = mapped_column(Text, default="")


class Draft(Base, TimestampMixin):
    __tablename__ = "drafts"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"))
    title: Mapped[str] = mapped_column(String(240))
    body: Mapped[str] = mapped_column(Text)
    source_urls: Mapped[list[str]] = mapped_column(JSON)
    confidence: Mapped[float] = mapped_column(Float)
    validated: Mapped[bool] = mapped_column(Boolean, default=False)
    validation_reason: Mapped[str] = mapped_column(Text, default="")
    version: Mapped[int] = mapped_column(Integer, default=1)


class PublicationJob(Base, TimestampMixin):
    __tablename__ = "publication_jobs"
    __table_args__ = (UniqueConstraint("draft_id", "platform", name="uq_draft_platform"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    draft_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("drafts.id", ondelete="CASCADE"))
    platform: Mapped[Platform] = mapped_column(String(20))
    operation: Mapped[JobOperation] = mapped_column(String(20), default=JobOperation.PUBLISH)
    state: Mapped[JobState] = mapped_column(String(20), default=JobState.PENDING)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)


class PlatformPublication(Base):
    __tablename__ = "platform_publications"
    __table_args__ = (UniqueConstraint("job_id", name="uq_publication_job"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("publication_jobs.id", ondelete="RESTRICT")
    )
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.id", ondelete="RESTRICT"))
    platform: Mapped[Platform] = mapped_column(String(20))
    external_id: Mapped[str] = mapped_column(String(500))
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

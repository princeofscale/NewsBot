import secrets
from contextlib import asynccontextmanager
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from fastapi import status as http_status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from newsbot.config import get_settings
from newsbot.db import SessionFactory, engine, get_session
from newsbot.db_models import (
    Article,
    AuditLog,
    Event,
    EventArticle,
    PlatformPublication,
    PublicationJob,
    RuntimeControl,
    Source,
    utcnow,
)
from newsbot.logging import configure_logging
from newsbot.pipeline import CycleAlreadyRunning, CycleResult
from newsbot.runtime import close_clients, make_pipeline, make_worker
from newsbot.schemas import EventState, JobState, Platform, SourceInput
from newsbot.security import validate_public_url

settings = get_settings()
configure_logging(settings.log_level)
SessionDep = Annotated[AsyncSession, Depends(get_session)]


async def require_management(
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    if not settings.management_token:
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="management API is disabled",
        )
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.casefold() != "bearer" or not secrets.compare_digest(
        token, settings.management_token
    ):
        raise HTTPException(
            status_code=http_status.HTTP_401_UNAUTHORIZED,
            detail="invalid management credentials",
        )


ManagementDep = Annotated[None, Depends(require_management)]


def audit(action: str, target: str, **details: object) -> AuditLog:
    return AuditLog(
        actor="management_api",
        action=action,
        target=target,
        details=details,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    app.state.pipeline = make_pipeline(SessionFactory, settings)
    app.state.worker, app.state.publisher_clients = await make_worker(SessionFactory, settings)
    try:
        yield
    finally:
        try:
            await app.state.pipeline.close()
            await close_clients(app.state.publisher_clients)
        finally:
            await engine.dispose()


app = FastAPI(title="Saratov NewsBot", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
async def ready(session: SessionDep) -> dict[str, str]:
    try:
        await session.execute(text("select 1"))
    except Exception as error:
        raise HTTPException(status_code=503, detail="database unavailable") from error
    return {"status": "ready"}


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/sources")
async def list_sources(session: SessionDep, _: ManagementDep) -> list[dict[str, Any]]:
    sources = (await session.scalars(select(Source).order_by(Source.name))).all()
    return [
        {
            "id": str(source.id),
            "name": source.name,
            "kind": source.kind,
            "enabled": source.enabled,
            "failures": source.consecutive_failures,
            "disabled_until": source.disabled_until,
            "health": source.health_state,
            "last_error": source.last_error,
        }
        for source in sources
    ]


@app.post("/sources", status_code=201)
async def create_source(
    payload: SourceInput, session: SessionDep, _: ManagementDep
) -> dict[str, str]:
    if await session.scalar(select(Source.id).where(Source.name == payload.name)):
        raise HTTPException(status_code=409, detail="source already exists")
    if settings.validate_public_source_ips:
        try:
            await validate_public_url(str(payload.feed_url))
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
    source = Source(
        name=payload.name,
        base_url=str(payload.base_url),
        kind=payload.kind,
        feed_url=str(payload.feed_url),
        selectors=payload.selectors,
        trust_level=payload.trust_level,
        is_official=payload.is_official,
        origin_group=payload.origin_group,
        min_interval_seconds=payload.min_interval_seconds,
        enabled=payload.enabled,
    )
    session.add(source)
    session.add(audit("source.created", payload.name, feed_url=str(payload.feed_url)))
    await session.commit()
    return {"id": str(source.id)}


@app.put("/sources/{source_id}/enabled/{enabled}")
async def set_source_enabled(
    source_id: UUID, enabled: bool, session: SessionDep, _: ManagementDep
) -> dict[str, object]:
    source = await session.get(Source, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source not found")
    source.enabled = enabled
    session.add(audit("source.enabled", str(source_id), enabled=enabled))
    await session.commit()
    return {"id": str(source_id), "enabled": enabled}


@app.post("/cycles")
async def run_cycle(session: SessionDep, _: ManagementDep) -> CycleResult:
    try:
        result = cast(CycleResult, await app.state.pipeline.run_cycle())
    except CycleAlreadyRunning as error:
        raise HTTPException(status_code=409, detail="cycle_already_running") from error
    session.add(audit("cycle.run", "pipeline", errors=result.errors))
    await session.commit()
    return result


@app.post("/publications/run")
async def run_publications(session: SessionDep, _: ManagementDep) -> dict[str, int | bool]:
    processed = await app.state.worker.run_once()
    session.add(audit("publication.run", "outbox", processed=processed))
    await session.commit()
    return {"processed": processed, "dry_run": settings.dry_run}


@app.put("/admin/publication/{enabled}")
async def set_publication_enabled(
    enabled: bool, session: SessionDep, _: ManagementDep
) -> dict[str, bool]:
    control = await session.get(RuntimeControl, 1)
    if not control:
        control = RuntimeControl(id=1)
        session.add(control)
    control.publication_enabled = enabled
    session.add(audit("publication.enabled", "runtime", enabled=enabled))
    await session.commit()
    return {"publication_enabled": enabled}


@app.put("/admin/platforms/{platform}/{enabled}")
async def set_platform_enabled(
    platform: Platform, enabled: bool, session: SessionDep, _: ManagementDep
) -> dict[str, object]:
    control = await session.get(RuntimeControl, 1)
    if not control:
        control = RuntimeControl(id=1)
        session.add(control)
    if platform == Platform.TELEGRAM:
        control.telegram_enabled = enabled
    else:
        control.max_enabled = enabled
    session.add(audit("platform.enabled", platform.value, enabled=enabled))
    await session.commit()
    return {"platform": platform, "enabled": enabled}


@app.get("/publications/queue")
async def publication_queue(session: SessionDep, _: ManagementDep) -> list[dict[str, object]]:
    jobs = list(
        (
            await session.scalars(
                select(PublicationJob)
                .where(
                    PublicationJob.state.in_(
                        [
                            JobState.PENDING,
                            JobState.RETRY,
                            JobState.UNCERTAIN,
                            JobState.DEAD_LETTER,
                        ]
                    )
                )
                .order_by(PublicationJob.created_at)
                .limit(500)
            )
        ).all()
    )
    return [
        {
            "id": str(job.id),
            "draft_id": str(job.draft_id),
            "platform": job.platform,
            "state": job.state,
            "attempts": job.attempts,
            "next_attempt_at": job.next_attempt_at,
            "last_error": job.last_error,
        }
        for job in jobs
    ]


@app.get("/events/review")
async def review_queue(session: SessionDep, _: ManagementDep) -> list[dict[str, object]]:
    rows = (
        await session.execute(
            select(Event, Article.canonical_url)
            .join(EventArticle, EventArticle.event_id == Event.id)
            .join(Article, Article.id == EventArticle.article_id)
            .where(Event.requires_manual_review.is_(True))
            .order_by(Event.updated_at.desc())
            .limit(500)
        )
    ).all()
    grouped: dict[UUID, dict[str, object]] = {}
    for event, url in rows:
        item = grouped.setdefault(
            event.id,
            {
                "event_id": str(event.id),
                "title": event.title,
                "reason": event.decision_reason,
                "source_urls": [],
            },
        )
        urls = cast(list[str], item["source_urls"])
        if url not in urls:
            urls.append(url)
    return list(grouped.values())


@app.post("/events/{event_id}/approve")
async def approve_event(event_id: UUID, session: SessionDep, _: ManagementDep) -> dict[str, object]:
    event = await session.get(Event, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="event not found")
    event.manually_approved = True
    event.requires_manual_review = False
    session.add(audit("event.approved", str(event_id)))
    await session.commit()
    drafts_created = await app.state.pipeline.reprocess_event(event_id)
    return {"event_id": str(event_id), "drafts_created": drafts_created}


@app.post("/events/{event_id}/reprocess")
async def reprocess_event(
    event_id: UUID, session: SessionDep, _: ManagementDep
) -> dict[str, object]:
    session.add(audit("event.reprocessed", str(event_id)))
    await session.commit()
    try:
        drafts_created = await app.state.pipeline.reprocess_event(event_id)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    return {"event_id": str(event_id), "drafts_created": drafts_created}


@app.delete("/publications/{publication_id}")
async def delete_publication(
    publication_id: UUID, session: SessionDep, _: ManagementDep
) -> dict[str, str]:
    publication = await session.get(PlatformPublication, publication_id)
    if not publication or publication.deleted_at:
        raise HTTPException(status_code=404, detail="publication not found")
    publisher = app.state.worker.publishers[publication.platform]
    await publisher.delete(publication.external_id)
    publication.deleted_at = utcnow()
    session.add(audit("publication.deleted", str(publication_id)))
    await session.commit()
    return {"id": str(publication_id), "state": "deleted"}


@app.get("/status")
async def status(session: SessionDep, _: ManagementDep) -> dict[str, int]:
    rows = (
        await session.execute(select(Event.state, func.count(Event.id)).group_by(Event.state))
    ).all()
    counts = {state.value: 0 for state in EventState}
    counts.update({str(state): count for state, count in rows})
    for state, count in (
        await session.execute(
            select(PublicationJob.state, func.count(PublicationJob.id)).group_by(
                PublicationJob.state
            )
        )
    ).all():
        counts[f"outbox_{state}"] = count
    return counts

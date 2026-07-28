import secrets
from contextlib import asynccontextmanager
from typing import Annotated, Any, cast

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from fastapi import status as http_status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from newsbot.config import get_settings
from newsbot.db import SessionFactory, engine, get_session
from newsbot.db_models import Event, Source
from newsbot.logging import configure_logging
from newsbot.pipeline import CycleResult
from newsbot.runtime import make_pipeline, make_worker
from newsbot.schemas import EventState, SourceInput
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


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    app.state.pipeline = make_pipeline(SessionFactory, settings)
    app.state.worker, app.state.publisher_clients = await make_worker(SessionFactory, settings)
    yield
    await app.state.pipeline.fetcher.close()
    for client in app.state.publisher_clients:
        stop = getattr(client, "stop", None) or getattr(client, "close", None)
        if stop:
            await stop()
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
    await session.commit()
    return {"id": str(source.id)}


@app.post("/cycles")
async def run_cycle(_: ManagementDep) -> CycleResult:
    return cast(CycleResult, await app.state.pipeline.run_cycle())


@app.post("/publications/run")
async def run_publications(_: ManagementDep) -> dict[str, int | bool]:
    processed = await app.state.worker.run_once()
    return {"processed": processed, "dry_run": settings.dry_run}


@app.get("/status")
async def status(session: SessionDep, _: ManagementDep) -> dict[str, int]:
    rows = (
        await session.execute(select(Event.state, func.count(Event.id)).group_by(Event.state))
    ).all()
    counts = {state.value: 0 for state in EventState}
    counts.update({str(state): count for state, count in rows})
    return counts

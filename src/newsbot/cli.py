import asyncio
import json
import signal
from pathlib import Path
from uuid import UUID

import typer
import uvicorn
from sqlalchemy import and_, func, select

from newsbot.config import get_settings
from newsbot.db import SessionFactory, engine
from newsbot.db_models import AuditLog, Draft, Event
from newsbot.dedupe_benchmark import evaluate
from newsbot.fetcher import SourceFetcher
from newsbot.pipeline import PublicationWorker
from newsbot.runtime import (
    close_clients,
    make_pipeline,
    make_worker,
    production_configuration_errors,
)
from newsbot.source_check import check_sources
from newsbot.source_config import load_sources, sync_sources

app = typer.Typer(no_args_is_help=True)


@app.command()
def doctor() -> None:
    settings = get_settings()
    missing = production_configuration_errors(settings)
    typer.echo(
        {
            "dry_run": settings.dry_run,
            "llm_provider": settings.llm_base_url,
            "llm_model": settings.llm_model,
            "telegram_chat_id": settings.telegram_chat_id,
            "max_chat_id": settings.max_chat_id,
            "production_ready": not missing,
            "missing": missing,
        }
    )


@app.command()
def cycle() -> None:
    async def run() -> None:
        pipeline = make_pipeline(SessionFactory, get_settings())
        try:
            typer.echo(await pipeline.run_cycle())
        finally:
            await pipeline.close()
            await engine.dispose()

    asyncio.run(run())


@app.command("sources-check")
def sources_check() -> None:
    async def run() -> None:
        settings = get_settings()
        fetcher = SourceFetcher(settings)
        try:
            results = await check_sources(SessionFactory, fetcher)
            for result in results:
                typer.echo(json.dumps(result.to_dict(), ensure_ascii=False, default=str))
        finally:
            await fetcher.close()
            await engine.dispose()

    asyncio.run(run())


@app.command("sources-sync")
def sources_sync(path: Path = Path("config/sources.json")) -> None:
    async def run() -> None:
        async with SessionFactory() as session:
            created, updated, disabled = await sync_sources(session, load_sources(path))
        typer.echo(
            {
                "created": created,
                "updated": updated,
                "disabled": disabled,
                "path": str(path),
            }
        )

    asyncio.run(run())


@app.command("review-export")
def review_export(limit: int = 100, output: Path | None = None) -> None:
    async def run() -> None:
        async with SessionFactory() as session:
            latest_versions = (
                select(
                    Draft.event_id,
                    func.max(Draft.version).label("version"),
                )
                .group_by(Draft.event_id)
                .subquery()
            )
            rows = (
                await session.execute(
                    select(Draft, Event)
                    .join(
                        latest_versions,
                        and_(
                            latest_versions.c.event_id == Draft.event_id,
                            latest_versions.c.version == Draft.version,
                        ),
                    )
                    .join(Event, Event.id == Draft.event_id)
                    .order_by(Draft.created_at.desc())
                    .limit(min(max(limit, 1), 1000))
                )
            ).all()
        payload = [
            {
                "event_id": str(event.id),
                "event_state": event.state,
                "decision_reason": event.decision_reason,
                "draft_version": draft.version,
                "validated": draft.validated,
                "validation_reason": draft.validation_reason,
                "confidence": draft.confidence,
                "title": draft.title,
                "body": draft.body,
                "source_urls": draft.source_urls,
                "created_at": draft.created_at.isoformat(),
            }
            for draft, event in rows
        ]
        rendered = json.dumps(payload, ensure_ascii=False, indent=2)
        if output:
            await asyncio.to_thread(output.write_text, rendered)
            typer.echo({"exported": len(payload), "output": str(output)})
        else:
            typer.echo(rendered)

    asyncio.run(run())


@app.command("dedupe-evaluate")
def dedupe_evaluate(path: Path) -> None:
    typer.echo(evaluate(path))


@app.command("cost-estimate")
def cost_estimate(input_tokens: int, output_tokens: int) -> None:
    settings = get_settings()
    daily = (
        input_tokens * settings.llm_input_usd_per_million
        + output_tokens * settings.llm_output_usd_per_million
    ) / 1_000_000
    typer.echo({"daily_usd": round(daily, 4), "monthly_usd": round(daily * 30, 2)})


@app.command()
def publish() -> None:
    async def run() -> None:
        worker, clients = await make_worker(SessionFactory, get_settings())
        try:
            typer.echo({"processed": await worker.run_once(), "dry_run": get_settings().dry_run})
        finally:
            await close_clients(clients)
            await engine.dispose()

    asyncio.run(run())


@app.command()
def reconcile() -> None:
    async def run() -> None:
        worker, clients = await make_worker(SessionFactory, get_settings())
        try:
            typer.echo(await worker.reconcile_uncertain())
        finally:
            await close_clients(clients)
            await engine.dispose()

    asyncio.run(run())


@app.command("resolve-published")
def resolve_published(job_id: UUID, external_id: str) -> None:
    async def run() -> None:
        worker = PublicationWorker(SessionFactory, {})
        await worker.resolve_published(job_id, external_id)
        async with SessionFactory.begin() as session:
            session.add(
                AuditLog(
                    actor="cli",
                    action="publication.resolve_published",
                    target=str(job_id),
                    details={"external_id": external_id},
                )
            )
        await engine.dispose()
        typer.echo({"job_id": str(job_id), "state": "PUBLISHED"})

    asyncio.run(run())


@app.command("resolve-retry")
def resolve_retry(job_id: UUID) -> None:
    async def run() -> None:
        worker = PublicationWorker(SessionFactory, {})
        await worker.resolve_retry(job_id)
        async with SessionFactory.begin() as session:
            session.add(
                AuditLog(
                    actor="cli",
                    action="publication.resolve_retry",
                    target=str(job_id),
                    details={},
                )
            )
        await engine.dispose()
        typer.echo({"job_id": str(job_id), "state": "RETRY"})

    asyncio.run(run())


@app.command()
def worker() -> None:
    async def run() -> None:
        settings = get_settings()
        pipeline = make_pipeline(SessionFactory, settings)
        publication_worker, clients = await make_worker(SessionFactory, settings)
        stopping = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stopping.set)
        try:
            while not stopping.is_set():
                try:
                    await pipeline.run_cycle()
                    await publication_worker.reconcile_uncertain()
                    await publication_worker.run_once()
                except Exception as error:
                    typer.echo(f"worker cycle failed: {type(error).__name__}", err=True)
                try:
                    await asyncio.wait_for(
                        stopping.wait(), timeout=settings.worker_interval_seconds
                    )
                except TimeoutError:
                    pass
        finally:
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(signum)
            await pipeline.close()
            await close_clients(clients)
            await engine.dispose()

    asyncio.run(run())


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    uvicorn.run("newsbot.api:app", host=host, port=port)

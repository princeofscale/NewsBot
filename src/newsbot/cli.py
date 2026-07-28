import asyncio
import json
from pathlib import Path

import typer
import uvicorn
from sqlalchemy import select

from newsbot.config import get_settings
from newsbot.db import SessionFactory, engine
from newsbot.db_models import Base, Draft, Event
from newsbot.runtime import make_pipeline, make_worker, production_configuration_errors
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


@app.command("init-db")
def init_db() -> None:
    async def run() -> None:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await engine.dispose()

    asyncio.run(run())


@app.command()
def cycle() -> None:
    async def run() -> None:
        pipeline = make_pipeline(SessionFactory, get_settings())
        result = await pipeline.run_cycle()
        await pipeline.fetcher.close()
        typer.echo(result)

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
            rows = (
                await session.execute(
                    select(Draft, Event)
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


@app.command()
def publish() -> None:
    async def run() -> None:
        worker, clients = await make_worker(SessionFactory, get_settings())
        typer.echo({"processed": await worker.run_once(), "dry_run": get_settings().dry_run})
        for client in clients:
            stop = getattr(client, "stop", None) or getattr(client, "close", None)
            if stop:
                await stop()

    asyncio.run(run())


@app.command()
def worker() -> None:
    async def run() -> None:
        settings = get_settings()
        pipeline = make_pipeline(SessionFactory, settings)
        publication_worker, clients = await make_worker(SessionFactory, settings)
        try:
            while True:
                try:
                    await pipeline.run_cycle()
                    await publication_worker.run_once()
                except Exception as error:
                    typer.echo(f"worker cycle failed: {type(error).__name__}", err=True)
                await asyncio.sleep(settings.worker_interval_seconds)
        finally:
            await pipeline.fetcher.close()
            for client in clients:
                stop = getattr(client, "stop", None) or getattr(client, "close", None)
                if stop:
                    await stop()

    asyncio.run(run())


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    uvicorn.run("newsbot.api:app", host=host, port=port)

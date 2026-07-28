import asyncio

import typer
import uvicorn

from newsbot.config import get_settings
from newsbot.db import SessionFactory, engine
from newsbot.db_models import Base
from newsbot.runtime import make_pipeline, make_worker, production_configuration_errors

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

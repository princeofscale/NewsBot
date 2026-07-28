import asyncio
import inspect
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from newsbot.config import Settings
from newsbot.fetcher import SourceFetcher
from newsbot.llm import DeterministicLLM, OpenAICompatibleLLM
from newsbot.pipeline import Pipeline, PublicationWorker
from newsbot.publishers import (
    DryRunPublisher,
    MaxPyromaxPublisher,
    TelegramPyroforkPublisher,
)
from newsbot.schemas import LLMPort, Platform, Publisher


def _missing_publisher_configuration(settings: Settings) -> list[str]:
    missing = []
    telegram_target = (
        settings.telegram_test_chat_id
        if settings.environment == "staging"
        else settings.telegram_chat_id
    )
    max_target = (
        settings.max_test_chat_id
        if settings.environment == "staging"
        else settings.max_chat_id
    )
    if not all(
        [
            settings.telegram_api_id,
            settings.telegram_api_hash,
            settings.telegram_session_string,
            telegram_target,
        ]
    ):
        missing.append("Telegram Pyrofork credentials")
    if not settings.max_token or max_target is None:
        missing.append("MAX Pyromax credentials")
    return missing


async def close_clients(clients: list[Any]) -> None:
    for client in clients:
        try:
            transport = getattr(client, "transport", None)
            if transport is not None:
                method = getattr(transport, "close", None)
                if method:
                    result = method()
                    if inspect.isawaitable(result):
                        await result
            method = getattr(client, "stop", None) or getattr(client, "close", None)
            if method:
                result = method()
                if inspect.isawaitable(result):
                    await result
        except Exception:
            continue


def make_pipeline(
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    fetcher: SourceFetcher | None = None,
) -> Pipeline:
    llm: LLMPort
    if settings.llm_api_key:
        llm = OpenAICompatibleLLM(settings)
    elif settings.dry_run:
        llm = DeterministicLLM()
    else:
        raise RuntimeError("NEWSBOT_LLM_API_KEY is required when dry-run is disabled")
    return Pipeline(sessions, fetcher or SourceFetcher(settings), llm, settings)


def production_configuration_errors(settings: Settings) -> list[str]:
    errors = []
    if not settings.llm_api_key:
        errors.append("NEWSBOT_LLM_API_KEY")
    errors.extend(_missing_publisher_configuration(settings))
    return errors


async def make_publishers(settings: Settings) -> tuple[dict[Platform, Publisher], list[Any]]:
    if settings.dry_run:
        return {
            Platform.TELEGRAM: DryRunPublisher(Platform.TELEGRAM),
            Platform.MAX: DryRunPublisher(Platform.MAX),
        }, []

    missing = _missing_publisher_configuration(settings)
    if missing:
        raise RuntimeError("missing publisher configuration: " + ", ".join(missing))

    from pyrogram import Client
    from pyromax import MaxApi  # type: ignore[import-untyped]

    telegram = Client(
        "newsbot",
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash,
        session_string=settings.telegram_session_string,
        in_memory=True,
    )
    async with asyncio.timeout(settings.publisher_connect_timeout_seconds):
        await telegram.start()
    try:
        async with asyncio.timeout(settings.publisher_connect_timeout_seconds):
            max_client = await MaxApi(
                token=settings.max_token, password=settings.max_password or None
            )
    except Exception:
        await telegram.stop()
        raise
    telegram_chat_id = (
        settings.telegram_test_chat_id
        if settings.environment == "staging"
        else settings.telegram_chat_id
    )
    max_chat_id = (
        settings.max_test_chat_id
        if settings.environment == "staging"
        else settings.max_chat_id
    )
    assert max_chat_id is not None
    return {
        Platform.TELEGRAM: TelegramPyroforkPublisher(telegram, telegram_chat_id),
        Platform.MAX: MaxPyromaxPublisher(max_client, max_chat_id),
    }, [telegram, max_client]


async def make_worker(
    sessions: async_sessionmaker[AsyncSession], settings: Settings
) -> tuple[PublicationWorker, list[Any]]:
    publishers, clients = await make_publishers(settings)
    return (
        PublicationWorker(
            sessions,
            publishers,
            retry_base_seconds=settings.publication_retry_base_seconds,
            sending_stale_seconds=settings.sending_stale_seconds,
            publish_timeout_seconds=settings.publisher_timeout_seconds,
            publication_min_interval_seconds=settings.publication_min_interval_seconds,
            reconciliation_absence_delay_seconds=settings.reconciliation_absence_delay_seconds,
        ),
        clients,
    )

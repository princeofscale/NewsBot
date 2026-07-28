import pytest
from pydantic import ValidationError

from newsbot.config import Settings
from newsbot.db import SessionFactory
from newsbot.runtime import close_clients, make_pipeline, production_configuration_errors


def test_cheapvibecode_is_default_but_target_chats_are_environment_only() -> None:
    settings = Settings(_env_file=None)
    assert settings.llm_base_url == "https://cheapvibecode.ru/v1"
    assert settings.llm_model == "deepseek-v4-flash"
    assert settings.telegram_chat_id == ""
    assert settings.max_chat_id is None


def test_production_requires_real_llm_credentials() -> None:
    settings = Settings(dry_run=False, llm_api_key="", _env_file=None)
    with pytest.raises(RuntimeError, match="NEWSBOT_LLM_API_KEY"):
        make_pipeline(SessionFactory, settings)
    assert "NEWSBOT_LLM_API_KEY" in production_configuration_errors(settings)


def test_staging_requires_test_targets_and_environment_is_validated() -> None:
    settings = Settings(
        environment="staging",
        llm_api_key="key",
        telegram_api_id=1,
        telegram_api_hash="hash",
        telegram_session_string="session",
        telegram_chat_id="-100-production",
        max_token="token",
        max_chat_id=-1,
        _env_file=None,
    )
    assert "Telegram Pyrofork credentials" in production_configuration_errors(settings)
    assert "MAX Pyromax credentials" in production_configuration_errors(settings)
    configured = settings.model_copy(
        update={"telegram_test_chat_id": "-100-test", "max_test_chat_id": -2}
    )
    assert production_configuration_errors(configured) == []
    with pytest.raises(ValidationError):
        Settings(environment="typo", _env_file=None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_close_clients_closes_pyromax_transport_and_pyrofork() -> None:
    class Transport:
        closed = False

        async def close(self) -> None:
            self.closed = True

    class MaxClient:
        transport = Transport()

    class TelegramClient:
        stopped = False

        async def stop(self) -> None:
            self.stopped = True

    max_client = MaxClient()
    telegram = TelegramClient()

    await close_clients([telegram, max_client])

    assert telegram.stopped
    assert max_client.transport.closed

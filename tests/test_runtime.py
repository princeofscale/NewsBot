import pytest

from newsbot.config import Settings
from newsbot.db import SessionFactory
from newsbot.runtime import make_pipeline, production_configuration_errors


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

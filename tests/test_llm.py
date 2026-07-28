import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from newsbot.config import Settings
from newsbot.llm import OpenAICompatibleLLM, _message_content


def test_message_content_accepts_cheapvibecode_framing() -> None:
    payload = json.dumps({"choices": [{"message": {"content": '{"ok":true}'}}]})
    for prefix in ("", "\r\n", "\r\ndata: "):
        response = prefix + payload + "data: [DONE]\n\n"
        assert _message_content(response) == '{"ok":true}'


def test_message_content_accepts_openai_object() -> None:
    response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content='{"ok":true}'))]
    )
    assert _message_content(response) == '{"ok":true}'


class FakeCompletions:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []

    async def create(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


@pytest.mark.asyncio
async def test_llm_receives_json_schema_and_retries_empty_or_timeout() -> None:
    article_id = uuid4()
    content = json.dumps(
        {
            "claims": [
                {
                    "subject": "Саратовводоканал",
                    "predicate": "отключит воду",
                    "claim": "Саратовводоканал отключит воду",
                    "source_article_id": str(article_id),
                }
            ]
        }
    )
    completions = FakeCompletions(
        [
            TimeoutError(),
            SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=""))]),
            SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))]),
        ]
    )
    llm = OpenAICompatibleLLM(
        Settings(llm_api_key="test", llm_retries=3, llm_retry_base_seconds=0, _env_file=None)
    )
    llm.client = SimpleNamespace(chat=SimpleNamespace(completions=completions))

    claims = await llm.extract_claims(article_id, "Отключение воды", "Текст")

    assert claims[0].source_article_id == article_id
    user_payload = json.loads(completions.calls[-1]["messages"][1]["content"])  # type: ignore[index]
    assert user_payload["json_schema"]["properties"]["claims"]

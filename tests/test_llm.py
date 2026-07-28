import json
from types import SimpleNamespace

from newsbot.llm import _message_content


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

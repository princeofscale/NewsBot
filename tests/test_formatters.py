from uuid import uuid4

from newsbot.formatters import format_max, format_telegram, publication_fingerprint
from newsbot.schemas import Post


def test_platform_formatters_escape_and_differ() -> None:
    post = Post(
        title="<Событие>",
        body="Текст & детали",
        source_urls=["https://example.test/?a=1&b=2"],
        event_id=uuid4(),
        confidence=0.9,
    )
    telegram = format_telegram(post)
    max_text = format_max(post)
    assert "&lt;Событие&gt;" in telegram
    assert "<Событие>" in max_text
    assert telegram != max_text
    assert publication_fingerprint(post) in telegram
    assert publication_fingerprint(post) in max_text
    assert publication_fingerprint(post) != publication_fingerprint(
        post.model_copy(update={"body": "Уточнённый текст"})
    )

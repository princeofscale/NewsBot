from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from newsbot.formatters import format_max, format_telegram
from newsbot.schemas import Platform, Post, PublicationResult


class DryRunPublisher:
    def __init__(self, platform: Platform) -> None:
        self.platform = platform
        self.posts: dict[str, Post] = {}

    async def publish(self, post: Post) -> PublicationResult:
        publication_id = f"dry-{uuid4()}"
        self.posts[publication_id] = post
        return PublicationResult(
            publication_id=publication_id,
            platform=self.platform,
            published_at=datetime.now(UTC),
        )

    async def edit(self, publication_id: str, post: Post) -> None:
        self.posts[publication_id] = post

    async def delete(self, publication_id: str) -> None:
        self.posts.pop(publication_id, None)


class TelegramPyroforkPublisher:
    def __init__(self, client: Any, chat_id: str) -> None:
        self.client = client
        self.chat_id = chat_id

    async def publish(self, post: Post) -> PublicationResult:
        message = await self.client.send_message(
            self.chat_id, format_telegram(post), parse_mode="html", disable_web_page_preview=True
        )
        return PublicationResult(
            publication_id=str(message.id),
            platform=Platform.TELEGRAM,
            published_at=datetime.now(UTC),
        )

    async def edit(self, publication_id: str, post: Post) -> None:
        await self.client.edit_message_text(
            self.chat_id, int(publication_id), format_telegram(post), parse_mode="html"
        )

    async def delete(self, publication_id: str) -> None:
        await self.client.delete_messages(self.chat_id, int(publication_id))


class MaxPyromaxPublisher:
    def __init__(self, client: Any, chat_id: int) -> None:
        self.client = client
        self.chat_id = chat_id

    async def publish(self, post: Post) -> PublicationResult:
        message = await self.client.send_message(chat_id=self.chat_id, text=format_max(post))
        if message is None:
            raise RuntimeError("Pyromax returned no message")
        message_id = getattr(message, "message_id", None)
        if message_id is None:
            raise RuntimeError("Pyromax response has no message_id")
        return PublicationResult(
            publication_id=str(message_id),
            platform=Platform.MAX,
            published_at=datetime.now(UTC),
        )

    async def edit(self, publication_id: str, post: Post) -> None:
        raise NotImplementedError("Pyromax 0.7.x has no supported edit-message API")

    async def delete(self, publication_id: str) -> None:
        raise NotImplementedError("Pyromax 0.7.x has no supported delete-message API")

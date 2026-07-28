import asyncio
import json
import random
import re
from typing import Any, TypeVar, cast
from uuid import UUID

from openai import (
    APIConnectionError,
    APITimeoutError,
    AsyncOpenAI,
    InternalServerError,
    RateLimitError,
)
from pydantic import BaseModel, Field

from newsbot.config import Settings
from newsbot.schemas import ClaimPayload, Post

SchemaT = TypeVar("SchemaT", bound=BaseModel)
NUMBER_RE = re.compile(r"\b\d+(?:[.,]\d+)?\b")


def _message_content(response: object) -> str:
    if isinstance(response, str):
        framed = response.lstrip()
        if framed.startswith("data:"):
            framed = framed.removeprefix("data:").lstrip()
        payload, _ = json.JSONDecoder().raw_decode(framed)
        return str(payload["choices"][0]["message"]["content"] or "")
    completion = cast(Any, response)
    return str(completion.choices[0].message.content or "")


class ClaimsResponse(BaseModel):
    claims: list[ClaimPayload]


class VerificationResponse(BaseModel):
    valid: bool
    unsupported: list[str] = Field(default_factory=list)


class OpenAICompatibleLLM:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = AsyncOpenAI(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            timeout=settings.llm_timeout_seconds,
        )

    async def _json(
        self, system: str, payload: dict[str, object], schema: type[SchemaT]
    ) -> SchemaT:
        error: Exception | None = None
        request_payload = {**payload, "json_schema": schema.model_json_schema()}
        for attempt in range(self.settings.llm_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=self.settings.llm_model,
                    response_format={"type": "json_object"},
                    temperature=0,
                    messages=[
                        {"role": "system", "content": system},
                        {
                            "role": "user",
                            "content": json.dumps(request_payload, ensure_ascii=False),
                        },
                    ],
                )
                content = _message_content(response)
                if not content.strip():
                    raise ValueError("LLM returned an empty response")
                return schema.model_validate_json(content)
            except (
                ValueError,
                TypeError,
                KeyError,
                IndexError,
                AttributeError,
                TimeoutError,
                APITimeoutError,
                APIConnectionError,
                RateLimitError,
                InternalServerError,
            ) as exc:
                error = exc
                if attempt + 1 < self.settings.llm_retries:
                    base = self.settings.llm_retry_base_seconds * 2**attempt
                    await asyncio.sleep(base + random.uniform(0, base * 0.2))
        raise ValueError(f"LLM returned invalid structured response: {error}")

    async def extract_claims(self, article_id: UUID, title: str, text: str) -> list[ClaimPayload]:
        result = await self._json(
            "Extract only explicit facts. Return JSON {claims:[...]}, "
            "matching the supplied schema.",
            {"article_id": str(article_id), "title": title, "text": text},
            ClaimsResponse,
        )
        return result.claims

    async def create_post(
        self,
        event_id: UUID,
        claims: list[ClaimPayload],
        source_urls: list[str],
        confidence: float,
    ) -> Post:
        result = await self._json(
            "Write a neutral Russian news post using only claims. "
            "Return Post JSON. Max 4000 chars.",
            {
                "event_id": str(event_id),
                "claims": [claim.model_dump(mode="json") for claim in claims],
                "source_urls": source_urls[:3],
                "confidence": confidence,
            },
            Post,
        )
        post = result
        if post.length > 4000:
            raise ValueError("generated post exceeds 4000 characters")
        return post

    async def verify_post(self, post: Post, claims: list[ClaimPayload]) -> bool:
        result = await self._json(
            "Check whether every draft fact is present in claims. Return {valid, unsupported}.",
            {
                "post": post.model_dump(mode="json"),
                "claims": [claim.model_dump(mode="json") for claim in claims],
            },
            VerificationResponse,
        )
        return result.valid


class DeterministicLLM:
    """Free local implementation for tests and dry development."""

    async def extract_claims(self, article_id: UUID, title: str, text: str) -> list[ClaimPayload]:
        numbers = NUMBER_RE.findall(text)
        return [
            ClaimPayload(
                subject=title,
                predicate=text[:300],
                claim=text[:300],
                numbers=numbers,
                source_article_id=article_id,
            )
        ]

    async def create_post(
        self,
        event_id: UUID,
        claims: list[ClaimPayload],
        source_urls: list[str],
        confidence: float,
    ) -> Post:
        return Post(
            title=claims[0].subject[:240],
            body=claims[0].claim,
            source_urls=source_urls[:3],
            event_id=event_id,
            confidence=confidence,
        )

    async def verify_post(self, post: Post, claims: list[ClaimPayload]) -> bool:
        corpus = " ".join(claim.claim for claim in claims).casefold()
        return all(number in corpus for number in NUMBER_RE.findall(post.body))

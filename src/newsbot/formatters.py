import hashlib
import html

from newsbot.schemas import Post


def publication_fingerprint(post: Post) -> str:
    content = "\0".join([post.title, post.body, *post.source_urls]).encode()
    return f"#nb_{post.event_id.hex[:10]}_{hashlib.sha256(content).hexdigest()[:10]}"


def format_telegram(post: Post) -> str:
    links = "\n".join(
        f'• <a href="{html.escape(url, quote=True)}">Источник</a>' for url in post.source_urls
    )
    return (
        f"<b>{html.escape(post.title)}</b>\n\n{html.escape(post.body)}\n\n"
        f"{links}\n\n{publication_fingerprint(post)}"
    )


def format_max(post: Post) -> str:
    links = "\n".join(f"Источник: {url}" for url in post.source_urls)
    return f"**{post.title}**\n\n{post.body}\n\n{links}\n\n{publication_fingerprint(post)}"

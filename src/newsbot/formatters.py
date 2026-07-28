import html

from newsbot.schemas import Post


def format_telegram(post: Post) -> str:
    links = "\n".join(
        f'• <a href="{html.escape(url, quote=True)}">Источник</a>' for url in post.source_urls
    )
    return f"<b>{html.escape(post.title)}</b>\n\n{html.escape(post.body)}\n\n{links}"


def format_max(post: Post) -> str:
    links = "\n".join(f"Источник: {url}" for url in post.source_urls)
    return f"**{post.title}**\n\n{post.body}\n\n{links}"

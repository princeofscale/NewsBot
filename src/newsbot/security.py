import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit


async def validate_public_url(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
        raise ValueError("source URL must be credential-free HTTPS")
    addresses = await asyncio.to_thread(
        socket.getaddrinfo, parts.hostname, parts.port or 443, type=socket.SOCK_STREAM
    )
    if not addresses or any(
        not ipaddress.ip_address(address[4][0]).is_global for address in addresses
    ):
        raise ValueError("source URL must resolve only to public addresses")

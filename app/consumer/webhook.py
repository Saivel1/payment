import hashlib
import hmac
import ipaddress
import socket
import time

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult
from aiohttp.resolver import DefaultResolver

from app.schemas import WebhookPayload
from app.url_safety import is_public_ip, validate_webhook_url


class WebhookDeliveryError(Exception):
    pass


class PublicOnlyResolver(AbstractResolver):
    """Refuses hostnames that resolve to private/loopback/link-local/reserved addresses.

    The check runs on the addresses aiohttp actually connects to, so DNS rebinding
    between validation and connection is not possible."""

    def __init__(self) -> None:
        self._resolver = DefaultResolver()

    async def resolve(self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET) -> list[ResolveResult]:
        results = await self._resolver.resolve(host, port, family)
        for result in results:
            if not is_public_ip(ipaddress.ip_address(result["host"])):
                raise OSError(f"Refusing to connect to {host}: resolves to a non-public address")
        return results

    async def close(self) -> None:
        await self._resolver.close()


class WebhookSender:
    def __init__(self, *, secret: str, timeout: float, allow_private_networks: bool) -> None:
        self._secret = secret.encode()
        self._timeout = timeout
        self._allow_private = allow_private_networks
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        connector = aiohttp.TCPConnector(
            resolver=DefaultResolver() if self._allow_private else PublicOnlyResolver(),
            limit=100,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=self._timeout),
            trust_env=False,  # never route through env-configured proxies
            headers={"User-Agent": "payments-service-webhooks/1.0"},
        )

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def send(self, url: str, payload: WebhookPayload) -> None:
        if self._session is None:
            raise RuntimeError("WebhookSender is not started")
        try:
            # Re-check here: literal IPs bypass the resolver.
            validate_webhook_url(url, allow_private=self._allow_private)
        except ValueError as exc:
            raise WebhookDeliveryError(str(exc)) from exc

        body = payload.model_dump_json().encode()
        timestamp = str(int(time.time()))
        signature = hmac.new(self._secret, timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Id": str(payload.payment_id),
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": f"sha256={signature}",
        }
        try:
            # Redirects are not followed: they could lead to an internal address.
            async with self._session.post(url, data=body, headers=headers, allow_redirects=False) as response:
                if not 200 <= response.status < 300:
                    raise WebhookDeliveryError(f"Webhook endpoint responded with HTTP {response.status}")
        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc

"""SSRF protection for webhook URLs.

Two layers:
  * `validate_webhook_url` — static checks on the URL itself (scheme, credentials, literal IPs,
    internal hostnames). Runs at the API and again right before sending.
  * `is_public_ip` — used by the consumer's DNS resolver to refuse connecting to any hostname
    that resolves to a non-public address (covers DNS rebinding as well, since the check
    happens on the exact addresses the connection is made to).
"""

import ipaddress
from urllib.parse import urlsplit

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


def is_public_ip(ip: IPAddress) -> bool:
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_global and not ip.is_multicast


def validate_webhook_url(url: str, *, allow_private: bool) -> None:
    """Raise ValueError if the URL must not be used as a webhook target."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise ValueError("webhook_url must use http or https")
    if parts.username is not None or parts.password is not None:
        raise ValueError("webhook_url must not contain credentials")
    host = parts.hostname
    if not host:
        raise ValueError("webhook_url must contain a host")
    if allow_private:
        return

    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        if not is_public_ip(ip):
            raise ValueError("webhook_url must not point to a private or reserved address")
        return

    host = host.rstrip(".").lower()
    # Single-label names ("postgres", "rabbitmq", "api") are resolved by the docker DNS to internal services.
    if "." not in host or host.endswith((".localhost", ".local", ".internal")):
        raise ValueError("webhook_url must point to a public host")

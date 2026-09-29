"""Webhook destination policy, independent of persistence and HTTP clients."""

import ipaddress
from urllib.parse import urlsplit


class UnsafeWebhookURL(ValueError):
    """A destination violates the public HTTPS-only webhook policy."""


def validate_public_address(address: str) -> None:
    """Reject internal, special-purpose, and IPv6 transition addresses."""
    ip = ipaddress.ip_address(address)
    if (
        not ip.is_global
        or ip.is_multicast
        or ip.is_reserved
        or (
            isinstance(ip, ipaddress.IPv6Address)
            and (
                ip.ipv4_mapped is not None
                or ip.sixtofour is not None
                or ip.teredo is not None
                or ip in ipaddress.ip_network("64:ff9b::/96")
            )
        )
    ):
        raise UnsafeWebhookURL("Webhook must resolve only to public addresses")


def validate_webhook_url(value: str) -> str:
    """Validate syntax and literal addresses; DNS is checked at delivery time."""
    if len(value) > 2048 or any(ord(c) <= 32 or ord(c) == 127 for c in value):
        raise UnsafeWebhookURL("Invalid webhook URL")
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        if (
            parsed.scheme != "https"
            or not host
            or parsed.port not in (None, 443)
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or "\\" in value
            or "%" in host
        ):
            raise UnsafeWebhookURL("Webhook requires public HTTPS on port 443")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            normalized = host.rstrip(".").lower()
            if "." not in normalized or normalized.endswith(
                (".localhost", ".local", ".internal", ".home.arpa")
            ):
                raise UnsafeWebhookURL(
                    "Local webhook destinations are forbidden"
                ) from None
        else:
            validate_public_address(host)
    except ValueError as exc:
        raise UnsafeWebhookURL("Invalid or non-public webhook URL") from exc
    return value

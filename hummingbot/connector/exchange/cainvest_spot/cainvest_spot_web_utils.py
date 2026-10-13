"""
Web utils for Cainvest Spot connector.
Provides REST URL builders and server time for ExchangePyBase time synchronizer.
"""
import time
from typing import Optional

import hummingbot.connector.exchange.cainvest_spot.cainvest_spot_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def _base_rest_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    env = CONSTANTS.ENVIRONMENTS.get(domain, CONSTANTS.ENVIRONMENTS["production"])
    return env["rest_url"].rstrip("/")


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Full URL for a public REST endpoint."""
    base = _base_rest_url(domain)
    path = path_url.lstrip("/")
    return f"{base}/{path}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Full URL for a private REST endpoint (same base as public for Cainvest)."""
    return public_rest_url(path_url, domain)


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """
    Returns current server time in milliseconds.
    Cainvest API does not expose a dedicated time endpoint; we use local time.
    Auth uses only API key/secret (no timestamp signing), so this is sufficient.
    """
    return time.time() * 1000.0

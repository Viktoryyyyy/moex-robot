from __future__ import annotations

from typing import Callable
from urllib.request import Request, urlopen

from .usdrubf_news_macro_runtime import RuntimeIntegrationError


FLOWISE_API_KEY_ENV = "MOEX_RUB_INTELLIGENCE_FLOWISE_API_KEY"


def flowise_bearer_opener(
    api_key: str,
    *,
    opener: Callable[..., object] = urlopen,
) -> Callable[..., object]:
    """Return an opener that adds the Flowise Bearer API key at call time.

    The secret is supplied by runtime environment only. It is never logged,
    serialized into request payloads, or persisted by this helper.
    """

    if not isinstance(api_key, str) or not api_key.strip():
        raise RuntimeIntegrationError("Flowise API key must be non-empty")
    token = api_key.strip()

    def _authenticated_open(request: Request, *, timeout: float) -> object:
        request.add_header("Authorization", f"Bearer {token}")
        return opener(request, timeout=timeout)

    return _authenticated_open

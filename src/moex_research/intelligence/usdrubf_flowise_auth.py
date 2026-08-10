from __future__ import annotations

from typing import Callable
from urllib.request import Request, urlopen

from .usdrubf_news_macro_runtime import RuntimeIntegrationError


FLOWISE_API_KEY_ENV = "MOEX_RUB_INTELLIGENCE_FLOWISE_API_KEY"
FLOWISE_USER_AGENT = "Mozilla/5.0"


def flowise_bearer_opener(
    api_key: str,
    *,
    opener: Callable[..., object] = urlopen,
) -> Callable[..., object]:
    """Return an opener that adds Flowise runtime HTTP headers at call time.

    The secret is supplied by runtime environment only. It is never logged,
    serialized into request payloads, or persisted by this helper. The explicit
    User-Agent is required by the deployed Flowise ingress path; the default
    Python urllib signature is rejected before the request reaches Flowise.
    """

    if not isinstance(api_key, str) or not api_key.strip():
        raise RuntimeIntegrationError("Flowise API key must be non-empty")
    token = api_key.strip()

    def _authenticated_open(request: Request, *, timeout: float) -> object:
        request.add_header("Authorization", f"Bearer {token}")
        request.add_header("User-Agent", FLOWISE_USER_AGENT)
        return opener(request, timeout=timeout)

    return _authenticated_open

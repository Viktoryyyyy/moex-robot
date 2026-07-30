from __future__ import annotations

import math
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from email.message import Message
from http.client import HTTPResponse
from typing import Any, Final
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

ALGOPACK_TOKEN_ENV: Final[str] = "MOEX_ALGOPACK_TOKEN"
HTTP_MAX_ATTEMPTS: Final[int] = 5
HTTP_RETRY_DELAYS_SECONDS: Final[tuple[float, ...]] = (0.5, 1.0, 2.0, 4.0)
MAX_RETRY_AFTER_SECONDS: Final[float] = 60.0

RouteValidator = Callable[[str], object]
HttpOpener = Callable[..., HTTPResponse]
Sleeper = Callable[[float], None]


@dataclass(frozen=True)
class AlgoPackHttpContext:
    status_code: int | None
    retryable: bool
    retry_after_seconds: float | None
    sanitized_header_markers: tuple[str, ...]
    transport_outcome: str


class AlgoPackHttpError(ValueError):
    """Sanitized generic AlgoPack transport error without source-specific blockers."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retryable: bool = False,
        retry_after_seconds: float | None = None,
        sanitized_header_markers: tuple[str, ...] = (),
        transport_outcome: str,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = bool(retryable)
        self.retry_after_seconds = retry_after_seconds
        self.sanitized_header_markers = tuple(sanitized_header_markers)
        self.transport_outcome = transport_outcome

    @property
    def context(self) -> AlgoPackHttpContext:
        return AlgoPackHttpContext(
            status_code=self.status_code,
            retryable=self.retryable,
            retry_after_seconds=self.retry_after_seconds,
            sanitized_header_markers=self.sanitized_header_markers,
            transport_outcome=self.transport_outcome,
        )


class RejectAllRedirects(HTTPRedirectHandler):
    """Reject every redirect so an Authorization header cannot be forwarded."""

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


def open_without_redirects(request: Request, timeout: int) -> HTTPResponse:
    return build_opener(RejectAllRedirects()).open(request, timeout=timeout)


def load_algopack_token() -> str:
    token = os.environ.get(ALGOPACK_TOKEN_ENV, "").strip()
    if not token:
        raise AlgoPackHttpError(
            "AlgoPack token environment variable is not configured",
            transport_outcome="token_env_not_configured",
        )
    return token


def _bounded_retry_after(headers: Mapping[str, Any] | Message | None) -> float | None:
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(str(value).strip())
    except ValueError:
        return None
    if (
        not math.isfinite(seconds)
        or seconds < 0
        or seconds > MAX_RETRY_AFTER_SECONDS
    ):
        return None
    return seconds


def _sanitized_header_markers(
    headers: Mapping[str, Any] | Message | None,
) -> tuple[str, ...]:
    if headers is None:
        return ()
    values: list[str] = []
    for name in ("X-MOEX-Error-Code", "X-Error-Code", "X-Route-Status"):
        raw = str(headers.get(name, "") or "").strip()
        if raw:
            values.append(f"{name.lower()}={raw[:128]}")
    return tuple(values)


def fetch_algopack_bytes(
    url: str,
    bearer_token: str,
    *,
    route_validator: RouteValidator,
    opener: HttpOpener = open_without_redirects,
    timeout: int = 30,
    user_agent: str = "moex-robot-algopack/1.0",
) -> bytes:
    route_validator(str(url))
    token = str(bearer_token).strip()
    if not token:
        raise AlgoPackHttpError(
            "AlgoPack token environment variable is not configured",
            transport_outcome="token_env_not_configured",
        )
    request = Request(
        str(url),
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": user_agent,
        },
    )
    try:
        with opener(request, timeout=timeout) as response:
            payload = response.read()
    except HTTPError as exc:
        status_code = int(exc.code)
        markers = _sanitized_header_markers(exc.headers)
        retry_after = _bounded_retry_after(exc.headers)
        if 300 <= status_code < 400:
            raise AlgoPackHttpError(
                "AlgoPack redirect was refused",
                status_code=status_code,
                sanitized_header_markers=markers,
                transport_outcome="algopack_http_redirect_refused",
            ) from None
        if status_code == 401:
            raise AlgoPackHttpError(
                "AlgoPack authentication failed",
                status_code=status_code,
                sanitized_header_markers=markers,
                transport_outcome="algopack_authentication_failed",
            ) from None
        if status_code == 403:
            raise AlgoPackHttpError(
                "AlgoPack subscription is not entitled",
                status_code=status_code,
                sanitized_header_markers=markers,
                transport_outcome="algopack_subscription_not_entitled",
            ) from None
        if status_code == 404:
            raise AlgoPackHttpError(
                "AlgoPack resource was not found",
                status_code=status_code,
                sanitized_header_markers=markers,
                transport_outcome="algopack_http_not_found",
            ) from None
        if status_code == 429:
            raise AlgoPackHttpError(
                "AlgoPack rate limit blocked the request",
                status_code=status_code,
                retryable=True,
                retry_after_seconds=retry_after,
                sanitized_header_markers=markers,
                transport_outcome="algopack_rate_limit_blocked",
            ) from None
        if 500 <= status_code <= 599:
            raise AlgoPackHttpError(
                "AlgoPack service is temporarily unavailable",
                status_code=status_code,
                retryable=True,
                retry_after_seconds=retry_after,
                sanitized_header_markers=markers,
                transport_outcome="algopack_http_service_unavailable",
            ) from None
        raise AlgoPackHttpError(
            "AlgoPack HTTP response is not accepted",
            status_code=status_code,
            sanitized_header_markers=markers,
            transport_outcome="algopack_http_unaccepted",
        ) from None
    except (URLError, TimeoutError, OSError):
        raise AlgoPackHttpError(
            "AlgoPack transport is temporarily unavailable",
            retryable=True,
            transport_outcome="algopack_http_service_unavailable",
        ) from None
    if not payload:
        raise AlgoPackHttpError(
            "AlgoPack response is empty",
            retryable=True,
            transport_outcome="algopack_http_service_unavailable",
        )
    return payload


def fetch_algopack_bytes_with_retry(
    url: str,
    bearer_token: str,
    *,
    route_validator: RouteValidator,
    opener: HttpOpener = open_without_redirects,
    sleeper: Sleeper = time.sleep,
    timeout: int = 30,
    user_agent: str = "moex-robot-algopack/1.0",
) -> bytes:
    error: AlgoPackHttpError | None = None
    for attempt in range(HTTP_MAX_ATTEMPTS):
        try:
            return fetch_algopack_bytes(
                url,
                bearer_token,
                route_validator=route_validator,
                opener=opener,
                timeout=timeout,
                user_agent=user_agent,
            )
        except AlgoPackHttpError as exc:
            error = exc
        if not error.retryable:
            raise error
        if attempt + 1 == HTTP_MAX_ATTEMPTS:
            raise AlgoPackHttpError(
                "AlgoPack source remained unavailable after bounded retries",
                status_code=error.status_code,
                sanitized_header_markers=error.sanitized_header_markers,
                transport_outcome=error.transport_outcome,
            ) from None
        delay = error.retry_after_seconds
        if delay is None:
            delay = HTTP_RETRY_DELAYS_SECONDS[attempt]
        sleeper(delay)
    raise AssertionError("unreachable retry state")


__all__ = [
    "ALGOPACK_TOKEN_ENV",
    "HTTP_MAX_ATTEMPTS",
    "HTTP_RETRY_DELAYS_SECONDS",
    "MAX_RETRY_AFTER_SECONDS",
    "AlgoPackHttpContext",
    "AlgoPackHttpError",
    "HttpOpener",
    "RejectAllRedirects",
    "RouteValidator",
    "Sleeper",
    "fetch_algopack_bytes",
    "fetch_algopack_bytes_with_retry",
    "load_algopack_token",
    "open_without_redirects",
]

from __future__ import annotations

from email.message import Message
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from moex_research.external_data import moex_algopack_http as http


TOKEN = "super-secret-token"
URL = "https://apim.moex.com/iss/example.json?from=2026-01-01"


class Response:
    def __init__(self, payload: bytes = b'{"ok":true}') -> None:
        self.payload = payload

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def exact_route(url: str) -> None:
    if url != URL:
        raise ValueError("not allowlisted")


def test_route_is_validated_before_request_construction() -> None:
    calls = 0

    def opener(*_args: object, **_kwargs: object) -> Response:
        nonlocal calls
        calls += 1
        return Response()

    with pytest.raises(ValueError):
        http.fetch_algopack_bytes(
            "https://evil.example/x",
            TOKEN,
            route_validator=exact_route,
            opener=opener,
        )
    assert calls == 0


def test_bearer_header_and_redirect_rejection() -> None:
    seen: Request | None = None

    def opener(request: Request, timeout: int) -> Response:
        nonlocal seen
        seen = request
        assert timeout == 30
        return Response()

    assert (
        http.fetch_algopack_bytes(
            URL,
            TOKEN,
            route_validator=exact_route,
            opener=opener,
        )
        == b'{"ok":true}'
    )
    assert seen is not None
    assert seen.get_header("Authorization") == f"Bearer {TOKEN}"
    assert seen.get_header("Accept") == "application/json"
    assert (
        http.RejectAllRedirects().redirect_request(
            seen,
            None,
            302,
            "Found",
            Message(),
            "https://evil.example/collect",
        )
        is None
    )


@pytest.mark.parametrize(
    "status,outcome,retryable",
    [
        (401, "algopack_authentication_failed", False),
        (403, "algopack_subscription_not_entitled", False),
        (404, "algopack_http_not_found", False),
        (429, "algopack_rate_limit_blocked", True),
        (503, "algopack_http_service_unavailable", True),
    ],
)
def test_http_outcomes_are_generic_and_sanitized(
    status: int,
    outcome: str,
    retryable: bool,
) -> None:
    headers = Message()
    headers["X-MOEX-Error-Code"] = "ticker-not-found"
    error = HTTPError(URL, status, "sanitized", headers, None)

    def opener(_request: Request, timeout: int) -> Response:
        assert timeout == 30
        raise error

    with pytest.raises(http.AlgoPackHttpError) as raised:
        http.fetch_algopack_bytes(
            URL,
            TOKEN,
            route_validator=exact_route,
            opener=opener,
        )
    value = raised.value
    assert value.transport_outcome == outcome
    assert value.status_code == status
    assert value.retryable is retryable
    assert value.sanitized_header_markers == (
        "x-moex-error-code=ticker-not-found",
    )
    assert TOKEN not in str(value)
    assert TOKEN not in repr(value)


def test_missing_token_and_empty_payload_fail_closed() -> None:
    with pytest.raises(http.AlgoPackHttpError) as raised:
        http.fetch_algopack_bytes(
            URL,
            "",
            route_validator=exact_route,
            opener=lambda *_args, **_kwargs: Response(),
        )
    assert raised.value.transport_outcome == "token_env_not_configured"

    with pytest.raises(http.AlgoPackHttpError) as raised:
        http.fetch_algopack_bytes(
            URL,
            TOKEN,
            route_validator=exact_route,
            opener=lambda *_args, **_kwargs: Response(b""),
        )
    assert raised.value.transport_outcome == "algopack_http_service_unavailable"
    assert raised.value.retryable is True


def test_bounded_retry_preserves_generic_outcome() -> None:
    attempts = 0
    sleeps: list[float] = []
    headers = Message()
    headers["Retry-After"] = "0"

    def opener(_request: Request, timeout: int) -> Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise HTTPError(URL, 503, "sanitized", headers, None)
        return Response(b"done")

    payload = http.fetch_algopack_bytes_with_retry(
        URL,
        TOKEN,
        route_validator=exact_route,
        opener=opener,
        sleeper=sleeps.append,
    )
    assert payload == b"done"
    assert attempts == 3
    assert sleeps == [0.0, 0.0]

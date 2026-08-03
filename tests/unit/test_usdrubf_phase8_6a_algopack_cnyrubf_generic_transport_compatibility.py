from __future__ import annotations

from email.message import Message
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from moex_research.external_data import moex_cnyrubf_algopack_history as source


TOKEN = "secret-token-value"


class Response:
    def __init__(self, payload: bytes = b'{"ok":true}') -> None:
        self.payload = payload

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def test_cnyrubf_adapter_uses_generic_transport_and_preserves_404_mapping() -> None:
    route = source.build_tradestats_url(
        source.date(2026, 6, 10),
        source.date(2026, 6, 10),
    )
    headers = Message()
    headers["X-MOEX-Error-Code"] = "ticker-not-found"
    error = HTTPError(route, 404, "sanitized", headers, None)

    def opener(_request: Request, timeout: int) -> Response:
        assert timeout == 30
        raise error

    with pytest.raises(source.CnyrubfAlgoPackError) as raised:
        source.fetch_algopack_bytes(route, TOKEN, opener=opener)
    assert raised.value.blocker == "cnyrubf_not_available"
    assert TOKEN not in str(raised.value)
    assert TOKEN not in repr(raised.value)


def test_cnyrubf_adapter_preserves_service_unavailable_mapping() -> None:
    route = source.build_tradestats_url(
        source.date(2026, 6, 10),
        source.date(2026, 6, 10),
    )
    error = HTTPError(route, 503, "sanitized", Message(), None)

    def opener(_request: Request, timeout: int) -> Response:
        assert timeout == 30
        raise error

    with pytest.raises(source.CnyrubfAlgoPackError) as raised:
        source.fetch_algopack_bytes(route, TOKEN, opener=opener)
    assert raised.value.blocker == "algopack_tradestats_not_available"
    assert raised.value.retryable is True

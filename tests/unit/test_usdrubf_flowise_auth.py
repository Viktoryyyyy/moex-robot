from __future__ import annotations

from urllib.request import Request

import pytest

from src.moex_research.intelligence.usdrubf_flowise_auth import flowise_bearer_opener
from src.moex_research.intelligence.usdrubf_news_macro_runtime import RuntimeIntegrationError


def test_flowise_bearer_opener_adds_authorization_header_without_touching_body() -> None:
    captured = {}

    def opener(request, timeout):
        captured["authorization"] = request.headers.get("Authorization")
        captured["body"] = request.data
        captured["timeout"] = timeout
        return object()

    wrapped = flowise_bearer_opener("  secret-key  ", opener=opener)
    request = Request(
        "https://flowise.example/prediction/id",
        data=b'{"question":"payload"}',
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    result = wrapped(request, timeout=9.0)

    assert result is not None
    assert captured["authorization"] == "Bearer secret-key"
    assert captured["body"] == b'{"question":"payload"}'
    assert captured["timeout"] == 9.0


@pytest.mark.parametrize("value", ["", " ", "\t"])
def test_flowise_bearer_opener_rejects_blank_api_key(value: str) -> None:
    with pytest.raises(RuntimeIntegrationError, match="API key must be non-empty"):
        flowise_bearer_opener(value)

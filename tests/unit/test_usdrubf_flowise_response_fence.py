import json

import pytest

from src.moex_research.intelligence.usdrubf_news_macro_runtime import (
    FlowiseJsonAdapter,
    FlowiseTransportConfig,
    RuntimeIntegrationError,
)


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._raw


def _adapter(text: str) -> FlowiseJsonAdapter:
    return FlowiseJsonAdapter(
        FlowiseTransportConfig(
            endpoint="https://flowise.example.test/prediction/id",
            request_field="question",
            response_field="text",
        ),
        opener=lambda *_args, **_kwargs: _FakeResponse({"text": text}),
    )


@pytest.mark.parametrize(
    "wrapped",
    [
        "```json\n{\"trade_state\":\"WAIT\"}\n```",
        "```\n{\"trade_state\":\"WAIT\"}\n```",
        "`json\n{\"trade_state\":\"WAIT\"}\n`",
    ],
)
def test_flowise_adapter_accepts_single_json_only_response_fence(wrapped: str) -> None:
    result = _adapter(wrapped)({"instrument": "USDRUBF"})
    assert result == {"trade_state": "WAIT"}


@pytest.mark.parametrize(
    "invalid",
    [
        "answer:\n```json\n{\"trade_state\":\"WAIT\"}\n```",
        "```json\n{\"trade_state\":\"WAIT\"}\n```\nextra",
        "`json\n{\"trade_state\":\"WAIT\"}\n` extra",
    ],
)
def test_flowise_adapter_rejects_fence_with_additional_text(invalid: str) -> None:
    with pytest.raises(RuntimeIntegrationError, match="not JSON"):
        _adapter(invalid)({"instrument": "USDRUBF"})

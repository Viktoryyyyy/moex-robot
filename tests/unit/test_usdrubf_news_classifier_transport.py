from __future__ import annotations

import json

import pytest

from src.moex_research.intelligence.usdrubf_flowise_auth import FLOWISE_API_KEY_ENV
from src.moex_research.intelligence.usdrubf_news_classifier_transport import (
    NEWS_CLASSIFIER_FLOWISE_ENDPOINT_ENV,
    NEWS_CLASSIFIER_FLOWISE_REQUEST_FIELD_ENV,
    NEWS_CLASSIFIER_FLOWISE_RESPONSE_FIELD_ENV,
    NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS_ENV,
    news_classifier_flowise_agent_from_env,
)
from src.moex_research.intelligence.usdrubf_news_macro_runtime import RuntimeIntegrationError


class _Response:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


def _env(**overrides: str) -> dict[str, str]:
    values = {
        NEWS_CLASSIFIER_FLOWISE_ENDPOINT_ENV: "https://flowise.example/classifier",
        NEWS_CLASSIFIER_FLOWISE_REQUEST_FIELD_ENV: "question",
        NEWS_CLASSIFIER_FLOWISE_RESPONSE_FIELD_ENV: "text",
        NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS_ENV: "12.5",
        FLOWISE_API_KEY_ENV: "test-runtime-key",
    }
    values.update(overrides)
    return values


def test_news_classifier_flowise_agent_uses_explicit_env_envelope_and_bearer_auth() -> None:
    captured = {}

    def opener(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        captured["authorization"] = request.headers.get("Authorization")
        return _Response(b'{"text":"{\\"event_type\\":\\"OFFICIAL_COMMUNICATION\\"}"}')

    agent = news_classifier_flowise_agent_from_env(env=_env(), opener=opener)
    result = agent({"instrument": "USDRUBF", "cluster_id": "cluster_1"})

    assert captured["url"] == "https://flowise.example/classifier"
    assert captured["timeout"] == 12.5
    assert captured["authorization"] == "Bearer test-runtime-key"
    assert set(captured["body"]) == {"question"}
    assert json.loads(captured["body"]["question"]) == {
        "cluster_id": "cluster_1",
        "instrument": "USDRUBF",
    }
    assert result == {"event_type": "OFFICIAL_COMMUNICATION"}


@pytest.mark.parametrize(
    "missing_name",
    [
        NEWS_CLASSIFIER_FLOWISE_ENDPOINT_ENV,
        NEWS_CLASSIFIER_FLOWISE_REQUEST_FIELD_ENV,
        NEWS_CLASSIFIER_FLOWISE_RESPONSE_FIELD_ENV,
        NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS_ENV,
        FLOWISE_API_KEY_ENV,
    ],
)
def test_news_classifier_flowise_agent_requires_all_env_values(missing_name: str) -> None:
    values = _env()
    values.pop(missing_name)
    with pytest.raises(RuntimeIntegrationError, match="required News Classifier transport env missing"):
        news_classifier_flowise_agent_from_env(env=values)


def test_news_classifier_flowise_agent_rejects_non_https_endpoint() -> None:
    with pytest.raises(RuntimeIntegrationError, match="explicit HTTPS URL"):
        news_classifier_flowise_agent_from_env(
            env=_env(**{NEWS_CLASSIFIER_FLOWISE_ENDPOINT_ENV: "http://flowise.example/classifier"})
        )


def test_news_classifier_flowise_agent_rejects_blank_api_key() -> None:
    with pytest.raises(RuntimeIntegrationError, match="required News Classifier transport env missing"):
        news_classifier_flowise_agent_from_env(env=_env(**{FLOWISE_API_KEY_ENV: "  "}))


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "not-a-number"])
def test_news_classifier_flowise_agent_rejects_invalid_timeout(value: str) -> None:
    with pytest.raises(RuntimeIntegrationError):
        news_classifier_flowise_agent_from_env(
            env=_env(**{NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS_ENV: value})
        )

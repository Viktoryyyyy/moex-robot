from __future__ import annotations

import math
import os
from typing import Callable, Mapping
from urllib.request import urlopen

from .usdrubf_flowise_auth import FLOWISE_API_KEY_ENV, flowise_bearer_opener
from .usdrubf_news_macro_runtime import (
    FlowiseJsonAdapter,
    FlowiseTransportConfig,
    RuntimeIntegrationError,
)


NEWS_CLASSIFIER_FLOWISE_ENDPOINT_ENV = "MOEX_RUB_INTELLIGENCE_NEWS_CLASSIFIER_FLOWISE_ENDPOINT"
NEWS_CLASSIFIER_FLOWISE_REQUEST_FIELD_ENV = "MOEX_RUB_INTELLIGENCE_NEWS_CLASSIFIER_FLOWISE_REQUEST_FIELD"
NEWS_CLASSIFIER_FLOWISE_RESPONSE_FIELD_ENV = "MOEX_RUB_INTELLIGENCE_NEWS_CLASSIFIER_FLOWISE_RESPONSE_FIELD"
NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS_ENV = "MOEX_RUB_INTELLIGENCE_NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS"

NEWS_CLASSIFIER_FLOWISE_REQUIRED_ENV = (
    NEWS_CLASSIFIER_FLOWISE_ENDPOINT_ENV,
    NEWS_CLASSIFIER_FLOWISE_REQUEST_FIELD_ENV,
    NEWS_CLASSIFIER_FLOWISE_RESPONSE_FIELD_ENV,
    NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS_ENV,
    FLOWISE_API_KEY_ENV,
)


def _required_env(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeIntegrationError(f"required News Classifier transport env missing: {name}")
    return value.strip()


def _timeout_seconds(value: str) -> float:
    try:
        timeout = float(value)
    except ValueError as exc:
        raise RuntimeIntegrationError(
            f"{NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS_ENV} must be numeric"
        ) from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise RuntimeIntegrationError(
            f"{NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS_ENV} must be finite and positive"
        )
    return timeout


def news_classifier_flowise_agent_from_env(
    *,
    env: Mapping[str, str] | None = None,
    opener: Callable[..., object] = urlopen,
) -> FlowiseJsonAdapter:
    """Build the authenticated News Classifier Flowise transport from env.

    This function does not load dotenv, guess an endpoint, create a Flowise flow,
    or persist credentials. The caller owns canonical dotenv load order. The
    API key is read only from explicit runtime environment and is attached as a
    Bearer Authorization header by the bounded opener.
    """

    values: Mapping[str, str] = os.environ if env is None else env
    endpoint = _required_env(values, NEWS_CLASSIFIER_FLOWISE_ENDPOINT_ENV)
    request_field = _required_env(values, NEWS_CLASSIFIER_FLOWISE_REQUEST_FIELD_ENV)
    response_field = _required_env(values, NEWS_CLASSIFIER_FLOWISE_RESPONSE_FIELD_ENV)
    timeout_seconds = _timeout_seconds(
        _required_env(values, NEWS_CLASSIFIER_FLOWISE_TIMEOUT_SECONDS_ENV)
    )
    api_key = _required_env(values, FLOWISE_API_KEY_ENV)

    config = FlowiseTransportConfig(
        endpoint=endpoint,
        request_field=request_field,
        response_field=response_field,
        timeout_seconds=timeout_seconds,
    )
    return FlowiseJsonAdapter(
        config,
        opener=flowise_bearer_opener(api_key, opener=opener),
    )

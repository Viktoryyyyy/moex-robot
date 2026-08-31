#!/usr/bin/env python3
from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import requests
from dotenv import dotenv_values
from mcp.server.fastmcp import FastMCP


PROJECT = "MOEX_Bot"
MCP_MODE = "rub_factual_snapshot_chatgpt_mcp_bridge_v1"
TOKEN_ENV = "MOEX_RUB_SNAPSHOT_API_TOKEN"
ENV_FILE_ENV = "MOEX_ENV_FILE"
UPSTREAM_BASE_URL = "http://127.0.0.1:8765"
SNAPSHOT_PATH = "/v1/rub/factual-snapshot"
READINESS_PATH = "/readyz"
CONNECT_TIMEOUT_SECONDS = 2.0
READ_TIMEOUT_SECONDS = 5.0

HTTPGet = Callable[..., requests.Response]


class RubSnapshotBridgeConfigurationError(RuntimeError):
    """Fail-closed configuration error for the read-only MCP bridge."""


class RubSnapshotBridgeError(RuntimeError):
    """Predictable factual-upstream error exposed through MCP."""


def _validated_token(value: object, *, source: str) -> str:
    if not isinstance(value, str) or not value:
        raise RubSnapshotBridgeConfigurationError(f"{TOKEN_ENV} is missing in {source}")
    if value != value.strip():
        raise RubSnapshotBridgeConfigurationError(
            f"{TOKEN_ENV} in {source} must not contain surrounding whitespace"
        )
    if not value.isascii():
        raise RubSnapshotBridgeConfigurationError(f"{TOKEN_ENV} in {source} must be ASCII")
    if any(char.isspace() for char in value):
        raise RubSnapshotBridgeConfigurationError(
            f"{TOKEN_ENV} in {source} must not contain whitespace"
        )
    return value


def load_api_token(environ: Mapping[str, str] | None = None) -> str:
    """Load only the existing factual API token from governed configuration."""

    env = os.environ if environ is None else environ
    direct = env.get(TOKEN_ENV)
    if direct is not None:
        return _validated_token(direct, source="process environment")

    raw_env_file = env.get(ENV_FILE_ENV)
    if not raw_env_file or raw_env_file != raw_env_file.strip():
        raise RubSnapshotBridgeConfigurationError(
            f"{TOKEN_ENV} is not set and {ENV_FILE_ENV} is missing or invalid"
        )
    env_path = Path(raw_env_file)
    if not env_path.is_absolute():
        raise RubSnapshotBridgeConfigurationError(f"{ENV_FILE_ENV} must be an absolute path")
    if not env_path.is_file():
        raise RubSnapshotBridgeConfigurationError(f"{ENV_FILE_ENV} must reference a regular file")

    configured = dotenv_values(env_path).get(TOKEN_ENV)
    return _validated_token(configured, source=ENV_FILE_ENV)


class RubFactualSnapshotHTTPBridge:
    """Thin authenticated GET-only client for the canonical localhost factual API."""

    def __init__(self, *, api_token: str, http_get: HTTPGet = requests.get) -> None:
        self._api_token = _validated_token(api_token, source="bridge configuration")
        self._http_get = http_get

    def _get_json(self, path: str) -> tuple[int, dict[str, Any]]:
        url = f"{UPSTREAM_BASE_URL}{path}"
        try:
            response = self._http_get(
                url,
                headers={
                    "Authorization": f"Bearer {self._api_token}",
                    "Accept": "application/json",
                },
                timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
            )
        except requests.RequestException as exc:
            raise RubSnapshotBridgeError("factual API unavailable") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise RubSnapshotBridgeError(
                f"factual API returned malformed JSON (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict):
            raise RubSnapshotBridgeError(
                f"factual API returned a non-object payload (HTTP {response.status_code})"
            )
        return response.status_code, payload

    def get_snapshot(self) -> dict[str, Any]:
        status_code, payload = self._get_json(SNAPSHOT_PATH)
        if status_code == 200:
            return payload
        if status_code == 401:
            raise RubSnapshotBridgeError("factual API authentication failed")
        if status_code == 503:
            reason = payload.get("error")
            suffix = f": {reason}" if isinstance(reason, str) and reason else ""
            raise RubSnapshotBridgeError(f"factual API snapshot unavailable{suffix}")
        raise RubSnapshotBridgeError(f"factual API snapshot request failed (HTTP {status_code})")

    def get_readiness(self) -> dict[str, Any]:
        status_code, payload = self._get_json(READINESS_PATH)
        if status_code in {200, 503}:
            return payload
        if status_code == 401:
            raise RubSnapshotBridgeError("factual API authentication failed")
        raise RubSnapshotBridgeError(f"factual API readiness request failed (HTTP {status_code})")


_bridge: RubFactualSnapshotHTTPBridge | None = None
mcp = FastMCP("moex-rub-factual-snapshot")


def configure_bridge(environ: Mapping[str, str] | None = None) -> RubFactualSnapshotHTTPBridge:
    """Initialize the bridge from governed configuration before serving MCP."""

    global _bridge
    _bridge = RubFactualSnapshotHTTPBridge(api_token=load_api_token(environ))
    return _bridge


def _configured_bridge() -> RubFactualSnapshotHTTPBridge:
    if _bridge is None:
        raise RubSnapshotBridgeConfigurationError("MCP bridge is not configured")
    return _bridge


@mcp.tool()
def get_rub_factual_snapshot() -> dict[str, Any]:
    """Return the canonical RUB factual snapshot exactly as supplied by the factual API.

    The tool performs only an authenticated localhost GET. It does not refresh data,
    read source systems, mutate state, calculate analysis, or perform trading actions.
    """

    return _configured_bridge().get_snapshot()


@mcp.tool()
def get_rub_snapshot_readiness() -> dict[str, Any]:
    """Return canonical factual snapshot readiness/freshness state from the factual API.

    HTTP 503 NOT_READY is a factual operational state and is returned unchanged rather
    than upgraded. This tool does not create analytical or trading readiness.
    """

    return _configured_bridge().get_readiness()


def main() -> int:
    configure_bridge()
    mcp.run(transport="stdio")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

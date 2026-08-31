from __future__ import annotations

import ast
import asyncio
import inspect
import threading
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import requests
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from src.misc import mcp_rub_factual_snapshot_bridge as bridge
from src.misc import rub_factual_snapshot_http_server as api


TOKEN = "test-bearer-token"


def _snapshot(*, readiness: str = "READY", freshness: str = "FRESH") -> dict[str, Any]:
    return {
        "schema_version": "rub_chat_analysis_snapshot.v1",
        "identity": {
            "project": "MOEX_Bot",
            "generated_at_utc": "2026-08-31T18:26:40.418656+00:00",
        },
        "readiness": {
            "status": readiness,
            "component_statuses": {
                "official_news": "READY",
                "oil": "GOVERNED_BLOCKED",
            },
        },
        "components": {
            "official_news": {
                "status": "READY",
                "data_as_of": "2026-08-31T18:20:00+00:00",
                "provenance": {"source": "canonical_test_source"},
                "data": {"items": []},
            },
            "oil": {
                "status": "GOVERNED_BLOCKED",
                "data_as_of": None,
                "provenance": {"source": "governed_block"},
                "data": {"action_authority": False},
            },
        },
        "read_freshness": {
            "read_at_utc": "2026-08-31T18:33:46.875800+00:00",
            "snapshot_age_seconds": 426,
            "status": freshness,
        },
        "authority": {
            "data_only": True,
            "factual_authority": False,
            "directional_authority": False,
            "action_authority": False,
            "broker_execution": False,
            "telegram_delivery": False,
        },
    }


class StubResponse:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise self._payload
        return deepcopy(self._payload)


def _stub_get(status_code: int, payload: object, calls: list[dict[str, object]]):
    def get(url: str, **kwargs: object) -> StubResponse:
        calls.append({"url": url, **kwargs})
        return StubResponse(status_code, payload)

    return get


def test_snapshot_tool_preserves_factual_api_payload_exactly() -> None:
    expected = _snapshot()
    calls: list[dict[str, object]] = []
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(200, expected, calls),
    )

    actual = client.get_snapshot()

    assert actual == expected
    assert actual["identity"]["generated_at_utc"] == expected["identity"]["generated_at_utc"]
    assert actual["read_freshness"] == expected["read_freshness"]
    assert actual["components"] == expected["components"]
    assert actual["authority"] == expected["authority"]
    assert calls == [
        {
            "url": f"{bridge.UPSTREAM_BASE_URL}{bridge.SNAPSHOT_PATH}",
            "headers": {
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/json",
            },
            "timeout": (
                bridge.CONNECT_TIMEOUT_SECONDS,
                bridge.READ_TIMEOUT_SECONDS,
            ),
        }
    ]


@pytest.mark.parametrize(
    ("readiness", "freshness"),
    [
        ("PARTIAL", "FRESH"),
        ("READY", "STALE"),
        ("PARTIAL", "STALE"),
    ],
)
def test_snapshot_tool_preserves_degraded_states(readiness: str, freshness: str) -> None:
    expected = _snapshot(readiness=readiness, freshness=freshness)
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(200, expected, []),
    )

    actual = client.get_snapshot()

    assert actual == expected
    assert actual["readiness"]["status"] == readiness
    assert actual["read_freshness"]["status"] == freshness
    assert actual["readiness"]["component_statuses"]["oil"] == "GOVERNED_BLOCKED"
    assert actual["components"]["oil"]["status"] == "GOVERNED_BLOCKED"


def test_readiness_tool_returns_ready_payload_unchanged() -> None:
    expected = {
        "status": "READY",
        "schema_version": "rub_chat_analysis_snapshot.v1",
        "snapshot_generated_at_utc": "2026-08-31T18:26:40.418656+00:00",
        "snapshot_read_at_utc": "2026-08-31T18:33:46.875800+00:00",
        "snapshot_readiness": "READY",
        "snapshot_freshness": "FRESH",
    }
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(200, expected, []),
    )

    assert client.get_readiness() == expected


def test_readiness_tool_returns_http_503_not_ready_payload_unchanged() -> None:
    expected = {
        "status": "NOT_READY",
        "schema_version": "rub_chat_analysis_snapshot.v1",
        "snapshot_generated_at_utc": "2026-08-31T18:26:40.418656+00:00",
        "snapshot_read_at_utc": "2026-08-31T18:33:46.875800+00:00",
        "snapshot_readiness": "PARTIAL",
        "snapshot_freshness": "STALE",
    }
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(503, expected, []),
    )

    assert client.get_readiness() == expected


def test_snapshot_authentication_failure_is_explicit() -> None:
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(401, {"error": "unauthorized"}, []),
    )

    with pytest.raises(bridge.RubSnapshotBridgeError, match="authentication failed"):
        client.get_snapshot()


def test_snapshot_unavailable_is_explicit_and_has_no_fallback() -> None:
    calls: list[dict[str, object]] = []
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(503, {"error": "snapshot_unavailable"}, calls),
    )

    with pytest.raises(bridge.RubSnapshotBridgeError, match="snapshot_unavailable"):
        client.get_snapshot()

    assert len(calls) == 1


def test_upstream_api_connection_failure_is_predictable() -> None:
    def unavailable(*args: object, **kwargs: object) -> StubResponse:
        raise requests.ConnectionError("connection refused")

    client = bridge.RubFactualSnapshotHTTPBridge(api_token=TOKEN, http_get=unavailable)

    with pytest.raises(bridge.RubSnapshotBridgeError, match="factual API unavailable"):
        client.get_snapshot()


def test_malformed_upstream_json_is_predictable() -> None:
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(200, ValueError("invalid json"), []),
    )

    with pytest.raises(bridge.RubSnapshotBridgeError, match="malformed JSON"):
        client.get_snapshot()


def test_non_object_upstream_json_is_rejected() -> None:
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(200, ["not", "an", "object"], []),
    )

    with pytest.raises(bridge.RubSnapshotBridgeError, match="non-object payload"):
        client.get_snapshot()


def test_bridge_uses_only_canonical_localhost_get_routes() -> None:
    calls: list[dict[str, object]] = []
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(200, _snapshot(), calls),
    )
    client.get_snapshot()

    readiness = {
        "status": "READY",
        "snapshot_readiness": "READY",
        "snapshot_freshness": "FRESH",
    }
    client = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(200, readiness, calls),
    )
    client.get_readiness()

    assert [call["url"] for call in calls] == [
        "http://127.0.0.1:8765/v1/rub/factual-snapshot",
        "http://127.0.0.1:8765/readyz",
    ]


def test_bridge_source_imports_no_data_refresh_analysis_or_trading_modules() -> None:
    source = inspect.getsource(bridge)
    tree = ast.parse(source)
    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    forbidden_fragments = (
        "moex_research.adapters",
        "moex_research.runners",
        "consumer",
        "algopack",
        "cbr",
        "news",
        "oil",
        "calendar",
        "broker",
        "telegram",
        "forecast",
        "scenario",
        "recommend",
        "stage5",
    )
    assert not any(
        fragment in module.lower()
        for module in imported_modules
        for fragment in forbidden_fragments
    )
    assert "subprocess" not in imported_modules
    assert "os.system" not in source
    assert "Popen(" not in source


def test_mcp_surface_has_exactly_two_explicitly_read_only_tools() -> None:
    async def inspect_tools() -> None:
        tools = await bridge.mcp.list_tools()
        assert [tool.name for tool in tools] == [
            "get_rub_factual_snapshot",
            "get_rub_snapshot_readiness",
        ]
        for tool in tools:
            assert tool.annotations is not None
            annotations = tool.annotations.model_dump(by_alias=True)
            assert annotations["readOnlyHint"] is True
            assert annotations["destructiveHint"] is False
            assert annotations["idempotentHint"] is True
            assert annotations["openWorldHint"] is False
            assert tool.inputSchema.get("properties", {}) == {}

    asyncio.run(inspect_tools())


def test_tool_functions_do_not_add_mcp_request_time_or_transform_payload(monkeypatch) -> None:
    expected = _snapshot()
    configured = bridge.RubFactualSnapshotHTTPBridge(
        api_token=TOKEN,
        http_get=_stub_get(200, expected, []),
    )
    monkeypatch.setattr(bridge, "_bridge", configured)

    actual = bridge.get_rub_factual_snapshot()

    assert actual == expected
    assert "mcp_request_time" not in actual
    assert "mcp_requested_at_utc" not in actual


def test_load_api_token_prefers_direct_environment() -> None:
    assert bridge.load_api_token({bridge.TOKEN_ENV: TOKEN}) == TOKEN


def test_load_api_token_reads_governed_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "project.env"
    env_file.write_text(
        f"OTHER_SECRET=ignored\n{bridge.TOKEN_ENV}={TOKEN}\n",
        encoding="utf-8",
    )

    assert bridge.load_api_token({bridge.ENV_FILE_ENV: str(env_file)}) == TOKEN


@pytest.mark.parametrize(
    "environ",
    [
        {},
        {bridge.TOKEN_ENV: ""},
        {bridge.TOKEN_ENV: " token"},
        {bridge.TOKEN_ENV: "token "},
        {bridge.TOKEN_ENV: "token with space"},
        {bridge.TOKEN_ENV: "tökën"},
        {bridge.ENV_FILE_ENV: "relative.env"},
    ],
)
def test_bridge_configuration_fails_closed(environ: dict[str, str]) -> None:
    with pytest.raises(bridge.RubSnapshotBridgeConfigurationError):
        bridge.load_api_token(environ)


@contextmanager
def _canonical_api_on_bridge_port(snapshot: dict[str, Any]):
    server = api.create_server(
        api_token=TOKEN,
        port=8765,
        snapshot_loader=lambda: deepcopy(snapshot),
    )
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.01},
        daemon=True,
    )
    thread.start()
    try:
        yield
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _structured_content(result: object) -> object:
    if hasattr(result, "structuredContent"):
        return getattr(result, "structuredContent")
    return getattr(result, "structured_content")


def _is_error(result: object) -> bool:
    if hasattr(result, "isError"):
        return bool(getattr(result, "isError"))
    return bool(getattr(result, "is_error"))


def test_stdio_mcp_round_trip_matches_secure_tunnel_supported_transport() -> None:
    expected = _snapshot()
    repo_root = Path(__file__).resolve().parents[2]

    async def run_client() -> None:
        params = StdioServerParameters(
            command="python",
            args=["-m", "misc.mcp_rub_factual_snapshot_bridge"],
            env={
                bridge.TOKEN_ENV: TOKEN,
                "PYTHONPATH": f"{repo_root}:{repo_root / 'src'}",
            },
            cwd=str(repo_root),
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                listed = await session.list_tools()
                assert [tool.name for tool in listed.tools] == [
                    "get_rub_factual_snapshot",
                    "get_rub_snapshot_readiness",
                ]
                result = await session.call_tool("get_rub_factual_snapshot", arguments={})
                assert _is_error(result) is False
                assert _structured_content(result) == expected

    with _canonical_api_on_bridge_port(expected):
        asyncio.run(run_client())

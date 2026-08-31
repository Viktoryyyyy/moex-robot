from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from src.misc import mcp_rub_factual_snapshot_bridge as bridge
from src.misc import rub_factual_snapshot_http_server as api


TOKEN = "test-bearer-token"


def _snapshot() -> dict[str, Any]:
    return {
        "schema_version": "rub_chat_analysis_snapshot.v1",
        "identity": {
            "project": "MOEX_Bot",
            "generated_at_utc": "2026-08-31T19:06:44.284385+00:00",
        },
        "readiness": {
            "status": "PARTIAL",
            "component_statuses": {"oil": "GOVERNED_BLOCKED"},
        },
        "components": {
            "oil": {
                "status": "GOVERNED_BLOCKED",
                "data_as_of": None,
                "provenance": {"source": "governed_block"},
                "data": {"action_authority": False},
            }
        },
        "read_freshness": {
            "read_at_utc": "2026-08-31T19:07:00+00:00",
            "snapshot_age_seconds": 16,
            "status": "STALE",
        },
        "authority": {
            "data_only": True,
            "factual_authority": False,
            "directional_authority": False,
            "action_authority": False,
        },
    }


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((bridge.DEFAULT_MCP_HTTP_HOST, 0))
        return int(sock.getsockname()[1])


def _wait_for_listener(port: int, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                f"MCP HTTP process exited early rc={process.returncode}; "
                f"stdout={stdout!r}; stderr={stderr!r}"
            )
        try:
            with socket.create_connection((bridge.DEFAULT_MCP_HTTP_HOST, port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise AssertionError("MCP HTTP listener did not become ready")


@contextmanager
def _canonical_api(snapshot: dict[str, Any]):
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


def test_cli_defaults_preserve_existing_stdio_transport() -> None:
    args = bridge.build_arg_parser().parse_args([])
    assert args.transport == "stdio"
    assert args.host == bridge.DEFAULT_MCP_HTTP_HOST
    assert args.port == bridge.DEFAULT_MCP_HTTP_PORT


def test_streamable_http_transport_is_loopback_only(monkeypatch) -> None:
    configured = False
    built = False

    def configure(*args: object, **kwargs: object) -> None:
        nonlocal configured
        configured = True

    def build(*args: object, **kwargs: object) -> object:
        nonlocal built
        built = True
        return object()

    monkeypatch.setattr(bridge, "configure_bridge", configure)
    monkeypatch.setattr(bridge, "build_mcp_server", build)

    with pytest.raises(
        bridge.RubSnapshotBridgeConfigurationError,
        match="must bind only to 127.0.0.1",
    ):
        bridge.run_mcp(transport="streamable-http", host="0.0.0.0", port=8766)

    assert configured is False
    assert built is False


@pytest.mark.parametrize("port", [0, 65536, -1])
def test_streamable_http_transport_rejects_invalid_port_before_runtime(
    port: int, monkeypatch
) -> None:
    configured = False

    def configure(*args: object, **kwargs: object) -> None:
        nonlocal configured
        configured = True

    monkeypatch.setattr(bridge, "configure_bridge", configure)

    with pytest.raises(bridge.RubSnapshotBridgeConfigurationError, match="port must be"):
        bridge.run_mcp(transport="streamable-http", port=port)

    assert configured is False


def test_streamable_http_server_has_exact_sdk_1x_origin_settings() -> None:
    server = bridge.build_mcp_server(host="127.0.0.1", port=8766)

    assert server.settings.host == "127.0.0.1"
    assert server.settings.port == 8766
    assert server.settings.streamable_http_path == "/mcp"
    assert server.settings.stateless_http is True
    assert server.settings.json_response is True


def test_streamable_http_run_builds_bounded_server_then_uses_transport(monkeypatch) -> None:
    configured = False
    built: list[tuple[str, int]] = []
    run_calls: list[str] = []

    class StubServer:
        def run(self, transport: str) -> None:
            run_calls.append(transport)

    def configure(*args: object, **kwargs: object) -> None:
        nonlocal configured
        configured = True

    def build(*, host: str, port: int) -> StubServer:
        built.append((host, port))
        return StubServer()

    monkeypatch.setattr(bridge, "configure_bridge", configure)
    monkeypatch.setattr(bridge, "build_mcp_server", build)

    bridge.run_mcp(transport="streamable-http", host="127.0.0.1", port=8766)

    assert configured is True
    assert built == [("127.0.0.1", 8766)]
    assert run_calls == ["streamable-http"]


def test_streamable_http_surface_still_has_only_two_read_only_tools() -> None:
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

    asyncio.run(inspect_tools())


def test_streamable_http_round_trip_preserves_canonical_snapshot() -> None:
    expected = _snapshot()
    repo_root = Path(__file__).resolve().parents[2]
    port = _free_loopback_port()
    env = os.environ.copy()
    env.update(
        {
            bridge.TOKEN_ENV: TOKEN,
            "PYTHONPATH": f"{repo_root}:{repo_root / 'src'}",
        }
    )

    with _canonical_api(expected):
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "misc.mcp_rub_factual_snapshot_bridge",
                "--transport",
                "streamable-http",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=repo_root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            _wait_for_listener(port, process)

            async def run_client() -> None:
                async with streamable_http_client(
                    f"http://127.0.0.1:{port}/mcp",
                    terminate_on_close=False,
                ) as (read, write, _):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        tools = await session.list_tools()
                        assert [tool.name for tool in tools.tools] == [
                            "get_rub_factual_snapshot",
                            "get_rub_snapshot_readiness",
                        ]
                        result = await session.call_tool(
                            "get_rub_factual_snapshot", arguments={}
                        )
                        actual = _structured_content(result)
                        assert actual == expected
                        assert actual["identity"]["generated_at_utc"] == (
                            "2026-08-31T19:06:44.284385+00:00"
                        )
                        assert actual["readiness"]["status"] == "PARTIAL"
                        assert actual["read_freshness"]["status"] == "STALE"
                        assert actual["components"]["oil"]["status"] == "GOVERNED_BLOCKED"

            asyncio.run(run_client())
        finally:
            process.terminate()
            try:
                process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=3)

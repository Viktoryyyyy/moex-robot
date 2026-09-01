from __future__ import annotations

import json
import socket
import threading
from typing import Any

import pytest
import requests

from src.misc import moex_analyst_web_chat as chat


class StubResponse:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> object:
        return self._payload


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((chat.DEFAULT_HOST, 0))
        return int(sock.getsockname()[1])


def test_tool_surface_is_exact_read_only_factual_pair() -> None:
    assert tuple(tool["name"] for tool in chat.TOOLS) == chat.ALLOWED_TOOL_NAMES
    for tool in chat.TOOLS:
        assert tool["type"] == "function"
        assert tool["strict"] is True
        assert tool["parameters"] == {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        }


def test_load_config_is_fail_closed_and_private_by_default() -> None:
    with pytest.raises(chat.WebChatConfigurationError, match="OPENAI_API_KEY"):
        chat.load_config({})

    config = chat.load_config(
        {
            chat.OPENAI_API_KEY_ENV: "sk-test",
            chat.WEB_PASSWORD_ENV: "web-secret",
        }
    )
    assert config == {
        "api_key": "sk-test",
        "password": "web-secret",
        "user": "moex",
        "model": chat.DEFAULT_MODEL,
        "mcp_url": "http://127.0.0.1:18766/mcp",
    }


def test_load_config_rejects_noncanonical_mcp_url() -> None:
    with pytest.raises(chat.WebChatConfigurationError, match="private KZ loopback MCP URL"):
        chat.load_config(
            {
                chat.OPENAI_API_KEY_ENV: "sk-test",
                chat.WEB_PASSWORD_ENV: "web-secret",
                chat.MCP_URL_ENV: "https://example.com/mcp",
            }
        )


def test_message_validation_accepts_only_bounded_chat_history() -> None:
    assert chat._normalize_messages(
        [
            {"role": "user", "content": "status?"},
            {"role": "assistant", "content": "PARTIAL"},
            {"role": "user", "content": "freshness?"},
        ]
    )[-1] == {"role": "user", "content": "freshness?"}

    with pytest.raises(ValueError, match="last message must be from user"):
        chat._normalize_messages([{"role": "assistant", "content": "x"}])

    with pytest.raises(ValueError, match="role must be user or assistant"):
        chat._normalize_messages([{"role": "system", "content": "x"}])


def test_responses_function_call_round_trip_uses_only_private_mcp() -> None:
    calls: list[dict[str, Any]] = []
    tool_calls: list[tuple[str, str]] = []

    responses = iter(
        [
            StubResponse(
                200,
                {
                    "output": [
                        {
                            "type": "function_call",
                            "name": "get_rub_snapshot_readiness",
                            "call_id": "call-1",
                            "arguments": "{}",
                        }
                    ]
                },
            ),
            StubResponse(
                200,
                {
                    "output": [
                        {
                            "type": "message",
                            "content": [
                                {"type": "output_text", "text": "Snapshot is PARTIAL and FRESH."}
                            ],
                        }
                    ]
                },
            ),
        ]
    )

    def post(*args: object, **kwargs: Any) -> StubResponse:
        calls.append(kwargs)
        return next(responses)

    def tool_caller(mcp_url: str, tool_name: str) -> object:
        tool_calls.append((mcp_url, tool_name))
        return {"status": "NOT_READY", "snapshot_readiness": "PARTIAL", "snapshot_freshness": "FRESH"}

    client = chat.OpenAIResponsesClient(
        api_key="sk-test",
        model=chat.DEFAULT_MODEL,
        mcp_url=chat.DEFAULT_MCP_URL,
        post=post,
        tool_caller=tool_caller,
    )

    answer = client.answer([{"role": "user", "content": "Current readiness?"}])

    assert answer == "Snapshot is PARTIAL and FRESH."
    assert tool_calls == [
        ("http://127.0.0.1:18766/mcp", "get_rub_snapshot_readiness")
    ]
    assert len(calls) == 2
    first_request = calls[0]["json"]
    assert first_request["model"] == chat.DEFAULT_MODEL
    assert first_request["store"] is False
    assert first_request["tool_choice"] == "auto"
    assert tuple(tool["name"] for tool in first_request["tools"]) == chat.ALLOWED_TOOL_NAMES

    second_input = calls[1]["json"]["input"]
    function_output = second_input[-1]
    assert function_output["type"] == "function_call_output"
    assert function_output["call_id"] == "call-1"
    assert json.loads(function_output["output"]) == {
        "snapshot_freshness": "FRESH",
        "snapshot_readiness": "PARTIAL",
        "status": "NOT_READY",
    }


def test_responses_client_rejects_unknown_tool_without_execution() -> None:
    executed = False

    def post(*args: object, **kwargs: Any) -> StubResponse:
        return StubResponse(
            200,
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "refresh_market_data",
                        "call_id": "call-x",
                        "arguments": "{}",
                    }
                ]
            },
        )

    def tool_caller(*args: object, **kwargs: object) -> object:
        nonlocal executed
        executed = True
        return {}

    client = chat.OpenAIResponsesClient(
        api_key="sk-test",
        model=chat.DEFAULT_MODEL,
        mcp_url=chat.DEFAULT_MCP_URL,
        post=post,
        tool_caller=tool_caller,
    )

    with pytest.raises(chat.WebChatUpstreamError, match="unsupported tool"):
        client.answer([{"role": "user", "content": "refresh"}])
    assert executed is False


def test_web_server_is_loopback_only() -> None:
    with pytest.raises(chat.WebChatConfigurationError, match="loopback 127.0.0.1"):
        chat.create_server(
            api_key="sk-test",
            model=chat.DEFAULT_MODEL,
            mcp_url=chat.DEFAULT_MCP_URL,
            user="moex",
            password="secret",
            host="0.0.0.0",
            port=18767,
        )


def test_web_http_requires_basic_auth_and_serves_chat() -> None:
    port = _free_loopback_port()

    def post(*args: object, **kwargs: Any) -> StubResponse:
        return StubResponse(
            200,
            {
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": "OK"}],
                    }
                ]
            },
        )

    server = chat.create_server(
        api_key="sk-test",
        model=chat.DEFAULT_MODEL,
        mcp_url=chat.DEFAULT_MCP_URL,
        user="moex",
        password="secret",
        port=port,
        post=post,
        tool_caller=lambda *_: {},
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{port}"
        unauthorized = requests.get(base + "/", timeout=2)
        assert unauthorized.status_code == 401
        assert unauthorized.headers["WWW-Authenticate"].startswith("Basic ")

        page = requests.get(base + "/", auth=("moex", "secret"), timeout=2)
        assert page.status_code == 200
        assert "MOEX Analyst" in page.text

        response = requests.post(
            base + "/api/chat",
            auth=("moex", "secret"),
            json={"messages": [{"role": "user", "content": "status"}]},
            timeout=2,
        )
        assert response.status_code == 200
        assert response.json() == {"reply": "OK"}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import base64
import hmac
import json
import os
from collections.abc import Mapping, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

import requests
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


PROJECT = "MOEX_Bot"
APP_MODE = "moex_analyst_web_chat_v1"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 18767
DEFAULT_MCP_URL = "http://127.0.0.1:18766/mcp"
DEFAULT_MODEL = "gpt-5.6-sol"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
OPENAI_MODEL_ENV = "MOEX_ANALYST_OPENAI_MODEL"
MCP_URL_ENV = "MOEX_ANALYST_MCP_URL"
WEB_USER_ENV = "MOEX_ANALYST_WEB_USER"
WEB_PASSWORD_ENV = "MOEX_ANALYST_WEB_PASSWORD"

MAX_BODY_BYTES = 256 * 1024
MAX_MESSAGES = 40
MAX_MESSAGE_CHARS = 12_000
MAX_TOOL_ROUNDS = 4
OPENAI_CONNECT_TIMEOUT_SECONDS = 5.0
OPENAI_READ_TIMEOUT_SECONDS = 120.0

ALLOWED_TOOL_NAMES = (
    "get_rub_factual_snapshot",
    "get_rub_snapshot_readiness",
)

INSTRUCTIONS = """You are the private MOEX_Bot RUB factual analyst interface.

Use the provided tools whenever the answer depends on the current canonical RUB factual snapshot
or its readiness. Treat tool output as authoritative factual input exactly as returned.

Rules:
- Do not invent missing market data or silently upgrade readiness.
- Explicitly state PARTIAL, STALE, NOT_READY, GOVERNED_BLOCKED, RETAINED_PREVIOUS, or other degraded
  states when they materially affect the answer.
- Do not refresh data, access source systems, or claim access beyond the two provided tools.
- Do not execute trades, broker actions, Telegram actions, or other state-changing operations.
- Keep factual observations separate from interpretation.
- If the tools do not support a requested fact, say that the current factual snapshot does not
  provide it.
- Answer in the language used by the user.
"""

TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "name": "get_rub_factual_snapshot",
        "description": (
            "Return the current canonical persisted RUB factual snapshot exactly as supplied "
            "through the governed read-only MCP bridge."
        ),
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "type": "function",
        "name": "get_rub_snapshot_readiness",
        "description": (
            "Return current factual snapshot readiness and freshness state through the governed "
            "read-only MCP bridge."
        ),
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
]


class WebChatConfigurationError(RuntimeError):
    """Fail-closed runtime configuration error."""


class WebChatUpstreamError(RuntimeError):
    """Predictable OpenAI or MCP upstream error."""


def _validated_secret(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise WebChatConfigurationError(f"{name} is missing")
    if value != value.strip():
        raise WebChatConfigurationError(f"{name} must not contain surrounding whitespace")
    if any(char in "\r\n" for char in value):
        raise WebChatConfigurationError(f"{name} must not contain line breaks")
    return value


def load_config(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    env = os.environ if environ is None else environ
    api_key = _validated_secret(env.get(OPENAI_API_KEY_ENV), name=OPENAI_API_KEY_ENV)
    password = _validated_secret(env.get(WEB_PASSWORD_ENV), name=WEB_PASSWORD_ENV)
    user = env.get(WEB_USER_ENV, "moex").strip()
    if not user or ":" in user or any(char in "\r\n" for char in user):
        raise WebChatConfigurationError(f"{WEB_USER_ENV} is invalid")

    model = env.get(OPENAI_MODEL_ENV, DEFAULT_MODEL).strip()
    if not model or any(char.isspace() for char in model):
        raise WebChatConfigurationError(f"{OPENAI_MODEL_ENV} is invalid")

    mcp_url = env.get(MCP_URL_ENV, DEFAULT_MCP_URL).strip()
    if mcp_url != DEFAULT_MCP_URL:
        raise WebChatConfigurationError(
            f"{MCP_URL_ENV} must remain the private KZ loopback MCP URL {DEFAULT_MCP_URL}"
        )

    return {
        "api_key": api_key,
        "password": password,
        "user": user,
        "model": model,
        "mcp_url": mcp_url,
    }


def _structured_content(result: object) -> object:
    if hasattr(result, "structuredContent"):
        return getattr(result, "structuredContent")
    return getattr(result, "structured_content")


async def _call_mcp_tool_async(mcp_url: str, tool_name: str) -> object:
    if tool_name not in ALLOWED_TOOL_NAMES:
        raise WebChatUpstreamError(f"unsupported tool requested: {tool_name}")
    try:
        async with streamable_http_client(
            mcp_url,
            terminate_on_close=False,
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments={})
    except Exception as exc:
        raise WebChatUpstreamError("private factual MCP unavailable") from exc

    content = _structured_content(result)
    if content is None:
        raise WebChatUpstreamError(f"{tool_name} returned no structured content")
    return content


def call_mcp_tool(mcp_url: str, tool_name: str) -> object:
    return asyncio.run(_call_mcp_tool_async(mcp_url, tool_name))


def _normalize_messages(payload: object) -> list[dict[str, str]]:
    if not isinstance(payload, list) or not payload:
        raise ValueError("messages must be a non-empty list")
    if len(payload) > MAX_MESSAGES:
        raise ValueError(f"messages exceeds maximum of {MAX_MESSAGES}")

    normalized: list[dict[str, str]] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("each message must be an object")
        if set(item) != {"role", "content"}:
            raise ValueError("each message must contain only role and content")
        role = item.get("role")
        content = item.get("content")
        if role not in {"user", "assistant"}:
            raise ValueError("message role must be user or assistant")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("message content must be non-empty text")
        if len(content) > MAX_MESSAGE_CHARS:
            raise ValueError(f"message exceeds {MAX_MESSAGE_CHARS} characters")
        normalized.append({"role": role, "content": content})

    if normalized[-1]["role"] != "user":
        raise ValueError("last message must be from user")
    return normalized


def _extract_output_text(response: Mapping[str, Any]) -> str:
    parts: list[str] = []
    output = response.get("output")
    if not isinstance(output, list):
        return ""
    for item in output:
        if not isinstance(item, Mapping) or item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if (
                isinstance(part, Mapping)
                and part.get("type") == "output_text"
                and isinstance(part.get("text"), str)
            ):
                parts.append(part["text"])
    return "\n".join(part for part in parts if part).strip()


def _function_calls(response: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    output = response.get("output")
    if not isinstance(output, list):
        raise WebChatUpstreamError("OpenAI response missing output list")
    return [
        item
        for item in output
        if isinstance(item, Mapping) and item.get("type") == "function_call"
    ]


class OpenAIResponsesClient:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        mcp_url: str,
        post: Any = requests.post,
        tool_caller: Any = call_mcp_tool,
    ) -> None:
        self.api_key = _validated_secret(api_key, name=OPENAI_API_KEY_ENV)
        self.model = model
        self.mcp_url = mcp_url
        self._post = post
        self._tool_caller = tool_caller

    def _create_response(self, input_items: list[object]) -> dict[str, Any]:
        try:
            response = self._post(
                OPENAI_RESPONSES_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json={
                    "model": self.model,
                    "instructions": INSTRUCTIONS,
                    "input": input_items,
                    "tools": TOOLS,
                    "tool_choice": "auto",
                    "store": False,
                },
                timeout=(OPENAI_CONNECT_TIMEOUT_SECONDS, OPENAI_READ_TIMEOUT_SECONDS),
            )
        except requests.RequestException as exc:
            raise WebChatUpstreamError("OpenAI Responses API unavailable") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise WebChatUpstreamError(
                f"OpenAI Responses API returned malformed JSON (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict):
            raise WebChatUpstreamError("OpenAI Responses API returned a non-object payload")
        if response.status_code != 200:
            error = payload.get("error")
            message = error.get("message") if isinstance(error, Mapping) else None
            suffix = f": {message}" if isinstance(message, str) and message else ""
            raise WebChatUpstreamError(
                f"OpenAI Responses API failed (HTTP {response.status_code}){suffix}"
            )
        return payload

    def answer(self, messages: list[dict[str, str]]) -> str:
        input_items: list[object] = [dict(item) for item in messages]

        for round_index in range(MAX_TOOL_ROUNDS + 1):
            response = self._create_response(input_items)
            calls = _function_calls(response)
            if not calls:
                text = _extract_output_text(response)
                if not text:
                    raise WebChatUpstreamError("OpenAI response contained no answer text")
                return text
            if round_index >= MAX_TOOL_ROUNDS:
                raise WebChatUpstreamError("tool-call round limit exceeded")

            output_items = response.get("output")
            assert isinstance(output_items, list)
            input_items.extend(output_items)

            for call in calls:
                name = call.get("name")
                call_id = call.get("call_id")
                raw_arguments = call.get("arguments")
                if name not in ALLOWED_TOOL_NAMES:
                    raise WebChatUpstreamError(f"OpenAI requested unsupported tool: {name}")
                if not isinstance(call_id, str) or not call_id:
                    raise WebChatUpstreamError("OpenAI function call missing call_id")
                if not isinstance(raw_arguments, str):
                    raise WebChatUpstreamError("OpenAI function call arguments are invalid")
                try:
                    arguments = json.loads(raw_arguments)
                except json.JSONDecodeError as exc:
                    raise WebChatUpstreamError("OpenAI function call arguments are malformed") from exc
                if arguments != {}:
                    raise WebChatUpstreamError(f"{name} does not accept arguments")

                result = self._tool_caller(self.mcp_url, name)
                input_items.append(
                    {
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": json.dumps(
                            result,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                            allow_nan=False,
                        ),
                    }
                )

        raise WebChatUpstreamError("tool-call round limit exceeded")


INDEX_HTML = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>MOEX Analyst</title>
<style>
:root{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#111;background:#f5f5f7}
*{box-sizing:border-box}
body{margin:0}
main{max-width:760px;margin:0 auto;min-height:100vh;padding:18px 14px 110px}
h1{font-size:20px;margin:4px 0 18px}
#chat{display:flex;flex-direction:column;gap:12px}
.msg{white-space:pre-wrap;line-height:1.45;padding:12px 14px;border-radius:14px;max-width:92%;overflow-wrap:anywhere}
.user{align-self:flex-end;background:#111;color:#fff}
.assistant{align-self:flex-start;background:#fff;border:1px solid #ddd}
.error{align-self:flex-start;background:#fff;border:1px solid #b00020;color:#b00020}
form{position:fixed;left:0;right:0;bottom:0;background:rgba(245,245,247,.96);border-top:1px solid #ddd;padding:10px 14px calc(10px + env(safe-area-inset-bottom))}
.row{max-width:760px;margin:0 auto;display:flex;gap:8px}
textarea{flex:1;resize:none;min-height:46px;max-height:140px;border:1px solid #bbb;border-radius:12px;padding:11px;font:inherit;background:#fff}
button{border:0;border-radius:12px;padding:0 16px;background:#111;color:#fff;font:inherit;font-weight:600}
button:disabled{opacity:.45}
</style>
</head>
<body>
<main>
<h1>MOEX Analyst</h1>
<div id="chat"></div>
</main>
<form id="form">
<div class="row">
<textarea id="input" rows="1" placeholder="Вопрос по RUB..." maxlength="12000"></textarea>
<button id="send" type="submit">Send</button>
</div>
</form>
<script>
const chat=document.getElementById("chat");
const form=document.getElementById("form");
const input=document.getElementById("input");
const send=document.getElementById("send");
const messages=[];
function add(role,text,cls=role){
  const el=document.createElement("div");
  el.className="msg "+cls;
  el.textContent=text;
  chat.appendChild(el);
  window.scrollTo({top:document.body.scrollHeight,behavior:"smooth"});
}
form.addEventListener("submit",async(e)=>{
  e.preventDefault();
  const text=input.value.trim();
  if(!text||send.disabled)return;
  messages.push({role:"user",content:text});
  add("user",text);
  input.value="";
  send.disabled=true;
  try{
    const r=await fetch("/api/chat",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({messages})
    });
    const data=await r.json();
    if(!r.ok)throw new Error(data.error||("HTTP "+r.status));
    messages.push({role:"assistant",content:data.reply});
    add("assistant",data.reply);
  }catch(err){
    add("assistant",String(err.message||err),"error");
  }finally{
    send.disabled=false;
    input.focus();
  }
});
input.addEventListener("keydown",(e)=>{
  if(e.key==="Enter"&&!e.shiftKey){
    e.preventDefault();
    form.requestSubmit();
  }
});
</script>
</body>
</html>
"""


class AnalystHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        *,
        user: str,
        password: str,
        client: OpenAIResponsesClient,
    ) -> None:
        self.web_user = user
        self.web_password = password
        self.responses_client = client
        super().__init__(server_address, handler_class)


class AnalystRequestHandler(BaseHTTPRequestHandler):
    server: AnalystHTTPServer
    server_version = "MOEXAnalyst/1"
    sys_version = ""

    def _send_headers(self, status: int, content_type: str, length: int) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(length))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
            "connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
        )
        self.end_headers()

    def _send_json(self, status: int, payload: object) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        self._send_headers(status, "application/json; charset=utf-8", len(body))
        self.wfile.write(body)

    def _authorized(self) -> bool:
        raw = self.headers.get("Authorization", "")
        scheme, separator, credential = raw.partition(" ")
        if scheme != "Basic" or separator != " " or not credential:
            return False
        try:
            decoded = base64.b64decode(credential, validate=True).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return False
        user, separator, password = decoded.partition(":")
        if separator != ":":
            return False
        return hmac.compare_digest(user.encode("utf-8"), self.server.web_user.encode("utf-8")) and hmac.compare_digest(
            password.encode("utf-8"), self.server.web_password.encode("utf-8")
        )

    def _require_auth(self) -> bool:
        if self._authorized():
            return True
        body = b'{"error":"unauthorized"}'
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="MOEX Analyst", charset="UTF-8"')
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
        return False

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            self._send_json(400, {"error": "unsupported_query"})
            return
        if parsed.path == "/healthz":
            self._send_json(200, {"project": PROJECT, "status": "OK"})
            return
        if parsed.path != "/":
            self._send_json(404, {"error": "not_found"})
            return
        if not self._require_auth():
            return
        body = INDEX_HTML.encode("utf-8")
        self._send_headers(200, "text/html; charset=utf-8", len(body))
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        if parsed.path != "/api/chat" or parsed.query or parsed.fragment:
            self._send_json(404, {"error": "not_found"})
            return
        if not self._require_auth():
            return

        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length or "")
        except ValueError:
            self._send_json(411, {"error": "content_length_required"})
            return
        if length <= 0 or length > MAX_BODY_BYTES:
            self._send_json(413, {"error": "request_too_large"})
            return
        if self.headers.get_content_type() != "application/json":
            self._send_json(415, {"error": "content_type_must_be_application_json"})
            return

        try:
            payload = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json(400, {"error": "malformed_json"})
            return
        if not isinstance(payload, dict) or set(payload) != {"messages"}:
            self._send_json(400, {"error": "payload_must_contain_only_messages"})
            return

        try:
            messages = _normalize_messages(payload["messages"])
            reply = self.server.responses_client.answer(messages)
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
            return
        except WebChatUpstreamError as exc:
            self.log_error("%s", str(exc))
            self._send_json(502, {"error": "upstream_unavailable"})
            return
        except Exception:
            self.log_error("unexpected chat failure")
            self._send_json(500, {"error": "internal_error"})
            return

        self._send_json(200, {"reply": reply})

    def do_PUT(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method_not_allowed"})

    def do_PATCH(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method_not_allowed"})

    def do_DELETE(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method_not_allowed"})


def create_server(
    *,
    api_key: str,
    model: str,
    mcp_url: str,
    user: str,
    password: str,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    post: Any = requests.post,
    tool_caller: Any = call_mcp_tool,
) -> AnalystHTTPServer:
    if host != DEFAULT_HOST:
        raise WebChatConfigurationError(f"web-chat must bind only to loopback {DEFAULT_HOST}")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise WebChatConfigurationError("port must be an integer in 1..65535")
    if mcp_url != DEFAULT_MCP_URL:
        raise WebChatConfigurationError(
            f"MCP URL must remain the private KZ loopback endpoint {DEFAULT_MCP_URL}"
        )
    client = OpenAIResponsesClient(
        api_key=api_key,
        model=model,
        mcp_url=mcp_url,
        post=post,
        tool_caller=tool_caller,
    )
    return AnalystHTTPServer(
        (host, port),
        AnalystRequestHandler,
        user=user,
        password=_validated_secret(password, name=WEB_PASSWORD_ENV),
        client=client,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Private MOEX Analyst web-chat")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    config = load_config()
    server = create_server(
        api_key=config["api_key"],
        model=config["model"],
        mcp_url=config["mcp_url"],
        user=config["user"],
        password=config["password"],
        host=args.host,
        port=args.port,
    )
    print(f"PROJECT={PROJECT}")
    print(f"MODE={APP_MODE}")
    print(f"LISTEN={DEFAULT_HOST}:{server.server_address[1]}")
    print(f"MODEL={config['model']}")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

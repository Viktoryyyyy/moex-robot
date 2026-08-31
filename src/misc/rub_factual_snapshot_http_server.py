#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hmac
import json
import os
from collections.abc import Callable, Mapping, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from dotenv import dotenv_values

from src.moex_research.consumers.usdrubf_chat_snapshot_consumer import (
    load_analysis_chat_snapshot,
)


PROJECT = "MOEX_Bot"
API_MODE = "rub_factual_snapshot_network_api_v1"
TOKEN_ENV = "MOEX_RUB_SNAPSHOT_API_TOKEN"
ENV_FILE_ENV = "MOEX_ENV_FILE"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
SNAPSHOT_PATH = "/v1/rub/factual-snapshot"
READINESS_PATH = "/readyz"

SnapshotLoader = Callable[[], dict[str, object]]


class SnapshotAPIConfigurationError(RuntimeError):
    """Fail-closed runtime configuration error for the network transport."""


def _validated_token(value: object, *, source: str) -> str:
    if not isinstance(value, str) or not value:
        raise SnapshotAPIConfigurationError(f"{TOKEN_ENV} is missing in {source}")
    if value != value.strip():
        raise SnapshotAPIConfigurationError(
            f"{TOKEN_ENV} in {source} must not contain surrounding whitespace"
        )
    if not value:
        raise SnapshotAPIConfigurationError(f"{TOKEN_ENV} is blank in {source}")
    return value


def load_api_token(environ: Mapping[str, str] | None = None) -> str:
    """Load the API bearer token without exporting unrelated project secrets."""

    env = os.environ if environ is None else environ
    direct = env.get(TOKEN_ENV)
    if direct is not None:
        return _validated_token(direct, source="process environment")

    raw_env_file = env.get(ENV_FILE_ENV)
    if not raw_env_file or raw_env_file != raw_env_file.strip():
        raise SnapshotAPIConfigurationError(
            f"{TOKEN_ENV} is not set and {ENV_FILE_ENV} is missing or invalid"
        )
    env_path = Path(raw_env_file)
    if not env_path.is_absolute():
        raise SnapshotAPIConfigurationError(f"{ENV_FILE_ENV} must be an absolute path")
    if env_path.is_symlink() or not env_path.is_file():
        raise SnapshotAPIConfigurationError(f"{ENV_FILE_ENV} must reference a regular non-symlink file")

    configured = dotenv_values(env_path).get(TOKEN_ENV)
    return _validated_token(configured, source=ENV_FILE_ENV)


class SnapshotHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        *,
        api_token: str,
        snapshot_loader: SnapshotLoader,
    ) -> None:
        self.api_token = api_token
        self.snapshot_loader = snapshot_loader
        super().__init__(server_address, handler_class)


class SnapshotRequestHandler(BaseHTTPRequestHandler):
    server: SnapshotHTTPServer

    def _send_json(self, status_code: int, payload: object) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        raw = self.headers.get("Authorization", "")
        scheme, separator, credential = raw.partition(" ")
        if separator != " " or scheme != "Bearer" or not credential:
            return False
        if credential != credential.strip() or any(char.isspace() for char in credential):
            return False
        return hmac.compare_digest(credential, self.server.api_token)

    def _require_authorization(self) -> bool:
        if self._authorized():
            return True
        body = json.dumps(
            {"error": "unauthorized"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.send_response(401)
        self.send_header("WWW-Authenticate", "Bearer")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
        return False

    def _load_snapshot(self) -> dict[str, object] | None:
        try:
            snapshot = self.server.snapshot_loader()
            if not isinstance(snapshot, dict):
                raise RuntimeError("governed snapshot consumer returned a non-object")
            return snapshot
        except Exception:
            self.log_error("canonical snapshot read/validation failed")
            return None

    def _serve_snapshot(self) -> None:
        snapshot = self._load_snapshot()
        if snapshot is None:
            self._send_json(503, {"error": "snapshot_unavailable"})
            return
        self._send_json(200, snapshot)

    def _serve_readiness(self) -> None:
        snapshot = self._load_snapshot()
        if snapshot is None:
            self._send_json(503, {"status": "NOT_READY", "reason": "snapshot_unavailable"})
            return

        readiness = snapshot.get("readiness")
        freshness = snapshot.get("read_freshness")
        identity = snapshot.get("identity")
        readiness_status = readiness.get("status") if isinstance(readiness, Mapping) else None
        freshness_status = freshness.get("status") if isinstance(freshness, Mapping) else None
        generated_at = identity.get("generated_at_utc") if isinstance(identity, Mapping) else None
        read_at = freshness.get("read_at_utc") if isinstance(freshness, Mapping) else None
        ready = readiness_status == "READY" and freshness_status == "FRESH"
        payload = {
            "status": "READY" if ready else "NOT_READY",
            "schema_version": snapshot.get("schema_version"),
            "snapshot_generated_at_utc": generated_at,
            "snapshot_read_at_utc": read_at,
            "snapshot_readiness": readiness_status,
            "snapshot_freshness": freshness_status,
        }
        self._send_json(200 if ready else 503, payload)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        parsed = urlsplit(self.path)
        if parsed.path not in {SNAPSHOT_PATH, READINESS_PATH}:
            self._send_json(404, {"error": "not_found"})
            return
        if parsed.query or parsed.fragment:
            self._send_json(400, {"error": "unsupported_query"})
            return
        if not self._require_authorization():
            return
        if parsed.path == SNAPSHOT_PATH:
            self._serve_snapshot()
            return
        self._serve_readiness()

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        self._send_json(405, {"error": "method_not_allowed"})

    def do_PUT(self) -> None:  # noqa: N802 - stdlib handler contract
        self._send_json(405, {"error": "method_not_allowed"})

    def do_PATCH(self) -> None:  # noqa: N802 - stdlib handler contract
        self._send_json(405, {"error": "method_not_allowed"})

    def do_DELETE(self) -> None:  # noqa: N802 - stdlib handler contract
        self._send_json(405, {"error": "method_not_allowed"})



def create_server(
    *,
    api_token: str,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    snapshot_loader: SnapshotLoader | None = None,
) -> SnapshotHTTPServer:
    if host != DEFAULT_HOST:
        raise SnapshotAPIConfigurationError(
            f"application transport must bind to loopback {DEFAULT_HOST}; external ingress is separate"
        )
    if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65535):
        raise SnapshotAPIConfigurationError("port must be an integer in 1..65535")
    token = _validated_token(api_token, source="server configuration")
    loader = load_analysis_chat_snapshot if snapshot_loader is None else snapshot_loader
    return SnapshotHTTPServer(
        (host, port),
        SnapshotRequestHandler,
        api_token=token,
        snapshot_loader=loader,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve the canonical persisted MOEX Bot RUB factual snapshot over read-only HTTP"
    )
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    token = load_api_token()
    server = create_server(api_token=token, port=args.port)
    print(f"PROJECT={PROJECT}")
    print(f"MODE={API_MODE}")
    print(f"LISTEN={DEFAULT_HOST}:{server.server_address[1]}")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

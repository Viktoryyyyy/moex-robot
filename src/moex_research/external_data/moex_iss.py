from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final
from urllib.parse import parse_qsl, urlsplit

from .models import (
    ExternalDataError,
    HttpTransport,
    fetch_bytes,
    parse_json_object,
    raw_payload_sha256,
)

Sleeper = Callable[[float], None]
UtcClock = Callable[[], datetime]

MOEX_ISS_HOST: Final[str] = "iss.moex.com"
TRANSIENT_HTTP_ERROR_MESSAGE: Final[str] = "external-data request failed"
_SHA256_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")

__all__ = (
    "IssBlock",
    "MOEX_ISS_HOST",
    "MoexIssClient",
    "MoexIssClientError",
    "RetryPolicy",
    "Sleeper",
    "TRANSIENT_HTTP_ERROR_MESSAGE",
    "UtcClock",
    "parse_iss_block",
    "require_utc",
    "utc_now",
    "validate_official_route",
)


class MoexIssClientError(ValueError):
    """A structured route, schema, payload, or timestamp validation failure."""

    def __init__(self, reason: str, *, block_name: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.block_name = block_name


@dataclass(frozen=True)
class RetryPolicy:
    maximum_total_attempts: int
    retry_delays_seconds: tuple[float, ...]
    transient_error_message: str = TRANSIENT_HTTP_ERROR_MESSAGE

    def __post_init__(self) -> None:
        if self.maximum_total_attempts < 1:
            raise ValueError("maximum_total_attempts must be positive")
        if len(self.retry_delays_seconds) != self.maximum_total_attempts - 1:
            raise ValueError("retry delay count must equal maximum_total_attempts minus one")
        if any(delay < 0 for delay in self.retry_delays_seconds):
            raise ValueError("retry delays must be non-negative")
        if not self.transient_error_message:
            raise ValueError("transient_error_message must be non-empty")


@dataclass(frozen=True)
class IssBlock:
    rows: list[dict[str, object]]
    columns: tuple[str, ...]
    raw_payload_sha256: str
    root: Mapping[str, Any]


@dataclass(frozen=True)
class MoexIssClient:
    retry_policy: RetryPolicy
    transport: HttpTransport = fetch_bytes
    sleeper: Sleeper = time.sleep
    clock: UtcClock = lambda: datetime.now(timezone.utc).replace(microsecond=0)
    host: str = MOEX_ISS_HOST

    def fetch(self, url: str) -> bytes:
        for attempt in range(1, self.retry_policy.maximum_total_attempts + 1):
            try:
                return self.transport(url)
            except ExternalDataError as exc:
                if exc.args != (self.retry_policy.transient_error_message,):
                    raise
                if attempt == self.retry_policy.maximum_total_attempts:
                    raise ExternalDataError(
                        f"{self.retry_policy.transient_error_message}; "
                        f"official route={url}; "
                        f"attempts={self.retry_policy.maximum_total_attempts}"
                    ) from exc
                self.sleeper(self.retry_policy.retry_delays_seconds[attempt - 1])
        raise AssertionError("unreachable MOEX ISS retry state")

    def now_utc(self) -> datetime:
        return require_utc(self.clock())

    def validate_route(
        self,
        url: str,
        *,
        expected_path: str | None = None,
        allowed_path_prefix: str | None = None,
    ) -> dict[str, str]:
        return validate_official_route(
            url,
            expected_path=expected_path,
            allowed_path_prefix=allowed_path_prefix,
            expected_host=self.host,
        )

    def parse_block(
        self,
        payload: bytes,
        *,
        block_name: str,
        required_columns: Sequence[str],
    ) -> IssBlock:
        return parse_iss_block(
            payload,
            block_name=block_name,
            required_columns=required_columns,
        )


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def require_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise MoexIssClientError("timestamp_not_timezone_aware")
    if value.utcoffset() != timedelta(0):
        raise MoexIssClientError("timestamp_not_expressed_in_utc")
    return value.astimezone(timezone.utc)


def validate_official_route(
    url: str,
    *,
    expected_path: str | None = None,
    allowed_path_prefix: str | None = None,
    expected_host: str = MOEX_ISS_HOST,
) -> dict[str, str]:
    if (expected_path is None) == (allowed_path_prefix is None):
        raise ValueError("exactly one path constraint must be supplied")
    parsed = urlsplit(url)
    path_matches = (
        parsed.path == expected_path
        if expected_path is not None
        else parsed.path.startswith(str(allowed_path_prefix))
    )
    if (
        parsed.scheme != "https"
        or parsed.hostname != expected_host
        or parsed.port not in (None, 443)
        or parsed.username
        or parsed.password
        or not path_matches
    ):
        raise MoexIssClientError("route_not_allowlisted")
    return dict(parse_qsl(parsed.query, keep_blank_values=True))


def parse_iss_block(
    payload: bytes,
    *,
    block_name: str,
    required_columns: Sequence[str],
) -> IssBlock:
    try:
        root = parse_json_object(payload)
    except ExternalDataError as exc:
        raise MoexIssClientError("invalid_json", block_name=block_name) from exc
    value = root.get(block_name)
    if not isinstance(value, Mapping):
        raise MoexIssClientError("missing_block", block_name=block_name)
    columns = value.get("columns")
    data = value.get("data")
    if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
        raise MoexIssClientError("malformed_columns", block_name=block_name)
    if not set(required_columns).issubset(columns):
        raise MoexIssClientError("missing_required_columns", block_name=block_name)
    if not isinstance(data, list):
        raise MoexIssClientError("malformed_data", block_name=block_name)
    rows: list[dict[str, object]] = []
    for raw in data:
        if not isinstance(raw, list) or len(raw) != len(columns):
            raise MoexIssClientError("row_width_mismatch", block_name=block_name)
        rows.append(dict(zip(columns, raw, strict=True)))
    digest = raw_payload_sha256(payload)
    if not _SHA256_PATTERN.fullmatch(digest):  # pragma: no cover - hashlib invariant
        raise MoexIssClientError("invalid_payload_digest", block_name=block_name)
    return IssBlock(
        rows=rows,
        columns=tuple(columns),
        raw_payload_sha256=digest,
        root=root,
    )

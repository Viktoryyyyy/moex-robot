from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping
from datetime import date, datetime, timezone
from typing import Any, Final
from urllib.request import Request, urlopen


HttpTransport = Callable[[str], bytes]

BLOCKED_STATUSES: Final[frozenset[str]] = frozenset(
    {
        "diagnostic_only",
        "blocked_pending_historical_intraday_source",
        "blocked_pending_license",
        "blocked_pending_source_validation",
        "blocked_pending_vintage_policy",
    }
)
ALLOWED_HISTORICAL_STATUSES: Final[frozenset[str]] = frozenset(
    {"candidate_for_phase8_2", *BLOCKED_STATUSES}
)


class ExternalDataError(ValueError):
    """A fail-closed source, schema, chronology, or availability error."""


def fetch_bytes(url: str) -> bytes:
    if not url.startswith("https://"):
        raise ExternalDataError("external-data route must use https")
    request = Request(
        url,
        headers={
            "Accept": "application/json,text/html;q=0.9,*/*;q=0.1",
            "User-Agent": "moex-robot-phase8.1/1.0",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = response.read()
    except Exception as exc:  # pragma: no cover - exercised only by real HTTP
        raise ExternalDataError("external-data request failed") from exc
    if not payload:
        raise ExternalDataError("external-data response is empty")
    return payload


def raw_payload_sha256(payload: bytes) -> str:
    if not isinstance(payload, bytes) or not payload:
        raise ExternalDataError("raw payload must be non-empty bytes")
    return hashlib.sha256(payload).hexdigest()


def parse_json_object(payload: bytes) -> Mapping[str, Any]:
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalDataError("response is not valid UTF-8 JSON") from exc
    if not isinstance(decoded, Mapping):
        raise ExternalDataError("JSON response root must be an object")
    return decoded


def parse_date(value: object, *, field: str) -> date:
    text = str(value).strip()
    for pattern in ("%Y-%m-%d", "%d.%m.%Y", "%d %b %Y"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    raise ExternalDataError(f"{field} is not a supported date")


def parse_datetime(value: object, *, field: str) -> datetime:
    text = str(value).strip()
    if not text:
        raise ExternalDataError(f"{field} is required")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalDataError(f"{field} is not ISO-compatible") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalDataError(f"{field} timezone is missing or ambiguous")
    return parsed


def require_retrieved_at_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ExternalDataError("retrieved_at_utc must be timezone-aware")
    normalized = value.astimezone(timezone.utc)
    if normalized.utcoffset() != timezone.utc.utcoffset(normalized):
        raise ExternalDataError("retrieved_at_utc must normalize to UTC")
    return normalized


def parse_number(value: object, *, field: str) -> float:
    text = str(value).strip().replace("\u00a0", "").replace(" ", "")
    if not text or text in {"-", "—"}:
        raise ExternalDataError(f"{field} is malformed")
    if not re.fullmatch(r"[+-]?[0-9][0-9.,]*", text):
        raise ExternalDataError(f"{field} is malformed")
    if "," in text and "." in text:
        normalized = text.replace(",", "")
    elif "," in text:
        left, right = text.rsplit(",", 1)
        normalized = text.replace(",", "") if len(right) == 3 else left + "." + right
    else:
        normalized = text
    try:
        number = float(normalized)
    except ValueError as exc:  # defensive; regex handles ordinary cases
        raise ExternalDataError(f"{field} is malformed") from exc
    if not math.isfinite(number):
        raise ExternalDataError(f"{field} is non-finite")
    return number


def parse_integer(value: object, *, field: str) -> int:
    number = parse_number(value, field=field)
    if not number.is_integer():
        raise ExternalDataError(f"{field} must be an integer")
    return int(number)


def provenance(
    *,
    source_id: str,
    source_route: str,
    payload: bytes,
    retrieved_at_utc: datetime,
    source_revision_status: str,
    historical_model_use_status: str,
) -> dict[str, object]:
    if historical_model_use_status not in ALLOWED_HISTORICAL_STATUSES:
        raise ExternalDataError("unsupported historical_model_use_status")
    retrieved = require_retrieved_at_utc(retrieved_at_utc)
    return {
        "source_id": source_id,
        "source_route": source_route,
        "retrieved_at_utc": retrieved.isoformat().replace("+00:00", "Z"),
        "raw_payload_sha256": raw_payload_sha256(payload),
        "source_revision_status": source_revision_status,
        "historical_model_use_status": historical_model_use_status,
    }

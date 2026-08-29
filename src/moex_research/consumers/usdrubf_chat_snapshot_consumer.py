from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

from src.moex_research.runners.usdrubf_s7_3_chat_analysis_snapshot import (
    PROJECT,
    SCHEMA_VERSION,
    read_current_snapshot,
)


class ChatSnapshotConsumerError(RuntimeError):
    """Fail-closed error for the snapshot-only analysis-chat consumer boundary."""


SnapshotReader = Callable[..., tuple[dict[str, object], object]]


def _require_mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ChatSnapshotConsumerError(f"{field} must be an object")
    return value


def validate_analysis_chat_snapshot(snapshot: Mapping[str, object]) -> None:
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        raise ChatSnapshotConsumerError("snapshot schema_version mismatch")

    identity = _require_mapping(snapshot.get("identity"), "identity")
    if identity.get("project") != PROJECT:
        raise ChatSnapshotConsumerError("snapshot project identity mismatch")
    generated_at = identity.get("generated_at_utc")
    if not isinstance(generated_at, str) or not generated_at.strip():
        raise ChatSnapshotConsumerError("snapshot identity.generated_at_utc missing")

    refresh_policy = _require_mapping(snapshot.get("refresh_policy"), "refresh_policy")
    stale_after = refresh_policy.get("snapshot_stale_after_seconds")
    if isinstance(stale_after, bool) or not isinstance(stale_after, int) or stale_after < 0:
        raise ChatSnapshotConsumerError(
            "snapshot refresh_policy.snapshot_stale_after_seconds is invalid"
        )

    freshness = _require_mapping(snapshot.get("read_freshness"), "read_freshness")
    if freshness.get("status") not in {"FRESH", "STALE"}:
        raise ChatSnapshotConsumerError("snapshot read_freshness.status is invalid")
    age = freshness.get("snapshot_age_seconds")
    if isinstance(age, bool) or not isinstance(age, int) or age < 0:
        raise ChatSnapshotConsumerError("snapshot read_freshness.snapshot_age_seconds is invalid")
    read_at = freshness.get("read_at_utc")
    if not isinstance(read_at, str) or not read_at.strip():
        raise ChatSnapshotConsumerError("snapshot read_freshness.read_at_utc missing")

    readiness = _require_mapping(snapshot.get("readiness"), "readiness")
    if readiness.get("status") not in {"READY", "PARTIAL"}:
        raise ChatSnapshotConsumerError("snapshot readiness.status is invalid")

    authority = _require_mapping(snapshot.get("authority"), "authority")
    expected_false = (
        "server_generates_market_analysis",
        "server_generates_scenario",
        "server_generates_buy_sell_out",
        "server_generates_invalidation",
        "ema_standalone_directional_authority",
        "news_directional_action_authority",
        "broker_execution",
        "telegram_delivery",
    )
    if authority.get("data_only") is not True:
        raise ChatSnapshotConsumerError("snapshot authority.data_only must be true")
    if any(authority.get(field) is not False for field in expected_false):
        raise ChatSnapshotConsumerError("snapshot authority boundary mismatch")


def load_analysis_chat_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    reader: SnapshotReader = read_current_snapshot,
) -> dict[str, object]:
    """Return the canonical reader-enriched snapshot and nothing else.

    The reader adds read-time freshness metadata. Weekly/Daily chat contracts still
    require freshness to be recomputed at their actual analysis time, so this
    transport never upgrades data authority or freezes an earlier FRESH verdict.
    """

    snapshot, _path = reader(now_fn=now_fn)
    validate_analysis_chat_snapshot(snapshot)
    return snapshot


__all__ = [
    "ChatSnapshotConsumerError",
    "load_analysis_chat_snapshot",
    "validate_analysis_chat_snapshot",
]

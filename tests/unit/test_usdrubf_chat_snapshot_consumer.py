from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.moex_research.consumers.usdrubf_chat_snapshot_consumer import (
    ChatSnapshotConsumerError,
    load_analysis_chat_snapshot,
    validate_analysis_chat_snapshot,
)


def _snapshot() -> dict[str, object]:
    return {
        "schema_version": "rub_chat_analysis_snapshot.v1",
        "identity": {
            "project": "MOEX_Bot",
            "generated_at_utc": "2026-08-29T15:37:34+00:00",
        },
        "refresh_policy": {"snapshot_stale_after_seconds": 1200},
        "read_freshness": {
            "read_at_utc": "2026-08-29T15:38:00+00:00",
            "snapshot_age_seconds": 26,
            "status": "FRESH",
        },
        "readiness": {"status": "PARTIAL"},
        "authority": {
            "data_only": True,
            "server_generates_market_analysis": False,
            "server_generates_scenario": False,
            "server_generates_buy_sell_out": False,
            "server_generates_invalidation": False,
            "ema_standalone_directional_authority": False,
            "news_directional_action_authority": False,
            "broker_execution": False,
            "telegram_delivery": False,
        },
        "components": {},
    }


def test_load_analysis_chat_snapshot_returns_reader_enriched_snapshot_only() -> None:
    expected = _snapshot()
    seen: list[datetime] = []
    now = datetime(2026, 8, 29, 15, 38, tzinfo=timezone.utc)

    def reader(*, now_fn):
        seen.append(now_fn())
        return deepcopy(expected), Path("/ignored/by/consumer")

    result = load_analysis_chat_snapshot(now_fn=lambda: now, reader=reader)

    assert result == expected
    assert seen == [now]
    assert "path" not in result


def test_validate_analysis_chat_snapshot_rejects_action_authority() -> None:
    value = _snapshot()
    value["authority"]["server_generates_buy_sell_out"] = True  # type: ignore[index]

    with pytest.raises(ChatSnapshotConsumerError, match="authority boundary mismatch"):
        validate_analysis_chat_snapshot(value)


def test_validate_analysis_chat_snapshot_rejects_missing_reader_freshness() -> None:
    value = _snapshot()
    value.pop("read_freshness")

    with pytest.raises(ChatSnapshotConsumerError, match="read_freshness must be an object"):
        validate_analysis_chat_snapshot(value)


@pytest.mark.parametrize("stale_after", [None, -1, True, 1.5, "1200"])
def test_validate_analysis_chat_snapshot_rejects_invalid_stale_threshold(stale_after) -> None:
    value = _snapshot()
    value["refresh_policy"] = {"snapshot_stale_after_seconds": stale_after}

    with pytest.raises(ChatSnapshotConsumerError, match="snapshot_stale_after_seconds is invalid"):
        validate_analysis_chat_snapshot(value)


def test_validate_analysis_chat_snapshot_accepts_stale_for_downstream_recheck() -> None:
    value = _snapshot()
    value["read_freshness"] = {
        "read_at_utc": "2026-08-29T16:00:00+00:00",
        "snapshot_age_seconds": 1346,
        "status": "STALE",
    }

    validate_analysis_chat_snapshot(value)

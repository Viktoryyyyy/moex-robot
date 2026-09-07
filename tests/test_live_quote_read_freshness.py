from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from moex_data.rub_snapshot_read_freshness import apply_read_freshness


SOURCE_TS = "2026-09-07T08:00:00+00:00"


def _snapshot(*, quote_usable: bool, quote_status: str, quote_reason: str | None) -> dict[str, object]:
    return {
        "components": {
            "synchronized_live_market_oi": {
                "status": "PARTIAL",
                "data": {
                    "status": "PARTIAL",
                    "instruments": {
                        "usdrubf": {
                            "timestamp": SOURCE_TS,
                            "stale": False,
                            "price_oi_usable": True,
                            "quote_usable": quote_usable,
                            "quote_status": quote_status,
                            "quote_reason": quote_reason,
                            "quote_stale": False,
                        }
                    },
                    "synchronization": {
                        "synchronized": False,
                        "all_instruments_fresh": True,
                        "futures_synchronized": True,
                        "futures_all_fresh": True,
                    },
                    "quality": {
                        "analysis_usable": False,
                        "price_oi_usable_by_instrument": {"usdrubf": True},
                        "price_oi_all_futures_usable": True,
                        "spot_price_usable": False,
                        "factual_context_usable": True,
                        "quote_usable_by_instrument": {"usdrubf": quote_usable},
                        "quote_all_instruments_usable": quote_usable,
                        "quote_required_for_analysis": False,
                    },
                },
            }
        }
    }


def test_read_time_staleness_downgrades_quote_without_mutating_persisted_snapshot() -> None:
    original = _snapshot(quote_usable=True, quote_status="available", quote_reason=None)
    persisted = deepcopy(original)

    view = apply_read_freshness(
        original,
        now=datetime(2026, 9, 7, 8, 2, 0, tzinfo=timezone.utc),
    )

    item = view["components"]["synchronized_live_market_oi"]["data"]["instruments"]["usdrubf"]
    quality = view["components"]["synchronized_live_market_oi"]["data"]["quality"]
    assert item["stale"] is True
    assert item["quote_stale"] is True
    assert item["quote_usable"] is False
    assert item["quote_status"] == "stale_quote_at_read"
    assert item["quote_reason"] == "source_not_fresh_at_read"
    assert quality["quote_usable_by_instrument"]["usdrubf"] is False
    assert quality["quote_all_instruments_usable"] is False
    assert original == persisted


def test_read_freshness_never_upgrades_structurally_unusable_crossed_quote() -> None:
    original = _snapshot(
        quote_usable=False,
        quote_status="crossed_quote_unusable",
        quote_reason="positive_BID_exceeds_positive_OFFER_and_quote_side_temporal_coherence_is_unproven",
    )

    view = apply_read_freshness(
        original,
        now=datetime(2026, 9, 7, 8, 0, 30, tzinfo=timezone.utc),
    )

    item = view["components"]["synchronized_live_market_oi"]["data"]["instruments"]["usdrubf"]
    quality = view["components"]["synchronized_live_market_oi"]["data"]["quality"]
    assert item["stale"] is False
    assert item["quote_stale"] is False
    assert item["quote_usable"] is False
    assert item["quote_status"] == "crossed_quote_unusable"
    assert quality["quote_usable_by_instrument"]["usdrubf"] is False
    assert quality["quote_all_instruments_usable"] is False


@pytest.mark.parametrize("persisted_gate", [True, False, None, 1, "true"])
@pytest.mark.parametrize("age_seconds", [30, 120])
def test_read_freshness_preserves_aggregate_quote_gate(
    persisted_gate: object, age_seconds: int
) -> None:
    original = _snapshot(quote_usable=True, quote_status="available", quote_reason=None)
    quality = original["components"]["synchronized_live_market_oi"]["data"]["quality"]
    quality["quote_all_instruments_usable"] = persisted_gate
    persisted = deepcopy(original)

    view = apply_read_freshness(
        original,
        now=datetime.fromisoformat(SOURCE_TS) + timedelta(seconds=age_seconds),
    )

    data = view["components"]["synchronized_live_market_oi"]["data"]
    fresh = age_seconds <= 60
    assert data["quality"]["quote_all_instruments_usable"] is (persisted_gate is True and fresh)
    assert data["quality"]["quote_usable_by_instrument"]["usdrubf"] is fresh
    assert data["instruments"]["usdrubf"]["quote_usable"] is fresh
    assert data["instruments"]["usdrubf"]["price_oi_usable"] is fresh
    assert data["quality"]["quote_required_for_analysis"] is False
    assert original == persisted


def test_read_freshness_does_not_invent_missing_aggregate_quote_approval() -> None:
    original = _snapshot(quote_usable=True, quote_status="available", quote_reason=None)
    quality = original["components"]["synchronized_live_market_oi"]["data"]["quality"]
    quality.pop("quote_all_instruments_usable")
    persisted = deepcopy(original)

    view = apply_read_freshness(
        original,
        now=datetime(2026, 9, 7, 8, 0, 30, tzinfo=timezone.utc),
    )

    data = view["components"]["synchronized_live_market_oi"]["data"]
    assert data["quality"]["quote_all_instruments_usable"] is False
    assert data["instruments"]["usdrubf"]["quote_usable"] is True
    assert data["instruments"]["usdrubf"]["price_oi_usable"] is True
    assert original == persisted


def test_read_freshness_empty_quote_map_cannot_keep_aggregate_approval() -> None:
    original = _snapshot(quote_usable=True, quote_status="available", quote_reason=None)
    original["components"]["synchronized_live_market_oi"]["data"]["quality"]["quote_usable_by_instrument"] = {}
    view = apply_read_freshness(
        original,
        now=datetime(2026, 9, 7, 8, 0, 30, tzinfo=timezone.utc),
    )
    assert view["components"]["synchronized_live_market_oi"]["data"]["quality"]["quote_all_instruments_usable"] is False


@pytest.mark.parametrize("quote_map", [None, [], ["usdrubf"], "invalid", 1, True])
@pytest.mark.parametrize("age_seconds", [30, 120])
def test_malformed_quote_map_fails_aggregate_closed(quote_map: object, age_seconds: int) -> None:
    original = _snapshot(quote_usable=True, quote_status="available", quote_reason=None)
    data = original["components"]["synchronized_live_market_oi"]["data"]
    data["quality"]["quote_usable_by_instrument"] = quote_map
    persisted = deepcopy(original)

    view = apply_read_freshness(
        original, now=datetime.fromisoformat(SOURCE_TS) + timedelta(seconds=age_seconds)
    )
    result = view["components"]["synchronized_live_market_oi"]["data"]
    assert result["quality"]["quote_usable_by_instrument"] == {}
    assert result["quality"]["quote_all_instruments_usable"] is False
    assert result["instruments"]["usdrubf"]["price_oi_usable"] is (age_seconds <= 60)
    assert result["instruments"]["usdrubf"]["quote_usable"] is (age_seconds <= 60)
    assert view["live_read_freshness"]["additional_live_fetch_performed"] is False
    assert original == persisted


def test_missing_quote_map_fails_aggregate_closed() -> None:
    original = _snapshot(quote_usable=True, quote_status="available", quote_reason=None)
    data = original["components"]["synchronized_live_market_oi"]["data"]
    data["quality"].pop("quote_usable_by_instrument")
    persisted = deepcopy(original)
    view = apply_read_freshness(
        original, now=datetime.fromisoformat(SOURCE_TS) + timedelta(seconds=30)
    )
    result = view["components"]["synchronized_live_market_oi"]["data"]
    assert result["quality"]["quote_all_instruments_usable"] is False
    assert result["quality"]["quote_usable_by_instrument"] == {}
    assert result["instruments"]["usdrubf"]["price_oi_usable"] is True
    assert original == persisted


@pytest.mark.parametrize("missing_id", ["usdrubf", "si_front", "si_next", "cnyrubf", "cr_front", "cr_next", "cnyrub_tom"])
def test_incomplete_quote_map_cannot_approve_all_instruments(missing_id: str) -> None:
    original = _snapshot(quote_usable=True, quote_status="available", quote_reason=None)
    data = original["components"]["synchronized_live_market_oi"]["data"]
    template = data["instruments"]["usdrubf"]
    keys = ("usdrubf", "si_front", "si_next", "cnyrubf", "cr_front", "cr_next", "cnyrub_tom")
    data["instruments"] = {key: deepcopy(template) for key in keys}
    data["quality"]["quote_usable_by_instrument"] = dict.fromkeys(keys, True)
    now = datetime.fromisoformat(SOURCE_TS) + timedelta(seconds=30)
    baseline = apply_read_freshness(original, now=now)
    assert baseline["components"]["synchronized_live_market_oi"]["data"]["quality"]["quote_all_instruments_usable"] is True

    data["quality"]["quote_usable_by_instrument"].pop(missing_id)
    persisted = deepcopy(original)
    view = apply_read_freshness(original, now=now)
    result = view["components"]["synchronized_live_market_oi"]["data"]
    assert result["quality"]["quote_all_instruments_usable"] is False
    assert missing_id not in result["quality"]["quote_usable_by_instrument"]
    assert result["instruments"] == baseline["components"]["synchronized_live_market_oi"]["data"]["instruments"]
    assert result["quality"]["price_oi_usable_by_instrument"] == baseline["components"]["synchronized_live_market_oi"]["data"]["quality"]["price_oi_usable_by_instrument"]
    assert result["synchronization"] == baseline["components"]["synchronized_live_market_oi"]["data"]["synchronization"]
    assert original == persisted


@pytest.mark.parametrize("map_value", [False, None, 1, "true", True])
@pytest.mark.parametrize("invalid_item", [False, True])
def test_quote_map_needs_true_entries_and_valid_instruments(map_value: object, invalid_item: bool) -> None:
    original = _snapshot(quote_usable=True, quote_status="available", quote_reason=None)
    data = original["components"]["synchronized_live_market_oi"]["data"]
    data["quality"]["quote_usable_by_instrument"]["usdrubf"] = map_value
    if invalid_item:
        data["instruments"]["usdrubf"] = None
    persisted = deepcopy(original)
    view = apply_read_freshness(
        original, now=datetime.fromisoformat(SOURCE_TS) + timedelta(seconds=30)
    )
    quality = view["components"]["synchronized_live_market_oi"]["data"]["quality"]
    expected = map_value is True and not invalid_item
    assert quality["quote_usable_by_instrument"]["usdrubf"] is expected
    assert quality["quote_all_instruments_usable"] is expected
    assert original == persisted


def test_extra_quote_map_key_fails_closed_without_affecting_valid_facts() -> None:
    original = _snapshot(quote_usable=True, quote_status="available", quote_reason=None)
    data = original["components"]["synchronized_live_market_oi"]["data"]
    data["quality"]["quote_usable_by_instrument"]["unknown"] = True
    persisted = deepcopy(original)
    view = apply_read_freshness(
        original, now=datetime.fromisoformat(SOURCE_TS) + timedelta(seconds=30)
    )
    result = view["components"]["synchronized_live_market_oi"]["data"]
    assert result["quality"]["quote_all_instruments_usable"] is False
    assert result["quality"]["quote_usable_by_instrument"] == {"usdrubf": True, "unknown": False}
    assert result["instruments"]["usdrubf"]["quote_usable"] is True
    assert result["instruments"]["usdrubf"]["price_oi_usable"] is True
    assert original == persisted

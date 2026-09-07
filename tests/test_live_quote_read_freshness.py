from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

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

from __future__ import annotations

from moex_data import synchronized_live_market_oi_context as core
from moex_data import synchronized_live_market_oi_context_partial as partial
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as overlay


def _instrument(logical_id: str, *, timestamp: str, stale: bool, future: bool) -> dict[str, object]:
    return {
        "logical_id": logical_id,
        "secid": logical_id.upper(),
        "last": 87.0 if future else 12.9,
        "oi": 1000 if future else None,
        "oi_status": "available" if future else "not_applicable",
        "timestamp": timestamp,
        "stale": stale,
        "price_oi_same_source_row": future,
        "price_oi_usable": False,
    }


def _stale_spot_snapshot() -> dict[str, object]:
    instruments = {
        logical_id: _instrument(
            logical_id,
            timestamp="2026-09-02T16:43:02+00:00",
            stale=False,
            future=True,
        )
        for logical_id in core.FUTURES_LOGICAL_ORDER
    }
    instruments["cnyrub_tom"] = _instrument(
        "cnyrub_tom",
        timestamp="2026-09-02T16:15:00+00:00",
        stale=True,
        future=False,
    )
    return {
        "schema_version": core.SCHEMA_VERSION,
        "status": "UNAVAILABLE",
        "synchronization": {
            "status": "FAIL",
            "synchronized": False,
            "as_of_utc": "2026-09-02T16:43:02+00:00",
            "oldest_timestamp_utc": "2026-09-02T16:15:00+00:00",
            "max_skew_seconds": 1682.0,
            "all_instruments_fresh": False,
        },
        "instruments": instruments,
        "quality": {
            "status": "FAIL",
            "analysis_usable": False,
            "price_oi_all_futures_usable": False,
            "price_oi_usable_by_instrument": {
                logical_id: False for logical_id in core.FUTURES_LOGICAL_ORDER
            },
            "spot_price_usable": False,
            "fail_closed": True,
        },
        "provenance": {},
    }


def _base_s7_snapshot() -> dict[str, object]:
    return {
        "components": {"existing": {"status": "READY"}},
        "authority": {},
        "analysis_views": {},
        "analysis_workflow": {},
    }


def test_stale_spot_preserves_fresh_same_row_futures_price_oi() -> None:
    result = partial._reclassify(_stale_spot_snapshot())

    assert result["status"] == "PARTIAL"
    assert result["synchronization"]["synchronized"] is False
    assert result["synchronization"]["futures_synchronized"] is True
    assert result["quality"]["analysis_usable"] is False
    assert result["quality"]["spot_price_usable"] is False
    assert result["quality"]["factual_context_usable"] is True
    assert result["quality"]["price_oi_all_futures_usable"] is True
    for logical_id in core.FUTURES_LOGICAL_ORDER:
        assert result["instruments"][logical_id]["price_oi_usable"] is True


def test_partial_component_is_factual_but_not_full_cross_market_ready() -> None:
    live = partial._reclassify(_stale_spot_snapshot())
    snapshot = _base_s7_snapshot()

    overlay.attach_live_market_oi_context(
        snapshot,
        live,
        attempted_at_utc="2026-09-02T16:43:05+00:00",
    )

    component = snapshot["components"][overlay.COMPONENT]
    assert component["status"] == "PARTIAL"
    assert component["refresh_error"] is None
    assert snapshot["readiness"]["status"] == "PARTIAL"
    assert overlay.COMPONENT in snapshot["readiness"]["partial_components"]
    assert overlay.COMPONENT not in snapshot["readiness"]["unavailable_components"]
    assert snapshot["authority"]["live_market_oi_factual_authority"] is True
    workflow = snapshot["analysis_workflow"]["price_x_oi"]
    assert "PARTIAL" in workflow["factual_use_requires"]
    assert "status=READY" in workflow["full_cross_market_use_requires"]
    assert workflow["directional_authority"] is False
    assert workflow["action_authority"] is False

from __future__ import annotations

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as overlay


def _base_snapshot() -> dict[str, object]:
    return {
        "components": {"existing": {"status": "READY"}},
        "authority": {},
        "analysis_views": {},
        "analysis_workflow": {},
    }


def _live(*, usable: bool) -> dict[str, object]:
    return {
        "schema_version": "synchronized_live_market_oi_context.v1",
        "status": "READY" if usable else "UNAVAILABLE",
        "quality": {
            "status": "PASS" if usable else "FAIL",
            "analysis_usable": usable,
        },
        "synchronization": {
            "status": "PASS" if usable else "FAIL",
            "synchronized": usable,
            "as_of_utc": "2026-09-02T10:00:12+00:00",
        },
        "instruments": {},
    }


def test_attach_ready_live_market_oi_component_grants_factual_only_authority() -> None:
    snapshot = _base_snapshot()
    overlay.attach_live_market_oi_context(
        snapshot,
        _live(usable=True),
        attempted_at_utc="2026-09-02T10:00:20+00:00",
    )

    component = snapshot["components"][overlay.COMPONENT]
    assert component["status"] == "READY"
    assert component["data_as_of"] == "2026-09-02T10:00:12+00:00"
    assert snapshot["readiness"]["status"] == "READY"
    assert snapshot["authority"]["live_market_oi_factual_authority"] is True
    assert snapshot["authority"]["live_market_oi_directional_authority"] is False
    assert snapshot["authority"]["live_market_oi_action_authority"] is False
    assert snapshot["analysis_workflow"]["price_x_oi"]["price_oi_same_source_row_required"] is True


def test_attach_unsynchronized_live_market_oi_component_makes_snapshot_partial() -> None:
    snapshot = _base_snapshot()
    overlay.attach_live_market_oi_context(
        snapshot,
        _live(usable=False),
        attempted_at_utc="2026-09-02T10:00:20+00:00",
    )

    component = snapshot["components"][overlay.COMPONENT]
    assert component["status"] == "UNAVAILABLE"
    assert snapshot["readiness"]["status"] == "PARTIAL"
    assert overlay.COMPONENT in snapshot["readiness"]["unavailable_components"]
    assert snapshot["authority"]["live_market_oi_factual_authority"] is False

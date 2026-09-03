from __future__ import annotations

import pytest

from moex_data.futures import futoi_intraday_previous_session_context as context
from moex_data.futures import futoi_live_factual_refresh_source_native as source
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_current_context as snapshot_context
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as futoi


def _factual(instrument_id: str, trade_date: str) -> dict[str, object]:
    return {
        "trade_date": trade_date,
        "snapshot_ts": trade_date + "T18:00:00+00:00",
        "source_ticker": "si" if instrument_id == source.SI_INSTRUMENT_ID else "cr",
        "secid": "SiU6" if instrument_id == source.SI_INSTRUMENT_ID else "CRU6",
    }


def _instrument_context(instrument_id: str) -> dict[str, object]:
    return {
        "schema_version": context.SCHEMA_VERSION,
        "status": "PASS",
        "quality_status": "PASS",
        "acceptance_status": "PASS",
        "through_date": "2026-09-03",
        "refresh_attempted_at": "2026-09-03T15:00:00+00:00",
        "observed_trade_dates": ["2026-09-02", "2026-09-03"],
        "observed_current_trade_date": "2026-09-03",
        "previous_observed_trade_date": "2026-09-02",
        context.CURRENT_ROLE: {
            "status": "FRESH",
            "last_success_at": "2026-09-03T15:00:00+00:00",
            "refresh_error_class": None,
            "refresh_error": None,
            "freshness": {"status": "FRESH"},
            "factual": _factual(instrument_id, "2026-09-03"),
            "provenance": {"raw_partition_ref": "current"},
        },
        context.PREVIOUS_ROLE: {
            "status": "FRESH",
            "last_success_at": "2026-09-03T15:00:00+00:00",
            "refresh_error_class": None,
            "refresh_error": None,
            "freshness": {"status": "FRESH"},
            "factual": _factual(instrument_id, "2026-09-02"),
            "provenance": {"raw_partition_ref": "previous"},
        },
    }


def test_attach_exposes_delta_statistics_without_granting_directional_or_action_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(futoi, "_load_governance", lambda: {})
    monkeypatch.setattr(
        futoi,
        "_governance_state",
        lambda _values, instrument_id: {
            "instrument_id": instrument_id,
            "factual_use_allowed": False,
            "factual_live_authority": False,
            "directional_authority": False,
            "action_authority": False,
            "standalone_buy_sell_authority": False,
        },
    )
    snapshot = {
        "components": {
            futoi.FUTOI_COMPONENT: {"status": "GOVERNED_BLOCKED", "data": {}},
            futoi.FUTOI_CR_COMPONENT: {"status": "GOVERNED_BLOCKED", "data": {}},
        },
        "authority": {},
        "analysis_views": {},
        "analysis_workflow": {},
    }
    refresh_bundle = {
        "instrument_results": {
            instrument_id: _instrument_context(instrument_id)
            for instrument_id in source.LIVE_INSTRUMENT_IDS
        }
    }
    delta_bundle = {
        "instrument_results": {
            instrument_id: {
                "schema_version": "futoi_delta_statistics_context.v1",
                "status": "PARTIAL",
                "instrument_id": instrument_id,
                "lag_targets": {
                    "delta_1d": "2026-09-02",
                    "delta_5d": "2026-08-29",
                    "delta_20d": "2026-08-12",
                },
                "deltas": {
                    "delta_1d": {"status": "AVAILABLE"},
                    "delta_5d": {"status": "UNAVAILABLE"},
                    "delta_20d": {"status": "AVAILABLE"},
                },
                "statistics": {"status": "AVAILABLE"},
                "factual_authority": False,
                "directional_authority": False,
                "action_authority": False,
                "standalone_buy_sell_authority": False,
                "stage5_full_mode_ready": False,
                "stage5_pointer_promotion_performed": False,
            }
            for instrument_id in source.LIVE_INSTRUMENT_IDS
        }
    }

    snapshot_context._attach_futoi_context(snapshot, refresh_bundle, delta_bundle)

    for instrument_id, component_name in futoi.FUTOI_COMPONENT_BY_INSTRUMENT.items():
        data = snapshot["components"][component_name]["data"]
        assert data["delta_statistics"]["instrument_id"] == instrument_id
        assert data["delta_statistics"]["factual_authority"] is False
        assert data["delta_statistics"]["directional_authority"] is False
        assert data["delta_statistics"]["action_authority"] is False
        assert data["delta_statistics"]["standalone_buy_sell_authority"] is False
        assert data["delta_statistics"]["stage5_full_mode_ready"] is False
        assert data["delta_statistics"]["stage5_pointer_promotion_performed"] is False
        assert data["consumer_factual_use_allowed"] is False
    assert snapshot["analysis_views"]["futoi_context_fields"]["delta_1d"] == "delta_statistics.deltas.delta_1d"
    assert snapshot["analysis_views"]["futoi_context_fields"]["delta_5d"] == "delta_statistics.deltas.delta_5d"
    assert snapshot["analysis_views"]["futoi_context_fields"]["delta_20d"] == "delta_statistics.deltas.delta_20d"
    assert snapshot["authority"]["futoi_directional_authority"] is False
    assert snapshot["authority"]["futoi_action_authority"] is False

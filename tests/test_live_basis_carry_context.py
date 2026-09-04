from __future__ import annotations

import copy
import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from moex_data import live_basis_carry_context as basis
from moex_data import synchronized_live_market_oi_context as live_core
from moex_data.analytics.materialize_rub_basis_carry_5m import build_basis_carry_frame


TS = "2026-09-04T07:00:00+00:00"
RECEIVED = "2026-09-04T07:00:10+00:00"


def _instrument(
    logical_id: str,
    secid: str,
    last: float,
    *,
    source_id: str = "moex_apim_forts_rfud_live_marketdata",
    timestamp: str = TS,
    age_seconds: float = 10.0,
    stale: bool = False,
    expiry_date: str | None = None,
) -> dict[str, object]:
    item: dict[str, object] = {
        "logical_id": logical_id,
        "secid": secid,
        "last": last,
        "timestamp": timestamp,
        "received_at_utc": RECEIVED,
        "age_seconds": age_seconds,
        "stale": stale,
        "source_id": source_id,
    }
    if expiry_date is not None:
        item["expiry_date"] = expiry_date
        item["expiry_metadata"] = {
            "source_id": "moex_apim_forts_rfud_live_securities",
            "source_field": "LASTTRADEDATE",
            "same_rfud_response_as_live_binding": True,
        }
    return item


def _live_snapshot() -> dict[str, object]:
    return {
        "schema_version": live_core.SCHEMA_VERSION,
        "status": "READY",
        "snapshot_received_at_utc": RECEIVED,
        "instruments": {
            "usdrubf": _instrument("usdrubf", "USDRUBF", 91.0),
            "si_front": _instrument("si_front", "SiU6", 92000.0, expiry_date="2026-09-17"),
            "si_next": _instrument("si_next", "SiZ6", 93000.0, expiry_date="2026-12-17"),
            "cnyrubf": _instrument("cnyrubf", "CNYRUBF", 12.1),
            "cr_front": _instrument("cr_front", "CRU6", 12.2, expiry_date="2026-09-17"),
            "cr_next": _instrument("cr_next", "CRZ6", 12.3, expiry_date="2026-12-17"),
            "cnyrub_tom": _instrument(
                "cnyrub_tom",
                "CNYRUB_TOM",
                12.0,
                source_id="moex_apim_cets_cnyrub_tom_live_marketdata",
            ),
        },
    }


def _metric(context: dict[str, object], pair_key: str, stage4_id: str) -> dict[str, object]:
    pair = context["pairs"][pair_key]
    return next(item for item in pair["metrics"] if item["stage4_metric_id"] == stage4_id)


def test_live_basis_carry_reuses_stage4_units_formulas_and_missing_usd_spot_fails_closed() -> None:
    context = basis.build_context(_live_snapshot())

    assert context["status"] == "PARTIAL"
    assert context["live_leg_availability"]["unavailable"] == ["usd_tom"]
    assert context["live_input_policy"]["additional_live_fetch_performed"] is False
    assert context["live_input_policy"]["stale_stage3_value_allowed_as_live"] is False
    assert context["live_input_policy"]["missing_value_interpreted_as_zero"] is False

    usd = context["pairs"]["usd_rub"]
    cny = context["pairs"]["cny_rub"]
    assert usd["legs"]["si_front"]["raw_unit"] == "RUB_per_1000_USD"
    assert usd["legs"]["si_front"]["normalization_divisor"] == 1000.0
    assert usd["legs"]["si_front"]["normalized_rate"] == pytest.approx(92.0)
    assert cny["legs"]["cr_front"]["normalization_divisor"] == 1.0
    assert cny["legs"]["cr_front"]["normalized_rate"] == pytest.approx(12.2)

    missing = _metric(context, "usd_rub", "perpetual_spot_basis_abs")
    assert missing["status"] == "UNAVAILABLE"
    assert missing["value"] is None
    assert missing["synchronized"] is False
    assert "usd_tom" in missing["unavailable_reason"]

    front_perp = _metric(context, "usd_rub", "front_perpetual_basis_abs")
    assert front_perp["status"] == "READY"
    assert front_perp["value"] == pytest.approx(1.0)
    assert front_perp["units"] == "RUB_per_USD"
    assert front_perp["source_timestamps"] == {"si_front": TS, "usdrubf": TS}
    assert front_perp["max_accepted_skew_seconds"] == 60
    assert front_perp["synchronized"] is True

    front_next = _metric(context, "usd_rub", "front_next_spread_bps")
    assert front_next["value"] == pytest.approx((93.0 / 92.0 - 1.0) * 10000.0)
    term = _metric(context, "usd_rub", "front_next_term_carry_annualized")
    assert term["status"] == "READY"
    assert term["value"] == pytest.approx((93.0 / 92.0 - 1.0) * 365.0 / 91.0)
    assert term["expiry_metadata"]["calendar_days_between_expiries"] == 91
    assert term["expiry_metadata"]["expiry_day_contract_allowed"] is False

    assert context["stage4_semantics"]["contract_ref"] == basis.STAGE4_CONTRACT_REF
    assert context["stage4_semantics"]["annualization_basis_days"] == 365
    assert context["synchronization_policy"]["forward_fill_allowed"] is False
    assert context["synchronization_policy"]["asof_join_allowed"] is False
    assert context["synchronization_policy"]["nearest_join_allowed"] is False
    assert context["synchronization_policy"]["calendar_inference_allowed"] is False
    assert context["directional_authority"] is False
    assert context["action_authority"] is False
    assert context["standalone_buy_sell_authority"] is False
    assert context["stage5_full_mode_ready"] is False
    assert context["stage5_pointer_promotion_performed"] is False


def test_cny_live_metrics_match_accepted_stage4_materializer_semantics() -> None:
    context = basis.build_context(_live_snapshot())
    live_metrics = {
        item["stage4_metric_id"]: item
        for item in context["pairs"]["cny_rub"]["metrics"]
    }
    row = {
        "trade_date": "2026-09-04",
        "ts": TS,
    }

    def frame(instrument_id: str, close: float) -> pd.DataFrame:
        return pd.DataFrame([{**row, "instrument_id": instrument_id, "close": close}])

    accepted = build_basis_carry_frame(
        instrument_id="cny_rub_basis_carry",
        trade_date="2026-09-04",
        spot_frame=frame("cny_tom", 12.0),
        perpetual_frame=frame("cnyrubf_futures_family", 12.1),
        front_frame=frame("cr_front_contract", 12.2),
        next_frame=frame("cr_next_contract", 12.3),
        front_binding={
            "root": "CR",
            "role": "front",
            "instrument_id": "cr_front_contract",
            "as_of_date": "2026-09-04",
            "secid": "CRU6",
            "last_trade_date": "2026-09-17",
        },
        next_binding={
            "root": "CR",
            "role": "next",
            "instrument_id": "cr_next_contract",
            "as_of_date": "2026-09-04",
            "secid": "CRZ6",
            "last_trade_date": "2026-12-17",
        },
        build_ts="2026-09-04T07:01:00+00:00",
    ).iloc[0]

    expected_ids = (
        "perpetual_spot_basis_abs",
        "perpetual_spot_basis_bps",
        "front_spot_basis_abs",
        "front_spot_basis_bps",
        "next_spot_basis_abs",
        "next_spot_basis_bps",
        "front_perpetual_basis_abs",
        "front_perpetual_basis_bps",
        "next_perpetual_basis_abs",
        "next_perpetual_basis_bps",
        "front_next_spread_abs",
        "front_next_spread_bps",
        "front_spot_implied_carry_annualized",
        "next_spot_implied_carry_annualized",
        "front_next_term_carry_annualized",
    )
    assert set(live_metrics) == set(expected_ids)
    for metric_id in expected_ids:
        assert live_metrics[metric_id]["status"] == "READY"
        assert live_metrics[metric_id]["value"] == pytest.approx(float(accepted[metric_id]))


def test_stale_fresh_mix_and_timestamp_skew_fail_closed_per_metric() -> None:
    stale_snapshot = _live_snapshot()
    stale_snapshot["instruments"]["cnyrub_tom"]["stale"] = True
    stale_snapshot["instruments"]["cnyrub_tom"]["age_seconds"] = 61.0
    stale_context = basis.build_context(stale_snapshot)
    spot_metric = _metric(stale_context, "cny_rub", "perpetual_spot_basis_abs")
    futures_only = _metric(stale_context, "cny_rub", "front_perpetual_basis_abs")
    assert spot_metric["status"] == "UNAVAILABLE"
    assert spot_metric["value"] is None
    assert "source_leg_stale" in spot_metric["unavailable_reason"]
    assert futures_only["status"] == "READY"

    skewed_snapshot = _live_snapshot()
    skewed_snapshot["instruments"]["cnyrub_tom"]["timestamp"] = "2026-09-04T07:02:00+00:00"
    skewed_context = basis.build_context(skewed_snapshot)
    skewed = _metric(skewed_context, "cny_rub", "front_spot_basis_abs")
    assert skewed["status"] == "UNAVAILABLE"
    assert skewed["value"] is None
    assert skewed["synchronized"] is False
    assert skewed["max_leg_skew_seconds"] == 120.0
    assert skewed["unavailable_reason"] == "source_timestamp_skew_exceeds_threshold"


def test_expiry_day_contract_is_not_annualized_and_trade_date_comes_from_source_timestamp() -> None:
    snapshot = _live_snapshot()
    snapshot["instruments"]["cr_front"]["expiry_date"] = "2026-09-04"
    context = basis.build_context(snapshot)
    front_carry = _metric(context, "cny_rub", "front_spot_implied_carry_annualized")
    term_carry = _metric(context, "cny_rub", "front_next_term_carry_annualized")
    assert front_carry["status"] == "UNAVAILABLE"
    assert front_carry["value"] is None
    assert front_carry["unavailable_reason"] == "front_contract_not_strictly_after_source_trade_date"
    assert front_carry["expiry_metadata"]["source_trade_date"] == "2026-09-04"
    assert front_carry["expiry_metadata"]["source_trade_date_semantics"] == (
        "observed_source_event_timestamp_Europe/Moscow"
    )
    assert term_carry["status"] == "UNAVAILABLE"
    assert term_carry["unavailable_reason"] == "front_contract_not_strictly_after_source_trade_date"


def test_live_basis_context_is_deterministic_json_for_same_input() -> None:
    snapshot = _live_snapshot()
    first = basis.build_context(snapshot)
    second = basis.build_context(copy.deepcopy(snapshot))
    first_json = json.dumps(first, sort_keys=True, separators=(",", ":"), allow_nan=False)
    second_json = json.dumps(second, sort_keys=True, separators=(",", ":"), allow_nan=False)
    assert first_json == second_json
    assert datetime.fromisoformat(first["data_as_of"]).tzinfo == timezone.utc

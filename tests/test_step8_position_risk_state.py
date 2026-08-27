from __future__ import annotations

import copy

import pytest

from moex_data.step8_position_risk_state import Step8PositionRiskError, build_position_risk_state


def _payload() -> dict[str, object]:
    return {
        "schema_version": "step8_position_risk_input.v1",
        "snapshot_id": "risk_20260827_v1",
        "as_of_ts_utc": "2026-08-27T10:00:00Z",
        "source": {"mode": "manual", "reference": "manual_snapshot_20260827"},
        "account": {
            "currency": "RUB",
            "free_funds_rub": "1000000",
            "current_initial_margin_rub": "250000",
            "variation_margin_rub": "15000",
            "liquidity_buffer_rub": "500000",
            "max_total_contracts": 100,
            "max_allowed_loss_rub": "200000",
        },
        "positions": [
            {
                "position_id": "usd_rub_main",
                "instrument_id": "usdrubf_futures_family",
                "expiry": None,
                "expiry_not_applicable_reason": "perpetual_family",
                "contracts": 30,
                "average_price": "84.25",
                "fills": [
                    {
                        "fill_id": "fill_1",
                        "ts_utc": "2026-08-27T09:00:00+00:00",
                        "contracts": 30,
                        "price": "84.25",
                        "commission_rub": "120.50",
                    }
                ],
                "commission_total_rub": "120.50",
                "realized_pnl_rub": "5000",
                "unrealized_pnl_rub": "-2500",
                "horizon": "swing_1_4_weeks",
                "invalidation": {"level": "81.00", "loss_rub": "97500"},
                "protective_stop": {"level": "80.90"},
                "tranches": [
                    {"level": "83.50", "contracts_delta": 10},
                    {"level": "82.80", "contracts_delta": 15},
                ],
            },
            {
                "position_id": "cny_rub_hedge",
                "instrument_id": "cnyrubf_futures_family",
                "expiry": None,
                "expiry_not_applicable_reason": "perpetual_family",
                "contracts": -5,
                "average_price": "11.75",
                "fills": [],
                "commission_total_rub": "0",
                "realized_pnl_rub": "0",
                "unrealized_pnl_rub": "1000",
                "horizon": "hedge",
                "invalidation": {"level": "12.20", "loss_rub": "10000"},
                "protective_stop": None,
                "tranches": [],
            },
        ],
        "scenario_pnl_rub": {
            "usd_rub_minus_5": "-180000",
            "usd_rub_minus_3": "-110000",
            "usd_rub_minus_1": "-35000",
            "usd_rub_plus_1": "30000",
            "usd_rub_plus_3": "95000",
            "usd_rub_plus_5": "165000",
            "gap": {"usd_rub_move": "-7", "pnl_rub": "-240000"},
        },
    }


def test_builds_exact_arithmetic_risk_aggregates_without_recomputing_pnl() -> None:
    result = build_position_risk_state(_payload())
    assert result["schema_version"] == "step8_position_risk_state.v1"
    assert result["derived"] == {
        "current_gross_contracts": 35,
        "current_contract_headroom": 65,
        "current_contract_limit_breach": False,
        "planned_conservative_additional_gross_contracts": 25,
        "planned_conservative_gross_contracts": 60,
        "planned_conservative_contract_headroom": 40,
        "planned_conservative_contract_limit_breach": False,
        "total_invalidation_loss_rub": "107500",
        "invalidation_loss_headroom_rub": "92500",
        "invalidation_loss_limit_breach": False,
        "worst_supplied_scenario_pnl_rub": "-240000",
        "worst_supplied_scenario_loss_rub": "240000",
        "scenario_loss_headroom_rub": "-40000",
        "scenario_loss_limit_breach": True,
        "liquidity_buffer_breach": False,
        "total_commission_rub": "120.5",
        "total_realized_pnl_rub": "5000",
        "total_unrealized_pnl_rub": "-1500",
    }
    policy = result["calculation_policy"]
    assert policy["broker_write_access_used"] is False
    assert policy["automatic_position_sizing_allowed"] is False
    assert policy["trade_recommendation_generated"] is False
    assert policy["realized_pnl_recomputed"] is False
    assert policy["unrealized_pnl_recomputed"] is False
    assert policy["invalidation_loss_recomputed_from_price"] is False
    assert policy["scenario_pnl_recomputed_from_market_move"] is False
    assert policy["supplied_pnl_fields_are_external_evidence"] is True


def test_contract_limit_breach_is_derived_not_normalized() -> None:
    payload = _payload()
    payload["account"]["max_total_contracts"] = 50
    result = build_position_risk_state(payload)
    assert result["derived"]["current_contract_limit_breach"] is False
    assert result["derived"]["planned_conservative_gross_contracts"] == 60
    assert result["derived"]["planned_conservative_contract_headroom"] == -10
    assert result["derived"]["planned_conservative_contract_limit_breach"] is True


def test_rejects_unknown_input_key_fail_closed() -> None:
    payload = _payload()
    payload["account"]["invented_margin_threshold"] = 123
    with pytest.raises(Step8PositionRiskError, match="unknown keys"):
        build_position_risk_state(payload)


def test_rejects_incomplete_scenario_grid() -> None:
    payload = _payload()
    del payload["scenario_pnl_rub"]["usd_rub_plus_5"]
    with pytest.raises(Step8PositionRiskError, match="missing keys"):
        build_position_risk_state(payload)


def test_rejects_zero_position_contracts() -> None:
    payload = _payload()
    payload["positions"][0]["contracts"] = 0
    with pytest.raises(Step8PositionRiskError, match="must be non-zero"):
        build_position_risk_state(payload)


def test_rejects_non_utc_snapshot_timestamp() -> None:
    payload = _payload()
    payload["as_of_ts_utc"] = "2026-08-27T13:00:00+03:00"
    with pytest.raises(Step8PositionRiskError, match="must be UTC"):
        build_position_risk_state(payload)


def test_null_expiry_requires_explicit_reason() -> None:
    payload = _payload()
    del payload["positions"][0]["expiry_not_applicable_reason"]
    with pytest.raises(Step8PositionRiskError, match="expiry_not_applicable_reason"):
        build_position_risk_state(payload)


def test_duplicate_position_identity_rejected() -> None:
    payload = _payload()
    duplicate = copy.deepcopy(payload["positions"][0])
    duplicate["position_id"] = "another_id"
    payload["positions"].append(duplicate)
    with pytest.raises(Step8PositionRiskError, match="duplicate instrument_id/expiry"):
        build_position_risk_state(payload)

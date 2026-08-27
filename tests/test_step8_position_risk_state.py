from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest

from moex_data.step8_position_risk_state import Step8PositionRiskError, build_position_risk_state


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs" / "datasets" / "step8_position_risk_state.v1.yaml"
CONTRACT = ROOT / "contracts" / "datasets" / "position_risk_state.v1.yaml"


def _payload() -> dict[str, object]:
    return {
        "schema_version": "step8_position_risk_input.v1",
        "snapshot_id": "risk_20260827_v1",
        "as_of_ts_utc": "2026-08-27T10:00:00.123456Z",
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
                        "ts_utc": "2026-08-27T09:00:00.000123+00:00",
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
                "fills": [
                    {
                        "fill_id": "fill_2",
                        "ts_utc": "2026-08-27T09:30:00+00:00",
                        "contracts": -5,
                        "price": "11.75",
                        "commission_rub": "0",
                    }
                ],
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


def _parse_scalar(raw: str) -> object:
    value = raw.strip()
    if value == "true":
        return True
    if value == "false":
        return False
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if value.startswith('"') and value.endswith('"'):
        return json.loads(value)
    return value


def _parse_scalar_mapping(text: str, header: str, child_indent: int) -> dict[str, object]:
    lines = text.splitlines()
    try:
        start = lines.index(header) + 1
    except ValueError as exc:
        raise AssertionError(f"missing mapping header: {header}") from exc
    prefix = " " * child_indent
    values: dict[str, object] = {}
    for line in lines[start:]:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent < child_indent:
            break
        if indent != child_indent or not line.startswith(prefix) or ":" not in line:
            raise AssertionError(f"unexpected nested/non-scalar YAML under {header}: {line}")
        key, raw = line.strip().split(":", 1)
        values[key] = _parse_scalar(raw)
    return values


def _parse_string_list(text: str, header: str, child_indent: int) -> list[str]:
    lines = text.splitlines()
    try:
        start = lines.index(header) + 1
    except ValueError as exc:
        raise AssertionError(f"missing list header: {header}") from exc
    prefix = " " * child_indent + "- "
    values: list[str] = []
    for line in lines[start:]:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent < child_indent:
            break
        if indent != child_indent or not line.startswith(prefix):
            raise AssertionError(f"unexpected nested/non-list YAML under {header}: {line}")
        values.append(line[len(prefix):].strip())
    return values


def test_builds_exact_arithmetic_risk_aggregates_without_recomputing_pnl() -> None:
    result = build_position_risk_state(_payload())
    assert result["schema_version"] == "step8_position_risk_state.v1"
    assert result["as_of_ts_utc"] == "2026-08-27T10:00:00.123456+00:00"
    assert result["positions"][0]["fills"][0]["ts_utc"] == "2026-08-27T09:00:00.000123+00:00"
    assert result["positions"][0]["fill_contract_sum"] == 30
    assert result["positions"][0]["fill_commission_sum_rub"] == "120.5"
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
        "best_supplied_scenario_pnl_rub": "165000",
        "worst_supplied_scenario_loss_rub": "240000",
        "scenario_loss_headroom_rub": "-40000",
        "scenario_loss_limit_breach": True,
        "liquidity_buffer_breach": False,
        "total_commission_rub": "120.5",
        "total_realized_pnl_rub": "5000",
        "total_unrealized_pnl_rub": "-1500",
    }
    assert result["calculation_policy"] == {
        "broker_write_access_used": False,
        "automatic_order_placement_allowed": False,
        "automatic_position_sizing_allowed": False,
        "trade_recommendation_generated": False,
        "stop_or_invalidation_generation_allowed": False,
        "tranche_generation_allowed": False,
        "realized_pnl_recomputed": False,
        "unrealized_pnl_recomputed": False,
        "invalidation_loss_recomputed_from_price": False,
        "scenario_pnl_recomputed_from_market_move": False,
        "supplied_pnl_fields_are_external_evidence": True,
        "instrument_payout_mapping_required_before_pnl_recalculation": True,
    }


def test_contract_limit_breach_is_derived_not_normalized() -> None:
    payload = _payload()
    payload["account"]["max_total_contracts"] = 50
    result = build_position_risk_state(payload)
    assert result["derived"]["current_contract_limit_breach"] is False
    assert result["derived"]["planned_conservative_gross_contracts"] == 60
    assert result["derived"]["planned_conservative_contract_headroom"] == -10
    assert result["derived"]["planned_conservative_contract_limit_breach"] is True


def test_max_loss_breach_is_derived_from_supplied_loss_only() -> None:
    payload = _payload()
    payload["account"]["max_allowed_loss_rub"] = "100000"
    result = build_position_risk_state(payload)
    assert result["derived"]["total_invalidation_loss_rub"] == "107500"
    assert result["derived"]["invalidation_loss_headroom_rub"] == "-7500"
    assert result["derived"]["invalidation_loss_limit_breach"] is True


def test_negative_tranche_delta_is_counted_conservatively_by_absolute_value() -> None:
    payload = _payload()
    payload["positions"][0]["tranches"].append({"level": "86.00", "contracts_delta": -7})
    result = build_position_risk_state(payload)
    assert result["derived"]["planned_conservative_additional_gross_contracts"] == 32
    assert result["derived"]["planned_conservative_gross_contracts"] == 67


def test_supplied_pnl_and_invalidation_loss_are_not_recomputed_from_prices() -> None:
    payload = _payload()
    payload["positions"][0]["average_price"] = "1"
    payload["positions"][0]["fills"][0]["price"] = "1"
    payload["positions"][0]["invalidation"] = {"level": "999", "loss_rub": "123"}
    payload["positions"][0]["realized_pnl_rub"] = "777"
    payload["positions"][0]["unrealized_pnl_rub"] = "-888"
    payload["scenario_pnl_rub"]["usd_rub_minus_5"] = "42"
    result = build_position_risk_state(payload)
    assert result["positions"][0]["invalidation"]["loss_rub"] == "123"
    assert result["positions"][0]["realized_pnl_rub"] == "777"
    assert result["positions"][0]["unrealized_pnl_rub"] == "-888"
    assert result["scenario_pnl_rub"]["usd_rub_minus_5"] == "42"
    assert result["calculation_policy"]["realized_pnl_recomputed"] is False
    assert result["calculation_policy"]["unrealized_pnl_recomputed"] is False
    assert result["calculation_policy"]["invalidation_loss_recomputed_from_price"] is False
    assert result["calculation_policy"]["scenario_pnl_recomputed_from_market_move"] is False


def test_empty_positions_and_zero_limits_are_valid_exact_boundary_state() -> None:
    payload = _payload()
    payload["positions"] = []
    payload["account"]["max_total_contracts"] = 0
    payload["account"]["max_allowed_loss_rub"] = "0"
    for key in (
        "usd_rub_minus_5",
        "usd_rub_minus_3",
        "usd_rub_minus_1",
        "usd_rub_plus_1",
        "usd_rub_plus_3",
        "usd_rub_plus_5",
    ):
        payload["scenario_pnl_rub"][key] = "0"
    payload["scenario_pnl_rub"]["gap"] = {"usd_rub_move": "-1", "pnl_rub": "0"}
    result = build_position_risk_state(payload)
    assert result["positions"] == []
    assert result["derived"]["current_gross_contracts"] == 0
    assert result["derived"]["current_contract_headroom"] == 0
    assert result["derived"]["current_contract_limit_breach"] is False
    assert result["derived"]["total_invalidation_loss_rub"] == "0"
    assert result["derived"]["invalidation_loss_limit_breach"] is False
    assert result["derived"]["scenario_loss_limit_breach"] is False


def test_negative_free_funds_variation_margin_and_liquidity_buffer_are_preserved() -> None:
    payload = _payload()
    payload["account"]["free_funds_rub"] = "-10"
    payload["account"]["variation_margin_rub"] = "-20"
    payload["account"]["liquidity_buffer_rub"] = "-30"
    result = build_position_risk_state(payload)
    assert result["account"]["free_funds_rub"] == "-10"
    assert result["account"]["variation_margin_rub"] == "-20"
    assert result["account"]["liquidity_buffer_rub"] == "-30"
    assert result["derived"]["liquidity_buffer_breach"] is True


def test_negative_initial_margin_rejected() -> None:
    payload = _payload()
    payload["account"]["current_initial_margin_rub"] = "-1"
    with pytest.raises(Step8PositionRiskError, match="below minimum"):
        build_position_risk_state(payload)


def test_rejects_unknown_input_key_fail_closed() -> None:
    payload = _payload()
    payload["account"]["invented_margin_threshold"] = 123
    with pytest.raises(Step8PositionRiskError, match="unknown keys"):
        build_position_risk_state(payload)


def test_rejects_non_string_text_instead_of_coercing_it() -> None:
    payload = _payload()
    payload["snapshot_id"] = 123
    with pytest.raises(Step8PositionRiskError, match="must be string"):
        build_position_risk_state(payload)


def test_rejects_unsafe_identifier_token() -> None:
    payload = _payload()
    payload["positions"][0]["position_id"] = "../unsafe"
    with pytest.raises(Step8PositionRiskError, match="safe token"):
        build_position_risk_state(payload)


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), "-Infinity"])
def test_rejects_nan_and_inf_fail_closed(bad_value: object) -> None:
    payload = _payload()
    payload["account"]["free_funds_rub"] = bad_value
    with pytest.raises(Step8PositionRiskError, match="finite decimal"):
        build_position_risk_state(payload)


def test_rejects_unsupported_source_mode() -> None:
    payload = _payload()
    payload["source"]["mode"] = "broker_write"
    with pytest.raises(Step8PositionRiskError, match="source.mode unsupported"):
        build_position_risk_state(payload)


def test_rejects_incomplete_scenario_grid() -> None:
    payload = _payload()
    del payload["scenario_pnl_rub"]["usd_rub_plus_5"]
    with pytest.raises(Step8PositionRiskError, match="missing keys"):
        build_position_risk_state(payload)


def test_rejects_extra_scenario_key() -> None:
    payload = _payload()
    payload["scenario_pnl_rub"]["usd_rub_plus_10"] = "1"
    with pytest.raises(Step8PositionRiskError, match="unknown keys"):
        build_position_risk_state(payload)


def test_rejects_zero_position_contracts() -> None:
    payload = _payload()
    payload["positions"][0]["contracts"] = 0
    with pytest.raises(Step8PositionRiskError, match="must be non-zero"):
        build_position_risk_state(payload)


def test_rejects_zero_fill_contracts() -> None:
    payload = _payload()
    payload["positions"][0]["fills"][0]["contracts"] = 0
    with pytest.raises(Step8PositionRiskError, match="must be non-zero"):
        build_position_risk_state(payload)


def test_rejects_zero_tranche_contract_delta() -> None:
    payload = _payload()
    payload["positions"][0]["tranches"][0]["contracts_delta"] = 0
    with pytest.raises(Step8PositionRiskError, match="must be non-zero"):
        build_position_risk_state(payload)


def test_rejects_non_utc_snapshot_timestamp() -> None:
    payload = _payload()
    payload["as_of_ts_utc"] = "2026-08-27T13:00:00+03:00"
    with pytest.raises(Step8PositionRiskError, match="must be UTC"):
        build_position_risk_state(payload)


def test_rejects_non_utc_fill_timestamp() -> None:
    payload = _payload()
    payload["positions"][0]["fills"][0]["ts_utc"] = "2026-08-27T12:00:00+03:00"
    with pytest.raises(Step8PositionRiskError, match="must be UTC"):
        build_position_risk_state(payload)


def test_rejects_invalid_timestamp() -> None:
    payload = _payload()
    payload["as_of_ts_utc"] = "not-a-timestamp"
    with pytest.raises(Step8PositionRiskError, match="ISO-8601 UTC timestamp"):
        build_position_risk_state(payload)


def test_rejects_invalid_expiry_date() -> None:
    payload = _payload()
    payload["positions"][0]["expiry"] = "2026-02-30"
    payload["positions"][0].pop("expiry_not_applicable_reason")
    with pytest.raises(Step8PositionRiskError, match="YYYY-MM-DD"):
        build_position_risk_state(payload)


def test_null_expiry_requires_explicit_reason() -> None:
    payload = _payload()
    del payload["positions"][0]["expiry_not_applicable_reason"]
    with pytest.raises(Step8PositionRiskError, match="expiry_not_applicable_reason"):
        build_position_risk_state(payload)


def test_duplicate_position_id_rejected() -> None:
    payload = _payload()
    duplicate = copy.deepcopy(payload["positions"][0])
    duplicate["instrument_id"] = "different_instrument"
    payload["positions"].append(duplicate)
    with pytest.raises(Step8PositionRiskError, match="duplicate position_id"):
        build_position_risk_state(payload)


def test_duplicate_position_identity_rejected() -> None:
    payload = _payload()
    duplicate = copy.deepcopy(payload["positions"][0])
    duplicate["position_id"] = "another_id"
    payload["positions"].append(duplicate)
    with pytest.raises(Step8PositionRiskError, match="duplicate instrument_id/expiry"):
        build_position_risk_state(payload)


def test_duplicate_fill_id_within_position_rejected() -> None:
    payload = _payload()
    duplicate = copy.deepcopy(payload["positions"][0]["fills"][0])
    duplicate["contracts"] = 1
    payload["positions"][0]["fills"].append(duplicate)
    payload["positions"][0]["contracts"] = 31
    payload["positions"][0]["commission_total_rub"] = "241"
    with pytest.raises(Step8PositionRiskError, match="duplicate fill_id"):
        build_position_risk_state(payload)


def test_fill_contract_sum_must_reconcile_to_position_contracts() -> None:
    payload = _payload()
    payload["positions"][0]["fills"][0]["contracts"] = 29
    with pytest.raises(Step8PositionRiskError, match="contracts do not reconcile"):
        build_position_risk_state(payload)


def test_fill_commission_sum_must_reconcile_to_commission_total() -> None:
    payload = _payload()
    payload["positions"][0]["commission_total_rub"] = "120.51"
    with pytest.raises(Step8PositionRiskError, match="commissions do not reconcile"):
        build_position_risk_state(payload)


def test_output_preserves_supplied_list_order_and_is_repeatable() -> None:
    payload = _payload()
    payload["positions"].reverse()
    first = build_position_risk_state(payload)
    second = build_position_risk_state(copy.deepcopy(payload))
    assert first == second
    assert [position["position_id"] for position in first["positions"]] == [
        "cny_rub_hedge",
        "usd_rub_main",
    ]
    assert [item["contracts_delta"] for item in first["positions"][1]["tranches"]] == [10, 15]


def test_config_readiness_flags_are_exact_typed_values() -> None:
    readiness = _parse_scalar_mapping(CONFIG.read_text(encoding="utf-8"), "readiness_flags:", 2)
    assert readiness == {
        "implementation_ready": True,
        "deterministic_validation_ready": True,
        "price_based_pnl_recalculation_ready": False,
        "broker_ingest_ready": False,
        "accepted_pointer_ready": False,
        "daily_weekly_bundle_ready": False,
        "scheduler_ready": False,
        "research_ready": False,
    }


def test_contract_safety_and_provenance_are_exact_typed_values() -> None:
    text = CONTRACT.read_text(encoding="utf-8")
    safety = _parse_scalar_mapping(text, "safety_policy:", 2)
    assert safety == {
        "broker_write_access_used": False,
        "automatic_order_placement_allowed": False,
        "automatic_position_sizing_allowed": False,
        "trade_recommendation_generated": False,
        "stop_or_invalidation_generation_allowed": False,
        "tranche_generation_allowed": False,
    }
    provenance = _parse_scalar_mapping(text, "pnl_provenance_policy:", 2)
    assert provenance == {
        "realized_pnl_recomputed": False,
        "unrealized_pnl_recomputed": False,
        "invalidation_loss_recomputed_from_price": False,
        "scenario_pnl_recomputed_from_market_move": False,
        "supplied_pnl_fields_are_external_evidence": True,
        "instrument_payout_mapping_required_before_pnl_recalculation": True,
    }


def test_contract_exact_derived_field_list_includes_best_and_no_unapproved_math() -> None:
    fields = _parse_string_list(
        CONTRACT.read_text(encoding="utf-8"),
        "  exact_derived_fields:",
        4,
    )
    assert fields == [
        "current_gross_contracts",
        "current_contract_headroom",
        "current_contract_limit_breach",
        "planned_conservative_additional_gross_contracts",
        "planned_conservative_gross_contracts",
        "planned_conservative_contract_headroom",
        "planned_conservative_contract_limit_breach",
        "total_invalidation_loss_rub",
        "invalidation_loss_headroom_rub",
        "invalidation_loss_limit_breach",
        "worst_supplied_scenario_pnl_rub",
        "best_supplied_scenario_pnl_rub",
        "worst_supplied_scenario_loss_rub",
        "scenario_loss_headroom_rub",
        "scenario_loss_limit_breach",
        "liquidity_buffer_breach",
        "total_commission_rub",
        "total_realized_pnl_rub",
        "total_unrealized_pnl_rub",
    ]

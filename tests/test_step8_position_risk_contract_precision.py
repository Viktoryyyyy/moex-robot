from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs" / "datasets" / "step8_position_risk_state.v1.yaml"
CONTRACT = ROOT / "contracts" / "datasets" / "position_risk_state.v1.yaml"


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


def test_config_validation_scope_is_exact_typed_policy() -> None:
    validation = _parse_scalar_mapping(
        CONFIG.read_text(encoding="utf-8"),
        "validation_scope:",
        2,
    )
    assert validation == {
        "strict_string_types": True,
        "safe_identifier_tokens": True,
        "utc_timestamps_only": True,
        "preserve_fractional_timestamp_precision": True,
        "finite_numeric_values_only": True,
        "binary_float_coercion_forbidden": True,
        "json_decimal_tokens_parsed_as_decimal": True,
        "duplicate_json_object_members_rejected": True,
        "supplied_decimal_precision_preserved": True,
        "complete_fill_contract_reconciliation": True,
        "exact_fill_commission_reconciliation": True,
        "duplicate_ids_fail_closed": True,
        "exact_scenario_keys_fail_closed": True,
    }


def test_contract_precision_and_timestamp_policies_are_exact_typed_values() -> None:
    text = CONTRACT.read_text(encoding="utf-8")
    timestamp = _parse_scalar_mapping(text, "  timestamp_policy:", 4)
    assert timestamp == {
        "utc_only": True,
        "fractional_seconds_preserved": True,
        "output_offset": "+00:00",
    }
    precision = _parse_scalar_mapping(text, "  numeric_precision_policy:", 4)
    assert precision == {
        "binary_float_direct_inputs_rejected": True,
        "json_decimal_tokens_parsed_as_decimal": True,
        "supplied_decimal_precision_preserved": True,
        "exact_decimal_aggregation_without_default_context_rounding": True,
    }


def test_contract_fail_closed_policy_is_exact_typed_values() -> None:
    fail_closed = _parse_scalar_mapping(
        CONTRACT.read_text(encoding="utf-8"),
        "fail_closed_policy:",
        2,
    )
    assert fail_closed == {
        "unknown_input_keys_rejected": True,
        "duplicate_json_object_members_rejected": True,
        "non_string_text_fields_rejected": True,
        "unsafe_identifier_tokens_rejected": True,
        "nonfinite_numeric_values_rejected": True,
        "binary_float_coercion_rejected": True,
        "unsupported_source_modes_rejected": True,
        "duplicate_position_ids_rejected": True,
        "duplicate_instrument_expiry_positions_rejected": True,
        "duplicate_fill_ids_within_position_rejected": True,
        "zero_position_contracts_rejected": True,
        "zero_fill_contracts_rejected": True,
        "zero_tranche_contract_delta_rejected": True,
        "non_utc_snapshot_and_fill_timestamps_rejected": True,
        "invalid_expiry_dates_rejected": True,
        "incomplete_or_extra_scenario_keys_rejected": True,
        "fill_contract_mismatch_rejected": True,
        "fill_commission_mismatch_rejected": True,
    }

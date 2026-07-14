from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

APPROVED_PHASE2_SOURCE_REFS: tuple[str, ...] = (
    "contracts/sources/internal/usdrubf_d1_ohlc_from_5m.v1.yaml",
    "contracts/sources/internal/usdrubf_d1_ema_3_19_cross_context.v1.yaml",
    "contracts/sources/internal/usdrubf_d1_classical_indicators.v1.yaml",
    "contracts/sources/futures/roll_expiry_mapping.v1.yaml",
    "contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml",
    "contracts/calendars/calendar/ru_tax_periods.v1.yaml",
    "contracts/calendars/calendar/ru_us_holidays.v1.yaml",
)

BLOCKED_PROVIDER_REFS: tuple[str, ...] = (
    "contracts/sources/futoi/participant_positioning.v1.yaml",
    "contracts/sources/oil/**",
    "contracts/sources/dollar_index/**",
    "contracts/sources/currency/**",
    "news_events.raw_ingestion",
    "news_events.llm_classification",
)

READINESS_CAPABILITIES: tuple[str, ...] = (
    "ingestion",
    "runtime",
    "loader",
    "materialization",
    "feature_computation",
    "modeling",
    "prediction",
)

DATASET_READINESS_FALSE_FLAGS: tuple[str, ...] = (
    "ingestion_ready",
    "runtime_ready",
    "loader_ready",
    "materialization_ready",
    "feature_computation_ready",
    "modeling_ready",
)

REGISTRY_ENABLEMENT_FALSE_FLAGS: tuple[str, ...] = (
    "enabled_for_loading",
    "enabled_for_update",
    "enabled_for_retrieval",
    "enabled_for_raw_5m_materialization",
    "enabled_for_d1_derivation",
    "enabled_for_research",
)

EXECUTION_AUTHORIZATION_TRUE_KEYS: tuple[str, ...] = tuple(
    key
    for capability in READINESS_CAPABILITIES
    for key in (
        capability,
        f"{capability}_ready",
        f"{capability}_allowed",
        f"{capability}_authorized",
        f"can_run_{capability}",
    )
) + (
    "can_compute_features",
    "can_model",
    "runtime_loader_authorized",
)

REQUIRED_REPOSITORY_FILES: tuple[str, ...] = (
    "configs/instruments/forts_instrument_registry.v1.yaml",
    "configs/datasets/futures_data_lake.v1.yaml",
    "contracts/sources/internal/usdrubf_d1_ohlc_from_5m.v1.yaml",
    "contracts/sources/internal/usdrubf_d1_ema_3_19_cross_context.v1.yaml",
    "contracts/sources/internal/usdrubf_d1_classical_indicators.v1.yaml",
    "contracts/sources/futures/roll_expiry_mapping.v1.yaml",
    "contracts/calendars/rates/cbr_key_rate_calendar.v1.yaml",
    "contracts/calendars/calendar/ru_tax_periods.v1.yaml",
    "contracts/calendars/calendar/ru_us_holidays.v1.yaml",
    "contracts/features/usdrubf_phase2_d1_feature_export_v1.json",
    "contracts/validation/usdrubf_phase2_pit_availability_validation_v1.yaml",
    "tests/ema_3_19_ai/test_phase2_registry_dataset_config_contracts.py",
    "tests/ema_3_19_ai/test_phase2_pit_and_label_leakage_contracts.py",
)


def build_materialization_gate_report(repo_root: str | Path) -> dict[str, Any]:
    """Build a deterministic repository-file gate report for Phase 2.8.

    The function reads only checked-in repository config, contract, and test files
    under the supplied repo_root. It does not load market data, call external
    services, run commands, write files, compute features, or fit models.
    """

    root = Path(repo_root).resolve()
    texts = {path: _read_repo_file(root, path) for path in REQUIRED_REPOSITORY_FILES}

    dataset_text = texts["configs/datasets/futures_data_lake.v1.yaml"]
    registry_text = texts["configs/instruments/forts_instrument_registry.v1.yaml"]
    feature_contract = _read_json_contract(
        root, "contracts/features/usdrubf_phase2_d1_feature_export_v1.json"
    )
    pit_text = texts["contracts/validation/usdrubf_phase2_pit_availability_validation_v1.yaml"]
    phase2_6_test_text = texts[
        "tests/ema_3_19_ai/test_phase2_registry_dataset_config_contracts.py"
    ]
    phase2_7_pit_test_text = texts[
        "tests/ema_3_19_ai/test_phase2_pit_and_label_leakage_contracts.py"
    ]

    approved_refs = tuple(_extract_yaml_list(dataset_text, "approved_source_contract_refs"))
    blocked_refs = tuple(_extract_yaml_list(dataset_text, "blocked_source_refs"))

    validations = {
        "required_repository_files_exist": all(text.strip() for text in texts.values()),
        "approved_phase2_source_refs_match": approved_refs == APPROVED_PHASE2_SOURCE_REFS,
        "blocked_provider_refs_declared": all(ref in blocked_refs for ref in BLOCKED_PROVIDER_REFS),
        "phase2_6_readiness_flags_blocked": _dataset_readiness_flags_blocked(dataset_text),
        "instrument_registry_loading_flags_blocked": _registry_loading_flags_blocked(registry_text),
        "execution_authorization_true_values_absent": _execution_authorization_true_values_absent(
            registry_text, dataset_text
        ),
        "generated_data_path_not_authorized": "no generated data path" in dataset_text,
        "current_contract_month_selection_not_authorized": (
            "no current contract month selection automation" in registry_text
            and "latest_autodetect_allowed: false" in registry_text
            and "implicit_contract_selection_allowed: false" in registry_text
        ),
        "pit_availability_gate_declared": _contains_all(
            pit_text,
            (
                "availability_ts_utc",
                "forecast_anchor_ts",
                "availability_ts_utc must be <= forecast_anchor_ts",
                "D1 trade_date T must not be used before T+1 06:00 Europe/Moscow",
                "one trading day or excluded",
            ),
        ),
        "label_leakage_denylist_declared": _label_denylist_declared(feature_contract),
        "phase2_7_test_coverage_assumptions_present": _phase2_test_coverage_assumptions_present(
            phase2_6_test_text, phase2_7_pit_test_text
        ),
    }

    all_validations_passed = all(validations.values())

    return {
        "materialization_gate_status": "blocked_pending_data_build_approval",
        "gate_passed": all_validations_passed,
        "can_run_ingestion": False,
        "can_run_materialization": False,
        "can_compute_features": False,
        "can_model": False,
        "capability_authorization": {
            "ingestion": False,
            "runtime": False,
            "loader": False,
            "materialization": False,
            "feature_computation": False,
            "modeling": False,
            "prediction": False,
        },
        "blocked_readiness_status": {
            "ingestion": "not_ready",
            "runtime": "not_ready",
            "loader": "not_ready",
            "materialization": "not_ready",
            "feature_computation": "not_ready",
            "modeling": "blocked",
            "prediction": "blocked",
        },
        "approved_source_contract_refs": list(approved_refs),
        "blocked_provider_refs": {ref: {"allowed": False} for ref in blocked_refs},
        "required_pit_gates": {
            "availability_ts_utc": {"required": True},
            "forecast_anchor_ts": {"required": True},
            "rule": "availability_ts_utc <= forecast_anchor_ts",
            "unknown_availability_ts_utc_rule": "exclude_or_shift_by_at_least_one_trading_day",
        },
        "label_leakage_denylist_gate": {
            "represented": validations["label_leakage_denylist_declared"],
            "denied_groups": sorted(feature_contract.get("denylist", {}).keys()),
        },
        "generated_data_path_authorized": False,
        "current_contract_month_selection_automation_authorized": False,
        "server_apply_authorized": False,
        "runtime_authorized": False,
        "output_files_authorized": False,
        "repository_files_read": list(REQUIRED_REPOSITORY_FILES),
        "validation_checks": validations,
        "side_effects": {
            "market_data_loaded": False,
            "network_calls_performed": False,
            "server_commands_performed": False,
            "output_files_written": False,
            "feature_computation_performed": False,
            "model_fitting_performed": False,
        },
    }


def assert_materialization_gate_ready(repo_root: str | Path) -> dict[str, Any]:
    """Return the gate report or raise AssertionError on repository drift."""

    report = build_materialization_gate_report(repo_root)
    failed = [
        name for name, passed in report["validation_checks"].items() if not bool(passed)
    ]
    if failed:
        raise AssertionError(f"Phase 2.8 materialization gate failed: {failed}")
    return report


def _read_repo_file(repo_root: Path, relative_path: str) -> str:
    path = (repo_root / relative_path).resolve()
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise ValueError(f"Path escapes repo_root: {relative_path}") from exc
    return path.read_text(encoding="utf-8")


def _read_json_contract(repo_root: Path, relative_path: str) -> dict[str, Any]:
    payload = json.loads(_read_repo_file(repo_root, relative_path))
    if not isinstance(payload, dict):
        raise TypeError(f"JSON contract must be an object: {relative_path}")
    return payload


def _extract_yaml_list(text: str, key: str) -> list[str]:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.strip() != f"{key}:":
            continue
        base_indent = len(line) - len(line.lstrip(" "))
        values: list[str] = []
        for candidate in lines[index + 1 :]:
            if not candidate.strip():
                continue
            indent = len(candidate) - len(candidate.lstrip(" "))
            if indent <= base_indent:
                break
            stripped = candidate.strip()
            if stripped.startswith("- "):
                values.append(stripped[2:].strip().strip('"').strip("'"))
        return values
    raise KeyError(f"YAML list not found: {key}")


def _contains_all(text: str, markers: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return all(marker.lower() in lowered for marker in markers)


def _yaml_flag_false_and_not_true(text: str, flag: str) -> bool:
    return bool(
        re.search(rf"^\s*{re.escape(flag)}:\s*false\s*(?:#.*)?$", text, flags=re.MULTILINE)
    ) and not bool(
        re.search(rf"^\s*{re.escape(flag)}:\s*true\s*(?:#.*)?$", text, flags=re.MULTILINE)
    )


def _yaml_key_true_absent(text: str, key: str) -> bool:
    return not bool(
        re.search(rf"^\s*{re.escape(key)}:\s*true\s*(?:#.*)?$", text, flags=re.MULTILINE)
    )


def _dataset_readiness_flags_blocked(dataset_text: str) -> bool:
    return all(
        _yaml_flag_false_and_not_true(dataset_text, flag)
        for flag in DATASET_READINESS_FALSE_FLAGS
    )


def _registry_loading_flags_blocked(registry_text: str) -> bool:
    return all(
        _yaml_flag_false_and_not_true(registry_text, flag)
        for flag in REGISTRY_ENABLEMENT_FALSE_FLAGS
    )


def _execution_authorization_true_values_absent(*texts: str) -> bool:
    combined_text = "\n".join(texts)
    return all(
        _yaml_key_true_absent(combined_text, key)
        for key in EXECUTION_AUTHORIZATION_TRUE_KEYS
    )


def _phase2_test_coverage_assumptions_present(
    phase2_6_test_text: str, phase2_7_pit_test_text: str
) -> bool:
    return (
        all(ref in phase2_6_test_text for ref in APPROVED_PHASE2_SOURCE_REFS)
        and all(ref in phase2_6_test_text for ref in BLOCKED_PROVIDER_REFS)
        and all(flag in phase2_6_test_text for flag in DATASET_READINESS_FALSE_FLAGS)
        and all(flag in phase2_6_test_text for flag in REGISTRY_ENABLEMENT_FALSE_FLAGS)
        and _contains_all(
            phase2_6_test_text,
            (
                "test_registry_and_dataset_readiness_flags_do_not_authorize_execution",
                "test_no_generated_data_runtime_loader_or_current_contract_automation_is_authorized",
                "no runtime loader",
                "no materialization job",
                "no feature computation",
                "no model fitting",
                "no prediction",
            ),
        )
        and _contains_all(
            phase2_7_pit_test_text,
            (
                "test_pit_time_fields_and_d1_forecast_anchor_rules_are_declared",
                "test_label_leakage_denylist_blocks_labels_intervals_future_targets_and_annotations",
                "test_contracts_do_not_authorize_runtime_data_loading_feature_computation_or_modeling",
            ),
        )
    )


def _label_denylist_declared(feature_contract: dict[str, Any]) -> bool:
    denylist = feature_contract.get("denylist")
    if not isinstance(denylist, dict):
        return False
    flattened = json.dumps(denylist, sort_keys=True).lower()
    required_markers = (
        "phase_label",
        "phase_remaining_sessions",
        "current_regime_ends_within_*",
        "next_regime_if_current_ends",
        "interval_id",
        "interval_start",
        "interval_end",
        "annotation_*",
        "future_return_*",
        "future_drawdown_*",
        "future_volatility_*",
    )
    return all(marker in flattened for marker in required_markers)

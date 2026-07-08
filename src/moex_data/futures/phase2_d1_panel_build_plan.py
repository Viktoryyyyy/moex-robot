from __future__ import annotations

from pathlib import Path
from typing import Any

from .phase2_materialization_gate import build_materialization_gate_report

PLAN_STATUS = "blocked_pending_data_build_approval"
SOURCE_DATASET = "futures_raw_5m.v1"
TARGET_PANEL = "usdrubf_phase2_d1_panel.v1"
TARGET_GRAIN = "one row per trade_date per canonical instrument"
FORECAST_ANCHOR = "06:00 Europe/Moscow"
PIT_CUTOFF_RULE = "availability_ts_utc <= forecast_anchor_ts"
D1_AVAILABILITY_RULE = "D1 trade_date T available no earlier than T+1 06:00 Europe/Moscow"
LABEL_ROLE = "y_only_not_feature"
EMA_ROLE = "context_diagnostic_only_not_label_source"
CBR_OFFICIAL_USDRUB_ROLE = "reference_only_not_causal_market_input"
FUTOI_STATUS = "blocked_until_provider_timestamp_schema_revision_policy"

BLOCKED_EXTERNAL_SOURCES: tuple[str, ...] = (
    "FUTOI source contract",
    "oil",
    "DXY",
    "CNY/RUB proxy",
    "USD/RUB spot proxy",
    "news raw ingestion",
    "news LLM classification",
)

REQUIRED_GATES_BEFORE_DATA_BUILD: tuple[str, ...] = (
    "materialization_gate_passed",
    "explicit_PM_L2_data_build_approval",
    "server_apply_window_if_needed",
    "data_root_defined_by_config_or_runtime_context",
    "no_label_leakage_validation",
    "PIT availability validation",
    "output_schema_validation",
)

REQUIRED_REPOSITORY_FILES: tuple[str, ...] = (
    "src/moex_data/futures/phase2_materialization_gate.py",
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
    "contracts/features/usdrubf_phase2_unified_external_feature_contract_v1.json",
    "contracts/validation/usdrubf_phase2_pit_availability_validation_v1.yaml",
    "docs/sot/strategies/ema_3_19_ai/phase2_materialization_gate_v1.md",
)

AUTHORIZATION_FLAGS: dict[str, bool] = {
    "can_run_ingestion": False,
    "can_run_backfill": False,
    "can_run_materialization": False,
    "can_compute_features": False,
    "can_train_model": False,
    "can_predict": False,
}

SIDE_EFFECTS: dict[str, bool] = {
    "reads_market_data": False,
    "writes_files": False,
    "network_calls": False,
    "subprocess": False,
    "server_access": False,
}


def build_d1_panel_build_plan(repo_root: str | Path) -> dict[str, Any]:
    """Build a deterministic Phase 2.9 D1 panel build plan.

    The function reads only checked-in repository files under ``repo_root`` and
    returns a plain dictionary. It does not load market data, write files, call
    networks, spawn commands, access a server, compute features, train models,
    or produce predictions.
    """

    root = Path(repo_root).resolve()
    repository_texts = {
        relative_path: _read_repository_file(root, relative_path)
        for relative_path in REQUIRED_REPOSITORY_FILES
    }
    materialization_gate_report = build_materialization_gate_report(root)

    materialization_gate_passed = bool(materialization_gate_report.get("gate_passed"))
    required_files_present = all(bool(text.strip()) for text in repository_texts.values())

    return {
        "phase": "2.9",
        "plan_status": PLAN_STATUS,
        "source_dataset": SOURCE_DATASET,
        "target_panel": TARGET_PANEL,
        "target_grain": TARGET_GRAIN,
        "forecast_anchor": FORECAST_ANCHOR,
        "pit_cutoff_rule": PIT_CUTOFF_RULE,
        "d1_availability_rule": D1_AVAILABILITY_RULE,
        "label_role": LABEL_ROLE,
        "ema_role": EMA_ROLE,
        "cbr_official_usdrub_role": CBR_OFFICIAL_USDRUB_ROLE,
        "futoi_status": FUTOI_STATUS,
        "blocked_external_sources": list(BLOCKED_EXTERNAL_SOURCES),
        "required_gates_before_data_build": list(REQUIRED_GATES_BEFORE_DATA_BUILD),
        "authorization_flags": dict(AUTHORIZATION_FLAGS),
        "side_effects": dict(SIDE_EFFECTS),
        "generated_data_path_authorized": False,
        "server_path_authorized": False,
        "output_files_authorized": False,
        "authorized_output_paths": [],
        "repository_files_checked": list(REQUIRED_REPOSITORY_FILES),
        "repository_file_presence": {
            relative_path: bool(text.strip())
            for relative_path, text in repository_texts.items()
        },
        "materialization_gate": {
            "status": materialization_gate_report.get("materialization_gate_status"),
            "gate_passed": materialization_gate_passed,
            "server_apply_authorized": bool(
                materialization_gate_report.get("server_apply_authorized")
            ),
            "runtime_authorized": bool(materialization_gate_report.get("runtime_authorized")),
            "output_files_authorized": bool(
                materialization_gate_report.get("output_files_authorized")
            ),
        },
        "plan_checks": {
            "required_repository_files_present": required_files_present,
            "phase2_materialization_gate_passed": materialization_gate_passed,
            "explicit_pm_l2_data_build_approval_present": False,
            "data_build_authorized": False,
        },
    }


def assert_d1_panel_build_plan_blocked(repo_root: str | Path) -> dict[str, Any]:
    """Return the build plan or raise AssertionError on repository drift."""

    plan = build_d1_panel_build_plan(repo_root)
    failed_checks = [
        name
        for name, passed in plan["plan_checks"].items()
        if name != "explicit_pm_l2_data_build_approval_present"
        and name != "data_build_authorized"
        and not bool(passed)
    ]
    if failed_checks:
        raise AssertionError(f"Phase 2.9 D1 panel build plan failed checks: {failed_checks}")
    if plan["plan_status"] != PLAN_STATUS:
        raise AssertionError("Phase 2.9 D1 panel build plan status drifted")
    if any(plan["authorization_flags"].values()) or any(plan["side_effects"].values()):
        raise AssertionError("Phase 2.9 D1 panel build plan authorized execution or side effects")
    return plan


def _read_repository_file(repo_root: Path, relative_path: str) -> str:
    path = (repo_root / relative_path).resolve()
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise ValueError(f"Path escapes repo_root: {relative_path}") from exc
    return path.read_text(encoding="utf-8")

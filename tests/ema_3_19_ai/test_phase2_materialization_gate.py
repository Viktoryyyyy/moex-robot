from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from moex_data.futures.phase2_materialization_gate import (  # noqa: E402
    APPROVED_PHASE2_SOURCE_REFS,
    BLOCKED_PROVIDER_REFS,
    assert_materialization_gate_ready,
    build_materialization_gate_report,
)


MODULE_PATH = REPO_ROOT / "src/moex_data/futures/phase2_materialization_gate.py"


def test_phase2_materialization_gate_module_imports() -> None:
    assert callable(build_materialization_gate_report)
    assert callable(assert_materialization_gate_ready)


def test_gate_report_builds_from_repository_files_only() -> None:
    report = assert_materialization_gate_ready(REPO_ROOT)

    assert report["gate_passed"] is True
    assert report["materialization_gate_status"] == "blocked_pending_data_build_approval"
    assert report["repository_files_read"]
    assert all(not Path(path).is_absolute() for path in report["repository_files_read"])


def test_gate_blocks_ingestion_materialization_features_and_modeling() -> None:
    report = build_materialization_gate_report(REPO_ROOT)

    assert report["can_run_ingestion"] is False
    assert report["can_run_materialization"] is False
    assert report["can_compute_features"] is False
    assert report["can_model"] is False

    for capability in (
        "ingestion",
        "runtime",
        "loader",
        "materialization",
        "feature_computation",
        "modeling",
        "prediction",
    ):
        assert report["capability_authorization"][capability] is False


def test_approved_source_refs_match_phase2_5_placeholders() -> None:
    report = build_materialization_gate_report(REPO_ROOT)

    assert tuple(report["approved_source_contract_refs"]) == APPROVED_PHASE2_SOURCE_REFS
    assert report["validation_checks"]["approved_phase2_source_refs_match"] is True


def test_blocked_providers_are_not_allowed() -> None:
    report = build_materialization_gate_report(REPO_ROOT)

    for blocked_ref in BLOCKED_PROVIDER_REFS:
        assert report["blocked_provider_refs"][blocked_ref]["allowed"] is False
    assert report["validation_checks"]["blocked_provider_refs_declared"] is True


def test_pit_anchor_and_availability_gates_are_represented() -> None:
    report = build_materialization_gate_report(REPO_ROOT)
    gates = report["required_pit_gates"]

    assert gates["availability_ts_utc"]["required"] is True
    assert gates["forecast_anchor_ts"]["required"] is True
    assert gates["rule"] == "availability_ts_utc <= forecast_anchor_ts"
    assert (
        gates["unknown_availability_ts_utc_rule"]
        == "exclude_or_shift_by_at_least_one_trading_day"
    )
    assert report["validation_checks"]["pit_availability_gate_declared"] is True


def test_label_leakage_denylist_gate_is_represented() -> None:
    report = build_materialization_gate_report(REPO_ROOT)
    denylist_gate = report["label_leakage_denylist_gate"]

    assert denylist_gate["represented"] is True
    assert "label_fields" in denylist_gate["denied_groups"]
    assert "annotation_fields" in denylist_gate["denied_groups"]
    assert "future_target_fields" in denylist_gate["denied_groups"]


def test_no_generated_data_path_or_contract_month_automation_is_authorized() -> None:
    report = build_materialization_gate_report(REPO_ROOT)

    assert report["generated_data_path_authorized"] is False
    assert report["current_contract_month_selection_automation_authorized"] is False
    assert report["output_files_authorized"] is False
    assert report["validation_checks"]["generated_data_path_not_authorized"] is True
    assert report["validation_checks"]["current_contract_month_selection_not_authorized"] is True


def test_no_server_runtime_data_loading_side_effects_are_declared_or_imported() -> None:
    report = build_materialization_gate_report(REPO_ROOT)

    assert report["server_apply_authorized"] is False
    assert report["runtime_authorized"] is False
    assert report["side_effects"] == {
        "market_data_loaded": False,
        "network_calls_performed": False,
        "server_commands_performed": False,
        "output_files_written": False,
        "feature_computation_performed": False,
        "model_fitting_performed": False,
    }

    module_source = MODULE_PATH.read_text(encoding="utf-8")
    forbidden_markers = (
        "import pandas",
        "read_parquet",
        "to_parquet",
        "read_csv",
        "requests",
        "urllib",
        "socket",
        "subprocess",
        "MOEX_DATA_ROOT",
    )
    for marker in forbidden_markers:
        assert marker not in module_source

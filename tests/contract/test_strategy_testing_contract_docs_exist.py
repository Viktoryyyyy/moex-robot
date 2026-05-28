from pathlib import Path

import pytest


CONTRACT_ROOT = Path(__file__).resolve().parents[2] / "contracts" / "strategy_testing"

CONTRACT_FILES = (
    "strategy_test_manifest.v1.md",
    "dataset_reference_contract.v1.md",
    "feature_contract.v1.md",
    "label_contract.v1.md",
    "signal_contract.v1.md",
    "canonical_backtest_semantics.v1.md",
    "cost_slippage_contract.v1.md",
    "result_artifact_contract.v1.md",
    "artifact_manifest.v1.md",
    "experiment_registry_entry.v1.md",
    "promotion_verdict.v1.md",
)

COMMON_REQUIRED_MARKERS = (
    "contract_id:",
    "schema_version:",
    "artifact_class:",
    "producer:",
    "consumer:",
    "## required_fields",
    "## validation_rules",
    "## forbidden_patterns",
)


def _contract_text(filename: str) -> str:
    return (CONTRACT_ROOT / filename).read_text(encoding="utf-8")


def test_strategy_testing_contract_docs_exist():
    for filename in CONTRACT_FILES:
        assert (CONTRACT_ROOT / filename).is_file(), filename


@pytest.mark.parametrize("filename", CONTRACT_FILES)
def test_strategy_testing_contract_docs_declare_common_contract_sections(filename):
    text = _contract_text(filename)

    for marker in COMMON_REQUIRED_MARKERS:
        assert marker in text, f"{filename} missing {marker}"


def test_strategy_test_manifest_contract_blocks_runtime_live_by_default():
    text = _contract_text("strategy_test_manifest.v1.md")

    for field_name in (
        "strategy_test_id",
        "strategy_id",
        "strategy_version",
        "test_type",
        "instrument_scope",
        "timeframe_scope",
        "dataset_refs",
        "feature_refs",
        "label_refs",
        "signal_refs",
        "backtest_semantics_ref",
        "cost_slippage_ref",
        "artifact_contract_ref",
        "runtime_live_allowed=false",
    ):
        assert field_name in text


@pytest.mark.parametrize(
    "filename",
    ("feature_contract.v1.md", "label_contract.v1.md", "signal_contract.v1.md"),
)
def test_feature_label_signal_contracts_require_known_by_when_and_anti_leakage_fields(filename):
    text = _contract_text(filename)

    assert "known_by_when" in text
    assert "anti_leakage" in text


def test_label_contract_separates_primary_and_secondary_label_classes():
    text = _contract_text("label_contract.v1.md")

    assert "primary_research" in text
    assert "secondary_execution_compatible" in text


def test_canonical_backtest_semantics_contract_requires_all_execution_rules():
    text = _contract_text("canonical_backtest_semantics.v1.md")

    for field_name in (
        "signal_timestamp_rule",
        "known_by_when_rule",
        "execution_delay_rule",
        "execution_price_rule",
        "fill_rule",
        "position_transition_rule",
        "reversal_rule",
        "sizing_rule",
        "cost_slippage_rule",
        "terminal_close_rule",
        "missing_bar_rule",
        "invalid_data_rule",
        "calendar_session_rule",
        "aggregation_rule",
        "anti_leakage_invariants",
    ):
        assert field_name in text


def test_artifact_manifest_contract_aligns_with_pr3_skeleton():
    text = _contract_text("artifact_manifest.v1.md")

    for field_name in (
        "artifact_manifest_id",
        "run_id",
        "schema_version",
        "created_ts",
        "producer_component",
        "repo_commit",
        "artifacts",
        "artifact_id",
        "artifact_role",
        "artifact_class",
        "producer",
        "consumer",
        "format",
        "schema_version",
        "path",
        "required_for_canonical",
    ):
        assert field_name in text


def test_experiment_registry_contract_aligns_with_pr3_skeleton():
    text = _contract_text("experiment_registry_entry.v1.md")

    for field_name in (
        "registry_entry_id",
        "run_id",
        "strategy_id",
        "strategy_version",
        "test_type",
        "instrument_scope",
        "timeframe_scope",
        "run_status",
        "result_status",
        "canonicality_status",
        "artifact_manifest_ref",
        "repo_commit",
        "created_ts",
        "promotion_verdict_ref",
    ):
        assert field_name in text


def test_promotion_verdict_contract_keeps_runtime_promotion_separate():
    text = _contract_text("promotion_verdict.v1.md").lower()

    assert "promotion verdict is separate from registry metrics" in text
    assert "runner must not create promotion verdict" in text
    assert "runtime/live remains blocked unless explicitly allowed by separate review" in text

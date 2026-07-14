import json
from pathlib import Path


CONTRACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "contracts"
    / "labels"
    / "usdrubf_d1_manual_phase_labels_v1.json"
)


def load_contract():
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_contains_required_top_level_sections():
    contract = load_contract()

    required_sections = {
        "provenance",
        "allowed_labels",
        "label_meanings",
        "raw_interval_schema",
        "normalized_session_schema",
        "boundary_semantics",
        "non_trading_date_mapping",
        "overlap_rule",
        "target_derivation_rules",
        "leakage_policy",
        "execution_blockers",
        "manual_phase_labels",
    }

    assert required_sections.issubset(contract)


def test_contract_allowed_labels_and_interval_labels_are_consistent():
    contract = load_contract()

    assert contract["allowed_labels"] == ["B", "S", "OUT"]
    assert set(contract["label_meanings"]) == {"B", "S", "OUT"}

    interval_labels = {row["label"] for row in contract["manual_phase_labels"]}
    assert interval_labels == {"B", "S", "OUT"}


def test_contract_declares_inclusive_boundaries_and_no_synthetic_rows():
    contract = load_contract()

    assert contract["boundary_semantics"]["start_date"] == "inclusive"
    assert contract["boundary_semantics"]["end_date"] == "inclusive"
    assert contract["non_trading_date_mapping"]["synthetic_rows"] == "forbidden"


def test_contract_declares_overlap_previous_interval_wins():
    contract = load_contract()

    overlap = contract["overlap_rule"]
    assert overlap["known_overlap_date"] == "2025-09-11"
    assert (
        overlap["deterministic_rule"]
        == "previous_interval_wins_for_primary_label_on_overlap_date"
    )
    assert overlap["primary_label_for_2025_09_11"] == "B"
    assert overlap["transition_exit_day"] is True
    assert overlap["OUT_effective_from"] == "next_valid_D1_session_after_2025_09_11"


def test_contract_declares_leakage_policy_for_runtime_ai_assistant():
    contract = load_contract()

    leakage_policy = contract["leakage_policy"]
    assert leakage_policy["phase_label_not_runtime_feature"] is True
    assert leakage_policy["transition_targets_not_runtime_features"] is True
    assert leakage_policy["phase_remaining_sessions_not_runtime_feature"] is True
    assert leakage_policy["next_phase_label_not_runtime_feature"] is True
    assert leakage_policy["source_interval_id_not_runtime_feature"] is True

    must_not_consume = set(leakage_policy["runtime_AI_assistant_must_not_consume"])
    assert "manual_phase_labels" in must_not_consume
    assert "future-derived_transition_labels" in must_not_consume
    assert "phase_remaining_sessions" in must_not_consume


def test_contract_preserves_execution_blockers_without_solving_them():
    contract = load_contract()

    blockers = contract["execution_blockers"]
    assert blockers["source_data_coverage_gap_for_execution"]["status"] == "active"
    assert (
        blockers["raw_csv_xlsx_missing_for_final_validation"]["status"]
        == "active_but_not_blocking_contract_implementation"
    )
    assert blockers["calendar_contract_partial"]["status"] == "active"
    assert (
        blockers["external_factor_sources_unknown"]["status"]
        == "active_for_later_model_layer"
    )


def test_contract_does_not_authorize_forbidden_output_artifact():
    contract = load_contract()

    assert (
        "data/labels/usdrubf_d1_manual_phase_labels_v1.csv"
        in contract["forbidden_outputs_for_this_task"]
    )

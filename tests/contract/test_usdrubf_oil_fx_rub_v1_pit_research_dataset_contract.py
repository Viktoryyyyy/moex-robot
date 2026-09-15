from __future__ import annotations

import json
from pathlib import Path


CONTRACT_PATH = Path(
    "contracts/experiments/usdrubf_oil_fx_rub_v1_pit_research_dataset.json"
)


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_is_additive_create_only_scope() -> None:
    contract = _contract()
    scope = contract["approved_file_scope"]
    assert scope["existing_files_to_modify"] == []
    assert scope["scope_widening_allowed"] is False
    assert scope["exact_file_count"] == 6
    assert scope["create_only"] == [
        "contracts/experiments/usdrubf_oil_fx_rub_v1_pit_research_dataset.json",
        "src/moex_research/features/oil_fx_rub_v1_features.py",
        "src/moex_research/runners/usdrubf_oil_fx_rub_v1_pit_research_dataset.py",
        "tests/unit/test_oil_fx_rub_v1_features.py",
        "tests/unit/test_usdrubf_oil_fx_rub_v1_pit_research_dataset.py",
        "tests/contract/test_usdrubf_oil_fx_rub_v1_pit_research_dataset_contract.py",
    ]
    assert all(value is False for value in contract["authority_boundary"].values())


def test_contract_pins_existing_phase6_and_brent_evidence() -> None:
    upstream = _contract()["upstream_contracts"]
    phase6 = upstream["phase6_identity_and_price_lineage"]
    assert phase6["frozen_modeling_dataset_sha256"] == (
        "fdd626f9e0522c6bbb653f9e17fbbbeef7ded77f57ff187b35246a2458d55d00"
    )
    assert phase6["frozen_dataset_manifest_sha256"] == (
        "fcbbb5e5ed0549c5c6f397e34f203f01836271f6bf471f90cab5a2fd64ace082"
    )
    assert "terminal source row" in phase6["source_panel_binding"]
    brent = upstream["brent"]
    assert brent["runtime_must_not_be_repeated"] is True
    assert brent["source_artifact_regeneration_allowed"] is False
    assert brent["accepted_artifact_sha256"]["brent_pit_acceptance_matrix"] == (
        "78b60c9542fc08667267849b9ce03fdf161d2dc971fe99e1ab9d6a8d56266c43"
    )
    assert brent["accepted_artifact_sha256"]["phase84a_gate_results"] == (
        "aceaefb4d2e2a236539dd527c98464ddd1ea6bf5f1cdb8121662e1ce087f9c4c"
    )
    assert brent["accepted_artifact_sha256"]["phase84a_input_identity"] == (
        "3fa20b2daf45f196937064b5f1cc6a58b8009e544d6141e32a77d841d68b65ae"
    )


def test_contract_keeps_cnyrubf_full_mode_blocked_until_separate_admission() -> None:
    contract = _contract()
    modes = contract["staged_modes"]
    cny = contract["upstream_contracts"]["cnyrubf"]
    assert modes["brent_only"]["authorized_now"] is True
    assert modes["brent_only"]["cnyrubf_input_allowed"] is False
    assert modes["oil_fx_full"]["authorized_now"] is False
    assert "separate additive admission contract" in modes["oil_fx_full"]["authorization_condition"]
    assert "separate additive admission contract" in cny["future_enablement_policy"]
    assert cny["security_id"] == "CNYRUBF"
    assert cny["spot_cnyrub_tom_allowed"] is False
    assert cny["synthetic_cross_allowed"] is False


def test_contract_separates_labels_and_forbids_unbound_terminal_and_cross_contract_returns() -> None:
    contract = _contract()
    pit = contract["point_in_time_policy"]
    labels = contract["label_policy"]
    assert pit["target_day_source_data_allowed"] is False
    assert pit["forward_fill_allowed"] is False
    assert pit["backward_fill_allowed"] is False
    assert pit["interpolation_allowed"] is False
    assert pit["brent_cross_contract_return_allowed"] is False
    assert pit["terminal_unbound_source_row_as_label_endpoint_allowed"] is False
    assert labels["feature_and_label_artifacts_separate"] is True
    assert "Phase 6 source D1 panel" in labels["price_source"]
    assert labels["labels_forbidden_from_feature_artifact"] is True
    assert labels["forward_return_horizons_sessions"] == [1, 3, 5, 10]
    assert labels["unbound_terminal_source_close_allowed"] is False
    assert labels["terminal_endpoint_policy"] == "structural_null"

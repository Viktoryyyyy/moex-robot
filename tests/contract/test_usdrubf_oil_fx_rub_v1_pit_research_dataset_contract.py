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


def test_contract_reuses_existing_source_lineage_without_redefining_it() -> None:
    contract = _contract()
    upstream = contract["upstream_contracts"]
    assert upstream["brent"]["source_contract"].endswith(
        "usdrubf_phase8_4a_moex_brent_source_validation_v1.json"
    )
    assert upstream["brent"]["runtime_must_not_be_repeated"] is True
    assert upstream["brent"]["source_artifact_regeneration_allowed"] is False
    assert upstream["cnyrubf"]["source_contract"].endswith(
        "usdrubf_phase8_6a_algopack_cnyrubf_fo_source_correction_v1.json"
    )
    assert upstream["cnyrubf"]["security_id"] == "CNYRUBF"
    assert upstream["cnyrubf"]["spot_cnyrub_tom_allowed"] is False
    assert upstream["cnyrubf"]["synthetic_cross_allowed"] is False


def test_contract_stages_brent_now_and_full_fx_only_after_post_fix_gate() -> None:
    modes = _contract()["staged_modes"]
    assert modes["brent_only"]["authorized_now"] is True
    assert modes["brent_only"]["cnyrubf_input_allowed"] is False
    assert modes["oil_fx_full"]["authorized_now"] is False
    assert "post-fix CNYRUBF" in modes["oil_fx_full"]["authorization_condition"]


def test_contract_separates_labels_and_forbids_cross_contract_returns() -> None:
    contract = _contract()
    pit = contract["point_in_time_policy"]
    labels = contract["label_policy"]
    assert pit["target_day_source_data_allowed"] is False
    assert pit["forward_fill_allowed"] is False
    assert pit["backward_fill_allowed"] is False
    assert pit["interpolation_allowed"] is False
    assert pit["brent_cross_contract_return_allowed"] is False
    assert labels["feature_and_label_artifacts_separate"] is True
    assert labels["labels_forbidden_from_feature_artifact"] is True
    assert labels["forward_return_horizons_sessions"] == [1, 3, 5, 10]

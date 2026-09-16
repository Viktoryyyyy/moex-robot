from __future__ import annotations

import json
from pathlib import Path

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase03_lead_lag as runner


CONTRACT = Path("contracts/experiments/usdrubf_oil_fx_rub_v1_phase03_lead_lag_research.json")


def test_contract_identity_scope_and_runtime_inventory() -> None:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    identity = contract["contract_identity"]
    assert identity["project"] == "MOEX_Bot"
    assert identity["task_id"] == runner.TASK_ID
    assert identity["contract_id"] == runner.CONTRACT_ID
    assert identity["contract_version"] == runner.CONTRACT_VERSION
    scope = contract["scope"]
    assert scope["brent_only"] is True
    assert scope["cnyrubf_allowed"] is False
    assert scope["model_fit_allowed"] is False
    assert scope["trading_rule_design_allowed"] is False
    assert contract["runtime_artifacts"] == list(runner.DECLARED_OUTPUTS)
    runner._validate_contract(contract)


def test_methodology_is_frozen_and_non_trading() -> None:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    method = contract["methodology"]
    assert method["bootstrap_samples"] == runner.BOOTSTRAP_SAMPLES == 1000
    assert method["bootstrap_block_length_sessions"] == runner.BOOTSTRAP_BLOCK_LENGTH == 5
    assert method["bootstrap_seed"] == runner.BOOTSTRAP_SEED == 20260916
    assert method["h2_min_abs_spearman_improvement"] == runner.H2_MIN_ABS_RHO_IMPROVEMENT == 0.03
    boundary = contract["authority_boundary"]
    assert boundary["direct_main_write_allowed"] is False
    assert boundary["server_runtime_allowed_after_merge_with_owner_approval"] is True
    assert boundary["model_fit_allowed"] is False
    assert boundary["trading_allowed"] is False


def test_exact_create_only_scope() -> None:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    scope = contract["approved_file_scope"]
    assert scope["existing_files_to_modify"] == []
    assert scope["scope_widening_allowed"] is False
    assert scope["exact_file_count"] == 4
    assert len(scope["create_only"]) == 4

from __future__ import annotations

import json
from pathlib import Path

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase06a_robustness_sensitivity as phase06a


CONTRACT = Path("contracts/experiments/usdrubf_oil_fx_rub_v1_phase06a_robustness_sensitivity.json")


def _payload() -> dict:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def test_phase06a_contract_matches_frozen_repository_semantics() -> None:
    payload = _payload()
    phase06a._validate_contract(payload)
    grid = payload["sensitivity_grid"]
    assert grid["rolling_window_sessions"] == [63, 126, 252]
    assert grid["high_threshold"] == [0.70, 0.75, 0.80]
    assert grid["cooldown_source_sessions"] == [15, 21, 30]
    assert grid["exit_horizons_sessions"] == [5, 10, 15, 20, 30]
    assert grid["round_trip_cost_bps"] == 20
    assert grid["configuration_count"] == 27
    assert grid["metric_cell_count"] == 135


def test_phase06a_contract_forbids_optimization_and_winner_selection() -> None:
    payload = _payload()
    purpose = payload["purpose"]
    assert purpose["parameter_optimization_allowed"] is False
    assert purpose["winner_selection_allowed"] is False
    assert purpose["parameter_ranking_allowed"] is False
    assert purpose["statistical_inference_allowed"] is False
    assert payload["aggregate_diagnostics"]["robustness_claim_allowed"] is False
    assert payload["methodology"]["best_parameter_combination_must_not_be_selected"] is True


def test_phase06a_runtime_artifact_inventory_is_exact() -> None:
    payload = _payload()
    assert payload["runtime_artifacts"] == list(phase06a.DECLARED_OUTPUTS)

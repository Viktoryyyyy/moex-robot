from __future__ import annotations

import json
from pathlib import Path

from moex_research.runners import (
    usdrubf_oil_fx_rub_v1_phase04_regime_research as phase04,
)
from moex_research.runners import usdrubf_oil_fx_rub_v1_phase05_signal_design as phase05


CONTRACT = Path("contracts/experiments/usdrubf_oil_fx_rub_v1_phase05_signal_design.json")


def test_phase05_contract_is_frozen_and_research_only() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    phase05._validate_contract(payload)

    assert payload["contract_identity"]["task_id"] == phase05.TASK_ID
    assert payload["signal_definition"]["minimum_entry_separation_source_sessions"] == 21
    assert payload["signal_definition"]["fixed_exit_horizons_sessions"] == [5, 10, 20]
    assert payload["signal_definition"]["regime_exit_allowed"] is False
    assert payload["signal_definition"]["stop_loss_allowed_in_phase05"] is False
    assert payload["phase05_output_policy"]["future_exit_prices_allowed"] is False
    assert payload["phase05_output_policy"]["future_returns_allowed"] is False
    assert payload["phase05_output_policy"]["pnl_allowed"] is False
    assert payload["purpose"]["performance_evaluation_allowed"] is False
    assert payload["purpose"]["parameter_optimization_allowed"] is False
    assert payload["purpose"]["trading_allowed"] is False


def test_phase05_contract_hashes_match_frozen_phase04_inputs() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    inputs = payload["inputs"]
    for key, expected_hash in phase04.EXPECTED_SHA256.items():
        assert inputs[f"{key}_sha256"] == expected_hash


def test_phase05_runtime_artifacts_exact() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    assert payload["runtime_artifacts"] == list(phase05.DECLARED_OUTPUTS)
    assert len(payload["gates"]) == 9

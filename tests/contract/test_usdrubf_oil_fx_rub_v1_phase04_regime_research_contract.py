from __future__ import annotations

import json
from pathlib import Path


CONTRACT = Path("contracts/experiments/usdrubf_oil_fx_rub_v1_phase04_regime_research.json")


def _load() -> dict:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def test_phase04_contract_identity_and_scope() -> None:
    payload = _load()
    identity = payload["contract_identity"]
    assert identity["project"] == "MOEX_Bot"
    assert identity["task_id"] == "STRAT_OIL_FX_RUB_V1_04_REGIME_RESEARCH"
    assert identity["contract_version"] == "1.0"
    assert payload["scope"]["brent_only"] is True
    assert payload["scope"]["cnyrubf_allowed"] is False
    assert payload["scope"]["oil_rub_product_allowed"] is False
    assert payload["scope"]["target_day_data_allowed"] is False


def test_phase04_methodology_is_frozen_ex_ante() -> None:
    method = _load()["methodology"]
    assert method["rolling_window_sessions"] == 126
    assert method["minimum_history_sessions"] == 63
    assert method["high_threshold"] == 0.75
    assert method["low_threshold"] == 0.25
    assert method["forward_return_horizons_sessions"] == [1, 3, 5, 10, 20]
    assert method["no_threshold_optimization"] is True
    assert method["no_horizon_optimization"] is True


def test_phase04_authority_and_create_only_scope_remain_closed() -> None:
    payload = _load()
    assert payload["approved_file_scope"]["existing_files_to_modify"] == []
    assert payload["approved_file_scope"]["scope_widening_allowed"] is False
    assert all(value is False for value in payload["authority_boundary"].values())
    assert payload["purpose"]["model_fit_allowed"] is False
    assert payload["purpose"]["trading_rule_design_allowed"] is False
    assert payload["purpose"]["trading_allowed"] is False

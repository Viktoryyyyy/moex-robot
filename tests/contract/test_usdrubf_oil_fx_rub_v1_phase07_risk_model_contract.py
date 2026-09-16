from __future__ import annotations

import json
from pathlib import Path

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase07_risk_model as phase07


CONTRACT = Path("contracts/experiments/usdrubf_oil_fx_rub_v1_phase07_risk_model.json")


def test_phase07_contract_is_frozen_and_research_only() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    phase07._validate_contract(payload)

    assert payload["contract_identity"]["task_id"] == "STRAT_OIL_FX_RUB_V1_07_RISK_MODEL"
    assert payload["baseline_reference"]["exit_horizons_sessions"] == [5, 10, 20]
    assert payload["risk_definition"]["stop_grid_pct"] == [0.01, 0.02, 0.03, 0.04, 0.05]
    assert payload["risk_definition"]["risk_budget_pct_nav"] == [0.0025, 0.005, 0.01]
    assert payload["risk_definition"]["round_trip_cost_bps"] == 20
    assert payload["risk_definition"]["gap_aware_stop_fill"] == "max(stop_price, session_open)"
    assert payload["risk_definition"]["gap_risk_budget_breach_reported"] is True
    assert payload["risk_definition"]["contract_count_sizing_allowed"] is False
    assert payload["purpose"]["parameter_optimization_allowed"] is False
    assert payload["purpose"]["best_stop_selection_allowed"] is False
    assert payload["purpose"]["strategy_promotion_allowed"] is False
    assert payload["purpose"]["trading_allowed"] is False
    assert payload["methodology"]["terminal_source_ohlc_allowed"] is False
    assert payload["methodology"]["overlapping_positions_allowed"] is False
    assert payload["runtime_artifacts"] == list(phase07.DECLARED_OUTPUTS)

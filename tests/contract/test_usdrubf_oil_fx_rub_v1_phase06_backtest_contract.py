from __future__ import annotations

import json
from pathlib import Path

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase06_backtest as phase06


CONTRACT = Path("contracts/experiments/usdrubf_oil_fx_rub_v1_phase06_backtest.json")


def test_phase06_contract_is_frozen_research_only_backtest() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    phase06._validate_contract(payload)

    assert payload["contract_identity"]["task_id"] == phase06.TASK_ID
    assert payload["purpose"]["performance_evaluation_allowed"] is True
    assert payload["purpose"]["parameter_optimization_allowed"] is False
    assert payload["purpose"]["horizon_ranking_allowed"] is False
    assert payload["purpose"]["position_sizing_allowed"] is False
    assert payload["purpose"]["trading_allowed"] is False
    assert payload["backtest_definition"]["fixed_exit_horizons_sessions"] == [5, 10, 20]
    assert payload["backtest_definition"]["round_trip_cost_bps_sensitivity"] == [0, 5, 10, 20]
    assert payload["sample_policy"]["frozen_independent_trade_count"] == 3
    assert payload["sample_policy"]["statistical_inference_allowed"] is False


def test_phase06_frozen_schedule_exact() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    assert phase06._contract_schedule_rows(payload) == phase06.FROZEN_SCHEDULE
    assert [row[0] for row in phase06.FROZEN_SCHEDULE] == [
        "2025-01-13",
        "2025-09-29",
        "2026-03-17",
    ]


def test_phase06_runtime_artifacts_and_gate_inventory_exact() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    assert payload["runtime_artifacts"] == list(phase06.DECLARED_OUTPUTS)
    assert len(payload["gates"]) == 9

from __future__ import annotations

import json
from pathlib import Path

from moex_research.runners import usdrubf_oil_fx_rub_v1_phase07a_data_expansion_admission as phase07a


CONTRACT = Path("contracts/experiments/usdrubf_oil_fx_rub_v1_phase07a_data_expansion_admission.json")


def test_phase07a_contract_is_frozen_and_research_only() -> None:
    payload = json.loads(CONTRACT.read_text(encoding="utf-8"))
    phase07a._validate_contract(payload)

    assert payload["contract_identity"]["task_id"] == phase07a.TASK_ID
    assert payload["usdrubf_frozen_prefix"]["prefix_start_date"] == "2022-04-26"
    assert payload["usdrubf_frozen_prefix"]["prefix_end_date"] == "2026-10-01"
    assert payload["usdrubf_frozen_prefix"]["accepted_date_count"] == 1143
    assert payload["usdrubf_frozen_prefix"]["raw_row_count"] == 188760
    assert payload["usdrubf_frozen_prefix"]["partition_content_set_sha256"] == phase07a.EXPECTED_PREFIX_CONTENT_SHA256
    assert payload["usdrubf_frozen_prefix"]["target_identity_count"] == 1142

    assert payload["brent_admission"]["minimum_days_to_expiration"] == 7
    assert payload["brent_admission"]["candle_trade_date"] == "exact prior_trade_date"
    assert payload["brent_admission"]["continuous_alias_allowed"] is False
    assert payload["brent_admission"]["cross_contract_return_allowed"] is False

    assert payload["purpose"]["signal_evaluation_allowed"] is False
    assert payload["purpose"]["backtest_allowed"] is False
    assert payload["purpose"]["parameter_optimization_allowed"] is False
    assert payload["purpose"]["strategy_promotion_allowed"] is False
    assert payload["purpose"]["trading_allowed"] is False

    assert payload["runtime_artifacts"] == list(phase07a.DECLARED_OUTPUTS)

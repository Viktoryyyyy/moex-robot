from __future__ import annotations

import numpy as np
import pandas as pd

from moex_research.runners.usdrubf_oil_fx_rub_v1_phase03_lead_lag import (
    FEATURE_COLUMNS,
    IDENTITY_COLUMNS,
    LABEL_COLUMNS,
    Phase03LeadLagError,
    _build_gates,
    _validate_input_bundle_paths,
    analyze_lead_lag,
)


def _frame(n: int = 420) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    base = np.linspace(-1.0, 1.0, n) + rng.normal(0.0, 0.03, n)
    frame = pd.DataFrame(
        {
            "target_trade_date": pd.date_range("2024-01-01", periods=n, freq="D").strftime("%Y-%m-%d"),
            "target_instrument_id": "forts.usdrubf",
        }
    )
    frame["ext_brent_intraday_return"] = base
    frame["ext_brent_range_pct"] = np.abs(base) + 0.2
    frame["ext_brent_close_location"] = np.linspace(0.1, 0.9, n)
    frame["ext_log1p_brent_volume"] = np.log1p(np.arange(n) + 1000.0)
    frame["ext_brent_same_contract_close_return_1session"] = base
    frame["ext_brent_same_contract_close_return_3session"] = base * 1.1
    frame["ext_brent_same_contract_close_return_5session"] = base * 1.2
    frame["fwd_usdrubf_close_return_1session"] = -0.2 * base + rng.normal(0.0, 0.10, n)
    frame["fwd_usdrubf_close_return_3session"] = -0.5 * base + rng.normal(0.0, 0.08, n)
    frame["fwd_usdrubf_close_return_5session"] = -0.7 * base + rng.normal(0.0, 0.07, n)
    frame["fwd_usdrubf_close_return_10session"] = -0.4 * base + rng.normal(0.0, 0.09, n)
    return frame


def test_analysis_inventory_and_hypotheses() -> None:
    pairwise, quantiles, temporal, summary = analyze_lead_lag(
        _frame(), bootstrap_samples=120, bootstrap_block_length=5, bootstrap_seed=11
    )
    assert len(pairwise) == len(FEATURE_COLUMNS) * len(LABEL_COLUMNS) == 28
    assert len(quantiles) == 28
    assert len(temporal) == 28
    assert set(pairwise["evidence_status"]) <= {"robust", "suggestive", "not_supported"}
    primary = pairwise[pairwise["feature"].eq("ext_brent_same_contract_close_return_1session")]
    assert (primary["spearman"] < 0).all()
    assert summary["H1_oil_impulse"]["status"] in {
        "robust_evidence_present",
        "suggestive_evidence_only",
    }
    assert summary["model_fit_performed"] is False
    assert summary["trading_rule_design_performed"] is False


def test_gates_pass_for_well_formed_analysis() -> None:
    pairwise, quantiles, temporal, summary = analyze_lead_lag(
        _frame(), bootstrap_samples=120, bootstrap_block_length=5, bootstrap_seed=13
    )
    gates = _build_gates(pairwise, quantiles, temporal, summary)
    assert gates["G9_final"]["passed"] is True
    assert gates["G9_final"]["status"] == "oil_fx_rub_phase03_lead_lag_research_complete"


def test_schema_constants_do_not_mix_labels_into_features() -> None:
    assert not (set(FEATURE_COLUMNS) & set(LABEL_COLUMNS))
    assert IDENTITY_COLUMNS == ("target_trade_date", "target_instrument_id")


def test_runtime_inputs_must_share_one_materialization_directory(tmp_path) -> None:
    bundle = tmp_path / "bundle"
    other = tmp_path / "other"
    bundle.mkdir()
    other.mkdir()
    _validate_input_bundle_paths(
        bundle / "features.parquet",
        bundle / "labels.parquet",
        bundle / "manifest.json",
        bundle / "gate_results.json",
    )
    try:
        _validate_input_bundle_paths(
            bundle / "features.parquet",
            bundle / "labels.parquet",
            bundle / "manifest.json",
            other / "gate_results.json",
        )
    except Phase03LeadLagError:
        pass
    else:
        raise AssertionError("mixed materialization directories must fail closed")

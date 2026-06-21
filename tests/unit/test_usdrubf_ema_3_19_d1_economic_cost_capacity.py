from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.moex_research.runners import usdrubf_ema_3_19_d1_economic_cost_capacity as m5a


def _artifacts(*, profitable: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, dict, dict, dict]:
    timestamps = pd.date_range("2024-01-02 18:50:00", periods=6, freq="D")
    if profitable:
        opens = [100.0, 101.0, 102.0, 104.0, 103.0, 102.0]
        closes = [100.0, 102.0, 103.0, 104.0, 102.0, 103.0]
    else:
        opens = [100.0, 105.0, 103.0, 100.0, 100.0, 99.0]
        closes = [100.0, 104.0, 102.0, 100.0, 100.0, 101.0]
    d1 = pd.DataFrame(
        {
            "instrument_id": ["usdrubf"] * 6,
            "end": timestamps,
            "open": opens,
            "high": [max(open_price, close_price) + 1.0 for open_price, close_price in zip(opens, closes)],
            "low": [min(open_price, close_price) - 1.0 for open_price, close_price in zip(opens, closes)],
            "close": closes,
            "known_by_when": ["D close after finalized D1 bar"] * 6,
        }
    )
    context = pd.DataFrame(
        {
            "instrument_id": ["usdrubf"] * 3,
            "end": [timestamps[0], timestamps[2], timestamps[4]],
            "cross_dir": ["cross_up", "cross_down", "cross_up"],
            "ema3": [101.0, 102.0, 103.0],
            "ema19": [100.0, 103.0, 102.0],
            "known_by_when": ["D close after finalized D1 bar"] * 3,
        }
    )
    quality = {
        "experiment_id": m5a.M2_EXPERIMENT_ID,
        "event_count": 3,
        "d1_ohlc_row_count": 6,
        "row_counts": {
            "d1_ohlc": 6,
            "cross_context": 3,
            "cross_labels": 3,
            "raw_baseline_summary": 1,
        },
        "time_semantics": {
            "event_day_symbol": "D",
            "d_close_known_by_when": "D close after finalized D1 bar",
            "earliest_label_outcome_anchor": "D+1 open",
            "feature_context_uses_d_plus_1_values": False,
        },
        "leakage_checks": {
            "feature_context_label_like_columns": {
                "usdrubf_d1_ohlc.csv": [],
                "usdrubf_d1_ema_3_19_cross_context.csv": [],
            },
            "feature_context_future_outcome_columns": {
                "usdrubf_d1_ohlc.csv": [],
                "usdrubf_d1_ema_3_19_cross_context.csv": [],
            },
            "no_d_plus_1_values_in_feature_context_rows": True,
            "labels_kept_research_only": True,
        },
    }
    m4b = {
        "experiment_id": m5a.M4B_EXPERIMENT_ID,
        "result_status": "rule_gate_not_supported",
        "selected_rule": None,
        "model_training_performed": False,
        "threshold_sweep_performed": False,
        "post_hoc_rule_search_performed": False,
        "runtime_or_trading_action_performed": False,
        "strategy_promotion_allowed": False,
    }
    m4c = {
        "experiment_id": m5a.M4C_EXPERIMENT_ID,
        "result_status": "technical_ml_not_supported",
        "selected_feature_group": None,
        "persistent_model_artifact_emitted": False,
        "hyperparameter_search_performed": False,
        "threshold_tuning_performed": False,
        "post_hoc_feature_selection_performed": False,
        "model_promotion_allowed": False,
        "strategy_promotion_allowed": False,
        "runtime_or_trading_action_performed": False,
    }
    return d1, context, quality, m4b, m4c


def _write_inputs(tmp_path: Path, *, profitable: bool = True) -> tuple[Path, Path, Path, Path, Path]:
    d1, context, quality, m4b, m4c = _artifacts(profitable=profitable)
    d1_path = tmp_path / "d1.csv"
    context_path = tmp_path / "cross_context.csv"
    quality_path = tmp_path / "m2_quality.json"
    m4b_path = tmp_path / "m4b_decision.json"
    m4c_path = tmp_path / "m4c_decision.json"
    d1.to_csv(d1_path, index=False)
    context.to_csv(context_path, index=False)
    quality_path.write_text(json.dumps(quality), encoding="utf-8")
    m4b_path.write_text(json.dumps(m4b), encoding="utf-8")
    m4c_path.write_text(json.dumps(m4c), encoding="utf-8")
    return d1_path, context_path, quality_path, m4b_path, m4c_path


def test_prepare_inputs_maps_crosses_to_target_positions_without_labels() -> None:
    d1, context, _, _, _ = _artifacts()
    bars, signals = m5a.prepare_backtest_inputs(d1, context)

    assert [row["target_position"] for row in signals.to_dict("records")] == [1, -1, 1]
    assert list(signals["cross_dir"]) == ["cross_up", "cross_down", "cross_up"]
    assert list(bars.columns) == ["timestamp", "open", "close", "valid"]
    assert not any("return" in column or "allow_trade" in column for column in signals.columns)

    leaked = context.copy()
    leaked["h2_signed_return"] = 0.01
    with pytest.raises(ValueError, match="future or label"):
        m5a.prepare_backtest_inputs(d1, leaked)


def test_canonical_engine_next_open_reversal_terminal_close_and_break_even() -> None:
    d1, context, _, _, _ = _artifacts(profitable=True)
    bars, signals = m5a.prepare_backtest_inputs(d1, context)
    result, fills, costs, position_path = m5a.run_canonical_gross_backtest(bars, signals)
    metrics, capacity = m5a.build_economic_measurements(result, fills, costs, position_path)

    assert list(fills["raw_price"]) == [101.0, 104.0, 102.0, 103.0]
    assert list(fills["quantity"]) == [1, -2, 2, -1]
    assert list(fills["transition_type"]) == [
        "flat_to_long",
        "long_to_short_reversal",
        "short_to_long_reversal",
        "long_to_flat",
    ]
    assert list(fills["reason"]) == ["signal", "signal", "signal", "forced_terminal_close"]
    assert costs["total_cost"].eq(0.0).all()
    assert metrics["gross_total_pnl_price_units"] == pytest.approx(6.0)
    assert metrics["gross_turnover_notional"] == pytest.approx(616.0)
    assert capacity["break_even_all_in_bps_per_traded_notional"] == pytest.approx(
        6.0 / 616.0 * 10_000.0
    )
    assert capacity["pnl_change_per_one_all_in_bp"] == pytest.approx(-616.0 / 10_000.0)
    assert position_path.iloc[-1]["position"] == 0


def test_decision_requires_real_cost_binding_even_when_gross_is_positive() -> None:
    d1, context, _, _, _ = _artifacts(profitable=True)
    bars, signals = m5a.prepare_backtest_inputs(d1, context)
    result, fills, costs, position_path = m5a.run_canonical_gross_backtest(bars, signals)
    metrics, capacity = m5a.build_economic_measurements(result, fills, costs, position_path)
    decision = m5a.build_decision(
        metrics,
        capacity,
        run_id="positive-gross",
        git_commit_sha="a" * 40,
    )

    assert decision["result_status"] == "economic_cost_binding_required"
    assert decision["gross_supported_vs_cash"] is True
    assert decision["full_economic_support_available"] is False
    assert decision["strategy_promotion_allowed"] is False
    assert decision["runtime_or_trading_action_performed"] is False


def test_nonpositive_gross_result_is_rejected_before_cost_binding() -> None:
    d1, context, _, _, _ = _artifacts(profitable=False)
    bars, signals = m5a.prepare_backtest_inputs(d1, context)
    result, fills, costs, position_path = m5a.run_canonical_gross_backtest(bars, signals)
    metrics, capacity = m5a.build_economic_measurements(result, fills, costs, position_path)
    decision = m5a.build_decision(
        metrics,
        capacity,
        run_id="negative-gross",
        git_commit_sha="b" * 40,
    )

    assert metrics["gross_total_pnl_price_units"] < 0.0
    assert decision["result_status"] == "economic_baseline_not_supported_gross"
    assert decision["gross_supported_vs_cash"] is False
    assert decision["selected_economic_configuration"] is None


def test_lineage_validation_rejects_promoted_or_selected_prior_results() -> None:
    d1, context, quality, m4b, m4c = _artifacts()
    normalized_d1, normalized_context = m5a.validate_source_artifacts(d1, context, quality)
    assert len(normalized_d1) == 6
    assert len(normalized_context) == 3
    m5a.validate_prior_decisions(m4b, m4c)

    invalid_m4c = dict(m4c)
    invalid_m4c["selected_feature_group"] = "trend"
    with pytest.raises(ValueError, match="selected_feature_group"):
        m5a.validate_prior_decisions(m4b, invalid_m4c)


def test_run_package_writes_exact_declared_outputs(tmp_path: Path) -> None:
    d1_path, context_path, quality_path, m4b_path, m4c_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "run"

    result = m5a.run_research_package(
        d1_ohlc_path=d1_path,
        cross_context_path=context_path,
        m2_quality_report_path=quality_path,
        m4b_decision_path=m4b_path,
        m4c_decision_path=m4c_path,
        output_dir=output_dir,
        run_id="m5a-unit-run",
        git_commit_sha="c" * 40,
    )

    assert sorted(path.name for path in output_dir.iterdir()) == sorted(m5a.DECLARED_OUTPUT_FILES)
    assert result["metadata"]["git_commit_sha"] == "c" * 40
    assert result["quality_report"]["canonical_engine"]["custom_strategy_pnl_engine_used"] is False
    assert result["quality_report"]["anti_leakage"]["label_or_future_fields_used"] == []
    assert result["decision"]["result_status"] == "economic_cost_binding_required"
    assert result["decision"]["strategy_promotion_allowed"] is False

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.moex_research.runners import usdrubf_ema_3_19_d1_technical_ml_benchmark as m4c


def _artifacts(rows: int = 64) -> tuple[pd.DataFrame, pd.DataFrame, dict, dict]:
    context_rows: list[dict[str, object]] = []
    label_rows: list[dict[str, object]] = []
    for index in range(rows):
        direction = 1 if index % 2 == 0 else -1
        cross_dir = "cross_up" if direction == 1 else "cross_down"
        target = int(index % 5 in (0, 1, 3))
        signal = 1 if target else -1
        end = pd.Timestamp("2018-01-05 18:50:00") + pd.Timedelta(days=index * 7)
        context_rows.append(
            {
                "instrument_id": "usdrubf",
                "end": end,
                "cross_dir": cross_dir,
                "session_index": index * 4,
                "rsi_14": 50.0 + direction * signal * 8.0,
                "roc_10": direction * signal * 0.02,
                "stoch_k_14": 50.0 + direction * signal * 10.0,
                "stoch_d_3": 50.0 + direction * signal * 7.0,
                "adx_14": 22.0 + 4.0 * target,
                "di_spread_14": direction * signal * 2.0,
                "macd_hist_12_26_9_pct": direction * signal * 0.01,
                "atr_14_pct": 0.02 + 0.001 * (index % 4),
                "bb_percent_b_20_2": 0.5 + direction * signal * 0.15,
                "bb_bandwidth_20_2": 0.10 + 0.01 * (index % 3),
                "indicator_ready": True,
            }
        )
        h2_return = 0.02 if target else -0.012
        row: dict[str, object] = {
            "instrument_id": "usdrubf",
            "end": end,
            "cross_dir": cross_dir,
            "event_session_index": index * 4,
            "entry_session_index": index * 4 + 1,
        }
        for horizon_index, horizon in enumerate(("h1", "h2", "h3", "h5", "h10"), start=1):
            signed_return = h2_return - 0.001 * abs(horizon_index - 2)
            row[f"{horizon}_completion_index"] = index * 4 + horizon_index + 1
            row[f"{horizon}_signed_return"] = signed_return
            row[f"{horizon}_allow_trade"] = int(signed_return > 0.0)
            row[f"{horizon}_opposite_cross_before_exit"] = bool(not target)
        row.update(
            {
                "reverse_event_session_index": index * 4 + 2,
                "reverse_completion_index": index * 4 + 3,
                "holding_sessions_to_reverse_exit": 2,
                "reverse_signed_return": h2_return,
                "reverse_allow_trade": target,
                "reverse_label_censored": False,
            }
        )
        label_rows.append(row)
    context = pd.DataFrame(context_rows)
    labels = pd.DataFrame(label_rows)
    quality = {
        "experiment_id": m4c.M4A_EXPERIMENT_ID,
        "counts": {
            "total_event_rows": rows,
            "indicator_ready_event_rows": rows,
            "indicator_non_ready_event_rows": 0,
            "cross_up_rows": int(context["cross_dir"].eq("cross_up").sum()),
            "cross_down_rows": int(context["cross_dir"].eq("cross_down").sum()),
        },
        "model_training_performed": False,
    }
    decision = {
        "experiment_id": m4c.M4B_EXPERIMENT_ID,
        "result_status": "rule_gate_not_supported",
        "selected_rule": None,
        "model_training_performed": False,
        "threshold_sweep_performed": False,
        "post_hoc_rule_search_performed": False,
        "runtime_or_trading_action_performed": False,
        "strategy_promotion_allowed": False,
    }
    return context, labels, quality, decision


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    context, labels, quality, decision = _artifacts()
    context_path = tmp_path / "indicator_context.csv"
    labels_path = tmp_path / "labels.csv"
    quality_path = tmp_path / "quality.json"
    decision_path = tmp_path / "m4b_decision.json"
    context.to_csv(context_path, index=False)
    labels.to_csv(labels_path, index=False)
    quality_path.write_text(json.dumps(quality), encoding="utf-8")
    decision_path.write_text(json.dumps(decision), encoding="utf-8")
    return context_path, labels_path, quality_path, decision_path


def _feature_columns() -> list[str]:
    return sorted(
        {
            column
            for definition in m4c.FEATURE_GROUPS.values()
            for key in ("numeric", "categorical")
            for column in definition[key]
        }
    )


def test_prepare_analysis_frame_builds_signal_time_features_and_is_label_invariant() -> None:
    context, labels, _, _ = _artifacts()
    full, eligible = m4c.prepare_analysis_frame(context, labels)
    assert len(full) == len(eligible) == 64
    assert set(eligible["target_value"]) == {0, 1}
    assert eligible.loc[eligible["cross_dir"].eq("cross_down"), "dir_roc_10"].notna().all()
    assert eligible.loc[eligible["cross_dir"].eq("cross_down"), "dir_stoch_k_centered"].notna().all()
    assert not any("h2" in column or "allow_trade" in column for column in _feature_columns())

    changed = labels.copy()
    changed["h2_signed_return"] = -changed["h2_signed_return"]
    changed["h2_allow_trade"] = changed["h2_signed_return"].gt(0.0).astype(int)
    _, changed_eligible = m4c.prepare_analysis_frame(context, changed)
    pd.testing.assert_frame_equal(
        eligible[_feature_columns()].reset_index(drop=True),
        changed_eligible[_feature_columns()].reset_index(drop=True),
    )


def test_walk_forward_folds_are_expanding_chronological_and_h2_purged() -> None:
    context, labels, _, _ = _artifacts()
    _, eligible = m4c.prepare_analysis_frame(context, labels)
    folds = m4c.build_walk_forward_folds(eligible)
    assert len(folds) == 4
    assert [len(fold.test_positions) for fold in folds] == [8, 8, 8, 8]
    assert [len(fold.train_positions) for fold in folds] == [32, 40, 48, 56]
    for fold in folds:
        assert fold.maximum_train_label_completion_index < fold.first_test_session_index
        assert max(fold.train_positions) < min(fold.test_positions)


def test_model_evaluation_is_deterministic_and_uses_identical_oos_rows() -> None:
    context, labels, _, _ = _artifacts()
    _, eligible = m4c.prepare_analysis_frame(context, labels)
    folds = m4c.build_walk_forward_folds(eligible)
    first, first_folds, first_coefficients = m4c.evaluate_models(eligible, folds)
    second, second_folds, second_coefficients = m4c.evaluate_models(eligible, folds)
    pd.testing.assert_frame_equal(first, second)
    pd.testing.assert_frame_equal(first_folds, second_folds)
    pd.testing.assert_frame_equal(first_coefficients, second_coefficients)
    assert len(first) == len(m4c.FEATURE_GROUP_NAMES) * 32
    assert len(m4c._oos_reference(first)) == 32
    assert first.groupby("feature_group")["source_index"].nunique().eq(32).all()


def test_permutation_control_is_seed_deterministic_and_adjusts_across_candidates() -> None:
    context, labels, _, _ = _artifacts()
    full, eligible = m4c.prepare_analysis_frame(context, labels)
    folds = m4c.build_walk_forward_folds(eligible)
    predictions, fold_metrics, _ = m4c.evaluate_models(eligible, folds)
    reference = m4c._oos_reference(predictions)
    fixed = m4c.build_fixed_rule_metrics(full, reference)
    metrics = m4c.build_model_metrics(predictions, fold_metrics, fixed)
    first = m4c.compute_permutation_control(eligible, folds, metrics, seed=319, repetitions=12)
    second = m4c.compute_permutation_control(eligible, folds, metrics, seed=319, repetitions=12)
    assert first == second
    assert first["candidate_groups"] == list(m4c.MODEL_CANDIDATES)
    for group_name in m4c.MODEL_CANDIDATES:
        candidate = first["candidates"][group_name]
        for statistic in ("roc_auc_uplift", "policy_h2_uplift"):
            adjusted = candidate[statistic]["max_stat_adjusted_p_value"]
            unadjusted = candidate[statistic]["unadjusted_p_value"]
            assert 0.0 < adjusted <= 1.0
            assert 0.0 < unadjusted <= 1.0
            assert adjusted >= unadjusted


def test_decision_selects_only_a_candidate_that_passes_every_frozen_condition() -> None:
    base = {
        "experiment_id": m4c.EXPERIMENT_ID,
        "oos_events": 32,
        "positive_rate": 0.55,
        "pr_auc": 0.70,
        "fold_prevalence_baseline_brier": 0.25,
        "accuracy": 0.70,
        "balanced_accuracy": 0.70,
        "precision": 0.70,
        "recall": 0.70,
        "f1": 0.70,
        "ece_5_bins": 0.08,
        "accepted_events": 12,
        "acceptance_rate": 0.375,
        "retained_h2_mean_signed_return": 0.010,
        "retained_h2_median_signed_return": 0.008,
        "retained_h2_win_rate": 0.70,
        "policy_h2_mean_return_per_signal": 0.0030,
        "no_gate_h2_mean_signed_return": 0.001,
        "no_gate_h2_median_signed_return": 0.0005,
        "no_gate_h2_win_rate": 0.55,
        "no_gate_h2_mean_return_per_signal": 0.001,
        "retained_h2_mean_uplift_vs_no_gate": 0.009,
        "retained_h2_median_uplift_vs_no_gate": 0.0075,
        "retained_h2_win_rate_uplift_vs_no_gate": 0.15,
        "policy_h2_uplift_vs_no_gate": 0.002,
        "positive_policy_uplift_folds": 3,
        "maximum_positive_fold_contribution_share": 0.45,
        "best_fixed_rule": "adx_di",
        "best_fixed_policy_h2_mean_return_per_signal": 0.0015,
    }
    rows = []
    direction = dict(base, feature_group="direction_only", is_candidate_model=False)
    direction.update(roc_auc=0.51, brier_score=0.24, policy_h2_mean_return_per_signal=0.0012)
    rows.append(direction)
    for group_name in m4c.MODEL_CANDIDATES:
        row = dict(base, feature_group=group_name, is_candidate_model=True)
        row.update(roc_auc=0.60, brier_score=0.20)
        if group_name != "momentum":
            row["policy_h2_mean_return_per_signal"] = 0.0011
        rows.append(row)
    metrics = pd.DataFrame(rows)
    fixed = pd.DataFrame(
        [
            {
                "rule_name": "no_gate",
                "is_candidate_rule": False,
                "accepted_events": 32,
                "acceptance_rate": 1.0,
                "policy_h2_mean_return_per_signal": 0.001,
            },
            {
                "rule_name": "adx_di",
                "is_candidate_rule": True,
                "accepted_events": 10,
                "acceptance_rate": 0.3125,
                "policy_h2_mean_return_per_signal": 0.0015,
            },
            {
                "rule_name": "adx_di_momentum",
                "is_candidate_rule": True,
                "accepted_events": 7,
                "acceptance_rate": 0.21875,
                "policy_h2_mean_return_per_signal": 0.0012,
            },
            {
                "rule_name": "moderate_trend_confirmation",
                "is_candidate_rule": True,
                "accepted_events": 7,
                "acceptance_rate": 0.21875,
                "policy_h2_mean_return_per_signal": 0.0010,
            },
        ]
    )
    permutation = {
        "candidates": {
            group_name: {
                "roc_auc_uplift": {"max_stat_adjusted_p_value": 0.05 if group_name == "momentum" else 0.5},
                "policy_h2_uplift": {"max_stat_adjusted_p_value": 0.05 if group_name == "momentum" else 0.5},
            }
            for group_name in m4c.MODEL_CANDIDATES
        }
    }
    decision = m4c.build_decision(
        metrics,
        fixed,
        permutation,
        run_id="decision-test",
        git_commit_sha="a" * 40,
    )
    assert decision["result_status"] == "technical_ml_supported_for_next_research_phase"
    assert decision["selected_feature_group"] == "momentum"
    assert decision["model_promotion_allowed"] is False
    assert decision["strategy_promotion_allowed"] is False

    permutation["candidates"]["momentum"]["policy_h2_uplift"]["max_stat_adjusted_p_value"] = 0.11
    rejected = m4c.build_decision(
        metrics,
        fixed,
        permutation,
        run_id="decision-test",
        git_commit_sha="a" * 40,
    )
    assert rejected["result_status"] == "technical_ml_not_supported"
    assert rejected["selected_feature_group"] is None


def test_run_package_writes_exact_declared_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context_path, labels_path, quality_path, decision_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "run"
    monkeypatch.setattr(m4c, "PERMUTATION_REPETITIONS", 12)
    result = m4c.run_research_package(
        indicator_context_path=context_path,
        labels_path=labels_path,
        quality_report_path=quality_path,
        m4b_decision_path=decision_path,
        output_dir=output_dir,
        run_id="m4c-unit-run",
        git_commit_sha="b" * 40,
    )
    assert sorted(path.name for path in output_dir.iterdir()) == sorted(m4c.DECLARED_OUTPUT_FILES)
    assert result["metadata"]["git_commit_sha"] == "b" * 40
    assert result["metadata"]["persistent_model_artifact_emitted"] is False
    assert result["quality_report"]["anti_leakage"]["label_or_future_fields_used_as_features"] == []
    assert result["quality_report"]["counts"]["model_metric_rows"] == len(m4c.FEATURE_GROUP_NAMES)
    assert result["decision"]["model_promotion_allowed"] is False
    assert result["decision"]["runtime_or_trading_action_performed"] is False

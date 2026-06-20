from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.moex_research.runners import usdrubf_ema_3_19_d1_rule_gate_benchmark as m4b


def _artifacts(*, good_per_year: int = 6) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    context_rows: list[dict[str, object]] = []
    label_rows: list[dict[str, object]] = []
    for index in range(36):
        year = 2022 + index // 12
        month_index = index % 12
        end = pd.Timestamp(year=year, month=month_index + 1, day=15, hour=18, minute=50)
        cross_dir = "cross_up" if index % 2 == 0 else "cross_down"
        direction = 1 if cross_dir == "cross_up" else -1
        good = month_index < good_per_year
        context_rows.append(
            {
                "instrument_id": "usdrubf",
                "end": end,
                "cross_dir": cross_dir,
                "session_index": index * 5,
                "rsi_14": 55.0 if direction == 1 else 45.0,
                "roc_10": 0.02 * direction,
                "stoch_k_14": 60.0,
                "stoch_d_3": 58.0,
                "adx_14": 27.0 if good else 18.0,
                "di_spread_14": (3.0 if good else -2.0) * direction,
                "macd_hist_12_26_9_pct": 0.01 * direction,
                "atr_14_pct": 0.02,
                "bb_percent_b_20_2": 0.70 if direction == 1 else 0.30,
                "bb_bandwidth_20_2": 0.10,
                "indicator_ready": True,
            }
        )
        h2_return = 0.03 if good else -0.01
        row: dict[str, object] = {
            "instrument_id": "usdrubf",
            "end": end,
            "cross_dir": cross_dir,
            "event_session_index": index * 5,
            "entry_session_index": index * 5 + 1,
        }
        for horizon_index, horizon in enumerate(("h1", "h2", "h3", "h5", "h10"), start=1):
            signed_return = h2_return - 0.001 * abs(horizon_index - 2)
            row[f"{horizon}_completion_index"] = index * 5 + horizon_index + 1
            row[f"{horizon}_signed_return"] = signed_return
            row[f"{horizon}_allow_trade"] = int(signed_return > 0.0)
            row[f"{horizon}_opposite_cross_before_exit"] = not good
        row.update(
            {
                "reverse_event_session_index": index * 5 + 3,
                "reverse_completion_index": index * 5 + 4,
                "holding_sessions_to_reverse_exit": 3,
                "reverse_signed_return": 0.04 if good else -0.02,
                "reverse_allow_trade": int(good),
                "reverse_label_censored": False,
            }
        )
        label_rows.append(row)
    context = pd.DataFrame(context_rows)
    labels = pd.DataFrame(label_rows)
    quality = {
        "experiment_id": m4b.SOURCE_EXPERIMENT_ID,
        "counts": {
            "total_event_rows": len(context),
            "indicator_ready_event_rows": len(context),
            "indicator_non_ready_event_rows": 0,
            "cross_up_rows": int(context["cross_dir"].eq("cross_up").sum()),
            "cross_down_rows": int(context["cross_dir"].eq("cross_down").sum()),
        },
        "model_training_performed": False,
    }
    return context, labels, quality


def _write_inputs(tmp_path: Path, *, good_per_year: int = 6) -> tuple[Path, Path, Path]:
    context, labels, quality = _artifacts(good_per_year=good_per_year)
    context_path = tmp_path / "indicator_context.csv"
    labels_path = tmp_path / "labels.csv"
    quality_path = tmp_path / "quality.json"
    context.to_csv(context_path, index=False)
    labels.to_csv(labels_path, index=False)
    quality_path.write_text(json.dumps(quality), encoding="utf-8")
    return context_path, labels_path, quality_path


def test_frozen_rules_use_directional_signal_time_fields_and_not_labels() -> None:
    context, labels, _ = _artifacts()
    frame = m4b.build_analysis_frame(context, labels)
    masks = m4b.build_rule_masks(frame)

    assert int(masks["no_gate"].sum()) == 36
    assert int(masks["adx_di"].sum()) == 18
    assert int(masks["adx_di_momentum"].sum()) == 18
    assert int(masks["moderate_trend_confirmation"].sum()) == 18
    assert frame.loc[frame["cross_dir"].eq("cross_down"), "dir_di_spread"].max() > 0.0

    changed_labels = labels.copy()
    changed_labels["h2_signed_return"] = -changed_labels["h2_signed_return"]
    changed_labels["h2_allow_trade"] = changed_labels["h2_signed_return"].gt(0.0).astype(int)
    changed_frame = m4b.build_analysis_frame(context, changed_labels)
    changed_masks = m4b.build_rule_masks(changed_frame)
    for gate_name in m4b.RULE_NAMES:
        assert masks[gate_name].equals(changed_masks[gate_name])


def test_random_gate_and_bootstrap_controls_are_seed_deterministic() -> None:
    context, labels, _ = _artifacts()
    frame = m4b.build_analysis_frame(context, labels)
    masks = m4b.build_rule_masks(frame)

    first_null = m4b.compute_random_gate_null(frame, masks, seed=319, repetitions=120)
    second_null = m4b.compute_random_gate_null(frame, masks, seed=319, repetitions=120)
    assert first_null == second_null
    assert first_null["matching"]["all_candidate_plans_feasible"] is True
    for gate_name in m4b.CANDIDATE_RULES:
        candidate = first_null["candidates"][gate_name]
        assert candidate["accepted_events"] == 18
        assert sum(candidate["stratum_counts"].values()) == 18
        assert 0.0 < candidate["h2_mean_uplift"]["max_stat_adjusted_p_value"] <= 1.0

    first_bootstrap = m4b.compute_bootstrap_intervals(
        frame, masks, seed=319, repetitions=80, confidence_level=0.90
    )
    second_bootstrap = m4b.compute_bootstrap_intervals(
        frame, masks, seed=319, repetitions=80, confidence_level=0.90
    )
    assert first_bootstrap == second_bootstrap
    assert first_bootstrap["intervals"]["no_gate"]["acceptance_rate"]["point_estimate"] == 1.0


def test_candidate_limits_block_small_apparently_strong_gate() -> None:
    context, labels, _ = _artifacts(good_per_year=2)
    frame = m4b.build_analysis_frame(context, labels)
    masks = m4b.build_rule_masks(frame)
    diagnostics = m4b.build_horizon_diagnostics(frame, masks)
    year_metrics = m4b.build_year_metrics(frame, masks)
    fake_null = {
        "candidates": {
            gate_name: {
                "h2_mean_uplift": {"max_stat_adjusted_p_value": 0.001},
                "h10_persistence_uplift": {"max_stat_adjusted_p_value": 0.001},
            }
            for gate_name in m4b.CANDIDATE_RULES
        }
    }
    decision, metrics = m4b.build_decision_and_gate_metrics(
        frame,
        masks,
        diagnostics,
        year_metrics,
        fake_null,
        run_id="unit-small-gate",
        git_commit_sha="a" * 40,
    )
    candidate_rows = metrics.loc[metrics["is_candidate_gate"].astype(bool)]
    assert set(candidate_rows["accepted_events"]) == {6}
    assert not candidate_rows["minimum_accepted_events_pass"].any()
    assert not candidate_rows["rule_supported"].any()
    assert decision["result_status"] == "rule_gate_not_supported"


def test_context_rejects_future_fields_and_serialized_readiness_mismatch() -> None:
    context, labels, _ = _artifacts()
    leaked = context.copy()
    leaked["h2_allow_trade"] = 1
    with pytest.raises(ValueError, match="future-outcome"):
        m4b.build_analysis_frame(leaked, labels)

    mismatch = context.copy()
    mismatch.loc[0, "indicator_ready"] = False
    with pytest.raises(ValueError, match="indicator_ready"):
        m4b.build_analysis_frame(mismatch, labels)


def test_run_package_writes_exact_nine_declared_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context_path, labels_path, quality_path = _write_inputs(tmp_path)
    output_dir = tmp_path / "run"
    monkeypatch.setattr(m4b, "RANDOM_REPETITIONS", 240)
    monkeypatch.setattr(m4b, "BOOTSTRAP_REPETITIONS", 160)

    result = m4b.run_research_package(
        indicator_context_path=context_path,
        labels_path=labels_path,
        quality_report_path=quality_path,
        output_dir=output_dir,
        run_id="m4b-unit-run",
        git_commit_sha="b" * 40,
    )

    assert sorted(path.name for path in output_dir.iterdir()) == sorted(m4b.DECLARED_OUTPUT_FILES)
    assert result["metadata"]["git_commit_sha"] == "b" * 40
    assert result["metadata"]["model_training_performed"] is False
    assert result["quality_report"]["anti_leakage"]["label_or_future_fields_used_in_gate"] == []
    assert result["quality_report"]["counts"]["formal_horizon_diagnostic_rows"] == 24
    assert result["decision"]["threshold_sweep_performed"] is False
    assert result["decision"]["strategy_promotion_allowed"] is False
    gate_metrics = pd.read_csv(output_dir / m4b.OUTPUT_GATE_METRICS)
    assert list(gate_metrics["gate_name"]) == list(m4b.RULE_NAMES)

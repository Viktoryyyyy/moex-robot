from __future__ import annotations

import argparse
from statistics import median
from typing import Mapping, Sequence

import pandas as pd

from src.moex_research.intelligence.usdrubf_historical_sparse_bridge import (
    build_historical_sparse_decision_input,
)
from src.moex_research.intelligence.usdrubf_intelligence_benchmark import realized_bias
from src.moex_research.intelligence.usdrubf_live_shadow_bridge import LiveShadowBridgeError
from src.moex_research.runners import usdrubf_s7_2_historical_component_benchmark as base


PROJECT = "MOEX_Bot"
MODE = "s7_2_empirical_stability_analysis"
EXPERIMENT_ID = "usdrubf_rub_intelligence_s7_2_empirical_stability_v1"

OUTPUT_RUN_METADATA = "run_metadata.json"
OUTPUT_QUALITY_REPORT = "quality_report.json"
OUTPUT_SPARSE_REPLAY = "historical_sparse_replay_rows.csv"
OUTPUT_COMPLETE_REPLAY = "historical_complete_only_replay_rows.csv"
OUTPUT_SPARSE_EXCLUSIONS = "historical_sparse_replay_exclusions.csv"
OUTPUT_COMPLETE_EXCLUSIONS = "historical_complete_only_replay_exclusions.csv"
OUTPUT_EMA_OVERALL = "ema_overall_stability.json"
OUTPUT_EMA_YEARLY = "ema_yearly_stability.csv"
OUTPUT_STRUCTURE_YEARLY = "structure_yearly_stability.csv"
OUTPUT_STRUCTURE_SUMMARY = "structure_stability_summary.csv"
OUTPUT_SENSITIVITY = "ema_sparse_vs_complete_sensitivity.json"

DECLARED_OUTPUTS = (
    OUTPUT_RUN_METADATA,
    OUTPUT_QUALITY_REPORT,
    OUTPUT_SPARSE_REPLAY,
    OUTPUT_COMPLETE_REPLAY,
    OUTPUT_SPARSE_EXCLUSIONS,
    OUTPUT_COMPLETE_EXCLUSIONS,
    OUTPUT_EMA_OVERALL,
    OUTPUT_EMA_YEARLY,
    OUTPUT_STRUCTURE_YEARLY,
    OUTPUT_STRUCTURE_SUMMARY,
    OUTPUT_SENSITIVITY,
)


def _parser() -> argparse.ArgumentParser:
    parser = base._parser()
    parser.description = (
        "Evaluate S7.2 EMA and structure stability on exact historical inputs, including "
        "post-hoc majority-class references, yearly stability, structure sample gates, "
        "and sparse-safe versus complete-only 15m sensitivity."
    )
    parser.add_argument("--min-group-sample", type=int, default=20)
    return parser


def _validate_min_group_sample(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise base.HistoricalComponentBenchmarkError("min_group_sample must be a positive integer")
    return value


def build_historical_replay_variant(
    daily: pd.DataFrame,
    intraday: pd.DataFrame,
    *,
    horizons: Sequence[int],
    ema_min_constituents: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if ema_min_constituents not in {1, 3}:
        raise base.HistoricalComponentBenchmarkError("ema_min_constituents must be 1 or 3")
    if len(daily) < 2:
        raise base.HistoricalComponentBenchmarkError("at least two complete trading days are required")

    daily = daily.copy().reset_index(drop=True)
    daily["trade_date"] = pd.to_datetime(daily["end"]).dt.normalize()
    grouped = {
        pd.Timestamp(key): group.reset_index(drop=True)
        for key, group in intraday.groupby("trade_date", sort=True)
    }

    rows: list[dict[str, object]] = []
    exclusion_rows: list[dict[str, object]] = []
    for index in range(1, len(daily)):
        current_date = pd.Timestamp(daily.loc[index, "trade_date"])
        prior_date = pd.Timestamp(daily.loc[index - 1, "trade_date"])
        current_frame = grouped.get(current_date)
        prior_frame = grouped.get(prior_date)
        if current_frame is None or prior_frame is None:
            raise base.HistoricalComponentBenchmarkError(
                "complete daily date is missing its intraday session"
            )

        current_bars = base._aware_bars(current_frame)
        prior_bars = base._aware_bars(prior_frame)
        wall_clock = current_bars[-1]["end"]
        try:
            decision_input = build_historical_sparse_decision_input(
                current_session_bars=current_bars,
                prior_session_bars=prior_bars,
                wall_clock_as_of=wall_clock,
                ema_min_constituents=ema_min_constituents,
            )
        except LiveShadowBridgeError as exc:
            exclusion_rows.append(
                {
                    "trade_date": current_date.date().isoformat(),
                    "prior_trade_date": prior_date.date().isoformat(),
                    "error_type": type(exc).__name__,
                    "reason": str(exc),
                    "current_bar_count": len(current_bars),
                    "prior_bar_count": len(prior_bars),
                }
            )
            continue

        high_state, high_quality = base._interaction_state(
            decision_input, "PREVIOUS_SESSION_HIGH"
        )
        low_state, low_quality = base._interaction_state(
            decision_input, "PREVIOUS_SESSION_LOW"
        )
        ema_details = dict(decision_input.ema_3_19_ai.details or {})
        row: dict[str, object] = {
            "trade_date": current_date.date().isoformat(),
            "prior_trade_date": prior_date.date().isoformat(),
            "as_of_timestamp": decision_input.as_of_timestamp.isoformat(),
            "price": float(decision_input.price),
            "trend": decision_input.trend,
            "market_regime": decision_input.market_regime,
            "ema_direction": decision_input.ema_3_19_ai.direction,
            "ema_confidence": float(decision_input.ema_3_19_ai.confidence),
            "ema_bar_count": int(ema_details.get("bar_count", 0)),
            "ema_sparse_bucket_count": int(ema_details.get("sparse_bucket_count", 0)),
            "ema_min_constituent_count": int(ema_details.get("min_constituent_count", 0)),
            "ema_min_required_constituents": int(
                ema_details.get("min_required_constituents", ema_min_constituents)
            ),
            "previous_session_high_state": high_state,
            "previous_session_high_quality": high_quality,
            "previous_session_low_state": low_state,
            "previous_session_low_quality": low_quality,
            "structure_signature": f"HIGH:{high_state}|LOW:{low_state}",
        }
        for horizon in horizons:
            future_index = index + horizon
            row[f"future_price_h{horizon}"] = (
                None
                if future_index >= len(daily)
                else float(daily.loc[future_index, "close"])
            )
        rows.append(row)

    replay = pd.DataFrame(rows)
    exclusions = pd.DataFrame(exclusion_rows, columns=base._EXCLUSION_COLUMNS)
    if replay.empty:
        raise base.HistoricalComponentBenchmarkError(
            "historical stability replay produced zero eligible rows; "
            f"ema_min_constituents={ema_min_constituents}; excluded_days={len(exclusions)}"
        )
    if replay["as_of_timestamp"].duplicated().any():
        raise base.HistoricalComponentBenchmarkError(
            "historical stability replay produced duplicate as_of_timestamp values"
        )
    return replay, exclusions


def _eligible_horizon_rows(frame: pd.DataFrame, horizon: int) -> pd.DataFrame:
    column = f"future_price_h{horizon}"
    if column not in frame.columns:
        raise base.HistoricalComponentBenchmarkError(f"missing future label column {column}")
    return frame[frame[column].notna()].copy().reset_index(drop=True)


def _ema_slice_metrics(
    frame: pd.DataFrame,
    *,
    horizon: int,
    neutral_band_bps: float,
) -> dict[str, object]:
    eligible = _eligible_horizon_rows(frame, horizon)
    realized_counts = {"BEARISH_USD": 0, "BULLISH_USD": 0, "NEUTRAL": 0}
    prediction_counts = {"BEARISH_USD": 0, "BULLISH_USD": 0, "NEUTRAL": 0}
    correct = 0
    signed_returns: list[float] = []

    for row in eligible.itertuples(index=False):
        start = float(row.price)
        future = float(getattr(row, f"future_price_h{horizon}"))
        outcome = realized_bias(
            start_price=start,
            future_price=future,
            neutral_band_bps=neutral_band_bps,
        )
        prediction = str(row.ema_direction)
        if prediction not in prediction_counts:
            raise base.HistoricalComponentBenchmarkError("unexpected EMA direction")
        realized_counts[outcome] += 1
        prediction_counts[prediction] += 1
        correct += int(prediction == outcome)
        return_bps = (future / start - 1.0) * 10_000.0
        if prediction == "BULLISH_USD":
            signed_returns.append(return_bps)
        elif prediction == "BEARISH_USD":
            signed_returns.append(-return_bps)

    count = len(eligible)
    if count == 0:
        return {
            "eligible_count": 0,
            "ema_accuracy": None,
            "majority_class": None,
            "majority_accuracy": None,
            "ema_accuracy_minus_majority_accuracy": None,
            "mean_signed_return_bps": None,
            "directional_prediction_count": 0,
            "prediction_distribution": prediction_counts,
            "realized_distribution": realized_counts,
        }

    majority_class, majority_count = sorted(
        realized_counts.items(), key=lambda item: (-item[1], item[0])
    )[0]
    ema_accuracy = correct / count
    majority_accuracy = majority_count / count
    return {
        "eligible_count": count,
        "ema_accuracy": ema_accuracy,
        "majority_class": majority_class,
        "majority_accuracy": majority_accuracy,
        "ema_accuracy_minus_majority_accuracy": ema_accuracy - majority_accuracy,
        "mean_signed_return_bps": (
            None if not signed_returns else sum(signed_returns) / len(signed_returns)
        ),
        "directional_prediction_count": len(signed_returns),
        "prediction_distribution": prediction_counts,
        "realized_distribution": realized_counts,
    }


def build_ema_stability(
    replay: pd.DataFrame,
    *,
    horizons: Sequence[int],
    neutral_band_bps: float,
) -> tuple[dict[str, object], pd.DataFrame]:
    work = replay.copy()
    work["year"] = pd.to_datetime(work["trade_date"]).dt.year.astype(int)

    overall = {
        "reference_semantics": (
            "majority_class is a post-hoc descriptive reference on the same realized labels; "
            "it is not a causal deployable predictor"
        ),
        "by_horizon": {},
    }
    yearly_rows: list[dict[str, object]] = []
    for horizon in horizons:
        overall["by_horizon"][str(horizon)] = _ema_slice_metrics(
            work,
            horizon=horizon,
            neutral_band_bps=neutral_band_bps,
        )
        for year, group in work.groupby("year", sort=True):
            metrics = _ema_slice_metrics(
                group,
                horizon=horizon,
                neutral_band_bps=neutral_band_bps,
            )
            yearly_rows.append({"year": int(year), "horizon": int(horizon), **metrics})
    return overall, pd.DataFrame(yearly_rows)


def build_structure_yearly_stability(
    replay: pd.DataFrame,
    *,
    horizons: Sequence[int],
    neutral_band_bps: float,
    min_group_sample: int,
) -> pd.DataFrame:
    threshold = _validate_min_group_sample(min_group_sample)
    work = replay.copy()
    work["year"] = pd.to_datetime(work["trade_date"]).dt.year.astype(int)
    rows: list[dict[str, object]] = []
    for grouping_field in ("market_regime", "structure_signature"):
        for horizon in horizons:
            eligible = _eligible_horizon_rows(work, horizon)
            future_column = f"future_price_h{horizon}"
            for (year, group_value), group in eligible.groupby(
                ["year", grouping_field], sort=True
            ):
                returns: list[float] = []
                realized_counts = {"BEARISH_USD": 0, "BULLISH_USD": 0, "NEUTRAL": 0}
                for item in group.itertuples(index=False):
                    start = float(item.price)
                    future = float(getattr(item, future_column))
                    return_bps = (future / start - 1.0) * 10_000.0
                    returns.append(return_bps)
                    realized_counts[
                        realized_bias(
                            start_price=start,
                            future_price=future,
                            neutral_band_bps=neutral_band_bps,
                        )
                    ] += 1
                count = len(returns)
                rows.append(
                    {
                        "grouping_field": grouping_field,
                        "group_value": str(group_value),
                        "year": int(year),
                        "horizon": int(horizon),
                        "count": count,
                        "min_group_sample": threshold,
                        "sample_gate_pass": bool(count >= threshold),
                        "mean_return_bps": sum(returns) / count,
                        "median_return_bps": median(returns),
                        "bullish_rate": realized_counts["BULLISH_USD"] / count,
                        "bearish_rate": realized_counts["BEARISH_USD"] / count,
                        "neutral_rate": realized_counts["NEUTRAL"] / count,
                    }
                )
    return pd.DataFrame(rows)


def build_structure_stability_summary(yearly: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (field, value, horizon), group in yearly.groupby(
        ["grouping_field", "group_value", "horizon"], sort=True
    ):
        passing = group[group["sample_gate_pass"] == True]  # noqa: E712
        bullish_years = int((passing["bullish_rate"] > passing["bearish_rate"]).sum())
        bearish_years = int((passing["bearish_rate"] > passing["bullish_rate"]).sum())
        tied_years = int(len(passing) - bullish_years - bearish_years)
        if len(passing) == 0:
            consistency = "INSUFFICIENT_SAMPLE"
        elif bullish_years == len(passing):
            consistency = "BULLISH_EVERY_PASSING_YEAR"
        elif bearish_years == len(passing):
            consistency = "BEARISH_EVERY_PASSING_YEAR"
        else:
            consistency = "MIXED"
        rows.append(
            {
                "grouping_field": str(field),
                "group_value": str(value),
                "horizon": int(horizon),
                "years_observed": int(group["year"].nunique()),
                "years_passing_sample_gate": int(len(passing)),
                "bullish_passing_years": bullish_years,
                "bearish_passing_years": bearish_years,
                "tied_passing_years": tied_years,
                "directional_consistency": consistency,
                "min_passing_year_count": (
                    None if passing.empty else int(passing["count"].min())
                ),
                "max_passing_year_count": (
                    None if passing.empty else int(passing["count"].max())
                ),
            }
        )
    return pd.DataFrame(rows)


def build_sparse_complete_sensitivity(
    sparse_replay: pd.DataFrame,
    complete_replay: pd.DataFrame,
    *,
    horizons: Sequence[int],
    neutral_band_bps: float,
    candidate_days: int,
    sparse_excluded_days: int,
    complete_excluded_days: int,
) -> dict[str, object]:
    merged = sparse_replay[["trade_date", "ema_direction"]].merge(
        complete_replay[["trade_date", "ema_direction"]],
        on="trade_date",
        how="inner",
        suffixes=("_sparse", "_complete"),
        validate="one_to_one",
    )
    match_count = int(
        (merged["ema_direction_sparse"] == merged["ema_direction_complete"]).sum()
    )
    common_count = len(merged)
    result: dict[str, object] = {
        "candidate_days": int(candidate_days),
        "sparse_replay_rows": int(len(sparse_replay)),
        "complete_only_replay_rows": int(len(complete_replay)),
        "sparse_excluded_days": int(sparse_excluded_days),
        "complete_only_excluded_days": int(complete_excluded_days),
        "sparse_coverage": None if candidate_days == 0 else len(sparse_replay) / candidate_days,
        "complete_only_coverage": (
            None if candidate_days == 0 else len(complete_replay) / candidate_days
        ),
        "common_trade_dates": common_count,
        "ema_direction_match_count": match_count,
        "ema_direction_match_rate": None if common_count == 0 else match_count / common_count,
        "ema_direction_changed_days": common_count - match_count,
        "by_horizon": {},
    }
    by_horizon: dict[str, object] = {}
    for horizon in horizons:
        sparse_metrics = _ema_slice_metrics(
            sparse_replay,
            horizon=horizon,
            neutral_band_bps=neutral_band_bps,
        )
        complete_metrics = _ema_slice_metrics(
            complete_replay,
            horizon=horizon,
            neutral_band_bps=neutral_band_bps,
        )
        sparse_acc = sparse_metrics["ema_accuracy"]
        complete_acc = complete_metrics["ema_accuracy"]
        sparse_return = sparse_metrics["mean_signed_return_bps"]
        complete_return = complete_metrics["mean_signed_return_bps"]
        by_horizon[str(horizon)] = {
            "sparse": sparse_metrics,
            "complete_only": complete_metrics,
            "complete_minus_sparse_accuracy": (
                None
                if sparse_acc is None or complete_acc is None
                else float(complete_acc) - float(sparse_acc)
            ),
            "complete_minus_sparse_mean_signed_return_bps": (
                None
                if sparse_return is None or complete_return is None
                else float(complete_return) - float(sparse_return)
            ),
        }
    result["by_horizon"] = by_horizon
    return result


def run(args: argparse.Namespace) -> Mapping[str, object]:
    source_mode, source, output_dir, run_id, start, end, horizons = base._validate_cli(args)
    min_group_sample = _validate_min_group_sample(args.min_group_sample)
    output_dir.mkdir(parents=True, exist_ok=True)
    root = output_dir.resolve()
    paths = {name: root / name for name in DECLARED_OUTPUTS}
    if any(path.resolve().parent != root for path in paths.values()):
        raise base.HistoricalComponentBenchmarkError("declared output escaped output_dir")

    if source_mode == "phase3_panel_manifest":
        daily, intraday, provenance = base._phase3_manifest_source(source)
    else:
        daily, intraday = base._complete_daily_and_intraday(source)
        provenance = {
            "source_mode": "explicit_csv",
            "source_dataset_path": str(source),
            "source_dataset_sha256": base._sha256(source),
            "directory_scan_used": False,
        }

    sparse_full, sparse_exclusions_full = build_historical_replay_variant(
        daily,
        intraday,
        horizons=horizons,
        ema_min_constituents=1,
    )
    complete_full, complete_exclusions_full = build_historical_replay_variant(
        daily,
        intraday,
        horizons=horizons,
        ema_min_constituents=3,
    )
    sparse = base._filter_prediction_rows(sparse_full, start=start, end=end)
    complete = base._filter_prediction_rows(complete_full, start=start, end=end)
    sparse_exclusions = base._filter_exclusion_rows(
        sparse_exclusions_full, start=start, end=end
    )
    complete_exclusions = base._filter_exclusion_rows(
        complete_exclusions_full, start=start, end=end
    )

    ema_overall, ema_yearly = build_ema_stability(
        sparse,
        horizons=horizons,
        neutral_band_bps=float(args.neutral_band_bps),
    )
    structure_yearly = build_structure_yearly_stability(
        sparse,
        horizons=horizons,
        neutral_band_bps=float(args.neutral_band_bps),
        min_group_sample=min_group_sample,
    )
    structure_summary = build_structure_stability_summary(structure_yearly)
    candidate_days = max(int(len(daily)) - 1, 0)
    sensitivity = build_sparse_complete_sensitivity(
        sparse,
        complete,
        horizons=horizons,
        neutral_band_bps=float(args.neutral_band_bps),
        candidate_days=candidate_days,
        sparse_excluded_days=len(sparse_exclusions_full),
        complete_excluded_days=len(complete_exclusions_full),
    )

    sparse.to_csv(paths[OUTPUT_SPARSE_REPLAY], index=False)
    complete.to_csv(paths[OUTPUT_COMPLETE_REPLAY], index=False)
    sparse_exclusions.to_csv(paths[OUTPUT_SPARSE_EXCLUSIONS], index=False)
    complete_exclusions.to_csv(paths[OUTPUT_COMPLETE_EXCLUSIONS], index=False)
    ema_yearly.to_csv(paths[OUTPUT_EMA_YEARLY], index=False)
    structure_yearly.to_csv(paths[OUTPUT_STRUCTURE_YEARLY], index=False)
    structure_summary.to_csv(paths[OUTPUT_STRUCTURE_SUMMARY], index=False)
    base._write_json(paths[OUTPUT_EMA_OVERALL], ema_overall)
    base._write_json(paths[OUTPUT_SENSITIVITY], sensitivity)

    run_metadata = {
        "project": PROJECT,
        "mode": MODE,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "created_at": base._utc_now_iso(),
        "source_provenance": dict(provenance),
        "output_dir": str(root),
        "start_date": None if start is None else start.date().isoformat(),
        "end_date": None if end is None else end.date().isoformat(),
        "horizons": list(horizons),
        "neutral_band_bps": float(args.neutral_band_bps),
        "high_confidence_threshold": float(args.high_confidence_threshold),
        "min_group_sample": min_group_sample,
        "declared_outputs": list(DECLARED_OUTPUTS),
        "majority_reference_post_hoc_only": True,
        "historical_sparse_min_constituents": 1,
        "historical_complete_only_min_constituents": 3,
        "missing_5m_imputation": False,
        "live_bridge_runtime_changed": False,
        "full_decision_agent_evaluated": False,
    }
    base._write_json(paths[OUTPUT_RUN_METADATA], run_metadata)

    quality_report = {
        "project": PROJECT,
        "mode": MODE,
        "run_id": run_id,
        "source_mode": source_mode,
        "candidate_prediction_days": candidate_days,
        "sparse_replay_rows": int(len(sparse_full)),
        "sparse_excluded_days": int(len(sparse_exclusions_full)),
        "sparse_coverage": None if candidate_days == 0 else len(sparse_full) / candidate_days,
        "complete_only_replay_rows": int(len(complete_full)),
        "complete_only_excluded_days": int(len(complete_exclusions_full)),
        "complete_only_coverage": (
            None if candidate_days == 0 else len(complete_full) / candidate_days
        ),
        "prediction_rows_after_filter": int(len(sparse)),
        "first_prediction_trade_date": str(sparse.iloc[0]["trade_date"]),
        "last_prediction_trade_date": str(sparse.iloc[-1]["trade_date"]),
        "future_labels_post_hoc_only": True,
        "majority_reference_post_hoc_only": True,
        "decision_input_future_data_used": False,
        "historical_missing_5m_imputed": False,
        "historical_bars_synthesized": False,
        "historical_timestamps_shifted": False,
        "complete_only_incomplete_bucket_policy": "DROP_BUCKET_NO_REPAIR",
        "live_bridge_runtime_semantics_relaxed": False,
        "structure_directional_rule_invented": False,
        "full_decision_agent_evaluated": False,
        "full_decision_agent_blocker": (
            "operational scheduler remains SAFE_WAIT / no frozen non-SAFE_WAIT production policy"
        ),
        "server_runtime_modified": False,
        "broker_order_execution": False,
    }
    base._write_json(paths[OUTPUT_QUALITY_REPORT], quality_report)

    h1 = ema_overall["by_horizon"].get("1", {})
    return {
        "project": PROJECT,
        "mode": MODE,
        "status": "COMPLETED",
        "run_id": run_id,
        "source_mode": source_mode,
        "sparse_rows": int(len(sparse)),
        "complete_only_rows": int(len(complete)),
        "sparse_coverage": quality_report["sparse_coverage"],
        "complete_only_coverage": quality_report["complete_only_coverage"],
        "h1_ema_accuracy_minus_majority": h1.get(
            "ema_accuracy_minus_majority_accuracy"
        ),
        "full_decision_agent_evaluated": False,
        "output_dir": str(root),
    }


def main() -> int:
    args = _parser().parse_args()
    result = run(args)
    for key, value in result.items():
        print(f"{str(key).upper()}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

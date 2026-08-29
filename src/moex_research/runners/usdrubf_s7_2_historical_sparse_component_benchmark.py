from __future__ import annotations

import argparse
from typing import Mapping, Sequence

import pandas as pd

from src.moex_research.intelligence.usdrubf_historical_sparse_bridge import (
    HISTORICAL_SPARSE_15M_SOURCE,
    build_historical_sparse_decision_input,
)
from src.moex_research.intelligence.usdrubf_live_shadow_bridge import LiveShadowBridgeError
from src.moex_research.intelligence.usdrubf_intelligence_benchmark import evaluate_intelligence_quality
from src.moex_research.runners import usdrubf_s7_2_historical_component_benchmark as base


PROJECT = "MOEX_Bot"
MODE = "s7_2_historical_sparse_component_benchmark"
EXPERIMENT_ID = "usdrubf_rub_intelligence_s7_2_historical_sparse_component_benchmark_v1"


def _parser() -> argparse.ArgumentParser:
    parser = base._parser()
    parser.description = (
        "Replay historical USDRUBF native 5m data with sparse-safe historical-only "
        "15m EMA aggregation while preserving current live fail-closed semantics."
    )
    return parser


def build_historical_sparse_replay_with_exclusions(
    daily: pd.DataFrame,
    intraday: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if len(daily) < 2:
        raise base.HistoricalComponentBenchmarkError(
            "at least two complete trading days are required"
        )

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
            "historical sparse replay produced zero eligible rows; "
            f"excluded_days={len(exclusions)}"
        )
    if replay["as_of_timestamp"].duplicated().any():
        raise base.HistoricalComponentBenchmarkError(
            "historical sparse replay produced duplicate as_of_timestamp values"
        )
    return replay, exclusions


def run(args: argparse.Namespace) -> Mapping[str, object]:
    source_mode, source, output_dir, run_id, start, end, horizons = base._validate_cli(args)
    output_dir.mkdir(parents=True, exist_ok=True)
    root = output_dir.resolve()
    paths = {name: root / name for name in base.DECLARED_OUTPUTS}
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

    full_replay, full_exclusions = build_historical_sparse_replay_with_exclusions(
        daily,
        intraday,
        horizons=horizons,
    )
    replay = base._filter_prediction_rows(full_replay, start=start, end=end)
    exclusions = base._filter_exclusion_rows(full_exclusions, start=start, end=end)

    bias_only = evaluate_intelligence_quality(
        base._benchmark_observations(replay, horizons=horizons, always_active=False),
        horizons=horizons,
        neutral_band_bps=float(args.neutral_band_bps),
        high_confidence_threshold=float(args.high_confidence_threshold),
    )
    always_active = evaluate_intelligence_quality(
        base._benchmark_observations(replay, horizons=horizons, always_active=True),
        horizons=horizons,
        neutral_band_bps=float(args.neutral_band_bps),
        high_confidence_threshold=float(args.high_confidence_threshold),
    )
    structure_summary = base.build_structure_forward_summary(
        replay,
        horizons=horizons,
        neutral_band_bps=float(args.neutral_band_bps),
    )

    replay.to_csv(paths[base.OUTPUT_REPLAY_ROWS], index=False)
    exclusions.to_csv(paths[base.OUTPUT_REPLAY_EXCLUSIONS], index=False)
    structure_summary.to_csv(paths[base.OUTPUT_STRUCTURE_SUMMARY], index=False)
    base._write_json(paths[base.OUTPUT_EMA_BIAS_ONLY], bias_only)
    base._write_json(paths[base.OUTPUT_EMA_ALWAYS_ACTIVE], always_active)

    candidate_days = max(int(len(daily)) - 1, 0)
    historical_coverage = None if candidate_days == 0 else float(len(full_replay) / candidate_days)
    exclusion_reasons = (
        {}
        if full_exclusions.empty
        else {
            str(key): int(value)
            for key, value in full_exclusions["reason"].value_counts().items()
        }
    )

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
        "declared_outputs": list(base.DECLARED_OUTPUTS),
        "historical_ema_replay_policy": "SPARSE_NATIVE_5M_NO_IMPUTATION",
        "historical_ema_source": HISTORICAL_SPARSE_15M_SOURCE,
        "live_bridge_runtime_changed": False,
        "full_decision_agent_evaluated": False,
        "full_decision_agent_blocker": (
            "operational scheduler is pinned to SAFE_WAIT; no frozen non-SAFE_WAIT "
            "production decision policy exists"
        ),
    }
    base._write_json(paths[base.OUTPUT_RUN_METADATA], run_metadata)

    quality_report = {
        "project": PROJECT,
        "mode": MODE,
        "run_id": run_id,
        "source_mode": source_mode,
        "complete_daily_rows": int(len(daily)),
        "candidate_prediction_days": candidate_days,
        "historical_sparse_eligible_prediction_days": int(len(full_replay)),
        "historical_sparse_excluded_prediction_days": int(len(full_exclusions)),
        "historical_sparse_coverage": historical_coverage,
        "historical_sparse_exclusion_reasons": exclusion_reasons,
        "prediction_window_excluded_days": int(len(exclusions)),
        "full_replay_rows_before_prediction_filter": int(len(full_replay)),
        "prediction_rows_after_filter": int(len(replay)),
        "first_prediction_trade_date": str(replay.iloc[0]["trade_date"]),
        "last_prediction_trade_date": str(replay.iloc[-1]["trade_date"]),
        "prior_context_preserved_before_start_date": True,
        "post_end_rows_used_only_for_forward_labels": True,
        "future_labels_post_hoc_only": True,
        "decision_input_future_data_used": False,
        "historical_sparse_15m_enabled": True,
        "historical_missing_5m_imputed": False,
        "historical_bars_synthesized": False,
        "historical_timestamps_shifted": False,
        "live_bridge_runtime_semantics_relaxed": False,
        "futoi_authority": "BLOCKED/EXCLUDED",
        "news_authority": "EXCLUDED_FROM_HISTORICAL_COMPONENT_REPLAY",
        "macro_authority": "EXCLUDED_FROM_HISTORICAL_COMPONENT_REPLAY",
        "decision_agent": "NOT_EVALUATED",
        "ema_component": "HISTORICAL_SPARSE_15M_USING_CURRENT_EMA_STATE_ENGINE",
        "ema_confidence_semantics": "CURRENT_BRIDGE_FIXED_1_0_WHEN_AVAILABLE",
        "structure_component": "CURRENT_PREVIOUS_SESSION_LEVEL_ENGINE_REPLAYED_ON_NATIVE_5M",
        "structure_directional_rule_invented": False,
        "server_runtime_modified": False,
        "broker_order_execution": False,
    }
    base._write_json(paths[base.OUTPUT_QUALITY_REPORT], quality_report)

    return {
        "project": PROJECT,
        "mode": MODE,
        "status": "COMPLETED",
        "run_id": run_id,
        "source_mode": source_mode,
        "replay_rows": int(len(replay)),
        "excluded_rows": int(len(exclusions)),
        "historical_sparse_coverage": historical_coverage,
        "structure_summary_rows": int(len(structure_summary)),
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

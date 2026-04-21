from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

import pandas as pd

from src.moex_core.contracts.registry_loader import load_registered_backtest
from src.moex_research.publishers.backtest_research_result_publisher import publish_backtest_research_result
from src.moex_strategy_sdk.errors import InterfaceValidationError, StrategyRegistrationError


DEFAULT_COMMISSION_POINTS = 2.0


def _resolve_external_pattern_path(
    *,
    locator_ref: str,
    environment_record: Mapping[str, object],
    format_kwargs: Mapping[str, object],
) -> Path:
    if not isinstance(locator_ref, str) or not locator_ref:
        raise StrategyRegistrationError("locator_ref is required")
    artifact_root_refs = environment_record.get("artifact_root_refs")
    if not isinstance(artifact_root_refs, list) or len(artifact_root_refs) != 1:
        raise StrategyRegistrationError("wave-2 requires exactly one artifact_root_ref")
    artifact_root_key = artifact_root_refs[0]
    if not isinstance(artifact_root_key, str) or not artifact_root_key:
        raise StrategyRegistrationError("invalid artifact_root_ref")
    artifact_root = os.environ.get(artifact_root_key)
    if not artifact_root:
        raise StrategyRegistrationError("missing required artifact root env var: " + artifact_root_key)
    relative_path = locator_ref.format(**format_kwargs)
    return Path(artifact_root) / relative_path


def _to_strategy_inputs(feature_frame: pd.DataFrame) -> tuple[dict[str, object], ...]:
    return tuple(dict(row) for row in feature_frame.to_dict(orient="records"))


def _validate_bar_frame(feature_frame: pd.DataFrame) -> pd.DataFrame:
    required = ["instrument_id", "end", "open", "close"]
    missing = [column for column in required if column not in feature_frame.columns]
    if missing:
        raise InterfaceValidationError("feature frame missing required columns: " + ", ".join(missing))

    work = feature_frame.copy()
    work["end"] = pd.to_datetime(work["end"], errors="coerce")
    if work["end"].isna().any():
        raise InterfaceValidationError("feature frame contains invalid end timestamp")
    if getattr(work["end"].dt, "tz", None) is None:
        raise InterfaceValidationError("feature frame end timestamps must be timezone-aware")
    if work["end"].duplicated().any():
        raise InterfaceValidationError("feature frame timestamps must be unique")
    if not work["end"].is_monotonic_increasing:
        raise InterfaceValidationError("feature frame timestamps must be strictly increasing")

    for column in ["open", "close"]:
        work[column] = pd.to_numeric(work[column], errors="coerce")
        if work[column].isna().any():
            raise InterfaceValidationError("feature frame contains invalid " + column)
    return work.reset_index(drop=True)


def _execute_canonical_backtest(
    *,
    feature_frame: pd.DataFrame,
    normalized_signals: tuple[dict[str, object], ...],
    commission_points: float,
) -> pd.DataFrame:
    bars = _validate_bar_frame(feature_frame)
    signal_by_ts: dict[pd.Timestamp, float] = {}
    bar_end_set = {pd.Timestamp(value) for value in bars["end"]}
    final_bar_end = pd.Timestamp(bars.iloc[-1]["end"])

    previous_signal_ts: pd.Timestamp | None = None
    for signal in normalized_signals:
        decision_ts = pd.Timestamp(signal["decision_ts"])
        if previous_signal_ts is not None and decision_ts <= previous_signal_ts:
            raise InterfaceValidationError("normalized signals must be strictly increasing")
        if decision_ts not in bar_end_set:
            raise InterfaceValidationError("signal decision_ts must match a finalized feature bar")
        if decision_ts == final_bar_end:
            raise InterfaceValidationError("signal on final bar has no valid next-bar execution point")
        signal_by_ts[decision_ts] = float(signal["desired_position"])
        previous_signal_ts = decision_ts

    current_position = 0.0
    previous_bar_end: pd.Timestamp | None = None
    bar_rows: list[dict[str, object]] = []

    for index in range(len(bars)):
        row = bars.iloc[index]
        trades = 0.0

        if previous_bar_end is not None and previous_bar_end in signal_by_ts:
            next_position = float(signal_by_ts[previous_bar_end])
            trades = abs(next_position - current_position)
            current_position = next_position

        next_open = None
        if index + 1 < len(bars):
            next_open = float(bars.iloc[index + 1]["open"])

        terminal_fee = 0.0
        if next_open is None:
            terminal_fee = abs(current_position) * float(commission_points)

        pnl_bar = (
            current_position * ((next_open - float(row["open"])) if next_open is not None else (float(row["close"]) - float(row["open"])))
            - (trades * float(commission_points))
            - terminal_fee
        )

        trade_session_date = (pd.Timestamp(row["end"]) - pd.Timedelta("1ns")).date().isoformat()
        bar_rows.append(
            {
                "trade_session_date": trade_session_date,
                "pnl_bar": float(pnl_bar),
                "trades": float(trades),
            }
        )
        previous_bar_end = pd.Timestamp(row["end"])

    day_metrics = pd.DataFrame(bar_rows)
    out = (
        day_metrics.groupby("trade_session_date", as_index=False)
        .agg(pnl_day=("pnl_bar", "sum"), num_trades_day=("trades", "sum"))
        .sort_values("trade_session_date")
        .reset_index(drop=True)
    )
    out["cum_pnl_day"] = out["pnl_day"].cumsum()
    out["dd_day"] = out["cum_pnl_day"] - out["cum_pnl_day"].cummax()
    out["max_dd_day"] = out["dd_day"].mul(-1.0)
    out["EMA_EDGE_DAY"] = (out["pnl_day"] > 0.0).astype(int)
    out = out.rename(columns={"trade_session_date": "date"})
    return out[["date", "pnl_day", "max_dd_day", "num_trades_day", "EMA_EDGE_DAY"]].copy()


def run_registered_backtest(*, strategy_id: str, portfolio_id: str, environment_id: str, run_id: str | None = None) -> dict[str, object]:
    resolved = load_registered_backtest(
        strategy_id=strategy_id,
        portfolio_id=portfolio_id,
        environment_id=environment_id,
    )

    if run_id is None:
        run_id = strategy_id + "__" + portfolio_id + "__" + environment_id

    dataset_path = _resolve_external_pattern_path(
        locator_ref=str(resolved.dataset_contract["locator_ref"]),
        environment_record=resolved.environment_record,
        format_kwargs={"run_id": run_id},
    )

    feature_frame = resolved.backtest_feature_builder(
        dataset_artifact_path=dataset_path,
        instrument_id=str(resolved.instrument_record["instrument_id"]),
        timezone_name=str(resolved.instrument_record["timezone"]),
    )
    inputs = _to_strategy_inputs(feature_frame)
    signals = resolved.backtest_signal_builder(inputs=inputs, config=resolved.strategy_config)
    backtest_request = resolved.backtest_request_builder(
        inputs=inputs,
        signals=signals,
        config=resolved.strategy_config,
    )
    day_metrics = _execute_canonical_backtest(
        feature_frame=feature_frame,
        normalized_signals=backtest_request.normalized_signals,
        commission_points=float(DEFAULT_COMMISSION_POINTS),
    )

    output_path = _resolve_external_pattern_path(
        locator_ref=resolved.backtest_output_contract.locator_ref,
        environment_record=resolved.environment_record,
        format_kwargs={"run_id": run_id},
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    day_metrics.to_csv(output_path, index=False)

    publication_refs = publish_backtest_research_result(
        run_id=run_id,
        strategy_id=strategy_id,
        portfolio_id=portfolio_id,
        environment_id=environment_id,
        resolved=resolved,
        dataset_path=dataset_path,
        primary_result_path=output_path,
        day_metrics=day_metrics,
    )

    return {
        "strategy_id": strategy_id,
        "portfolio_id": portfolio_id,
        "environment_id": environment_id,
        "run_id": run_id,
        "dataset_path": str(dataset_path),
        "output_path": str(output_path),
        "rows": int(len(day_metrics)),
        "signal_count": int(len(backtest_request.normalized_signals)),
        **publication_refs,
    }

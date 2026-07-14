from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

SOURCE_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "trade_date", "instrument_id", "open", "high", "low", "close",
    "volume", "value", "num_trades",
)
IDENTITY_COLUMNS: Final[tuple[str, ...]] = ("target_trade_date", "target_instrument_id")
PHASE6_IDENTITY_AND_TARGET_COLUMNS: Final[tuple[str, ...]] = (
    "target_phase_label", "target_is_labeled", "target_source",
    "target_trade_date", "target_instrument_id", "prior_trade_date",
)
PHASE6_LEGACY_NUMERIC_COLUMNS: Final[tuple[str, ...]] = (
    "session_index", "days_since_prior_trade_date", "lag1_close_return_1d",
    "lag1_intraday_return", "rolling_past_return_mean", "rolling_past_return_std",
    "lag1_hl_range_pct", "rolling_past_hl_range_mean",
    "rolling_past_hl_range_std", "lag1_volume", "lag1_value",
    "lag1_num_trades", "rolling_past_volume_mean", "lag1_ema_3",
    "lag1_ema_19", "lag1_ema_3_19_spread",
)
PHASE6_LEGACY_CATEGORICAL_COLUMNS: Final[tuple[str, ...]] = ("lag1_ema_3_19_state",)
RETAINED_NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    "lag1_close_return_1d", "lag1_intraday_return",
    "rolling_past_return_mean", "lag1_hl_range_pct",
)
RETAINED_CATEGORICAL_FEATURES: Final[tuple[str, ...]] = ("lag1_ema_3_19_state",)
NEW_NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    "lag1_intersession_gap_days", "lag1_ema_3_19_spread_pct",
    "lag1_close_ema_19_distance_pct", "lag1_ema_3_slope_5_pct",
    "lag1_ema_19_slope_5_pct", "lag1_ema_3_19_spread_pct_change_5",
    "lag1_ema_3_19_state_run_length_log", "rolling_past_return_std_5",
    "rolling_past_return_std_20", "rolling_return_std_ratio_5_20",
    "lag1_hl_range_to_prior20_mean", "rolling_hl_range_mean_ratio_5_20",
    "lag1_log_volume_rel_prior20", "lag1_log_num_trades_rel_prior20",
    "lag1_log_avg_trade_value_rel_prior20", "rolling_log_volume_mean_diff_5_20",
)
M1_NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    "lag1_close_return_1d", "lag1_intraday_return", "rolling_past_return_mean",
    "lag1_intersession_gap_days", "lag1_ema_3_19_spread_pct",
    "lag1_close_ema_19_distance_pct", "lag1_ema_3_slope_5_pct",
    "lag1_ema_19_slope_5_pct", "lag1_ema_3_19_spread_pct_change_5",
    "lag1_ema_3_19_state_run_length_log", "rolling_past_return_std_5",
    "rolling_past_return_std_20", "rolling_return_std_ratio_5_20",
    "lag1_hl_range_pct", "lag1_hl_range_to_prior20_mean",
    "rolling_hl_range_mean_ratio_5_20", "lag1_log_volume_rel_prior20",
    "lag1_log_num_trades_rel_prior20", "lag1_log_avg_trade_value_rel_prior20",
    "rolling_log_volume_mean_diff_5_20",
)
M1_CATEGORICAL_FEATURES: Final[tuple[str, ...]] = ("lag1_ema_3_19_state",)
EMA_GROUP: Final[tuple[str, ...]] = (
    "lag1_ema_3_19_spread_pct", "lag1_close_ema_19_distance_pct",
    "lag1_ema_3_slope_5_pct", "lag1_ema_19_slope_5_pct",
    "lag1_ema_3_19_spread_pct_change_5", "lag1_ema_3_19_state_run_length_log",
)
VOLATILITY_GROUP: Final[tuple[str, ...]] = (
    "rolling_past_return_std_5", "rolling_past_return_std_20",
    "rolling_return_std_ratio_5_20", "lag1_hl_range_pct",
    "lag1_hl_range_to_prior20_mean", "rolling_hl_range_mean_ratio_5_20",
)
VOLUME_GROUP: Final[tuple[str, ...]] = (
    "lag1_log_volume_rel_prior20", "lag1_log_num_trades_rel_prior20",
    "lag1_log_avg_trade_value_rel_prior20", "rolling_log_volume_mean_diff_5_20",
)
MATRIX_NUMERIC_FEATURES: Final[dict[str, tuple[str, ...]]] = {
    "M1_REVISED_FULL": M1_NUMERIC_FEATURES,
    "M2_MINUS_NORMALIZED_EMA_TREND": tuple(x for x in M1_NUMERIC_FEATURES if x not in EMA_GROUP),
    "M3_MINUS_VOLATILITY_RANGE": tuple(x for x in M1_NUMERIC_FEATURES if x not in VOLATILITY_GROUP),
    "M4_MINUS_VOLUME_ACTIVITY": tuple(x for x in M1_NUMERIC_FEATURES if x not in VOLUME_GROUP),
    "M5_MINUS_LAGGED_INTERSESSION_GAP": tuple(x for x in M1_NUMERIC_FEATURES if x != "lag1_intersession_gap_days"),
}
MATRIX_CATEGORICAL_FEATURES: Final[dict[str, tuple[str, ...]]] = {
    "M1_REVISED_FULL": M1_CATEGORICAL_FEATURES,
    "M2_MINUS_NORMALIZED_EMA_TREND": (),
    "M3_MINUS_VOLATILITY_RANGE": M1_CATEGORICAL_FEATURES,
    "M4_MINUS_VOLUME_ACTIVITY": M1_CATEGORICAL_FEATURES,
    "M5_MINUS_LAGGED_INTERSESSION_GAP": M1_CATEGORICAL_FEATURES,
}


class Phase74FeaturePolicyBuilderError(ValueError):
    """Raised when Phase 7.4 feature construction must fail closed."""


@dataclass(frozen=True)
class FeatureBuildResult:
    matrices: dict[str, pd.DataFrame]
    diagnostics: pd.DataFrame
    ordered_identities: pd.DataFrame


@dataclass(frozen=True)
class _ComputedValue:
    value: float
    warmup_null: bool = False
    denominator_failure: bool = False


def _value(value: float, *, warmup: bool = False, denominator: bool = False) -> _ComputedValue:
    if warmup:
        return _ComputedValue(np.nan, warmup_null=True)
    if denominator or not np.isfinite(value):
        return _ComputedValue(np.nan, denominator_failure=True)
    return _ComputedValue(float(value))


def _ratio(numerator: float, denominator: float) -> _ComputedValue:
    if not np.isfinite(denominator) or denominator == 0:
        return _value(np.nan, denominator=True)
    return _value(numerator / denominator)


def _required_columns(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise Phase74FeaturePolicyBuilderError(
            f"{label} missing required columns: " + ", ".join(missing)
        )


def _prepare_source(source_panel: pd.DataFrame) -> pd.DataFrame:
    _required_columns(source_panel, SOURCE_REQUIRED_COLUMNS, "source panel")
    source = source_panel.loc[:, SOURCE_REQUIRED_COLUMNS].copy()
    parsed = pd.to_datetime(source["trade_date"], errors="coerce")
    if parsed.isna().any():
        raise Phase74FeaturePolicyBuilderError("source trade_date contains invalid values")
    source["trade_date"] = parsed.dt.normalize()
    source["instrument_id"] = source["instrument_id"].astype("string").str.strip()
    if source["instrument_id"].isna().any() or source["instrument_id"].eq("").any():
        raise Phase74FeaturePolicyBuilderError("source instrument_id must be nonempty")
    numeric = ("open", "high", "low", "close", "volume", "value", "num_trades")
    for column in numeric:
        source[column] = pd.to_numeric(source[column], errors="coerce")
    if source.loc[:, numeric].isna().any().any() or not np.isfinite(source.loc[:, numeric]).all().all():
        raise Phase74FeaturePolicyBuilderError("source numeric columns must be finite")
    if source.duplicated(["trade_date", "instrument_id"], keep=False).any():
        raise Phase74FeaturePolicyBuilderError("duplicate source trade_date/instrument_id identity")
    if source["high"].lt(source["low"]).any():
        raise Phase74FeaturePolicyBuilderError("source high is lower than low")
    if source[["open", "close"]].le(0).any().any():
        raise Phase74FeaturePolicyBuilderError("source OHLC denominator must be positive")
    if source[["volume", "value", "num_trades"]].lt(0).any().any():
        raise Phase74FeaturePolicyBuilderError("source activity columns must be nonnegative")
    return source.sort_values(["instrument_id", "trade_date"], kind="mergesort").reset_index(drop=True)


def _prepare_phase6(dataset: pd.DataFrame) -> pd.DataFrame:
    required = (
        *PHASE6_IDENTITY_AND_TARGET_COLUMNS,
        *PHASE6_LEGACY_NUMERIC_COLUMNS,
        *PHASE6_LEGACY_CATEGORICAL_COLUMNS,
    )
    _required_columns(dataset, required, "Phase 6 modeling dataset")
    rows = dataset.loc[:, required].copy()
    target_dates = pd.to_datetime(rows["target_trade_date"], errors="coerce").dt.normalize()
    instruments = rows["target_instrument_id"].astype("string").str.strip()
    eligible = (
        rows["target_source"].eq("manual_phase_labels_v1")
        & rows["target_is_labeled"].eq(True)
        & rows["target_phase_label"].isin(("B", "S", "OUT"))
        & target_dates.notna()
        & instruments.notna()
        & instruments.ne("")
    )
    rows = rows.loc[eligible].copy()
    if rows.empty:
        raise Phase74FeaturePolicyBuilderError("no eligible Phase 6 rows")
    rows["target_trade_date"] = target_dates.loc[eligible]
    rows["target_instrument_id"] = instruments.loc[eligible].astype(str)
    prior_dates = pd.to_datetime(rows["prior_trade_date"], errors="coerce").dt.normalize()
    if prior_dates.isna().any():
        raise Phase74FeaturePolicyBuilderError(
            "eligible Phase 6 prior_trade_date is missing or invalid"
        )
    if prior_dates.ge(rows["target_trade_date"]).any():
        raise Phase74FeaturePolicyBuilderError(
            "eligible Phase 6 prior_trade_date must be strictly earlier than target_trade_date"
        )
    rows["prior_trade_date"] = prior_dates
    rows = rows.sort_values(["target_trade_date", "target_instrument_id"], kind="mergesort").reset_index(drop=True)
    if rows.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise Phase74FeaturePolicyBuilderError("duplicate eligible target identity")
    rows["target_trade_date"] = rows["target_trade_date"].dt.strftime("%Y-%m-%d")
    return rows


def _state_run_lengths(states: np.ndarray) -> np.ndarray:
    output = np.ones(len(states), dtype=int)
    for index in range(1, len(states)):
        output[index] = output[index - 1] + 1 if states[index] == states[index - 1] else 1
    return output


def _features_for_group(group: pd.DataFrame) -> dict[pd.Timestamp, tuple[dict[str, _ComputedValue], str]]:
    group = group.sort_values("trade_date", kind="mergesort").reset_index(drop=True)
    dates = group["trade_date"].tolist()
    close = group["close"].to_numpy(float)
    high = group["high"].to_numpy(float)
    low = group["low"].to_numpy(float)
    volume = group["volume"].to_numpy(float)
    value = group["value"].to_numpy(float)
    trades = group["num_trades"].to_numpy(float)
    returns = pd.Series(close).pct_change().to_numpy(float)
    ranges = (high - low) / close
    ema3 = pd.Series(close).ewm(span=3, adjust=False).mean().to_numpy(float)
    ema19 = pd.Series(close).ewm(span=19, adjust=False).mean().to_numpy(float)
    spread_pct = (ema3 - ema19) / close
    states = np.where(
        ema3 > ema19, "ema3_above_ema19",
        np.where(ema3 < ema19, "ema3_below_ema19", "ema3_equal_ema19"),
    )
    runs = _state_run_lengths(states)
    result: dict[pd.Timestamp, tuple[dict[str, _ComputedValue], str]] = {}
    for index, date in enumerate(dates):
        computed: dict[str, _ComputedValue] = {}
        computed["lag1_intersession_gap_days"] = (
            _value(np.nan, warmup=True) if index < 1
            else _value((date - dates[index - 1]).days)
        )
        computed["lag1_ema_3_19_spread_pct"] = _value(spread_pct[index])
        computed["lag1_close_ema_19_distance_pct"] = _ratio(close[index] - ema19[index], ema19[index])
        for name, series in (
            ("lag1_ema_3_slope_5_pct", ema3),
            ("lag1_ema_19_slope_5_pct", ema19),
        ):
            computed[name] = (
                _value(np.nan, warmup=True) if index < 5
                else _ratio(series[index], series[index - 5])
            )
            if not computed[name].warmup_null and not computed[name].denominator_failure:
                computed[name] = _value(computed[name].value - 1.0)
        computed["lag1_ema_3_19_spread_pct_change_5"] = (
            _value(np.nan, warmup=True) if index < 5
            else _value(spread_pct[index] - spread_pct[index - 5])
        )
        computed["lag1_ema_3_19_state_run_length_log"] = _value(np.log1p(runs[index]))
        computed["rolling_past_return_std_5"] = (
            _value(np.nan, warmup=True) if index < 5
            else _value(np.std(returns[index - 4:index + 1], ddof=1))
        )
        std20 = (
            _value(np.nan, warmup=True) if index < 20
            else _value(np.std(returns[index - 19:index + 1], ddof=1))
        )
        computed["rolling_past_return_std_20"] = std20
        computed["rolling_return_std_ratio_5_20"] = (
            _value(np.nan, warmup=True)
            if computed["rolling_past_return_std_5"].warmup_null or std20.warmup_null
            else _ratio(computed["rolling_past_return_std_5"].value, std20.value)
        )
        if index < 20:
            for name in (
                "lag1_hl_range_to_prior20_mean", "rolling_hl_range_mean_ratio_5_20",
                "lag1_log_volume_rel_prior20", "lag1_log_num_trades_rel_prior20",
                "lag1_log_avg_trade_value_rel_prior20", "rolling_log_volume_mean_diff_5_20",
            ):
                computed[name] = _value(np.nan, warmup=True)
        else:
            prior20 = slice(index - 20, index)
            current20 = slice(index - 19, index + 1)
            current5 = slice(index - 4, index + 1)
            ratio = _ratio(ranges[index], float(np.mean(ranges[prior20])))
            computed["lag1_hl_range_to_prior20_mean"] = (
                ratio if ratio.denominator_failure else _value(ratio.value - 1.0)
            )
            ratio = _ratio(float(np.mean(ranges[current5])), float(np.mean(ranges[current20])))
            computed["rolling_hl_range_mean_ratio_5_20"] = (
                ratio if ratio.denominator_failure else _value(ratio.value - 1.0)
            )
            computed["lag1_log_volume_rel_prior20"] = _value(
                np.log1p(volume[index]) - np.mean(np.log1p(volume[prior20]))
            )
            computed["lag1_log_num_trades_rel_prior20"] = _value(
                np.log1p(trades[index]) - np.mean(np.log1p(trades[prior20]))
            )
            prior_trades = trades[prior20]
            if trades[index] == 0 or np.any(prior_trades == 0):
                computed["lag1_log_avg_trade_value_rel_prior20"] = _value(np.nan, denominator=True)
            else:
                computed["lag1_log_avg_trade_value_rel_prior20"] = _value(
                    np.log1p(value[index] / trades[index])
                    - np.mean(np.log1p(value[prior20] / prior_trades))
                )
            computed["rolling_log_volume_mean_diff_5_20"] = _value(
                np.mean(np.log1p(volume[current5])) - np.mean(np.log1p(volume[current20]))
            )
        result[pd.Timestamp(date)] = (computed, str(states[index]))
    return result


def build_feature_matrices(
    source_panel: pd.DataFrame,
    phase6_modeling_dataset: pd.DataFrame,
) -> FeatureBuildResult:
    """Build exactly M1-M5 in memory from explicit past-only source identities."""
    source = _prepare_source(source_panel)
    phase6 = _prepare_phase6(phase6_modeling_dataset)
    by_instrument = {
        str(instrument): _features_for_group(group)
        for instrument, group in source.groupby("instrument_id", sort=False)
    }
    records: list[dict[str, object]] = []
    diagnostic_rows: list[dict[str, object]] = []
    for row_index, row in phase6.iterrows():
        instrument = str(row["target_instrument_id"])
        if instrument not in by_instrument:
            raise Phase74FeaturePolicyBuilderError("target/source identity is absent")
        source_date = pd.Timestamp(row["prior_trade_date"])
        if source_date not in by_instrument[instrument]:
            raise Phase74FeaturePolicyBuilderError(
                "exact target prior_trade_date/instrument_id source identity is absent"
            )
        computed, source_state = by_instrument[instrument][source_date]
        if str(row["lag1_ema_3_19_state"]) != source_state:
            raise Phase74FeaturePolicyBuilderError("retained EMA state disagrees with source panel")
        record = {column: row[column] for column in PHASE6_IDENTITY_AND_TARGET_COLUMNS}
        record.update({column: row[column] for column in RETAINED_NUMERIC_FEATURES})
        record.update({column: row[column] for column in RETAINED_CATEGORICAL_FEATURES})
        for feature in NEW_NUMERIC_FEATURES:
            item = computed[feature]
            record[feature] = item.value
            diagnostic_rows.append({
                "row_index": int(row_index),
                "target_trade_date": row["target_trade_date"],
                "target_instrument_id": instrument,
                "feature": feature,
                "warmup_null": item.warmup_null,
                "denominator_failure": item.denominator_failure,
            })
        records.append(record)
    full = pd.DataFrame(records)
    if np.isinf(full.loc[:, NEW_NUMERIC_FEATURES].to_numpy(float)).any():
        raise Phase74FeaturePolicyBuilderError("infinite feature value emitted")
    matrices: dict[str, pd.DataFrame] = {}
    identity_prefix = list(PHASE6_IDENTITY_AND_TARGET_COLUMNS)
    for matrix_id in MATRIX_NUMERIC_FEATURES:
        columns = [
            *identity_prefix,
            *MATRIX_NUMERIC_FEATURES[matrix_id],
            *MATRIX_CATEGORICAL_FEATURES[matrix_id],
        ]
        matrix = full.loc[:, columns].copy()
        if not matrix.loc[:, IDENTITY_COLUMNS].equals(full.loc[:, IDENTITY_COLUMNS]):
            raise Phase74FeaturePolicyBuilderError("matrix identity drift")
        matrices[matrix_id] = matrix
    return FeatureBuildResult(
        matrices=matrices,
        diagnostics=pd.DataFrame(diagnostic_rows),
        ordered_identities=full.loc[:, IDENTITY_COLUMNS].copy(),
    )

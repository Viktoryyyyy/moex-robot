from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd


IDENTITY_COLUMNS: Final[tuple[str, str]] = (
    "target_trade_date",
    "target_instrument_id",
)
TARGET_COLUMNS: Final[tuple[str, ...]] = (
    "target_phase_label",
    "target_is_labeled",
    "target_source",
)
M0_NUMERIC_FEATURES: Final[tuple[str, ...]] = (
    "session_index",
    "days_since_prior_trade_date",
    "lag1_close_return_1d",
    "lag1_intraday_return",
    "rolling_past_return_mean",
    "rolling_past_return_std",
    "lag1_hl_range_pct",
    "rolling_past_hl_range_mean",
    "rolling_past_hl_range_std",
    "lag1_volume",
    "lag1_value",
    "lag1_num_trades",
    "rolling_past_volume_mean",
    "lag1_ema_3",
    "lag1_ema_19",
    "lag1_ema_3_19_spread",
)
M0_CATEGORICAL_FEATURES: Final[tuple[str, ...]] = ("lag1_ema_3_19_state",)
POLICY_AND_MONEY_MARKET_FEATURES: Final[tuple[str, ...]] = (
    "ext_key_rate_pct",
    "ext_ruonia_minus_key_rate_pp",
    "ext_ruonia_rate_range_pp",
    "ext_ruonia_rate_iqr_pp",
    "ext_log1p_key_rate_age_days",
)
RUONIA_ACTIVITY_FEATURES: Final[tuple[str, ...]] = (
    "ext_log1p_ruonia_transaction_volume_rub_bn",
    "ext_log1p_ruonia_transaction_count",
    "ext_log1p_ruonia_participant_count",
)
EXTERNAL_FEATURES: Final[tuple[str, ...]] = (
    *POLICY_AND_MONEY_MARKET_FEATURES,
    *RUONIA_ACTIVITY_FEATURES,
)
EXTERNAL_SOURCE_COLUMNS: Final[tuple[str, ...]] = (
    "key_rate_pct",
    "ruonia_minus_key_rate_pp",
    "ruonia_minimum_rate_pct",
    "ruonia_percentile_25_rate_pct",
    "ruonia_percentile_75_rate_pct",
    "ruonia_maximum_rate_pct",
    "key_rate_age_calendar_days",
    "ruonia_transaction_volume_rub_bn",
    "ruonia_transaction_count",
    "ruonia_participant_count",
)
MATRIX_NUMERIC_FEATURES: Final[dict[str, tuple[str, ...]]] = {
    "E1_M0_PLUS_EXTERNAL_FULL": (*M0_NUMERIC_FEATURES, *EXTERNAL_FEATURES),
    "E2_M0_PLUS_POLICY_AND_MONEY_MARKET": (
        *M0_NUMERIC_FEATURES,
        *POLICY_AND_MONEY_MARKET_FEATURES,
    ),
    "E3_M0_PLUS_RUONIA_ACTIVITY": (
        *M0_NUMERIC_FEATURES,
        *RUONIA_ACTIVITY_FEATURES,
    ),
    "E4_EXTERNAL_ONLY": EXTERNAL_FEATURES,
}
MATRIX_CATEGORICAL_FEATURES: Final[dict[str, tuple[str, ...]]] = {
    "E1_M0_PLUS_EXTERNAL_FULL": M0_CATEGORICAL_FEATURES,
    "E2_M0_PLUS_POLICY_AND_MONEY_MARKET": M0_CATEGORICAL_FEATURES,
    "E3_M0_PLUS_RUONIA_ACTIVITY": M0_CATEGORICAL_FEATURES,
    "E4_EXTERNAL_ONLY": (),
}
MATRIX_ROLES: Final[dict[str, str]] = {
    "E0_FROZEN_PHASE7_2_CONTROL": "immutable_historical_control",
    "E1_M0_PLUS_EXTERNAL_FULL": "sole_acceptance_candidate",
    "E2_M0_PLUS_POLICY_AND_MONEY_MARKET": "diagnostic_ablation",
    "E3_M0_PLUS_RUONIA_ACTIVITY": "diagnostic_ablation",
    "E4_EXTERNAL_ONLY": "diagnostic_only",
}
EXPECTED_ELIGIBLE_IDENTITIES: Final[int] = 472
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"
TARGET_SOURCE: Final[str] = "manual_phase_labels_v1"
CLASS_ORDER: Final[tuple[str, str, str]] = ("B", "S", "OUT")
FORBIDDEN_FEATURE_TOKENS: Final[tuple[str, ...]] = (
    "target",
    "label",
    "prediction",
    "probability",
    "candidate_y_pred",
    "brent",
    "wti",
    "liquidity",
    "cme",
    "ine_",
)


class Phase83ExternalFactorBuilderError(ValueError):
    """Raised when Phase 8.3 feature construction must fail closed."""


@dataclass(frozen=True)
class ExternalFeatureBuildResult:
    matrices: dict[str, pd.DataFrame]
    eligible: pd.DataFrame
    external_features: pd.DataFrame


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise Phase83ExternalFactorBuilderError(
            f"{label} missing required columns: " + ", ".join(missing)
        )


def _normalize_identities(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    identities = frame.loc[:, IDENTITY_COLUMNS].copy()
    dates = pd.to_datetime(identities["target_trade_date"], errors="coerce")
    instruments = identities["target_instrument_id"].astype("string").str.strip()
    if dates.isna().any():
        raise Phase83ExternalFactorBuilderError(f"{label} contains invalid target_trade_date")
    if instruments.isna().any() or instruments.eq("").any():
        raise Phase83ExternalFactorBuilderError(
            f"{label} contains empty target_instrument_id"
        )
    identities["target_trade_date"] = dates.dt.strftime("%Y-%m-%d")
    identities["target_instrument_id"] = instruments.astype(str)
    if identities.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise Phase83ExternalFactorBuilderError(f"duplicate identity in {label}")
    return identities


def prepare_eligible_modeling_rows(modeling_dataset: pd.DataFrame) -> pd.DataFrame:
    required = (
        *IDENTITY_COLUMNS,
        *TARGET_COLUMNS,
        *M0_NUMERIC_FEATURES,
        *M0_CATEGORICAL_FEATURES,
    )
    _require_columns(modeling_dataset, required, "modeling dataset")
    rows = modeling_dataset.loc[:, required].copy()
    dates = pd.to_datetime(rows["target_trade_date"], errors="coerce")
    instruments = rows["target_instrument_id"].astype("string").str.strip()
    eligible_mask = (
        rows["target_source"].eq(TARGET_SOURCE)
        & rows["target_is_labeled"].eq(True)
        & rows["target_phase_label"].isin(CLASS_ORDER)
        & dates.notna()
        & instruments.notna()
        & instruments.ne("")
    )
    rows = rows.loc[eligible_mask].copy()
    rows["target_trade_date"] = dates.loc[eligible_mask].dt.strftime("%Y-%m-%d")
    rows["target_instrument_id"] = instruments.loc[eligible_mask].astype(str)
    rows = rows.sort_values(
        list(IDENTITY_COLUMNS), kind="mergesort"
    ).reset_index(drop=True)
    if len(rows) != EXPECTED_ELIGIBLE_IDENTITIES:
        raise Phase83ExternalFactorBuilderError(
            f"eligible identity count must equal {EXPECTED_ELIGIBLE_IDENTITIES}"
        )
    if rows.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise Phase83ExternalFactorBuilderError("duplicate eligible identity")
    if set(rows["target_instrument_id"]) != {EXPECTED_INSTRUMENT}:
        raise Phase83ExternalFactorBuilderError("eligible instrument identity mismatch")
    numeric = rows.loc[:, M0_NUMERIC_FEATURES].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any() or not np.isfinite(numeric.to_numpy(float)).all():
        raise Phase83ExternalFactorBuilderError("M0 numeric features must be finite")
    rows.loc[:, M0_NUMERIC_FEATURES] = numeric
    categorical = rows.loc[:, M0_CATEGORICAL_FEATURES]
    if categorical.isna().any().any() or categorical.astype(str).eq("").any().any():
        raise Phase83ExternalFactorBuilderError("M0 categorical features must be nonempty")
    return rows


def _prepare_external_matrix(external_matrix: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        external_matrix,
        (*IDENTITY_COLUMNS, *EXTERNAL_SOURCE_COLUMNS),
        "Phase 8.2 external matrix",
    )
    identities = _normalize_identities(external_matrix, "Phase 8.2 external matrix")
    source = external_matrix.loc[:, EXTERNAL_SOURCE_COLUMNS].apply(
        pd.to_numeric, errors="coerce"
    )
    if source.isna().any().any() or not np.isfinite(source.to_numpy(float)).all():
        raise Phase83ExternalFactorBuilderError("external source values must be finite")
    nonnegative = (
        "key_rate_age_calendar_days",
        "ruonia_transaction_volume_rub_bn",
        "ruonia_transaction_count",
        "ruonia_participant_count",
    )
    if source.loc[:, nonnegative].lt(0).any().any():
        if source["key_rate_age_calendar_days"].lt(0).any():
            raise Phase83ExternalFactorBuilderError("negative key-rate age")
        raise Phase83ExternalFactorBuilderError("negative transaction activity")
    rate_range = source["ruonia_maximum_rate_pct"] - source["ruonia_minimum_rate_pct"]
    rate_iqr = (
        source["ruonia_percentile_75_rate_pct"]
        - source["ruonia_percentile_25_rate_pct"]
    )
    if rate_range.lt(0).any() or rate_iqr.lt(0).any():
        raise Phase83ExternalFactorBuilderError("negative rate range or IQR")
    features = identities.copy()
    features["ext_key_rate_pct"] = source["key_rate_pct"].to_numpy(float)
    features["ext_ruonia_minus_key_rate_pp"] = source[
        "ruonia_minus_key_rate_pp"
    ].to_numpy(float)
    features["ext_ruonia_rate_range_pp"] = rate_range.to_numpy(float)
    features["ext_ruonia_rate_iqr_pp"] = rate_iqr.to_numpy(float)
    features["ext_log1p_key_rate_age_days"] = np.log1p(
        source["key_rate_age_calendar_days"].to_numpy(float)
    )
    features["ext_log1p_ruonia_transaction_volume_rub_bn"] = np.log1p(
        source["ruonia_transaction_volume_rub_bn"].to_numpy(float)
    )
    features["ext_log1p_ruonia_transaction_count"] = np.log1p(
        source["ruonia_transaction_count"].to_numpy(float)
    )
    features["ext_log1p_ruonia_participant_count"] = np.log1p(
        source["ruonia_participant_count"].to_numpy(float)
    )
    if not np.isfinite(features.loc[:, EXTERNAL_FEATURES].to_numpy(float)).all():
        raise Phase83ExternalFactorBuilderError("derived external features must be finite")
    return features


def _validate_feature_inventories() -> None:
    if set(MATRIX_NUMERIC_FEATURES) != {
        "E1_M0_PLUS_EXTERNAL_FULL",
        "E2_M0_PLUS_POLICY_AND_MONEY_MARKET",
        "E3_M0_PLUS_RUONIA_ACTIVITY",
        "E4_EXTERNAL_ONLY",
    }:
        raise Phase83ExternalFactorBuilderError("candidate matrix inventory mismatch")
    feature_union = {
        feature
        for features in MATRIX_NUMERIC_FEATURES.values()
        for feature in features
    } | {
        feature
        for features in MATRIX_CATEGORICAL_FEATURES.values()
        for feature in features
    }
    if any(
        token in feature.lower()
        for feature in feature_union
        for token in FORBIDDEN_FEATURE_TOKENS
    ):
        raise Phase83ExternalFactorBuilderError("forbidden feature entered inventory")
    if "E0_FROZEN_PHASE7_2_CONTROL" in MATRIX_NUMERIC_FEATURES:
        raise Phase83ExternalFactorBuilderError("E0 must not be built as a feature matrix")


def build_external_feature_matrices(
    modeling_dataset: pd.DataFrame,
    external_matrix: pd.DataFrame,
) -> ExternalFeatureBuildResult:
    """Join Phase 8.2 data exactly and build the four fixed candidate matrices."""

    _validate_feature_inventories()
    eligible = prepare_eligible_modeling_rows(modeling_dataset)
    external = _prepare_external_matrix(external_matrix)
    if len(external) != EXPECTED_ELIGIBLE_IDENTITIES:
        raise Phase83ExternalFactorBuilderError("external matrix has missing or extra identity")
    eligible_identities = eligible.loc[:, IDENTITY_COLUMNS].reset_index(drop=True)
    external_identities = external.loc[:, IDENTITY_COLUMNS].reset_index(drop=True)
    eligible_index = pd.MultiIndex.from_frame(eligible_identities)
    external_index = pd.MultiIndex.from_frame(external_identities)
    if not eligible_index.isin(external_index).all():
        raise Phase83ExternalFactorBuilderError("external matrix is missing eligible identity")
    if not external_index.isin(eligible_index).all():
        raise Phase83ExternalFactorBuilderError("external matrix has extra identity")
    if not external_identities.equals(eligible_identities):
        raise Phase83ExternalFactorBuilderError(
            "external matrix identity order differs from Phase 6"
        )
    joined = pd.concat(
        [eligible.reset_index(drop=True), external.loc[:, EXTERNAL_FEATURES]], axis=1
    )
    matrices: dict[str, pd.DataFrame] = {}
    for matrix_id in MATRIX_NUMERIC_FEATURES:
        feature_columns = (
            *MATRIX_NUMERIC_FEATURES[matrix_id],
            *MATRIX_CATEGORICAL_FEATURES[matrix_id],
        )
        matrix = joined.loc[:, feature_columns].copy()
        if matrix.isna().any().any():
            raise Phase83ExternalFactorBuilderError(f"null value entered {matrix_id}")
        matrices[matrix_id] = matrix
    return ExternalFeatureBuildResult(
        matrices=matrices,
        eligible=eligible,
        external_features=external,
    )

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

from moex_research.runners.usdrubf_phase8_3_external_factor_builder import (
    CLASS_ORDER,
    EXPECTED_ELIGIBLE_IDENTITIES,
    EXPECTED_INSTRUMENT,
    IDENTITY_COLUMNS,
    M0_CATEGORICAL_FEATURES,
    M0_NUMERIC_FEATURES,
    TARGET_COLUMNS,
    TARGET_SOURCE,
)


PRICE_ACTION_FEATURES: Final[tuple[str, ...]] = (
    "ext_brent_log_close",
    "ext_brent_intraday_return",
    "ext_brent_range_pct",
    "ext_brent_close_location",
)
ACTIVITY_FEATURES: Final[tuple[str, ...]] = (
    "ext_log1p_brent_volume",
    "ext_log1p_brent_value",
)
BRENT_FEATURES: Final[tuple[str, ...]] = (
    *PRICE_ACTION_FEATURES,
    *ACTIVITY_FEATURES,
)
BRENT_SOURCE_COLUMNS: Final[tuple[str, ...]] = (
    "prior_trade_date",
    "brent_contract_code",
    "brent_contract_changed",
    "brent_previous_contract_code",
    "brent_trade_date",
    "brent_open",
    "brent_high",
    "brent_low",
    "brent_close",
    "brent_volume",
    "brent_value",
)
MATRIX_NUMERIC_FEATURES: Final[dict[str, tuple[str, ...]]] = {
    "E1_M0_PLUS_BRENT_FULL": (*M0_NUMERIC_FEATURES, *BRENT_FEATURES),
    "E2_M0_PLUS_BRENT_PRICE_ACTION": (
        *M0_NUMERIC_FEATURES,
        *PRICE_ACTION_FEATURES,
    ),
    "E3_M0_PLUS_BRENT_ACTIVITY": (*M0_NUMERIC_FEATURES, *ACTIVITY_FEATURES),
    "E4_BRENT_ONLY": BRENT_FEATURES,
}
MATRIX_CATEGORICAL_FEATURES: Final[dict[str, tuple[str, ...]]] = {
    "E1_M0_PLUS_BRENT_FULL": M0_CATEGORICAL_FEATURES,
    "E2_M0_PLUS_BRENT_PRICE_ACTION": M0_CATEGORICAL_FEATURES,
    "E3_M0_PLUS_BRENT_ACTIVITY": M0_CATEGORICAL_FEATURES,
    "E4_BRENT_ONLY": (),
}
MATRIX_ROLES: Final[dict[str, str]] = {
    "E0_FROZEN_PHASE7_2_CONTROL": "immutable_historical_control",
    "E1_M0_PLUS_BRENT_FULL": "sole_acceptance_candidate",
    "E2_M0_PLUS_BRENT_PRICE_ACTION": "diagnostic_ablation",
    "E3_M0_PLUS_BRENT_ACTIVITY": "diagnostic_ablation",
    "E4_BRENT_ONLY": "diagnostic_only",
}
FORBIDDEN_FEATURE_TOKENS: Final[tuple[str, ...]] = (
    "target",
    "label",
    "prediction",
    "probability",
    "candidate_y_pred",
    "contract_code",
    "previous_contract",
    "trade_date",
    "retrieved",
    "timestamp",
    "payload",
    "sha256",
    "route",
    "roll",
    "days_to_expiration",
    "ruonia",
    "key_rate",
    "cross_contract",
    "back_adjusted",
)


class BrentIncrementalFeatureError(ValueError):
    """Raised when frozen Phase 8.5 feature construction must fail closed."""


@dataclass(frozen=True)
class BrentFeatureBuildResult:
    matrices: dict[str, pd.DataFrame]
    eligible: pd.DataFrame
    brent_features: pd.DataFrame


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise BrentIncrementalFeatureError(
            f"{label} missing required columns: " + ", ".join(missing)
        )


def _normalize_date(series: pd.Series, label: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.isna().any():
        raise BrentIncrementalFeatureError(f"{label} contains invalid date")
    return parsed.dt.strftime("%Y-%m-%d")


def prepare_eligible_modeling_rows(modeling_dataset: pd.DataFrame) -> pd.DataFrame:
    required = (
        *IDENTITY_COLUMNS,
        "prior_trade_date",
        *TARGET_COLUMNS,
        *M0_NUMERIC_FEATURES,
        *M0_CATEGORICAL_FEATURES,
    )
    _require_columns(modeling_dataset, required, "modeling dataset")
    rows = modeling_dataset.loc[:, required].copy()
    target_dates = pd.to_datetime(rows["target_trade_date"], errors="coerce")
    prior_dates = pd.to_datetime(rows["prior_trade_date"], errors="coerce")
    instruments = rows["target_instrument_id"].astype("string").str.strip()
    eligible_mask = (
        rows["target_source"].eq(TARGET_SOURCE)
        & rows["target_is_labeled"].eq(True)
        & rows["target_phase_label"].isin(CLASS_ORDER)
        & target_dates.notna()
        & prior_dates.notna()
        & instruments.notna()
        & instruments.ne("")
    )
    rows = rows.loc[eligible_mask].copy()
    rows["target_trade_date"] = target_dates.loc[eligible_mask].dt.strftime("%Y-%m-%d")
    rows["prior_trade_date"] = prior_dates.loc[eligible_mask].dt.strftime("%Y-%m-%d")
    rows["target_instrument_id"] = instruments.loc[eligible_mask].astype(str)
    rows = rows.sort_values(list(IDENTITY_COLUMNS), kind="mergesort").reset_index(drop=True)
    if len(rows) != EXPECTED_ELIGIBLE_IDENTITIES:
        raise BrentIncrementalFeatureError(
            f"eligible identity count must equal {EXPECTED_ELIGIBLE_IDENTITIES}"
        )
    if rows.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise BrentIncrementalFeatureError("duplicate eligible identity")
    if set(rows["target_instrument_id"]) != {EXPECTED_INSTRUMENT}:
        raise BrentIncrementalFeatureError("eligible instrument identity mismatch")
    target = pd.to_datetime(rows["target_trade_date"])
    prior = pd.to_datetime(rows["prior_trade_date"])
    if not (prior < target).all():
        raise BrentIncrementalFeatureError("prior_trade_date must precede target_trade_date")
    numeric = rows.loc[:, M0_NUMERIC_FEATURES].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any() or not np.isfinite(numeric.to_numpy(float)).all():
        raise BrentIncrementalFeatureError("M0 numeric features must be finite")
    rows.loc[:, M0_NUMERIC_FEATURES] = numeric
    categorical = rows.loc[:, M0_CATEGORICAL_FEATURES]
    if categorical.isna().any().any() or categorical.astype(str).eq("").any().any():
        raise BrentIncrementalFeatureError("M0 categorical features must be nonempty")
    return rows


def _prepare_brent_features(brent_matrix: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        brent_matrix,
        (*IDENTITY_COLUMNS, *BRENT_SOURCE_COLUMNS),
        "Phase 8.4A Brent PIT matrix",
    )
    identities = brent_matrix.loc[:, IDENTITY_COLUMNS].copy()
    identities["target_trade_date"] = _normalize_date(
        identities["target_trade_date"], "Brent target_trade_date"
    )
    instruments = identities["target_instrument_id"].astype("string").str.strip()
    if instruments.isna().any() or instruments.eq("").any():
        raise BrentIncrementalFeatureError("Brent matrix contains empty instrument identity")
    identities["target_instrument_id"] = instruments.astype(str)
    if identities.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise BrentIncrementalFeatureError("duplicate identity in Phase 8.4A Brent PIT matrix")

    prior = _normalize_date(brent_matrix["prior_trade_date"], "Brent prior_trade_date")
    candle = _normalize_date(brent_matrix["brent_trade_date"], "Brent trade date")
    if candle.eq(identities["target_trade_date"]).any():
        raise BrentIncrementalFeatureError("target-day Brent candle is forbidden")
    if not candle.eq(prior).all():
        raise BrentIncrementalFeatureError("Brent trade date must equal prior_trade_date")

    contract_codes = brent_matrix["brent_contract_code"].astype("string").str.strip()
    if contract_codes.isna().any() or contract_codes.eq("").any():
        raise BrentIncrementalFeatureError("explicit Brent contract identity is required")
    changed = brent_matrix["brent_contract_changed"]
    if changed.isna().any() or not changed.isin((True, False)).all():
        raise BrentIncrementalFeatureError("Brent roll flag must be boolean")

    numeric_columns = (
        "brent_open",
        "brent_high",
        "brent_low",
        "brent_close",
        "brent_volume",
        "brent_value",
    )
    numeric = brent_matrix.loc[:, numeric_columns].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any() or not np.isfinite(numeric.to_numpy(float)).all():
        raise BrentIncrementalFeatureError("Brent OHLC, volume and value must be finite")
    if numeric["brent_open"].le(0).any():
        raise BrentIncrementalFeatureError("Brent open must be strictly positive")
    if numeric["brent_close"].le(0).any():
        raise BrentIncrementalFeatureError("Brent close must be strictly positive")
    if numeric[["brent_volume", "brent_value"]].lt(0).any().any():
        if numeric["brent_volume"].lt(0).any():
            raise BrentIncrementalFeatureError("Brent volume must be non-negative")
        raise BrentIncrementalFeatureError("Brent value must be non-negative")
    if (
        numeric["brent_high"].lt(numeric[["brent_open", "brent_close"]].max(axis=1)).any()
        or numeric["brent_low"].gt(numeric[["brent_open", "brent_close"]].min(axis=1)).any()
        or numeric["brent_high"].lt(numeric["brent_low"]).any()
    ):
        raise BrentIncrementalFeatureError("Brent OHLC ordering is inconsistent")

    feature_frame = identities.copy()
    feature_frame["prior_trade_date"] = prior.to_numpy(str)
    feature_frame["brent_trade_date"] = candle.to_numpy(str)
    feature_frame["brent_contract_code"] = contract_codes.astype(str).to_numpy()
    feature_frame["ext_brent_log_close"] = np.log(numeric["brent_close"].to_numpy(float))
    feature_frame["ext_brent_intraday_return"] = (
        numeric["brent_close"].to_numpy(float) / numeric["brent_open"].to_numpy(float)
    ) - 1.0
    feature_frame["ext_brent_range_pct"] = (
        numeric["brent_high"].to_numpy(float) - numeric["brent_low"].to_numpy(float)
    ) / numeric["brent_open"].to_numpy(float)
    ranges = numeric["brent_high"].to_numpy(float) - numeric["brent_low"].to_numpy(float)
    feature_frame["ext_brent_close_location"] = np.divide(
        numeric["brent_close"].to_numpy(float) - numeric["brent_low"].to_numpy(float),
        ranges,
        out=np.full(len(numeric), 0.5, dtype=float),
        where=ranges != 0.0,
    )
    feature_frame["ext_log1p_brent_volume"] = np.log1p(
        numeric["brent_volume"].to_numpy(float)
    )
    feature_frame["ext_log1p_brent_value"] = np.log1p(
        numeric["brent_value"].to_numpy(float)
    )
    if not np.isfinite(feature_frame.loc[:, BRENT_FEATURES].to_numpy(float)).all():
        raise BrentIncrementalFeatureError("derived Brent features must be finite")
    return feature_frame


def _validate_feature_inventories() -> None:
    expected_matrices = {
        "E1_M0_PLUS_BRENT_FULL",
        "E2_M0_PLUS_BRENT_PRICE_ACTION",
        "E3_M0_PLUS_BRENT_ACTIVITY",
        "E4_BRENT_ONLY",
    }
    if set(MATRIX_NUMERIC_FEATURES) != expected_matrices:
        raise BrentIncrementalFeatureError("candidate matrix inventory mismatch")
    if set(MATRIX_CATEGORICAL_FEATURES) != expected_matrices:
        raise BrentIncrementalFeatureError("candidate categorical inventory mismatch")
    feature_union = set(BRENT_FEATURES)
    if any(
        token in feature.lower()
        for feature in feature_union
        for token in FORBIDDEN_FEATURE_TOKENS
    ):
        raise BrentIncrementalFeatureError("forbidden field entered feature inventory")
    authorized = set(M0_NUMERIC_FEATURES) | set(M0_CATEGORICAL_FEATURES) | set(
        BRENT_FEATURES
    )
    matrix_union = {
        feature for values in MATRIX_NUMERIC_FEATURES.values() for feature in values
    } | {
        feature for values in MATRIX_CATEGORICAL_FEATURES.values() for feature in values
    }
    if not matrix_union <= authorized:
        raise BrentIncrementalFeatureError("undeclared feature entered inventory")
    if "E0_FROZEN_PHASE7_2_CONTROL" in MATRIX_NUMERIC_FEATURES:
        raise BrentIncrementalFeatureError("E0 must remain frozen and must not be refit")


def build_brent_feature_matrices(
    modeling_dataset: pd.DataFrame,
    brent_matrix: pd.DataFrame,
) -> BrentFeatureBuildResult:
    """Build the four fixed Phase 8.5 candidate matrices without cross-session returns."""

    _validate_feature_inventories()
    eligible = prepare_eligible_modeling_rows(modeling_dataset)
    brent = _prepare_brent_features(brent_matrix)
    if len(brent) != EXPECTED_ELIGIBLE_IDENTITIES:
        raise BrentIncrementalFeatureError("Brent matrix has missing or extra identity")
    eligible_identity = eligible.loc[:, IDENTITY_COLUMNS].reset_index(drop=True)
    brent_identity = brent.loc[:, IDENTITY_COLUMNS].reset_index(drop=True)
    if not brent_identity.equals(eligible_identity):
        raise BrentIncrementalFeatureError(
            "Brent matrix identity or order differs from frozen Phase 6"
        )
    if not brent["prior_trade_date"].reset_index(drop=True).equals(
        eligible["prior_trade_date"].reset_index(drop=True)
    ):
        raise BrentIncrementalFeatureError(
            "Brent prior_trade_date differs from frozen Phase 6"
        )
    joined = pd.concat(
        [eligible.reset_index(drop=True), brent.loc[:, BRENT_FEATURES]], axis=1
    )
    matrices: dict[str, pd.DataFrame] = {}
    for matrix_id in MATRIX_NUMERIC_FEATURES:
        columns = (
            *MATRIX_NUMERIC_FEATURES[matrix_id],
            *MATRIX_CATEGORICAL_FEATURES[matrix_id],
        )
        matrix = joined.loc[:, columns].copy()
        if matrix.isna().any().any():
            raise BrentIncrementalFeatureError(f"null value entered {matrix_id}")
        matrices[matrix_id] = matrix
    return BrentFeatureBuildResult(
        matrices=matrices,
        eligible=eligible,
        brent_features=brent,
    )

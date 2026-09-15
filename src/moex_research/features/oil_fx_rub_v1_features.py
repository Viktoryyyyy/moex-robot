from __future__ import annotations

from typing import Final

import numpy as np
import pandas as pd


IDENTITY_COLUMNS: Final[tuple[str, str]] = (
    "target_trade_date",
    "target_instrument_id",
)
EXPECTED_INSTRUMENT: Final[str] = "forts.usdrubf"
EXPECTED_IDENTITY_COUNT: Final[int] = 472

BRENT_REQUIRED: Final[tuple[str, ...]] = (
    *IDENTITY_COLUMNS,
    "prior_trade_date",
    "brent_contract_code",
    "brent_trade_date",
    "brent_open",
    "brent_high",
    "brent_low",
    "brent_close",
    "brent_volume",
)
CNYRUBF_REQUIRED: Final[tuple[str, ...]] = (
    *IDENTITY_COLUMNS,
    "prior_trade_date",
    "cnyrubf_security_id",
    "cnyrubf_trade_date",
    "cnyrubf_open",
    "cnyrubf_high",
    "cnyrubf_low",
    "cnyrubf_close",
    "cnyrubf_volume",
    "cnyrubf_volume_imbalance",
    "cnyrubf_open_interest_close",
)

BRENT_FEATURES: Final[tuple[str, ...]] = (
    "ext_brent_intraday_return",
    "ext_brent_range_pct",
    "ext_brent_close_location",
    "ext_log1p_brent_volume",
    "ext_brent_same_contract_close_return_1session",
    "ext_brent_same_contract_close_return_3session",
    "ext_brent_same_contract_close_return_5session",
)
CNYRUBF_FEATURES: Final[tuple[str, ...]] = (
    "ext_cnyrubf_intraday_return",
    "ext_cnyrubf_range_pct",
    "ext_cnyrubf_close_location",
    "ext_cnyrubf_log1p_volume",
    "ext_cnyrubf_volume_imbalance",
    "ext_cnyrubf_close_return_1session",
    "ext_cnyrubf_close_return_3session",
    "ext_cnyrubf_close_return_5session",
    "ext_cnyrubf_open_interest_change_1session",
)
INTERACTION_FEATURES: Final[tuple[str, ...]] = (
    "ext_brent_cnyrubf_intraday_interaction",
    "ext_brent_cnyrubf_return_1session_interaction",
)
LABEL_COLUMNS: Final[tuple[str, ...]] = (
    "fwd_usdrubf_close_return_1session",
    "fwd_usdrubf_close_return_3session",
    "fwd_usdrubf_close_return_5session",
    "fwd_usdrubf_close_return_10session",
)
FORBIDDEN_FEATURE_TOKENS: Final[tuple[str, ...]] = (
    "target_phase_label",
    "target_is_labeled",
    "target_source",
    "prediction",
    "probability",
    "candidate_y_pred",
    "future_",
    "fwd_usdrubf",
)


class OilFxRubFeatureError(ValueError):
    """Fail-closed error for STRAT_OIL_FX_RUB_V1 research feature construction."""


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...], label: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise OilFxRubFeatureError(
            f"{label} missing required columns: " + ", ".join(missing)
        )


def _date_strings(series: pd.Series, label: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.isna().any():
        raise OilFxRubFeatureError(f"{label} contains invalid date")
    return parsed.dt.strftime("%Y-%m-%d")


def prepare_identity_panel(identity_panel: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        identity_panel,
        (*IDENTITY_COLUMNS, "prior_trade_date"),
        "identity panel",
    )
    work = identity_panel.loc[:, (*IDENTITY_COLUMNS, "prior_trade_date")].copy()
    work["target_trade_date"] = _date_strings(
        work["target_trade_date"], "target_trade_date"
    )
    work["prior_trade_date"] = _date_strings(
        work["prior_trade_date"], "prior_trade_date"
    )
    instruments = work["target_instrument_id"].astype("string").str.strip()
    if instruments.isna().any() or instruments.eq("").any():
        raise OilFxRubFeatureError("identity panel contains empty instrument")
    work["target_instrument_id"] = instruments.astype(str)
    work = work.sort_values(list(IDENTITY_COLUMNS), kind="mergesort").reset_index(drop=True)
    if len(work) != EXPECTED_IDENTITY_COUNT:
        raise OilFxRubFeatureError(
            f"identity count must equal {EXPECTED_IDENTITY_COUNT}"
        )
    if work.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise OilFxRubFeatureError("duplicate identity")
    if set(work["target_instrument_id"]) != {EXPECTED_INSTRUMENT}:
        raise OilFxRubFeatureError("instrument identity mismatch")
    target = pd.to_datetime(work["target_trade_date"])
    prior = pd.to_datetime(work["prior_trade_date"])
    if not target.is_monotonic_increasing:
        raise OilFxRubFeatureError("target_trade_date must be increasing")
    if not (prior < target).all():
        raise OilFxRubFeatureError("prior_trade_date must precede target_trade_date")
    if not prior.is_monotonic_increasing:
        raise OilFxRubFeatureError("prior_trade_date must be increasing")
    return work


def _validate_identity_alignment(
    source: pd.DataFrame,
    identities: pd.DataFrame,
    *,
    label: str,
) -> pd.DataFrame:
    _require_columns(source, (*IDENTITY_COLUMNS, "prior_trade_date"), label)
    work = source.copy()
    work["target_trade_date"] = _date_strings(
        work["target_trade_date"], f"{label} target_trade_date"
    )
    work["prior_trade_date"] = _date_strings(
        work["prior_trade_date"], f"{label} prior_trade_date"
    )
    work["target_instrument_id"] = (
        work["target_instrument_id"].astype("string").str.strip().astype(str)
    )
    if work.duplicated(list(IDENTITY_COLUMNS), keep=False).any():
        raise OilFxRubFeatureError(f"{label} contains duplicate identity")
    work = work.sort_values(list(IDENTITY_COLUMNS), kind="mergesort").reset_index(drop=True)
    if len(work) != len(identities):
        raise OilFxRubFeatureError(f"{label} identity count mismatch")
    if not work.loc[:, IDENTITY_COLUMNS].equals(
        identities.loc[:, IDENTITY_COLUMNS]
    ):
        raise OilFxRubFeatureError(f"{label} identity or order mismatch")
    if not work["prior_trade_date"].equals(identities["prior_trade_date"]):
        raise OilFxRubFeatureError(f"{label} prior_trade_date mismatch")
    return work


def _finite_numeric(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    *,
    label: str,
) -> pd.DataFrame:
    numeric = frame.loc[:, columns].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any():
        raise OilFxRubFeatureError(f"{label} contains missing/non-numeric source values")
    values = numeric.to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise OilFxRubFeatureError(f"{label} contains non-finite source values")
    return numeric


def _validate_ohlcv(
    numeric: pd.DataFrame,
    *,
    prefix: str,
    volume_column: str,
) -> None:
    open_ = numeric[f"{prefix}_open"]
    high = numeric[f"{prefix}_high"]
    low = numeric[f"{prefix}_low"]
    close = numeric[f"{prefix}_close"]
    if open_.le(0).any() or close.le(0).any():
        raise OilFxRubFeatureError(f"{prefix} open/close must be positive")
    if high.lt(pd.concat([open_, close], axis=1).max(axis=1)).any():
        raise OilFxRubFeatureError(f"{prefix} high is inconsistent")
    if low.gt(pd.concat([open_, close], axis=1).min(axis=1)).any():
        raise OilFxRubFeatureError(f"{prefix} low is inconsistent")
    if high.lt(low).any():
        raise OilFxRubFeatureError(f"{prefix} high is below low")
    if numeric[volume_column].lt(0).any():
        raise OilFxRubFeatureError(f"{prefix} volume must be non-negative")


def _close_location(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
) -> np.ndarray:
    span = high - low
    return np.divide(
        close - low,
        span,
        out=np.full(len(close), 0.5, dtype=float),
        where=span != 0.0,
    )


def _same_contract_return(
    close: np.ndarray,
    contract_codes: list[str],
    lag: int,
) -> np.ndarray:
    result = np.full(len(close), np.nan, dtype=float)
    for index in range(lag, len(close)):
        window = contract_codes[index - lag : index + 1]
        if len(set(window)) != 1:
            continue
        result[index] = close[index] / close[index - lag] - 1.0
    return result


def _lag_return(values: np.ndarray, lag: int) -> np.ndarray:
    result = np.full(len(values), np.nan, dtype=float)
    if len(values) > lag:
        result[lag:] = values[lag:] / values[:-lag] - 1.0
    return result


def build_feature_frame(
    identity_panel: pd.DataFrame,
    brent_matrix: pd.DataFrame,
    *,
    mode: str = "brent_only",
    cnyrubf_matrix: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if mode not in {"brent_only", "oil_fx_full"}:
        raise OilFxRubFeatureError("mode must be brent_only or oil_fx_full")
    identities = prepare_identity_panel(identity_panel)

    _require_columns(brent_matrix, BRENT_REQUIRED, "Brent PIT matrix")
    brent = _validate_identity_alignment(brent_matrix, identities, label="Brent PIT matrix")
    brent["brent_trade_date"] = _date_strings(
        brent["brent_trade_date"], "Brent trade date"
    )
    if not brent["brent_trade_date"].equals(brent["prior_trade_date"]):
        raise OilFxRubFeatureError("Brent trade date must equal prior_trade_date")
    contract_codes = brent["brent_contract_code"].astype("string").str.strip()
    if contract_codes.isna().any() or contract_codes.eq("").any():
        raise OilFxRubFeatureError("explicit Brent contract code is required")
    brent_numeric_columns = (
        "brent_open",
        "brent_high",
        "brent_low",
        "brent_close",
        "brent_volume",
    )
    bnum = _finite_numeric(brent, brent_numeric_columns, label="Brent")
    _validate_ohlcv(bnum, prefix="brent", volume_column="brent_volume")
    bopen = bnum["brent_open"].to_numpy(float)
    bhigh = bnum["brent_high"].to_numpy(float)
    blow = bnum["brent_low"].to_numpy(float)
    bclose = bnum["brent_close"].to_numpy(float)
    bvolume = bnum["brent_volume"].to_numpy(float)

    features = identities.loc[:, IDENTITY_COLUMNS].copy()
    features["ext_brent_intraday_return"] = bclose / bopen - 1.0
    features["ext_brent_range_pct"] = (bhigh - blow) / bopen
    features["ext_brent_close_location"] = _close_location(bhigh, blow, bclose)
    features["ext_log1p_brent_volume"] = np.log1p(bvolume)
    codes = contract_codes.astype(str).tolist()
    for lag in (1, 3, 5):
        features[f"ext_brent_same_contract_close_return_{lag}session"] = (
            _same_contract_return(bclose, codes, lag)
        )

    if mode == "brent_only":
        if cnyrubf_matrix is not None:
            raise OilFxRubFeatureError("CNYRUBF input is forbidden in brent_only mode")
    else:
        if cnyrubf_matrix is None:
            raise OilFxRubFeatureError("oil_fx_full requires CNYRUBF matrix")
        _require_columns(cnyrubf_matrix, CNYRUBF_REQUIRED, "CNYRUBF PIT matrix")
        cny = _validate_identity_alignment(
            cnyrubf_matrix, identities, label="CNYRUBF PIT matrix"
        )
        cny["cnyrubf_trade_date"] = _date_strings(
            cny["cnyrubf_trade_date"], "CNYRUBF trade date"
        )
        if not cny["cnyrubf_trade_date"].equals(cny["prior_trade_date"]):
            raise OilFxRubFeatureError("CNYRUBF trade date must equal prior_trade_date")
        secids = cny["cnyrubf_security_id"].astype("string").str.strip()
        if secids.isna().any() or not secids.eq("CNYRUBF").all():
            raise OilFxRubFeatureError("CNYRUBF security identity mismatch")
        cny_numeric_columns = (
            "cnyrubf_open",
            "cnyrubf_high",
            "cnyrubf_low",
            "cnyrubf_close",
            "cnyrubf_volume",
            "cnyrubf_volume_imbalance",
            "cnyrubf_open_interest_close",
        )
        cnum = _finite_numeric(cny, cny_numeric_columns, label="CNYRUBF")
        _validate_ohlcv(cnum, prefix="cnyrubf", volume_column="cnyrubf_volume")
        imbalance = cnum["cnyrubf_volume_imbalance"].to_numpy(float)
        if ((imbalance < -1.0) | (imbalance > 1.0)).any():
            raise OilFxRubFeatureError("CNYRUBF volume imbalance must be within [-1, 1]")
        coi = cnum["cnyrubf_open_interest_close"].to_numpy(float)
        if (coi < 0.0).any():
            raise OilFxRubFeatureError("CNYRUBF open interest must be non-negative")
        copen = cnum["cnyrubf_open"].to_numpy(float)
        chigh = cnum["cnyrubf_high"].to_numpy(float)
        clow = cnum["cnyrubf_low"].to_numpy(float)
        cclose = cnum["cnyrubf_close"].to_numpy(float)
        cvolume = cnum["cnyrubf_volume"].to_numpy(float)
        features["ext_cnyrubf_intraday_return"] = cclose / copen - 1.0
        features["ext_cnyrubf_range_pct"] = (chigh - clow) / copen
        features["ext_cnyrubf_close_location"] = _close_location(chigh, clow, cclose)
        features["ext_cnyrubf_log1p_volume"] = np.log1p(cvolume)
        features["ext_cnyrubf_volume_imbalance"] = imbalance
        for lag in (1, 3, 5):
            features[f"ext_cnyrubf_close_return_{lag}session"] = _lag_return(cclose, lag)
        features["ext_cnyrubf_open_interest_change_1session"] = _lag_return(coi, 1)
        features["ext_brent_cnyrubf_intraday_interaction"] = (
            features["ext_brent_intraday_return"]
            * features["ext_cnyrubf_intraday_return"]
        )
        features["ext_brent_cnyrubf_return_1session_interaction"] = (
            features["ext_brent_same_contract_close_return_1session"]
            * features["ext_cnyrubf_close_return_1session"]
        )

    feature_columns = [
        column for column in features.columns if column not in IDENTITY_COLUMNS
    ]
    if any(
        token in column
        for column in feature_columns
        for token in FORBIDDEN_FEATURE_TOKENS
    ):
        raise OilFxRubFeatureError("forbidden target/future field entered feature artifact")
    numeric = features.loc[:, feature_columns].apply(pd.to_numeric, errors="coerce")
    finite_or_null = np.isfinite(numeric.to_numpy(float)) | np.isnan(
        numeric.to_numpy(float)
    )
    if not finite_or_null.all():
        raise OilFxRubFeatureError("derived feature contains infinite value")
    return features


def build_forward_return_labels(
    identity_panel: pd.DataFrame,
    usdrubf_d1: pd.DataFrame,
) -> pd.DataFrame:
    identities = prepare_identity_panel(identity_panel)
    _require_columns(usdrubf_d1, ("trade_date", "close"), "USDRUBF D1 panel")
    panel = usdrubf_d1.loc[:, ["trade_date", "close"]].copy()
    panel["trade_date"] = _date_strings(panel["trade_date"], "USDRUBF D1 trade_date")
    if panel.duplicated(["trade_date"], keep=False).any():
        raise OilFxRubFeatureError("USDRUBF D1 panel contains duplicate trade_date")
    panel = panel.sort_values("trade_date", kind="mergesort").reset_index(drop=True)
    close = pd.to_numeric(panel["close"], errors="coerce")
    if close.isna().any() or not np.isfinite(close.to_numpy(float)).all():
        raise OilFxRubFeatureError("USDRUBF D1 close must be finite")
    if close.le(0).any():
        raise OilFxRubFeatureError("USDRUBF D1 close must be positive")

    date_to_index = {date: index for index, date in enumerate(panel["trade_date"])}
    labels = identities.loc[:, IDENTITY_COLUMNS].copy()
    close_values = close.to_numpy(float)
    for horizon in (1, 3, 5, 10):
        values = np.full(len(identities), np.nan, dtype=float)
        for row_index, trade_date in enumerate(identities["target_trade_date"]):
            base_index = date_to_index.get(trade_date)
            if base_index is None:
                raise OilFxRubFeatureError(
                    f"USDRUBF D1 panel missing target date {trade_date}"
                )
            future_index = base_index + horizon
            if future_index < len(close_values):
                values[row_index] = (
                    close_values[future_index] / close_values[base_index] - 1.0
                )
        labels[f"fwd_usdrubf_close_return_{horizon}session"] = values
    return labels

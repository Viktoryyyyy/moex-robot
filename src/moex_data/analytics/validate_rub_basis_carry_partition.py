from __future__ import annotations

import math
from datetime import date
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = (
    "instrument_id",
    "pair_id",
    "trade_date",
    "ts",
    "spot_rate",
    "perpetual_rate",
    "front_rate",
    "next_rate",
    "perpetual_secid",
    "front_secid",
    "next_secid",
    "front_expiry_date",
    "next_expiry_date",
    "calendar_days_to_front_expiry",
    "calendar_days_to_next_expiry",
    "calendar_days_between_expiries",
    "perpetual_spot_basis_abs",
    "perpetual_spot_basis_bps",
    "front_spot_basis_abs",
    "front_spot_basis_bps",
    "next_spot_basis_abs",
    "next_spot_basis_bps",
    "front_perpetual_basis_abs",
    "front_perpetual_basis_bps",
    "next_perpetual_basis_abs",
    "next_perpetual_basis_bps",
    "front_next_spread_abs",
    "front_next_spread_bps",
    "front_spot_implied_carry_annualized",
    "next_spot_implied_carry_annualized",
    "front_next_term_carry_annualized",
    "alignment_policy",
    "build_ts",
)
RATE_COLUMNS = ("spot_rate", "perpetual_rate", "front_rate", "next_rate")
DERIVED_NUMERIC_COLUMNS = (
    "perpetual_spot_basis_abs",
    "perpetual_spot_basis_bps",
    "front_spot_basis_abs",
    "front_spot_basis_bps",
    "next_spot_basis_abs",
    "next_spot_basis_bps",
    "front_perpetual_basis_abs",
    "front_perpetual_basis_bps",
    "next_perpetual_basis_abs",
    "next_perpetual_basis_bps",
    "front_next_spread_abs",
    "front_next_spread_bps",
    "front_spot_implied_carry_annualized",
    "next_spot_implied_carry_annualized",
    "front_next_term_carry_annualized",
)
DAY_COLUMNS = (
    "calendar_days_to_front_expiry",
    "calendar_days_to_next_expiry",
    "calendar_days_between_expiries",
)
PAIR_IDENTITIES = {
    "usd_rub_basis_carry": ("USD/RUB", "USDRUBF"),
    "cny_rub_basis_carry": ("CNY/RUB", "CNYRUBF"),
}


class BasisCarryPartitionValidationError(ValueError):
    pass


def _fail(message: str) -> None:
    raise BasisCarryPartitionValidationError(message)


def _single_text(frame: pd.DataFrame, column: str) -> str:
    values = {str(value).strip() for value in frame[column].dropna().unique()}
    if len(values) != 1 or not next(iter(values), ""):
        _fail("derived partition must contain one non-empty value for " + column)
    return next(iter(values))


def _finite_numeric(frame: pd.DataFrame, column: str, *, positive: bool = False) -> pd.Series:
    numeric = pd.to_numeric(frame[column], errors="coerce")
    if numeric.isna().any():
        _fail("derived partition contains nonnumeric value: " + column)
    numeric = numeric.astype(float)
    if not numeric.map(math.isfinite).all():
        _fail("derived partition contains non-finite value: " + column)
    if positive and (numeric <= 0).any():
        _fail("derived partition rate must be finite and positive: " + column)
    return numeric


def _require_close(actual: pd.Series, expected: pd.Series, field_name: str) -> None:
    tolerance = 1e-10 + 1e-10 * expected.abs()
    if ((actual - expected).abs() > tolerance).any():
        _fail("derived partition formula mismatch: " + field_name)


def validate_partition(
    path: str | Path,
    *,
    expected_instrument_id: str,
    expected_trade_date: str,
    expected_row_count: int,
) -> dict[str, object]:
    partition_path = Path(path)
    if not partition_path.is_file():
        _fail("derived partition must be an existing regular file")
    try:
        frame = pd.read_parquet(partition_path)
    except Exception as exc:
        raise BasisCarryPartitionValidationError("derived partition is not readable parquet: " + str(exc)) from exc

    if len(frame.index) != expected_row_count or expected_row_count <= 0:
        _fail("derived partition row_count mismatch")
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        _fail("derived partition missing required columns: " + ",".join(missing))
    if frame[list(REQUIRED_COLUMNS)].isna().any().any():
        _fail("derived partition contains null required values")

    expected_pair = PAIR_IDENTITIES.get(expected_instrument_id)
    if expected_pair is None:
        _fail("expected instrument_id is outside Stage 4 pair scope")
    pair_id_expected, perpetual_secid_expected = expected_pair

    if _single_text(frame, "instrument_id") != expected_instrument_id:
        _fail("derived partition instrument_id mismatch")
    if _single_text(frame, "trade_date") != expected_trade_date:
        _fail("derived partition trade_date mismatch")
    if _single_text(frame, "pair_id") != pair_id_expected:
        _fail("derived partition pair_id mismatch")
    if _single_text(frame, "perpetual_secid") != perpetual_secid_expected:
        _fail("derived partition perpetual_secid mismatch")
    front_secid = _single_text(frame, "front_secid")
    next_secid = _single_text(frame, "next_secid")
    if front_secid.casefold() == next_secid.casefold():
        _fail("derived partition front and next SECIDs must be distinct")

    try:
        trade_date_value = date.fromisoformat(expected_trade_date)
        front_expiry = date.fromisoformat(_single_text(frame, "front_expiry_date"))
        next_expiry = date.fromisoformat(_single_text(frame, "next_expiry_date"))
    except ValueError as exc:
        raise BasisCarryPartitionValidationError("derived partition date metadata must be YYYY-MM-DD") from exc
    if front_expiry <= trade_date_value:
        _fail("derived partition front expiry must be strictly after trade_date")
    if next_expiry <= front_expiry:
        _fail("derived partition next expiry must be after front expiry")

    expected_days = {
        "calendar_days_to_front_expiry": (front_expiry - trade_date_value).days,
        "calendar_days_to_next_expiry": (next_expiry - trade_date_value).days,
        "calendar_days_between_expiries": (next_expiry - front_expiry).days,
    }
    day_values: dict[str, pd.Series] = {}
    for column in DAY_COLUMNS:
        numeric = _finite_numeric(frame, column, positive=True)
        if not numeric.eq(float(expected_days[column])).all():
            _fail("derived partition expiry day-count mismatch: " + column)
        day_values[column] = numeric

    if not frame["alignment_policy"].astype(str).eq("exact_timestamp_inner_join").all():
        _fail("derived partition alignment_policy mismatch")

    parsed_ts = pd.to_datetime(frame["ts"], errors="coerce")
    if parsed_ts.isna().any():
        _fail("derived partition contains invalid ts")
    timezone_value = getattr(parsed_ts.dt, "tz", None)
    if timezone_value is None:
        _fail("derived partition ts must be timezone-aware")
    utc_ts = parsed_ts.dt.tz_convert("UTC")
    if utc_ts.duplicated().any():
        _fail("derived partition contains duplicate ts")
    if not utc_ts.is_monotonic_increasing:
        _fail("derived partition ts must be monotonic increasing")

    build_ts = pd.to_datetime(frame["build_ts"], errors="coerce", utc=True)
    if build_ts.isna().any():
        _fail("derived partition contains invalid build_ts")

    rates = {column: _finite_numeric(frame, column, positive=True) for column in RATE_COLUMNS}
    derived_values = {column: _finite_numeric(frame, column) for column in DERIVED_NUMERIC_COLUMNS}

    spot = rates["spot_rate"]
    perpetual = rates["perpetual_rate"]
    front = rates["front_rate"]
    nxt = rates["next_rate"]
    expected_formulas = {
        "perpetual_spot_basis_abs": perpetual - spot,
        "perpetual_spot_basis_bps": ((perpetual / spot) - 1.0) * 10000.0,
        "front_spot_basis_abs": front - spot,
        "front_spot_basis_bps": ((front / spot) - 1.0) * 10000.0,
        "next_spot_basis_abs": nxt - spot,
        "next_spot_basis_bps": ((nxt / spot) - 1.0) * 10000.0,
        "front_perpetual_basis_abs": front - perpetual,
        "front_perpetual_basis_bps": ((front / perpetual) - 1.0) * 10000.0,
        "next_perpetual_basis_abs": nxt - perpetual,
        "next_perpetual_basis_bps": ((nxt / perpetual) - 1.0) * 10000.0,
        "front_next_spread_abs": nxt - front,
        "front_next_spread_bps": ((nxt / front) - 1.0) * 10000.0,
        "front_spot_implied_carry_annualized": ((front / spot) - 1.0) * 365.0 / day_values["calendar_days_to_front_expiry"],
        "next_spot_implied_carry_annualized": ((nxt / spot) - 1.0) * 365.0 / day_values["calendar_days_to_next_expiry"],
        "front_next_term_carry_annualized": ((nxt / front) - 1.0) * 365.0 / day_values["calendar_days_between_expiries"],
    }
    for column, expected in expected_formulas.items():
        _require_close(derived_values[column], expected.astype(float), column)

    return {
        "row_count": int(len(frame.index)),
        "instrument_id": expected_instrument_id,
        "trade_date": expected_trade_date,
        "required_column_count": len(REQUIRED_COLUMNS),
        "required_schema_complete": True,
        "first_ts_utc": utc_ts.iloc[0].isoformat(),
        "last_ts_utc": utc_ts.iloc[-1].isoformat(),
        "duplicate_ts_count": int(utc_ts.duplicated().sum()),
        "rates_valid": True,
        "derived_metrics_valid": True,
        "derived_formulas_valid": True,
        "expiry_metadata_valid": True,
        "physical_readback_passed": True,
    }

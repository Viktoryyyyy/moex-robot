from __future__ import annotations

import json
import math
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import pandas as pd

DATASET_ID: Final[str] = "rub_basis_carry_5m"
SCHEMA_VERSION: Final[str] = "rub_basis_carry_5m.v1"
PRODUCER_ID: Final[str] = "moex_data.analytics.materialize_rub_basis_carry_5m.v1"
ALIGNMENT_POLICY: Final[str] = "exact_timestamp_inner_join"
MARKET_TIMEZONE: Final[str] = "Europe/Moscow"
TIMESTAMP_POLICY: Final[str] = "naive_exchange_localize_europe_moscow_then_utc"


class BasisCarryMaterializationError(ValueError):
    pass


@dataclass(frozen=True)
class PairSpec:
    instrument_id: str
    pair_id: str
    spot_instrument_id: str
    perpetual_instrument_id: str
    perpetual_secid: str
    front_instrument_id: str
    next_instrument_id: str
    root: str
    expiring_price_divisor: float


PAIR_SPECS: Final[dict[str, PairSpec]] = {
    "usd_rub_basis_carry": PairSpec(
        instrument_id="usd_rub_basis_carry",
        pair_id="USD/RUB",
        spot_instrument_id="usd_tom",
        perpetual_instrument_id="usdrubf_futures_family",
        perpetual_secid="USDRUBF",
        front_instrument_id="si_front_contract",
        next_instrument_id="si_next_contract",
        root="Si",
        expiring_price_divisor=1000.0,
    ),
    "cny_rub_basis_carry": PairSpec(
        instrument_id="cny_rub_basis_carry",
        pair_id="CNY/RUB",
        spot_instrument_id="cny_tom",
        perpetual_instrument_id="cnyrubf_futures_family",
        perpetual_secid="CNYRUBF",
        front_instrument_id="cr_front_contract",
        next_instrument_id="cr_next_contract",
        root="CR",
        expiring_price_divisor=1.0,
    ),
}


def _fail(message: str) -> None:
    raise BasisCarryMaterializationError(message)


def _require_date(value: object, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value or "").strip())
    except ValueError as exc:
        raise BasisCarryMaterializationError(field_name + " must be YYYY-MM-DD") from exc


def _require_text(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    return text


def _validate_binding(
    binding: Mapping[str, object],
    *,
    expected_root: str,
    expected_role: str,
    expected_instrument_id: str,
    trade_date: date,
) -> tuple[str, date]:
    if _require_text(binding.get("root"), "binding.root") != expected_root:
        _fail("binding root mismatch")
    if _require_text(binding.get("role"), "binding.role") != expected_role:
        _fail("binding role mismatch")
    if _require_text(binding.get("instrument_id"), "binding.instrument_id") != expected_instrument_id:
        _fail("binding instrument_id mismatch")
    if _require_text(binding.get("as_of_date"), "binding.as_of_date") != trade_date.isoformat():
        _fail("binding as_of_date must equal trade_date")
    secid = _require_text(binding.get("secid"), "binding.secid")
    expiry = _require_date(binding.get("last_trade_date"), "binding.last_trade_date")
    return secid, expiry


def _normalize_market_ts_value(value: object) -> pd.Timestamp | pd.NaT:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return pd.NaT
    if pd.isna(timestamp):
        return pd.NaT
    try:
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(MARKET_TIMEZONE)
        return timestamp.tz_convert("UTC")
    except (TypeError, ValueError):
        return pd.NaT


def _prepare_rate_frame(
    frame: pd.DataFrame,
    *,
    expected_instrument_id: str,
    trade_date: date,
    label: str,
    price_divisor: float = 1.0,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        _fail(label + " input frame must be non-empty")
    missing = [column for column in ("instrument_id", "trade_date", "ts", "close") if column not in frame.columns]
    if missing:
        _fail(label + " input frame missing columns: " + ",".join(missing))
    if not math.isfinite(price_divisor) or price_divisor <= 0:
        _fail(label + " price_divisor must be positive")

    instrument_values = {str(value).strip() for value in frame["instrument_id"].dropna().unique()}
    if instrument_values != {expected_instrument_id}:
        _fail(label + " instrument_id mismatch")
    trade_dates = {str(value).strip() for value in frame["trade_date"].dropna().unique()}
    if trade_dates != {trade_date.isoformat()}:
        _fail(label + " trade_date mismatch")

    result = frame[["ts", "close"]].copy()
    result["ts"] = pd.to_datetime(
        result["ts"].map(_normalize_market_ts_value), utc=True, errors="coerce"
    )
    if result["ts"].isna().any():
        _fail(label + " contains invalid ts")
    if result["ts"].duplicated().any():
        _fail(label + " contains duplicate ts")
    numeric = pd.to_numeric(result["close"], errors="coerce")
    if numeric.isna().any():
        _fail(label + " contains nonnumeric close")
    numeric = numeric.astype(float) / float(price_divisor)
    if (numeric <= 0).any() or not numeric.map(math.isfinite).all():
        _fail(label + " normalized rates must be finite and positive")
    result[label + "_rate"] = numeric
    return result[["ts", label + "_rate"]].sort_values("ts", kind="stable").reset_index(drop=True)


def _bps(comparison: pd.Series, reference: pd.Series) -> pd.Series:
    return ((comparison / reference) - 1.0) * 10000.0


def build_basis_carry_frame(
    *,
    instrument_id: str,
    trade_date: str,
    spot_frame: pd.DataFrame,
    perpetual_frame: pd.DataFrame,
    front_frame: pd.DataFrame,
    next_frame: pd.DataFrame,
    front_binding: Mapping[str, object],
    next_binding: Mapping[str, object],
    build_ts: str | None = None,
) -> pd.DataFrame:
    spec = PAIR_SPECS.get(str(instrument_id).strip())
    if spec is None:
        _fail("instrument_id is outside Stage 4 pair scope")
    checked_trade_date = _require_date(trade_date, "trade_date")
    front_secid, front_expiry = _validate_binding(
        front_binding,
        expected_root=spec.root,
        expected_role="front",
        expected_instrument_id=spec.front_instrument_id,
        trade_date=checked_trade_date,
    )
    next_secid, next_expiry = _validate_binding(
        next_binding,
        expected_root=spec.root,
        expected_role="next",
        expected_instrument_id=spec.next_instrument_id,
        trade_date=checked_trade_date,
    )
    if front_expiry < checked_trade_date:
        _fail("front expiry cannot precede trade_date")
    if next_expiry <= front_expiry:
        _fail("next expiry must be after front expiry")

    spot = _prepare_rate_frame(
        spot_frame,
        expected_instrument_id=spec.spot_instrument_id,
        trade_date=checked_trade_date,
        label="spot",
    )
    perpetual = _prepare_rate_frame(
        perpetual_frame,
        expected_instrument_id=spec.perpetual_instrument_id,
        trade_date=checked_trade_date,
        label="perpetual",
    )
    front = _prepare_rate_frame(
        front_frame,
        expected_instrument_id=spec.front_instrument_id,
        trade_date=checked_trade_date,
        label="front",
        price_divisor=spec.expiring_price_divisor,
    )
    nxt = _prepare_rate_frame(
        next_frame,
        expected_instrument_id=spec.next_instrument_id,
        trade_date=checked_trade_date,
        label="next",
        price_divisor=spec.expiring_price_divisor,
    )

    merged = spot.merge(perpetual, on="ts", how="inner", validate="one_to_one")
    merged = merged.merge(front, on="ts", how="inner", validate="one_to_one")
    merged = merged.merge(nxt, on="ts", how="inner", validate="one_to_one")
    merged = merged.sort_values("ts", kind="stable").reset_index(drop=True)
    if merged.empty:
        _fail("exact 5m timestamp intersection is empty")
    if merged["ts"].duplicated().any() or not merged["ts"].is_monotonic_increasing:
        _fail("derived timestamp identity is invalid")

    days_front = (front_expiry - checked_trade_date).days
    days_next = (next_expiry - checked_trade_date).days
    days_term = (next_expiry - front_expiry).days
    if days_front <= 0 or days_next <= 0 or days_term <= 0:
        _fail("carry annualization requires strictly positive expiry horizons")

    result = pd.DataFrame(
        {
            "instrument_id": spec.instrument_id,
            "pair_id": spec.pair_id,
            "trade_date": checked_trade_date.isoformat(),
            "ts": merged["ts"],
            "spot_rate": merged["spot_rate"],
            "perpetual_rate": merged["perpetual_rate"],
            "front_rate": merged["front_rate"],
            "next_rate": merged["next_rate"],
            "perpetual_secid": spec.perpetual_secid,
            "front_secid": front_secid,
            "next_secid": next_secid,
            "front_expiry_date": front_expiry.isoformat(),
            "next_expiry_date": next_expiry.isoformat(),
            "calendar_days_to_front_expiry": days_front,
            "calendar_days_to_next_expiry": days_next,
            "calendar_days_between_expiries": days_term,
        }
    )
    result["perpetual_spot_basis_abs"] = result["perpetual_rate"] - result["spot_rate"]
    result["perpetual_spot_basis_bps"] = _bps(result["perpetual_rate"], result["spot_rate"])
    result["front_spot_basis_abs"] = result["front_rate"] - result["spot_rate"]
    result["front_spot_basis_bps"] = _bps(result["front_rate"], result["spot_rate"])
    result["next_spot_basis_abs"] = result["next_rate"] - result["spot_rate"]
    result["next_spot_basis_bps"] = _bps(result["next_rate"], result["spot_rate"])
    result["front_perpetual_basis_abs"] = result["front_rate"] - result["perpetual_rate"]
    result["front_perpetual_basis_bps"] = _bps(result["front_rate"], result["perpetual_rate"])
    result["next_perpetual_basis_abs"] = result["next_rate"] - result["perpetual_rate"]
    result["next_perpetual_basis_bps"] = _bps(result["next_rate"], result["perpetual_rate"])
    result["front_next_spread_abs"] = result["next_rate"] - result["front_rate"]
    result["front_next_spread_bps"] = _bps(result["next_rate"], result["front_rate"])
    result["front_spot_implied_carry_annualized"] = (
        (result["front_rate"] / result["spot_rate"] - 1.0) * 365.0 / float(days_front)
    )
    result["next_spot_implied_carry_annualized"] = (
        (result["next_rate"] / result["spot_rate"] - 1.0) * 365.0 / float(days_next)
    )
    result["front_next_term_carry_annualized"] = (
        (result["next_rate"] / result["front_rate"] - 1.0) * 365.0 / float(days_term)
    )
    result["alignment_policy"] = ALIGNMENT_POLICY
    result["build_ts"] = build_ts or datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    numeric_columns = [
        column
        for column in result.columns
        if column.endswith("_rate")
        or column.endswith("_abs")
        or column.endswith("_bps")
        or column.endswith("_annualized")
    ]
    if result[numeric_columns].isna().any().any():
        _fail("derived metrics contain null values")
    for column in numeric_columns:
        if not result[column].astype(float).map(math.isfinite).all():
            _fail("derived metrics contain non-finite values: " + column)
    return result


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        _fail("MOEX_DATA_ROOT is required")
    root = Path(value)
    if not root.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    return root


def _write_json_atomic(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".stage") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        staged = Path(handle.name)
    staged.replace(path)


def materialize_pair_partition(
    *,
    instrument_id: str,
    trade_date: str,
    artifact_version: str,
    spot_frame: pd.DataFrame,
    perpetual_frame: pd.DataFrame,
    front_frame: pd.DataFrame,
    next_frame: pd.DataFrame,
    front_binding: Mapping[str, object],
    next_binding: Mapping[str, object],
    input_lineage: Mapping[str, object],
) -> dict[str, object]:
    spec = PAIR_SPECS.get(str(instrument_id).strip())
    if spec is None:
        _fail("instrument_id is outside Stage 4 pair scope")
    checked_trade_date = _require_date(trade_date, "trade_date").isoformat()
    checked_version = _require_text(artifact_version, "artifact_version")
    frame = build_basis_carry_frame(
        instrument_id=spec.instrument_id,
        trade_date=checked_trade_date,
        spot_frame=spot_frame,
        perpetual_frame=perpetual_frame,
        front_frame=front_frame,
        next_frame=next_frame,
        front_binding=front_binding,
        next_binding=next_binding,
    )

    root = _data_root()
    partition = root / "market" / "derived" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + spec.instrument_id) / ("trade_date=" + checked_trade_date) / "part.parquet"
    manifest = root / "state" / "refresh" / ("dataset_id=" + DATASET_ID) / ("run_date=" + checked_trade_date) / ("run_id=" + checked_version) / ("instrument_id=" + spec.instrument_id) / "manifest.json"
    quality = root / "state" / "quality" / ("dataset_id=" + DATASET_ID) / ("run_date=" + checked_trade_date) / ("run_id=" + checked_version) / ("instrument_id=" + spec.instrument_id) / "quality_report.json"
    partition.parent.mkdir(parents=True, exist_ok=True)
    staged_partition = partition.with_name(partition.name + ".stage")
    if staged_partition.exists():
        staged_partition.unlink()
    frame.to_parquet(staged_partition, index=False)
    staged_partition.replace(partition)

    quality_values: dict[str, object] = {
        "dataset_id": DATASET_ID,
        "schema_version": SCHEMA_VERSION,
        "instrument_id": spec.instrument_id,
        "trade_date": checked_trade_date,
        "run_id": checked_version,
        "quality_status": "pass",
        "row_count": int(len(frame.index)),
        "duplicate_ts_count": int(frame["ts"].duplicated().sum()),
        "monotonic_ts": bool(frame["ts"].is_monotonic_increasing),
        "exact_timestamp_inner_join": True,
        "timestamp_policy": TIMESTAMP_POLICY,
        "forward_fill_used": False,
        "asof_join_used": False,
        "positive_rate_check": bool((frame[["spot_rate", "perpetual_rate", "front_rate", "next_rate"]] > 0).all().all()),
        "non_null_derived_metrics": True,
    }
    if quality_values["row_count"] <= 0 or quality_values["duplicate_ts_count"] != 0 or quality_values["monotonic_ts"] is not True or quality_values["positive_rate_check"] is not True:
        _fail("derived quality gate failed")
    _write_json_atomic(quality, quality_values)

    manifest_values: dict[str, object] = {
        "dataset_id": DATASET_ID,
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_ID,
        "instrument_id": spec.instrument_id,
        "pair_id": spec.pair_id,
        "trade_date": checked_trade_date,
        "run_id": checked_version,
        "refresh_status": "succeeded",
        "quality_status": "pass",
        "row_count": int(len(frame.index)),
        "partition_path": partition.as_posix(),
        "quality_report_path": quality.as_posix(),
        "alignment_policy": ALIGNMENT_POLICY,
        "timestamp_policy": TIMESTAMP_POLICY,
        "forward_fill_used": False,
        "asof_join_used": False,
        "continuous_series_used": False,
        "input_lineage": dict(input_lineage),
    }
    _write_json_atomic(manifest, manifest_values)
    return {
        "dataset_id": DATASET_ID,
        "instrument_id": spec.instrument_id,
        "pair_id": spec.pair_id,
        "trade_date": checked_trade_date,
        "run_id": checked_version,
        "partition_path": partition.as_posix(),
        "manifest_path": manifest.as_posix(),
        "quality_report_path": quality.as_posix(),
        "quality_status": "pass",
        "row_count": int(len(frame.index)),
        "alignment_policy": ALIGNMENT_POLICY,
        "timestamp_policy": TIMESTAMP_POLICY,
        "forward_fill_used": False,
        "asof_join_used": False,
        "continuous_series_used": False,
    }

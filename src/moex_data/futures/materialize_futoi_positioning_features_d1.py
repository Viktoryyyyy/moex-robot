from __future__ import annotations

import json
import math
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd

DATASET_ID: Final[str] = "futures_futoi_positioning_features_d1"
SOURCE_DATASET_ID: Final[str] = "futures_futoi_eod"
BASE_FIELDS: Final[tuple[str, ...]] = (
    "phys_net_share_of_oi",
    "phys_gross_share_of_two_sided_oi",
    "phys_long_num",
    "phys_short_num",
    "phys_avg_long_per_participant",
    "phys_avg_short_per_participant",
)
WINDOWS: Final[tuple[int, ...]] = (252, 504)
LAGS: Final[tuple[int, ...]] = (1, 5)


class FutoiPositioningFeaturesError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiPositioningFeaturesError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def _validate_source(frame: pd.DataFrame, instrument_id: str) -> pd.DataFrame:
    required = ["instrument_id", "trade_date", "snapshot_ts_utc", "availability_ts_utc", *BASE_FIELDS]
    missing = [field for field in required if field not in frame.columns]
    if missing:
        _fail("EOD source missing required columns: " + ",".join(missing))
    if frame.empty:
        _fail("EOD source is empty")
    work = frame.copy().sort_values("trade_date").reset_index(drop=True)
    if set(work["instrument_id"].astype(str).str.strip().unique()) != {instrument_id}:
        _fail("EOD source instrument_id mismatch")
    parsed_dates = pd.to_datetime(work["trade_date"], errors="coerce")
    if bool(parsed_dates.isna().any()) or not parsed_dates.is_monotonic_increasing:
        _fail("EOD source trade_date must be valid and monotonic")
    if work.duplicated(subset=["instrument_id", "trade_date"]).any():
        _fail("EOD source contains duplicate instrument/trade_date")
    for field in BASE_FIELDS:
        numeric = pd.to_numeric(work[field], errors="coerce")
        invalid = numeric.notna() & ~numeric.astype(float).map(math.isfinite)
        if bool(invalid.any()):
            _fail("EOD source contains nonfinite base feature: " + field)
        work[field] = numeric.astype(float)
    work["snapshot_ts_utc"] = pd.to_datetime(work["snapshot_ts_utc"], errors="coerce", utc=True)
    work["availability_ts_utc"] = pd.to_datetime(work["availability_ts_utc"], errors="coerce", utc=True)
    if bool(work[["snapshot_ts_utc", "availability_ts_utc"]].isna().any().any()):
        _fail("EOD source contains invalid timestamp metadata")
    return work


def _rolling_percentile(values: np.ndarray) -> float:
    current = values[-1]
    if np.isnan(values).any() or not np.isfinite(current):
        return np.nan
    return float(np.mean(values <= current))


def build_features(frame: pd.DataFrame, *, instrument_id: str) -> pd.DataFrame:
    instrument = _safe_token(instrument_id, "instrument_id")
    work = _validate_source(frame, instrument)
    result = work[["instrument_id", "trade_date", "snapshot_ts_utc", "availability_ts_utc", *BASE_FIELDS]].copy()
    for field in BASE_FIELDS:
        source = result[field].astype(float)
        for lag in LAGS:
            result[f"{field}_chg_{lag}obs"] = source - source.shift(lag)
        for window in WINDOWS:
            rolling = source.rolling(window=window, min_periods=window)
            mean = rolling.mean()
            std = rolling.std(ddof=0)
            z = (source - mean) / std
            z = z.mask(std.eq(0.0))
            result[f"{field}_zscore_{window}obs"] = z
            result[f"{field}_pctile_{window}obs"] = rolling.apply(_rolling_percentile, raw=True)
    for column in result.columns:
        if column.endswith("_pctile_252obs") or column.endswith("_pctile_504obs"):
            valid = result[column].dropna()
            if bool(((valid < 0.0) | (valid > 1.0)).any()):
                _fail("rolling percentile escaped unit interval")
        if any(token in column for token in ("_chg_", "_zscore_", "_pctile_")):
            valid = pd.to_numeric(result[column], errors="coerce").dropna().astype(float)
            if not valid.map(math.isfinite).all():
                _fail("derived positioning feature is nonfinite: " + column)
    return result.reset_index(drop=True)


def _atomic_json(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        temp = Path(handle.name)
    temp.replace(path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name("." + path.name + ".tmp")
    try:
        frame.to_parquet(temp, index=False)
        temp.replace(path)
    finally:
        if temp.exists():
            temp.unlink()


def materialize_features(
    *,
    eod_partition: str | Path,
    output_root: str | Path,
    instrument_id: str,
    run_id: str,
) -> dict[str, object]:
    checked_instrument = _safe_token(instrument_id, "instrument_id")
    checked_run = _safe_token(run_id, "run_id")
    source_path = Path(eod_partition).resolve()
    if not source_path.is_file():
        _fail("EOD source partition does not exist")
    source = pd.read_parquet(source_path)
    frame = build_features(source, instrument_id=checked_instrument)
    out_root = Path(output_root).resolve()
    base = out_root / "market" / "derived" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + checked_instrument)
    partition = base / ("run_id=" + checked_run) / "part.parquet"
    manifest = out_root / "state" / "refresh" / ("dataset_id=" + DATASET_ID) / ("run_id=" + checked_run) / ("instrument_id=" + checked_instrument) / "manifest.json"
    quality = out_root / "state" / "quality" / ("dataset_id=" + DATASET_ID) / ("run_id=" + checked_run) / ("instrument_id=" + checked_instrument) / "quality_report.json"
    for target in (partition, manifest, quality):
        if target.exists():
            _fail("immutable Stage 5 feature target already exists")

    feature_columns = [c for c in frame.columns if any(token in c for token in ("_chg_", "_zscore_", "_pctile_"))]
    quality_values = {
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "run_id": checked_run,
        "quality_status": "pass",
        "row_count": int(len(frame.index)),
        "feature_column_count": len(feature_columns),
        "min_trade_date": str(frame["trade_date"].min()),
        "max_trade_date": str(frame["trade_date"].max()),
        "duplicate_identity_count": int(frame.duplicated(subset=["instrument_id", "trade_date"]).sum()),
        "causal_current_and_past_only": True,
        "historical_pit_research_ready_claimed": False,
        "windows_observations": list(WINDOWS),
        "change_lags_observations": list(LAGS),
    }
    manifest_values = {
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "run_id": checked_run,
        "row_count": int(len(frame.index)),
        "quality_status": "pass",
        "partition_path": partition.as_posix(),
        "quality_report_path": quality.as_posix(),
        "source_eod_partition_path": source_path.as_posix(),
        "producer": "moex_data.futures.materialize_futoi_positioning_features_d1.v1",
        "feature_columns": feature_columns,
        "network_calls_used": False,
        "future_rows_used": False,
        "historical_pit_research_ready_claimed": False,
        "build_ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    _atomic_parquet(partition, frame)
    _atomic_json(quality, quality_values)
    _atomic_json(manifest, manifest_values)
    return {
        "status": "succeeded",
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "run_id": checked_run,
        "row_count": int(len(frame.index)),
        "quality_status": "pass",
        "feature_column_count": len(feature_columns),
        "partition_path": partition.as_posix(),
        "manifest_path": manifest.as_posix(),
        "quality_report_path": quality.as_posix(),
        "source_eod_partition_path": source_path.as_posix(),
        "network_calls_used": False,
        "future_rows_used": False,
        "historical_pit_research_ready_claimed": False,
    }

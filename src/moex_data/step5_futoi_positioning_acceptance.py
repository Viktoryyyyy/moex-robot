from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd

from moex_data.futures import materialize_futoi_instrument as futoi_materializer
from moex_data.futures import stage2_raw_history_acceptance as raw_acceptance
from moex_data.futures.materialize_futoi_eod import (
    FROZEN_INPUT_PRODUCER,
    FROZEN_INPUT_SCHEMA,
    SOURCE_DATASET_ID,
    SOURCE_ID,
    _accepted_history_scope,
    raw_partition_path,
)
from moex_data.futures.materialize_futoi_positioning_features_d1 import BASE_FIELDS, LAGS, WINDOWS

CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
CONTRACT_ID: Final[str] = "step5_futoi_positioning_acceptance.v1"
EXPECTED_ROWS: Final[dict[str, int]] = {"si_futures_family": 1757, "cr_futures_family": 1177}
EXPECTED_INSTRUMENTS: Final[frozenset[str]] = frozenset(EXPECTED_ROWS)
EOD_DATASET: Final[str] = "futures_futoi_eod"
FEATURE_DATASET: Final[str] = "futures_futoi_positioning_features_d1"
RAW_DATASET: Final[str] = "futures_futoi_raw"
ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
EOD_REQUIRED: Final[tuple[str, ...]] = (
    "instrument_id", "trade_date", "snapshot_ts_msk", "snapshot_ts_utc", "availability_ts_utc",
    "phys_sess_id", "phys_seqnum", "phys_systime_utc", "legal_sess_id", "legal_seqnum", "legal_systime_utc",
    "phys_net", "phys_long", "phys_short_abs", "phys_long_num", "phys_short_num",
    "legal_net", "legal_long", "legal_short_abs", "legal_long_num", "legal_short_num",
    "total_open_interest", "total_short_abs", "phys_gross", "legal_gross",
    "phys_long_share_of_oi", "phys_short_share_of_oi", "phys_net_share_of_oi",
    "legal_long_share_of_oi", "legal_short_share_of_oi", "legal_net_share_of_oi",
    "phys_gross_share_of_two_sided_oi", "legal_gross_share_of_two_sided_oi",
    "phys_avg_long_per_participant", "phys_avg_short_per_participant",
    "legal_avg_long_per_participant", "legal_avg_short_per_participant",
    "source_row_count", "source_revision_rows_dropped", "source_partition_ref",
    "source_canonical_partition_ref", "source_frozen_partition_sha256",
)


class Step5AcceptanceError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step5AcceptanceError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.is_file():
        _fail("env_file does not exist")
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        _fail("MOEX_DATA_ROOT is required")
    root = Path(value)
    if not root.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    return root.resolve()


def _run_root(run_id: str) -> Path:
    return _data_root() / "runs" / "step5_futoi_positioning" / ("run_id=" + _safe_token(run_id, "run_id"))


def _evidence_dir(run_id: str) -> Path:
    return _data_root() / "state" / "acceptance" / "step5_futoi_positioning" / ("run_id=" + _safe_token(run_id, "run_id"))


def _load_json(path: Path, field: str) -> Mapping[str, object]:
    if not path.is_file() or path.is_symlink():
        _fail(field + " does not exist as regular non-symlink file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise Step5AcceptanceError(field + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(value, Mapping):
        _fail(field + " must be a JSON object")
    return value


def _artifact_path(value: object, run_root: Path, field: str) -> Path:
    path = Path(str(value or ""))
    if not path.is_absolute():
        _fail(field + " must be absolute")
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(run_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step5AcceptanceError(field + " must be a file inside immutable run root") from exc
    if not resolved.is_file() or resolved.is_symlink():
        _fail(field + " must be a regular non-symlink file")
    return resolved


def _expand_root_ref(value: object, field: str, *, require_run_root: Path | None = None) -> Path:
    text = str(value or "").strip()
    if not text.startswith(ROOT_PREFIX):
        _fail(field + " must be a ${MOEX_DATA_ROOT} rooted reference")
    relative = text[len(ROOT_PREFIX):]
    if not relative or relative.startswith("/") or ".." in Path(relative).parts:
        _fail(field + " contains invalid rooted path")
    root = _data_root().resolve(strict=True)
    path = (root / relative).resolve(strict=True)
    try:
        path.relative_to(root)
        if require_run_root is not None:
            path.relative_to(require_run_root.resolve(strict=True))
    except ValueError as exc:
        raise Step5AcceptanceError(field + " escaped approved root") from exc
    if not path.is_file() or path.is_symlink():
        _fail(field + " must resolve to a regular non-symlink file")
    return path


def _finite(series: pd.Series, field: str, *, allow_null: bool = False) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if not allow_null and bool(numeric.isna().any()):
        _fail("nonnumeric/null required field: " + field)
    valid = numeric.dropna().astype(float)
    if not valid.map(math.isfinite).all():
        _fail("nonfinite field: " + field)
    return numeric


def _integral(series: pd.Series, field: str, *, nonnegative: bool = False) -> pd.Series:
    numeric = _finite(series, field).astype(float)
    rounded = numeric.round()
    if not np.allclose(numeric.to_numpy(), rounded.to_numpy(), rtol=0.0, atol=0.0):
        _fail("nonintegral field: " + field)
    if nonnegative and bool((rounded < 0).any()):
        _fail("negative field: " + field)
    return rounded.astype("int64")


def _require_allclose(actual: pd.Series | np.ndarray, expected: pd.Series | np.ndarray, field: str) -> None:
    left = np.asarray(actual, dtype=float)
    right = np.asarray(expected, dtype=float)
    if not np.allclose(left, right, equal_nan=True, rtol=1e-10, atol=1e-12):
        _fail("derived metric formula mismatch: " + field)


def _validate_average(frame: pd.DataFrame, position_field: str, count_field: str, average_field: str) -> None:
    position = _integral(frame[position_field], position_field, nonnegative=True).astype(float)
    count = _integral(frame[count_field], count_field, nonnegative=True).astype(float)
    actual = _finite(frame[average_field], average_field, allow_null=True).astype(float)
    zero_count = count.eq(0.0)
    if bool((zero_count & position.ne(0.0)).any()):
        _fail("nonzero position with zero participant count: " + average_field)
    expected = position / count.replace(0.0, np.nan)
    if bool(actual[zero_count].notna().any()):
        _fail("average must be null when participant count is zero: " + average_field)
    _require_allclose(actual, expected, average_field)


def _validate_eod_metrics(frame: pd.DataFrame) -> None:
    phys_net = _integral(frame["phys_net"], "phys_net").astype(float)
    legal_net = _integral(frame["legal_net"], "legal_net").astype(float)
    phys_long = _integral(frame["phys_long"], "phys_long", nonnegative=True).astype(float)
    legal_long = _integral(frame["legal_long"], "legal_long", nonnegative=True).astype(float)
    phys_short = _integral(frame["phys_short_abs"], "phys_short_abs", nonnegative=True).astype(float)
    legal_short = _integral(frame["legal_short_abs"], "legal_short_abs", nonnegative=True).astype(float)
    total_oi = _integral(frame["total_open_interest"], "total_open_interest", nonnegative=True).astype(float)
    total_short = _integral(frame["total_short_abs"], "total_short_abs", nonnegative=True).astype(float)
    phys_gross = _integral(frame["phys_gross"], "phys_gross", nonnegative=True).astype(float)
    legal_gross = _integral(frame["legal_gross"], "legal_gross", nonnegative=True).astype(float)
    for field in ("phys_long_num", "phys_short_num", "legal_long_num", "legal_short_num", "source_row_count", "source_revision_rows_dropped"):
        _integral(frame[field], field, nonnegative=True)

    if bool((total_oi <= 0).any()):
        _fail("EOD total_open_interest must be positive")
    _require_allclose(phys_net + legal_net, np.zeros(len(frame.index)), "phys_net_plus_legal_net")
    _require_allclose(phys_long + legal_long, total_oi, "total_open_interest")
    _require_allclose(phys_short + legal_short, total_short, "total_short_abs")
    _require_allclose(total_oi, total_short, "long_short_open_interest_balance")
    _require_allclose(phys_long - phys_short, phys_net, "phys_net")
    _require_allclose(legal_long - legal_short, legal_net, "legal_net")
    _require_allclose(phys_long + phys_short, phys_gross, "phys_gross")
    _require_allclose(legal_long + legal_short, legal_gross, "legal_gross")

    formulas = {
        "phys_long_share_of_oi": phys_long / total_oi,
        "phys_short_share_of_oi": phys_short / total_oi,
        "phys_net_share_of_oi": phys_net / total_oi,
        "legal_long_share_of_oi": legal_long / total_oi,
        "legal_short_share_of_oi": legal_short / total_oi,
        "legal_net_share_of_oi": legal_net / total_oi,
        "phys_gross_share_of_two_sided_oi": phys_gross / (2.0 * total_oi),
        "legal_gross_share_of_two_sided_oi": legal_gross / (2.0 * total_oi),
    }
    for field, expected in formulas.items():
        actual = _finite(frame[field], field).astype(float)
        _require_allclose(actual, expected, field)

    _validate_average(frame, "phys_long", "phys_long_num", "phys_avg_long_per_participant")
    _validate_average(frame, "phys_short_abs", "phys_short_num", "phys_avg_short_per_participant")
    _validate_average(frame, "legal_long", "legal_long_num", "legal_avg_long_per_participant")
    _validate_average(frame, "legal_short_abs", "legal_short_num", "legal_avg_short_per_participant")


def _validate_eod(path: Path, instrument_id: str, expected_rows: int) -> dict[str, object]:
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        raise Step5AcceptanceError("EOD partition unreadable: " + str(exc)) from exc
    if len(frame.index) != expected_rows:
        _fail("EOD physical row count mismatch")
    missing = [c for c in EOD_REQUIRED if c not in frame.columns]
    if missing:
        _fail("EOD physical schema missing: " + ",".join(missing))
    if set(frame["instrument_id"].astype(str).unique()) != {instrument_id}:
        _fail("EOD physical instrument mismatch")
    if frame.duplicated(subset=["instrument_id", "trade_date"]).any():
        _fail("EOD physical duplicate identity")
    dates = pd.to_datetime(frame["trade_date"], errors="coerce")
    if bool(dates.isna().any()) or not dates.is_monotonic_increasing:
        _fail("EOD physical trade dates invalid/nonmonotonic")
    for field in ("snapshot_ts_utc", "availability_ts_utc", "phys_systime_utc", "legal_systime_utc"):
        parsed = pd.to_datetime(frame[field], errors="coerce", utc=True)
        if bool(parsed.isna().any()):
            _fail("EOD invalid UTC timestamp metadata: " + field)
    snapshot_msk = pd.to_datetime(frame["snapshot_ts_msk"], errors="coerce")
    if bool(snapshot_msk.isna().any()) or getattr(snapshot_msk.dt, "tz", None) is None:
        _fail("EOD snapshot_ts_msk must be timezone-aware")
    for field in ("source_partition_ref", "source_canonical_partition_ref", "source_frozen_partition_sha256"):
        if bool(frame[field].astype(str).str.strip().eq("").any()):
            _fail("EOD source lineage missing: " + field)
    _validate_eod_metrics(frame)
    return {
        "row_count": int(len(frame.index)),
        "required_schema_complete": True,
        "all_derived_metrics_recomputed": True,
        "balance_invariants_valid": True,
        "min_trade_date": str(frame["trade_date"].min()),
        "max_trade_date": str(frame["trade_date"].max()),
        "source_revision_rows_dropped": int(pd.to_numeric(frame["source_revision_rows_dropped"]).sum()),
        "physical_readback_passed": True,
    }


def _feature_columns() -> list[str]:
    result: list[str] = []
    for field in BASE_FIELDS:
        for lag in LAGS:
            result.append(f"{field}_chg_{lag}obs")
        for window in WINDOWS:
            result.extend([f"{field}_zscore_{window}obs", f"{field}_pctile_{window}obs"])
    return result


def _pctile(values: np.ndarray) -> float:
    if np.isnan(values).any():
        return np.nan
    return float(np.mean(values <= values[-1]))


def _validate_features(path: Path, instrument_id: str, expected_rows: int) -> dict[str, object]:
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        raise Step5AcceptanceError("feature partition unreadable: " + str(exc)) from exc
    required = ["instrument_id", "trade_date", "snapshot_ts_utc", "availability_ts_utc", *BASE_FIELDS, *_feature_columns()]
    missing = [c for c in required if c not in frame.columns]
    if missing:
        _fail("feature physical schema missing: " + ",".join(missing))
    if len(frame.index) != expected_rows:
        _fail("feature physical row count mismatch")
    if set(frame["instrument_id"].astype(str).unique()) != {instrument_id}:
        _fail("feature physical instrument mismatch")
    if frame.duplicated(subset=["instrument_id", "trade_date"]).any():
        _fail("feature physical duplicate identity")
    dates = pd.to_datetime(frame["trade_date"], errors="coerce")
    if bool(dates.isna().any()) or not dates.is_monotonic_increasing:
        _fail("feature physical trade dates invalid/nonmonotonic")
    for field in BASE_FIELDS:
        source = _finite(frame[field], field, allow_null=True).astype(float)
        for lag in LAGS:
            column = f"{field}_chg_{lag}obs"
            actual = _finite(frame[column], column, allow_null=True).astype(float)
            expected = source - source.shift(lag)
            if not np.allclose(actual.to_numpy(), expected.to_numpy(), equal_nan=True, rtol=1e-10, atol=1e-12):
                _fail("feature change formula mismatch: " + column)
        for window in WINDOWS:
            rolling = source.rolling(window=window, min_periods=window)
            mean = rolling.mean()
            std = rolling.std(ddof=0)
            expected_z = ((source - mean) / std).mask(std.eq(0.0))
            zcol = f"{field}_zscore_{window}obs"
            actual_z = _finite(frame[zcol], zcol, allow_null=True).astype(float)
            if not np.allclose(actual_z.to_numpy(), expected_z.to_numpy(), equal_nan=True, rtol=1e-10, atol=1e-12):
                _fail("feature zscore formula mismatch: " + zcol)
            expected_p = rolling.apply(_pctile, raw=True)
            pcol = f"{field}_pctile_{window}obs"
            actual_p = _finite(frame[pcol], pcol, allow_null=True).astype(float)
            if not np.allclose(actual_p.to_numpy(), expected_p.to_numpy(), equal_nan=True, rtol=1e-10, atol=1e-12):
                _fail("feature percentile formula mismatch: " + pcol)
            valid_p = actual_p.dropna()
            if bool(((valid_p < 0.0) | (valid_p > 1.0)).any()):
                _fail("feature percentile outside unit interval")
    return {
        "row_count": int(len(frame.index)),
        "required_schema_complete": True,
        "feature_formula_revalidation_passed": True,
        "feature_column_count": len(_feature_columns()),
        "physical_readback_passed": True,
    }


def _validate_feature_source_alignment(feature_path: Path, eod_path: Path) -> None:
    features = pd.read_parquet(feature_path)
    eod = pd.read_parquet(eod_path)
    if len(features.index) != len(eod.index):
        _fail("feature/EOD source row count mismatch")
    for field in ("instrument_id", "trade_date"):
        left = features[field].astype(str).reset_index(drop=True)
        right = eod[field].astype(str).reset_index(drop=True)
        if not left.equals(right):
            _fail("feature/EOD source identity mismatch: " + field)
    for field in ("snapshot_ts_utc", "availability_ts_utc"):
        left = pd.to_datetime(features[field], errors="coerce", utc=True)
        right = pd.to_datetime(eod[field], errors="coerce", utc=True)
        if bool(left.isna().any()) or bool(right.isna().any()) or not left.reset_index(drop=True).equals(right.reset_index(drop=True)):
            _fail("feature/EOD source timestamp mismatch: " + field)
    for field in BASE_FIELDS:
        left = _finite(features[field], "feature " + field, allow_null=True).astype(float)
        right = _finite(eod[field], "EOD " + field, allow_null=True).astype(float)
        if not np.allclose(left.to_numpy(), right.to_numpy(), equal_nan=True, rtol=1e-10, atol=1e-12):
            _fail("feature/EOD source base-column mismatch: " + field)


def _validate_eod_raw_lineage(manifest_values: Mapping[str, object], instrument_id: str) -> None:
    expected_pointer = _data_root() / "state" / "datasets" / ("dataset_id=" + RAW_DATASET) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"
    pointer_ref = str(manifest_values.get("accepted_raw_pointer_ref") or "").strip()
    pointer_path = _expand_root_ref(pointer_ref, "EOD accepted_raw_pointer_ref")
    if pointer_path.resolve() != expected_pointer.resolve():
        _fail("EOD accepted raw pointer path mismatch")
    pointer = _load_json(pointer_path, "EOD accepted raw pointer")
    if pointer.get("dataset_id") != RAW_DATASET or pointer.get("instrument_id") != instrument_id:
        _fail("EOD accepted raw pointer identity mismatch")
    if pointer.get("quality_status") != "pass" or pointer.get("acceptance_status") != "pass" or pointer.get("promotion_basis") != "raw_history_acceptance":
        _fail("EOD accepted raw pointer is not canonical PASS raw-history promotion")
    if pointer.get("run_id") != manifest_values.get("accepted_raw_history_run_id"):
        _fail("EOD accepted raw run identity mismatch")
    if pointer.get("manifest_ref") != manifest_values.get("accepted_raw_manifest_ref"):
        _fail("EOD accepted raw manifest binding mismatch")
    _expand_root_ref(pointer.get("manifest_ref"), "EOD accepted_raw_manifest_ref")


def _frozen_expectation(instrument_id: str, frozen: Mapping[str, object]) -> raw_acceptance.HistoryExpectation:
    binding = futoi_materializer._registry_binding(Path.cwd() / futoi_materializer.REGISTRY_PATH, instrument_id)
    if str(binding.get("futoi.source_id")) != SOURCE_ID:
        _fail("frozen input registry source binding mismatch")
    return raw_acceptance.HistoryExpectation(
        target_dataset_id=SOURCE_DATASET_ID,
        instrument_id=instrument_id,
        source_id=SOURCE_ID,
        date_start=str(frozen.get("requested_from")),
        date_end=str(frozen.get("requested_till")),
        expected_partitions=int(frozen.get("partition_count") or 0),
        expected_rows=int(frozen.get("row_count") or 0),
        expected_secid=str(binding["secid"]),
        expected_source_ticker=str(binding["futoi.ticker"]),
    )


def _validate_frozen_input(
    manifest_values: Mapping[str, object],
    instrument_id: str,
    run_root: Path,
    expected_rows: int,
) -> dict[str, object]:
    frozen_manifest = _artifact_path(manifest_values.get("frozen_input_manifest_path"), run_root, "frozen_input_manifest_path")
    expected_manifest_sha = str(manifest_values.get("frozen_input_manifest_sha256") or "").strip().lower()
    if len(expected_manifest_sha) != 64 or hashlib.sha256(frozen_manifest.read_bytes()).hexdigest() != expected_manifest_sha:
        _fail("frozen input manifest SHA-256 mismatch")
    frozen = _load_json(frozen_manifest, "frozen input manifest")
    if frozen.get("schema_version") != FROZEN_INPUT_SCHEMA or frozen.get("producer") != FROZEN_INPUT_PRODUCER:
        _fail("frozen input schema/producer mismatch")
    if frozen.get("dataset_id") != RAW_DATASET or frozen.get("instrument_id") != instrument_id:
        _fail("frozen input dataset/instrument mismatch")
    if frozen.get("physical_validation") != "stage2_futoi_partition_validator_reapplied":
        _fail("frozen input physical validation marker mismatch")
    if frozen.get("freeze_mode") != "create_only_hardlink_same_validated_inode":
        _fail("frozen input freeze mode mismatch")
    if frozen.get("network_calls_used") is not False or frozen.get("latest_autodetect_used") is not False:
        _fail("frozen input network/latest evidence mismatch")
    for field in ("accepted_raw_pointer_ref", "accepted_raw_manifest_ref", "accepted_raw_acceptance_report_ref", "accepted_raw_history_run_id", "accepted_partition_dates_sha256"):
        eod_field = "accepted_raw_partition_dates_sha256" if field == "accepted_partition_dates_sha256" else field
        if frozen.get(field) != manifest_values.get(eod_field):
            _fail("frozen/EOD accepted-raw binding mismatch: " + field)

    start_date = str(frozen.get("requested_from") or "")
    end_date = str(frozen.get("requested_till") or "")
    accepted = _accepted_history_scope(_data_root(), instrument_id, start_date, end_date)
    if frozen.get("accepted_raw_pointer_ref") != accepted.pointer_ref or frozen.get("accepted_raw_manifest_ref") != accepted.manifest_ref:
        _fail("frozen input no longer binds current accepted raw history")
    if frozen.get("accepted_partition_dates_sha256") != accepted.partition_dates_sha256:
        _fail("frozen input accepted date-set digest mismatch")

    records = frozen.get("records")
    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
        _fail("frozen input records must be a sequence")
    if len(records) != expected_rows or int(frozen.get("partition_count") or 0) != expected_rows:
        _fail("frozen input partition count mismatch")
    expectation = _frozen_expectation(instrument_id, frozen)
    total_raw_rows = 0
    record_by_date: dict[str, Mapping[str, object]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            _fail("frozen input record must be object")
        trade_date = str(record.get("trade_date") or "")
        if trade_date in record_by_date:
            _fail("duplicate frozen input trade_date")
        frozen_path = _expand_root_ref(record.get("frozen_partition_ref"), "frozen_partition_ref", require_run_root=run_root)
        frozen_sha = str(record.get("frozen_sha256") or "").strip().lower()
        source_sha = str(record.get("source_sha256_at_freeze") or "").strip().lower()
        if len(frozen_sha) != 64 or frozen_sha != source_sha:
            _fail("frozen source/snapshot hash binding mismatch")
        if hashlib.sha256(frozen_path.read_bytes()).hexdigest() != frozen_sha:
            _fail("frozen partition content SHA-256 mismatch")
        if record.get("hardlink_same_validated_inode") is not True or record.get("physical_validation_status") != "pass":
            _fail("frozen partition validation/inode evidence mismatch")
        expected_canonical = raw_partition_path(_data_root(), instrument_id, trade_date)
        if str(record.get("canonical_source_ref") or "") != _rooted_ref(expected_canonical):
            _fail("frozen canonical source path identity mismatch")
        try:
            frame = pd.read_parquet(frozen_path)
            rows, _ = raw_acceptance._validate_futoi_partition(frame, expectation, trade_date)
        except Exception as exc:
            raise Step5AcceptanceError("frozen FUTOI physical revalidation failed for " + trade_date + ": " + str(exc)) from exc
        if int(record.get("row_count") or 0) != rows:
            _fail("frozen partition row count evidence mismatch")
        total_raw_rows += rows
        record_by_date[trade_date] = record
    if tuple(record_by_date) != accepted.accepted_dates:
        _fail("frozen partition dates differ from accepted raw date set")
    if total_raw_rows != int(frozen.get("row_count") or 0):
        _fail("frozen aggregate raw row count mismatch")
    return {
        "manifest_path": frozen_manifest,
        "manifest_sha256": expected_manifest_sha,
        "records_by_date": record_by_date,
        "physical_partition_revalidation_passed": True,
        "partition_count": len(records),
        "raw_row_count": total_raw_rows,
    }


def _rooted_ref(path: Path) -> str:
    try:
        rel = path.resolve(strict=True).relative_to(_data_root().resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step5AcceptanceError("artifact must be inside MOEX_DATA_ROOT") from exc
    return ROOT_PREFIX + rel.as_posix()


def _validate_eod_frozen_lineage(eod_path: Path, frozen_validation: Mapping[str, object]) -> None:
    eod = pd.read_parquet(eod_path)
    records = frozen_validation.get("records_by_date")
    if not isinstance(records, Mapping):
        _fail("frozen validation records missing")
    for _, row in eod.iterrows():
        trade_date = str(row["trade_date"])
        record = records.get(trade_date)
        if not isinstance(record, Mapping):
            _fail("EOD trade_date missing from frozen input")
        if str(row["source_partition_ref"]) != str(record["frozen_partition_ref"]):
            _fail("EOD frozen partition lineage mismatch")
        if str(row["source_canonical_partition_ref"]) != str(record["canonical_source_ref"]):
            _fail("EOD canonical raw lineage mismatch")
        if str(row["source_frozen_partition_sha256"]) != str(record["frozen_sha256"]):
            _fail("EOD frozen partition hash lineage mismatch")


def _validate_output_record(row: Mapping[str, object], *, dataset_id: str, run_root: Path, expected_rows: int) -> dict[str, object]:
    instrument_id = _safe_token(row.get("instrument_id"), "instrument_id")
    if instrument_id not in EXPECTED_INSTRUMENTS:
        _fail("unexpected Stage 5 instrument")
    if row.get("dataset_id") != dataset_id or row.get("quality_status") != "pass":
        _fail("output dataset/quality mismatch")
    if int(row.get("row_count") or 0) != expected_rows:
        _fail("output evidence row count mismatch")
    producer_run_id = _safe_token(row.get("run_id"), "producer_run_id")
    partition = _artifact_path(row.get("partition_path"), run_root, "partition_path")
    manifest = _artifact_path(row.get("manifest_path"), run_root, "manifest_path")
    quality = _artifact_path(row.get("quality_report_path"), run_root, "quality_report_path")
    manifest_values = _load_json(manifest, "manifest")
    quality_values = _load_json(quality, "quality")
    for values, name in ((manifest_values, "manifest"), (quality_values, "quality")):
        if values.get("dataset_id") != dataset_id or values.get("instrument_id") != instrument_id or values.get("run_id") != producer_run_id:
            _fail(name + " identity/run mismatch")
        if int(values.get("row_count") or 0) != expected_rows or values.get("quality_status") != "pass":
            _fail(name + " row/quality mismatch")
    if Path(str(manifest_values.get("partition_path") or "")).resolve() != partition:
        _fail("manifest partition path mismatch")
    if Path(str(manifest_values.get("quality_report_path") or "")).resolve() != quality:
        _fail("manifest quality path mismatch")
    frozen_validation: dict[str, object] | None = None
    if dataset_id == EOD_DATASET:
        if manifest_values.get("canonical_raw_partition_reads_used") is not False:
            _fail("EOD manifest must forbid canonical raw reads after freeze")
        _validate_eod_raw_lineage(manifest_values, instrument_id)
        frozen_validation = _validate_frozen_input(manifest_values, instrument_id, run_root, expected_rows)
        physical = _validate_eod(partition, instrument_id, expected_rows)
        _validate_eod_frozen_lineage(partition, frozen_validation)
        physical["immutable_frozen_raw_revalidation_passed"] = True
    else:
        physical = _validate_features(partition, instrument_id, expected_rows)
    return {
        "dataset_id": dataset_id,
        "instrument_id": instrument_id,
        "producer_run_id": producer_run_id,
        "partition": partition,
        "manifest": manifest,
        "quality": quality,
        "physical_readback": physical,
        "source_eod_partition_path": manifest_values.get("source_eod_partition_path"),
        "frozen_input_manifest_path": manifest_values.get("frozen_input_manifest_path"),
    }


def validate_pilot(values: Mapping[str, object], *, run_id: str) -> list[dict[str, object]]:
    if values.get("project") != "MOEX_Bot" or values.get("step") != 5 or values.get("status") != "pilot_passed":
        _fail("pilot identity/status mismatch")
    if values.get("artifact_version") != run_id or values.get("run_id") != run_id:
        _fail("pilot run identity mismatch")
    required_false = (
        "run_id_reuse_allowed", "raw_ingestion_changed", "network_calls_used", "latest_autodetect_used",
        "canonical_raw_partition_reads_after_freeze_used", "front_next_split_claimed", "historical_pit_research_ready_claimed",
    )
    for field in required_false:
        if values.get(field) is not False:
            _fail(field + " must be false")
    if values.get("run_artifacts_immutable") is not True or values.get("root_aggregate_semantics") is not True:
        _fail("pilot immutable/root aggregate semantics mismatch")
    if values.get("immutable_raw_input_freeze_used") is not True:
        _fail("pilot immutable_raw_input_freeze_used must be true")
    if values.get("raw_input_freeze_mode") != "create_only_hardlink_same_validated_inode":
        _fail("pilot raw_input_freeze_mode mismatch")
    if values.get("revision_policy") != "same_analytical_key_single_sess_id_then_max_seqnum":
        _fail("pilot revision policy mismatch")
    if values.get("snapshot_policy") != "max_resolved_ts_requires_FIZ_and_YUR":
        _fail("pilot snapshot policy mismatch")
    run_root = _run_root(run_id)
    if not run_root.is_dir() or Path(str(values.get("run_root") or "")).resolve() != run_root:
        _fail("pilot run_root mismatch")

    frozen_rows = values.get("frozen_inputs")
    eod_rows = values.get("eod_outputs")
    feature_rows = values.get("feature_outputs")
    if isinstance(frozen_rows, (str, bytes)) or not isinstance(frozen_rows, Sequence) or len(frozen_rows) != 2:
        _fail("pilot must contain two frozen raw inputs")
    if isinstance(eod_rows, (str, bytes)) or not isinstance(eod_rows, Sequence) or len(eod_rows) != 2:
        _fail("pilot must contain two EOD outputs")
    if isinstance(feature_rows, (str, bytes)) or not isinstance(feature_rows, Sequence) or len(feature_rows) != 2:
        _fail("pilot must contain two feature outputs")
    frozen_manifest_by_instrument: dict[str, Path] = {}
    for row in frozen_rows:
        if not isinstance(row, Mapping):
            _fail("frozen input output record must be object")
        instrument = _safe_token(row.get("instrument_id"), "instrument_id")
        if instrument in frozen_manifest_by_instrument or instrument not in EXPECTED_INSTRUMENTS:
            _fail("duplicate/unexpected frozen input instrument")
        if row.get("status") != "succeeded" or row.get("physical_validation_status") != "pass":
            _fail("frozen input pilot output is not pass")
        if int(row.get("partition_count") or 0) != EXPECTED_ROWS[instrument]:
            _fail("frozen input pilot partition count mismatch")
        manifest_path = _artifact_path(row.get("manifest_path"), run_root, "frozen pilot manifest_path")
        frozen_manifest_by_instrument[instrument] = manifest_path
    if set(frozen_manifest_by_instrument) != EXPECTED_INSTRUMENTS:
        _fail("frozen input instrument set mismatch")

    outputs: list[dict[str, object]] = []
    seen_eod: dict[str, dict[str, object]] = {}
    for row in eod_rows:
        if not isinstance(row, Mapping):
            _fail("EOD output record must be object")
        instrument = _safe_token(row.get("instrument_id"), "instrument_id")
        checked = _validate_output_record(row, dataset_id=EOD_DATASET, run_root=run_root, expected_rows=EXPECTED_ROWS[instrument])
        if instrument in seen_eod:
            _fail("duplicate EOD output instrument")
        if Path(str(checked.get("frozen_input_manifest_path") or "")).resolve() != frozen_manifest_by_instrument[instrument]:
            _fail("EOD frozen input does not match pilot frozen input output")
        seen_eod[instrument] = checked
        outputs.append(checked)
    if set(seen_eod) != EXPECTED_INSTRUMENTS:
        _fail("EOD output instrument set mismatch")

    seen_features: set[str] = set()
    for row in feature_rows:
        if not isinstance(row, Mapping):
            _fail("feature output record must be object")
        instrument = _safe_token(row.get("instrument_id"), "instrument_id")
        checked = _validate_output_record(row, dataset_id=FEATURE_DATASET, run_root=run_root, expected_rows=EXPECTED_ROWS[instrument])
        if instrument in seen_features:
            _fail("duplicate feature output instrument")
        seen_features.add(instrument)
        source_path = str(checked.get("source_eod_partition_path") or "")
        if not source_path or Path(source_path).resolve() != seen_eod[instrument]["partition"]:
            _fail("feature source EOD lineage mismatch")
        _validate_feature_source_alignment(checked["partition"], seen_eod[instrument]["partition"])
        physical = dict(checked["physical_readback"])
        physical["source_eod_identity_timestamp_base_match"] = True
        checked["physical_readback"] = physical
        outputs.append(checked)
    if seen_features != EXPECTED_INSTRUMENTS:
        _fail("feature output instrument set mismatch")
    return outputs


def _pointer_path(dataset_id: str, instrument_id: str) -> Path:
    return _data_root() / "state" / "datasets" / ("dataset_id=" + dataset_id) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"


def _stage_json(path: Path, values: Mapping[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".stage") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        return Path(handle.name)


def _restore(path: Path, previous: bytes | None) -> None:
    if previous is None:
        if path.exists():
            path.unlink()
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".rollback") as handle:
        handle.write(previous)
        staged = Path(handle.name)
    staged.replace(path)


def _transactional_replace(records: Sequence[tuple[Path, Mapping[str, object]]]) -> None:
    paths = [p for p, _ in records]
    if len(paths) != len(set(paths)):
        _fail("transaction target paths must be unique")
    previous = {p: p.read_bytes() if p.exists() else None for p in paths}
    staged: list[tuple[Path, Path]] = []
    applied: list[Path] = []
    try:
        for final, values in records:
            staged.append((_stage_json(final, values), final))
        for source, final in staged:
            source.replace(final)
            applied.append(final)
    except Exception as exc:
        rollback_errors: list[str] = []
        for final in reversed(applied):
            try:
                _restore(final, previous[final])
            except Exception as rollback_exc:
                rollback_errors.append(str(rollback_exc))
        if rollback_errors:
            raise Step5AcceptanceError("promotion failed and rollback incomplete: " + ";".join(rollback_errors)) from exc
        raise Step5AcceptanceError("promotion transaction failed: " + str(exc)) from exc
    finally:
        for source, _ in staged:
            if source.exists():
                source.unlink(missing_ok=True)


def promote(*, run_id: str) -> dict[str, object]:
    checked_run = _safe_token(run_id, "run_id")
    evidence_path = _evidence_dir(checked_run) / "pilot_evidence.json"
    outputs = validate_pilot(_load_json(evidence_path, "pilot_evidence"), run_id=checked_run)
    records: list[tuple[Path, Mapping[str, object]]] = []
    pointer_summaries: list[dict[str, object]] = []
    for output in outputs:
        dataset_id = str(output["dataset_id"])
        instrument_id = str(output["instrument_id"])
        pointer = _pointer_path(dataset_id, instrument_id)
        values = {
            "dataset_id": dataset_id,
            "instrument_id": instrument_id,
            "run_id": str(output["producer_run_id"]),
            "acceptance_run_id": checked_run,
            "manifest_ref": _rooted_ref(output["manifest"]),
            "quality_report_ref": _rooted_ref(output["quality"]),
            "partition_ref": _rooted_ref(output["partition"]),
            "quality_status": "pass",
            "acceptance_contract_id": CONTRACT_ID,
            "immutable_frozen_raw_input_verified": True,
            "historical_pit_research_ready_claimed": False,
        }
        records.append((pointer, values))
        pointer_summaries.append({
            "dataset_id": dataset_id,
            "instrument_id": instrument_id,
            "run_id": str(output["producer_run_id"]),
            "acceptance_run_id": checked_run,
            "pointer_path": pointer.as_posix(),
            "physical_readback": output["physical_readback"],
        })
    if len(pointer_summaries) != 4:
        _fail("accepted pointer count mismatch")
    marker = _evidence_dir(checked_run) / "accepted_pointers.json"
    result: dict[str, object] = {
        "project": "MOEX_Bot",
        "step": 5,
        "status": "accepted",
        "run_id": checked_run,
        "acceptance_contract_id": CONTRACT_ID,
        "accepted_pointer_count": 4,
        "expected_pointer_count": 4,
        "pointers": pointer_summaries,
        "promotion_semantics": "transactional_with_rollback",
        "physical_partition_readback_required": True,
        "immutable_frozen_raw_input_verified": True,
        "root_aggregate_semantics": True,
        "front_next_split_claimed": False,
        "historical_pit_research_ready_claimed": False,
    }
    records.append((marker, result))
    _transactional_replace(records)
    result["acceptance_evidence_path"] = marker.as_posix()
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote a passed Stage 5 FUTOI EOD/positioning pilot.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_env_file(args.env_file)
    try:
        result = promote(run_id=args.run_id)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 5, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

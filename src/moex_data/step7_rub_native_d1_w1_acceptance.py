from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Final
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

from moex_data.futures import stage2_raw_history_acceptance as stage2
from moex_data.futures.freeze_step7_accepted_raw_5m import accepted_quote_history, quote_validation_expectation

CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
CONTRACT_ID: Final[str] = "step7_rub_native_d1_w1_technical_acceptance.v1"
OHLCV_DATASET: Final[str] = "rub_native_ohlcv_htf"
TECH_DATASET: Final[str] = "rub_technical_features_htf"
SOURCE_DATASET: Final[str] = "futures_raw_5m"
SOURCE_ID: Final[str] = "moex_algopack_fo_tradestats_5m"
MARKET_TZ: Final[str] = "Europe/Moscow"
ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
EXPECTED_SECID: Final[dict[str, str]] = {
    "usdrubf_futures_family": "USDRUBF",
    "cnyrubf_futures_family": "CNYRUBF",
}
HISTORY: Final[dict[str, tuple[str, str, int]]] = {
    "usdrubf_futures_family": ("2022-04-26", "2026-08-17", 1100),
    "cnyrubf_futures_family": ("2022-04-26", "2026-08-17", 1100),
}
EXPECTED_KEYS: Final[frozenset[tuple[str, str, str]]] = frozenset(
    (dataset_id, timeframe, instrument_id)
    for dataset_id in (OHLCV_DATASET, TECH_DATASET)
    for timeframe in ("1D", "1W")
    for instrument_id in HISTORY
)
OHLCV_COMMON_REQUIRED: Final[tuple[str, ...]] = (
    "instrument_id", "secid", "timeframe", "period_start_date", "period_end_date",
    "availability_ts_utc", "open", "high", "low", "close", "volume", "value",
    "num_trades", "source_row_count", "source_period_count", "source_lineage_sha256",
    "build_ts_utc",
)
D1_REQUIRED: Final[tuple[str, ...]] = OHLCV_COMMON_REQUIRED + ("trade_date",)
W1_REQUIRED: Final[tuple[str, ...]] = OHLCV_COMMON_REQUIRED + ("week_start_date", "week_end_date", "trading_day_count")
TECH_REQUIRED: Final[tuple[str, ...]] = (
    "instrument_id", "secid", "timeframe", "period_start_date", "period_end_date",
    "availability_ts_utc", "close", "return_1obs", "gap_abs", "gap_pct", "range_abs",
    "true_range", "atr_14_wilder", "atr_20_wilder", "higher_high_vs_prev_bar",
    "higher_low_vs_prev_bar", "lower_high_vs_prev_bar", "lower_low_vs_prev_bar",
    "close_break_prev_high", "close_break_prev_low", "source_ohlcv_run_id", "build_ts_utc",
)


class Step7AcceptanceError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step7AcceptanceError(message)


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
    path = Path(value)
    if not path.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    return path.resolve()


def _load_json(path: Path, field: str) -> dict[str, object]:
    if not path.is_file() or path.is_symlink():
        _fail(field + " missing/non-regular")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise Step7AcceptanceError(field + " invalid JSON: " + str(exc)) from exc
    if not isinstance(value, dict):
        _fail(field + " must be object")
    return value


def _run_root(run_id: str) -> Path:
    return _data_root() / "runs" / "step7_rub_native_d1_w1" / ("run_id=" + _safe_token(run_id, "run_id"))


def _evidence_dir(run_id: str) -> Path:
    return _data_root() / "state" / "acceptance" / "step7_rub_native_d1_w1" / ("run_id=" + _safe_token(run_id, "run_id"))


def _inside_run(path_value: object, run_root: Path, field: str) -> Path:
    path = Path(str(path_value or ""))
    if not path.is_absolute():
        _fail(field + " must be absolute")
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(run_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step7AcceptanceError(field + " must exist inside run root") from exc
    if not resolved.is_file() or resolved.is_symlink():
        _fail(field + " must be regular non-symlink file")
    return resolved


def _expand_root_ref(value: object, run_root: Path | None = None) -> Path:
    text = str(value or "").strip()
    if not text.startswith(ROOT_PREFIX):
        _fail("rooted reference must start with ${MOEX_DATA_ROOT}/")
    relative = text[len(ROOT_PREFIX):]
    if not relative or relative.startswith("/") or ".." in Path(relative).parts:
        _fail("invalid rooted reference")
    root = _data_root().resolve(strict=True)
    path = (root / relative).resolve(strict=True)
    try:
        path.relative_to(root)
        if run_root is not None:
            path.relative_to(run_root.resolve(strict=True))
    except ValueError as exc:
        raise Step7AcceptanceError("rooted reference escaped approved root") from exc
    if not path.is_file() or path.is_symlink():
        _fail("rooted reference must resolve to regular non-symlink file")
    return path


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sum_or_null(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce")
    value = numeric.sum(min_count=1)
    return None if pd.isna(value) else float(value)


def _availability_d1(trade_date: str) -> str:
    local_date = date.fromisoformat(trade_date) + timedelta(days=1)
    local = datetime.combine(local_date, time(hour=6), tzinfo=ZoneInfo(MARKET_TZ))
    return local.astimezone(timezone.utc).isoformat()


def _availability_w1(week_end_date: str) -> str:
    local_date = date.fromisoformat(week_end_date) + timedelta(days=1)
    local = datetime.combine(local_date, time(hour=6), tzinfo=ZoneInfo(MARKET_TZ))
    return local.astimezone(timezone.utc).isoformat()


def _week_dates(value: str) -> tuple[str, str]:
    current = date.fromisoformat(value)
    monday = current - timedelta(days=current.weekday())
    sunday = monday + timedelta(days=6)
    return monday.isoformat(), sunday.isoformat()


def _revalidate_frozen(*, repo_root: Path, data_root: Path, manifest_path: Path, instrument_id: str, start: str, end: str, validation_run_id: str) -> dict[str, object]:
    frozen = _load_json(manifest_path, "frozen raw manifest")
    if frozen.get("schema_version") != "step7_frozen_raw_5m_manifest.v1" or frozen.get("dataset_id") != SOURCE_DATASET:
        _fail("frozen raw manifest schema/dataset mismatch")
    if frozen.get("instrument_id") != instrument_id or frozen.get("source_id") != SOURCE_ID:
        _fail("frozen raw manifest identity/source mismatch")
    if frozen.get("freeze_method") != "validated_inode_create_only_hardlink" or frozen.get("mutable_canonical_raw_read_after_freeze_allowed") is not False:
        _fail("frozen raw freeze semantics mismatch")
    current_scope = accepted_quote_history(data_root, instrument_id, start, end)
    if frozen.get("accepted_raw_history_run_id") != current_scope.acceptance_run_id:
        _fail("frozen raw upstream accepted run is stale")
    if frozen.get("accepted_raw_manifest_ref") != current_scope.manifest_ref:
        _fail("frozen raw upstream accepted manifest is stale")
    if frozen.get("accepted_raw_pointer_ref") != current_scope.pointer_ref:
        _fail("frozen raw upstream accepted pointer binding mismatch")
    if frozen.get("accepted_partition_dates_sha256") != current_scope.partition_dates_sha256:
        _fail("frozen raw upstream accepted date-set digest mismatch")
    records = frozen.get("partitions")
    if not isinstance(records, list) or len(records) != int(frozen.get("partition_count") or -1):
        _fail("frozen raw partition records/count mismatch")
    expectation = quote_validation_expectation(instrument_id, start, end)
    expected_secid = EXPECTED_SECID[instrument_id]
    physical_records: list[dict[str, object]] = []
    content_lines: list[str] = []
    total_rows = 0
    dates: list[str] = []
    for record in records:
        if not isinstance(record, dict):
            _fail("frozen raw record must be object")
        trade_date = str(record.get("trade_date") or "")
        if trade_date in dates:
            _fail("duplicate frozen trade_date")
        dates.append(trade_date)
        path = _expand_root_ref(record.get("frozen_ref"), run_root=manifest_path.parents[4] if False else None)
        try:
            path.relative_to(manifest_path.parents[4])
        except ValueError:
            pass
        expected_sha = str(record.get("sha256") or "").strip().lower()
        if len(expected_sha) != 64 or _sha_file(path) != expected_sha:
            _fail("frozen raw physical SHA-256 mismatch")
        frame = pd.read_parquet(path)
        rows, secids = stage2._validate_quote_partition(repo_root, frame, expectation, trade_date, validation_run_id)
        if int(rows) != int(record.get("row_count") or -1):
            _fail("frozen raw physical row_count mismatch")
        if set(secids) != {expected_secid} or set(record.get("secids") or []) != {expected_secid}:
            _fail("frozen raw physical secid mismatch")
        physical_records.append({"trade_date": trade_date, "path": path, "sha256": expected_sha, "row_count": int(rows)})
        content_lines.append(trade_date + "\t" + expected_sha + "\n")
        total_rows += int(rows)
    if tuple(dates) != current_scope.accepted_dates:
        _fail("frozen raw date set no longer equals current accepted scope")
    digest = hashlib.sha256("".join(content_lines).encode("utf-8")).hexdigest()
    if str(frozen.get("frozen_content_sha256") or "").strip().lower() != digest:
        _fail("frozen raw aggregate content digest mismatch")
    return {
        "partition_count": len(physical_records),
        "row_count": total_rows,
        "content_sha256": digest,
        "records": physical_records,
        "current_accepted_scope_match": True,
        "physical_revalidation_passed": True,
    }


def _oracle_d1(records: Sequence[Mapping[str, object]], instrument_id: str) -> pd.DataFrame:
    expected_secid = EXPECTED_SECID[instrument_id]
    rows: list[dict[str, object]] = []
    for record in records:
        trade_date = str(record["trade_date"])
        frame = pd.read_parquet(Path(record["path"]))
        required = ("instrument_id", "trade_date", "ts", "secid", "open", "high", "low", "close", "volume", "value", "num_trades")
        missing = [field for field in required if field not in frame.columns]
        if missing or frame.empty:
            _fail("oracle D1 raw input missing fields/empty")
        if set(frame["instrument_id"].astype(str)) != {instrument_id} or set(frame["trade_date"].astype(str)) != {trade_date}:
            _fail("oracle D1 raw identity mismatch")
        if set(frame["secid"].astype(str)) != {expected_secid}:
            _fail("oracle D1 raw SECID mismatch")
        work = frame.copy()
        work["_ts"] = pd.to_datetime(work["ts"], errors="coerce")
        if bool(work["_ts"].isna().any()):
            _fail("oracle D1 invalid source ts")
        work = work.sort_values("_ts", kind="mergesort").reset_index(drop=True)
        for field in ("open", "high", "low", "close"):
            work[field] = pd.to_numeric(work[field], errors="coerce")
            if bool(work[field].isna().any()) or not np.isfinite(work[field].astype(float)).all():
                _fail("oracle D1 invalid OHLC: " + field)
        rows.append({
            "instrument_id": instrument_id,
            "secid": expected_secid,
            "timeframe": "1D",
            "period_start_date": trade_date,
            "period_end_date": trade_date,
            "trade_date": trade_date,
            "availability_ts_utc": _availability_d1(trade_date),
            "open": float(work["open"].iloc[0]),
            "high": float(work["high"].max()),
            "low": float(work["low"].min()),
            "close": float(work["close"].iloc[-1]),
            "volume": _sum_or_null(work["volume"]),
            "value": _sum_or_null(work["value"]),
            "num_trades": _sum_or_null(work["num_trades"]),
            "source_row_count": int(len(work.index)),
            "source_period_count": 1,
            "source_lineage_sha256": str(record["sha256"]),
        })
    return pd.DataFrame(rows).sort_values("trade_date").reset_index(drop=True)


def _oracle_w1(d1: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    work = d1.copy().sort_values("trade_date").reset_index(drop=True)
    week_pairs = work["trade_date"].map(lambda value: _week_dates(str(value)))
    work["week_start_date"] = [pair[0] for pair in week_pairs]
    work["week_end_date"] = [pair[1] for pair in week_pairs]
    work = work.loc[
        work["week_start_date"].map(lambda value: date.fromisoformat(value) >= start_date)
        & work["week_end_date"].map(lambda value: date.fromisoformat(value) <= end_date)
    ].copy()
    rows: list[dict[str, object]] = []
    for week_start, part in work.groupby("week_start_date", sort=True):
        part = part.sort_values("trade_date").reset_index(drop=True)
        week_end = str(part["week_end_date"].iloc[0])
        lineage = "".join(str(row.trade_date) + "\t" + str(row.source_lineage_sha256) + "\n" for row in part.itertuples())
        rows.append({
            "instrument_id": str(part["instrument_id"].iloc[0]),
            "secid": str(part["secid"].iloc[0]),
            "timeframe": "1W",
            "period_start_date": str(week_start),
            "period_end_date": week_end,
            "week_start_date": str(week_start),
            "week_end_date": week_end,
            "availability_ts_utc": _availability_w1(week_end),
            "open": float(part["open"].iloc[0]),
            "high": float(part["high"].max()),
            "low": float(part["low"].min()),
            "close": float(part["close"].iloc[-1]),
            "volume": _sum_or_null(part["volume"]),
            "value": _sum_or_null(part["value"]),
            "num_trades": _sum_or_null(part["num_trades"]),
            "source_row_count": int(pd.to_numeric(part["source_row_count"], errors="raise").sum()),
            "source_period_count": int(len(part.index)),
            "trading_day_count": int(len(part.index)),
            "source_lineage_sha256": hashlib.sha256(lineage.encode("utf-8")).hexdigest(),
        })
    return pd.DataFrame(rows).reset_index(drop=True)


def _oracle_wilder(true_range: pd.Series, window: int) -> pd.Series:
    values = pd.to_numeric(true_range, errors="coerce").astype(float)
    output = pd.Series(np.nan, index=values.index, dtype="float64")
    if len(values.index) < window:
        return output
    seed = float(values.iloc[:window].sum()) / float(window)
    output.iloc[window - 1] = seed
    previous = seed
    for index in range(window, len(values.index)):
        current = float(values.iloc[index])
        previous = ((float(window - 1) * previous) + current) / float(window)
        output.iloc[index] = previous
    return output


def _oracle_technical(ohlcv: pd.DataFrame, source_run_id: str) -> pd.DataFrame:
    sort_field = "trade_date" if str(ohlcv["timeframe"].iloc[0]) == "1D" else "week_start_date"
    work = ohlcv.copy().sort_values(sort_field).reset_index(drop=True)
    for field in ("open", "high", "low", "close"):
        work[field] = pd.to_numeric(work[field], errors="coerce").astype(float)
    previous_close = work["close"].shift(1)
    previous_high = work["high"].shift(1)
    previous_low = work["low"].shift(1)
    range_abs = work["high"] - work["low"]
    true_range = pd.DataFrame({
        "hl": range_abs,
        "hc": (work["high"] - previous_close).abs(),
        "lc": (work["low"] - previous_close).abs(),
    }).max(axis=1, skipna=True)
    out = work[["instrument_id", "secid", "timeframe", "period_start_date", "period_end_date", "availability_ts_utc", "close"]].copy()
    out["return_1obs"] = work["close"] / previous_close - 1.0
    out["gap_abs"] = work["open"] - previous_close
    out["gap_pct"] = work["open"] / previous_close - 1.0
    out["range_abs"] = range_abs
    out["true_range"] = true_range
    out["atr_14_wilder"] = _oracle_wilder(true_range, 14)
    out["atr_20_wilder"] = _oracle_wilder(true_range, 20)
    comparisons = {
        "higher_high_vs_prev_bar": work["high"] > previous_high,
        "higher_low_vs_prev_bar": work["low"] > previous_low,
        "lower_high_vs_prev_bar": work["high"] < previous_high,
        "lower_low_vs_prev_bar": work["low"] < previous_low,
        "close_break_prev_high": work["close"] > previous_high,
        "close_break_prev_low": work["close"] < previous_low,
    }
    for field, values in comparisons.items():
        out[field] = values.astype("boolean")
        out.loc[0, field] = pd.NA
    out["source_ohlcv_run_id"] = source_run_id
    return out


def _validate_build_ts(actual: pd.DataFrame, manifest: Mapping[str, object], name: str) -> None:
    if "build_ts_utc" not in actual.columns:
        _fail(name + " missing contracted build_ts_utc")
    parsed = pd.to_datetime(actual["build_ts_utc"], errors="coerce", utc=True)
    if bool(parsed.isna().any()):
        _fail(name + " invalid build_ts_utc")
    manifest_value = str(manifest.get("build_ts_utc") or "")
    manifest_ts = pd.to_datetime([manifest_value], errors="coerce", utc=True)
    if bool(manifest_ts.isna().any()) or set(actual["build_ts_utc"].astype(str)) != {manifest_value}:
        _fail(name + " build_ts_utc does not match producer manifest")


def _compare_contract_frame(actual: pd.DataFrame, expected: pd.DataFrame, required: Sequence[str], manifest: Mapping[str, object], name: str) -> None:
    missing = [field for field in required if field not in actual.columns]
    if missing:
        _fail(name + " physical schema missing: " + ",".join(missing))
    if len(actual.index) != len(expected.index):
        _fail(name + " row count mismatch")
    _validate_build_ts(actual, manifest, name)
    for column in expected.columns:
        try:
            assert_series_equal(
                actual[column].reset_index(drop=True),
                expected[column].reset_index(drop=True),
                check_dtype=False,
                check_names=False,
                rtol=1e-10,
                atol=1e-12,
            )
        except AssertionError as exc:
            raise Step7AcceptanceError(name + " independent oracle mismatch: " + column) from exc


def _validate_manifest_quality(record: Mapping[str, object], run_root: Path) -> dict[str, object]:
    partition = _inside_run(record.get("partition_path"), run_root, "partition_path")
    manifest_path = _inside_run(record.get("manifest_path"), run_root, "manifest_path")
    quality_path = _inside_run(record.get("quality_report_path"), run_root, "quality_report_path")
    manifest = _load_json(manifest_path, "output manifest")
    quality = _load_json(quality_path, "output quality")
    physical = pd.read_parquet(partition)
    dataset_id = str(record.get("dataset_id") or "")
    instrument_id = str(record.get("instrument_id") or "")
    timeframe = str(record.get("timeframe") or "")
    producer_run_id = str(record.get("run_id") or "")
    evidence_rows = int(record.get("row_count") or -1)
    if len(physical.index) != evidence_rows:
        _fail("physical parquet row count differs from evidence record")
    for values, name in ((manifest, "manifest"), (quality, "quality")):
        if values.get("dataset_id") != dataset_id or values.get("instrument_id") != instrument_id or values.get("timeframe") != timeframe:
            _fail(name + " output identity mismatch")
        if values.get("run_id") != producer_run_id or values.get("quality_status") != "pass":
            _fail(name + " run/quality mismatch")
        if int(values.get("row_count") or -2) != len(physical.index):
            _fail(name + " row_count does not match physical parquet")
    if Path(str(manifest.get("partition_path") or "")).resolve() != partition:
        _fail("manifest partition path mismatch")
    if Path(str(manifest.get("quality_report_path") or "")).resolve() != quality_path:
        _fail("manifest quality path mismatch")
    if manifest.get("network_calls_used") is not False or manifest.get("latest_autodetect_used") is not False or manifest.get("continuous_series_used") is not False:
        _fail("manifest execution boundary mismatch")
    return {
        "record": record, "partition": partition, "manifest_path": manifest_path, "quality_path": quality_path,
        "manifest": manifest, "quality": quality, "physical": physical,
    }


def validate_pilot(values: Mapping[str, object], *, run_id: str, repo_root: str | Path = ".") -> list[dict[str, object]]:
    checked_run = _safe_token(run_id, "run_id")
    if values.get("project") != "MOEX_Bot" or values.get("step") != 7 or values.get("status") != "pilot_passed":
        _fail("pilot identity/status mismatch")
    if values.get("artifact_version") != checked_run or values.get("run_id") != checked_run:
        _fail("pilot run identity mismatch")
    false_fields = (
        "run_id_reuse_allowed", "network_calls_used", "latest_autodetect_used", "continuous_series_used",
        "mutable_canonical_raw_read_after_freeze_allowed", "si_cr_continuous_ready", "weekly_oi_ready",
        "advanced_technical_policy_ready", "research_ready",
    )
    for field in false_fields:
        if values.get(field) is not False:
            _fail(field + " must be false")
    if values.get("run_artifacts_immutable") is not True or values.get("accepted_raw_history_required") is not True:
        _fail("pilot immutable/accepted-history semantics mismatch")
    run_root = _run_root(checked_run)
    if not run_root.is_dir() or Path(str(values.get("run_root") or "")).resolve() != run_root:
        _fail("pilot run_root mismatch")
    repo = Path(repo_root).resolve()
    data_root = _data_root()

    frozen_rows = values.get("frozen_inputs")
    if not isinstance(frozen_rows, list) or len(frozen_rows) != 2:
        _fail("pilot must have two frozen input manifests")
    frozen_by_instrument: dict[str, dict[str, object]] = {}
    for row in frozen_rows:
        if not isinstance(row, dict):
            _fail("frozen input evidence must be object")
        instrument_id = _safe_token(row.get("instrument_id"), "instrument_id")
        if instrument_id not in HISTORY or instrument_id in frozen_by_instrument:
            _fail("frozen input instrument set mismatch")
        start, end, expected_partitions = HISTORY[instrument_id]
        manifest_path = _inside_run(row.get("manifest_path"), run_root, "frozen manifest_path")
        physical = _revalidate_frozen(
            repo_root=repo, data_root=data_root, manifest_path=manifest_path, instrument_id=instrument_id,
            start=start, end=end, validation_run_id=checked_run + "_acceptance_frozen_revalidation",
        )
        if int(physical["partition_count"]) != expected_partitions:
            _fail("frozen physical partition count mismatch")
        frozen_by_instrument[instrument_id] = {"manifest_path": manifest_path, "physical": physical}
    if set(frozen_by_instrument) != set(HISTORY):
        _fail("frozen instrument set incomplete")

    output_rows = values.get("outputs")
    if not isinstance(output_rows, list) or len(output_rows) != 8:
        _fail("pilot must have eight output records")
    output_by_key: dict[tuple[str, str, str], dict[str, object]] = {}
    for record in output_rows:
        if not isinstance(record, dict):
            _fail("output evidence must be object")
        key = (str(record.get("dataset_id") or ""), str(record.get("timeframe") or ""), str(record.get("instrument_id") or ""))
        if key not in EXPECTED_KEYS or key in output_by_key:
            _fail("unexpected/duplicate output key")
        output_by_key[key] = _validate_manifest_quality(record, run_root)
    if set(output_by_key) != set(EXPECTED_KEYS):
        _fail("Stage 7 output key set mismatch")

    validated: list[dict[str, object]] = []
    for instrument_id, (start, end, expected_d1_rows) in HISTORY.items():
        frozen_validation = frozen_by_instrument[instrument_id]["physical"]
        expected_d1 = _oracle_d1(frozen_validation["records"], instrument_id)
        d1_item = output_by_key[(OHLCV_DATASET, "1D", instrument_id)]
        if len(expected_d1.index) != expected_d1_rows:
            _fail("independent D1 oracle row count mismatch")
        _compare_contract_frame(d1_item["physical"], expected_d1, D1_REQUIRED, d1_item["manifest"], instrument_id + " D1")
        if Path(str(d1_item["manifest"].get("source_ref") or "")).resolve() != Path(frozen_by_instrument[instrument_id]["manifest_path"]).resolve():
            _fail("D1 manifest frozen lineage mismatch")

        expected_w1 = _oracle_w1(expected_d1, start, end)
        w1_item = output_by_key[(OHLCV_DATASET, "1W", instrument_id)]
        _compare_contract_frame(w1_item["physical"], expected_w1, W1_REQUIRED, w1_item["manifest"], instrument_id + " W1")
        if Path(str(w1_item["manifest"].get("source_ref") or "")).resolve() != Path(d1_item["partition"]).resolve():
            _fail("W1 manifest D1 lineage mismatch")

        d1_tech_item = output_by_key[(TECH_DATASET, "1D", instrument_id)]
        expected_d1_tech = _oracle_technical(expected_d1, str(d1_item["record"]["run_id"]))
        _compare_contract_frame(d1_tech_item["physical"], expected_d1_tech, TECH_REQUIRED, d1_tech_item["manifest"], instrument_id + " D1 technical")
        if Path(str(d1_tech_item["manifest"].get("source_ref") or "")).resolve() != Path(d1_item["partition"]).resolve():
            _fail("D1 technical source lineage mismatch")

        w1_tech_item = output_by_key[(TECH_DATASET, "1W", instrument_id)]
        expected_w1_tech = _oracle_technical(expected_w1, str(w1_item["record"]["run_id"]))
        _compare_contract_frame(w1_tech_item["physical"], expected_w1_tech, TECH_REQUIRED, w1_tech_item["manifest"], instrument_id + " W1 technical")
        if Path(str(w1_tech_item["manifest"].get("source_ref") or "")).resolve() != Path(w1_item["partition"]).resolve():
            _fail("W1 technical source lineage mismatch")

        for key in (
            (OHLCV_DATASET, "1D", instrument_id), (OHLCV_DATASET, "1W", instrument_id),
            (TECH_DATASET, "1D", instrument_id), (TECH_DATASET, "1W", instrument_id),
        ):
            item = output_by_key[key]
            validated.append({
                "dataset_id": key[0], "timeframe": key[1], "instrument_id": key[2],
                "producer_run_id": str(item["record"]["run_id"]), "partition": item["partition"],
                "manifest_path": item["manifest_path"], "quality_path": item["quality_path"],
                "row_count": len(item["physical"].index), "physical_readback_passed": True,
            })
    if len(validated) != 8:
        _fail("validated output count mismatch")
    return validated


def _rooted_ref(path: Path) -> str:
    root = _data_root().resolve(strict=True)
    try:
        rel = path.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as exc:
        raise Step7AcceptanceError("accepted artifact must be inside MOEX_DATA_ROOT") from exc
    return ROOT_PREFIX + rel.as_posix()


def _pointer_path(dataset_id: str, timeframe: str, instrument_id: str) -> Path:
    return _data_root() / "state" / "datasets" / ("dataset_id=" + dataset_id) / ("timeframe=" + timeframe) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"


def _stage_json(path: Path, values: Mapping[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".stage") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        return Path(handle.name)


def _restore(path: Path, previous: bytes | None) -> None:
    if previous is None:
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".rollback") as handle:
        handle.write(previous)
        staged = Path(handle.name)
    staged.replace(path)


def _transactional_replace(records: Sequence[tuple[Path, Mapping[str, object]]]) -> None:
    paths = [path for path, _ in records]
    if len(paths) != len(set(paths)):
        _fail("transaction target paths must be unique")
    previous = {path: path.read_bytes() if path.exists() else None for path in paths}
    staged: list[tuple[Path, Path]] = []
    applied: list[Path] = []
    try:
        for final, values in records:
            staged.append((_stage_json(final, values), final))
        for source, final in staged:
            source.replace(final)
            applied.append(final)
    except Exception as exc:
        errors: list[str] = []
        for final in reversed(applied):
            try:
                _restore(final, previous[final])
            except Exception as rollback_exc:
                errors.append(str(rollback_exc))
        if errors:
            raise Step7AcceptanceError("promotion failed and rollback incomplete: " + ";".join(errors)) from exc
        raise Step7AcceptanceError("promotion transaction failed: " + str(exc)) from exc
    finally:
        for source, _ in staged:
            source.unlink(missing_ok=True)


def promote(*, run_id: str, repo_root: str | Path = ".") -> dict[str, object]:
    checked_run = _safe_token(run_id, "run_id")
    evidence_path = _evidence_dir(checked_run) / "pilot_evidence.json"
    validated = validate_pilot(_load_json(evidence_path, "pilot_evidence"), run_id=checked_run, repo_root=repo_root)
    records: list[tuple[Path, Mapping[str, object]]] = []
    summaries: list[dict[str, object]] = []
    for item in validated:
        pointer_path = _pointer_path(str(item["dataset_id"]), str(item["timeframe"]), str(item["instrument_id"]))
        pointer_values = {
            "dataset_id": item["dataset_id"], "timeframe": item["timeframe"], "instrument_id": item["instrument_id"],
            "run_id": item["producer_run_id"], "acceptance_run_id": checked_run,
            "manifest_ref": _rooted_ref(item["manifest_path"]), "quality_report_ref": _rooted_ref(item["quality_path"]),
            "partition_ref": _rooted_ref(item["partition"]), "quality_status": "pass",
            "acceptance_contract_id": CONTRACT_ID, "continuous_series_used": False, "research_ready": False,
        }
        records.append((pointer_path, pointer_values))
        summaries.append({
            "dataset_id": item["dataset_id"], "timeframe": item["timeframe"], "instrument_id": item["instrument_id"],
            "run_id": item["producer_run_id"], "acceptance_run_id": checked_run, "row_count": item["row_count"],
            "pointer_path": pointer_path.as_posix(), "physical_readback_passed": True,
        })
    if len(summaries) != 8:
        _fail("accepted pointer count mismatch")
    marker = _evidence_dir(checked_run) / "accepted_pointers.json"
    result: dict[str, object] = {
        "project": "MOEX_Bot", "step": 7, "status": "accepted", "run_id": checked_run,
        "acceptance_contract_id": CONTRACT_ID, "accepted_pointer_count": 8, "expected_pointer_count": 8,
        "pointers": summaries, "promotion_semantics": "transactional_with_rollback",
        "physical_partition_readback_required": True, "frozen_raw_physical_revalidation_required": True,
        "current_accepted_raw_scope_match_required": True, "independent_d1_w1_oracle_required": True,
        "independent_technical_oracle_required": True, "physical_row_count_binding_required": True,
        "contracted_build_ts_required": True, "continuous_series_used": False,
        "si_cr_continuous_ready": False, "weekly_oi_ready": False,
        "advanced_technical_policy_ready": False, "research_ready": False,
    }
    records.append((marker, result))
    _transactional_replace(records)
    result["acceptance_evidence_path"] = marker.as_posix()
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Accept a passed Stage 7 native RUB D1/W1 technical pilot.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_env_file(args.env_file)
    try:
        result = promote(run_id=args.run_id, repo_root=args.repo_root)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 7, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

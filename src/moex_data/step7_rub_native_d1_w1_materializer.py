from __future__ import annotations

import hashlib
import json
import math
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Final
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

OHLCV_DATASET: Final[str] = "rub_native_ohlcv_htf"
TECH_DATASET: Final[str] = "rub_technical_features_htf"
SOURCE_DATASET: Final[str] = "futures_raw_5m"
MARKET_TZ: Final[str] = "Europe/Moscow"
ALLOWED_INSTRUMENTS: Final[frozenset[str]] = frozenset({"usdrubf_futures_family", "cnyrubf_futures_family"})
EXPECTED_SECID: Final[dict[str, str]] = {
    "usdrubf_futures_family": "USDRUBF",
    "cnyrubf_futures_family": "CNYRUBF",
}
ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"


class Step7MaterializationError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step7MaterializationError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def _iso_date(value: object, field: str) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise Step7MaterializationError(field + " must be YYYY-MM-DD") from exc


def _load_json(path: Path, field: str) -> dict[str, object]:
    if not path.is_file() or path.is_symlink():
        _fail(field + " must be a regular non-symlink JSON file")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise Step7MaterializationError(field + " invalid JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field + " must be an object")
    return values


def _expand_ref(root: Path, value: object, field: str) -> Path:
    text = str(value or "").strip()
    if not text.startswith(ROOT_PREFIX):
        _fail(field + " must be rooted at ${MOEX_DATA_ROOT}")
    rel = text[len(ROOT_PREFIX):]
    if not rel or rel.startswith("/") or ".." in Path(rel).parts:
        _fail(field + " invalid rooted reference")
    path = (root / rel).resolve(strict=True)
    try:
        path.relative_to(root.resolve(strict=True))
    except ValueError as exc:
        raise Step7MaterializationError(field + " escaped MOEX_DATA_ROOT") from exc
    if not path.is_file() or path.is_symlink():
        _fail(field + " must resolve to a regular non-symlink file")
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


def _validate_frozen_manifest(root: Path, manifest_path: Path, instrument_id: str, history_start: str, history_end: str) -> tuple[list[dict[str, object]], str]:
    values = _load_json(manifest_path, "frozen raw manifest")
    if values.get("schema_version") != "step7_frozen_raw_5m_manifest.v1":
        _fail("frozen raw manifest schema mismatch")
    if values.get("dataset_id") != SOURCE_DATASET or values.get("instrument_id") != instrument_id:
        _fail("frozen raw manifest identity mismatch")
    if values.get("source_id") != "moex_algopack_fo_tradestats_5m":
        _fail("frozen raw manifest source mismatch")
    if values.get("requested_start_date") != history_start or values.get("requested_end_date") != history_end:
        _fail("frozen raw manifest requested range mismatch")
    if values.get("freeze_method") != "validated_descriptor_create_only_independent_inode_exact_byte_copy" or values.get("mutable_canonical_raw_read_after_freeze_allowed") is not False:
        _fail("frozen raw manifest freeze semantics mismatch")
    rows = values.get("partitions")
    if not isinstance(rows, list) or not rows:
        _fail("frozen raw manifest partitions missing")
    if int(values.get("partition_count") or 0) != len(rows):
        _fail("frozen raw manifest partition_count mismatch")
    checked: list[dict[str, object]] = []
    content_lines: list[str] = []
    previous = ""
    for row in rows:
        if not isinstance(row, dict):
            _fail("frozen raw partition record must be object")
        trade_date = _iso_date(row.get("trade_date"), "frozen trade_date")
        if trade_date < history_start or trade_date > history_end or (previous and trade_date <= previous):
            _fail("frozen raw partition dates must be unique monotonic inside requested range")
        previous = trade_date
        if row.get("instrument_id") != instrument_id:
            _fail("frozen raw partition instrument mismatch")
        if row.get("independent_inode_exact_byte_copy") is not True:
            _fail("frozen raw partition independent-copy evidence missing")
        expected_sha = str(row.get("sha256") or "").strip().lower()
        if len(expected_sha) != 64:
            _fail("frozen raw partition SHA-256 missing")
        path = _expand_ref(root, row.get("frozen_ref"), "frozen_ref")
        if _sha_file(path) != expected_sha:
            _fail("frozen raw partition SHA-256 mismatch")
        checked.append({**row, "trade_date": trade_date, "path": path})
        content_lines.append(trade_date + "\t" + expected_sha + "\n")
    digest = hashlib.sha256("".join(content_lines).encode("utf-8")).hexdigest()
    if str(values.get("frozen_content_sha256") or "").strip().lower() != digest:
        _fail("frozen raw manifest content digest mismatch")
    return checked, digest


def build_d1(*, data_root: str | Path, frozen_manifest_path: str | Path, instrument_id: str, history_start: str, history_end: str) -> pd.DataFrame:
    root = Path(data_root).resolve()
    instrument = _safe_token(instrument_id, "instrument_id")
    if instrument not in ALLOWED_INSTRUMENTS:
        _fail("Stage 7 D1 scope is USDRUBF/CNYRUBF only")
    start = _iso_date(history_start, "history_start")
    end = _iso_date(history_end, "history_end")
    records, _ = _validate_frozen_manifest(root, Path(frozen_manifest_path).resolve(), instrument, start, end)
    result: list[dict[str, object]] = []
    expected_secid = EXPECTED_SECID[instrument]
    for record in records:
        frame = pd.read_parquet(record["path"])
        required = ["instrument_id", "trade_date", "ts", "secid", "open", "high", "low", "close", "volume", "value", "num_trades"]
        missing = [c for c in required if c not in frame.columns]
        if missing or frame.empty:
            _fail("frozen raw partition missing D1 fields or empty")
        trade_date = str(record["trade_date"])
        if set(frame["instrument_id"].astype(str)) != {instrument} or set(frame["trade_date"].astype(str)) != {trade_date}:
            _fail("frozen raw partition D1 identity mismatch")
        if set(frame["secid"].astype(str)) != {expected_secid}:
            _fail("frozen raw partition D1 secid mismatch")
        work = frame.copy()
        work["_ts"] = pd.to_datetime(work["ts"], errors="coerce")
        if bool(work["_ts"].isna().any()):
            _fail("frozen raw partition has invalid ts")
        for field in ("open", "high", "low", "close"):
            work[field] = pd.to_numeric(work[field], errors="coerce")
            if bool(work[field].isna().any()) or not np.isfinite(work[field].astype(float)).all():
                _fail("frozen raw partition invalid OHLC: " + field)
        work = work.sort_values("_ts", kind="mergesort").reset_index(drop=True)
        if bool((work["high"] < work["low"]).any()):
            _fail("frozen raw high below low")
        result.append({
            "instrument_id": instrument,
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
    out = pd.DataFrame(result).sort_values("trade_date").reset_index(drop=True)
    if out.empty or out.duplicated(subset=["instrument_id", "trade_date"]).any():
        _fail("D1 output empty or duplicate")
    return out


def _week_dates(value: str) -> tuple[str, str]:
    current = date.fromisoformat(value)
    monday = current - timedelta(days=current.weekday())
    sunday = monday + timedelta(days=6)
    return monday.isoformat(), sunday.isoformat()


def build_w1(d1: pd.DataFrame, *, history_start: str, history_end: str) -> pd.DataFrame:
    if d1.empty:
        _fail("D1 source empty")
    start = date.fromisoformat(_iso_date(history_start, "history_start"))
    end = date.fromisoformat(_iso_date(history_end, "history_end"))
    work = d1.copy().sort_values("trade_date").reset_index(drop=True)
    work[["week_start_date", "week_end_date"]] = work["trade_date"].apply(lambda x: pd.Series(_week_dates(str(x))))
    work = work.loc[
        work["week_start_date"].map(lambda x: date.fromisoformat(x) >= start)
        & work["week_end_date"].map(lambda x: date.fromisoformat(x) <= end)
    ].copy()
    rows: list[dict[str, object]] = []
    for week_start, part in work.groupby("week_start_date", sort=True):
        part = part.sort_values("trade_date").reset_index(drop=True)
        week_end = str(part["week_end_date"].iloc[0])
        lineage_payload = "".join(str(row.trade_date) + "\t" + str(row.source_lineage_sha256) + "\n" for row in part.itertuples()).encode("utf-8")
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
            "source_lineage_sha256": hashlib.sha256(lineage_payload).hexdigest(),
        })
    out = pd.DataFrame(rows)
    if out.empty or out.duplicated(subset=["instrument_id", "week_start_date"]).any():
        _fail("W1 output empty or duplicate")
    return out.reset_index(drop=True)


def _wilder_atr(tr: pd.Series, window: int) -> pd.Series:
    values = pd.to_numeric(tr, errors="coerce").astype(float)
    out = pd.Series(np.nan, index=values.index, dtype="float64")
    if len(values.index) < window:
        return out
    seed = float(values.iloc[:window].mean())
    out.iloc[window - 1] = seed
    previous = seed
    for idx in range(window, len(values.index)):
        current = float(values.iloc[idx])
        previous = previous + (current - previous) / float(window)
        out.iloc[idx] = previous
    return out


def _require_finite_feature(series: pd.Series, field: str) -> None:
    numeric = pd.to_numeric(series, errors="coerce")
    present = numeric.dropna().to_numpy(dtype="float64")
    if not np.isfinite(present).all():
        _fail("technical feature contains non-finite value: " + field)


def build_technical_features(ohlcv: pd.DataFrame, *, source_ohlcv_run_id: str) -> pd.DataFrame:
    if ohlcv.empty:
        _fail("technical source OHLCV empty")
    source_run = _safe_token(source_ohlcv_run_id, "source_ohlcv_run_id")
    sort_field = "trade_date" if str(ohlcv["timeframe"].iloc[0]) == "1D" else "week_start_date"
    work = ohlcv.copy().sort_values(sort_field).reset_index(drop=True)
    for field in ("open", "high", "low", "close"):
        work[field] = pd.to_numeric(work[field], errors="coerce").astype(float)
        if bool(work[field].isna().any()) or not np.isfinite(work[field]).all():
            _fail("technical source invalid OHLC")
    prev_close = work["close"].shift(1)
    if len(work.index) > 1 and bool(prev_close.iloc[1:].eq(0.0).any()):
        _fail("technical previous close denominator is zero")
    prev_high = work["high"].shift(1)
    prev_low = work["low"].shift(1)
    high_low = work["high"] - work["low"]
    true_range = pd.concat([high_low, (work["high"] - prev_close).abs(), (work["low"] - prev_close).abs()], axis=1).max(axis=1, skipna=True)
    out = work[["instrument_id", "secid", "timeframe", "period_start_date", "period_end_date", "availability_ts_utc", "close"]].copy()
    out["return_1obs"] = work["close"] / prev_close - 1.0
    out["gap_abs"] = work["open"] - prev_close
    out["gap_pct"] = work["open"] / prev_close - 1.0
    out["range_abs"] = high_low
    out["true_range"] = true_range
    out["atr_14_wilder"] = _wilder_atr(true_range, 14)
    out["atr_20_wilder"] = _wilder_atr(true_range, 20)
    for field in ("return_1obs", "gap_abs", "gap_pct", "range_abs", "true_range", "atr_14_wilder", "atr_20_wilder"):
        _require_finite_feature(out[field], field)
    relations = {
        "higher_high_vs_prev_bar": work["high"] > prev_high,
        "higher_low_vs_prev_bar": work["low"] > prev_low,
        "lower_high_vs_prev_bar": work["high"] < prev_high,
        "lower_low_vs_prev_bar": work["low"] < prev_low,
        "close_break_prev_high": work["close"] > prev_high,
        "close_break_prev_low": work["close"] < prev_low,
    }
    for field, series in relations.items():
        out[field] = series.astype("boolean")
        out.loc[0, field] = pd.NA
    out["source_ohlcv_run_id"] = source_run
    return out


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name("." + path.name + ".tmp")
    try:
        frame.to_parquet(temp, index=False)
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def _atomic_json(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        temp = Path(handle.name)
    temp.replace(path)


def _write_output(*, run_root: Path, dataset_id: str, instrument_id: str, timeframe: str, producer_run_id: str, frame: pd.DataFrame, source_ref: str, history_start: str, history_end: str) -> dict[str, object]:
    partition = run_root / "market" / "derived" / ("dataset_id=" + dataset_id) / ("timeframe=" + timeframe) / ("instrument_id=" + instrument_id) / "part.parquet"
    manifest = run_root / "state" / "refresh" / ("dataset_id=" + dataset_id) / ("timeframe=" + timeframe) / ("instrument_id=" + instrument_id) / "manifest.json"
    quality = run_root / "state" / "quality" / ("dataset_id=" + dataset_id) / ("timeframe=" + timeframe) / ("instrument_id=" + instrument_id) / "quality_report.json"
    for target in (partition, manifest, quality):
        if target.exists():
            _fail("immutable Stage 7 output target already exists")
    build_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    frame = frame.copy()
    frame["build_ts_utc"] = build_ts
    _atomic_parquet(partition, frame)
    quality_values = {
        "dataset_id": dataset_id,
        "instrument_id": instrument_id,
        "timeframe": timeframe,
        "run_id": producer_run_id,
        "quality_status": "pass",
        "row_count": int(len(frame.index)),
        "duplicate_period_count": int(frame.duplicated(subset=["instrument_id", "period_start_date", "timeframe"]).sum()),
        "history_start": history_start,
        "history_end": history_end,
    }
    manifest_values = {
        **quality_values,
        "partition_path": partition.as_posix(),
        "quality_report_path": quality.as_posix(),
        "source_ref": source_ref,
        "producer": "moex_data.step7_rub_native_d1_w1_materializer.v1",
        "network_calls_used": False,
        "latest_autodetect_used": False,
        "continuous_series_used": False,
        "build_ts_utc": build_ts,
    }
    _atomic_json(quality, quality_values)
    _atomic_json(manifest, manifest_values)
    return {
        "status": "succeeded",
        "dataset_id": dataset_id,
        "instrument_id": instrument_id,
        "timeframe": timeframe,
        "run_id": producer_run_id,
        "quality_status": "pass",
        "row_count": int(len(frame.index)),
        "partition_path": partition.as_posix(),
        "manifest_path": manifest.as_posix(),
        "quality_report_path": quality.as_posix(),
        "source_ref": source_ref,
    }


def materialize_instrument(*, data_root: str | Path, run_root: str | Path, frozen_manifest_path: str | Path, instrument_id: str, history_start: str, history_end: str, run_id: str) -> list[dict[str, object]]:
    root = Path(data_root).resolve()
    run = Path(run_root).resolve()
    instrument = _safe_token(instrument_id, "instrument_id")
    checked_run = _safe_token(run_id, "run_id")
    start = _iso_date(history_start, "history_start")
    end = _iso_date(history_end, "history_end")
    frozen_manifest = Path(frozen_manifest_path).resolve()
    d1 = build_d1(data_root=root, frozen_manifest_path=frozen_manifest, instrument_id=instrument, history_start=start, history_end=end)
    d1_run = checked_run + "_" + instrument + "_d1"
    d1_output = _write_output(
        run_root=run, dataset_id=OHLCV_DATASET, instrument_id=instrument, timeframe="1D", producer_run_id=d1_run,
        frame=d1, source_ref=frozen_manifest.as_posix(), history_start=start, history_end=end,
    )
    w1 = build_w1(d1, history_start=start, history_end=end)
    w1_run = checked_run + "_" + instrument + "_w1"
    w1_output = _write_output(
        run_root=run, dataset_id=OHLCV_DATASET, instrument_id=instrument, timeframe="1W", producer_run_id=w1_run,
        frame=w1, source_ref=d1_output["partition_path"], history_start=start, history_end=end,
    )
    d1_tech = build_technical_features(d1, source_ohlcv_run_id=d1_run)
    d1_tech_run = checked_run + "_" + instrument + "_d1_technical"
    d1_tech_output = _write_output(
        run_root=run, dataset_id=TECH_DATASET, instrument_id=instrument, timeframe="1D", producer_run_id=d1_tech_run,
        frame=d1_tech, source_ref=d1_output["partition_path"], history_start=start, history_end=end,
    )
    w1_tech = build_technical_features(w1, source_ohlcv_run_id=w1_run)
    w1_tech_run = checked_run + "_" + instrument + "_w1_technical"
    w1_tech_output = _write_output(
        run_root=run, dataset_id=TECH_DATASET, instrument_id=instrument, timeframe="1W", producer_run_id=w1_tech_run,
        frame=w1_tech, source_ref=w1_output["partition_path"], history_start=start, history_end=end,
    )
    return [d1_output, w1_output, d1_tech_output, w1_tech_output]

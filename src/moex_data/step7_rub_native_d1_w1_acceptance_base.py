from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import stat
import sys
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd

# Load the reviewed Stage 7 implementation body under the canonical module name,
# then replace the security-sensitive shared surfaces below. The .inc body is not
# importable/executable as an alternate Python module, so every public entrypoint
# uses the same hardened implementation.
_IMPL_PATH = Path(__file__).with_name("step7_rub_native_d1_w1_acceptance_impl.inc")
_REAL_NAME = __name__
_CANONICAL_NAME = "moex_data.step7_rub_native_d1_w1_acceptance_base"
_current_module = sys.modules[_REAL_NAME]
_existing_canonical = sys.modules.get(_CANONICAL_NAME)
if _existing_canonical is not None and _existing_canonical is not _current_module:
    raise RuntimeError("canonical Stage 7 acceptance base already loaded as a different object")
sys.modules[_CANONICAL_NAME] = _current_module
globals()["__name__"] = _CANONICAL_NAME
try:
    exec(compile(_IMPL_PATH.read_text(encoding="utf-8"), _IMPL_PATH.as_posix(), "exec"), globals(), globals())
finally:
    globals()["__name__"] = _REAL_NAME

from moex_data.futures import stage2_raw_history_content_reattestation as content_attestation
from moex_data.futures.freeze_step7_accepted_raw_5m import accepted_quote_history as _content_attested_history
from moex_data.futures.freeze_step7_accepted_raw_5m import quote_validation_expectation as _quote_validation_expectation

_BASE_VALIDATE_PILOT = validate_pilot
_BASE_TRANSACTIONAL_REPLACE = _transactional_replace


class _FrozenFrameRecord(dict):
    """Validation-only record carrying the exact captured parquet frame."""


def _stat_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (int(info.st_dev), int(info.st_ino), int(info.st_size), int(info.st_mtime_ns), int(info.st_ctime_ns))


def _inside_run(path_value: object, run_root: Path, field: str) -> Path:
    path = Path(str(path_value or ""))
    if not path.is_absolute():
        _fail(field + " must be absolute")
    try:
        parent = path.parent.resolve(strict=True)
        candidate = parent / path.name
        candidate.relative_to(run_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Step7AcceptanceError(field + " must exist inside run root") from exc
    if candidate.is_symlink() or not candidate.is_file():
        _fail(field + " must be regular non-symlink file")
    return candidate


def _capture_regular_bytes(path: Path, field: str) -> tuple[bytes, str, tuple[int, int, int, int, int]]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise Step7AcceptanceError(field + " cannot be opened as regular non-symlink file") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            _fail(field + " descriptor is not regular")
        chunks: list[bytes] = []
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            chunks.append(block)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        try:
            pathname = os.stat(path, follow_symlinks=False)
        except OSError as exc:
            raise Step7AcceptanceError(field + " pathname changed during validated byte capture") from exc
        identity = _stat_identity(before)
        if identity != _stat_identity(after) or identity != _stat_identity(pathname) or len(raw) != int(before.st_size):
            _fail(field + " changed during validated byte capture")
        return raw, hashlib.sha256(raw).hexdigest(), identity
    finally:
        os.close(descriptor)


def _capture_output_json(path: Path, field: str) -> tuple[dict[str, object], str, tuple[int, int, int, int, int]]:
    raw, digest, identity = _capture_regular_bytes(path, field)
    try:
        values = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise Step7AcceptanceError(field + " invalid JSON") from exc
    if not isinstance(values, dict):
        _fail(field + " must be object")
    return values, digest, identity


def _capture_output_parquet(path: Path) -> tuple[pd.DataFrame, str, tuple[int, int, int, int, int]]:
    raw, digest, identity = _capture_regular_bytes(path, "output partition")
    try:
        frame = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        raise Step7AcceptanceError("output partition parquet unreadable") from exc
    return frame, digest, identity


def _guard_frozen_refs_inside_run_root(manifest_path: Path) -> Path:
    manifest = Path(manifest_path).resolve(strict=True)
    try:
        run_root = manifest.parents[3]
    except IndexError as exc:
        raise Step7AcceptanceError("frozen raw manifest is not under declared Stage 7 run root") from exc
    if not run_root.name.startswith("run_id=") or run_root.parent.name != "step7_rub_native_d1_w1":
        raise Step7AcceptanceError("frozen raw manifest is not under declared Stage 7 run root")
    values = _load_json(manifest, "frozen raw manifest")
    records = values.get("partitions")
    if not isinstance(records, list) or not records:
        raise Step7AcceptanceError("frozen raw manifest partition records missing")
    expected_root = (run_root / "inputs" / "dataset_id=futures_raw_5m").resolve()
    for record in records:
        if not isinstance(record, Mapping):
            raise Step7AcceptanceError("frozen raw record must be object")
        path = _expand_root_ref(record.get("frozen_ref"), run_root=run_root)
        try:
            path.relative_to(expected_root)
        except ValueError as exc:
            raise Step7AcceptanceError("frozen raw partition escaped immutable Stage 7 input root") from exc
    return run_root


def _guard_current_content_attestation(*, repo_root: Path, data_root: Path, manifest_path: Path, instrument_id: str, start: str, end: str):
    values = _load_json(Path(manifest_path), "frozen raw manifest")
    if values.get("source_mode") != "stage2_content_attested_generation_snapshots_only":
        raise Step7AcceptanceError("frozen raw source_mode is not content-attested snapshots only")
    if values.get("legacy_pointer_consumption_used") is not False:
        raise Step7AcceptanceError("legacy accepted pointer consumption is forbidden")
    if values.get("network_calls_used") is not False or values.get("latest_autodetect_used") is not False:
        raise Step7AcceptanceError("frozen raw execution boundary mismatch")
    current = _content_attested_history(data_root, instrument_id, start, end, repo_root=repo_root)
    exact_pairs = (
        ("content_attestation_generation_id", current.acceptance_run_id),
        ("content_attestation_marker_ref", current.pointer_ref),
        ("content_attestation_marker_sha256", current.marker_sha256),
        ("content_attested_manifest_ref", current.manifest_ref),
        ("content_attested_manifest_sha256", current.manifest_sha256),
        ("content_attested_partition_content_set_sha256", current.partition_content_set_sha256),
        ("frozen_content_sha256", current.partition_content_set_sha256),
        ("accepted_partition_dates_sha256", current.partition_dates_sha256),
    )
    for field, expected in exact_pairs:
        if values.get(field) != expected:
            raise Step7AcceptanceError("frozen raw current content-attestation mismatch: " + field)
    if int(values.get("partition_count") or -1) != len(current.accepted_dates):
        raise Step7AcceptanceError("frozen raw current content-attestation partition_count mismatch")
    if int(values.get("row_count") or -1) != current.row_count:
        raise Step7AcceptanceError("frozen raw current content-attestation row_count mismatch")
    return current


def _capture_frozen_frame(path: Path, expected_sha: str) -> tuple[pd.DataFrame, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise Step7AcceptanceError("frozen raw descriptor is not regular")
        chunks: list[bytes] = []
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            chunks.append(block)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        pathname = os.stat(path, follow_symlinks=False)
        if _stat_identity(before) != _stat_identity(after) or _stat_identity(before) != _stat_identity(pathname) or len(raw) != int(before.st_size):
            raise Step7AcceptanceError("frozen raw partition changed during validated byte capture")
        if hashlib.sha256(raw).hexdigest() != expected_sha:
            raise Step7AcceptanceError("frozen raw physical SHA-256 mismatch")
        try:
            frame = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            raise Step7AcceptanceError("frozen raw parquet unreadable") from exc
        return frame, before
    finally:
        os.close(descriptor)


def _revalidate_frozen(*, repo_root: Path, data_root: Path, manifest_path: Path, instrument_id: str, start: str, end: str, validation_run_id: str) -> dict[str, object]:
    run_root = _guard_frozen_refs_inside_run_root(Path(manifest_path))
    current = _guard_current_content_attestation(
        repo_root=repo_root,
        data_root=data_root,
        manifest_path=Path(manifest_path),
        instrument_id=instrument_id,
        start=start,
        end=end,
    )
    frozen = _load_json(Path(manifest_path), "frozen raw manifest")
    if frozen.get("schema_version") != "step7_frozen_raw_5m_manifest.v1" or frozen.get("dataset_id") != SOURCE_DATASET:
        _fail("frozen raw manifest schema/dataset mismatch")
    if frozen.get("instrument_id") != instrument_id or frozen.get("source_id") != SOURCE_ID:
        _fail("frozen raw manifest identity/source mismatch")
    if frozen.get("freeze_method") != "validated_descriptor_create_only_independent_inode_exact_byte_copy":
        _fail("frozen raw freeze semantics mismatch")
    if frozen.get("mutable_canonical_raw_read_after_freeze_allowed") is not False:
        _fail("mutable canonical raw read after freeze is forbidden")
    records = frozen.get("partitions")
    if not isinstance(records, list) or len(records) != int(frozen.get("partition_count") or -1):
        _fail("frozen raw partition records/count mismatch")
    expectation = _quote_validation_expectation(
        instrument_id,
        start,
        end,
        expected_partitions=len(current.accepted_dates),
        expected_rows=current.row_count,
    )
    expected_secid = EXPECTED_SECID[instrument_id]
    physical_records: list[dict[str, object]] = []
    content_lines: list[str] = []
    dates: list[str] = []
    total_rows = 0
    expected_root = (run_root / "inputs" / "dataset_id=futures_raw_5m").resolve()
    for record in records:
        if not isinstance(record, Mapping):
            _fail("frozen raw record must be object")
        trade_date = str(record.get("trade_date") or "")
        if trade_date in dates:
            _fail("duplicate frozen trade_date")
        dates.append(trade_date)
        if record.get("independent_inode_exact_byte_copy") is not True:
            _fail("frozen raw independent-copy evidence missing")
        path = _expand_root_ref(record.get("frozen_ref"), run_root=run_root)
        try:
            path.relative_to(expected_root)
        except ValueError as exc:
            raise Step7AcceptanceError("frozen raw partition escaped immutable Stage 7 input root") from exc
        expected_sha = str(record.get("sha256") or "").strip().lower()
        if len(expected_sha) != 64:
            _fail("frozen raw partition SHA-256 missing")
        frame, frozen_stat = _capture_frozen_frame(path, expected_sha)
        source_identity = record.get("validated_source_identity")
        if not isinstance(source_identity, Mapping):
            _fail("validated source identity evidence missing")
        if (int(frozen_stat.st_dev), int(frozen_stat.st_ino)) == (int(source_identity.get("st_dev") or -1), int(source_identity.get("st_ino") or -1)):
            _fail("frozen raw copy shares content-attested source inode")
        rows, secids = stage2._validate_quote_partition(repo_root, frame, expectation, trade_date, validation_run_id)
        if int(rows) != int(record.get("row_count") or -1):
            _fail("frozen raw physical row_count mismatch")
        if set(secids) != {expected_secid} or set(record.get("secids") or []) != {expected_secid}:
            _fail("frozen raw physical secid mismatch")
        physical_records.append(_FrozenFrameRecord({
            "trade_date": trade_date,
            "path": path,
            "sha256": expected_sha,
            "row_count": int(rows),
            "frame": frame,
        }))
        content_lines.append(trade_date + "\t" + expected_sha + "\n")
        total_rows += int(rows)
    if tuple(dates) != current.accepted_dates:
        _fail("frozen raw date set no longer equals current content-attested scope")
    digest = hashlib.sha256("".join(content_lines).encode("utf-8")).hexdigest()
    if str(frozen.get("frozen_content_sha256") or "").strip().lower() != digest or digest != current.partition_content_set_sha256:
        _fail("frozen raw aggregate content digest mismatch")
    if total_rows != current.row_count:
        _fail("frozen raw aggregate row_count mismatch")
    return {
        "partition_count": len(physical_records),
        "row_count": total_rows,
        "content_sha256": digest,
        "records": physical_records,
        "current_accepted_scope_match": True,
        "current_content_attestation_match": True,
        "physical_revalidation_passed": True,
    }


def _oracle_d1(records: Sequence[Mapping[str, object]], instrument_id: str) -> pd.DataFrame:
    expected_secid = EXPECTED_SECID[instrument_id]
    rows: list[dict[str, object]] = []
    for record in records:
        trade_date = str(record["trade_date"])
        frame = record.get("frame")
        if not isinstance(frame, pd.DataFrame):
            _fail("independent D1 oracle requires captured validated frame")
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


def _oracle_technical(ohlcv: pd.DataFrame, source_run_id: str) -> pd.DataFrame:
    sort_field = "trade_date" if str(ohlcv["timeframe"].iloc[0]) == "1D" else "week_start_date"
    work = ohlcv.copy().sort_values(sort_field).reset_index(drop=True)
    for field in ("open", "high", "low", "close"):
        work[field] = pd.to_numeric(work[field], errors="coerce").astype(float)
        if bool(work[field].isna().any()) or not np.isfinite(work[field]).all():
            _fail("oracle technical source invalid OHLC")
    previous_close = work["close"].shift(1)
    if len(work.index) > 1 and bool(previous_close.iloc[1:].eq(0.0).any()):
        _fail("oracle technical previous close denominator is zero")
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
    for field in ("return_1obs", "gap_abs", "gap_pct", "range_abs", "true_range", "atr_14_wilder", "atr_20_wilder"):
        present = pd.to_numeric(out[field], errors="coerce").dropna().to_numpy(dtype="float64")
        if not np.isfinite(present).all():
            _fail("oracle technical feature contains non-finite value: " + field)
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


def _validate_manifest_quality(record: Mapping[str, object], run_root: Path) -> dict[str, object]:
    partition = _inside_run(record.get("partition_path"), run_root, "partition_path")
    manifest_path = _inside_run(record.get("manifest_path"), run_root, "manifest_path")
    quality_path = _inside_run(record.get("quality_report_path"), run_root, "quality_report_path")
    physical, partition_sha, partition_identity = _capture_output_parquet(partition)
    manifest, manifest_sha, manifest_identity = _capture_output_json(manifest_path, "output manifest")
    quality, quality_sha, quality_identity = _capture_output_json(quality_path, "output quality")
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
    if Path(str(manifest.get("partition_path") or "")).resolve() != partition.resolve():
        _fail("manifest partition path mismatch")
    if Path(str(manifest.get("quality_report_path") or "")).resolve() != quality_path.resolve():
        _fail("manifest quality path mismatch")
    if manifest.get("network_calls_used") is not False or manifest.get("latest_autodetect_used") is not False or manifest.get("continuous_series_used") is not False:
        _fail("manifest execution boundary mismatch")
    if not isinstance(record, dict):
        _fail("output evidence record must be mutable object")
    record["_validated_partition_sha256"] = partition_sha
    record["_validated_manifest_sha256"] = manifest_sha
    record["_validated_quality_report_sha256"] = quality_sha
    record["_validated_partition_identity"] = partition_identity
    record["_validated_manifest_identity"] = manifest_identity
    record["_validated_quality_report_identity"] = quality_identity
    return {
        "record": record,
        "partition": partition,
        "manifest_path": manifest_path,
        "quality_path": quality_path,
        "manifest": manifest,
        "quality": quality,
        "physical": physical,
        "partition_sha256": partition_sha,
        "manifest_sha256": manifest_sha,
        "quality_report_sha256": quality_sha,
        "partition_identity": partition_identity,
        "manifest_identity": manifest_identity,
        "quality_report_identity": quality_identity,
    }


def validate_pilot(values: Mapping[str, object], *, run_id: str, repo_root: str | Path = ".") -> list[dict[str, object]]:
    validated = _BASE_VALIDATE_PILOT(values, run_id=run_id, repo_root=repo_root)
    output_rows = values.get("outputs")
    if not isinstance(output_rows, list) or len(output_rows) != 8:
        _fail("pilot must have eight output records")
    bound: dict[tuple[str, str, str], Mapping[str, object]] = {}
    for record in output_rows:
        if not isinstance(record, Mapping):
            _fail("output evidence must be object")
        key = (str(record.get("dataset_id") or ""), str(record.get("timeframe") or ""), str(record.get("instrument_id") or ""))
        if key in bound:
            _fail("duplicate output content binding key")
        for field in (
            "_validated_partition_sha256", "_validated_manifest_sha256", "_validated_quality_report_sha256",
            "_validated_partition_identity", "_validated_manifest_identity", "_validated_quality_report_identity",
        ):
            if field not in record:
                _fail("validated output content binding missing: " + field)
        bound[key] = record
    enriched: list[dict[str, object]] = []
    for item in validated:
        key = (str(item["dataset_id"]), str(item["timeframe"]), str(item["instrument_id"]))
        record = bound.get(key)
        if record is None:
            _fail("validated output content binding key missing")
        enriched.append({
            **item,
            "partition_sha256": record["_validated_partition_sha256"],
            "manifest_sha256": record["_validated_manifest_sha256"],
            "quality_report_sha256": record["_validated_quality_report_sha256"],
            "partition_identity": tuple(record["_validated_partition_identity"]),
            "manifest_identity": tuple(record["_validated_manifest_identity"]),
            "quality_report_identity": tuple(record["_validated_quality_report_identity"]),
        })
    return enriched


def _recheck_validated_output(item: Mapping[str, object]) -> None:
    checks = (
        ("partition", "partition_sha256", "partition_identity", "output partition"),
        ("manifest_path", "manifest_sha256", "manifest_identity", "output manifest"),
        ("quality_path", "quality_report_sha256", "quality_report_identity", "output quality"),
    )
    for path_field, sha_field, identity_field, label in checks:
        path = Path(item[path_field])
        _, digest, identity = _capture_regular_bytes(path, label + " pre-promotion recheck")
        expected_sha = str(item.get(sha_field) or "").strip().lower()
        expected_identity = tuple(item.get(identity_field) or ())
        if len(expected_sha) != 64 or digest != expected_sha or identity != expected_identity:
            _fail(label + " changed after validation")


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _stage2_marker_sha() -> tuple[Path, str]:
    marker = content_attestation._current_marker_path()
    if not marker.is_file() or marker.is_symlink():
        _fail("current Stage 2 content-attestation marker missing/non-regular")
    return marker, _sha_file(marker)


def promote(*, run_id: str, repo_root: str | Path = ".") -> dict[str, object]:
    checked_run = _safe_token(run_id, "run_id")
    stage7_lock = _data_root() / "state" / "locks" / "step7_rub_native_d1_w1_acceptance.lock"
    stage2_lock = content_attestation._lock_path()
    with _exclusive_lock(stage7_lock):
        with _exclusive_lock(stage2_lock):
            marker_path, marker_sha = _stage2_marker_sha()
            evidence_path = _evidence_dir(checked_run) / "pilot_evidence.json"
            evidence = _load_json(evidence_path, "pilot_evidence")
            validated = validate_pilot(evidence, run_id=checked_run, repo_root=repo_root)
            frozen_rows = evidence.get("frozen_inputs")
            if not isinstance(frozen_rows, list) or len(frozen_rows) != 2:
                _fail("pilot must have two frozen input manifests")
            for frozen_row in frozen_rows:
                if not isinstance(frozen_row, Mapping):
                    _fail("frozen input evidence must be object")
                manifest_path = Path(str(frozen_row.get("manifest_path") or ""))
                manifest = _load_json(manifest_path, "frozen raw manifest")
                if manifest.get("content_attestation_marker_sha256") != marker_sha:
                    _fail("Stage 2 content-attestation marker changed before promotion")

            records: list[tuple[Path, Mapping[str, object]]] = []
            summaries: list[dict[str, object]] = []
            for item in validated:
                pointer_path = _pointer_path(str(item["dataset_id"]), str(item["timeframe"]), str(item["instrument_id"]))
                pointer_values = {
                    "dataset_id": item["dataset_id"],
                    "timeframe": item["timeframe"],
                    "instrument_id": item["instrument_id"],
                    "run_id": item["producer_run_id"],
                    "acceptance_run_id": checked_run,
                    "manifest_ref": _rooted_ref(item["manifest_path"]),
                    "manifest_sha256": item["manifest_sha256"],
                    "quality_report_ref": _rooted_ref(item["quality_path"]),
                    "quality_report_sha256": item["quality_report_sha256"],
                    "partition_ref": _rooted_ref(item["partition"]),
                    "partition_sha256": item["partition_sha256"],
                    "quality_status": "pass",
                    "acceptance_contract_id": CONTRACT_ID,
                    "continuous_series_used": False,
                    "research_ready": False,
                }
                records.append((pointer_path, pointer_values))
                summaries.append({
                    "dataset_id": item["dataset_id"],
                    "timeframe": item["timeframe"],
                    "instrument_id": item["instrument_id"],
                    "run_id": item["producer_run_id"],
                    "acceptance_run_id": checked_run,
                    "row_count": item["row_count"],
                    "pointer_path": pointer_path.as_posix(),
                    "partition_sha256": item["partition_sha256"],
                    "manifest_sha256": item["manifest_sha256"],
                    "quality_report_sha256": item["quality_report_sha256"],
                    "physical_readback_passed": True,
                })
            if len(summaries) != 8:
                _fail("accepted pointer count mismatch")
            marker = _evidence_dir(checked_run) / "accepted_pointers.json"
            result: dict[str, object] = {
                "project": "MOEX_Bot",
                "step": 7,
                "status": "accepted",
                "run_id": checked_run,
                "acceptance_contract_id": CONTRACT_ID,
                "accepted_pointer_count": 8,
                "expected_pointer_count": 8,
                "pointers": summaries,
                "promotion_semantics": "serialized_transactional_with_rollback",
                "stage7_publication_lock_held": True,
                "stage2_content_attestation_lock_held": True,
                "stage2_content_attestation_marker_sha256": marker_sha,
                "physical_partition_readback_required": True,
                "frozen_raw_physical_revalidation_required": True,
                "current_accepted_raw_scope_match_required": True,
                "independent_d1_w1_oracle_required": True,
                "independent_technical_oracle_required": True,
                "physical_row_count_binding_required": True,
                "contracted_build_ts_required": True,
                "output_single_descriptor_capture_required": True,
                "output_content_sha256_binding_required": True,
                "output_identity_sha256_prewrite_recheck_required": True,
                "continuous_series_used": False,
                "si_cr_continuous_ready": False,
                "weekly_oi_ready": False,
                "advanced_technical_policy_ready": False,
                "research_ready": False,
            }
            records.append((marker, result))
            if _sha_file(marker_path) != marker_sha:
                _fail("Stage 2 content-attestation marker changed immediately before output recheck")
            for item in validated:
                _recheck_validated_output(item)
            if _sha_file(marker_path) != marker_sha:
                _fail("Stage 2 content-attestation marker changed immediately before pointer writes")
            _BASE_TRANSACTIONAL_REPLACE(records)
            result["acceptance_evidence_path"] = marker.as_posix()
            return result


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


if _REAL_NAME == "__main__":
    raise SystemExit(main())

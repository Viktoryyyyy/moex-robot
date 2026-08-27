from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import stat
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd

from moex_data import step7_rub_native_d1_w1_acceptance_base as base
from moex_data.futures import stage2_raw_history_content_reattestation as content_attestation
from moex_data.futures.freeze_step7_accepted_raw_5m import accepted_quote_history, quote_validation_expectation

# Re-export the established Stage 7 acceptance surface for existing callers/tests.
for _name in dir(base):
    if _name not in globals():
        globals()[_name] = getattr(base, _name)


class _FrozenFrameRecord(dict):
    """Validation-only record carrying the exact captured parquet frame."""


def _guard_frozen_refs_inside_run_root(manifest_path: Path) -> Path:
    manifest = Path(manifest_path).resolve(strict=True)
    try:
        run_root = manifest.parents[3]
    except IndexError as exc:
        raise base.Step7AcceptanceError("frozen raw manifest is not under declared Stage 7 run root") from exc
    if run_root.name.startswith("run_id=") is False or run_root.parent.name != "step7_rub_native_d1_w1":
        raise base.Step7AcceptanceError("frozen raw manifest is not under declared Stage 7 run root")

    values = base._load_json(manifest, "frozen raw manifest")
    records = values.get("partitions")
    if not isinstance(records, list) or not records:
        raise base.Step7AcceptanceError("frozen raw manifest partition records missing")

    expected_root = (run_root / "inputs" / "dataset_id=futures_raw_5m").resolve()
    for record in records:
        if not isinstance(record, Mapping):
            raise base.Step7AcceptanceError("frozen raw record must be object")
        path = base._expand_root_ref(record.get("frozen_ref"), run_root=run_root)
        try:
            path.relative_to(expected_root)
        except ValueError as exc:
            raise base.Step7AcceptanceError("frozen raw partition escaped immutable Stage 7 input root") from exc
    return run_root


def _guard_current_content_attestation(*, repo_root: Path, data_root: Path, manifest_path: Path, instrument_id: str, start: str, end: str):
    values = base._load_json(Path(manifest_path), "frozen raw manifest")
    if values.get("source_mode") != "stage2_content_attested_generation_snapshots_only":
        raise base.Step7AcceptanceError("frozen raw source_mode is not content-attested snapshots only")
    if values.get("legacy_pointer_consumption_used") is not False:
        raise base.Step7AcceptanceError("legacy accepted pointer consumption is forbidden")
    if values.get("network_calls_used") is not False or values.get("latest_autodetect_used") is not False:
        raise base.Step7AcceptanceError("frozen raw execution boundary mismatch")

    current = accepted_quote_history(data_root, instrument_id, start, end, repo_root=repo_root)
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
            raise base.Step7AcceptanceError("frozen raw current content-attestation mismatch: " + field)
    if int(values.get("partition_count") or -1) != len(current.accepted_dates):
        raise base.Step7AcceptanceError("frozen raw current content-attestation partition_count mismatch")
    if int(values.get("row_count") or -1) != current.row_count:
        raise base.Step7AcceptanceError("frozen raw current content-attestation row_count mismatch")
    return current


def _stat_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (int(info.st_dev), int(info.st_ino), int(info.st_size), int(info.st_mtime_ns), int(info.st_ctime_ns))


def _capture_frozen_frame(path: Path, expected_sha: str) -> tuple[pd.DataFrame, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise base.Step7AcceptanceError("frozen raw descriptor is not regular")
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
            raise base.Step7AcceptanceError("frozen raw partition changed during validated byte capture")
        if hashlib.sha256(raw).hexdigest() != expected_sha:
            raise base.Step7AcceptanceError("frozen raw physical SHA-256 mismatch")
        try:
            frame = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            raise base.Step7AcceptanceError("frozen raw parquet unreadable") from exc
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
    frozen = base._load_json(Path(manifest_path), "frozen raw manifest")
    if frozen.get("schema_version") != "step7_frozen_raw_5m_manifest.v1" or frozen.get("dataset_id") != base.SOURCE_DATASET:
        raise base.Step7AcceptanceError("frozen raw manifest schema/dataset mismatch")
    if frozen.get("instrument_id") != instrument_id or frozen.get("source_id") != base.SOURCE_ID:
        raise base.Step7AcceptanceError("frozen raw manifest identity/source mismatch")
    if frozen.get("freeze_method") != "validated_descriptor_create_only_independent_inode_exact_byte_copy":
        raise base.Step7AcceptanceError("frozen raw freeze semantics mismatch")
    if frozen.get("mutable_canonical_raw_read_after_freeze_allowed") is not False:
        raise base.Step7AcceptanceError("mutable canonical raw read after freeze is forbidden")

    records = frozen.get("partitions")
    if not isinstance(records, list) or len(records) != int(frozen.get("partition_count") or -1):
        raise base.Step7AcceptanceError("frozen raw partition records/count mismatch")
    expectation = quote_validation_expectation(
        instrument_id,
        start,
        end,
        expected_partitions=len(current.accepted_dates),
        expected_rows=current.row_count,
    )
    expected_secid = base.EXPECTED_SECID[instrument_id]
    physical_records: list[dict[str, object]] = []
    content_lines: list[str] = []
    dates: list[str] = []
    total_rows = 0
    expected_root = (run_root / "inputs" / "dataset_id=futures_raw_5m").resolve()

    for record in records:
        if not isinstance(record, Mapping):
            raise base.Step7AcceptanceError("frozen raw record must be object")
        trade_date = str(record.get("trade_date") or "")
        if trade_date in dates:
            raise base.Step7AcceptanceError("duplicate frozen trade_date")
        dates.append(trade_date)
        if record.get("independent_inode_exact_byte_copy") is not True:
            raise base.Step7AcceptanceError("frozen raw independent-copy evidence missing")
        path = base._expand_root_ref(record.get("frozen_ref"), run_root=run_root)
        try:
            path.relative_to(expected_root)
        except ValueError as exc:
            raise base.Step7AcceptanceError("frozen raw partition escaped immutable Stage 7 input root") from exc
        expected_sha = str(record.get("sha256") or "").strip().lower()
        if len(expected_sha) != 64:
            raise base.Step7AcceptanceError("frozen raw partition SHA-256 missing")
        frame, frozen_stat = _capture_frozen_frame(path, expected_sha)
        source_identity = record.get("validated_source_identity")
        if not isinstance(source_identity, Mapping):
            raise base.Step7AcceptanceError("validated source identity evidence missing")
        if (int(frozen_stat.st_dev), int(frozen_stat.st_ino)) == (int(source_identity.get("st_dev") or -1), int(source_identity.get("st_ino") or -1)):
            raise base.Step7AcceptanceError("frozen raw copy shares content-attested source inode")
        rows, secids = base.stage2._validate_quote_partition(repo_root, frame, expectation, trade_date, validation_run_id)
        if int(rows) != int(record.get("row_count") or -1):
            raise base.Step7AcceptanceError("frozen raw physical row_count mismatch")
        if set(secids) != {expected_secid} or set(record.get("secids") or []) != {expected_secid}:
            raise base.Step7AcceptanceError("frozen raw physical secid mismatch")
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
        raise base.Step7AcceptanceError("frozen raw date set no longer equals current content-attested scope")
    digest = hashlib.sha256("".join(content_lines).encode("utf-8")).hexdigest()
    if str(frozen.get("frozen_content_sha256") or "").strip().lower() != digest or digest != current.partition_content_set_sha256:
        raise base.Step7AcceptanceError("frozen raw aggregate content digest mismatch")
    if total_rows != current.row_count:
        raise base.Step7AcceptanceError("frozen raw aggregate row_count mismatch")
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
    expected_secid = base.EXPECTED_SECID[instrument_id]
    rows: list[dict[str, object]] = []
    for record in records:
        trade_date = str(record["trade_date"])
        frame = record.get("frame")
        if not isinstance(frame, pd.DataFrame):
            raise base.Step7AcceptanceError("independent D1 oracle requires captured validated frame")
        required = ("instrument_id", "trade_date", "ts", "secid", "open", "high", "low", "close", "volume", "value", "num_trades")
        missing = [field for field in required if field not in frame.columns]
        if missing or frame.empty:
            raise base.Step7AcceptanceError("oracle D1 raw input missing fields/empty")
        if set(frame["instrument_id"].astype(str)) != {instrument_id} or set(frame["trade_date"].astype(str)) != {trade_date}:
            raise base.Step7AcceptanceError("oracle D1 raw identity mismatch")
        if set(frame["secid"].astype(str)) != {expected_secid}:
            raise base.Step7AcceptanceError("oracle D1 raw SECID mismatch")
        work = frame.copy()
        work["_ts"] = pd.to_datetime(work["ts"], errors="coerce")
        if bool(work["_ts"].isna().any()):
            raise base.Step7AcceptanceError("oracle D1 invalid source ts")
        work = work.sort_values("_ts", kind="mergesort").reset_index(drop=True)
        for field in ("open", "high", "low", "close"):
            work[field] = pd.to_numeric(work[field], errors="coerce")
            if bool(work[field].isna().any()) or not np.isfinite(work[field].astype(float)).all():
                raise base.Step7AcceptanceError("oracle D1 invalid OHLC: " + field)
        rows.append({
            "instrument_id": instrument_id,
            "secid": expected_secid,
            "timeframe": "1D",
            "period_start_date": trade_date,
            "period_end_date": trade_date,
            "trade_date": trade_date,
            "availability_ts_utc": base._availability_d1(trade_date),
            "open": float(work["open"].iloc[0]),
            "high": float(work["high"].max()),
            "low": float(work["low"].min()),
            "close": float(work["close"].iloc[-1]),
            "volume": base._sum_or_null(work["volume"]),
            "value": base._sum_or_null(work["value"]),
            "num_trades": base._sum_or_null(work["num_trades"]),
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
            raise base.Step7AcceptanceError("oracle technical source invalid OHLC")
    previous_close = work["close"].shift(1)
    if len(work.index) > 1 and bool(previous_close.iloc[1:].eq(0.0).any()):
        raise base.Step7AcceptanceError("oracle technical previous close denominator is zero")
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
    out["atr_14_wilder"] = base._oracle_wilder(true_range, 14)
    out["atr_20_wilder"] = base._oracle_wilder(true_range, 20)
    for field in ("return_1obs", "gap_abs", "gap_pct", "range_abs", "true_range", "atr_14_wilder", "atr_20_wilder"):
        present = pd.to_numeric(out[field], errors="coerce").dropna().to_numpy(dtype="float64")
        if not np.isfinite(present).all():
            raise base.Step7AcceptanceError("oracle technical feature contains non-finite value: " + field)
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


def _with_hardening(callable_, *args, **kwargs):
    originals = (base._revalidate_frozen, base._oracle_d1, base._oracle_technical)
    base._revalidate_frozen = _revalidate_frozen
    base._oracle_d1 = _oracle_d1
    base._oracle_technical = _oracle_technical
    try:
        return callable_(*args, **kwargs)
    finally:
        base._revalidate_frozen, base._oracle_d1, base._oracle_technical = originals


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
        raise base.Step7AcceptanceError("current Stage 2 content-attestation marker missing/non-regular")
    return marker, base._sha_file(marker)


def validate_pilot(values: Mapping[str, object], *, run_id: str, repo_root: str | Path = ".") -> list[dict[str, object]]:
    return _with_hardening(base.validate_pilot, values, run_id=run_id, repo_root=repo_root)


def promote(*, run_id: str, repo_root: str | Path = ".") -> dict[str, object]:
    checked_run = base._safe_token(run_id, "run_id")
    stage7_lock = base._data_root() / "state" / "locks" / "step7_rub_native_d1_w1_acceptance.lock"
    stage2_lock = content_attestation._lock_path()
    with _exclusive_lock(stage7_lock):
        with _exclusive_lock(stage2_lock):
            marker_path, marker_sha = _stage2_marker_sha()
            evidence_path = base._evidence_dir(checked_run) / "pilot_evidence.json"
            evidence = base._load_json(evidence_path, "pilot_evidence")
            validated = _with_hardening(base.validate_pilot, evidence, run_id=checked_run, repo_root=repo_root)

            frozen_rows = evidence.get("frozen_inputs")
            if not isinstance(frozen_rows, list) or len(frozen_rows) != 2:
                raise base.Step7AcceptanceError("pilot must have two frozen input manifests")
            for frozen_row in frozen_rows:
                if not isinstance(frozen_row, Mapping):
                    raise base.Step7AcceptanceError("frozen input evidence must be object")
                manifest_path = Path(str(frozen_row.get("manifest_path") or ""))
                manifest = base._load_json(manifest_path, "frozen raw manifest")
                if manifest.get("content_attestation_marker_sha256") != marker_sha:
                    raise base.Step7AcceptanceError("Stage 2 content-attestation marker changed before promotion")

            records: list[tuple[Path, Mapping[str, object]]] = []
            summaries: list[dict[str, object]] = []
            for item in validated:
                pointer_path = base._pointer_path(str(item["dataset_id"]), str(item["timeframe"]), str(item["instrument_id"]))
                pointer_values = {
                    "dataset_id": item["dataset_id"],
                    "timeframe": item["timeframe"],
                    "instrument_id": item["instrument_id"],
                    "run_id": item["producer_run_id"],
                    "acceptance_run_id": checked_run,
                    "manifest_ref": base._rooted_ref(item["manifest_path"]),
                    "quality_report_ref": base._rooted_ref(item["quality_path"]),
                    "partition_ref": base._rooted_ref(item["partition"]),
                    "quality_status": "pass",
                    "acceptance_contract_id": base.CONTRACT_ID,
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
                    "physical_readback_passed": True,
                })
            if len(summaries) != 8:
                raise base.Step7AcceptanceError("accepted pointer count mismatch")

            marker = base._evidence_dir(checked_run) / "accepted_pointers.json"
            result: dict[str, object] = {
                "project": "MOEX_Bot",
                "step": 7,
                "status": "accepted",
                "run_id": checked_run,
                "acceptance_contract_id": base.CONTRACT_ID,
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
                "continuous_series_used": False,
                "si_cr_continuous_ready": False,
                "weekly_oi_ready": False,
                "advanced_technical_policy_ready": False,
                "research_ready": False,
            }
            records.append((marker, result))

            if base._sha_file(marker_path) != marker_sha:
                raise base.Step7AcceptanceError("Stage 2 content-attestation marker changed immediately before pointer writes")
            base._transactional_replace(records)
            result["acceptance_evidence_path"] = marker.as_posix()
            return result


def parse_args(argv: Sequence[str] | None = None):
    return base.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    base.load_env_file(args.env_file)
    try:
        result = promote(run_id=args.run_id, repo_root=args.repo_root)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 7, "status": "acceptance_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

import pandas as pd

from moex_data import step3_raw_acceptance as step3_acceptance
from moex_data import step3_raw_pilot_runner as step3_pilot
from moex_data import step4_basis_carry_acceptance as step4_acceptance
from moex_data import step4_basis_carry_pilot_runner as step4_pilot
from moex_data import step7_rub_native_d1_w1_materializer as step7_materializer
from moex_data import step9_rub_analysis_bundle as step9
from moex_data.futures import materialize_forts_raw_5m_instrument as forts_raw
from moex_data.futures import materialize_futoi_eod as futoi_eod
from moex_data.futures import materialize_futoi_instrument as futoi_raw
from moex_data.futures import materialize_futoi_positioning_features_d1 as futoi_features
from moex_data.futures import observed_tradestats_dates as observed_dates
from moex_data.futures import refresh_forts_raw_5m_incremental as forts_incremental


CONTRACT_ID: Final[str] = "step10_rub_daily_refresh_acceptance.v1"
STAGE5_ACCEPTANCE_CONTRACT_ID: Final[str] = "step5_futoi_positioning_acceptance.v1"
STAGE7_ACCEPTANCE_CONTRACT_ID: Final[str] = "step7_rub_native_d1_w1_technical_acceptance.v1"
FUTOI_GOVERNANCE_CONTRACT_ID: Final[str] = "usdrubf_futoi_live_acceptance_governance_v1"
FUTOI_GOVERNANCE_RELATIVE_PATH: Final[Path] = Path("contracts/intelligence/usdrubf_futoi_live_acceptance_governance_v1.json")
SCHEMA_VERSION: Final[str] = "step10_rub_daily_refresh_run.v1"
CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
MARKET_TZ: Final[str] = "Europe/Moscow"
REGISTRY_PATH: Final[str] = "configs/instruments/forts_instrument_registry.v1.yaml"
ROOT_REF_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
OBSERVED_DATE_REFERENCE_INSTRUMENT_ID: Final[str] = "si_futures_family"
STAGE5_INSTRUMENTS: Final[tuple[str, ...]] = ("si_futures_family", "cr_futures_family")
STAGE7_INSTRUMENTS: Final[dict[str, str]] = {
    "usdrubf_futures_family": "USDRUBF",
    "cnyrubf_futures_family": "CNYRUBF",
}


class Step10RefreshError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step10RefreshError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def _iso_date(value: object, field: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise Step10RefreshError(field + " must be YYYY-MM-DD") from exc
    if parsed.isoformat() != text:
        _fail(field + " must be YYYY-MM-DD")
    return text


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.is_file() or env_path.is_symlink():
        _fail("env_file must be a regular non-symlink file")
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _data_root() -> Path:
    raw = os.environ.get("MOEX_DATA_ROOT")
    if not isinstance(raw, str) or not raw.strip():
        _fail("MOEX_DATA_ROOT is required")
    if raw != raw.strip():
        _fail("MOEX_DATA_ROOT must not contain surrounding whitespace")
    root = Path(raw)
    if not root.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    if root.is_symlink() or not root.is_dir():
        _fail("MOEX_DATA_ROOT must be an existing non-symlink directory")
    return root.resolve(strict=True)


def _repo_root(value: str | Path) -> Path:
    root = Path(value).resolve(strict=True)
    required = root / "configs" / "datasets" / "step9_rub_analysis_bundle.v1.yaml"
    if not required.is_file():
        _fail("repo_root is not the canonical MOEX Bot repository checkout")
    return root


def _futoi_stage5_promotion_governance(repo: Path) -> dict[str, object]:
    path = repo / FUTOI_GOVERNANCE_RELATIVE_PATH
    if path.is_symlink() or not path.is_file():
        return {
            "contract_ref": FUTOI_GOVERNANCE_RELATIVE_PATH.as_posix(),
            "status": "MISSING_OR_INVALID",
            "all_required_gates_pass": False,
            "factual_live_authority": False,
            "promotion_allowed": False,
            "blocked_gate_ids": ["governance_contract_missing"],
        }
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "contract_ref": FUTOI_GOVERNANCE_RELATIVE_PATH.as_posix(),
            "status": "MISSING_OR_INVALID",
            "all_required_gates_pass": False,
            "factual_live_authority": False,
            "promotion_allowed": False,
            "blocked_gate_ids": ["governance_contract_invalid_json"],
        }
    if not isinstance(values, Mapping):
        _fail("FUTOI governance contract must contain a JSON object")
    if values.get("project") != "MOEX_Bot" or values.get("contract_id") != FUTOI_GOVERNANCE_CONTRACT_ID:
        _fail("FUTOI governance contract identity mismatch")
    gates = values.get("gates")
    if isinstance(gates, (str, bytes)) or not isinstance(gates, Sequence) or not gates:
        _fail("FUTOI governance gates must be a non-empty array")
    required_gate_ids: list[str] = []
    blocked_gate_ids: list[str] = []
    seen: set[str] = set()
    for raw_gate in gates:
        if not isinstance(raw_gate, Mapping):
            _fail("FUTOI governance gate must be an object")
        gate_id = str(raw_gate.get("gate_id") or "").strip()
        if not gate_id or gate_id in seen:
            _fail("FUTOI governance gate_id must be unique and non-empty")
        seen.add(gate_id)
        if raw_gate.get("required") is not True:
            _fail("FUTOI governance gates must explicitly declare required=true")
        required_gate_ids.append(gate_id)
        if raw_gate.get("status") != "PASS":
            blocked_gate_ids.append(gate_id)
    authority = values.get("authority")
    if not isinstance(authority, Mapping):
        _fail("FUTOI governance authority must be an object")
    factual_live_authority = authority.get("factual_live_authority") is True
    all_required_gates_pass = not blocked_gate_ids
    promotion_allowed = all_required_gates_pass and factual_live_authority
    return {
        "contract_ref": FUTOI_GOVERNANCE_RELATIVE_PATH.as_posix(),
        "status": str(values.get("status") or ""),
        "required_gate_ids": required_gate_ids,
        "blocked_gate_ids": blocked_gate_ids,
        "all_required_gates_pass": all_required_gates_pass,
        "factual_live_authority": factual_live_authority,
        "directional_authority": authority.get("directional_authority") is True,
        "action_authority": authority.get("action_authority") is True,
        "promotion_allowed": promotion_allowed,
    }


def _rooted_ref(root: Path, path: str | Path) -> str:
    candidate = Path(path).resolve(strict=True)
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise Step10RefreshError("artifact escaped MOEX_DATA_ROOT") from exc
    if candidate.is_symlink() or not candidate.is_file():
        _fail("artifact must be a regular non-symlink file")
    return ROOT_REF_PREFIX + relative.as_posix()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_json(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        staged = Path(handle.name)
    staged.replace(path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name("." + path.name + ".tmp")
    try:
        frame.to_parquet(staged, index=False)
        staged.replace(path)
    finally:
        staged.unlink(missing_ok=True)


def _reserve_run_root(root: Path, run_id: str) -> Path:
    run_root = root / "runs" / "step10_rub_daily_refresh" / ("run_id=" + run_id)
    try:
        run_root.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise Step10RefreshError("Stage 10 run_id is immutable and cannot be reused") from exc
    return run_root


def _pointer_specs() -> tuple[step9.PointerSpec, ...]:
    specs = step9.pointer_specs("weekly")
    if len({(x.stage, x.dataset_id, x.instrument_id, x.timeframe) for x in specs}) != len(specs):
        _fail("Stage 9 pointer specification set is not unique")
    return specs


def _pointer_path(root: Path, spec: step9.PointerSpec) -> Path:
    return step9._pointer_path(root, spec)


def _snapshot_pointers(root: Path) -> dict[Path, bytes]:
    snapshot: dict[Path, bytes] = {}
    for spec in _pointer_specs():
        path = _pointer_path(root, spec)
        if path.is_symlink() or not path.is_file():
            _fail("mandatory current accepted pointer is missing: " + spec.block_id)
        snapshot[path] = path.read_bytes()
    return snapshot


def _capture_pointer_state(root: Path, stages: set[int]) -> dict[Path, bytes]:
    captured: dict[Path, bytes] = {}
    for spec in _pointer_specs():
        if spec.stage not in stages:
            continue
        path = _pointer_path(root, spec)
        if path.is_file() and not path.is_symlink():
            captured[path] = path.read_bytes()
    return captured


def _pointer_payload_bytes(values: Mapping[str, object]) -> bytes:
    return (json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")


def _capture_written_pointer_state(records: Sequence[tuple[Path, Mapping[str, object]]]) -> dict[Path, bytes]:
    captured: dict[Path, bytes] = {}
    for path, values in records:
        expected = _pointer_payload_bytes(values)
        if path.is_file() and not path.is_symlink() and path.read_bytes() == expected:
            captured[path] = expected
    return captured


def _restore_pointer_snapshot(snapshot: Mapping[Path, bytes], expected_current: Mapping[Path, bytes]) -> None:
    errors: list[str] = []
    for path, payload in snapshot.items():
        try:
            expected = expected_current.get(path)
            if expected is None or not path.is_file() or path.read_bytes() != expected:
                continue
            if expected == payload:
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".rollback") as handle:
                handle.write(payload)
                staged = Path(handle.name)
            staged.replace(path)
        except Exception as exc:  # pragma: no cover - catastrophic filesystem boundary
            errors.append(path.as_posix() + ":" + str(exc))
    if errors:
        _fail("current-pointer rollback incomplete: " + ";".join(errors))


def _transactional_pointer_replace(records: Sequence[tuple[Path, Mapping[str, object]]]) -> None:
    paths = [path for path, _ in records]
    if len(paths) != len(set(paths)):
        _fail("Stage 10 pointer transaction contains duplicate targets")
    previous = {path: path.read_bytes() if path.exists() else None for path in paths}
    staged: list[tuple[Path, Path]] = []
    applied: list[Path] = []
    try:
        for final_path, values in records:
            final_path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile("wb", dir=final_path.parent, delete=False, suffix=".stage10") as handle:
                handle.write(_pointer_payload_bytes(values))
                staged_path = Path(handle.name)
            staged.append((staged_path, final_path))
        for staged_path, final_path in staged:
            staged_path.replace(final_path)
            applied.append(final_path)
    except Exception as exc:
        rollback_errors: list[str] = []
        for final_path in reversed(applied):
            try:
                old = previous[final_path]
                if old is None:
                    final_path.unlink(missing_ok=True)
                else:
                    with tempfile.NamedTemporaryFile("wb", dir=final_path.parent, delete=False, suffix=".rollback") as handle:
                        handle.write(old)
                        rollback = Path(handle.name)
                    rollback.replace(final_path)
            except Exception as rollback_exc:  # pragma: no cover - catastrophic filesystem boundary
                rollback_errors.append(str(rollback_exc))
        if rollback_errors:
            raise Step10RefreshError("pointer transaction failed and rollback incomplete: " + ";".join(rollback_errors)) from exc
        raise Step10RefreshError("pointer transaction failed: " + str(exc)) from exc
    finally:
        for staged_path, _ in staged:
            staged_path.unlink(missing_ok=True)


def _validated_pointer_frame(root: Path, spec: step9.PointerSpec, as_of: datetime) -> tuple[Mapping[str, Any], pd.DataFrame]:
    step9._read_pointer_block(root, spec, as_of)
    pointer_path = _pointer_path(root, spec)
    pointer = step9._load_json(pointer_path, spec.block_id + ".pointer")
    partition = step9._resolve_root_ref(pointer.get("partition_ref"), spec.block_id + ".partition_ref", root)
    return pointer, pd.read_parquet(partition)


def _spec(stage: int, dataset_id: str, instrument_id: str, timeframe: str | None = None) -> step9.PointerSpec:
    matches = [
        item
        for item in _pointer_specs()
        if item.stage == stage and item.dataset_id == dataset_id and item.instrument_id == instrument_id and item.timeframe == timeframe
    ]
    if len(matches) != 1:
        _fail("canonical pointer specification not found uniquely")
    return matches[0]


def _frame_max_date(frame: pd.DataFrame, field: str, label: str) -> str:
    if field not in frame.columns or frame.empty:
        _fail(label + " has no date field/data")
    parsed = pd.to_datetime(frame[field], errors="coerce")
    if bool(parsed.isna().any()):
        _fail(label + " contains invalid date")
    return parsed.max().date().isoformat()


def _load_stage5_base(root: Path, as_of: datetime) -> tuple[str, dict[str, pd.DataFrame]]:
    frames: dict[str, pd.DataFrame] = {}
    max_dates: set[str] = set()
    for instrument in STAGE5_INSTRUMENTS:
        spec = _spec(5, "futures_futoi_eod", instrument)
        _, frame = _validated_pointer_frame(root, spec, as_of)
        if set(frame["instrument_id"].astype(str).unique()) != {instrument}:
            _fail("Stage 5 accepted EOD instrument mismatch")
        max_dates.add(_frame_max_date(frame, "trade_date", "Stage 5 EOD " + instrument))
        frames[instrument] = frame.copy()
    if len(max_dates) != 1:
        _fail("Stage 5 accepted EOD instruments are not date-aligned")
    return next(iter(max_dates)), frames


def _load_stage7_base(root: Path, as_of: datetime) -> tuple[str, dict[str, pd.DataFrame]]:
    frames: dict[str, pd.DataFrame] = {}
    max_dates: set[str] = set()
    for instrument in STAGE7_INSTRUMENTS:
        spec = _spec(7, "rub_native_ohlcv_htf", instrument, "1D")
        _, frame = _validated_pointer_frame(root, spec, as_of)
        if set(frame["instrument_id"].astype(str).unique()) != {instrument}:
            _fail("Stage 7 accepted D1 instrument mismatch")
        max_dates.add(_frame_max_date(frame, "trade_date", "Stage 7 D1 " + instrument))
        frames[instrument] = frame.copy()
    if len(max_dates) != 1:
        _fail("Stage 7 accepted D1 instruments are not date-aligned")
    return next(iter(max_dates)), frames


def _calendar_dates(*, start_date: str, end_date: str, timeout: float) -> list[str]:
    try:
        return observed_dates.observed_dates(
            start_date,
            end_date,
            instrument_id=OBSERVED_DATE_REFERENCE_INSTRUMENT_ID,
            timeout=timeout,
        )
    except Exception as exc:
        raise Step10RefreshError("Stage 10 observed TradeStats date source failed: " + str(exc)) from exc


def _latest_source_dates(root: Path, as_of: datetime) -> tuple[str, str]:
    stage3 = _spec(3, "futures_raw_5m", "si_front_contract")
    stage4 = _spec(4, "rub_basis_carry_5m", "usd_rub_basis_carry")
    block3 = step9._read_pointer_block(root, stage3, as_of)
    block4 = step9._read_pointer_block(root, stage4, as_of)
    date3 = str(block3["selected_observation"].get("trade_date") or "")
    date4 = str(block4["selected_observation"].get("trade_date") or "")
    return _iso_date(date3, "Stage 3 current trade_date"), _iso_date(date4, "Stage 4 current trade_date")


def _freeze_file(root: Path, source: str | Path, target: Path) -> dict[str, object]:
    src = Path(source).resolve(strict=True)
    try:
        src.relative_to(root)
    except ValueError as exc:
        raise Step10RefreshError("raw input escaped MOEX_DATA_ROOT") from exc
    if src.is_symlink() or not src.is_file():
        _fail("raw input must be a regular non-symlink file")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        _fail("immutable Stage 10 frozen input already exists")
    shutil.copyfile(src, target)
    src_stat = src.stat()
    dst_stat = target.stat()
    if (src_stat.st_dev, src_stat.st_ino) == (dst_stat.st_dev, dst_stat.st_ino):
        target.unlink(missing_ok=True)
        _fail("Stage 10 frozen input unexpectedly shares source inode")
    source_sha = _sha256(src)
    frozen_sha = _sha256(target)
    if source_sha != frozen_sha:
        target.unlink(missing_ok=True)
        _fail("Stage 10 frozen input SHA mismatch")
    return {
        "canonical_ref": _rooted_ref(root, src),
        "frozen_ref": _rooted_ref(root, target),
        "sha256": frozen_sha,
        "independent_inode_exact_byte_copy": True,
    }


def _write_stage5_output(
    *,
    root: Path,
    run_root: Path,
    dataset_id: str,
    instrument_id: str,
    producer_run_id: str,
    frame: pd.DataFrame,
    source_refs: Sequence[str],
) -> dict[str, object]:
    base = run_root / "market" / "derived" / ("dataset_id=" + dataset_id) / ("instrument_id=" + instrument_id)
    partition = base / "part.parquet"
    manifest = run_root / "state" / "refresh" / ("dataset_id=" + dataset_id) / ("instrument_id=" + instrument_id) / "manifest.json"
    quality = run_root / "state" / "quality" / ("dataset_id=" + dataset_id) / ("instrument_id=" + instrument_id) / "quality_report.json"
    if frame.empty or frame.duplicated(subset=["instrument_id", "trade_date"]).any():
        _fail("Stage 5 rolling output is empty or has duplicate trade_date")
    min_date = str(frame["trade_date"].min())
    max_date = str(frame["trade_date"].max())
    quality_values = {
        "dataset_id": dataset_id,
        "instrument_id": instrument_id,
        "run_id": producer_run_id,
        "quality_status": "pass",
        "row_count": int(len(frame.index)),
        "min_trade_date": min_date,
        "max_trade_date": max_date,
        "duplicate_identity_count": 0,
    }
    manifest_values = {
        **quality_values,
        "partition_ref": _rooted_ref(root, partition) if partition.exists() else None,
        "quality_report_ref": _rooted_ref(root, quality) if quality.exists() else None,
        "source_refs": list(source_refs),
        "producer": "moex_data.step10_rub_refresh_scheduler.stage5_rolling.v1",
        "latest_autodetect_used": False,
        "historical_pit_research_ready_claimed": False,
        "build_ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    _atomic_parquet(partition, frame)
    _atomic_json(quality, quality_values)
    manifest_values["partition_ref"] = _rooted_ref(root, partition)
    manifest_values["quality_report_ref"] = _rooted_ref(root, quality)
    _atomic_json(manifest, manifest_values)
    return {
        "dataset_id": dataset_id,
        "instrument_id": instrument_id,
        "timeframe": None,
        "run_id": producer_run_id,
        "partition_path": partition,
        "manifest_path": manifest,
        "quality_path": quality,
        "row_count": int(len(frame.index)),
    }


def _stage5_refresh(
    *,
    root: Path,
    repo: Path,
    run_root: Path,
    run_id: str,
    base_frames: Mapping[str, pd.DataFrame],
    trading_dates: Sequence[str],
    timeout: float,
) -> list[dict[str, object]]:
    outputs: list[dict[str, object]] = []
    registry = repo / REGISTRY_PATH
    for instrument in STAGE5_INSTRUMENTS:
        base = base_frames[instrument].copy().sort_values("trade_date").reset_index(drop=True)
        new_rows: list[dict[str, object]] = []
        raw_refs: list[str] = []
        for trade_date in trading_dates:
            raw_run_id = _safe_token(run_id + "_" + instrument + "_" + trade_date.replace("-", "") + "_futoi_raw", "raw_run_id")
            payload = futoi_raw.materialize_futoi_partition(
                trade_date=trade_date,
                instrument_id=instrument,
                run_id=raw_run_id,
                registry_path=registry,
                timeout=timeout,
                require_enabled=True,
            )
            if payload.get("quality_status") != "pass" or int(payload.get("row_count") or 0) <= 0:
                _fail("Stage 10 FUTOI raw materialization failed for " + instrument + " " + trade_date)
            canonical_partition = Path(str(payload.get("storage_partition_path") or ""))
            frozen = _freeze_file(
                root,
                canonical_partition,
                run_root / "inputs" / "futoi" / ("instrument_id=" + instrument) / ("trade_date=" + trade_date) / "part.parquet",
            )
            raw_refs.append(str(frozen["frozen_ref"]))
            frame = pd.read_parquet(root / str(frozen["frozen_ref"])[len(ROOT_REF_PREFIX):])
            row = futoi_eod._single_eod_row(
                frame,
                instrument_id=instrument,
                trade_date=trade_date,
                frozen_ref=str(frozen["frozen_ref"]),
                canonical_source_ref=str(frozen["canonical_ref"]),
                frozen_sha256=str(frozen["sha256"]),
            )
            new_rows.append(row)
        if not new_rows:
            continue
        added = pd.DataFrame(new_rows)
        combined = pd.concat([base, added], ignore_index=True, sort=False).sort_values("trade_date").reset_index(drop=True)
        if combined.duplicated(subset=["instrument_id", "trade_date"]).any():
            _fail("Stage 5 rolling EOD append created duplicate trade_date")
        eod_run = _safe_token(run_id + "_" + instrument + "_eod", "eod_run_id")
        eod_output = _write_stage5_output(
            root=root,
            run_root=run_root,
            dataset_id="futures_futoi_eod",
            instrument_id=instrument,
            producer_run_id=eod_run,
            frame=combined,
            source_refs=raw_refs,
        )
        feature_frame = futoi_features.build_features(combined, instrument_id=instrument)
        feature_run = _safe_token(run_id + "_" + instrument + "_features", "feature_run_id")
        feature_output = _write_stage5_output(
            root=root,
            run_root=run_root,
            dataset_id="futures_futoi_positioning_features_d1",
            instrument_id=instrument,
            producer_run_id=feature_run,
            frame=feature_frame,
            source_refs=[_rooted_ref(root, eod_output["partition_path"])],
        )
        outputs.extend((eod_output, feature_output))
    if trading_dates and len(outputs) != 4:
        _fail("Stage 5 rolling refresh must produce four derived outputs")
    return outputs


def _write_step7_frozen_manifest(
    *,
    root: Path,
    run_root: Path,
    instrument_id: str,
    records: Sequence[Mapping[str, object]],
) -> Path:
    if not records:
        _fail("Stage 7 frozen manifest requires records")
    dates = [str(item["trade_date"]) for item in records]
    content = "".join(trade_date + "\t" + str(item["sha256"]) + "\n" for trade_date, item in zip(dates, records, strict=True))
    path = run_root / "inputs" / "stage7_frozen" / ("instrument_id=" + instrument_id) / "manifest.json"
    values = {
        "schema_version": "step7_frozen_raw_5m_manifest.v1",
        "dataset_id": "futures_raw_5m",
        "instrument_id": instrument_id,
        "source_id": "moex_algopack_fo_tradestats_5m",
        "requested_start_date": min(dates),
        "requested_end_date": max(dates),
        "partition_count": len(records),
        "partitions": [dict(item) for item in records],
        "frozen_content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "freeze_method": "validated_descriptor_create_only_independent_inode_exact_byte_copy",
        "mutable_canonical_raw_read_after_freeze_allowed": False,
        "producer": "moex_data.step10_rub_refresh_scheduler.stage7_freeze.v1",
        "latest_autodetect_used": False,
    }
    _atomic_json(path, values)
    return path


def _write_stage7_rolling_lineage(
    *,
    root: Path,
    run_root: Path,
    instrument_id: str,
    base_snapshot: Path,
    delta_manifest: Path,
    history_start: str,
    base_history_end: str,
    delta_start: str,
    delta_end: str,
) -> Path:
    if not base_snapshot.is_file() or base_snapshot.is_symlink():
        _fail("Stage 7 rolling lineage base snapshot missing")
    if not delta_manifest.is_file() or delta_manifest.is_symlink():
        _fail("Stage 7 rolling lineage delta manifest missing")
    base_ref = _rooted_ref(root, base_snapshot)
    delta_ref = _rooted_ref(root, delta_manifest)
    path = run_root / "inputs" / "stage7_lineage" / ("instrument_id=" + instrument_id) / "lineage.json"
    values = {
        "schema_version": "step10_stage7_rolling_lineage.v1",
        "instrument_id": instrument_id,
        "history_start": _iso_date(history_start, "history_start"),
        "base_history_end": _iso_date(base_history_end, "base_history_end"),
        "delta_start": _iso_date(delta_start, "delta_start"),
        "delta_end": _iso_date(delta_end, "delta_end"),
        "base_snapshot_ref": base_ref,
        "base_snapshot_sha256": _sha256(base_snapshot),
        "delta_manifest_ref": delta_ref,
        "delta_manifest_sha256": _sha256(delta_manifest),
        "source_refs": [base_ref, delta_ref],
        "lineage_order": ["accepted_base_frame_snapshot", "frozen_delta_manifest"],
        "exact_base_frame_snapshot_bound": True,
        "immutable_inputs_bound": True,
        "producer": "moex_data.step10_rub_refresh_scheduler.stage7_rolling_lineage.v1",
        "latest_autodetect_used": False,
    }
    _atomic_json(path, values)
    return path


def _stage7_refresh(
    *,
    root: Path,
    run_root: Path,
    run_id: str,
    base_frames: Mapping[str, pd.DataFrame],
    trading_dates: Sequence[str],
    rebuild_weekly: bool,
    weekly_boundary_end: str,
    timeout: float,
) -> list[dict[str, object]]:
    outputs: list[dict[str, object]] = []
    for instrument, secid in STAGE7_INSTRUMENTS.items():
        base = base_frames[instrument].copy().sort_values("trade_date").reset_index(drop=True)
        records: list[dict[str, object]] = []
        for trade_date in trading_dates:
            raw_run = _safe_token(run_id + "_" + instrument + "_" + trade_date.replace("-", "") + "_raw5m", "raw_run_id")
            payload = forts_raw.materialize_instrument_partition(
                trade_date=trade_date,
                instrument_id=instrument,
                secid=secid,
                artifact_version=raw_run,
                timeout=timeout,
            ).payload
            if payload.get("quality_status") != "pass" or int(payload.get("row_count") or 0) <= 0:
                _fail("Stage 10 perpetual raw 5m materialization failed for " + instrument + " " + trade_date)
            canonical = Path(str(payload.get("storage_partition_path") or ""))
            frozen = _freeze_file(
                root,
                canonical,
                run_root / "inputs" / "stage7_raw" / ("instrument_id=" + instrument) / ("trade_date=" + trade_date) / "part.parquet",
            )
            records.append({
                "trade_date": trade_date,
                "instrument_id": instrument,
                "sha256": str(frozen["sha256"]),
                "frozen_ref": str(frozen["frozen_ref"]),
                "canonical_ref": str(frozen["canonical_ref"]),
                "independent_inode_exact_byte_copy": True,
            })
        if not records:
            if not rebuild_weekly:
                continue
            full_d1 = base
            history_start = str(full_d1["trade_date"].min())
            d1_history_end = str(full_d1["trade_date"].max())
            weekly_history_end = _iso_date(weekly_boundary_end, "weekly_boundary_end")
            base_snapshot = run_root / "inputs" / "stage7_base_d1" / ("instrument_id=" + instrument) / "part.parquet"
            _atomic_parquet(base_snapshot, full_d1)
            base_snapshot_ref = _rooted_ref(root, base_snapshot)
            d1_run = _safe_token(run_id + "_" + instrument + "_d1", "d1_run_id")
            w1_run = _safe_token(run_id + "_" + instrument + "_w1", "w1_run_id")
            d1_tech = step7_materializer.build_technical_features(full_d1, source_ohlcv_run_id=d1_run)
            w1 = step7_materializer.build_w1(full_d1, history_start=history_start, history_end=weekly_history_end)
            w1_tech = step7_materializer.build_technical_features(w1, source_ohlcv_run_id=w1_run)
            d1_output = step7_materializer._write_output(
                run_root=run_root,
                dataset_id="rub_native_ohlcv_htf",
                instrument_id=instrument,
                timeframe="1D",
                producer_run_id=d1_run,
                frame=full_d1,
                source_ref=base_snapshot_ref,
                history_start=history_start,
                history_end=d1_history_end,
            )
            w1_output = step7_materializer._write_output(
                run_root=run_root,
                dataset_id="rub_native_ohlcv_htf",
                instrument_id=instrument,
                timeframe="1W",
                producer_run_id=w1_run,
                frame=w1,
                source_ref=_rooted_ref(root, d1_output["partition_path"]),
                history_start=history_start,
                history_end=weekly_history_end,
            )
            d1_tech_output = step7_materializer._write_output(
                run_root=run_root,
                dataset_id="rub_technical_features_htf",
                instrument_id=instrument,
                timeframe="1D",
                producer_run_id=_safe_token(run_id + "_" + instrument + "_d1_technical", "d1_tech_run_id"),
                frame=d1_tech,
                source_ref=_rooted_ref(root, d1_output["partition_path"]),
                history_start=history_start,
                history_end=d1_history_end,
            )
            w1_tech_output = step7_materializer._write_output(
                run_root=run_root,
                dataset_id="rub_technical_features_htf",
                instrument_id=instrument,
                timeframe="1W",
                producer_run_id=_safe_token(run_id + "_" + instrument + "_w1_technical", "w1_tech_run_id"),
                frame=w1_tech,
                source_ref=_rooted_ref(root, w1_output["partition_path"]),
                history_start=history_start,
                history_end=weekly_history_end,
            )
            for output in (d1_output, w1_output, d1_tech_output, w1_tech_output):
                outputs.append({
                    "dataset_id": output["dataset_id"],
                    "instrument_id": output["instrument_id"],
                    "timeframe": output["timeframe"],
                    "run_id": output["run_id"],
                    "partition_path": Path(str(output["partition_path"])),
                    "manifest_path": Path(str(output["manifest_path"])),
                    "quality_path": Path(str(output["quality_report_path"])),
                    "row_count": int(output["row_count"]),
                })
            continue
        frozen_manifest = _write_step7_frozen_manifest(
            root=root,
            run_root=run_root,
            instrument_id=instrument,
            records=records,
        )
        base_snapshot = run_root / "inputs" / "stage7_base_d1" / ("instrument_id=" + instrument) / "part.parquet"
        _atomic_parquet(base_snapshot, base)
        base_history_end = str(base["trade_date"].max())
        d1_new = step7_materializer.build_d1(
            data_root=root,
            frozen_manifest_path=frozen_manifest,
            instrument_id=instrument,
            history_start=str(records[0]["trade_date"]),
            history_end=str(records[-1]["trade_date"]),
        )
        full_d1 = pd.concat([base, d1_new], ignore_index=True, sort=False).sort_values("trade_date").reset_index(drop=True)
        if full_d1.duplicated(subset=["instrument_id", "trade_date"]).any():
            _fail("Stage 7 rolling D1 append created duplicate trade_date")
        history_start = str(full_d1["trade_date"].min())
        history_end = str(full_d1["trade_date"].max())
        weekly_history_end = _iso_date(weekly_boundary_end, "weekly_boundary_end") if rebuild_weekly else history_end
        lineage_manifest = _write_stage7_rolling_lineage(
            root=root,
            run_root=run_root,
            instrument_id=instrument,
            base_snapshot=base_snapshot,
            delta_manifest=frozen_manifest,
            history_start=history_start,
            base_history_end=base_history_end,
            delta_start=str(records[0]["trade_date"]),
            delta_end=str(records[-1]["trade_date"]),
        )
        w1 = step7_materializer.build_w1(full_d1, history_start=history_start, history_end=weekly_history_end)
        d1_run = _safe_token(run_id + "_" + instrument + "_d1", "d1_run_id")
        w1_run = _safe_token(run_id + "_" + instrument + "_w1", "w1_run_id")
        d1_tech = step7_materializer.build_technical_features(full_d1, source_ohlcv_run_id=d1_run)
        w1_tech = step7_materializer.build_technical_features(w1, source_ohlcv_run_id=w1_run)
        d1_output = step7_materializer._write_output(
            run_root=run_root,
            dataset_id="rub_native_ohlcv_htf",
            instrument_id=instrument,
            timeframe="1D",
            producer_run_id=d1_run,
            frame=full_d1,
            source_ref=_rooted_ref(root, lineage_manifest),
            history_start=history_start,
            history_end=history_end,
        )
        w1_output = step7_materializer._write_output(
            run_root=run_root,
            dataset_id="rub_native_ohlcv_htf",
            instrument_id=instrument,
            timeframe="1W",
            producer_run_id=w1_run,
            frame=w1,
            source_ref=_rooted_ref(root, d1_output["partition_path"]),
            history_start=history_start,
            history_end=weekly_history_end,
        )
        d1_tech_output = step7_materializer._write_output(
            run_root=run_root,
            dataset_id="rub_technical_features_htf",
            instrument_id=instrument,
            timeframe="1D",
            producer_run_id=_safe_token(run_id + "_" + instrument + "_d1_technical", "d1_tech_run_id"),
            frame=d1_tech,
            source_ref=_rooted_ref(root, d1_output["partition_path"]),
            history_start=history_start,
            history_end=history_end,
        )
        w1_tech_output = step7_materializer._write_output(
            run_root=run_root,
            dataset_id="rub_technical_features_htf",
            instrument_id=instrument,
            timeframe="1W",
            producer_run_id=_safe_token(run_id + "_" + instrument + "_w1_technical", "w1_tech_run_id"),
            frame=w1_tech,
            source_ref=_rooted_ref(root, w1_output["partition_path"]),
            history_start=history_start,
            history_end=weekly_history_end,
        )
        for output in (d1_output, w1_output, d1_tech_output, w1_tech_output):
            outputs.append({
                "dataset_id": output["dataset_id"],
                "instrument_id": output["instrument_id"],
                "timeframe": output["timeframe"],
                "run_id": output["run_id"],
                "partition_path": Path(str(output["partition_path"])),
                "manifest_path": Path(str(output["manifest_path"])),
                "quality_path": Path(str(output["quality_report_path"])),
                "row_count": int(output["row_count"]),
            })
    if trading_dates and len(outputs) != 8:
        _fail("Stage 7 rolling refresh must produce eight derived outputs")
    if not trading_dates and rebuild_weekly and len(outputs) != 8:
        _fail("Stage 7 weekly-boundary refresh must produce coherent eight-pointer output set")
    return outputs


def _pointer_from_output(root: Path, output: Mapping[str, object], acceptance_run_id: str) -> tuple[Path, dict[str, object]]:
    dataset_id = str(output["dataset_id"])
    instrument_id = str(output["instrument_id"])
    timeframe_value = output.get("timeframe")
    timeframe = None if timeframe_value in (None, "None") else str(timeframe_value)
    stage = 5 if dataset_id.startswith("futures_futoi_") else 7
    spec = _spec(stage, dataset_id, instrument_id, timeframe)
    partition = Path(output["partition_path"]).resolve(strict=True)
    manifest = Path(output["manifest_path"]).resolve(strict=True)
    quality = Path(output["quality_path"]).resolve(strict=True)
    pointer: dict[str, object] = {
        "dataset_id": dataset_id,
        "instrument_id": instrument_id,
        "run_id": str(output["run_id"]),
        "acceptance_run_id": acceptance_run_id,
        "acceptance_contract_id": STAGE5_ACCEPTANCE_CONTRACT_ID if stage == 5 else STAGE7_ACCEPTANCE_CONTRACT_ID,
        "scheduler_contract_id": CONTRACT_ID,
        "manifest_ref": _rooted_ref(root, manifest),
        "manifest_sha256": _sha256(manifest),
        "quality_report_ref": _rooted_ref(root, quality),
        "quality_report_sha256": _sha256(quality),
        "partition_ref": _rooted_ref(root, partition),
        "partition_sha256": _sha256(partition),
        "quality_status": "pass",
        "promotion_basis": "stage10_validated_rolling_refresh",
        "latest_autodetect_used": False,
        "historical_pit_research_ready_claimed": False,
    }
    if timeframe is not None:
        pointer["timeframe"] = timeframe
    return _pointer_path(root, spec), pointer


def _run_stage3_stage4(
    *,
    latest_trade_date: str,
    reference_date: str,
    run_id: str,
    env_file: str,
    timeout: float,
    after_promotion: Callable[[int], None] | None = None,
) -> dict[str, object]:
    stage3_run = _safe_token(run_id + "_stage3", "stage3_run_id")
    stage4_run = _safe_token(run_id + "_stage4", "stage4_run_id")
    pilot3 = step3_pilot.run_pilot(
        trade_date=latest_trade_date,
        as_of_date=reference_date,
        artifact_version=stage3_run,
        env_file=env_file,
        timeout=timeout,
    )
    if pilot3.get("status") != "pilot_passed":
        _fail("Stage 3 pilot did not pass")
    accepted3 = step3_acceptance.promote_step3_pilot(run_id=stage3_run)
    if accepted3.get("status") != "accepted" or int(accepted3.get("accepted_pointer_count") or 0) != 10:
        _fail("Stage 3 promotion did not accept 10 pointers")
    if after_promotion is not None:
        after_promotion(3)
    pilot4 = step4_pilot.run_pilot(
        trade_date=latest_trade_date,
        as_of_date=reference_date,
        artifact_version=stage4_run,
        env_file=env_file,
        timeout=timeout,
    )
    if pilot4.get("status") != "pilot_passed":
        _fail("Stage 4 pilot did not pass")
    accepted4 = step4_acceptance.promote(run_id=stage4_run)
    if accepted4.get("status") != "accepted" or int(accepted4.get("accepted_pointer_count") or 0) != 2:
        _fail("Stage 4 promotion did not accept two pointers")
    if after_promotion is not None:
        after_promotion(4)
    return {
        "status": "refreshed",
        "trade_date": latest_trade_date,
        "stage3_run_id": stage3_run,
        "stage4_run_id": stage4_run,
        "stage3_pointer_count": 10,
        "stage4_pointer_count": 2,
    }


def _stage9_smoke(as_of: datetime) -> dict[str, object]:
    as_of_text = as_of.astimezone(timezone.utc).isoformat()
    daily = step9.build_analysis_bundle(scope="daily", as_of=as_of_text)
    weekly = step9.build_analysis_bundle(scope="weekly", as_of=as_of_text)
    if int(daily.get("server_core", {}).get("block_count") or 0) != 20:
        _fail("Stage 9 daily post-refresh smoke block count mismatch")
    if int(weekly.get("server_core", {}).get("block_count") or 0) != 24:
        _fail("Stage 9 weekly post-refresh smoke block count mismatch")
    return {
        "status": "passed",
        "as_of": as_of_text,
        "daily_block_count": 20,
        "weekly_block_count": 24,
        "daily_bundle_status": daily["readiness"]["bundle_status"],
        "weekly_bundle_status": weekly["readiness"]["bundle_status"],
    }


def run_refresh(
    *,
    through_date: str,
    run_id: str,
    repo_root: str | Path = ".",
    env_file: str = CANONICAL_ENV_PATH,
    timeout: float = 60.0,
    now_utc: datetime | None = None,
) -> dict[str, object]:
    checked_through = _iso_date(through_date, "through_date")
    checked_run = _safe_token(run_id, "run_id")
    load_env_file(env_file)
    root = _data_root()
    repo = _repo_root(repo_root)
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        _fail("now_utc must be timezone-aware")
    now = now.astimezone(timezone.utc)
    market_today = now.astimezone(ZoneInfo(MARKET_TZ)).date()
    if date.fromisoformat(checked_through) >= market_today:
        _fail("through_date must be a completed Moscow calendar date before today")

    run_root = _reserve_run_root(root, checked_run)
    pointer_snapshot: dict[Path, bytes] | None = None
    rollback_expected: dict[Path, bytes] = {}
    started = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    try:
        pointer_snapshot = _snapshot_pointers(root)
        rollback_expected.update(pointer_snapshot)
        futoi_governance = _futoi_stage5_promotion_governance(repo)

        def capture_promoted_pointers(stage: int) -> None:
            rollback_expected.update(_capture_pointer_state(root, {stage}))

        stage5_base_date, stage5_base = _load_stage5_base(root, now)
        stage7_base_date, stage7_base = _load_stage7_base(root, now)
        if stage5_base_date != stage7_base_date:
            _fail("Stage 5/7 current accepted histories are not aligned on the same max trade_date")
        if date.fromisoformat(stage5_base_date) > date.fromisoformat(checked_through):
            _fail("through_date is older than current Stage 5/7 accepted history")

        source_date_start = min(
            date.fromisoformat(stage5_base_date) + timedelta(days=1),
            date.fromisoformat(checked_through) - timedelta(days=14),
        ).isoformat()
        trading_dates_all = _calendar_dates(start_date=source_date_start, end_date=checked_through, timeout=timeout)
        if not trading_dates_all:
            _fail("Stage 10 observed TradeStats date source produced no trade dates")
        latest_trade_date = max(trading_dates_all)
        new_dates = [value for value in trading_dates_all if value > stage5_base_date]
        weekly_boundary_completed = date.fromisoformat(checked_through).weekday() == 6

        stage5_outputs = _stage5_refresh(
            root=root,
            repo=repo,
            run_root=run_root,
            run_id=checked_run,
            base_frames=stage5_base,
            trading_dates=new_dates,
            timeout=timeout,
        )
        stage7_outputs = _stage7_refresh(
            root=root,
            run_root=run_root,
            run_id=checked_run,
            base_frames=stage7_base,
            trading_dates=new_dates,
            rebuild_weekly=weekly_boundary_completed,
            weekly_boundary_end=checked_through,
            timeout=timeout,
        )

        stage3_date, stage4_date = _latest_source_dates(root, now)
        if stage3_date != stage4_date:
            _fail("Stage 3/4 current accepted trade dates are not aligned")
        if stage3_date > latest_trade_date:
            _fail("Stage 3/4 current accepted date is ahead of scheduler latest observed TradeStats date")
        if stage3_date < latest_trade_date:
            source_refresh = _run_stage3_stage4(
                latest_trade_date=latest_trade_date,
                reference_date=market_today.isoformat(),
                run_id=checked_run,
                env_file=env_file,
                timeout=timeout,
                after_promotion=capture_promoted_pointers,
            )
        else:
            source_refresh = {"status": "no_op", "trade_date": latest_trade_date}

        pointer_records: list[tuple[Path, Mapping[str, object]]] = []
        for output in [*stage5_outputs, *stage7_outputs]:
            pointer_records.append(_pointer_from_output(root, output, checked_run))
        needs_derived_promotion = bool(new_dates) or weekly_boundary_completed
        if new_dates:
            if len(stage5_outputs) != 4 or len(stage7_outputs) != 8 or len(pointer_records) != 12:
                _fail("Stage 10 derived pointer set incomplete")
        elif weekly_boundary_completed:
            if stage5_outputs or len(stage7_outputs) != 8 or len(pointer_records) != 8:
                _fail("Stage 10 weekly-boundary coherent Stage 7 pointer set incomplete")

        if needs_derived_promotion and bool(futoi_governance["promotion_allowed"]):
            _transactional_pointer_replace(pointer_records)
            rollback_expected.update(_capture_written_pointer_state(pointer_records))
            derived_pointer_promotion = {
                "status": "promoted",
                "pointer_count": len(pointer_records),
                "futoi_factual_live_authority_required": True,
            }
        elif needs_derived_promotion:
            derived_pointer_promotion = {
                "status": "blocked_by_futoi_governance",
                "pointer_count": 0,
                "prepared_pointer_count": len(pointer_records),
                "futoi_factual_live_authority_required": True,
                "reason": "Stage 5 and Stage 7 remain one aligned canonical promotion cohort while FUTOI factual live authority is blocked",
            }
        else:
            derived_pointer_promotion = {
                "status": "no_op",
                "pointer_count": 0,
                "futoi_factual_live_authority_required": True,
            }

        smoke_as_of = datetime.now(timezone.utc)
        smoke = _stage9_smoke(smoke_as_of)
        finished = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        prepared_not_promoted = needs_derived_promotion and not bool(futoi_governance["promotion_allowed"])
        result: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "project": "MOEX_Bot",
            "stage": 10,
            "status": "succeeded",
            "acceptance_contract_id": CONTRACT_ID,
            "run_id": checked_run,
            "through_date": checked_through,
            "latest_completed_trading_date": latest_trade_date,
            "base_trade_date": stage5_base_date,
            "new_trading_dates": new_dates,
            "new_trading_date_count": len(new_dates),
            "date_source_artifact_id": forts_incremental.SOURCE_ARTIFACT_ID,
            "date_source_id": forts_incremental.OBSERVED_DATE_SOURCE_ID,
            "date_source_endpoint": forts_incremental.OBSERVED_DATE_SOURCE_ENDPOINT,
            "date_selection_rule": "observed_trade_dates_only",
            "source_refresh": source_refresh,
            "futoi_governance": futoi_governance,
            "derived_pointer_promotion": derived_pointer_promotion,
            "stage5": {
                "status": "prepared_not_promoted" if prepared_not_promoted and new_dates else ("refreshed" if new_dates else "no_op"),
                "output_count": len(stage5_outputs),
            },
            "stage7": {
                "status": "prepared_not_promoted" if prepared_not_promoted else ("refreshed" if (new_dates or weekly_boundary_completed) else "no_op"),
                "output_count": len(stage7_outputs),
            },
            "stage9_smoke": smoke,
            "deterministic_refresh_order": ["observed_market_dates", "stage5_raw_and_derived", "stage7_raw_and_derived", "stage3", "stage4", "governed_derived_pointer_promotion", "stage9_smoke"],
            "pointer_rollback_on_failure": True,
            "implicit_latest_used": False,
            "network_sources_explicitly_bounded_by_date": True,
            "historical_pit_research_ready_claimed": False,
            "started_at_utc": started,
            "finished_at_utc": finished,
        }
        manifest_path = run_root / "run_manifest.json"
        _atomic_json(manifest_path, result)
        result["run_manifest_ref"] = _rooted_ref(root, manifest_path)
        return result
    except Exception as exc:
        rollback_error: str | None = None
        if pointer_snapshot is not None:
            try:
                _restore_pointer_snapshot(pointer_snapshot, rollback_expected)
            except Exception as rollback_exc:  # pragma: no cover - catastrophic filesystem boundary
                rollback_error = str(rollback_exc)
        failure: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "project": "MOEX_Bot",
            "stage": 10,
            "status": "failed",
            "run_id": checked_run,
            "through_date": checked_through,
            "error": str(exc),
            "current_pointer_rollback_attempted": pointer_snapshot is not None,
            "current_pointer_rollback_status": "failed" if rollback_error else ("restored" if pointer_snapshot is not None else "not_needed"),
            "rollback_error": rollback_error,
            "implicit_latest_used": False,
            "started_at_utc": started,
            "finished_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
        try:
            _atomic_json(run_root / "run_manifest.json", failure)
        except Exception:
            pass
        if rollback_error:
            raise Step10RefreshError(str(exc) + "; pointer rollback failed: " + rollback_error) from exc
        raise Step10RefreshError(str(exc)) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic fail-closed Stage 10 RUB daily refresh through an explicit completed date.")
    parser.add_argument("--through-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_refresh(
            through_date=args.through_date,
            run_id=args.run_id,
            repo_root=args.repo_root,
            env_file=args.env_file,
            timeout=args.timeout,
        )
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "stage": 10, "status": "failed", "error": str(exc), "implicit_latest_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

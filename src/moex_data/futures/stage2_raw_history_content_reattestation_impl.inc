from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import os
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Final

import pandas as pd

from . import materialize_futoi_instrument as futoi_materializer
from . import stage2_raw_history_acceptance as stage2
from .contract_io import expand_contract_path, load_simple_yaml_mapping

CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
GENERATION_SCHEMA: Final[str] = "futures_raw_history_content_attested_generation.v1"
INSTRUMENT_SCHEMA: Final[str] = "futures_raw_history_content_attested_manifest.v1"
MARKER_SCHEMA: Final[str] = "futures_raw_history_content_attested_batch_marker.v1"
PRODUCER: Final[str] = "moex_data.futures.stage2_raw_history_content_reattestation.v2"
ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
EXPECTED_SCOPE: Final[tuple[tuple[str, str], ...]] = (
    (stage2.QUOTE_DATASET_ID, "usdrubf_futures_family"),
    (stage2.QUOTE_DATASET_ID, "cnyrubf_futures_family"),
    (stage2.FUTOI_DATASET_ID, "si_futures_family"),
    (stage2.FUTOI_DATASET_ID, "cr_futures_family"),
)


class ContentReattestationError(ValueError):
    pass


@dataclass(frozen=True)
class RepositoryExpectation:
    history: stage2.HistoryExpectation
    partition_dates_sha256: str
    missing_dates_sha256: str
    missing_dates_count: int


@dataclass(frozen=True)
class LegacyState:
    dataset_id: str
    instrument_id: str
    pointer_path: Path
    pointer_sha256: str
    pointer_values: Mapping[str, object]
    manifest_path: Path
    manifest_sha256: str
    manifest_values: Mapping[str, object]
    report_path: Path
    report_sha256: str
    report_values: Mapping[str, object]
    expectation: RepositoryExpectation
    accepted_dates: tuple[str, ...]
    missing_dates: tuple[str, ...]


@dataclass(frozen=True)
class PreparedInstrument:
    dataset_id: str
    instrument_id: str
    manifest_path: Path
    manifest_sha256: str
    report_path: Path
    report_sha256: str
    content_set_sha256: str
    records: tuple[Mapping[str, object], ...]
    missing_dates: tuple[str, ...]


def _fail(message: str) -> None:
    raise ContentReattestationError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def _require_sha256(value: object, field: str) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text):
        _fail(field + " must be SHA-256 hex")
    return text


def _data_root() -> Path:
    value = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not value:
        _fail("MOEX_DATA_ROOT is required")
    path = Path(value)
    if not path.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    return path.resolve()


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


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _date_range(start: str, end: str) -> tuple[str, ...]:
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    if first > last:
        _fail("invalid date range")
    return tuple((first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1))


def _date_set_sha256(values: Sequence[str]) -> str:
    payload = (("\n".join(values) + "\n") if values else "").encode("utf-8")
    return _sha_bytes(payload)


def _content_set_sha256(records: Sequence[Mapping[str, object]]) -> str:
    payload = "".join(str(row["trade_date"]) + "\t" + str(row["sha256"]) + "\n" for row in records).encode("utf-8")
    return _sha_bytes(payload)


def _rooted_ref(path: Path) -> str:
    root = _data_root().resolve(strict=True)
    try:
        relative = path.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as exc:
        raise ContentReattestationError("artifact must be inside MOEX_DATA_ROOT") from exc
    return ROOT_PREFIX + relative.as_posix()


def _expand_root_ref(value: object, field: str) -> Path:
    text = str(value or "").strip()
    if not text.startswith(ROOT_PREFIX):
        _fail(field + " must be rooted at ${MOEX_DATA_ROOT}")
    relative = text[len(ROOT_PREFIX):]
    if not relative or relative.startswith("/") or ".." in Path(relative).parts:
        _fail(field + " contains invalid rooted path")
    root = _data_root().resolve(strict=True)
    path = (root / relative).resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ContentReattestationError(field + " escaped MOEX_DATA_ROOT") from exc
    if not path.is_file() or path.is_symlink():
        _fail(field + " must resolve to regular non-symlink file")
    return path


def _load_json_bytes(path: Path, field: str) -> tuple[dict[str, object], bytes, str]:
    if not path.is_file() or path.is_symlink():
        _fail(field + " must be a regular non-symlink JSON file")
    raw = path.read_bytes()
    try:
        values = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise ContentReattestationError(field + " invalid JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field + " must be a JSON object")
    return values, raw, _sha_bytes(raw)


def _write_json_create_only(path: Path, values: Mapping[str, object]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".tmp") as handle:
        handle.write(payload)
        temporary = Path(handle.name)
    try:
        os.link(temporary, path)
    except FileExistsError as exc:
        raise ContentReattestationError("immutable artifact already exists: " + path.as_posix()) from exc
    finally:
        temporary.unlink(missing_ok=True)
    return _sha_bytes(payload)


def _current_marker_path() -> Path:
    return _data_root() / "state" / "accepted_manifests" / "raw_history_content_attestation" / "current_batch.json"


def _generation_root(run_id: str) -> Path:
    return _data_root() / "state" / "accepted_manifests" / "raw_history_content_attestation" / ("generation_id=" + _safe_token(run_id, "run_id"))


def _lock_path() -> Path:
    return _data_root() / "state" / "locks" / "raw_history_content_attestation.lock"


def _pointer_path(dataset_id: str, instrument_id: str) -> Path:
    return _data_root() / "state" / "datasets" / ("dataset_id=" + dataset_id) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"


def _repo_expectation(repo_root: Path, dataset_id: str, instrument_id: str) -> RepositoryExpectation:
    values = load_simple_yaml_mapping(repo_root, stage2.DATA_LAKE_PATH)
    section = stage2._mapping(values.get("stage2_forts_source_bindings"), "stage2_forts_source_bindings")
    if str(section.get("status")) != "raw_history_accepted":
        _fail("repository Stage 2 status must be raw_history_accepted")
    if dataset_id == stage2.QUOTE_DATASET_ID:
        source = stage2._mapping(section.get("quote_source"), "quote_source")
        if str(source.get("source_id")) != stage2.QUOTE_SOURCE_ID:
            _fail("repository quote source mismatch")
        coverage = stage2._mapping(source.get("proven_coverage"), "quote_source.proven_coverage")
        item = stage2._mapping(coverage.get(instrument_id), "quote coverage " + instrument_id)
        if str(item.get("date_set_evidence_status")) != "pass" or str(item.get("physical_quality_status")) != "pass":
            _fail("repository quote evidence is not PASS")
        expectation = stage2.HistoryExpectation(
            target_dataset_id=dataset_id,
            instrument_id=instrument_id,
            source_id=stage2.QUOTE_SOURCE_ID,
            date_start=str(item.get("first_available")),
            date_end=str(item.get("last_available")),
            expected_partitions=int(item.get("partitions")),
            expected_rows=int(item.get("rows")),
            expected_secid=str(item.get("secid")),
        )
        return RepositoryExpectation(
            history=expectation,
            partition_dates_sha256=_require_sha256(item.get("partition_dates_sha256"), "partition_dates_sha256"),
            missing_dates_sha256=_require_sha256(item.get("missing_dates_sha256"), "missing_dates_sha256"),
            missing_dates_count=int(item.get("missing_dates_count")),
        )
    if dataset_id == stage2.FUTOI_DATASET_ID:
        source = stage2._mapping(section.get("futoi_source"), "futoi_source")
        if str(source.get("source_id")) != stage2.FUTOI_SOURCE_ID:
            _fail("repository FUTOI source mismatch")
        backfills = stage2._mapping(source.get("historical_priority_backfills"), "futoi_source.historical_priority_backfills")
        item = stage2._mapping(backfills.get(instrument_id), "FUTOI backfill " + instrument_id)
        if str(item.get("date_set_evidence_status")) != "pass" or str(item.get("physical_quality_status")) != "pass" or int(item.get("bad_partitions")) != 0:
            _fail("repository FUTOI evidence is not PASS")
        binding = futoi_materializer._registry_binding(repo_root / futoi_materializer.REGISTRY_PATH, instrument_id)
        expectation = stage2.HistoryExpectation(
            target_dataset_id=dataset_id,
            instrument_id=instrument_id,
            source_id=stage2.FUTOI_SOURCE_ID,
            date_start=str(item.get("first_available")),
            date_end=str(item.get("last_available")),
            expected_partitions=int(item.get("partitions")),
            expected_rows=int(item.get("rows")),
            expected_secid=str(binding.get("secid")),
            expected_source_ticker=str(binding.get("futoi.ticker")),
            expected_missing_dates=int(item.get("skipped_empty_source_dates")),
        )
        return RepositoryExpectation(
            history=expectation,
            partition_dates_sha256=_require_sha256(item.get("partition_dates_sha256"), "partition_dates_sha256"),
            missing_dates_sha256=_require_sha256(item.get("missing_dates_sha256"), "missing_dates_sha256"),
            missing_dates_count=int(item.get("skipped_empty_source_dates")),
        )
    _fail("dataset outside controlled Stage 2 content-attestation scope")


def _canonical_partition_path(repo_root: Path, expectation: stage2.HistoryExpectation, trade_date: str) -> Path:
    contract_path = stage2.QUOTE_CONTRACT_PATH if expectation.target_dataset_id == stage2.QUOTE_DATASET_ID else stage2.FUTOI_CONTRACT_PATH
    contract = load_simple_yaml_mapping(repo_root, contract_path)
    pattern = str(contract.get("path_pattern") or "")
    if not pattern:
        _fail("raw dataset contract path_pattern missing")
    try:
        return expand_contract_path(
            pattern,
            _data_root().as_posix(),
            {
                "DATASET_ID": expectation.target_dataset_id,
                "INSTRUMENT_ID": expectation.instrument_id,
                "YYYY-MM-DD": trade_date,
                "SOURCE_ID": expectation.source_id,
            },
        )
    except Exception as exc:
        raise ContentReattestationError("cannot expand canonical partition path: " + str(exc)) from exc


def _authenticate_legacy_state(repo_root: Path, dataset_id: str, instrument_id: str) -> LegacyState:
    expected = _repo_expectation(repo_root, dataset_id, instrument_id)
    pointer_path = _pointer_path(dataset_id, instrument_id)
    pointer, _, pointer_sha = _load_json_bytes(pointer_path, "legacy accepted pointer")
    if pointer.get("dataset_id") != dataset_id or pointer.get("instrument_id") != instrument_id:
        _fail("legacy pointer identity mismatch")
    if pointer.get("quality_status") != "pass" or pointer.get("acceptance_status") != "pass" or pointer.get("promotion_basis") != "raw_history_acceptance":
        _fail("legacy pointer is not canonical raw-history PASS")
    run_id = _safe_token(pointer.get("run_id"), "legacy pointer run_id")
    manifest_path = _expand_root_ref(pointer.get("manifest_ref"), "legacy manifest_ref")
    manifest, _, manifest_sha = _load_json_bytes(manifest_path, "legacy accepted manifest")
    if manifest.get("schema_version") != "futures_raw_history_accepted_manifest.v1" or manifest.get("producer") != "moex_data.futures.stage2_raw_history_promotion.v1":
        _fail("legacy manifest schema/producer mismatch")
    if manifest.get("target_dataset_id") != dataset_id or manifest.get("instrument_id") != instrument_id or manifest.get("acceptance_run_id") != run_id:
        _fail("legacy manifest identity mismatch")
    if manifest.get("source_id") != expected.history.source_id or manifest.get("acceptance_status") != "pass":
        _fail("legacy manifest source/status mismatch")
    if manifest.get("network_access_used") is not False or manifest.get("historical_backfill_used") is not False:
        _fail("legacy manifest execution-boundary mismatch")
    for field, wanted in (
        ("requested_from", expected.history.date_start),
        ("requested_till", expected.history.date_end),
        ("partition_count", expected.history.expected_partitions),
        ("row_count", expected.history.expected_rows),
        ("partition_dates_sha256", expected.partition_dates_sha256),
        ("missing_dates_sha256", expected.missing_dates_sha256),
    ):
        if manifest.get(field) != wanted:
            _fail("legacy manifest repository expectation mismatch: " + field)
    missing_raw = manifest.get("missing_partition_dates")
    if not isinstance(missing_raw, list):
        _fail("legacy manifest missing_partition_dates must be list")
    missing_dates = tuple(str(value) for value in missing_raw)
    if tuple(sorted(missing_dates)) != missing_dates or len(set(missing_dates)) != len(missing_dates):
        _fail("legacy missing dates must be sorted unique")
    if len(missing_dates) != expected.missing_dates_count or _date_set_sha256(missing_dates) != expected.missing_dates_sha256:
        _fail("legacy missing-date evidence mismatch")
    calendar_dates = _date_range(expected.history.date_start, expected.history.date_end)
    missing_set = set(missing_dates)
    accepted_dates = tuple(value for value in calendar_dates if value not in missing_set)
    if len(accepted_dates) != expected.history.expected_partitions or _date_set_sha256(accepted_dates) != expected.partition_dates_sha256:
        _fail("legacy accepted date-set evidence mismatch")
    report_path = _expand_root_ref(manifest.get("acceptance_report_ref"), "legacy acceptance_report_ref")
    report, report_raw, report_sha = _load_json_bytes(report_path, "legacy acceptance report snapshot")
    if _require_sha256(manifest.get("acceptance_report_sha256"), "legacy acceptance_report_sha256") != report_sha:
        _fail("legacy report snapshot SHA mismatch")
    if report.get("target_dataset_id") != dataset_id or report.get("instrument_id") != instrument_id or report.get("run_id") != run_id:
        _fail("legacy report identity mismatch")
    if report.get("source_id") != expected.history.source_id or report.get("acceptance_status") != "pass":
        _fail("legacy report source/status mismatch")
    if report.get("failed_partition_dates") not in ([], ()) or report.get("hard_check_failures") not in ([], ()):
        _fail("legacy report contains failures")
    for field, wanted in (
        ("requested_from", expected.history.date_start),
        ("requested_till", expected.history.date_end),
        ("actual_partition_count", expected.history.expected_partitions),
        ("actual_row_count", expected.history.expected_rows),
        ("actual_partition_dates_sha256", expected.partition_dates_sha256),
        ("actual_missing_dates_sha256", expected.missing_dates_sha256),
    ):
        if report.get(field) != wanted:
            _fail("legacy report repository expectation mismatch: " + field)
    return LegacyState(
        dataset_id=dataset_id,
        instrument_id=instrument_id,
        pointer_path=pointer_path,
        pointer_sha256=pointer_sha,
        pointer_values=pointer,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        manifest_values=manifest,
        report_path=report_path,
        report_sha256=_sha_bytes(report_raw),
        report_values=report,
        expectation=expected,
        accepted_dates=accepted_dates,
        missing_dates=missing_dates,
    )


def _prior_state_payload(states: Sequence[LegacyState], marker_sha: str | None) -> bytes:
    lines = ["marker\t" + (marker_sha or "NONE") + "\n"]
    for state in states:
        lines.append(state.dataset_id + "\t" + state.instrument_id + "\t" + state.pointer_sha256 + "\t" + state.manifest_sha256 + "\t" + state.report_sha256 + "\n")
    return "".join(lines).encode("utf-8")


def probe_state(*, repo_root: str | Path = ".") -> dict[str, object]:
    repo = Path(repo_root).resolve()
    states = tuple(_authenticate_legacy_state(repo, dataset_id, instrument_id) for dataset_id, instrument_id in EXPECTED_SCOPE)
    marker = _current_marker_path()
    marker_sha = _sha_file(marker) if marker.is_file() and not marker.is_symlink() else None
    prior_sha = _sha_bytes(_prior_state_payload(states, marker_sha))
    return {
        "project": "MOEX_Bot",
        "status": "probed",
        "content_attestation_scope_count": 4,
        "current_marker_sha256": marker_sha,
        "prior_state_sha256": prior_sha,
        "legacy_pointer_runs": [
            {"dataset_id": state.dataset_id, "instrument_id": state.instrument_id, "run_id": state.pointer_values.get("run_id")}
            for state in states
        ],
    }


def _open_validated_snapshot(
    *,
    repo_root: Path,
    state: LegacyState,
    trade_date: str,
    snapshot_path: Path,
    validation_run_id: str,
) -> Mapping[str, object]:
    canonical = _canonical_partition_path(repo_root, state.expectation.history, trade_date)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(canonical, flags)
    except OSError as exc:
        raise ContentReattestationError("cannot open canonical partition " + trade_date + ": " + str(exc)) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            _fail("canonical partition is not regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read()
        after = os.fstat(descriptor)
        identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        if identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) or len(raw) != before.st_size:
            _fail("canonical partition changed while captured")
        try:
            frame = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            raise ContentReattestationError("captured parquet unreadable for " + trade_date + ": " + str(exc)) from exc
        if state.dataset_id == stage2.QUOTE_DATASET_ID:
            rows, secids = stage2._validate_quote_partition(repo_root, frame, state.expectation.history, trade_date, validation_run_id)
        else:
            rows, secids = stage2._validate_futoi_partition(frame, state.expectation.history, trade_date)
        sha = _sha_bytes(raw)
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(canonical, snapshot_path)
        except FileExistsError as exc:
            raise ContentReattestationError("snapshot target already exists") from exc
        linked = os.stat(snapshot_path, follow_symlinks=False)
        current = os.stat(canonical, follow_symlinks=False)
        if (linked.st_dev, linked.st_ino) != (before.st_dev, before.st_ino) or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
            snapshot_path.unlink(missing_ok=True)
            _fail("canonical partition pathname changed before snapshot binding")
        if _sha_file(snapshot_path) != sha:
            snapshot_path.unlink(missing_ok=True)
            _fail("snapshot bytes differ from validated bytes")
        return {
            "trade_date": trade_date,
            "sha256": sha,
            "row_count": int(rows),
            "secids": list(secids),
            "canonical_ref": _rooted_ref(canonical),
            "snapshot_ref": _rooted_ref(snapshot_path),
            "source_device": int(before.st_dev),
            "source_inode": int(before.st_ino),
            "source_size": int(before.st_size),
            "source_mtime_ns": int(before.st_mtime_ns),
        }
    finally:
        os.close(descriptor)


def _prepare_instrument(repo_root: Path, state: LegacyState, generation_root: Path, run_id: str) -> PreparedInstrument:
    records: list[Mapping[str, object]] = []
    for trade_date in state.accepted_dates:
        snapshot = generation_root / "raw" / ("dataset_id=" + state.dataset_id) / ("instrument_id=" + state.instrument_id) / ("trade_date=" + trade_date) / "part.parquet"
        records.append(_open_validated_snapshot(
            repo_root=repo_root,
            state=state,
            trade_date=trade_date,
            snapshot_path=snapshot,
            validation_run_id=run_id + "_" + state.instrument_id,
        ))
    for trade_date in state.missing_dates:
        if _canonical_partition_path(repo_root, state.expectation.history, trade_date).exists():
            _fail("previously missing canonical partition is now present: " + state.instrument_id + " " + trade_date)
    if len(records) != state.expectation.history.expected_partitions:
        _fail("content-attested partition count mismatch")
    total_rows = sum(int(row["row_count"]) for row in records)
    if total_rows != state.expectation.history.expected_rows:
        _fail("content-attested row count mismatch")
    content_sha = _content_set_sha256(records)
    report_path = generation_root / "reports" / ("dataset_id=" + state.dataset_id) / ("instrument_id=" + state.instrument_id) / "content_attestation_report.json"
    manifest_path = generation_root / "manifests" / ("dataset_id=" + state.dataset_id) / ("instrument_id=" + state.instrument_id) / "accepted_manifest.json"
    report = {
        "schema_version": GENERATION_SCHEMA,
        "producer": PRODUCER,
        "run_id": run_id,
        "dataset_id": state.dataset_id,
        "instrument_id": state.instrument_id,
        "source_id": state.expectation.history.source_id,
        "requested_from": state.expectation.history.date_start,
        "requested_till": state.expectation.history.date_end,
        "partition_count": len(records),
        "row_count": total_rows,
        "partition_dates_sha256": state.expectation.partition_dates_sha256,
        "missing_partition_dates": list(state.missing_dates),
        "missing_dates_sha256": state.expectation.missing_dates_sha256,
        "partition_content_records": list(records),
        "partition_content_set_sha256": content_sha,
        "legacy_pointer_ref": _rooted_ref(state.pointer_path),
        "legacy_pointer_sha256": state.pointer_sha256,
        "legacy_manifest_ref": _rooted_ref(state.manifest_path),
        "legacy_manifest_sha256": state.manifest_sha256,
        "legacy_report_ref": _rooted_ref(state.report_path),
        "legacy_report_sha256": state.report_sha256,
        "physical_validation_status": "pass",
        "network_access_used": False,
        "historical_backfill_used": False,
    }
    report_sha = _write_json_create_only(report_path, report)
    manifest = {
        "schema_version": INSTRUMENT_SCHEMA,
        "producer": PRODUCER,
        "run_id": run_id,
        "dataset_id": state.dataset_id,
        "instrument_id": state.instrument_id,
        "source_id": state.expectation.history.source_id,
        "requested_from": state.expectation.history.date_start,
        "requested_till": state.expectation.history.date_end,
        "partition_count": len(records),
        "row_count": total_rows,
        "partition_dates_sha256": state.expectation.partition_dates_sha256,
        "missing_partition_dates": list(state.missing_dates),
        "missing_dates_sha256": state.expectation.missing_dates_sha256,
        "partition_content_records": list(records),
        "partition_content_set_sha256": content_sha,
        "content_attestation_report_ref": _rooted_ref(report_path),
        "content_attestation_report_sha256": report_sha,
        "legacy_pointer_sha256": state.pointer_sha256,
        "legacy_manifest_sha256": state.manifest_sha256,
        "legacy_report_sha256": state.report_sha256,
        "acceptance_status": "pass",
        "promotion_basis": "raw_history_content_attestation",
        "network_access_used": False,
        "historical_backfill_used": False,
    }
    manifest_sha = _write_json_create_only(manifest_path, manifest)
    return PreparedInstrument(
        dataset_id=state.dataset_id,
        instrument_id=state.instrument_id,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        report_path=report_path,
        report_sha256=report_sha,
        content_set_sha256=content_sha,
        records=tuple(records),
        missing_dates=state.missing_dates,
    )


def _recheck_before_publication(repo_root: Path, states: Sequence[LegacyState], prepared: Sequence[PreparedInstrument]) -> None:
    state_by_key = {(state.dataset_id, state.instrument_id): state for state in states}
    for item in prepared:
        state = state_by_key[(item.dataset_id, item.instrument_id)]
        for record in item.records:
            trade_date = str(record["trade_date"])
            canonical = _canonical_partition_path(repo_root, state.expectation.history, trade_date)
            if not canonical.is_file() or canonical.is_symlink():
                _fail("canonical partition disappeared before publication")
            info = os.stat(canonical, follow_symlinks=False)
            if (int(info.st_dev), int(info.st_ino)) != (int(record["source_device"]), int(record["source_inode"])):
                _fail("canonical partition inode changed before publication: " + state.instrument_id + " " + trade_date)
            if _sha_file(canonical) != str(record["sha256"]):
                _fail("canonical partition bytes changed before publication: " + state.instrument_id + " " + trade_date)
        for trade_date in item.missing_dates:
            if _canonical_partition_path(repo_root, state.expectation.history, trade_date).exists():
                _fail("previously missing partition appeared before publication: " + state.instrument_id + " " + trade_date)


def _read_current_marker_sha() -> str | None:
    marker = _current_marker_path()
    if not marker.exists():
        return None
    if not marker.is_file() or marker.is_symlink():
        _fail("current content-attestation marker must be regular non-symlink file")
    return _sha_file(marker)


def _publish_marker(marker: Mapping[str, object]) -> str:
    path = _current_marker_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(marker, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".stage") as handle:
        handle.write(payload)
        staged = Path(handle.name)
    try:
        os.replace(staged, path)
    finally:
        staged.unlink(missing_ok=True)
    return _sha_bytes(payload)


def reattest(*, run_id: str, expected_prior_state_sha256: str, repo_root: str | Path = ".") -> dict[str, object]:
    checked_run = _safe_token(run_id, "run_id")
    expected_prior = _require_sha256(expected_prior_state_sha256, "expected_prior_state_sha256")
    repo = Path(repo_root).resolve()
    generation_root = _generation_root(checked_run)
    if generation_root.exists():
        _fail("generation_id already exists")
    lock_path = _lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        states = tuple(_authenticate_legacy_state(repo, dataset_id, instrument_id) for dataset_id, instrument_id in EXPECTED_SCOPE)
        marker_before_sha = _read_current_marker_sha()
        actual_prior = _sha_bytes(_prior_state_payload(states, marker_before_sha))
        if actual_prior != expected_prior:
            _fail("expected prior state SHA-256 is stale")
        prepared = tuple(_prepare_instrument(repo, state, generation_root, checked_run) for state in states)
        _recheck_before_publication(repo, states, prepared)
        if _read_current_marker_sha() != marker_before_sha:
            _fail("current marker changed during locked generation build")
        states_after = tuple(_authenticate_legacy_state(repo, dataset_id, instrument_id) for dataset_id, instrument_id in EXPECTED_SCOPE)
        if _sha_bytes(_prior_state_payload(states_after, marker_before_sha)) != actual_prior:
            _fail("legacy accepted state changed during generation build")
        entries = [
            {
                "dataset_id": item.dataset_id,
                "instrument_id": item.instrument_id,
                "manifest_ref": _rooted_ref(item.manifest_path),
                "manifest_sha256": item.manifest_sha256,
                "report_ref": _rooted_ref(item.report_path),
                "report_sha256": item.report_sha256,
                "partition_content_set_sha256": item.content_set_sha256,
            }
            for item in prepared
        ]
        marker = {
            "schema_version": MARKER_SCHEMA,
            "producer": PRODUCER,
            "status": "accepted",
            "generation_id": checked_run,
            "scope_count": 4,
            "entries": entries,
            "prior_state_sha256": actual_prior,
            "prior_marker_sha256": marker_before_sha,
            "legacy_pointers_mutated": False,
            "single_atomic_marker_switch": True,
            "generation_artifacts_create_only": True,
            "network_access_used": False,
            "historical_backfill_used": False,
        }
        marker_sha = _publish_marker(marker)
        return {
            "project": "MOEX_Bot",
            "status": "accepted",
            "generation_id": checked_run,
            "scope_count": 4,
            "prior_state_sha256": actual_prior,
            "marker_path": _current_marker_path().as_posix(),
            "marker_sha256": marker_sha,
            "legacy_pointers_mutated": False,
            "single_atomic_marker_switch": True,
            "network_access_used": False,
            "historical_backfill_used": False,
        }
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def resolve_content_attested_history(*, dataset_id: str, instrument_id: str, repo_root: str | Path = ".") -> dict[str, object]:
    dataset = _safe_token(dataset_id, "dataset_id")
    instrument = _safe_token(instrument_id, "instrument_id")
    if (dataset, instrument) not in EXPECTED_SCOPE:
        _fail("requested history is outside content-attested scope")
    marker_path = _current_marker_path()
    marker, _, marker_sha = _load_json_bytes(marker_path, "current content-attestation marker")
    if marker.get("schema_version") != MARKER_SCHEMA or marker.get("producer") != PRODUCER or marker.get("status") != "accepted":
        _fail("current content-attestation marker identity/status mismatch")
    entries = marker.get("entries")
    if not isinstance(entries, list) or len(entries) != 4:
        _fail("current marker must contain exactly four entries")
    keys = [(str(row.get("dataset_id")), str(row.get("instrument_id"))) for row in entries if isinstance(row, dict)]
    if tuple(keys) != EXPECTED_SCOPE:
        _fail("current marker exact four-instrument scope mismatch")
    entry = next(row for row in entries if row.get("dataset_id") == dataset and row.get("instrument_id") == instrument)
    manifest_path = _expand_root_ref(entry.get("manifest_ref"), "content-attested manifest_ref")
    manifest, _, manifest_sha = _load_json_bytes(manifest_path, "content-attested manifest")
    if manifest_sha != _require_sha256(entry.get("manifest_sha256"), "marker manifest_sha256"):
        _fail("content-attested manifest SHA mismatch")
    report_path = _expand_root_ref(entry.get("report_ref"), "content-attested report_ref")
    report, _, report_sha = _load_json_bytes(report_path, "content-attested report")
    if report_sha != _require_sha256(entry.get("report_sha256"), "marker report_sha256"):
        _fail("content-attested report SHA mismatch")
    if manifest.get("content_attestation_report_ref") != _rooted_ref(report_path) or manifest.get("content_attestation_report_sha256") != report_sha:
        _fail("manifest/report binding mismatch")
    expected = _repo_expectation(Path(repo_root).resolve(), dataset, instrument)
    for values, name in ((manifest, "manifest"), (report, "report")):
        if values.get("dataset_id") != dataset or values.get("instrument_id") != instrument or values.get("source_id") != expected.history.source_id:
            _fail(name + " identity/source mismatch")
        for field, wanted in (
            ("requested_from", expected.history.date_start),
            ("requested_till", expected.history.date_end),
            ("partition_count", expected.history.expected_partitions),
            ("row_count", expected.history.expected_rows),
            ("partition_dates_sha256", expected.partition_dates_sha256),
            ("missing_dates_sha256", expected.missing_dates_sha256),
        ):
            if values.get(field) != wanted:
                _fail(name + " repository expectation mismatch: " + field)
    records = manifest.get("partition_content_records")
    if not isinstance(records, list) or len(records) != expected.history.expected_partitions:
        _fail("content-attested record count mismatch")
    if _content_set_sha256(records) != _require_sha256(manifest.get("partition_content_set_sha256"), "manifest content set SHA"):
        _fail("content-attested aggregate digest mismatch")
    if manifest.get("partition_content_set_sha256") != entry.get("partition_content_set_sha256") or report.get("partition_content_set_sha256") != entry.get("partition_content_set_sha256"):
        _fail("marker/manifest/report aggregate digest mismatch")
    dates: list[str] = []
    resolved_records: list[dict[str, object]] = []
    for record in records:
        if not isinstance(record, dict):
            _fail("content-attested partition record must be object")
        trade_date = str(record.get("trade_date") or "")
        if dates and trade_date <= dates[-1]:
            _fail("content-attested trade dates must be strictly increasing")
        dates.append(trade_date)
        snapshot = _expand_root_ref(record.get("snapshot_ref"), "content-attested snapshot_ref")
        expected_sha = _require_sha256(record.get("sha256"), "content-attested partition sha256")
        if _sha_file(snapshot) != expected_sha:
            _fail("content-attested snapshot bytes mismatch: " + trade_date)
        resolved_records.append({**record, "snapshot_path": snapshot.as_posix()})
    if _date_set_sha256(dates) != expected.partition_dates_sha256:
        _fail("content-attested date-set digest mismatch")
    missing = tuple(str(value) for value in (manifest.get("missing_partition_dates") or []))
    if len(missing) != expected.missing_dates_count or _date_set_sha256(missing) != expected.missing_dates_sha256:
        _fail("content-attested missing-date evidence mismatch")
    return {
        "dataset_id": dataset,
        "instrument_id": instrument,
        "generation_id": marker.get("generation_id"),
        "marker_path": marker_path.as_posix(),
        "marker_sha256": marker_sha,
        "manifest_path": manifest_path.as_posix(),
        "manifest_sha256": manifest_sha,
        "partition_content_set_sha256": entry.get("partition_content_set_sha256"),
        "requested_from": expected.history.date_start,
        "requested_till": expected.history.date_end,
        "partition_count": expected.history.expected_partitions,
        "row_count": expected.history.expected_rows,
        "accepted_dates": tuple(dates),
        "missing_dates": missing,
        "records": tuple(resolved_records),
        "canonical_raw_read_required": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled Stage 2 exact-byte content re-attestation.")
    parser.add_argument("--mode", choices=("probe", "reattest"), required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--expected-prior-state-sha256")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_env_file(args.env_file)
    try:
        if args.mode == "probe":
            result = probe_state(repo_root=args.repo_root)
        else:
            if not args.run_id or not args.expected_prior_state_sha256:
                _fail("reattest mode requires --run-id and --expected-prior-state-sha256")
            result = reattest(
                run_id=args.run_id,
                expected_prior_state_sha256=args.expected_prior_state_sha256,
                repo_root=args.repo_root,
            )
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "status": "failed", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from . import materialize_forts_raw_5m_instrument as quote_materializer
from . import stage2_raw_history_acceptance as acceptance
from . import stage2_raw_history_acceptance_gate as acceptance_gate
from .contract_io import expand_contract_path, load_simple_yaml_mapping

PROMOTION_CONTRACT_PATH: Final[str] = (
    "contracts/datasets/futures_raw_history_accepted_manifest.v1.yaml"
)
PROMOTION_DATASET_ID: Final[str] = "futures_raw_history_accepted_manifest"
PROMOTION_SCHEMA_VERSION: Final[str] = "futures_raw_history_accepted_manifest.v1"
PROMOTION_PRODUCER: Final[str] = (
    "moex_data.futures.stage2_raw_history_promotion.v1"
)
ACCEPTANCE_PRODUCER: Final[str] = (
    "moex_data.futures.stage2_raw_history_acceptance_gate.v1"
)
_ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"


class RawHistoryPromotionError(ValueError):
    pass


@dataclass(frozen=True)
class AcceptanceReportSnapshot:
    values: dict[str, object]
    raw_bytes: bytes
    sha256: str
    device: int
    inode: int
    size: int
    mtime_ns: int


def _fail(message: str) -> None:
    raise RawHistoryPromotionError(message)


def _env_root() -> str:
    root = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not root:
        _fail("MOEX_DATA_ROOT is required")
    return root


def _require_text(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    return text


def _require_bool(value: object, field_name: str, expected: bool) -> None:
    if value is not expected:
        _fail(field_name + " must be " + str(expected).lower())


def _require_equal(left: object, right: object, field_name: str) -> None:
    if left != right:
        _fail(field_name + " mismatch")


def _require_nonnegative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail(field_name + " must be a nonnegative integer")
    return value


def _require_sha256(value: object, field_name: str) -> str:
    text = _require_text(value, field_name).lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        _fail(field_name + " must be a 64-character SHA-256 hex digest")
    return text


def _date_set_sha256(values: Sequence[str]) -> str:
    payload = (("\n".join(values) + "\n") if values else "").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _snapshot_identity(file_stat: os.stat_result) -> tuple[int, int, int, int]:
    return (
        int(file_stat.st_dev),
        int(file_stat.st_ino),
        int(file_stat.st_size),
        int(file_stat.st_mtime_ns),
    )


def _read_acceptance_report_snapshot(path: Path) -> AcceptanceReportSnapshot:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RawHistoryPromotionError(
            "acceptance report must be an existing regular non-symlink file: " + str(exc)
        ) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            _fail("acceptance report must be a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read()
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if _snapshot_identity(before) != _snapshot_identity(after):
        _fail("acceptance report changed while validated snapshot was read")
    if len(raw) != before.st_size:
        _fail("acceptance report size changed while validated snapshot was read")
    try:
        values = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RawHistoryPromotionError(
            "acceptance report is not valid UTF-8 JSON: " + str(exc)
        ) from exc
    if not isinstance(values, dict):
        _fail("acceptance report must be a JSON object")
    return AcceptanceReportSnapshot(
        values=values,
        raw_bytes=raw,
        sha256=hashlib.sha256(raw).hexdigest(),
        device=int(before.st_dev),
        inode=int(before.st_ino),
        size=int(before.st_size),
        mtime_ns=int(before.st_mtime_ns),
    )


def _verify_report_path_identity(path: Path, snapshot: AcceptanceReportSnapshot) -> None:
    try:
        current = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise RawHistoryPromotionError(
            "acceptance report pathname no longer identifies validated snapshot: " + str(exc)
        ) from exc
    if not stat.S_ISREG(current.st_mode):
        _fail("acceptance report pathname no longer identifies a regular file")
    expected = (snapshot.device, snapshot.inode, snapshot.size, snapshot.mtime_ns)
    if _snapshot_identity(current) != expected:
        _fail("acceptance report pathname no longer identifies validated snapshot")


def _promotion_contract_patterns(repo_root: Path) -> tuple[str, str]:
    values = load_simple_yaml_mapping(repo_root, PROMOTION_CONTRACT_PATH)
    if values.get("dataset_id") != PROMOTION_DATASET_ID:
        _fail("promotion contract dataset_id mismatch")
    if values.get("schema_version") != PROMOTION_SCHEMA_VERSION:
        _fail("promotion contract schema_version mismatch")
    manifest_pattern = _require_text(
        values.get("path_pattern"), "promotion contract path_pattern"
    )
    snapshot_pattern = _require_text(
        values.get("report_snapshot_path_pattern"),
        "promotion contract report_snapshot_path_pattern",
    )
    return manifest_pattern, snapshot_pattern


def _target_dataset_contract_ref(target_dataset_id: str) -> str:
    if target_dataset_id == acceptance.QUOTE_DATASET_ID:
        return acceptance.QUOTE_CONTRACT_PATH
    if target_dataset_id == acceptance.FUTOI_DATASET_ID:
        return acceptance.FUTOI_CONTRACT_PATH
    _fail("target_dataset_id is outside Stage 2 promotion scope")


def _expand_promotion_path(
    *,
    repo_root: Path,
    pattern: str,
    target_dataset_id: str,
    instrument_id: str,
    acceptance_run_id: str,
) -> Path:
    try:
        return expand_contract_path(
            pattern,
            _env_root(),
            {
                "TARGET_DATASET_ID": target_dataset_id,
                "INSTRUMENT_ID": instrument_id,
                "ACCEPTANCE_RUN_ID": acceptance_run_id,
            },
        )
    except Exception as exc:
        raise RawHistoryPromotionError(str(exc)) from exc


def accepted_manifest_path(
    *,
    repo_root: str | Path,
    target_dataset_id: str,
    instrument_id: str,
    acceptance_run_id: str,
) -> Path:
    checked_dataset = acceptance._require_token(target_dataset_id, "target_dataset_id")
    checked_instrument = acceptance._require_token(instrument_id, "instrument_id")
    checked_run = acceptance._require_token(acceptance_run_id, "acceptance_run_id")
    _target_dataset_contract_ref(checked_dataset)
    manifest_pattern, _ = _promotion_contract_patterns(Path(repo_root))
    return _expand_promotion_path(
        repo_root=Path(repo_root),
        pattern=manifest_pattern,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        acceptance_run_id=checked_run,
    )


def acceptance_report_snapshot_path(
    *,
    repo_root: str | Path,
    target_dataset_id: str,
    instrument_id: str,
    acceptance_run_id: str,
) -> Path:
    checked_dataset = acceptance._require_token(target_dataset_id, "target_dataset_id")
    checked_instrument = acceptance._require_token(instrument_id, "instrument_id")
    checked_run = acceptance._require_token(acceptance_run_id, "acceptance_run_id")
    _target_dataset_contract_ref(checked_dataset)
    _, snapshot_pattern = _promotion_contract_patterns(Path(repo_root))
    return _expand_promotion_path(
        repo_root=Path(repo_root),
        pattern=snapshot_pattern,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        acceptance_run_id=checked_run,
    )


def _acceptance_report_path(
    *,
    repo_root: Path,
    target_dataset_id: str,
    instrument_id: str,
    acceptance_run_id: str,
) -> Path:
    contract = load_simple_yaml_mapping(repo_root, acceptance.ACCEPTANCE_CONTRACT_PATH)
    if contract.get("dataset_id") != acceptance.ACCEPTANCE_DATASET_ID:
        _fail("acceptance contract identity mismatch")
    pattern = _require_text(contract.get("path_pattern"), "acceptance path_pattern")
    try:
        return expand_contract_path(
            pattern,
            _env_root(),
            {
                "TARGET_DATASET_ID": target_dataset_id,
                "INSTRUMENT_ID": instrument_id,
                "RUN_ID": acceptance_run_id,
            },
        )
    except Exception as exc:
        raise RawHistoryPromotionError(str(exc)) from exc


def _rooted_ref(path: Path) -> str:
    root = Path(_env_root())
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise RawHistoryPromotionError(
            "promotion artifact path must be rooted at MOEX_DATA_ROOT"
        ) from exc
    return _ROOT_PREFIX + relative.as_posix()


def _validate_repository_expectation(
    *,
    values: Mapping[str, object],
    expectation: acceptance.HistoryExpectation,
    repository_partition_dates_sha256: str,
    repository_missing_dates_sha256: str,
) -> None:
    _require_equal(
        values.get("target_dataset_id"),
        expectation.target_dataset_id,
        "repository target_dataset_id",
    )
    _require_equal(
        values.get("instrument_id"), expectation.instrument_id, "repository instrument_id"
    )
    _require_equal(values.get("source_id"), expectation.source_id, "repository source_id")
    _require_equal(
        values.get("requested_from"), expectation.date_start, "repository requested_from"
    )
    _require_equal(
        values.get("requested_till"), expectation.date_end, "repository requested_till"
    )
    for field_name in ("expected_partition_count", "actual_partition_count"):
        _require_equal(
            values.get(field_name), expectation.expected_partitions, "repository " + field_name
        )
    for field_name in ("expected_row_count", "actual_row_count"):
        _require_equal(
            values.get(field_name), expectation.expected_rows, "repository " + field_name
        )
    _require_equal(
        values.get("expected_partition_dates_sha256"),
        repository_partition_dates_sha256,
        "repository expected_partition_dates_sha256",
    )
    _require_equal(
        values.get("actual_partition_dates_sha256"),
        repository_partition_dates_sha256,
        "repository actual_partition_dates_sha256",
    )
    _require_equal(
        values.get("expected_missing_dates_sha256"),
        repository_missing_dates_sha256,
        "repository expected_missing_dates_sha256",
    )
    _require_equal(
        values.get("actual_missing_dates_sha256"),
        repository_missing_dates_sha256,
        "repository actual_missing_dates_sha256",
    )
    authoritative_calendar_dates = acceptance._date_range(
        expectation.date_start, expectation.date_end
    )
    authoritative_missing_count = len(authoritative_calendar_dates) - expectation.expected_partitions
    for field_name in (
        "expected_calendar_missing_partition_count",
        "actual_calendar_missing_partition_count",
    ):
        _require_equal(
            values.get(field_name), authoritative_missing_count, "repository " + field_name
        )
    if expectation.expected_missing_dates is not None:
        _require_equal(
            authoritative_missing_count,
            expectation.expected_missing_dates,
            "repository expected_missing_dates",
        )
    if expectation.expected_secid is not None:
        _require_equal(
            values.get("secid_scope"),
            [expectation.expected_secid],
            "repository secid_scope",
        )


def _validate_acceptance_report(
    *,
    values: Mapping[str, object],
    target_dataset_id: str,
    instrument_id: str,
    acceptance_run_id: str,
    report_path: Path,
    pointer_path: Path,
    expectation: acceptance.HistoryExpectation,
    repository_partition_dates_sha256: str,
    repository_missing_dates_sha256: str,
) -> None:
    _require_equal(
        values.get("dataset_id"),
        acceptance.ACCEPTANCE_DATASET_ID,
        "acceptance dataset_id",
    )
    _require_equal(
        values.get("target_dataset_id"),
        target_dataset_id,
        "acceptance target_dataset_id",
    )
    _require_equal(values.get("instrument_id"), instrument_id, "acceptance instrument_id")
    _require_equal(values.get("run_id"), acceptance_run_id, "acceptance run id")
    _require_equal(values.get("producer"), ACCEPTANCE_PRODUCER, "acceptance producer")
    _require_equal(
        values.get("acceptance_contract_ref"),
        acceptance.ACCEPTANCE_CONTRACT_PATH,
        "acceptance contract ref",
    )
    _require_equal(
        values.get("acceptance_report_reference"),
        report_path.as_posix(),
        "acceptance report reference",
    )
    _require_equal(
        values.get("accepted_pointer_path_checked"),
        pointer_path.as_posix(),
        "accepted pointer path checked",
    )
    _require_equal(values.get("acceptance_status"), "pass", "acceptance_status")
    _require_bool(values.get("evidence_written"), "evidence_written", True)
    _require_bool(values.get("accepted_pointer_written"), "accepted_pointer_written", False)
    _require_bool(
        values.get("preexisting_accepted_pointer_present"),
        "preexisting_accepted_pointer_present",
        False,
    )
    _require_bool(values.get("network_access_used"), "network_access_used", False)
    _require_bool(
        values.get("historical_backfill_used"), "historical_backfill_used", False
    )
    _require_bool(
        values.get("implicit_partition_discovery_used"),
        "implicit_partition_discovery_used",
        False,
    )
    _require_bool(values.get("latest_autodetect_used"), "latest_autodetect_used", False)

    failed_dates = values.get("failed_partition_dates")
    hard_failures = values.get("hard_check_failures")
    if not isinstance(failed_dates, list) or failed_dates:
        _fail("failed_partition_dates must be an empty list")
    if not isinstance(hard_failures, list) or hard_failures:
        _fail("hard_check_failures must be an empty list")

    for expected_field, actual_field in (
        ("expected_partition_count", "actual_partition_count"),
        ("expected_row_count", "actual_row_count"),
        ("expected_partition_dates_sha256", "actual_partition_dates_sha256"),
        ("expected_missing_dates_sha256", "actual_missing_dates_sha256"),
        (
            "expected_calendar_missing_partition_count",
            "actual_calendar_missing_partition_count",
        ),
    ):
        _require_equal(
            values.get(expected_field),
            values.get(actual_field),
            expected_field + "/" + actual_field,
        )

    for field_name in (
        "expected_partition_count",
        "actual_partition_count",
        "expected_row_count",
        "actual_row_count",
        "expected_calendar_missing_partition_count",
        "actual_calendar_missing_partition_count",
    ):
        _require_nonnegative_int(values.get(field_name), field_name)

    for field_name in (
        "expected_partition_dates_sha256",
        "actual_partition_dates_sha256",
        "expected_missing_dates_sha256",
        "actual_missing_dates_sha256",
    ):
        _require_sha256(values.get(field_name), field_name)

    _require_text(values.get("source_id"), "source_id")
    requested_from = _require_text(values.get("requested_from"), "requested_from")
    requested_till = _require_text(values.get("requested_till"), "requested_till")

    secid_scope = values.get("secid_scope")
    if not isinstance(secid_scope, list) or not secid_scope:
        _fail("secid_scope must be a non-empty list")
    if any(not str(value or "").strip() for value in secid_scope):
        _fail("secid_scope contains blank value")

    missing_dates = values.get("missing_partition_dates")
    if not isinstance(missing_dates, list):
        _fail("missing_partition_dates must be a list")
    checked_missing_dates = [
        acceptance._require_date(value, "missing_partition_dates") for value in missing_dates
    ]
    if checked_missing_dates != missing_dates:
        _fail("missing_partition_dates must use canonical ISO dates")
    if len(missing_dates) != values.get("actual_calendar_missing_partition_count"):
        _fail("missing_partition_dates count mismatch")
    if len(set(missing_dates)) != len(missing_dates):
        _fail("missing_partition_dates must not contain duplicates")
    if missing_dates != sorted(missing_dates):
        _fail("missing_partition_dates must be sorted")

    try:
        all_dates = list(acceptance._date_range(requested_from, requested_till))
    except Exception as exc:
        raise RawHistoryPromotionError("requested date range is invalid: " + str(exc)) from exc
    all_date_set = set(all_dates)
    missing_date_set = set(missing_dates)
    if not missing_date_set.issubset(all_date_set):
        _fail("missing_partition_dates contains date outside requested range")
    present_dates = [value for value in all_dates if value not in missing_date_set]
    if len(present_dates) != values.get("actual_partition_count"):
        _fail("partition count does not match requested range minus missing dates")

    observed_partition_digest = _date_set_sha256(present_dates)
    observed_missing_digest = _date_set_sha256(missing_dates)
    if observed_partition_digest != values.get("actual_partition_dates_sha256"):
        _fail("partition date digest does not match requested range and missing dates")
    if observed_missing_digest != values.get("actual_missing_dates_sha256"):
        _fail("missing date digest does not match missing_partition_dates")

    _validate_repository_expectation(
        values=values,
        expectation=expectation,
        repository_partition_dates_sha256=repository_partition_dates_sha256,
        repository_missing_dates_sha256=repository_missing_dates_sha256,
    )


def _write_bytes_create_only(
    path: Path,
    payload: bytes,
    *,
    allow_identical_existing: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        _fail("target artifact must not be a symlink: " + path.as_posix())
    if path.exists():
        if allow_identical_existing and path.is_file() and path.read_bytes() == payload:
            return
        _fail("target artifact already exists with conflicting content: " + path.as_posix())

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb", dir=path.parent, delete=False, suffix=".tmp"
        ) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)
        try:
            os.link(temporary_path, path)
        except FileExistsError as exc:
            if (
                allow_identical_existing
                and not path.is_symlink()
                and path.is_file()
                and path.read_bytes() == payload
            ):
                return
            raise RawHistoryPromotionError(
                "target artifact appeared concurrently: " + path.as_posix()
            ) from exc
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _json_bytes(values: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            values,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            separators=(",", ": "),
        )
        + "\n"
    ).encode("utf-8")


def _write_json_create_only(
    path: Path,
    values: Mapping[str, object],
    *,
    allow_identical_existing: bool,
) -> None:
    _write_bytes_create_only(
        path,
        _json_bytes(values),
        allow_identical_existing=allow_identical_existing,
    )


def promote_history(
    *,
    repo_root: str | Path,
    target_dataset_id: str,
    instrument_id: str,
    acceptance_run_id: str,
) -> dict[str, object]:
    root = Path(repo_root)
    checked_dataset = acceptance._require_token(target_dataset_id, "target_dataset_id")
    checked_instrument = acceptance._require_token(instrument_id, "instrument_id")
    checked_acceptance_run = acceptance._require_token(
        acceptance_run_id, "acceptance_run_id"
    )
    target_contract_ref = _target_dataset_contract_ref(checked_dataset)

    pointer_path = acceptance_gate._pointer_path(root, checked_dataset, checked_instrument)
    if pointer_path.exists() or pointer_path.is_symlink():
        _fail("canonical accepted pointer already exists")

    try:
        expectation = acceptance._expectation(root, checked_dataset, checked_instrument)
        repository_partition_dates_sha256, repository_missing_dates_sha256 = (
            acceptance_gate._expected_date_set_evidence(
                root, checked_dataset, checked_instrument
            )
        )
    except Exception as exc:
        raise RawHistoryPromotionError(
            "repository Stage 2 expectation validation failed: " + str(exc)
        ) from exc

    source_report_path = _acceptance_report_path(
        repo_root=root,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        acceptance_run_id=checked_acceptance_run,
    )
    report_snapshot = _read_acceptance_report_snapshot(source_report_path)
    report = report_snapshot.values
    _validate_acceptance_report(
        values=report,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        acceptance_run_id=checked_acceptance_run,
        report_path=source_report_path,
        pointer_path=pointer_path,
        expectation=expectation,
        repository_partition_dates_sha256=repository_partition_dates_sha256,
        repository_missing_dates_sha256=repository_missing_dates_sha256,
    )

    manifest_path = accepted_manifest_path(
        repo_root=root,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        acceptance_run_id=checked_acceptance_run,
    )
    evidence_snapshot_path = acceptance_report_snapshot_path(
        repo_root=root,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        acceptance_run_id=checked_acceptance_run,
    )
    manifest_ref = _rooted_ref(manifest_path)
    source_report_ref = _rooted_ref(source_report_path)
    evidence_snapshot_ref = _rooted_ref(evidence_snapshot_path)

    manifest_values: dict[str, object] = {
        "schema_version": PROMOTION_SCHEMA_VERSION,
        "producer": PROMOTION_PRODUCER,
        "dataset_id": PROMOTION_DATASET_ID,
        "target_dataset_id": checked_dataset,
        "target_dataset_contract_ref": target_contract_ref,
        "instrument_id": checked_instrument,
        "acceptance_run_id": checked_acceptance_run,
        "acceptance_contract_ref": acceptance.ACCEPTANCE_CONTRACT_PATH,
        "source_acceptance_report_ref": source_report_ref,
        "acceptance_report_ref": evidence_snapshot_ref,
        "acceptance_report_sha256": report_snapshot.sha256,
        "source_id": report["source_id"],
        "secid_scope": report["secid_scope"],
        "requested_from": report["requested_from"],
        "requested_till": report["requested_till"],
        "partition_count": report["actual_partition_count"],
        "row_count": report["actual_row_count"],
        "partition_dates_sha256": report["actual_partition_dates_sha256"],
        "missing_partition_dates": report["missing_partition_dates"],
        "missing_dates_sha256": report["actual_missing_dates_sha256"],
        "calendar_missing_partition_count": report[
            "actual_calendar_missing_partition_count"
        ],
        "acceptance_status": "pass",
        "network_access_used": False,
        "historical_backfill_used": False,
    }

    _verify_report_path_identity(source_report_path, report_snapshot)
    if pointer_path.exists() or pointer_path.is_symlink():
        _fail("canonical accepted pointer appeared during promotion")

    _write_bytes_create_only(
        evidence_snapshot_path,
        report_snapshot.raw_bytes,
        allow_identical_existing=True,
    )
    _write_json_create_only(
        manifest_path,
        manifest_values,
        allow_identical_existing=True,
    )

    if pointer_path.exists() or pointer_path.is_symlink():
        _fail("canonical accepted pointer appeared during promotion")

    pointer_values: dict[str, object] = {
        "dataset_id": checked_dataset,
        "instrument_id": checked_instrument,
        "run_id": checked_acceptance_run,
        "manifest_ref": manifest_ref,
        "quality_report_ref": evidence_snapshot_ref,
        "acceptance_report_ref": evidence_snapshot_ref,
        "source_acceptance_report_ref": source_report_ref,
        "quality_status": "pass",
        "acceptance_status": "pass",
        "promotion_basis": "raw_history_acceptance",
    }
    _write_json_create_only(
        pointer_path,
        pointer_values,
        allow_identical_existing=False,
    )

    return {
        "status": "promoted",
        "dataset_id": checked_dataset,
        "instrument_id": checked_instrument,
        "acceptance_run_id": checked_acceptance_run,
        "source_acceptance_report_ref": source_report_ref,
        "acceptance_report_ref": evidence_snapshot_ref,
        "acceptance_report_sha256": report_snapshot.sha256,
        "accepted_manifest_ref": manifest_ref,
        "accepted_pointer_path": pointer_path.as_posix(),
        "accepted_pointer_written": True,
        "network_access_used": False,
        "historical_backfill_used": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Promote an explicit PASS Stage 2 raw history acceptance report."
    )
    parser.add_argument(
        "--target-dataset-id",
        required=True,
        choices=(acceptance.QUOTE_DATASET_ID, acceptance.FUTOI_DATASET_ID),
    )
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--acceptance-run-id", required=True)
    parser.add_argument("--repo-root", default=Path.cwd().as_posix())
    parser.add_argument("--env-file", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        quote_materializer.load_env_file(args.env_file)
        payload = promote_history(
            repo_root=args.repo_root,
            target_dataset_id=args.target_dataset_id,
            instrument_id=args.instrument_id,
            acceptance_run_id=args.acceptance_run_id,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": str(exc),
                    "accepted_pointer_written": False,
                    "network_access_used": False,
                    "historical_backfill_used": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

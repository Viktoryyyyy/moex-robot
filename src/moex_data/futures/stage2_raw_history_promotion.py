from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

from . import materialize_forts_raw_5m_instrument as quote_materializer
from . import stage2_raw_history_acceptance as acceptance
from . import stage2_raw_history_acceptance_gate as acceptance_gate
from .contract_io import expand_contract_path, load_simple_yaml_mapping

PROMOTION_CONTRACT_PATH: Final[str] = (
    "contracts/datasets/futures_raw_history_accepted_manifest.v1.yaml"
)
PROMOTION_SCHEMA_VERSION: Final[str] = "futures_raw_history_accepted_manifest.v1"
PROMOTION_PRODUCER: Final[str] = (
    "moex_data.futures.stage2_raw_history_promotion.v1"
)
_ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"


class RawHistoryPromotionError(ValueError):
    pass


def _fail(message: str) -> None:
    raise RawHistoryPromotionError(message)


def _env_root() -> str:
    root = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not root:
        _fail("MOEX_DATA_ROOT is required")
    return root


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        _fail(field_name + " must be a mapping")
    return value


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


def _read_json_object(path: Path, field_name: str) -> dict[str, object]:
    if not path.exists() or not path.is_file():
        _fail(field_name + " does not exist")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RawHistoryPromotionError(field_name + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field_name + " must be a JSON object")
    return values


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _contract_path_pattern(repo_root: Path) -> str:
    values = load_simple_yaml_mapping(repo_root, PROMOTION_CONTRACT_PATH)
    if values.get("schema_version") != PROMOTION_SCHEMA_VERSION:
        _fail("promotion contract schema_version mismatch")
    pattern = _require_text(values.get("path_pattern"), "promotion contract path_pattern")
    return pattern


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
    if checked_dataset not in (acceptance.QUOTE_DATASET_ID, acceptance.FUTOI_DATASET_ID):
        _fail("target_dataset_id is outside Stage 2 promotion scope")
    pattern = _contract_path_pattern(Path(repo_root))
    try:
        return expand_contract_path(
            pattern,
            _env_root(),
            {
                "TARGET_DATASET_ID": checked_dataset,
                "INSTRUMENT_ID": checked_instrument,
                "ACCEPTANCE_RUN_ID": checked_run,
            },
        )
    except Exception as exc:
        raise RawHistoryPromotionError(str(exc)) from exc


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


def _validate_acceptance_report(
    *,
    values: Mapping[str, object],
    target_dataset_id: str,
    instrument_id: str,
    acceptance_run_id: str,
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
    _require_equal(values.get("run_id"), acceptance_run_id, "acceptance run_id")
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

    _require_text(values.get("source_id"), "source_id")
    _require_text(values.get("requested_from"), "requested_from")
    _require_text(values.get("requested_till"), "requested_till")
    _require_text(values.get("actual_partition_dates_sha256"), "partition date digest")
    _require_text(values.get("actual_missing_dates_sha256"), "missing date digest")
    secid_scope = values.get("secid_scope")
    if not isinstance(secid_scope, list) or not secid_scope:
        _fail("secid_scope must be a non-empty list")
    if any(not str(value or "").strip() for value in secid_scope):
        _fail("secid_scope contains blank value")


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
    payload = _json_bytes(values)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        if (
            allow_identical_existing
            and path.is_file()
            and path.read_bytes() == payload
        ):
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

    pointer_path = acceptance_gate._pointer_path(
        root, checked_dataset, checked_instrument
    )
    if pointer_path.exists():
        _fail("canonical accepted pointer already exists")

    report_path = _acceptance_report_path(
        repo_root=root,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        acceptance_run_id=checked_acceptance_run,
    )
    report = _read_json_object(report_path, "acceptance report")
    _validate_acceptance_report(
        values=report,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        acceptance_run_id=checked_acceptance_run,
    )

    report_ref = _rooted_ref(report_path)
    report_sha256 = _sha256_file(report_path)
    manifest_path = accepted_manifest_path(
        repo_root=root,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        acceptance_run_id=checked_acceptance_run,
    )
    manifest_ref = _rooted_ref(manifest_path)

    manifest_values: dict[str, object] = {
        "schema_version": PROMOTION_SCHEMA_VERSION,
        "producer": PROMOTION_PRODUCER,
        "dataset_id": checked_dataset,
        "instrument_id": checked_instrument,
        "acceptance_run_id": checked_acceptance_run,
        "acceptance_report_ref": report_ref,
        "acceptance_report_sha256": report_sha256,
        "source_id": report["source_id"],
        "secid_scope": report["secid_scope"],
        "requested_from": report["requested_from"],
        "requested_till": report["requested_till"],
        "partition_count": report["actual_partition_count"],
        "row_count": report["actual_row_count"],
        "partition_dates_sha256": report["actual_partition_dates_sha256"],
        "missing_dates_sha256": report["actual_missing_dates_sha256"],
        "calendar_missing_partition_count": report[
            "actual_calendar_missing_partition_count"
        ],
        "acceptance_status": "pass",
        "network_access_used": False,
        "historical_backfill_used": False,
    }
    _write_json_create_only(
        manifest_path,
        manifest_values,
        allow_identical_existing=True,
    )

    if pointer_path.exists():
        _fail("canonical accepted pointer appeared during promotion")

    pointer_values: dict[str, object] = {
        "dataset_id": checked_dataset,
        "instrument_id": checked_instrument,
        "run_id": checked_acceptance_run,
        "manifest_ref": manifest_ref,
        "quality_report_ref": report_ref,
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
        "acceptance_report_ref": report_ref,
        "acceptance_report_sha256": report_sha256,
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

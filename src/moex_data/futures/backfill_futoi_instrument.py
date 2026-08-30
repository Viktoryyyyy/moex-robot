from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Final, Sequence

from . import materialize_futoi_instrument as materializer

DATASET_ID: Final[str] = materializer.DATASET_ID
SOURCE_ID: Final[str] = materializer.SOURCE_ID
REGISTRY_PATH: Final[str] = materializer.REGISTRY_PATH
DATA_LAKE_PATH: Final[str] = "configs/datasets/futures_data_lake.v1.yaml"
PRODUCER_ID: Final[str] = "moex_data.futures.backfill_futoi_instrument.v1"


class FutoiBackfillError(ValueError):
    pass


def _require_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise FutoiBackfillError(field_name + " must be YYYY-MM-DD") from exc


def _require_token(value: str, field_name: str) -> str:
    text = str(value or "").strip()
    if not text or any(marker in text for marker in ("/", "\\", "*", "{", "}", "`", "$(")):
        raise FutoiBackfillError(field_name + " must be an explicit safe token")
    return text


def _date_range(start: date, end: date) -> list[date]:
    result: list[date] = []
    current = start
    while current <= end:
        result.append(current)
        current += timedelta(days=1)
    return result


def _section(text: str, header: str) -> str:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.strip() != header:
            continue
        base_indent = len(line) - len(line.lstrip(" "))
        collected = [line]
        for candidate in lines[index + 1 :]:
            if candidate.strip() and len(candidate) - len(candidate.lstrip(" ")) <= base_indent:
                break
            collected.append(candidate)
        return "\n".join(collected)
    raise FutoiBackfillError("required config section missing: " + header)


def _stage2_backfill_ready(data_lake_path: str | Path = DATA_LAKE_PATH) -> bool:
    text = Path(data_lake_path).read_text(encoding="utf-8")
    section = _section(text, "stage2_forts_source_bindings:")
    return (
        "status: all_pilots_passed_backfill_ready" in section
        and "backfill_ready: true" in section
        and "accepted_pointer_ready: false" in section
        and "scheduler_ready: false" in section
        and "research_ready: false" in section
    )


def _empty_source_error(message: str) -> bool:
    lowered = message.lower()
    return any(
        marker in lowered
        for marker in (
            "returned no rows",
            "contains no rows for explicit trade_date",
            "contains no rows",
            "source contains no rows",
        )
    )


def _subrun_id(run_id: str, trade_date: str) -> str:
    return run_id + "_partition_" + trade_date.replace("-", "")


def _aggregate_quality_path(date_end: str, run_id: str) -> Path:
    return materializer._quality_path(date_end, run_id)


def _aggregate_manifest_path(date_end: str, run_id: str) -> Path:
    return materializer._manifest_path(date_end, run_id)


def _stable_partition_evidence(path: Path, *, trade_date: str, subrun_id: str, row_count: int) -> dict[str, object]:
    if not path.is_absolute():
        raise FutoiBackfillError("materialized FUTOI partition path must be absolute")
    if path.is_symlink():
        raise FutoiBackfillError("materialized FUTOI partition must not be a symlink")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise FutoiBackfillError("cannot snapshot materialized FUTOI partition: " + str(exc)) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise FutoiBackfillError("materialized FUTOI partition must be a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read()
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    identity_before = (int(before.st_dev), int(before.st_ino), int(before.st_size), int(before.st_mtime_ns))
    identity_after = (int(after.st_dev), int(after.st_ino), int(after.st_size), int(after.st_mtime_ns))
    if identity_before != identity_after or len(raw) != int(before.st_size):
        raise FutoiBackfillError("materialized FUTOI partition changed while run evidence was captured")
    return {
        "trade_date": trade_date,
        "subrun_id": subrun_id,
        "partition_path": path.as_posix(),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "row_count": int(row_count),
    }


def _accepted_ref(instrument_id: str) -> str:
    return (
        "${MOEX_DATA_ROOT}/state/datasets/dataset_id="
        + DATASET_ID
        + "/instrument_id="
        + instrument_id
        + "/current_accepted_manifest.json"
    )


def _write_pointer(instrument_id: str, run_id: str, manifest_path: Path, quality_path: Path) -> Path:
    pointer_path = materializer.accepted_pointer_path(instrument_id)
    materializer._write_json_atomic(
        pointer_path,
        {
            "dataset_id": DATASET_ID,
            "instrument_id": instrument_id,
            "run_id": run_id,
            "manifest_ref": manifest_path.as_posix(),
            "quality_report_ref": quality_path.as_posix(),
            "quality_status": "pass",
            "refresh_status": "succeeded",
        },
    )
    return pointer_path


def _authorize(binding: dict[str, object], data_lake_path: str | Path) -> None:
    if not _stage2_backfill_ready(data_lake_path):
        raise FutoiBackfillError("Stage 2 controlled backfill readiness is not enabled")
    if str(binding.get("evidence_status")) != "pilot_passed":
        raise FutoiBackfillError("registry instrument pilot evidence is not passed")
    if binding.get("futoi.enabled_for_materialization") is not False:
        raise FutoiBackfillError("global FUTOI materialization flag must remain false for controlled Stage 2 backfill")
    if str(binding.get("futoi.source_id")) != SOURCE_ID:
        raise FutoiBackfillError("registry FUTOI source_id does not match canonical source")
    if str(binding.get("futoi.availability_status")) != "available" or str(binding.get("futoi.probe_status")) != "completed":
        raise FutoiBackfillError("registry FUTOI APIM evidence is not available/completed")


def _emit_progress(
    *,
    instrument_id: str,
    secid: str,
    futoi_ticker: str,
    processed_dates: int,
    total_dates: int,
    trade_date: str | None,
    partition_count: int,
    skipped_count: int,
    failure_count: int,
    event: str = "progress",
) -> None:
    print(
        json.dumps(
            {
                "event": event,
                "instrument_id": instrument_id,
                "secid": secid,
                "futoi_ticker": futoi_ticker,
                "processed_dates": processed_dates,
                "total_dates": total_dates,
                "trade_date": trade_date,
                "partition_count": partition_count,
                "skipped_empty_source_dates": skipped_count,
                "failure_count": failure_count,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        file=sys.stderr,
        flush=True,
    )


def backfill_range(
    *,
    date_start: str,
    date_end: str,
    instrument_id: str,
    run_id: str,
    registry_path: str | Path = REGISTRY_PATH,
    data_lake_path: str | Path = DATA_LAKE_PATH,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
    create_accepted_pointer: bool = False,
    progress_every: int = 0,
) -> dict[str, object]:
    start = _require_date(date_start, "date_start")
    end = _require_date(date_end, "date_end")
    if start > end:
        raise FutoiBackfillError("date_start must be <= date_end")
    if progress_every < 0:
        raise FutoiBackfillError("progress_every must be >= 0")
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_run_id = _require_token(run_id, "run_id")
    binding = materializer._registry_binding(registry_path, checked_instrument)
    _authorize(binding, data_lake_path)

    successes: list[dict[str, object]] = []
    partition_evidence: list[dict[str, object]] = []
    skipped: list[str] = []
    failures: list[dict[str, str]] = []
    duplicate_total = 0
    null_total = 0
    invalid_total = 0
    dates = _date_range(start, end)

    if progress_every:
        _emit_progress(
            instrument_id=checked_instrument,
            secid=str(binding["secid"]),
            futoi_ticker=str(binding["futoi.ticker"]),
            processed_dates=0,
            total_dates=len(dates),
            trade_date=None,
            partition_count=0,
            skipped_count=0,
            failure_count=0,
            event="start",
        )

    for processed_dates, current in enumerate(dates, start=1):
        trade_date = current.isoformat()
        subrun_id = _subrun_id(checked_run_id, trade_date)
        try:
            payload = materializer.materialize_futoi_partition(
                trade_date=trade_date,
                instrument_id=checked_instrument,
                run_id=subrun_id,
                registry_path=registry_path,
                timeout=timeout,
                apim_base_url=apim_base_url,
                require_enabled=False,
            )
            evidence = _stable_partition_evidence(
                Path(str(payload["storage_partition_path"])),
                trade_date=trade_date,
                subrun_id=subrun_id,
                row_count=int(payload.get("row_count") or 0),
            )
            successes.append(payload)
            partition_evidence.append(evidence)
            quality = json.loads(Path(str(payload["quality_report_reference"])).read_text(encoding="utf-8"))
            duplicate_total += int(quality.get("duplicate_key_count") or 0)
            null_total += int(quality.get("null_required_count") or 0)
            invalid_total += int(quality.get("invalid_position_count") or 0)
        except Exception as exc:
            message = str(exc)
            if _empty_source_error(message):
                skipped.append(trade_date)
            else:
                failures.append({"trade_date": trade_date, "error": message})

        if progress_every and (processed_dates % progress_every == 0 or processed_dates == len(dates)):
            _emit_progress(
                instrument_id=checked_instrument,
                secid=str(binding["secid"]),
                futoi_ticker=str(binding["futoi.ticker"]),
                processed_dates=processed_dates,
                total_dates=len(dates),
                trade_date=trade_date,
                partition_count=len(successes),
                skipped_count=len(skipped),
                failure_count=len(failures),
            )

    row_count = sum(int(item.get("row_count") or 0) for item in successes)
    quality_status = "pass" if successes and not failures and duplicate_total == 0 and null_total == 0 and invalid_total == 0 else "fail"
    refresh_status = "succeeded" if quality_status == "pass" else ("partial" if successes else "failed")
    quality_path = _aggregate_quality_path(end.isoformat(), checked_run_id)
    manifest_path = _aggregate_manifest_path(end.isoformat(), checked_run_id)
    quality_values = {
        "run_id": checked_run_id,
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "source_id": SOURCE_ID,
        "secid": str(binding["secid"]),
        "futoi_ticker": str(binding["futoi.ticker"]),
        "trade_date": end.isoformat(),
        "requested_from": start.isoformat(),
        "requested_till": end.isoformat(),
        "quality_status": quality_status,
        "row_count": row_count,
        "duplicate_key_count": duplicate_total,
        "null_required_count": null_total,
        "invalid_position_count": invalid_total,
        "availability_status": str(binding["futoi.availability_status"]),
        "probe_status": str(binding["futoi.probe_status"]),
        "partition_count": len(successes),
        "skipped_empty_source_dates": skipped,
        "failed_dates": failures,
    }
    manifest_values = {
        "run_id": checked_run_id,
        "run_date": end.isoformat(),
        "dataset_id": DATASET_ID,
        "instrument_scope": [checked_instrument],
        "source_scope": [SOURCE_ID],
        "requested_from": start.isoformat(),
        "requested_till": end.isoformat(),
        "partitions_written": [str(item["storage_partition_path"]) for item in successes],
        "partition_evidence": partition_evidence,
        "partitions_skipped": skipped,
        "quality_report_ref": quality_path.as_posix(),
        "accepted_manifest_ref": _accepted_ref(checked_instrument),
        "refresh_status": refresh_status,
        "source_contract": {
            "source_id": SOURCE_ID,
            "source_contract_ref": materializer.SOURCE_CONTRACT_REF,
            "futoi_ticker": str(binding["futoi.ticker"]),
            "transport": "authenticated_apim",
        },
        "failed_dates": failures,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "producer": PRODUCER_ID,
        "stage2_controlled_backfill": True,
    }
    materializer._write_json_atomic(quality_path, quality_values)
    materializer._write_json_atomic(manifest_path, manifest_values)

    pointer_reference: str | None = None
    if create_accepted_pointer:
        if quality_status != "pass" or refresh_status != "succeeded":
            raise FutoiBackfillError("accepted pointer cannot be created unless full backfill passes")
        pointer_reference = _write_pointer(checked_instrument, checked_run_id, manifest_path, quality_path).as_posix()

    return {
        "status": refresh_status,
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "source_id": SOURCE_ID,
        "requested_from": start.isoformat(),
        "requested_till": end.isoformat(),
        "quality_status": quality_status,
        "row_count": row_count,
        "partition_count": len(successes),
        "skipped_empty_source_dates": skipped,
        "failed_dates": failures,
        "quality_report_reference": quality_path.as_posix(),
        "manifest_reference": manifest_path.as_posix(),
        "accepted_manifest_pointer_reference": pointer_reference,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "stage2_controlled_backfill": True,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled Stage 2 backfill for canonical FUTOI supplementary data after pilot acceptance.")
    parser.add_argument("--date-start", required=True)
    parser.add_argument("--date-end", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--registry-path", default=REGISTRY_PATH)
    parser.add_argument("--data-lake-path", default=DATA_LAKE_PATH)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    parser.add_argument("--progress-every", type=int, default=25)
    parser.add_argument("--create-accepted-pointer", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        materializer.load_env_file(args.env_file)
        payload = backfill_range(
            date_start=args.date_start,
            date_end=args.date_end,
            instrument_id=args.instrument_id,
            run_id=args.run_id,
            registry_path=args.registry_path,
            data_lake_path=args.data_lake_path,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
            create_accepted_pointer=args.create_accepted_pointer,
            progress_every=args.progress_every,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "dataset_id": DATASET_ID, "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
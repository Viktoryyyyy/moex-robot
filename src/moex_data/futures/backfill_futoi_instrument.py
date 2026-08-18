from __future__ import annotations

import argparse
import json
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
) -> dict[str, object]:
    start = _require_date(date_start, "date_start")
    end = _require_date(date_end, "date_end")
    if start > end:
        raise FutoiBackfillError("date_start must be <= date_end")
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_run_id = _require_token(run_id, "run_id")
    binding = materializer._registry_binding(registry_path, checked_instrument)
    _authorize(binding, data_lake_path)

    successes: list[dict[str, object]] = []
    skipped: list[str] = []
    failures: list[dict[str, str]] = []
    duplicate_total = 0
    null_total = 0
    invalid_total = 0

    for current in _date_range(start, end):
        trade_date = current.isoformat()
        try:
            payload = materializer.materialize_futoi_partition(
                trade_date=trade_date,
                instrument_id=checked_instrument,
                run_id=_subrun_id(checked_run_id, trade_date),
                registry_path=registry_path,
                timeout=timeout,
                apim_base_url=apim_base_url,
                require_enabled=False,
            )
            successes.append(payload)
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
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "dataset_id": DATASET_ID, "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Final, Sequence

from . import materialize_futoi_instrument as materializer
from . import observed_tradestats_dates as trade_dates

DATASET_ID: Final[str] = materializer.DATASET_ID
SOURCE_ID: Final[str] = materializer.SOURCE_ID
REGISTRY_PATH: Final[str] = materializer.REGISTRY_PATH
DATA_LAKE_PATH: Final[str] = "configs/datasets/futures_data_lake.v1.yaml"
PRODUCER_ID: Final[str] = "moex_data.futures.backfill_futoi_instrument.v2"
DATE_SOURCE_ARTIFACT_ID: Final[str] = trade_dates.SOURCE_ARTIFACT_ID
DATE_SOURCE_ID: Final[str] = trade_dates.SOURCE_ID
DATE_SOURCE_ENDPOINT: Final[str] = trade_dates.SOURCE_ENDPOINT
DATE_SELECTION_RULE: Final[str] = trade_dates.SELECTION_RULE
OBSERVED_DATE_EVIDENCE_SCHEMA: Final[str] = "futoi_backfill_observed_date_evidence.v1"


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


def _subrun_id(run_id: str, trade_date: str) -> str:
    return run_id + "_partition_" + trade_date.replace("-", "")


def _aggregate_quality_path(date_end: str, run_id: str) -> Path:
    return materializer._quality_path(date_end, run_id)


def _aggregate_manifest_path(date_end: str, run_id: str) -> Path:
    return materializer._manifest_path(date_end, run_id)


def _observed_date_evidence_path(date_end: str, run_id: str) -> Path:
    return _aggregate_manifest_path(date_end, run_id).with_name("observed_date_evidence.json")


def _json_bytes(values: dict[str, object]) -> bytes:
    return (json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")


def _write_bytes_create_only(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        raise FutoiBackfillError("immutable observed-date evidence artifact already exists")
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise FutoiBackfillError("immutable observed-date evidence artifact appeared concurrently") from exc


def _persist_observed_date_evidence(
    *,
    date_start: str,
    date_end: str,
    run_id: str,
    instrument_id: str,
    reference_secid: str,
    observed: Sequence[str],
) -> dict[str, object]:
    path = _observed_date_evidence_path(date_end, run_id)
    values: dict[str, object] = {
        "schema_version": OBSERVED_DATE_EVIDENCE_SCHEMA,
        "producer": PRODUCER_ID,
        "run_id": run_id,
        "dataset_id": DATASET_ID,
        "instrument_id": instrument_id,
        "date_source_artifact_id": DATE_SOURCE_ARTIFACT_ID,
        "date_source_id": DATE_SOURCE_ID,
        "date_source_endpoint": DATE_SOURCE_ENDPOINT,
        "date_selection_rule": DATE_SELECTION_RULE,
        "reference_secid": reference_secid,
        "requested_from": date_start,
        "requested_till": date_end,
        "observed_dates": list(observed),
        "observed_date_count": len(observed),
    }
    payload = _json_bytes(values)
    _write_bytes_create_only(path, payload)
    return {
        "path": path.as_posix(),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "row_count": len(observed),
    }


def _materializer_partition_evidence(
    payload: dict[str, object], *, trade_date: str, subrun_id: str
) -> dict[str, object]:
    if payload.get("publication_run_id") != subrun_id:
        raise FutoiBackfillError("materializer publication run_id mismatch")
    path = Path(str(payload.get("storage_partition_path") or ""))
    if not path.is_absolute():
        raise FutoiBackfillError("materialized FUTOI partition path must be absolute")
    sha256 = str(payload.get("published_partition_sha256") or "").strip().lower()
    if len(sha256) != 64 or any(char not in "0123456789abcdef" for char in sha256):
        raise FutoiBackfillError("materializer published partition sha256 invalid")
    row_count = int(payload.get("row_count") or 0)
    if row_count <= 0:
        raise FutoiBackfillError("materializer published partition row_count must be positive")
    return {
        "trade_date": trade_date,
        "subrun_id": subrun_id,
        "partition_path": path.as_posix(),
        "sha256": sha256,
        "row_count": row_count,
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
    observed_trade_dates: Sequence[str] | None = None,
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

    reference_secid = trade_dates.reference_secid(checked_instrument, registry_path)
    if observed_trade_dates is None:
        try:
            dates = trade_dates.observed_dates(
                start.isoformat(),
                end.isoformat(),
                instrument_id=checked_instrument,
                registry_path=registry_path,
                timeout=timeout,
                apim_base_url=apim_base_url,
            )
        except Exception as exc:
            raise FutoiBackfillError(
                "authoritative observed TradeStats date source failed for instrument_id="
                + checked_instrument
                + " reference_secid="
                + reference_secid
                + " range="
                + start.isoformat()
                + ".."
                + end.isoformat()
                + ": "
                + str(exc)
            ) from exc
    else:
        try:
            dates = trade_dates.normalize_observed_dates(
                observed_trade_dates,
                start.isoformat(),
                end.isoformat(),
            )
        except Exception as exc:
            raise FutoiBackfillError("observed_trade_dates are invalid: " + str(exc)) from exc

    evidence = _persist_observed_date_evidence(
        date_start=start.isoformat(),
        date_end=end.isoformat(),
        run_id=checked_run_id,
        instrument_id=checked_instrument,
        reference_secid=reference_secid,
        observed=dates,
    )

    successes: list[dict[str, object]] = []
    partition_evidence: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    duplicate_total = 0
    null_total = 0
    invalid_total = 0

    if progress_every:
        _emit_progress(
            instrument_id=checked_instrument,
            secid=str(binding["secid"]),
            futoi_ticker=str(binding["futoi.ticker"]),
            processed_dates=0,
            total_dates=len(dates),
            trade_date=None,
            partition_count=0,
            failure_count=0,
            event="start",
        )

    for processed_dates, trade_date in enumerate(dates, start=1):
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
            partition_record = _materializer_partition_evidence(
                payload,
                trade_date=trade_date,
                subrun_id=subrun_id,
            )
            successes.append(payload)
            partition_evidence.append(partition_record)
            quality = json.loads(Path(str(payload["quality_report_reference"])).read_text(encoding="utf-8"))
            duplicate_total += int(quality.get("duplicate_key_count") or 0)
            null_total += int(quality.get("null_required_count") or 0)
            invalid_total += int(quality.get("invalid_position_count") or 0)
        except Exception as exc:
            failures.append({"trade_date": trade_date, "error": str(exc)})

        if progress_every and (processed_dates % progress_every == 0 or processed_dates == len(dates)):
            _emit_progress(
                instrument_id=checked_instrument,
                secid=str(binding["secid"]),
                futoi_ticker=str(binding["futoi.ticker"]),
                processed_dates=processed_dates,
                total_dates=len(dates),
                trade_date=trade_date,
                partition_count=len(successes),
                failure_count=len(failures),
            )

    row_count = sum(int(item.get("row_count") or 0) for item in successes)
    quality_status = (
        "pass"
        if len(successes) == len(dates)
        and not failures
        and duplicate_total == 0
        and null_total == 0
        and invalid_total == 0
        else "fail"
    )
    refresh_status = "succeeded" if quality_status == "pass" else ("partial" if successes else "failed")
    quality_path = _aggregate_quality_path(end.isoformat(), checked_run_id)
    manifest_path = _aggregate_manifest_path(end.isoformat(), checked_run_id)
    common_date_source = {
        "date_source_artifact_id": DATE_SOURCE_ARTIFACT_ID,
        "date_source_id": DATE_SOURCE_ID,
        "date_source_endpoint": DATE_SOURCE_ENDPOINT,
        "date_selection_rule": DATE_SELECTION_RULE,
        "reference_secid": reference_secid,
        "observed_date_evidence_ref": evidence["path"],
        "observed_date_evidence_sha256": evidence["sha256"],
        "observed_date_count": evidence["row_count"],
    }
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
        "observed_trade_dates": list(dates),
        "failed_dates": failures,
        **common_date_source,
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
        "observed_trade_dates": list(dates),
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
        **common_date_source,
    }
    materializer._write_json_atomic(quality_path, quality_values)
    materializer._write_json_atomic(manifest_path, manifest_values)

    pointer_reference: str | None = None
    if create_accepted_pointer:
        if quality_status != "pass" or refresh_status != "succeeded":
            raise FutoiBackfillError("accepted pointer cannot be created unless full backfill passes")
        pointer_reference = _write_pointer(
            checked_instrument,
            checked_run_id,
            manifest_path,
            quality_path,
        ).as_posix()

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
        "observed_trade_dates": list(dates),
        "failed_dates": failures,
        "quality_report_reference": quality_path.as_posix(),
        "manifest_reference": manifest_path.as_posix(),
        "accepted_manifest_pointer_reference": pointer_reference,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "stage2_controlled_backfill": True,
        **common_date_source,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Controlled Stage 2 backfill for canonical FUTOI supplementary data using only "
            "trade dates observed from the authoritative TradeStats source."
        )
    )
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
        print(
            json.dumps(
                {
                    "status": "failed",
                    "dataset_id": DATASET_ID,
                    "error": str(exc),
                    "latest_autodetect_used": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
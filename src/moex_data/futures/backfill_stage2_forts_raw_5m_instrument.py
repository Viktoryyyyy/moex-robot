from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Final, Sequence

from . import accepted_manifest
from . import backfill_forts_raw_5m_instrument as base
from . import materialize_forts_raw_5m_instrument as materializer
from .manifest import validate_refresh_manifest_values
from .quality import validate_quality_report_rows
from .schemas import EXPECTED_DATASET_CONTRACT_IDS

DATASET_ID: Final[str] = materializer.DATASET_ID
SOURCE_ID: Final[str] = materializer.SOURCE_ID
SOURCE_CONTRACT_REF: Final[str] = materializer.SOURCE_CONTRACT_REF
REGISTRY_PATH: Final[str] = "configs/instruments/forts_instrument_registry.v1.yaml"
DATA_LAKE_PATH: Final[str] = "configs/datasets/futures_data_lake.v1.yaml"
PRODUCER_ID: Final[str] = "moex_data.futures.backfill_stage2_forts_raw_5m_instrument.v1"


class Stage2QuotesBackfillError(ValueError):
    pass


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
    raise Stage2QuotesBackfillError("required config section missing: " + header)


def _stage2_backfill_ready(data_lake_path: str | Path = DATA_LAKE_PATH) -> bool:
    text = Path(data_lake_path).read_text(encoding="utf-8")
    section = _section(text, "stage2_forts_source_bindings:")
    return (
        "status: all_pilots_passed_backfill_ready" in section
        and "backfill_ready: true" in section
        and "historical_quotes_backfill_ready: true" in section
        and "accepted_pointer_ready: false" in section
        and "scheduler_ready: false" in section
        and "research_ready: false" in section
    )


def _stage2_historical_quote_backfill_allows(
    data_lake_path: str | Path,
    instrument_id: str,
) -> bool:
    text = Path(data_lake_path).read_text(encoding="utf-8")
    stage2 = _section(text, "stage2_forts_source_bindings:")
    quote_source = _section(stage2, "quote_source:")
    historical_scope = _section(quote_source, "historical_backfill_instrument_ids:")
    return ("- " + instrument_id) in historical_scope


def _stage2_registry_allows(
    registry_path: str | Path,
    instrument_id: str,
    secid: str,
    source_id: str,
) -> bool:
    text = Path(registry_path).read_text(encoding="utf-8")
    if "family_partition_key_allowed: false" not in text:
        return False
    for entry in base._registry_entries(text):
        if entry.get("instrument_id") != instrument_id or entry.get("secid") != secid:
            continue
        if entry.get("source_id") != source_id:
            continue
        if entry.get("evidence_status") != "pilot_passed":
            continue
        if entry.get("enabled_for_raw_5m_materialization") is not False:
            continue
        return True
    return False


def _authorize(
    *,
    registry_path: str | Path,
    data_lake_path: str | Path,
    instrument_id: str,
    secid: str,
    source_id: str,
) -> None:
    if source_id != SOURCE_ID:
        raise Stage2QuotesBackfillError("source_id does not match canonical Stage 2 Quotes source")
    if not _stage2_backfill_ready(data_lake_path):
        raise Stage2QuotesBackfillError("Stage 2 controlled backfill readiness is not enabled")
    if not _stage2_historical_quote_backfill_allows(data_lake_path, instrument_id):
        raise Stage2QuotesBackfillError("instrument is current-reference only and is not authorized for Stage 2 full historical quote backfill")
    if not _stage2_registry_allows(registry_path, instrument_id, secid, source_id):
        raise Stage2QuotesBackfillError("instrument/source pilot evidence is not eligible for controlled Stage 2 backfill")


def backfill_range(
    *,
    date_start: str,
    date_end: str,
    instrument_id: str,
    secid: str,
    artifact_version: str,
    source_id: str = SOURCE_ID,
    registry_path: str | Path = REGISTRY_PATH,
    data_lake_path: str | Path = DATA_LAKE_PATH,
    timeout: float = 60.0,
    apim_base_url: str | None = None,
    max_dates: int | None = None,
    create_accepted_pointer: bool = False,
) -> base.BackfillSummary:
    checked_instrument = base._require_token(instrument_id, "instrument_id")
    checked_secid = base._require_token(secid, "secid")
    checked_source = base._require_token(source_id, "source_id")
    checked_version = base._require_token(artifact_version, "artifact_version")
    _authorize(
        registry_path=registry_path,
        data_lake_path=data_lake_path,
        instrument_id=checked_instrument,
        secid=checked_secid,
        source_id=checked_source,
    )

    requested_dates = base._date_range(date_start, date_end, max_dates=max_dates)
    if not requested_dates:
        raise Stage2QuotesBackfillError("requested date range is empty")
    top_paths = materializer.target_paths(
        requested_dates[-1], checked_instrument, checked_secid, checked_version, checked_source
    )
    started_at = base._utc_now()
    successes: list[dict[str, object]] = []
    skipped_empty_dates: list[str] = []
    failures: list[dict[str, str]] = []
    quality_rows: list[dict[str, object]] = []

    for trade_date in requested_dates:
        partition_version = base._partition_version(checked_version, trade_date, checked_instrument, checked_secid)
        try:
            result = materializer.materialize_instrument_partition(
                trade_date=trade_date,
                instrument_id=checked_instrument,
                secid=checked_secid,
                source_id=checked_source,
                artifact_version=partition_version,
                timeout=timeout,
                apim_base_url=apim_base_url,
            )
            payload = dict(result.payload)
            successes.append(payload)
            quality_rows.append(base._load_quality_row(str(payload["quality_report_reference"]), checked_version))
        except Exception as exc:
            message = str(exc)
            if base._empty_source_error(message):
                skipped_empty_dates.append(trade_date)
            else:
                failures.append({"trade_date": trade_date, "error": message})

    if quality_rows:
        validate_quality_report_rows(quality_rows)
    refresh_status = "succeeded" if quality_rows and not failures else ("partial" if quality_rows else "failed")
    quality_status = "pass" if quality_rows and not failures else "fail"
    finished_at = base._utc_now()
    manifest_values: dict[str, object] = {
        "run_id": checked_version,
        "run_date": requested_dates[-1],
        "requested_from": requested_dates[0],
        "requested_till": requested_dates[-1],
        "instrument_scope": [checked_instrument],
        "source_scope": [checked_source],
        "dataset_contract_refs": list(EXPECTED_DATASET_CONTRACT_IDS),
        "partitions_written": [str(item["storage_partition_path"]) for item in successes],
        "partitions_skipped": skipped_empty_dates,
        "quality_report_ref": top_paths.quality_report_path.as_posix(),
        "accepted_manifest_ref": base._accepted_manifest_ref(checked_instrument),
        "refresh_status": refresh_status,
        "started_at": started_at,
        "finished_at": finished_at,
        "source_contract": {
            "source_id": checked_source,
            "source_contract_ref": SOURCE_CONTRACT_REF,
            "secid": checked_secid,
            "board": "RFUD",
            "market": "forts",
            "engine": "futures",
        },
        "failed_dates": failures,
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "producer": PRODUCER_ID,
        "stage2_controlled_backfill": True,
    }
    validate_refresh_manifest_values(manifest_values)

    quality_report = {
        "run_id": checked_version,
        "quality_status": quality_status,
        "rows": quality_rows,
        "failed_dates": failures,
    }
    materializer.core._write_json_atomic(top_paths.quality_report_path, quality_report)
    materializer.core._write_json_atomic(top_paths.manifest_path, manifest_values)

    pointer_path: str | None = None
    if create_accepted_pointer:
        if refresh_status != "succeeded" or quality_status != "pass":
            raise Stage2QuotesBackfillError("accepted pointer cannot be created unless full backfill quality passes")
        pointer = accepted_manifest.write_accepted_manifest_pointer(
            env=None,
            dataset_id=DATASET_ID,
            instrument_id=checked_instrument,
            manifest_ref=top_paths.manifest_path.as_posix(),
            manifest_values=manifest_values,
            quality_rows=quality_rows,
        )
        pointer_path = pointer.accepted_manifest_path.as_posix()

    payload = dict(manifest_values)
    payload.update(
        {
            "status": refresh_status,
            "dataset_id": DATASET_ID,
            "source_id": checked_source,
            "quality_status": quality_status,
            "row_count": sum(int(item["row_count"]) for item in successes),
            "partition_count": len(successes),
            "instrument_id_scope": [checked_instrument],
            "secid_scope": [checked_secid],
            "skipped_empty_source_dates": skipped_empty_dates,
            "manifest_reference": top_paths.manifest_path.as_posix(),
            "quality_report_reference": top_paths.quality_report_path.as_posix(),
            "accepted_manifest_pointer_reference": pointer_path,
        }
    )
    return base.BackfillSummary(
        payload=payload,
        manifest_path=top_paths.manifest_path,
        quality_report_path=top_paths.quality_report_path,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Controlled Stage 2 backfill for canonical FORTS raw 5m Quotes after pilot acceptance.")
    parser.add_argument("--date-start", required=True)
    parser.add_argument("--date-end", required=True)
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--secid", required=True)
    parser.add_argument("--source-id", default=SOURCE_ID)
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--registry-path", default=REGISTRY_PATH)
    parser.add_argument("--data-lake-path", default=DATA_LAKE_PATH)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--apim-base-url", default=None)
    parser.add_argument("--max-dates", type=int, default=None)
    parser.add_argument("--create-accepted-pointer", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        base.load_env_file(args.env_file)
        summary = backfill_range(
            date_start=args.date_start,
            date_end=args.date_end,
            instrument_id=args.instrument_id,
            secid=args.secid,
            source_id=args.source_id,
            artifact_version=args.artifact_version,
            registry_path=args.registry_path,
            data_lake_path=args.data_lake_path,
            timeout=args.timeout,
            apim_base_url=args.apim_base_url,
            max_dates=args.max_dates,
            create_accepted_pointer=args.create_accepted_pointer,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "dataset_id": DATASET_ID, "error": str(exc), "latest_autodetect_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(summary.payload, ensure_ascii=False, sort_keys=True))
    return 0 if summary.payload["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())

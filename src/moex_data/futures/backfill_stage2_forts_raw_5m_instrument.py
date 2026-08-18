from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Final, Sequence

from . import backfill_forts_raw_5m_instrument as base
from . import materialize_forts_raw_5m_instrument as materializer

DATASET_ID: Final[str] = materializer.DATASET_ID
SOURCE_ID: Final[str] = materializer.SOURCE_ID
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
        and "accepted_pointer_ready: false" in section
        and "scheduler_ready: false" in section
        and "research_ready: false" in section
    )


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

    original_checker = base.registry_allows_instrument
    try:
        base.registry_allows_instrument = lambda *_args, **_kwargs: True
        return base.backfill_range(
            date_start=date_start,
            date_end=date_end,
            instrument_id=checked_instrument,
            secid=checked_secid,
            artifact_version=checked_version,
            source_id=checked_source,
            registry_path=registry_path,
            timeout=timeout,
            apim_base_url=apim_base_url,
            max_dates=max_dates,
            create_accepted_pointer=create_accepted_pointer,
        )
    finally:
        base.registry_allows_instrument = original_checker


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
    payload = dict(summary.payload)
    payload["producer"] = PRODUCER_ID
    payload["stage2_controlled_backfill"] = True
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())

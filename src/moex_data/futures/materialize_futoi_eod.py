from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Final

import pandas as pd

from . import materialize_futoi_instrument as raw_materializer

DATASET_ID: Final[str] = "futures_futoi_eod"
RAW_DATASET_ID: Final[str] = raw_materializer.DATASET_ID
SOURCE_ID: Final[str] = raw_materializer.SOURCE_ID
REGISTRY_PATH: Final[str] = raw_materializer.REGISTRY_PATH
DERIVATION_ID: Final[str] = "futoi_eod_last_observation_v1"
EOD_CONTRACT_REF: Final[str] = "contracts/datasets/futures_futoi_eod.v1.yaml"
QUALITY_CONTRACT_REF: Final[str] = "contracts/datasets/futures_futoi_eod_quality_report.v1.yaml"
MANIFEST_CONTRACT_REF: Final[str] = "contracts/datasets/futures_futoi_eod_refresh_manifest.v1.yaml"
PRODUCER_ID: Final[str] = "moex_data.futures.materialize_futoi_eod.v1"
EOD_KEY_FIELDS: Final[tuple[str, ...]] = ("trade_date", "secid", "clgroup")
EXPECTED_CLGROUPS: Final[frozenset[str]] = frozenset({"FIZ", "YUR"})
RAW_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "instrument_id",
    "trade_date",
    "ts",
    "moment",
    "systime",
    "sess_id",
    "seqnum",
    "secid",
    "board",
    "market",
    "engine",
    "source_id",
    "source_ticker",
    "clgroup",
    "pos",
    "pos_long",
    "pos_short",
    "pos_long_num",
    "pos_short_num",
    "availability_ts_utc",
    "ingest_ts",
)


class FutoiEodError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiEodError(message)


def _require_token(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    if text in (".", "..") or "/" in text or "\\" in text or any(marker in text for marker in ("*", "{", "}", "$(", "`")):
        _fail(field_name + " must be an explicit safe token")
    return text


def _require_date(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise FutoiEodError(field_name + " must be YYYY-MM-DD") from exc


def _data_root() -> Path:
    return raw_materializer._data_root().resolve()


def _resolve_root_reference(value: object, field_name: str) -> Path:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    root = _data_root()
    prefix = "${MOEX_DATA_ROOT}/"
    if text.startswith(prefix):
        candidate = (root / text[len(prefix) :]).resolve()
    else:
        candidate = Path(text).expanduser().resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        _fail(field_name + " must resolve inside MOEX_DATA_ROOT")
    return candidate


def _portable_ref(path: Path) -> str:
    root = _data_root()
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(root)
    except ValueError:
        _fail("path must resolve inside MOEX_DATA_ROOT")
    return "${MOEX_DATA_ROOT}/" + relative.as_posix()


def _eod_partition_path(trade_date: str, instrument_id: str) -> Path:
    return (
        _data_root()
        / "market"
        / "supplementary"
        / ("dataset_id=" + DATASET_ID)
        / ("instrument_id=" + instrument_id)
        / ("trade_date=" + trade_date)
        / ("source=" + SOURCE_ID)
        / "part.parquet"
    )


def _quality_path(run_date: str, run_id: str) -> Path:
    return (
        _data_root()
        / "state"
        / "quality"
        / ("dataset_id=" + DATASET_ID)
        / ("run_date=" + run_date)
        / ("run_id=" + run_id)
        / "quality_report.json"
    )


def _manifest_path(run_date: str, run_id: str) -> Path:
    return (
        _data_root()
        / "state"
        / "refresh"
        / ("dataset_id=" + DATASET_ID)
        / ("run_date=" + run_date)
        / ("run_id=" + run_id)
        / "manifest.json"
    )


def _load_json(path: Path, name: str) -> dict[str, object]:
    if not path.exists() or not path.is_file():
        _fail(name + " does not exist")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FutoiEodError(name + " is not valid JSON") from exc
    if not isinstance(values, dict):
        _fail(name + " root must be an object")
    return values


def _canonical_raw_partition_path(reference: object, instrument_id: str) -> tuple[str, Path]:
    path = _resolve_root_reference(reference, "raw partition reference")
    base = (
        _data_root()
        / "market"
        / "supplementary"
        / ("dataset_id=" + RAW_DATASET_ID)
        / ("instrument_id=" + instrument_id)
    ).resolve()
    try:
        relative = path.relative_to(base)
    except ValueError:
        _fail("raw partition is outside canonical raw instrument root")
    parts = relative.parts
    if len(parts) != 3 or not parts[0].startswith("trade_date=") or parts[1] != "source=" + SOURCE_ID or parts[2] != "part.parquet":
        _fail("raw partition path does not match canonical FUTOI raw pattern")
    trade_date = _require_date(parts[0].split("=", 1)[1], "raw partition trade_date")
    expected = (
        base
        / ("trade_date=" + trade_date)
        / ("source=" + SOURCE_ID)
        / "part.parquet"
    ).resolve()
    if path != expected:
        _fail("raw partition path is not canonical")
    if not path.exists() or not path.is_file():
        _fail("raw partition does not exist: " + trade_date)
    return trade_date, path


def _validate_raw_manifest(raw_manifest_path: str | Path, instrument_id: str) -> tuple[dict[str, object], Path, list[tuple[str, Path]], dict[str, object]]:
    manifest_path = _resolve_root_reference(raw_manifest_path, "raw_manifest_path")
    expected_manifest_root = (_data_root() / "state" / "refresh" / ("dataset_id=" + RAW_DATASET_ID)).resolve()
    try:
        manifest_relative = manifest_path.relative_to(expected_manifest_root)
    except ValueError:
        _fail("raw manifest is outside canonical raw refresh root")
    if len(manifest_relative.parts) != 3 or not manifest_relative.parts[0].startswith("run_date=") or not manifest_relative.parts[1].startswith("run_id=") or manifest_relative.parts[2] != "manifest.json":
        _fail("raw manifest path does not match canonical raw refresh pattern")

    manifest = _load_json(manifest_path, "raw manifest")
    if str(manifest.get("dataset_id")) != RAW_DATASET_ID:
        _fail("raw manifest dataset_id mismatch")
    if list(manifest.get("instrument_scope") or []) != [instrument_id]:
        _fail("raw manifest instrument_scope mismatch")
    if list(manifest.get("source_scope") or []) != [SOURCE_ID]:
        _fail("raw manifest source_scope mismatch")
    if str(manifest.get("refresh_status")) != "succeeded":
        _fail("raw manifest refresh_status is not succeeded")
    if list(manifest.get("failed_dates") or []):
        _fail("raw manifest contains failed_dates")
    requested_from = _require_date(manifest.get("requested_from"), "raw manifest requested_from")
    requested_till = _require_date(manifest.get("requested_till"), "raw manifest requested_till")
    if requested_from > requested_till:
        _fail("raw manifest requested_from exceeds requested_till")

    written = manifest.get("partitions_written")
    if not isinstance(written, list) or not written:
        _fail("raw manifest partitions_written must be a non-empty list")
    partition_pairs = [_canonical_raw_partition_path(item, instrument_id) for item in written]
    trade_dates = [item[0] for item in partition_pairs]
    if len(trade_dates) != len(set(trade_dates)):
        _fail("raw manifest contains duplicate trade_date partition references")
    if any(value < requested_from or value > requested_till for value in trade_dates):
        _fail("raw manifest partition date is outside requested range")

    skipped_values = manifest.get("partitions_skipped") or []
    if not isinstance(skipped_values, list):
        _fail("raw manifest partitions_skipped must be a list")
    skipped = [_require_date(item, "raw manifest skipped trade_date") for item in skipped_values]
    if len(skipped) != len(set(skipped)):
        _fail("raw manifest contains duplicate skipped dates")
    if set(skipped) & set(trade_dates):
        _fail("raw manifest written and skipped dates overlap")
    calendar_days = (date.fromisoformat(requested_till) - date.fromisoformat(requested_from)).days + 1
    if len(trade_dates) + len(skipped) != calendar_days:
        _fail("raw manifest written plus skipped dates do not reconcile to requested calendar range")

    quality_ref = manifest.get("quality_report_ref")
    quality_path = _resolve_root_reference(quality_ref, "raw quality_report_ref")
    expected_quality_root = (_data_root() / "state" / "quality" / ("dataset_id=" + RAW_DATASET_ID)).resolve()
    try:
        quality_path.relative_to(expected_quality_root)
    except ValueError:
        _fail("raw quality report is outside canonical raw quality root")
    raw_quality = _load_json(quality_path, "raw quality report")
    if str(raw_quality.get("dataset_id")) != RAW_DATASET_ID:
        _fail("raw quality dataset_id mismatch")
    if str(raw_quality.get("instrument_id")) != instrument_id:
        _fail("raw quality instrument_id mismatch")
    if str(raw_quality.get("source_id")) != SOURCE_ID:
        _fail("raw quality source_id mismatch")
    if str(raw_quality.get("quality_status")) != "pass":
        _fail("raw quality_status is not pass")
    if int(raw_quality.get("partition_count") or 0) != len(partition_pairs):
        _fail("raw quality partition_count does not match manifest")
    if list(raw_quality.get("failed_dates") or []):
        _fail("raw quality report contains failed_dates")
    quality_skipped = set(str(item) for item in (raw_quality.get("skipped_empty_source_dates") or []))
    if quality_skipped != set(skipped):
        _fail("raw quality skipped dates do not match manifest")
    for field in ("duplicate_key_count", "null_required_count", "invalid_position_count"):
        if int(raw_quality.get(field) or 0) != 0:
            _fail("raw quality " + field + " is nonzero")

    return manifest, manifest_path, sorted(partition_pairs), raw_quality


def _validate_raw_partition(frame: pd.DataFrame, trade_date: str, instrument_id: str, binding: Mapping[str, object]) -> pd.DataFrame:
    if frame.empty:
        _fail("raw partition is empty: " + trade_date)
    normalized_names = [str(column).strip().lower() for column in frame.columns]
    if len(normalized_names) != len(set(normalized_names)):
        _fail("raw partition has duplicate columns after case normalization: " + trade_date)
    result = frame.copy()
    result.columns = normalized_names
    missing = [field for field in RAW_REQUIRED_COLUMNS if field not in result.columns]
    if missing:
        _fail("raw partition missing required columns on " + trade_date + ": " + ",".join(missing))
    if int(result[list(RAW_REQUIRED_COLUMNS)].isna().any(axis=1).sum()) != 0:
        _fail("raw partition has null required values: " + trade_date)

    result = raw_materializer._validate_required_source_identifiers(result)
    if int(result.duplicated(subset=list(raw_materializer.SOURCE_RECORD_KEY_FIELDS)).sum()) != 0:
        _fail("raw partition has duplicate source-record key: " + trade_date)
    if not result["instrument_id"].astype(str).eq(instrument_id).all():
        _fail("raw partition instrument_id mismatch: " + trade_date)
    if not result["trade_date"].astype(str).eq(trade_date).all():
        _fail("raw partition trade_date mismatch: " + trade_date)
    if not result["source_id"].astype(str).eq(SOURCE_ID).all():
        _fail("raw partition source_id mismatch: " + trade_date)
    for field in ("secid", "board", "market", "engine"):
        if not result[field].astype(str).eq(str(binding[field])).all():
            _fail("raw partition " + field + " mismatch: " + trade_date)
    if not result["source_ticker"].astype(str).str.lower().eq(str(binding["futoi.ticker"]).lower()).all():
        _fail("raw partition source_ticker mismatch: " + trade_date)

    groups = result["clgroup"].astype(str).str.upper().str.strip()
    if set(groups.unique().tolist()) != set(EXPECTED_CLGROUPS):
        _fail("raw partition must contain exactly FIZ and YUR: " + trade_date)
    result["clgroup"] = groups

    ts = pd.to_datetime(result["ts"], errors="coerce")
    moment = pd.to_datetime(result["moment"], errors="coerce")
    systime = pd.to_datetime(result["systime"], errors="coerce")
    if bool(ts.isna().any()) or bool(moment.isna().any()) or bool(systime.isna().any()):
        _fail("raw partition contains invalid timestamps: " + trade_date)
    if not bool(ts.eq(moment).all()):
        _fail("raw partition ts does not equal moment: " + trade_date)
    if not bool(ts.dt.date.astype(str).eq(trade_date).all()):
        _fail("raw partition ts date mismatch: " + trade_date)
    if bool((systime < ts).any()):
        _fail("raw partition systime precedes ts: " + trade_date)
    result["ts"] = ts
    result["moment"] = moment
    result["systime"] = systime

    invalid = (
        (result["pos_long"] < 0)
        | (result["pos_short"] > 0)
        | (result["pos_long_num"] < 0)
        | (result["pos_short_num"] < 0)
    )
    if bool(invalid.fillna(True).any()):
        _fail("raw partition contains invalid position values: " + trade_date)
    return result.reset_index(drop=True)


def _derive_partition(
    raw: pd.DataFrame,
    *,
    trade_date: str,
    instrument_id: str,
    raw_run_id: str,
    raw_manifest_ref: str,
    raw_partition_ref: str,
    derived_ingest_ts: str,
) -> pd.DataFrame:
    selected_rows: list[pd.Series] = []
    for clgroup in sorted(EXPECTED_CLGROUPS):
        group = raw.loc[raw["clgroup"].astype(str) == clgroup].copy()
        if group.empty:
            _fail("raw partition missing clgroup " + clgroup + ": " + trade_date)
        max_ts = group["ts"].max()
        candidates = group.loc[group["ts"] == max_ts].copy()
        sess_ids = sorted(set(int(value) for value in candidates["sess_id"].tolist()))
        if len(sess_ids) != 1:
            _fail("ambiguous EOD session at maximum ts for " + clgroup + ": " + trade_date)
        candidates["seqnum"] = [raw_materializer._coerce_source_identifier(value, "seqnum") for value in candidates["seqnum"].tolist()]
        max_seqnum = max(int(value) for value in candidates["seqnum"].tolist())
        winner = candidates.loc[candidates["seqnum"].astype(object) == max_seqnum].copy()
        if len(winner) != 1:
            _fail("ambiguous EOD source revision for " + clgroup + ": " + trade_date)
        row = winner.iloc[0].copy()
        row["raw_source_record_count"] = int(len(raw))
        row["max_ts_revision_count"] = int(len(candidates))
        selected_rows.append(row)

    out = pd.DataFrame(selected_rows).reset_index(drop=True)
    if len(out) != 2 or set(out["clgroup"].astype(str).tolist()) != set(EXPECTED_CLGROUPS):
        _fail("derived EOD partition does not contain exactly FIZ and YUR: " + trade_date)
    if int(out.duplicated(subset=list(EOD_KEY_FIELDS)).sum()) != 0:
        _fail("derived EOD partition has duplicate canonical key: " + trade_date)

    coherence = out[["ts", "sess_id", "seqnum"]].drop_duplicates()
    if len(coherence) != 1:
        _fail("FIZ and YUR EOD selections are not the same source snapshot: " + trade_date)

    out["schema_version"] = "futures_futoi_eod.v1"
    out["derivation_id"] = DERIVATION_ID
    out["raw_dataset_id"] = RAW_DATASET_ID
    out["raw_run_id"] = raw_run_id
    out["raw_manifest_reference"] = raw_manifest_ref
    out["raw_partition_reference"] = raw_partition_ref
    out["raw_availability_ts_utc"] = out["availability_ts_utc"]
    out["raw_ingest_ts"] = out["ingest_ts"]
    out["derived_ingest_ts"] = derived_ingest_ts
    out["availability_ts_utc"] = derived_ingest_ts
    out["ingest_ts"] = derived_ingest_ts
    return out.sort_values(["trade_date", "clgroup"]).reset_index(drop=True)


def _quality_counts(frames: Sequence[pd.DataFrame]) -> dict[str, int]:
    if not frames:
        return {
            "row_count": 0,
            "duplicate_key_count": 0,
            "null_required_count": 0,
            "invalid_position_count": 0,
        }
    frame = pd.concat(list(frames), ignore_index=True)
    required = [
        "instrument_id",
        "trade_date",
        "ts",
        "sess_id",
        "seqnum",
        "secid",
        "clgroup",
        "pos",
        "pos_long",
        "pos_short",
        "pos_long_num",
        "pos_short_num",
        "derivation_id",
        "raw_dataset_id",
        "raw_run_id",
        "raw_manifest_reference",
        "raw_partition_reference",
        "derived_ingest_ts",
    ]
    null_required = int(frame[required].isna().any(axis=1).sum())
    duplicates = int(frame.duplicated(subset=list(EOD_KEY_FIELDS)).sum())
    invalid = (
        (frame["pos_long"] < 0)
        | (frame["pos_short"] > 0)
        | (frame["pos_long_num"] < 0)
        | (frame["pos_short_num"] < 0)
    )
    return {
        "row_count": int(len(frame)),
        "duplicate_key_count": duplicates,
        "null_required_count": null_required,
        "invalid_position_count": int(invalid.fillna(True).sum()),
    }


def materialize_futoi_eod(
    *,
    instrument_id: str,
    run_id: str,
    raw_manifest_path: str | Path,
    registry_path: str | Path = REGISTRY_PATH,
) -> dict[str, object]:
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_run_id = _require_token(run_id, "run_id")
    binding = raw_materializer._registry_binding(registry_path, checked_instrument)
    if str(binding["futoi.source_id"]) != SOURCE_ID:
        _fail("registry FUTOI source_id does not match canonical source")

    raw_manifest, raw_manifest_file, raw_partitions, _ = _validate_raw_manifest(raw_manifest_path, checked_instrument)
    raw_run_id = _require_token(raw_manifest.get("run_id"), "raw manifest run_id")
    requested_from = _require_date(raw_manifest.get("requested_from"), "requested_from")
    requested_till = _require_date(raw_manifest.get("requested_till"), "requested_till")
    derived_ingest_ts = raw_materializer._utc_now()
    portable_manifest_ref = _portable_ref(raw_manifest_file)

    derived: list[tuple[str, Path, pd.DataFrame]] = []
    failures: list[dict[str, str]] = []
    for trade_date, raw_path in raw_partitions:
        try:
            raw = pd.read_parquet(raw_path)
            raw = _validate_raw_partition(raw, trade_date, checked_instrument, binding)
            output = _derive_partition(
                raw,
                trade_date=trade_date,
                instrument_id=checked_instrument,
                raw_run_id=raw_run_id,
                raw_manifest_ref=portable_manifest_ref,
                raw_partition_ref=_portable_ref(raw_path),
                derived_ingest_ts=derived_ingest_ts,
            )
            derived.append((trade_date, _eod_partition_path(trade_date, checked_instrument), output))
        except Exception as exc:
            failures.append({"trade_date": trade_date, "error": str(exc)})

    frames = [item[2] for item in derived]
    counts = _quality_counts(frames)
    raw_partition_count = len(raw_partitions)
    partition_count = len(derived)
    expected_rows = 2 * raw_partition_count
    ambiguity_count = len(failures)
    quality_status = "pass"
    if (
        failures
        or raw_partition_count <= 0
        or partition_count != raw_partition_count
        or counts["row_count"] != expected_rows
        or counts["duplicate_key_count"] != 0
        or counts["null_required_count"] != 0
        or counts["invalid_position_count"] != 0
    ):
        quality_status = "fail"

    run_date = requested_till
    quality_path = _quality_path(run_date, checked_run_id)
    manifest_path = _manifest_path(run_date, checked_run_id)
    quality = {
        "run_id": checked_run_id,
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "source_id": SOURCE_ID,
        "raw_dataset_id": RAW_DATASET_ID,
        "raw_manifest_reference": portable_manifest_ref,
        "requested_from": requested_from,
        "requested_till": requested_till,
        "quality_status": quality_status,
        "raw_partition_count": raw_partition_count,
        "partition_count": partition_count,
        "row_count": counts["row_count"],
        "duplicate_key_count": counts["duplicate_key_count"],
        "null_required_count": counts["null_required_count"],
        "invalid_position_count": counts["invalid_position_count"],
        "ambiguity_count": ambiguity_count,
        "failed_dates": failures,
        "derivation_id": DERIVATION_ID,
        "quality_contract_ref": QUALITY_CONTRACT_REF,
    }
    partitions_written: list[str] = []
    if quality_status == "pass":
        for _, output_path, frame in derived:
            raw_materializer._write_parquet_atomic(output_path, frame, checked_run_id)
            partitions_written.append(output_path.as_posix())

    manifest = {
        "run_id": checked_run_id,
        "run_date": run_date,
        "dataset_id": DATASET_ID,
        "instrument_scope": [checked_instrument],
        "source_scope": [SOURCE_ID],
        "requested_from": requested_from,
        "requested_till": requested_till,
        "raw_dataset_id": RAW_DATASET_ID,
        "raw_manifest_reference": portable_manifest_ref,
        "raw_run_id": raw_run_id,
        "raw_partition_count": raw_partition_count,
        "partitions_written": partitions_written,
        "failed_dates": failures,
        "quality_report_ref": quality_path.as_posix(),
        "refresh_status": "succeeded" if quality_status == "pass" else "failed",
        "producer": PRODUCER_ID,
        "derivation_id": DERIVATION_ID,
        "eod_contract_ref": EOD_CONTRACT_REF,
        "quality_contract_ref": QUALITY_CONTRACT_REF,
        "manifest_contract_ref": MANIFEST_CONTRACT_REF,
        "hardcoded_server_path_used": False,
        "dynamic_scan_used": False,
        "direct_source_refetch_used": False,
        "accepted_manifest_pointer_reference": None,
    }
    raw_materializer._write_json_atomic(quality_path, quality)
    raw_materializer._write_json_atomic(manifest_path, manifest)

    if quality_status != "pass":
        _fail("FUTOI EOD derivation failed for " + str(len(failures)) + " raw partitions")
    return {
        "status": "succeeded",
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "source_id": SOURCE_ID,
        "raw_dataset_id": RAW_DATASET_ID,
        "raw_run_id": raw_run_id,
        "raw_manifest_reference": portable_manifest_ref,
        "requested_from": requested_from,
        "requested_till": requested_till,
        "raw_partition_count": raw_partition_count,
        "partition_count": partition_count,
        "row_count": counts["row_count"],
        "quality_status": quality_status,
        "derivation_id": DERIVATION_ID,
        "quality_report_reference": quality_path.as_posix(),
        "manifest_reference": manifest_path.as_posix(),
        "accepted_manifest_pointer_reference": None,
        "hardcoded_server_path_used": False,
        "dynamic_scan_used": False,
        "direct_source_refetch_used": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Derive canonical FUTOI EOD snapshots from an explicit pinned canonical raw backfill manifest.")
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--raw-manifest-path", required=True)
    parser.add_argument("--registry-path", default=REGISTRY_PATH)
    parser.add_argument("--env-file", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        raw_materializer.load_env_file(args.env_file)
        payload = materialize_futoi_eod(
            instrument_id=args.instrument_id,
            run_id=args.run_id,
            raw_manifest_path=args.raw_manifest_path,
            registry_path=args.registry_path,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "dataset_id": DATASET_ID, "error": str(exc), "dynamic_scan_used": False, "direct_source_refetch_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

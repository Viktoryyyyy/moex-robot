from __future__ import annotations

import argparse
import io
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

import pandas as pd

from . import materialize_futoi_instrument as raw_materializer
from . import materialize_futoi_raw_content_pin as raw_pin

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
    return raw_pin._require_date(value, field_name)


def _require_sha256(value: object, field_name: str) -> str:
    return raw_pin._require_sha256(value, field_name)


def _data_root() -> Path:
    return raw_materializer._data_root().resolve()


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


def _validate_raw_partition(
    frame: pd.DataFrame,
    trade_date: str,
    instrument_id: str,
    binding: Mapping[str, object],
) -> pd.DataFrame:
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
    raw_content_pin_ref: str,
    raw_content_pin_sha256: str,
    raw_partition_ref: str,
    raw_partition_sha256: str,
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
        candidates["seqnum"] = [
            raw_materializer._coerce_source_identifier(value, "seqnum")
            for value in candidates["seqnum"].tolist()
        ]
        max_seqnum = max(int(value) for value in candidates["seqnum"].tolist())
        winner = candidates.loc[candidates["seqnum"] == max_seqnum].copy()
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
    if not out["instrument_id"].astype(str).eq(instrument_id).all():
        _fail("derived EOD instrument_id mismatch: " + trade_date)

    coherence = out[["ts", "sess_id", "seqnum"]].drop_duplicates()
    if len(coherence) != 1:
        _fail("FIZ and YUR EOD selections are not the same source snapshot: " + trade_date)

    out["schema_version"] = "futures_futoi_eod.v1"
    out["derivation_id"] = DERIVATION_ID
    out["raw_dataset_id"] = RAW_DATASET_ID
    out["raw_run_id"] = raw_run_id
    out["raw_manifest_reference"] = raw_manifest_ref
    out["raw_content_pin_reference"] = raw_content_pin_ref
    out["raw_content_pin_sha256"] = raw_content_pin_sha256
    out["raw_partition_reference"] = raw_partition_ref
    out["raw_partition_sha256"] = raw_partition_sha256
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
        "schema_version",
        "instrument_id",
        "trade_date",
        "ts",
        "moment",
        "systime",
        "sess_id",
        "seqnum",
        "secid",
        "clgroup",
        "pos",
        "pos_long",
        "pos_short",
        "pos_long_num",
        "pos_short_num",
        "availability_ts_utc",
        "ingest_ts",
        "derivation_id",
        "raw_dataset_id",
        "raw_run_id",
        "raw_manifest_reference",
        "raw_content_pin_reference",
        "raw_content_pin_sha256",
        "raw_partition_reference",
        "raw_partition_sha256",
        "raw_availability_ts_utc",
        "raw_ingest_ts",
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
    raw_pin_path: str | Path,
    raw_pin_sha256: str,
    registry_path: str | Path = REGISTRY_PATH,
) -> dict[str, object]:
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_run_id = _require_token(run_id, "run_id")
    checked_pin_sha256 = _require_sha256(raw_pin_sha256, "raw_pin_sha256")
    binding = raw_materializer._registry_binding(registry_path, checked_instrument)
    if str(binding["futoi.source_id"]) != SOURCE_ID:
        _fail("registry FUTOI source_id does not match canonical source")

    pin, pin_file, raw_manifest, raw_manifest_file, raw_entries = raw_pin.load_and_verify_content_pin(
        raw_pin_path,
        checked_pin_sha256,
        checked_instrument,
    )
    raw_run_id = _require_token(raw_manifest.get("run_id"), "raw manifest run_id")
    requested_from = _require_date(raw_manifest.get("requested_from"), "requested_from")
    requested_till = _require_date(raw_manifest.get("requested_till"), "requested_till")
    derived_ingest_ts = raw_materializer._utc_now()
    portable_manifest_ref = _portable_ref(raw_manifest_file)
    portable_pin_ref = _portable_ref(pin_file)

    derived: list[tuple[str, Path, pd.DataFrame]] = []
    failures: list[dict[str, str]] = []
    for entry in raw_entries:
        trade_date = str(entry["trade_date"])
        raw_path = Path(str(entry["path"]))
        try:
            raw_bytes = raw_pin.read_verified_partition_bytes(entry)
            raw = pd.read_parquet(io.BytesIO(raw_bytes))
            raw = _validate_raw_partition(raw, trade_date, checked_instrument, binding)
            output = _derive_partition(
                raw,
                trade_date=trade_date,
                instrument_id=checked_instrument,
                raw_run_id=raw_run_id,
                raw_manifest_ref=portable_manifest_ref,
                raw_content_pin_ref=portable_pin_ref,
                raw_content_pin_sha256=checked_pin_sha256,
                raw_partition_ref=_portable_ref(raw_path),
                raw_partition_sha256=str(entry["sha256"]),
                derived_ingest_ts=derived_ingest_ts,
            )
            derived.append((trade_date, _eod_partition_path(trade_date, checked_instrument), output))
        except Exception as exc:
            failures.append({"trade_date": trade_date, "error": str(exc)})

    frames = [item[2] for item in derived]
    counts = _quality_counts(frames)
    raw_partition_count = len(raw_entries)
    partition_count = len(derived)
    expected_rows = 2 * raw_partition_count
    failure_count = len(failures)
    ambiguity_count = sum(
        1
        for item in failures
        if "ambiguous" in item["error"].lower() or "same source snapshot" in item["error"].lower()
    )
    quality_status = "pass"
    if (
        failure_count != 0
        or raw_partition_count <= 0
        or partition_count != raw_partition_count
        or counts["row_count"] != expected_rows
        or counts["duplicate_key_count"] != 0
        or counts["null_required_count"] != 0
        or counts["invalid_position_count"] != 0
        or ambiguity_count != 0
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
        "raw_content_pin_reference": portable_pin_ref,
        "raw_content_pin_sha256": checked_pin_sha256,
        "requested_from": requested_from,
        "requested_till": requested_till,
        "quality_status": quality_status,
        "raw_partition_count": raw_partition_count,
        "partition_count": partition_count,
        "row_count": counts["row_count"],
        "duplicate_key_count": counts["duplicate_key_count"],
        "null_required_count": counts["null_required_count"],
        "invalid_position_count": counts["invalid_position_count"],
        "failure_count": failure_count,
        "ambiguity_count": ambiguity_count,
        "failed_dates": failures,
        "derivation_id": DERIVATION_ID,
        "content_pin_status": str(pin.get("pin_status")),
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
        "raw_content_pin_reference": portable_pin_ref,
        "raw_content_pin_sha256": checked_pin_sha256,
        "raw_partition_count": raw_partition_count,
        "partitions_written": partitions_written,
        "failed_dates": failures,
        "quality_report_ref": quality_path.as_posix(),
        "quality_status": quality_status,
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
        _fail("FUTOI EOD derivation quality failed")
    return {
        "status": "succeeded",
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "source_id": SOURCE_ID,
        "raw_dataset_id": RAW_DATASET_ID,
        "raw_run_id": raw_run_id,
        "raw_manifest_reference": portable_manifest_ref,
        "raw_content_pin_reference": portable_pin_ref,
        "raw_content_pin_sha256": checked_pin_sha256,
        "requested_from": requested_from,
        "requested_till": requested_till,
        "raw_partition_count": raw_partition_count,
        "partition_count": partition_count,
        "row_count": counts["row_count"],
        "quality_status": quality_status,
        "failure_count": failure_count,
        "ambiguity_count": ambiguity_count,
        "derivation_id": DERIVATION_ID,
        "quality_report_reference": quality_path.as_posix(),
        "manifest_reference": manifest_path.as_posix(),
        "accepted_manifest_pointer_reference": None,
        "hardcoded_server_path_used": False,
        "dynamic_scan_used": False,
        "direct_source_refetch_used": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Derive canonical FUTOI EOD snapshots from an explicit immutable SHA-256 raw content pin."
    )
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--raw-pin-path", required=True)
    parser.add_argument("--raw-pin-sha256", required=True)
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
            raw_pin_path=args.raw_pin_path,
            raw_pin_sha256=args.raw_pin_sha256,
            registry_path=args.registry_path,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "dataset_id": DATASET_ID,
                    "error": str(exc),
                    "dynamic_scan_used": False,
                    "direct_source_refetch_used": False,
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

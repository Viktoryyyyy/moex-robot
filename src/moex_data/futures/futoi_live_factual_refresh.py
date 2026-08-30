from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

import pandas as pd

from . import backfill_futoi_instrument as backfill
from . import futoi_raw_incremental_acceptance as incremental
from . import materialize_futoi_instrument as materializer
from . import refresh_forts_raw_5m_incremental as futures_calendar

PROJECT: Final[str] = "MOEX_Bot"
SCHEMA_VERSION: Final[str] = "futoi_live_factual_refresh.v1"
DATASET_ID: Final[str] = "futoi_live_factual_context"
SOURCE_ID: Final[str] = materializer.SOURCE_ID
INSTRUMENT_ID: Final[str] = "si_futures_family"
MARKET_TZ: Final[str] = "Europe/Moscow"
ROOT_REF_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"
CALENDAR_LOOKBACK_DAYS: Final[int] = 31


class FutoiLiveFactualRefreshError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiLiveFactualRefreshError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or any(
        marker in text for marker in ("/", "\\", "*", "?", "[", "]", "{", "}", "$(", "`")
    ):
        _fail(field + " must be an explicit safe token")
    return text


def _iso_date(value: object, field: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise FutoiLiveFactualRefreshError(field + " must be YYYY-MM-DD") from exc
    if parsed.isoformat() != text:
        _fail(field + " must be canonical YYYY-MM-DD")
    return text


def _data_root() -> Path:
    raw = str(os.environ.get("MOEX_DATA_ROOT", ""))
    if not raw or raw != raw.strip():
        _fail("MOEX_DATA_ROOT is required without surrounding whitespace")
    root = Path(raw)
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        _fail("MOEX_DATA_ROOT must be an existing absolute non-symlink directory")
    return root.resolve(strict=True)


def _rooted_ref(root: Path, path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        _fail("artifact reference must be a regular non-symlink file")
    resolved = path.resolve(strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise FutoiLiveFactualRefreshError("artifact escaped MOEX_DATA_ROOT") from exc
    return ROOT_REF_PREFIX + relative.as_posix()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _current_path(root: Path) -> Path:
    return (
        root
        / "state"
        / "datasets"
        / ("dataset_id=" + DATASET_ID)
        / ("instrument_id=" + INSTRUMENT_ID)
        / "current.json"
    )


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.is_symlink():
        _fail("factual current artifact must not be a symlink")
    serialized = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n"
    ).encode("utf-8")
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False, suffix=".tmp") as handle:
        handle.write(serialized)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _latest_completed_trading_date(through_date: str, *, timeout: float) -> str:
    end = date.fromisoformat(_iso_date(through_date, "through_date"))
    start = end - timedelta(days=CALENDAR_LOOKBACK_DAYS)
    rows = futures_calendar.fetch_futures_calendar_rows(
        start.isoformat(), end.isoformat(), timeout=timeout
    )
    calendar = futures_calendar._calendar_map(rows)
    trading = sorted(day for day, is_trading in calendar.items() if day <= end and is_trading)
    if not trading:
        _fail("canonical MOEX futures calendar contains no completed trading date in lookback window")
    return trading[-1].isoformat()


def _as_int(value: object, field: str) -> int:
    if value is None or isinstance(value, bool) or pd.isna(value):
        _fail(field + " must be a finite integer")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise FutoiLiveFactualRefreshError(field + " must be numeric") from exc
    if not pd.notna(number) or not float(number).is_integer():
        _fail(field + " must be a finite integer")
    return int(number)


def _market_timestamp_to_utc(value: object, field: str) -> pd.Timestamp:
    try:
        parsed = pd.Timestamp(value)
    except Exception as exc:
        raise FutoiLiveFactualRefreshError(field + " must be a valid timestamp") from exc
    if pd.isna(parsed):
        _fail(field + " must be a valid timestamp")
    try:
        if parsed.tzinfo is None:
            parsed = parsed.tz_localize(MARKET_TZ)
        else:
            parsed = parsed.tz_convert(MARKET_TZ)
    except Exception as exc:
        raise FutoiLiveFactualRefreshError(field + " cannot be localized to " + MARKET_TZ) from exc
    return parsed.tz_convert("UTC")


def _resolved_group(frame: pd.DataFrame, group: str, ts: pd.Timestamp) -> pd.Series:
    rows = frame.loc[
        (frame["clgroup"].astype(str).str.upper() == group) & (frame["_parsed_ts"] == ts)
    ].copy()
    if rows.empty:
        _fail("latest aligned FUTOI snapshot is missing " + group)
    sessions = set(_as_int(value, group + ".sess_id") for value in rows["sess_id"].tolist())
    if len(sessions) != 1:
        _fail("latest aligned FUTOI snapshot has multiple sess_id values for " + group)
    seqnums = [_as_int(value, group + ".seqnum") for value in rows["seqnum"].tolist()]
    max_seq = max(seqnums)
    selected = rows.loc[[seq == max_seq for seq in seqnums]]
    if len(selected) != 1:
        _fail("latest aligned FUTOI snapshot has ambiguous max seqnum for " + group)
    return selected.iloc[0]


def latest_aligned_factual(
    frame: pd.DataFrame, *, expected_trade_date: str
) -> dict[str, object]:
    required = {
        "trade_date",
        "ts",
        "systime",
        "availability_ts_utc",
        "ingest_ts",
        "sess_id",
        "seqnum",
        "clgroup",
        "pos",
        "pos_long",
        "pos_short",
        "pos_long_num",
        "pos_short_num",
        "source_id",
        "source_ticker",
        "secid",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        _fail("accepted FUTOI partition missing columns: " + ",".join(missing))
    if frame.empty:
        _fail("accepted FUTOI partition is empty")
    dates = set(str(value) for value in frame["trade_date"].tolist())
    if dates != {expected_trade_date}:
        _fail("accepted FUTOI partition trade_date mismatch")
    sources = set(str(value) for value in frame["source_id"].tolist())
    if sources != {SOURCE_ID}:
        _fail("accepted FUTOI partition source_id mismatch")

    work = frame.copy()
    work["_parsed_ts"] = pd.to_datetime(work["ts"], errors="coerce")
    if bool(work["_parsed_ts"].isna().any()):
        _fail("accepted FUTOI partition contains invalid ts")
    groups_by_ts = work.groupby("_parsed_ts")["clgroup"].agg(
        lambda values: set(str(value).upper() for value in values)
    )
    aligned = [ts for ts, groups in groups_by_ts.items() if groups == {"FIZ", "YUR"}]
    if not aligned:
        _fail("accepted FUTOI partition has no exact aligned FIZ/YUR snapshot")
    selected_ts = max(aligned)
    fiz = _resolved_group(work, "FIZ", selected_ts)
    yur = _resolved_group(work, "YUR", selected_ts)

    fiz_sess_id = _as_int(fiz["sess_id"], "FIZ.sess_id")
    yur_sess_id = _as_int(yur["sess_id"], "YUR.sess_id")
    if fiz_sess_id != yur_sess_id:
        _fail("latest aligned FUTOI FIZ/YUR snapshot must share sess_id")
    if str(fiz["source_ticker"]) != str(yur["source_ticker"]):
        _fail("latest aligned FUTOI FIZ/YUR snapshot source_ticker mismatch")
    if str(fiz["secid"]) != str(yur["secid"]):
        _fail("latest aligned FUTOI FIZ/YUR snapshot secid mismatch")

    def side(row: pd.Series, label: str) -> dict[str, int]:
        long_value = _as_int(row["pos_long"], label + ".pos_long")
        short_signed = _as_int(row["pos_short"], label + ".pos_short")
        net_value = _as_int(row["pos"], label + ".pos")
        long_num = _as_int(row["pos_long_num"], label + ".pos_long_num")
        short_num = _as_int(row["pos_short_num"], label + ".pos_short_num")
        if long_value < 0 or short_signed > 0 or long_num < 0 or short_num < 0:
            _fail(label + " contains invalid position signs/counts")
        if net_value != long_value + short_signed:
            _fail(label + " net position identity failed")
        return {
            "long": long_value,
            "short": abs(short_signed),
            "net": net_value,
            "long_participants": long_num,
            "short_participants": short_num,
        }

    fiz_values = side(fiz, "FIZ")
    yur_values = side(yur, "YUR")
    if fiz_values["net"] + yur_values["net"] != 0:
        _fail("FIZ/YUR net positions do not balance to zero")
    total_long = fiz_values["long"] + yur_values["long"]
    total_short = fiz_values["short"] + yur_values["short"]
    if total_long != total_short:
        _fail("FIZ/YUR total long and short open interest do not balance")

    snapshot_utc = _market_timestamp_to_utc(selected_ts, "snapshot_ts")
    publication_utc = max(
        _market_timestamp_to_utc(fiz["systime"], "FIZ.systime"),
        _market_timestamp_to_utc(yur["systime"], "YUR.systime"),
    )
    availability = max(
        pd.to_datetime(
            [fiz["availability_ts_utc"], yur["availability_ts_utc"]],
            utc=True,
            errors="raise",
        )
    )
    ingest = max(
        pd.to_datetime([fiz["ingest_ts"], yur["ingest_ts"]], utc=True, errors="raise")
    )
    if availability < publication_utc:
        _fail("FUTOI availability timestamp precedes source publication timestamp")
    if ingest < availability:
        _fail("FUTOI ingest timestamp precedes availability timestamp")

    return {
        "trade_date": expected_trade_date,
        "snapshot_ts": snapshot_utc.isoformat(),
        "source_publication_time": publication_utc.isoformat(),
        "availability_ts_utc": availability.isoformat(),
        "ingest_ts_utc": ingest.isoformat(),
        "source_ticker": str(fiz["source_ticker"]),
        "secid": str(fiz["secid"]),
        "sess_id": fiz_sess_id,
        "fiz": fiz_values,
        "yur": yur_values,
        "total_open_interest": total_long,
        "short_semantics": "absolute_contract_count",
        "timestamp_semantics": "source_event_and_publication_localized_from_Europe/Moscow_to_UTC",
        "fiz_yur_alignment": (
            "latest_exact_shared_source_event_ts_and_sess_id_after_max_seqnum_revision_resolution"
        ),
    }


def _load_current_incremental(
    root: Path, *, expected_trade_date: str
) -> tuple[dict[str, object], dict[str, object], Path, dict[str, object]]:
    pointer_path = incremental.incremental_pointer_path(root, INSTRUMENT_ID)
    pointer, pointer_snapshot = incremental._load_json_snapshot(
        pointer_path, "FUTOI incremental pointer"
    )
    if (
        pointer.get("schema_version") != incremental.SCHEMA_VERSION
        or pointer.get("producer") != incremental.PRODUCER_ID
    ):
        _fail("FUTOI incremental pointer identity mismatch")
    if pointer.get("acceptance_status") != "pass" or pointer.get("quality_status") != "pass":
        _fail("FUTOI incremental pointer is not accepted")
    if (
        pointer.get("dataset_id") != incremental.DATASET_ID
        or pointer.get("instrument_id") != INSTRUMENT_ID
        or pointer.get("source_id") != SOURCE_ID
    ):
        _fail("FUTOI incremental pointer scope mismatch")
    if pointer.get("cumulative_till") != expected_trade_date:
        _fail("FUTOI incremental pointer is not fresh through expected latest trading date")

    manifest_path = incremental._resolve_ref(
        root, pointer.get("manifest_ref"), "FUTOI incremental manifest_ref"
    )
    manifest, manifest_snapshot = incremental._load_json_snapshot(
        manifest_path, "FUTOI incremental manifest"
    )
    if pointer.get("manifest_sha256") != manifest_snapshot["sha256"]:
        _fail("FUTOI incremental manifest SHA mismatch")
    if manifest.get("acceptance_status") != "pass" or manifest.get("quality_status") != "pass":
        _fail("FUTOI incremental manifest is not accepted")
    partitions = manifest.get("partitions")
    if isinstance(partitions, (str, bytes)) or not isinstance(partitions, Sequence) or not partitions:
        _fail("FUTOI incremental manifest has no accepted partitions")
    records = [value for value in partitions if isinstance(value, Mapping)]
    matching = [value for value in records if value.get("trade_date") == expected_trade_date]
    if len(matching) != 1:
        _fail("FUTOI incremental manifest must contain exactly one latest accepted partition")
    record = dict(matching[0])
    partition_path = incremental._resolve_ref(
        root, record.get("accepted_partition_ref"), "FUTOI accepted partition_ref"
    )
    expected_sha = str(record.get("sha256") or "").strip().lower()
    if len(expected_sha) != 64 or _sha256_file(partition_path) != expected_sha:
        _fail("FUTOI latest accepted partition SHA mismatch")
    return (
        pointer,
        manifest,
        partition_path,
        {
            "pointer_sha256": pointer_snapshot["sha256"],
            "manifest_sha256": manifest_snapshot["sha256"],
            "partition_sha256": expected_sha,
        },
    )


def run_refresh(*, through_date: str, run_id: str, timeout: float = 60.0) -> dict[str, object]:
    checked_through = _iso_date(through_date, "through_date")
    checked_run = _safe_token(run_id, "run_id")
    current_moscow_date = pd.Timestamp.now(tz=MARKET_TZ).date()
    if date.fromisoformat(checked_through) >= current_moscow_date:
        _fail("through_date must be a completed Europe/Moscow calendar date")
    target_trade_date = _latest_completed_trading_date(checked_through, timeout=timeout)
    root = _data_root()
    parent = incremental._parent_state(root, INSTRUMENT_ID)
    parent_end = _iso_date(parent.get("end"), "parent accepted end")
    if parent_end > target_trade_date:
        _fail("accepted FUTOI incremental state is ahead of requested target trade date")

    refresh: dict[str, object]
    if parent_end < target_trade_date:
        date_start = (date.fromisoformat(parent_end) + timedelta(days=1)).isoformat()
        backfill_run_id = checked_run + "_raw"
        raw = backfill.backfill_range(
            date_start=date_start,
            date_end=target_trade_date,
            instrument_id=INSTRUMENT_ID,
            run_id=backfill_run_id,
            timeout=timeout,
            create_accepted_pointer=False,
            progress_every=0,
        )
        if raw.get("status") != "succeeded" or raw.get("quality_status") != "pass":
            _fail("canonical FUTOI raw refresh did not pass")
        accepted = incremental.accept_incremental_backfill(
            instrument_id=INSTRUMENT_ID,
            backfill_run_id=backfill_run_id,
            date_end=target_trade_date,
        )
        if accepted.get("status") != "accepted":
            _fail("canonical FUTOI incremental acceptance did not pass")
        refresh = {
            "status": "refreshed_and_accepted",
            "date_start": date_start,
            "date_end": target_trade_date,
            "backfill_run_id": backfill_run_id,
            "incremental_partition_count": accepted.get("incremental_partition_count"),
            "incremental_row_count": accepted.get("incremental_row_count"),
        }
    else:
        refresh = {"status": "no_op_already_current", "date_end": target_trade_date}

    pointer, manifest, partition_path, hashes = _load_current_incremental(
        root, expected_trade_date=target_trade_date
    )
    del manifest
    frame = pd.read_parquet(partition_path)
    factual = latest_aligned_factual(frame, expected_trade_date=target_trade_date)
    completed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": "PASS",
        "source_id": SOURCE_ID,
        "instrument_id": INSTRUMENT_ID,
        "run_id": checked_run,
        "through_date": checked_through,
        "expected_latest_completed_trade_date": target_trade_date,
        "data_as_of": factual["snapshot_ts"],
        "last_success_at": completed_at,
        "freshness": {
            "status": "FRESH",
            "policy": (
                "latest_accepted_trade_date_must_equal_canonical_latest_completed_moex_futures_trade_date"
            ),
            "expected_trade_date": target_trade_date,
            "accepted_trade_date": factual["trade_date"],
        },
        "quality_status": "PASS",
        "acceptance_status": "PASS",
        "factual": factual,
        "provenance": {
            "incremental_pointer_ref": _rooted_ref(
                root, incremental.incremental_pointer_path(root, INSTRUMENT_ID)
            ),
            "incremental_pointer_sha256": hashes["pointer_sha256"],
            "incremental_manifest_ref": _rooted_ref(
                root,
                incremental._resolve_ref(root, pointer.get("manifest_ref"), "manifest_ref"),
            ),
            "incremental_manifest_sha256": hashes["manifest_sha256"],
            "accepted_partition_ref": _rooted_ref(root, partition_path),
            "accepted_partition_sha256": hashes["partition_sha256"],
            "source_contract_ref": materializer.SOURCE_CONTRACT_REF,
            "raw_contract_ref": materializer.RAW_CONTRACT_REF,
            "incremental_acceptance_contract_ref": incremental.CONTRACT_REF,
        },
        "refresh": refresh,
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "stage5_full_mode_required": False,
        "stage5_pointer_promotion_performed": False,
        "historical_pit_research_ready_claimed": False,
    }
    _atomic_json(_current_path(root), payload)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh and validate canonical Si FUTOI as factual-only live context without "
            "Stage 5 full-mode promotion."
        )
    )
    parser.add_argument("--through-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        materializer.load_env_file(args.env_file)
        result = run_refresh(
            through_date=args.through_date,
            run_id=args.run_id,
            timeout=args.timeout,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "project": PROJECT,
                    "schema_version": SCHEMA_VERSION,
                    "status": "BLOCKED",
                    "error": str(exc),
                    "factual_authority": False,
                    "directional_authority": False,
                    "action_authority": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

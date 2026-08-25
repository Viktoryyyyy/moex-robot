from __future__ import annotations

import hashlib
import json
import math
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

import pandas as pd

DATASET_ID: Final[str] = "futures_futoi_eod"
SOURCE_DATASET_ID: Final[str] = "futures_futoi_raw"
SOURCE_ID: Final[str] = "moex_algopack_futoi"
MARKET_TZ: Final[str] = "Europe/Moscow"
MANDATORY_INSTRUMENTS: Final[frozenset[str]] = frozenset({"si_futures_family", "cr_futures_family"})
GROUPS: Final[frozenset[str]] = frozenset({"FIZ", "YUR"})
POSITION_FIELDS: Final[tuple[str, ...]] = ("pos", "pos_long", "pos_short", "pos_long_num", "pos_short_num")
RAW_REQUIRED: Final[tuple[str, ...]] = (
    "instrument_id", "trade_date", "ts", "systime", "sess_id", "seqnum", "clgroup",
    *POSITION_FIELDS, "availability_ts_utc",
)
ACCEPTED_MANIFEST_SCHEMA: Final[str] = "futures_raw_history_accepted_manifest.v1"
ACCEPTED_MANIFEST_DATASET: Final[str] = "futures_raw_history_accepted_manifest"
ACCEPTED_MANIFEST_PRODUCER: Final[str] = "moex_data.futures.stage2_raw_history_promotion.v1"
ROOT_PREFIX: Final[str] = "${MOEX_DATA_ROOT}/"


class FutoiEodError(ValueError):
    pass


@dataclass(frozen=True)
class AcceptedHistoryScope:
    accepted_dates: tuple[str, ...]
    missing_requested_dates: tuple[str, ...]
    pointer_ref: str
    manifest_ref: str
    acceptance_report_ref: str
    acceptance_run_id: str
    partition_dates_sha256: str


def _fail(message: str) -> None:
    raise FutoiEodError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be an explicit safe token")
    return text


def _iso_date(value: object, field: str) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise FutoiEodError(field + " must be YYYY-MM-DD") from exc


def _date_range(start: str, end: str) -> list[str]:
    a = date.fromisoformat(start)
    b = date.fromisoformat(end)
    return [(a + timedelta(days=n)).isoformat() for n in range((b - a).days + 1)]


def _date_set_sha256(values: list[str] | tuple[str, ...]) -> str:
    payload = (("\n".join(values) + "\n") if values else "").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _rooted_ref(root: Path, path: Path) -> str:
    try:
        relative = path.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise FutoiEodError("artifact must be inside MOEX_DATA_ROOT") from exc
    return ROOT_PREFIX + relative.as_posix()


def _expand_root_ref(root: Path, value: object, field: str) -> Path:
    text = str(value or "").strip()
    if not text.startswith(ROOT_PREFIX):
        _fail(field + " must be a ${MOEX_DATA_ROOT} rooted reference")
    relative = text[len(ROOT_PREFIX):]
    if not relative or relative.startswith("/") or ".." in Path(relative).parts:
        _fail(field + " contains invalid rooted path")
    candidate = (root / relative).resolve(strict=True)
    try:
        candidate.relative_to(root.resolve(strict=True))
    except ValueError as exc:
        raise FutoiEodError(field + " escaped MOEX_DATA_ROOT") from exc
    if not candidate.is_file() or candidate.is_symlink():
        _fail(field + " must resolve to a regular non-symlink file")
    return candidate


def _load_json(path: Path, field: str) -> dict[str, object]:
    if not path.is_file() or path.is_symlink():
        _fail(field + " must be a regular non-symlink JSON file")
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FutoiEodError(field + " is not valid JSON: " + str(exc)) from exc
    if not isinstance(values, dict):
        _fail(field + " must be a JSON object")
    return values


def accepted_raw_pointer_path(data_root: Path, instrument_id: str) -> Path:
    return (
        data_root
        / "state" / "datasets"
        / ("dataset_id=" + SOURCE_DATASET_ID)
        / ("instrument_id=" + instrument_id)
        / "current_accepted_manifest.json"
    )


def _accepted_history_scope(root: Path, instrument_id: str, start_date: str, end_date: str) -> AcceptedHistoryScope:
    pointer_path = accepted_raw_pointer_path(root, instrument_id)
    pointer = _load_json(pointer_path, "accepted raw pointer")
    if pointer.get("dataset_id") != SOURCE_DATASET_ID or pointer.get("instrument_id") != instrument_id:
        _fail("accepted raw pointer identity mismatch")
    if pointer.get("quality_status") != "pass" or pointer.get("acceptance_status") != "pass":
        _fail("accepted raw pointer is not PASS")
    if pointer.get("promotion_basis") != "raw_history_acceptance":
        _fail("accepted raw pointer promotion_basis mismatch")
    acceptance_run_id = _safe_token(pointer.get("run_id"), "accepted raw pointer run_id")

    manifest_path = _expand_root_ref(root, pointer.get("manifest_ref"), "accepted raw manifest_ref")
    manifest = _load_json(manifest_path, "accepted raw manifest")
    if manifest.get("schema_version") != ACCEPTED_MANIFEST_SCHEMA:
        _fail("accepted raw manifest schema mismatch")
    if manifest.get("producer") != ACCEPTED_MANIFEST_PRODUCER:
        _fail("accepted raw manifest producer mismatch")
    if manifest.get("dataset_id") != ACCEPTED_MANIFEST_DATASET:
        _fail("accepted raw manifest dataset_id mismatch")
    if manifest.get("target_dataset_id") != SOURCE_DATASET_ID or manifest.get("instrument_id") != instrument_id:
        _fail("accepted raw manifest target identity mismatch")
    if manifest.get("acceptance_run_id") != acceptance_run_id:
        _fail("accepted raw manifest acceptance_run_id mismatch")
    if manifest.get("source_id") != SOURCE_ID or manifest.get("acceptance_status") != "pass":
        _fail("accepted raw manifest source/status mismatch")
    if manifest.get("network_access_used") is not False or manifest.get("historical_backfill_used") is not False:
        _fail("accepted raw manifest execution-boundary evidence mismatch")

    accepted_start = _iso_date(manifest.get("requested_from"), "accepted raw requested_from")
    accepted_end = _iso_date(manifest.get("requested_till"), "accepted raw requested_till")
    if start_date < accepted_start or end_date > accepted_end:
        _fail("requested Stage 5 range is outside accepted raw-history range")

    missing = manifest.get("missing_partition_dates")
    if not isinstance(missing, list):
        _fail("accepted raw missing_partition_dates must be a list")
    checked_missing = [_iso_date(value, "accepted raw missing_partition_dates") for value in missing]
    if checked_missing != sorted(checked_missing) or len(checked_missing) != len(set(checked_missing)):
        _fail("accepted raw missing_partition_dates must be sorted and unique")
    full_dates = _date_range(accepted_start, accepted_end)
    if not set(checked_missing).issubset(set(full_dates)):
        _fail("accepted raw missing_partition_dates contains date outside accepted range")
    present_dates = [value for value in full_dates if value not in set(checked_missing)]
    if manifest.get("partition_count") != len(present_dates):
        _fail("accepted raw partition_count does not match pinned date set")
    if manifest.get("calendar_missing_partition_count") != len(checked_missing):
        _fail("accepted raw calendar missing count mismatch")
    partition_digest = str(manifest.get("partition_dates_sha256") or "").strip().lower()
    missing_digest = str(manifest.get("missing_dates_sha256") or "").strip().lower()
    if partition_digest != _date_set_sha256(present_dates):
        _fail("accepted raw partition date digest mismatch")
    if missing_digest != _date_set_sha256(checked_missing):
        _fail("accepted raw missing date digest mismatch")

    acceptance_report_path = _expand_root_ref(root, manifest.get("acceptance_report_ref"), "accepted raw acceptance_report_ref")
    expected_report_sha = str(manifest.get("acceptance_report_sha256") or "").strip().lower()
    if len(expected_report_sha) != 64 or hashlib.sha256(acceptance_report_path.read_bytes()).hexdigest() != expected_report_sha:
        _fail("accepted raw acceptance report SHA-256 mismatch")
    pointer_quality_ref = str(pointer.get("quality_report_ref") or "").strip()
    pointer_acceptance_ref = str(pointer.get("acceptance_report_ref") or "").strip()
    manifest_acceptance_ref = str(manifest.get("acceptance_report_ref") or "").strip()
    if pointer_quality_ref != manifest_acceptance_ref or pointer_acceptance_ref != manifest_acceptance_ref:
        _fail("accepted raw pointer acceptance-report binding mismatch")

    requested_dates = _date_range(start_date, end_date)
    accepted_present = set(present_dates)
    accepted_dates = tuple(value for value in requested_dates if value in accepted_present)
    missing_requested = tuple(value for value in requested_dates if value not in accepted_present)
    if not accepted_dates:
        _fail("accepted raw history contains no partitions in requested Stage 5 range")
    return AcceptedHistoryScope(
        accepted_dates=accepted_dates,
        missing_requested_dates=missing_requested,
        pointer_ref=_rooted_ref(root, pointer_path),
        manifest_ref=_rooted_ref(root, manifest_path),
        acceptance_report_ref=_rooted_ref(root, acceptance_report_path),
        acceptance_run_id=acceptance_run_id,
        partition_dates_sha256=partition_digest,
    )


def raw_partition_path(data_root: Path, instrument_id: str, trade_date: str) -> Path:
    return (
        data_root
        / "market" / "supplementary"
        / ("dataset_id=" + SOURCE_DATASET_ID)
        / ("instrument_id=" + instrument_id)
        / ("trade_date=" + trade_date)
        / ("source=" + SOURCE_ID)
        / "part.parquet"
    )


def _localize_exchange(series: pd.Series, field: str) -> tuple[pd.Series, pd.Series]:
    parsed = pd.to_datetime(series, errors="coerce")
    if bool(parsed.isna().any()):
        _fail("invalid timestamp values: " + field)
    tz = getattr(parsed.dt, "tz", None)
    try:
        if tz is None:
            local = parsed.dt.tz_localize(MARKET_TZ, ambiguous="raise", nonexistent="raise")
        else:
            local = parsed.dt.tz_convert(MARKET_TZ)
    except Exception as exc:
        raise FutoiEodError("cannot localize timestamp field " + field + ": " + str(exc)) from exc
    return local, local.dt.tz_convert("UTC")


def _coerce_integral(series: pd.Series, field: str, *, nonnegative: bool | None = None) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if bool(numeric.isna().any()):
        _fail("nonnumeric required field: " + field)
    values = numeric.astype(float)
    if not values.map(math.isfinite).all():
        _fail("nonfinite required field: " + field)
    rounded = values.round()
    if not bool((values == rounded).all()):
        _fail("nonintegral required field: " + field)
    if nonnegative is True and bool((rounded < 0).any()):
        _fail("negative required field: " + field)
    return rounded.astype("int64")


def _validate_raw(frame: pd.DataFrame, *, instrument_id: str, trade_date: str) -> pd.DataFrame:
    if frame.empty:
        _fail("raw FUTOI partition is empty")
    missing = [field for field in RAW_REQUIRED if field not in frame.columns]
    if missing:
        _fail("raw FUTOI missing required columns: " + ",".join(missing))
    work = frame.copy()
    if set(work["instrument_id"].astype(str).str.strip().unique()) != {instrument_id}:
        _fail("raw FUTOI instrument_id mismatch")
    if set(work["trade_date"].astype(str).str.strip().unique()) != {trade_date}:
        _fail("raw FUTOI trade_date mismatch")
    groups = set(work["clgroup"].astype(str).str.upper().str.strip().unique())
    if not groups.issubset(GROUPS) or groups != GROUPS:
        _fail("raw FUTOI must contain exactly supported FIZ/YUR groups")

    work["clgroup"] = work["clgroup"].astype(str).str.upper().str.strip()
    work["sess_id"] = _coerce_integral(work["sess_id"], "sess_id")
    work["seqnum"] = _coerce_integral(work["seqnum"], "seqnum")
    work["pos"] = _coerce_integral(work["pos"], "pos")
    work["pos_long"] = _coerce_integral(work["pos_long"], "pos_long", nonnegative=True)
    work["pos_short"] = _coerce_integral(work["pos_short"], "pos_short")
    if bool((work["pos_short"] > 0).any()):
        _fail("pos_short must be nonpositive")
    work["pos_long_num"] = _coerce_integral(work["pos_long_num"], "pos_long_num", nonnegative=True)
    work["pos_short_num"] = _coerce_integral(work["pos_short_num"], "pos_short_num", nonnegative=True)

    ts_local, ts_utc = _localize_exchange(work["ts"], "ts")
    systime_local, systime_utc = _localize_exchange(work["systime"], "systime")
    availability = pd.to_datetime(work["availability_ts_utc"], errors="coerce", utc=True)
    if bool(availability.isna().any()):
        _fail("invalid availability_ts_utc")
    work["_ts_local"] = ts_local
    work["_ts_utc"] = ts_utc
    work["_systime_utc"] = systime_utc
    work["_availability_utc"] = availability
    if not bool(work["_ts_local"].dt.date.astype(str).eq(trade_date).all()):
        _fail("raw event timestamp date mismatch")
    if bool((systime_local < ts_local).any()):
        _fail("raw systime precedes event timestamp")
    return work.reset_index(drop=True)


def _resolve_revisions(work: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    selected: list[pd.Series] = []
    key = ["trade_date", "_ts_utc", "instrument_id", "clgroup"]
    for _, group in work.groupby(key, sort=False, dropna=False):
        sessions = group["sess_id"].drop_duplicates().tolist()
        if len(sessions) != 1:
            _fail("ambiguous multi-session revision for same FUTOI analytical key")
        max_seq = int(group["seqnum"].max())
        winners = group.loc[group["seqnum"].eq(max_seq)]
        if len(winners.index) != 1:
            _fail("ambiguous max-seqnum revision for same FUTOI analytical key")
        selected.append(winners.iloc[0])
    result = pd.DataFrame(selected).sort_values(["_ts_utc", "clgroup"]).reset_index(drop=True)
    return result, int(len(work.index) - len(result.index))


def _average(position: int, count: int, field: str) -> float | None:
    if count < 0:
        _fail("participant count is negative: " + field)
    if count == 0:
        if position != 0:
            _fail("nonzero position with zero participant count: " + field)
        return None
    return float(position) / float(count)


def _single_eod_row(frame: pd.DataFrame, *, instrument_id: str, trade_date: str, source_ref: str) -> dict[str, object]:
    work = _validate_raw(frame, instrument_id=instrument_id, trade_date=trade_date)
    resolved, revisions_dropped = _resolve_revisions(work)
    final_ts = resolved["_ts_utc"].max()
    final = resolved.loc[resolved["_ts_utc"].eq(final_ts)].copy()
    if len(final.index) != 2 or set(final["clgroup"].tolist()) != GROUPS:
        _fail("incomplete final FUTOI snapshot: final event must contain exactly FIZ and YUR")
    by_group = {str(row["clgroup"]): row for _, row in final.iterrows()}
    phys = by_group["FIZ"]
    legal = by_group["YUR"]

    phys_net = int(phys["pos"])
    legal_net = int(legal["pos"])
    phys_long = int(phys["pos_long"])
    legal_long = int(legal["pos_long"])
    phys_short_abs = abs(int(phys["pos_short"]))
    legal_short_abs = abs(int(legal["pos_short"]))
    phys_long_num = int(phys["pos_long_num"])
    phys_short_num = int(phys["pos_short_num"])
    legal_long_num = int(legal["pos_long_num"])
    legal_short_num = int(legal["pos_short_num"])

    total_oi = phys_long + legal_long
    total_short_abs = phys_short_abs + legal_short_abs
    if total_oi <= 0:
        _fail("total_open_interest must be positive")
    if total_oi != total_short_abs:
        _fail("FUTOI total longs do not equal total shorts")
    if phys_net + legal_net != 0:
        _fail("FUTOI phys/legal net positions do not balance to zero")
    if phys_net != phys_long - phys_short_abs or legal_net != legal_long - legal_short_abs:
        _fail("FUTOI source pos does not equal long minus short")

    phys_gross = phys_long + phys_short_abs
    legal_gross = legal_long + legal_short_abs
    two_sided_oi = 2.0 * float(total_oi)
    snapshot_local = phys["_ts_local"]
    if snapshot_local != legal["_ts_local"]:
        _fail("FUTOI FIZ/YUR final snapshot local timestamps differ")
    availability = max(phys["_availability_utc"], legal["_availability_utc"])

    return {
        "instrument_id": instrument_id,
        "trade_date": trade_date,
        "snapshot_ts_msk": snapshot_local.isoformat(),
        "snapshot_ts_utc": final_ts.isoformat(),
        "availability_ts_utc": availability.isoformat(),
        "phys_sess_id": int(phys["sess_id"]),
        "phys_seqnum": int(phys["seqnum"]),
        "phys_systime_utc": phys["_systime_utc"].isoformat(),
        "legal_sess_id": int(legal["sess_id"]),
        "legal_seqnum": int(legal["seqnum"]),
        "legal_systime_utc": legal["_systime_utc"].isoformat(),
        "phys_net": phys_net,
        "phys_long": phys_long,
        "phys_short_abs": phys_short_abs,
        "phys_long_num": phys_long_num,
        "phys_short_num": phys_short_num,
        "legal_net": legal_net,
        "legal_long": legal_long,
        "legal_short_abs": legal_short_abs,
        "legal_long_num": legal_long_num,
        "legal_short_num": legal_short_num,
        "total_open_interest": total_oi,
        "total_short_abs": total_short_abs,
        "phys_gross": phys_gross,
        "legal_gross": legal_gross,
        "phys_long_share_of_oi": phys_long / total_oi,
        "phys_short_share_of_oi": phys_short_abs / total_oi,
        "phys_net_share_of_oi": phys_net / total_oi,
        "legal_long_share_of_oi": legal_long / total_oi,
        "legal_short_share_of_oi": legal_short_abs / total_oi,
        "legal_net_share_of_oi": legal_net / total_oi,
        "phys_gross_share_of_two_sided_oi": phys_gross / two_sided_oi,
        "legal_gross_share_of_two_sided_oi": legal_gross / two_sided_oi,
        "phys_avg_long_per_participant": _average(phys_long, phys_long_num, "phys_long"),
        "phys_avg_short_per_participant": _average(phys_short_abs, phys_short_num, "phys_short"),
        "legal_avg_long_per_participant": _average(legal_long, legal_long_num, "legal_long"),
        "legal_avg_short_per_participant": _average(legal_short_abs, legal_short_num, "legal_short"),
        "source_row_count": int(len(work.index)),
        "source_revision_rows_dropped": revisions_dropped,
        "source_partition_ref": source_ref,
    }


def build_eod_history(*, data_root: str | Path, instrument_id: str, start_date: str, end_date: str) -> tuple[pd.DataFrame, list[str], AcceptedHistoryScope]:
    root = Path(data_root).resolve()
    instrument = _safe_token(instrument_id, "instrument_id")
    if instrument not in MANDATORY_INSTRUMENTS:
        _fail("Stage 5 mandatory scope is Si/CR only")
    start = _iso_date(start_date, "start_date")
    end = _iso_date(end_date, "end_date")
    if start > end:
        _fail("start_date must be <= end_date")
    scope = _accepted_history_scope(root, instrument, start, end)
    rows: list[dict[str, object]] = []
    inputs: list[str] = []
    for trade_date in scope.accepted_dates:
        partition = raw_partition_path(root, instrument, trade_date)
        if not partition.is_file() or partition.is_symlink():
            _fail("accepted raw FUTOI partition is missing or not a regular file: " + trade_date)
        try:
            resolved = partition.resolve(strict=True)
            rel = resolved.relative_to(root.resolve(strict=True))
        except (OSError, ValueError) as exc:
            raise FutoiEodError("accepted raw partition escaped data root") from exc
        frame = pd.read_parquet(resolved)
        source_ref = ROOT_PREFIX + rel.as_posix()
        rows.append(_single_eod_row(frame, instrument_id=instrument, trade_date=trade_date, source_ref=source_ref))
        inputs.append(source_ref)
    result = pd.DataFrame(rows).sort_values("trade_date").reset_index(drop=True)
    if result.empty or result.duplicated(subset=["instrument_id", "trade_date"]).any():
        _fail("derived EOD is empty or contains duplicate instrument/trade_date")
    return result, inputs, scope


def _atomic_json(path: Path, values: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        temp = Path(handle.name)
    temp.replace(path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name("." + path.name + ".tmp")
    try:
        frame.to_parquet(temp, index=False)
        temp.replace(path)
    finally:
        if temp.exists():
            temp.unlink()


def materialize_eod_history(
    *,
    data_root: str | Path,
    output_root: str | Path,
    instrument_id: str,
    start_date: str,
    end_date: str,
    run_id: str,
) -> dict[str, object]:
    checked_run = _safe_token(run_id, "run_id")
    checked_instrument = _safe_token(instrument_id, "instrument_id")
    frame, inputs, scope = build_eod_history(
        data_root=data_root,
        instrument_id=checked_instrument,
        start_date=start_date,
        end_date=end_date,
    )
    out_root = Path(output_root).resolve()
    base = out_root / "market" / "derived" / ("dataset_id=" + DATASET_ID) / ("instrument_id=" + checked_instrument)
    partition = base / ("run_id=" + checked_run) / "part.parquet"
    manifest = out_root / "state" / "refresh" / ("dataset_id=" + DATASET_ID) / ("run_id=" + checked_run) / ("instrument_id=" + checked_instrument) / "manifest.json"
    quality = out_root / "state" / "quality" / ("dataset_id=" + DATASET_ID) / ("run_id=" + checked_run) / ("instrument_id=" + checked_instrument) / "quality_report.json"
    for target in (partition, manifest, quality):
        if target.exists():
            _fail("immutable Stage 5 EOD target already exists")

    quality_values = {
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "run_id": checked_run,
        "quality_status": "pass",
        "row_count": int(len(frame.index)),
        "min_trade_date": str(frame["trade_date"].min()),
        "max_trade_date": str(frame["trade_date"].max()),
        "duplicate_identity_count": int(frame.duplicated(subset=["instrument_id", "trade_date"]).sum()),
        "input_partition_count": len(inputs),
        "source_revision_rows_dropped": int(frame["source_revision_rows_dropped"].sum()),
        "missing_calendar_date_count": len(scope.missing_requested_dates),
        "accepted_raw_history_required": True,
        "accepted_raw_history_run_id": scope.acceptance_run_id,
        "accepted_raw_partition_dates_sha256": scope.partition_dates_sha256,
        "root_aggregate_semantics": True,
        "front_next_split_claimed": False,
        "historical_pit_research_ready_claimed": False,
    }
    manifest_values = {
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "run_id": checked_run,
        "row_count": int(len(frame.index)),
        "quality_status": "pass",
        "requested_start_date": _iso_date(start_date, "start_date"),
        "requested_end_date": _iso_date(end_date, "end_date"),
        "partition_path": partition.as_posix(),
        "quality_report_path": quality.as_posix(),
        "input_partition_count": len(inputs),
        "input_partition_refs": inputs,
        "missing_calendar_dates": list(scope.missing_requested_dates),
        "accepted_raw_pointer_ref": scope.pointer_ref,
        "accepted_raw_manifest_ref": scope.manifest_ref,
        "accepted_raw_acceptance_report_ref": scope.acceptance_report_ref,
        "accepted_raw_history_run_id": scope.acceptance_run_id,
        "accepted_raw_partition_dates_sha256": scope.partition_dates_sha256,
        "producer": "moex_data.futures.materialize_futoi_eod.v1",
        "network_calls_used": False,
        "latest_autodetect_used": False,
        "revision_policy": "same_analytical_key_single_sess_id_then_max_seqnum",
        "snapshot_policy": "max_resolved_ts_requires_FIZ_and_YUR",
        "build_ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    _atomic_parquet(partition, frame)
    _atomic_json(quality, quality_values)
    _atomic_json(manifest, manifest_values)
    return {
        "status": "succeeded",
        "dataset_id": DATASET_ID,
        "instrument_id": checked_instrument,
        "run_id": checked_run,
        "row_count": int(len(frame.index)),
        "quality_status": "pass",
        "partition_path": partition.as_posix(),
        "manifest_path": manifest.as_posix(),
        "quality_report_path": quality.as_posix(),
        "input_partition_count": len(inputs),
        "min_trade_date": str(frame["trade_date"].min()),
        "max_trade_date": str(frame["trade_date"].max()),
        "accepted_raw_pointer_ref": scope.pointer_ref,
        "accepted_raw_manifest_ref": scope.manifest_ref,
        "accepted_raw_history_run_id": scope.acceptance_run_id,
        "network_calls_used": False,
        "latest_autodetect_used": False,
        "front_next_split_claimed": False,
    }

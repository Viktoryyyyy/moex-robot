from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

MARKET_TZ = "Europe/Moscow"
GROUPS = frozenset({"FIZ", "YUR"})
RAW_REQUIRED = (
    "instrument_id",
    "trade_date",
    "ts",
    "systime",
    "sess_id",
    "seqnum",
    "clgroup",
    "pos",
    "pos_long",
    "pos_short",
    "pos_long_num",
    "pos_short_num",
    "availability_ts_utc",
)
EOD_FIELDS = (
    "instrument_id", "trade_date", "snapshot_ts_msk", "snapshot_ts_utc", "availability_ts_utc",
    "phys_sess_id", "phys_seqnum", "phys_systime_utc", "legal_sess_id", "legal_seqnum", "legal_systime_utc",
    "phys_net", "phys_long", "phys_short_abs", "phys_long_num", "phys_short_num",
    "legal_net", "legal_long", "legal_short_abs", "legal_long_num", "legal_short_num",
    "total_open_interest", "total_short_abs", "phys_gross", "legal_gross",
    "phys_long_share_of_oi", "phys_short_share_of_oi", "phys_net_share_of_oi",
    "legal_long_share_of_oi", "legal_short_share_of_oi", "legal_net_share_of_oi",
    "phys_gross_share_of_two_sided_oi", "legal_gross_share_of_two_sided_oi",
    "phys_avg_long_per_participant", "phys_avg_short_per_participant",
    "legal_avg_long_per_participant", "legal_avg_short_per_participant",
    "source_row_count", "source_revision_rows_dropped", "source_partition_ref",
    "source_canonical_partition_ref", "source_frozen_partition_sha256",
)


class FrozenFutoiEodValidationError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FrozenFutoiEodValidationError(message)


def _integral(series: pd.Series, field: str, *, nonnegative: bool = False) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if bool(numeric.isna().any()):
        _fail("nonnumeric/null raw field: " + field)
    values = numeric.astype(float)
    if not values.map(math.isfinite).all():
        _fail("nonfinite raw field: " + field)
    rounded = values.round()
    if not np.allclose(values.to_numpy(), rounded.to_numpy(), rtol=0.0, atol=0.0):
        _fail("nonintegral raw field: " + field)
    if nonnegative and bool((rounded < 0).any()):
        _fail("negative raw field: " + field)
    return rounded.astype("int64")


def _localize_exchange(series: pd.Series, field: str) -> tuple[pd.Series, pd.Series]:
    parsed = pd.to_datetime(series, errors="coerce")
    if bool(parsed.isna().any()):
        _fail("invalid raw timestamp: " + field)
    tz = getattr(parsed.dt, "tz", None)
    try:
        if tz is None:
            local = parsed.dt.tz_localize(ZoneInfo(MARKET_TZ), ambiguous="raise", nonexistent="raise")
        else:
            local = parsed.dt.tz_convert(ZoneInfo(MARKET_TZ))
    except Exception as exc:
        raise FrozenFutoiEodValidationError("cannot localize raw timestamp " + field + ": " + str(exc)) from exc
    return local, local.dt.tz_convert("UTC")


def _validate_raw(frame: pd.DataFrame, instrument_id: str, trade_date: str) -> pd.DataFrame:
    if frame.empty:
        _fail("frozen raw FUTOI partition is empty")
    missing = [field for field in RAW_REQUIRED if field not in frame.columns]
    if missing:
        _fail("frozen raw FUTOI missing fields: " + ",".join(missing))
    work = frame.copy()
    if set(work["instrument_id"].astype(str).str.strip()) != {instrument_id}:
        _fail("frozen raw FUTOI instrument mismatch")
    if set(work["trade_date"].astype(str).str.strip()) != {trade_date}:
        _fail("frozen raw FUTOI trade_date mismatch")
    work["clgroup"] = work["clgroup"].astype(str).str.upper().str.strip()
    if set(work["clgroup"]) != GROUPS:
        _fail("frozen raw FUTOI must contain exactly FIZ/YUR")
    work["sess_id"] = _integral(work["sess_id"], "sess_id")
    work["seqnum"] = _integral(work["seqnum"], "seqnum")
    work["pos"] = _integral(work["pos"], "pos")
    work["pos_long"] = _integral(work["pos_long"], "pos_long", nonnegative=True)
    work["pos_short"] = _integral(work["pos_short"], "pos_short")
    if bool((work["pos_short"] > 0).any()):
        _fail("frozen raw pos_short must be nonpositive")
    work["pos_long_num"] = _integral(work["pos_long_num"], "pos_long_num", nonnegative=True)
    work["pos_short_num"] = _integral(work["pos_short_num"], "pos_short_num", nonnegative=True)
    ts_local, ts_utc = _localize_exchange(work["ts"], "ts")
    systime_local, systime_utc = _localize_exchange(work["systime"], "systime")
    availability = pd.to_datetime(work["availability_ts_utc"], errors="coerce", utc=True)
    if bool(availability.isna().any()):
        _fail("invalid frozen raw availability_ts_utc")
    if not bool(ts_local.dt.date.astype(str).eq(trade_date).all()):
        _fail("frozen raw event timestamp date mismatch")
    if bool((systime_local < ts_local).any()):
        _fail("frozen raw systime precedes event timestamp")
    work["_ts_local"] = ts_local
    work["_ts_utc"] = ts_utc
    work["_systime_utc"] = systime_utc
    work["_availability_utc"] = availability
    return work.reset_index(drop=True)


def _resolve_revisions(work: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    selected: list[pd.Series] = []
    for _, group in work.groupby(["trade_date", "_ts_utc", "instrument_id", "clgroup"], sort=False, dropna=False):
        sessions = group["sess_id"].drop_duplicates().tolist()
        if len(sessions) != 1:
            _fail("ambiguous multi-session revision in frozen raw FUTOI")
        max_seq = int(group["seqnum"].max())
        winners = group.loc[group["seqnum"].eq(max_seq)]
        if len(winners.index) != 1:
            _fail("ambiguous max-seqnum revision in frozen raw FUTOI")
        selected.append(winners.iloc[0])
    resolved = pd.DataFrame(selected).sort_values(["_ts_utc", "clgroup"]).reset_index(drop=True)
    return resolved, int(len(work.index) - len(resolved.index))


def _average(position: int, count: int, field: str) -> float | None:
    if count < 0:
        _fail("negative participant count: " + field)
    if count == 0:
        if position != 0:
            _fail("nonzero position with zero participant count: " + field)
        return None
    return float(position) / float(count)


def reconstruct_eod_row(
    frame: pd.DataFrame,
    *,
    instrument_id: str,
    trade_date: str,
    frozen_partition_ref: str,
    canonical_source_ref: str,
    frozen_sha256: str,
) -> dict[str, object]:
    work = _validate_raw(frame, instrument_id, trade_date)
    resolved, revisions_dropped = _resolve_revisions(work)
    final_ts = resolved["_ts_utc"].max()
    final = resolved.loc[resolved["_ts_utc"].eq(final_ts)].copy()
    if len(final.index) != 2 or set(final["clgroup"]) != GROUPS:
        _fail("final frozen raw event must contain exactly FIZ and YUR")
    by_group = {str(row["clgroup"]): row for _, row in final.iterrows()}
    phys = by_group["FIZ"]
    legal = by_group["YUR"]
    if phys["_ts_local"] != legal["_ts_local"]:
        _fail("FIZ/YUR final snapshot local timestamps differ")

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
    if total_oi <= 0 or total_oi != total_short_abs:
        _fail("frozen raw FUTOI long/short OI balance mismatch")
    if phys_net + legal_net != 0:
        _fail("frozen raw FUTOI net balance mismatch")
    if phys_net != phys_long - phys_short_abs or legal_net != legal_long - legal_short_abs:
        _fail("frozen raw FUTOI source pos mismatch")
    phys_gross = phys_long + phys_short_abs
    legal_gross = legal_long + legal_short_abs
    two_sided_oi = 2.0 * float(total_oi)
    availability = max(phys["_availability_utc"], legal["_availability_utc"])

    return {
        "instrument_id": instrument_id,
        "trade_date": trade_date,
        "snapshot_ts_msk": phys["_ts_local"].isoformat(),
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
        "source_partition_ref": frozen_partition_ref,
        "source_canonical_partition_ref": canonical_source_ref,
        "source_frozen_partition_sha256": frozen_sha256,
    }


def compare_candidate_row(candidate: Mapping[str, object], expected: Mapping[str, object]) -> None:
    timestamp_fields = {"snapshot_ts_msk", "snapshot_ts_utc", "availability_ts_utc", "phys_systime_utc", "legal_systime_utc"}
    float_fields = {
        "phys_long_share_of_oi", "phys_short_share_of_oi", "phys_net_share_of_oi",
        "legal_long_share_of_oi", "legal_short_share_of_oi", "legal_net_share_of_oi",
        "phys_gross_share_of_two_sided_oi", "legal_gross_share_of_two_sided_oi",
        "phys_avg_long_per_participant", "phys_avg_short_per_participant",
        "legal_avg_long_per_participant", "legal_avg_short_per_participant",
    }
    for field in EOD_FIELDS:
        actual = candidate.get(field)
        wanted = expected.get(field)
        if field in timestamp_fields:
            a = pd.Timestamp(actual)
            b = pd.Timestamp(wanted)
            if a.tzinfo is None or b.tzinfo is None or a.tz_convert("UTC") != b.tz_convert("UTC"):
                _fail("EOD frozen reconstruction mismatch: " + field)
            continue
        if field in float_fields:
            a_missing = actual is None or pd.isna(actual)
            b_missing = wanted is None or pd.isna(wanted)
            if a_missing or b_missing:
                if a_missing != b_missing:
                    _fail("EOD frozen reconstruction mismatch: " + field)
                continue
            if not math.isclose(float(actual), float(wanted), rel_tol=1e-10, abs_tol=1e-12):
                _fail("EOD frozen reconstruction mismatch: " + field)
            continue
        if str(actual) != str(wanted):
            _fail("EOD frozen reconstruction mismatch: " + field)


def validate_candidate_partition(
    *,
    eod_path: str | Path,
    records_by_date: Mapping[str, Mapping[str, object]],
    expand_frozen_ref,
) -> dict[str, object]:
    frame = pd.read_parquet(Path(eod_path))
    if len(frame.index) != len(records_by_date):
        _fail("EOD/frozen raw reconstruction row count mismatch")
    rebuilt = 0
    for _, candidate in frame.iterrows():
        trade_date = str(candidate["trade_date"])
        record = records_by_date.get(trade_date)
        if not isinstance(record, Mapping):
            _fail("EOD trade_date missing frozen raw reconstruction input")
        frozen_ref = str(record["frozen_partition_ref"])
        frozen_path = Path(expand_frozen_ref(frozen_ref))
        raw = pd.read_parquet(frozen_path)
        expected = reconstruct_eod_row(
            raw,
            instrument_id=str(candidate["instrument_id"]),
            trade_date=trade_date,
            frozen_partition_ref=frozen_ref,
            canonical_source_ref=str(record["canonical_source_ref"]),
            frozen_sha256=str(record["frozen_sha256"]),
        )
        compare_candidate_row(candidate.to_dict(), expected)
        rebuilt += 1
    return {
        "reconstructed_eod_rows": rebuilt,
        "reconstructed_from_frozen_raw_match": True,
        "independent_from_eod_producer": True,
    }

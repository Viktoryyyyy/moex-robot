from __future__ import annotations

import math
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd

from moex_data import step9_rub_analysis_bundle as step9
from moex_data.futures import futoi_intraday_previous_session_context as session_context
from moex_data.futures import futoi_live_factual_refresh_source_native as source
from moex_data.futures import materialize_futoi_instrument as raw_materializer
from moex_data.futures import materialize_futoi_positioning_features_d1 as positioning_features


PROJECT: Final[str] = "MOEX_Bot"
SCHEMA_VERSION: Final[str] = "futoi_delta_statistics_context.v1"
CONTRACT_REF: Final[str] = "contracts/intelligence/usdrubf_futoi_delta_statistics_context_v1.json"
OBSERVED_DATE_WITNESS_INSTRUMENT_ID: Final[str] = "usdrubf_futures_family"
OBSERVED_DATE_WITNESS_DATASET_ID: Final[str] = "rub_native_ohlcv_htf"
OBSERVED_DATE_WITNESS_TIMEFRAME: Final[str] = "1D"
OBSERVED_DATE_AUTHORITY_SOURCE_ID: Final[str] = "moex_algopack_fo_tradestats_5m"
EOD_DATASET_ID: Final[str] = "futures_futoi_eod"
LAGS: Final[tuple[int, ...]] = (1, 5, 20)
STAT_WINDOWS: Final[tuple[int, ...]] = positioning_features.WINDOWS
STAT_FIELDS: Final[tuple[str, ...]] = (
    "fiz.net_share_of_oi",
    "yur.net_share_of_oi",
    "total_open_interest",
)
DELTA_FIELDS: Final[tuple[str, ...]] = (
    "fiz.long",
    "fiz.short",
    "fiz.net",
    "fiz.long_participants",
    "fiz.short_participants",
    "fiz.net_share_of_oi",
    "yur.long",
    "yur.short",
    "yur.net",
    "yur.long_participants",
    "yur.short_participants",
    "yur.net_share_of_oi",
    "total_open_interest",
)


class FutoiDeltaStatisticsError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiDeltaStatisticsError(message)


def _number(value: object, field: str) -> float:
    if value is None or isinstance(value, bool):
        _fail(field + " is missing")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise FutoiDeltaStatisticsError(field + " must be numeric") from exc
    if not math.isfinite(number):
        _fail(field + " must be finite")
    return number


def _integer(value: object, field: str) -> int:
    number = _number(value, field)
    if not number.is_integer():
        _fail(field + " must be an integer")
    return int(number)


def _spec(*, stage: int, dataset_id: str, instrument_id: str, timeframe: str | None) -> step9.PointerSpec:
    matches = [
        item
        for item in step9.pointer_specs("daily")
        if item.stage == stage
        and item.dataset_id == dataset_id
        and item.instrument_id == instrument_id
        and item.timeframe == timeframe
    ]
    if len(matches) != 1:
        _fail("accepted pointer specification did not resolve uniquely")
    return matches[0]


def _accepted_frame(
    root: Path,
    *,
    spec: step9.PointerSpec,
    as_of: datetime,
) -> tuple[pd.DataFrame, dict[str, object]]:
    block = step9._read_pointer_block(root, spec, as_of)
    pointer_path = step9._pointer_path(root, spec)
    pointer = step9._load_json(pointer_path, spec.block_id + ".pointer")
    partition = step9._resolve_root_ref(
        pointer.get("partition_ref"), spec.block_id + ".partition_ref", root
    )
    frame = pd.read_parquet(partition)
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        _fail(spec.block_id + " accepted partition is empty")
    provenance = block.get("provenance")
    if not isinstance(provenance, Mapping):
        _fail(spec.block_id + " accepted provenance is missing")
    return frame, dict(provenance)


def _observed_witness(
    root: Path,
    *,
    as_of: datetime,
    raw_context: Mapping[str, object],
) -> dict[str, object]:
    spec = _spec(
        stage=7,
        dataset_id=OBSERVED_DATE_WITNESS_DATASET_ID,
        instrument_id=OBSERVED_DATE_WITNESS_INSTRUMENT_ID,
        timeframe=OBSERVED_DATE_WITNESS_TIMEFRAME,
    )
    frame, provenance = _accepted_frame(root, spec=spec, as_of=as_of)
    if "trade_date" not in frame.columns:
        _fail("Stage 7 observed-date witness is missing trade_date")
    parsed = pd.to_datetime(frame["trade_date"], errors="coerce")
    if bool(parsed.isna().any()):
        _fail("Stage 7 observed-date witness contains invalid trade_date")
    dates = sorted(set(parsed.dt.date.astype(str).tolist()))
    if not dates:
        _fail("Stage 7 observed-date witness contains no dates")

    current_raw = raw_context.get("observed_current_trade_date")
    previous_raw = raw_context.get("previous_observed_trade_date")
    current_date = current_raw if isinstance(current_raw, str) and current_raw else None
    previous_date = previous_raw if isinstance(previous_raw, str) and previous_raw else None
    if previous_date is None:
        _fail("previous observed TradeStats date is unavailable")

    combined = list(dates)
    if current_date is not None and current_date not in combined:
        if combined and current_date <= combined[-1]:
            _fail("current observed TradeStats date is absent inside accepted Stage 7 witness history")
        combined.append(current_date)
    combined = sorted(set(combined))

    if current_date is not None:
        if len(combined) < 2 or combined[-1] != current_date or combined[-2] != previous_date:
            _fail("Stage 7 observed-date witness is not aligned to current/previous FUTOI context")
    elif combined[-1] != previous_date:
        _fail("Stage 7 observed-date witness is not aligned to previous FUTOI context")

    return {
        "status": "PASS",
        "authority_source_id": OBSERVED_DATE_AUTHORITY_SOURCE_ID,
        "selection_rule": "ordinal_prior_observed_sessions_from_materialized_AlgoPack_FO_TradeStats_lineage",
        "calendar_dependency": False,
        "weekday_weekend_inference": False,
        "current_observed_trade_date": current_date,
        "previous_observed_trade_date": previous_date,
        "observed_trade_dates": combined,
        "witness_dataset_id": OBSERVED_DATE_WITNESS_DATASET_ID,
        "witness_instrument_id": OBSERVED_DATE_WITNESS_INSTRUMENT_ID,
        "witness_timeframe": OBSERVED_DATE_WITNESS_TIMEFRAME,
        "provenance": provenance,
    }


def _normalized_factual(
    value: Mapping[str, object], *, field: str, raw_schema_version: str = "v1",
) -> dict[str, object]:
    version = source._raw_version(raw_schema_version)
    integer = source._as_int if version == "v2" else _integer
    if version == "v1" and (
        value.get("raw_schema_version", "v1") != "v1"
        or value.get("source_identity_scope") == source.ROOT_IDENTITY_SCOPE
    ):
        _fail("v1 normalizer cannot admit another raw version or root scope")
    if version == "v2" and (
        value.get("raw_schema_version") != "v2"
        or value.get("source_identity_scope") != source.ROOT_IDENTITY_SCOPE
        or value.get("source_ticker") not in source.ROOT_TICKERS.values()
        or "secid" in value
        or not isinstance(value.get("selected_source_records"), Mapping)
        or set(value["selected_source_records"]) != {"FIZ", "YUR"}
    ):
        _fail("v2 normalized factual requires explicit root identity and selected records")
    fiz_raw = value.get("fiz")
    yur_raw = value.get("yur")
    if not isinstance(fiz_raw, Mapping) or not isinstance(yur_raw, Mapping):
        _fail(field + " FIZ/YUR payload is missing")
    oi = integer(value.get("total_open_interest"), field + ".total_open_interest")
    if oi <= 0:
        _fail(field + ".total_open_interest must be positive")

    def side(raw: Mapping[str, object], label: str) -> dict[str, object]:
        long_value = integer(raw.get("long"), field + "." + label + ".long")
        short_value = integer(raw.get("short"), field + "." + label + ".short")
        net_value = integer(raw.get("net"), field + "." + label + ".net")
        long_participants = integer(
            raw.get("long_participants"), field + "." + label + ".long_participants"
        )
        short_participants = integer(
            raw.get("short_participants"), field + "." + label + ".short_participants"
        )
        if min(long_value, short_value, long_participants, short_participants) < 0:
            _fail(field + "." + label + " contains negative absolute position/count")
        if long_value - short_value != net_value:
            _fail(field + "." + label + " net identity failed")
        return {
            "long": long_value,
            "short": short_value,
            "net": net_value,
            "long_participants": long_participants,
            "short_participants": short_participants,
            "net_share_of_oi": net_value / oi,
        }

    fiz = side(fiz_raw, "fiz")
    yur = side(yur_raw, "yur")
    if int(fiz["net"]) + int(yur["net"]) != 0:
        _fail(field + " FIZ/YUR net balance failed")
    if int(fiz["long"]) + int(yur["long"]) != oi:
        _fail(field + " total long OI identity failed")
    if int(fiz["short"]) + int(yur["short"]) != oi:
        _fail(field + " total short OI identity failed")

    result = {
        "trade_date": str(value.get("trade_date") or ""),
        "snapshot_ts": value.get("snapshot_ts"),
        "source_publication_time": value.get("source_publication_time"),
        "availability_ts_utc": value.get("availability_ts_utc"),
        "ingest_ts_utc": value.get("ingest_ts_utc"),
        "fiz": fiz,
        "yur": yur,
        "total_open_interest": oi,
        "net_share_formula": "side.net / total_open_interest",
        "participant_count_semantics": {
            "long_participants": "pos_long_num",
            "short_participants": "pos_short_num",
            "unique_participant_count": None,
            "long_plus_short_must_not_be_interpreted_as_unique_participants": True,
        },
    }

    if version == "v2":
        session = source._as_int(value.get("sess_id"), field + ".sess_id")
        records = deepcopy(value["selected_source_records"])
        for group, record in records.items():
            if not isinstance(record, Mapping) or any(record.get(k) != v for k, v in {
                "trade_date": value.get("trade_date"), "source_ticker": value["source_ticker"],
                "clgroup": group, "snapshot_ts": value.get("snapshot_ts"),
            }.items()):
                _fail("v2 normalized selected-record identity mismatch")
            record_session = source._as_int(record.get("sess_id"), field + "." + group + ".sess_id")
            revision = source._as_int(record.get("seqnum"), field + "." + group + ".seqnum")
            if record_session != session or not all(-(2**63) <= n < 2**63 for n in (session, revision)):
                _fail("v2 normalized selected-record session/range mismatch")
            record["sess_id"], record["seqnum"] = record_session, revision
        result.update(
            raw_schema_version="v2", source_identity_scope=source.ROOT_IDENTITY_SCOPE,
            source_ticker=value["source_ticker"], sess_id=session,
            selected_source_records=records,
        )
    return result


def _context_record(
    record: object,
    *,
    expected_trade_date: str | None,
    role: str,
) -> dict[str, object]:
    if not isinstance(record, Mapping):
        return {
            "status": "UNAVAILABLE",
            "role": role,
            "expected_trade_date": expected_trade_date,
            "factual": None,
            "provenance": None,
            "reason": "context_record_missing",
        }
    factual = record.get("factual")
    exact_fresh = (
        record.get("status") == "FRESH"
        and isinstance(factual, Mapping)
        and factual.get("trade_date") == expected_trade_date
    )
    if not exact_fresh:
        retained_trade_date = (
            str(factual.get("trade_date"))
            if isinstance(factual, Mapping) and factual.get("trade_date")
            else None
        )
        return {
            "status": "UNAVAILABLE",
            "role": role,
            "expected_trade_date": expected_trade_date,
            "source_record_status": record.get("status"),
            "retained_factual_trade_date": retained_trade_date,
            "refresh_error_class": record.get("refresh_error_class"),
            "refresh_error": record.get("refresh_error"),
            "factual": None,
            "retained_provenance": record.get("provenance"),
            "missing_or_stale_must_not_be_interpreted_as_zero_or_neutral": True,
        }
    return {
        "status": "AVAILABLE",
        "role": role,
        "expected_trade_date": expected_trade_date,
        "factual": _normalized_factual(factual, field=role),
        "provenance": record.get("provenance"),
    }


def _eod_factual(row: pd.Series, *, instrument_id: str) -> dict[str, object]:
    if str(row.get("instrument_id") or "") != instrument_id:
        _fail("accepted EOD row instrument mismatch")
    trade_date = str(row.get("trade_date") or "")
    snapshot = row.get("snapshot_ts_utc")
    availability = row.get("availability_ts_utc")
    factual = {
        "trade_date": trade_date,
        "snapshot_ts": str(snapshot) if snapshot is not None else None,
        "source_publication_time": None,
        "availability_ts_utc": str(availability) if availability is not None else None,
        "ingest_ts_utc": None,
        "fiz": {
            "long": row.get("phys_long"),
            "short": row.get("phys_short_abs"),
            "net": row.get("phys_net"),
            "long_participants": row.get("phys_long_num"),
            "short_participants": row.get("phys_short_num"),
        },
        "yur": {
            "long": row.get("legal_long"),
            "short": row.get("legal_short_abs"),
            "net": row.get("legal_net"),
            "long_participants": row.get("legal_long_num"),
            "short_participants": row.get("legal_short_num"),
        },
        "total_open_interest": row.get("total_open_interest"),
    }
    return _normalized_factual(factual, field="accepted_eod." + trade_date)


def _accepted_eod(
    root: Path,
    *,
    instrument_id: str,
    as_of: datetime,
) -> tuple[pd.DataFrame, dict[str, object]]:
    spec = _spec(stage=5, dataset_id=EOD_DATASET_ID, instrument_id=instrument_id, timeframe=None)
    frame, provenance = _accepted_frame(root, spec=spec, as_of=as_of)
    required = {
        "instrument_id",
        "trade_date",
        "snapshot_ts_utc",
        "availability_ts_utc",
        "phys_net",
        "phys_long",
        "phys_short_abs",
        "phys_long_num",
        "phys_short_num",
        "legal_net",
        "legal_long",
        "legal_short_abs",
        "legal_long_num",
        "legal_short_num",
        "total_open_interest",
        "phys_net_share_of_oi",
        "legal_net_share_of_oi",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        _fail("accepted EOD history missing fields: " + ",".join(missing))
    work = frame.copy().sort_values("trade_date").reset_index(drop=True)
    if set(work["instrument_id"].astype(str).unique()) != {instrument_id}:
        _fail("accepted EOD history instrument mismatch")
    if work.duplicated(subset=["instrument_id", "trade_date"]).any():
        _fail("accepted EOD history contains duplicate trade_date")

    availability = pd.to_datetime(work["availability_ts_utc"], utc=True, errors="coerce")
    if bool(availability.isna().any()):
        _fail("accepted EOD history contains invalid availability_ts_utc")
    as_of_ts = pd.Timestamp(as_of)
    if as_of_ts.tzinfo is None:
        _fail("as_of must be timezone-aware")
    causal = availability <= as_of_ts.tz_convert("UTC")
    work = work.loc[causal].copy().reset_index(drop=True)
    if work.empty:
        _fail("accepted EOD history has no causally available rows")

    numeric_fields = (
        "phys_net",
        "phys_long",
        "phys_short_abs",
        "phys_long_num",
        "phys_short_num",
        "legal_net",
        "legal_long",
        "legal_short_abs",
        "legal_long_num",
        "legal_short_num",
        "total_open_interest",
        "phys_net_share_of_oi",
        "legal_net_share_of_oi",
    )
    numeric = {field: pd.to_numeric(work[field], errors="coerce") for field in numeric_fields}
    if any(bool(values.isna().any()) for values in numeric.values()):
        _fail("accepted EOD history contains invalid numeric values")
    oi = numeric["total_open_interest"]
    if bool((oi <= 0).any()):
        _fail("accepted EOD history contains invalid total_open_interest")
    for field in (
        "phys_long",
        "phys_short_abs",
        "phys_long_num",
        "phys_short_num",
        "legal_long",
        "legal_short_abs",
        "legal_long_num",
        "legal_short_num",
    ):
        if bool((numeric[field] < 0).any()):
            _fail("accepted EOD history contains negative absolute position/count")
    if not bool((numeric["phys_net"] + numeric["legal_net"] == 0).all()):
        _fail("accepted EOD FIZ/YUR net balance failed")
    if not bool((numeric["phys_long"] - numeric["phys_short_abs"] == numeric["phys_net"]).all()):
        _fail("accepted EOD FIZ net identity failed")
    if not bool((numeric["legal_long"] - numeric["legal_short_abs"] == numeric["legal_net"]).all()):
        _fail("accepted EOD YUR net identity failed")
    if not bool((numeric["phys_long"] + numeric["legal_long"] == oi).all()):
        _fail("accepted EOD total long OI identity failed")
    if not bool((numeric["phys_short_abs"] + numeric["legal_short_abs"] == oi).all()):
        _fail("accepted EOD total short OI identity failed")
    if not np.allclose(
        numeric["phys_net_share_of_oi"].astype(float),
        (numeric["phys_net"] / oi).astype(float),
        rtol=0.0,
        atol=1e-12,
    ):
        _fail("accepted EOD phys_net_share_of_oi formula mismatch")
    if not np.allclose(
        numeric["legal_net_share_of_oi"].astype(float),
        (numeric["legal_net"] / oi).astype(float),
        rtol=0.0,
        atol=1e-12,
    ):
        _fail("accepted EOD legal_net_share_of_oi formula mismatch")
    return work, provenance


def _raw_factual(
    root: Path,
    *,
    instrument_id: str,
    trade_date: str,
    raw_schema_version: str = "v1",
) -> dict[str, object]:
    if source._raw_version(raw_schema_version) == "v2":
        return _raw_root_factual(root, instrument_id=instrument_id, trade_date=trade_date)
    path = raw_materializer._partition_path(trade_date, instrument_id, source.SOURCE_ID)
    if path.is_symlink() or not path.is_file():
        return {
            "status": "UNAVAILABLE",
            "trade_date": trade_date,
            "factual": None,
            "provenance": None,
            "reason": "canonical_raw_partition_missing",
        }
    identity = source.source_identity(instrument_id)
    try:
        factual = source.latest_aligned_factual(
            pd.read_parquet(path),
            expected_trade_date=trade_date,
            expected_instrument_id=instrument_id,
            expected_source_ticker=identity["source_ticker"],
            expected_secid=identity["secid"],
        )
        normalized = _normalized_factual(factual, field="raw." + trade_date)
    except Exception as exc:
        return {
            "status": "UNAVAILABLE",
            "trade_date": trade_date,
            "factual": None,
            "provenance": {
                "raw_partition_ref": source._rooted_ref(root, path),
                "raw_partition_sha256": source._sha256_file(path),
            },
            "reason": "canonical_raw_partition_failed_factual_validation",
            "error_class": exc.__class__.__name__,
            "error": str(exc),
        }
    return {
        "status": "AVAILABLE",
        "trade_date": trade_date,
        "factual": normalized,
        "provenance": {
            "source_state_kind": "validated_existing_canonical_raw_partition",
            "raw_partition_ref": source._rooted_ref(root, path),
            "raw_partition_sha256": source._sha256_file(path),
            "source_id": source.SOURCE_ID,
            "source_ticker": identity["source_ticker"],
            "secid": identity["secid"],
            "factual_validation": "PASS",
        },
    }


def _factual_for_date(
    root: Path,
    *,
    instrument_id: str,
    trade_date: str,
    previous: Mapping[str, object],
    eod: pd.DataFrame,
    eod_provenance: Mapping[str, object],
    raw_schema_version: str = "v1",
) -> dict[str, object]:
    if source._raw_version(raw_schema_version) == "v2":
        return _root_factual_for_date(
            root, instrument_id=instrument_id, trade_date=trade_date, previous=previous,
        )
    for value in (previous, previous.get("factual"), previous.get("provenance")):
        if isinstance(value, Mapping) and (
            value.get("raw_schema_version", "v1") != "v1"
            or value.get("source_identity_scope") == source.ROOT_IDENTITY_SCOPE
        ):
            _fail("v1 date reader cannot admit another raw version or root scope")
    previous_factual = previous.get("factual")
    if (
        previous.get("status") == "AVAILABLE"
        and isinstance(previous_factual, Mapping)
        and previous_factual.get("trade_date") == trade_date
    ):
        return {
            "status": "AVAILABLE",
            "trade_date": trade_date,
            "factual": dict(previous_factual),
            "provenance": previous.get("provenance"),
            "source_kind": "previous_session_context",
        }
    rows = eod.loc[eod["trade_date"].astype(str).eq(trade_date)]
    if len(rows.index) == 1:
        row = rows.iloc[0]
        return {
            "status": "AVAILABLE",
            "trade_date": trade_date,
            "factual": _eod_factual(row, instrument_id=instrument_id),
            "provenance": {
                "source_kind": "accepted_stage5_eod_historical_context_only",
                "accepted_pointer": dict(eod_provenance),
                "source_partition_ref": row.get("source_partition_ref"),
                "source_canonical_partition_ref": row.get("source_canonical_partition_ref"),
                "source_frozen_partition_sha256": row.get("source_frozen_partition_sha256"),
                "historical_stage5_outputs_supply_live_factual_authority": False,
            },
            "source_kind": "accepted_stage5_eod_historical_context_only",
        }
    if len(rows.index) > 1:
        _fail("accepted EOD history has duplicate requested trade_date")
    return _raw_factual(root, instrument_id=instrument_id, trade_date=trade_date)


def _value(factual: Mapping[str, object], path: str) -> float:
    current: object = factual
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            _fail("factual field is missing: " + path)
        current = current[part]
    return _number(current, path)


def _delta(
    current: Mapping[str, object],
    baseline: Mapping[str, object],
    *,
    lag: int,
    target_trade_date: str | None,
) -> dict[str, object]:
    current_factual = current.get("factual")
    baseline_factual = baseline.get("factual")
    if (
        current.get("status") != "AVAILABLE"
        or baseline.get("status") != "AVAILABLE"
        or not isinstance(current_factual, Mapping)
        or not isinstance(baseline_factual, Mapping)
        or target_trade_date is None
    ):
        return {
            "status": "UNAVAILABLE",
            "session_lag": lag,
            "target_trade_date": target_trade_date,
            "values": None,
            "reason": "current_or_lag_target_factual_unavailable",
            "missing_must_not_be_interpreted_as_zero_or_neutral": True,
        }
    values: dict[str, float | int] = {}
    for field in DELTA_FIELDS:
        difference = _value(current_factual, field) - _value(baseline_factual, field)
        if field.endswith("net_share_of_oi"):
            values[field] = float(difference)
        else:
            if not float(difference).is_integer():
                _fail("integer factual delta became non-integer: " + field)
            values[field] = int(difference)
    return {
        "status": "AVAILABLE",
        "session_lag": lag,
        "target_trade_date": target_trade_date,
        "current_trade_date": current_factual.get("trade_date"),
        "values": values,
        "baseline_provenance": baseline.get("provenance"),
    }


def _statistics(
    *,
    current: Mapping[str, object],
    eod: pd.DataFrame,
    witness_dates: Sequence[str],
) -> dict[str, object]:
    current_factual = current.get("factual")
    if current.get("status") != "AVAILABLE" or not isinstance(current_factual, Mapping):
        return {
            "status": "UNAVAILABLE",
            "variables": None,
            "reason": "current_factual_unavailable",
            "missing_must_not_be_interpreted_as_zero_or_neutral": True,
        }
    current_date = str(current_factual.get("trade_date") or "")
    history = eod.loc[eod["trade_date"].astype(str) < current_date].copy().sort_values("trade_date")
    if history.empty:
        return {
            "status": "UNAVAILABLE",
            "variables": None,
            "reason": "accepted_historical_context_empty_before_current",
        }
    history_series = {
        "fiz.net_share_of_oi": pd.to_numeric(history["phys_net_share_of_oi"], errors="coerce").astype(float),
        "yur.net_share_of_oi": pd.to_numeric(history["legal_net_share_of_oi"], errors="coerce").astype(float),
        "total_open_interest": pd.to_numeric(history["total_open_interest"], errors="coerce").astype(float),
    }
    variables: dict[str, object] = {}
    for field in STAT_FIELDS:
        current_value = _value(current_factual, field)
        source_values = history_series[field]
        if bool(source_values.isna().any()) or not source_values.map(math.isfinite).all():
            _fail("accepted historical statistical field is invalid: " + field)
        windows: dict[str, object] = {}
        for window in STAT_WINDOWS:
            needed_history = window - 1
            if len(source_values.index) < needed_history:
                windows[str(window)] = {
                    "status": "INSUFFICIENT_HISTORY",
                    "observation_count": len(source_values.index) + 1,
                    "percentile": None,
                    "zscore": None,
                }
                continue
            sample = np.append(source_values.tail(needed_history).to_numpy(dtype=float), current_value)
            mean = float(np.mean(sample))
            constant_sample = bool(np.all(sample == sample[0]))
            std = 0.0 if constant_sample else float(np.std(sample, ddof=0))
            percentile = float(np.mean(sample <= current_value))
            zscore = None if std == 0.0 else float((current_value - mean) / std)
            if not math.isfinite(percentile) or percentile < 0.0 or percentile > 1.0:
                _fail("statistical percentile escaped unit interval")
            if zscore is not None and not math.isfinite(zscore):
                _fail("statistical zscore is non-finite")
            windows[str(window)] = {
                "status": "AVAILABLE",
                "observation_count": window,
                "percentile": percentile,
                "zscore": zscore,
                "population_mean": mean,
                "population_std_ddof_0": std,
            }
        variables[field] = {"current_value": current_value, "windows": windows}
    max_history_date = str(history["trade_date"].max())
    recent_gap = [value for value in witness_dates if max_history_date < value < current_date]
    return {
        "status": "AVAILABLE",
        "variables": variables,
        "semantics": {
            "windows_observations": list(STAT_WINDOWS),
            "sample": "current_plus_latest_accepted_EOD_history_rows_strictly_before_current",
            "current_inclusive": True,
            "zscore": "(current - population_mean) / population_std_ddof_0",
            "zero_variance_zscore": None,
            "percentile": "weak_empirical_cdf_mean(sample_values_less_than_or_equal_to_current)",
            "insufficient_history": None,
            "future_rows_used": False,
            "accepted_history_gap_permitted_but_reported": True,
            "not_claimed_as_last_N_observed_sessions_when_history_gap_exists": True,
        },
        "accepted_history_max_trade_date": max_history_date,
        "observed_sessions_between_history_tail_and_current": recent_gap,
        "observed_session_gap_count": len(recent_gap),
    }


def build_instrument_context(
    *,
    root: Path,
    instrument_id: str,
    raw_context: Mapping[str, object],
    as_of: datetime,
) -> dict[str, object]:
    checked_instrument = source._instrument_id(instrument_id)
    witness = _observed_witness(root, as_of=as_of, raw_context=raw_context)
    raw_dates = witness["observed_trade_dates"]
    if isinstance(raw_dates, (str, bytes)) or not isinstance(raw_dates, Sequence):
        _fail("observed-date witness sequence is invalid")
    observed_dates = [str(value) for value in raw_dates]
    current_raw = witness.get("current_observed_trade_date")
    current_date = current_raw if isinstance(current_raw, str) else None
    previous_date = str(witness["previous_observed_trade_date"])
    current = _context_record(
        raw_context.get(session_context.CURRENT_ROLE),
        expected_trade_date=current_date,
        role=session_context.CURRENT_ROLE,
    )
    previous = _context_record(
        raw_context.get(session_context.PREVIOUS_ROLE),
        expected_trade_date=previous_date,
        role=session_context.PREVIOUS_ROLE,
    )
    eod, eod_provenance = _accepted_eod(root, instrument_id=checked_instrument, as_of=as_of)
    current_index = observed_dates.index(current_date) if current_date is not None else -1
    lag_targets: dict[int, str | None] = {}
    baselines: dict[int, dict[str, object]] = {}
    for lag in LAGS:
        target = observed_dates[current_index - lag] if current_date is not None and current_index >= lag else None
        lag_targets[lag] = target
        if target is None:
            baselines[lag] = {
                "status": "UNAVAILABLE",
                "trade_date": None,
                "factual": None,
                "provenance": None,
                "reason": "insufficient_observed_trade_date_history",
            }
        elif lag == 1:
            baselines[lag] = previous
        else:
            baselines[lag] = _factual_for_date(
                root,
                instrument_id=checked_instrument,
                trade_date=target,
                previous=previous,
                eod=eod,
                eod_provenance=eod_provenance,
            )
    deltas = {
        "delta_1d": _delta(current, baselines[1], lag=1, target_trade_date=lag_targets[1]),
        "delta_5d": _delta(current, baselines[5], lag=5, target_trade_date=lag_targets[5]),
        "delta_20d": _delta(current, baselines[20], lag=20, target_trade_date=lag_targets[20]),
    }
    statistics = _statistics(current=current, eod=eod, witness_dates=observed_dates)
    statuses = [
        str(current.get("status")),
        str(previous.get("status")),
        *(str(value.get("status")) for value in deltas.values()),
        str(statistics.get("status")),
    ]
    overall = "READY" if all(value == "AVAILABLE" for value in statuses) else "PARTIAL"
    return {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": overall,
        "instrument_id": checked_instrument,
        "contract_ref": CONTRACT_REF,
        "current": current,
        "previous_observed_session": previous,
        "lag_targets": {
            "delta_1d": lag_targets[1],
            "delta_5d": lag_targets[5],
            "delta_20d": lag_targets[20],
        },
        "deltas": deltas,
        "statistics": statistics,
        "observed_date_witness": witness,
        "historical_context": {
            "dataset_id": EOD_DATASET_ID,
            "provenance": eod_provenance,
            "row_count": int(len(eod.index)),
            "min_trade_date": str(eod["trade_date"].min()),
            "max_trade_date": str(eod["trade_date"].max()),
            "historical_stage5_outputs_supply_live_factual_authority": False,
        },
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
        "calendar_dependency": False,
        "weekday_weekend_inference": False,
        "missing_or_stale_must_not_be_interpreted_as_zero_or_neutral": True,
    }


def build_all(
    *,
    root: Path,
    refresh_bundle: Mapping[str, object],
    as_of: datetime,
) -> dict[str, object]:
    raw_results = refresh_bundle.get("instrument_results")
    if not isinstance(raw_results, Mapping):
        _fail("FUTOI refresh bundle instrument_results is missing")
    results: dict[str, object] = {}
    non_ready: list[str] = []
    for instrument_id in source.LIVE_INSTRUMENT_IDS:
        raw_context = raw_results.get(instrument_id)
        try:
            if not isinstance(raw_context, Mapping):
                _fail("FUTOI refresh bundle instrument context is missing")
            result = build_instrument_context(
                root=root,
                instrument_id=instrument_id,
                raw_context=raw_context,
                as_of=as_of,
            )
        except Exception as exc:
            result = {
                "schema_version": SCHEMA_VERSION,
                "project": PROJECT,
                "status": "UNAVAILABLE",
                "instrument_id": instrument_id,
                "error_class": exc.__class__.__name__,
                "error": str(exc),
                "factual_authority": False,
                "directional_authority": False,
                "action_authority": False,
                "standalone_buy_sell_authority": False,
                "stage5_full_mode_ready": False,
                "stage5_pointer_promotion_performed": False,
                "calendar_dependency": False,
                "weekday_weekend_inference": False,
                "missing_or_stale_must_not_be_interpreted_as_zero_or_neutral": True,
            }
        results[instrument_id] = result
        if result.get("status") != "READY":
            non_ready.append(instrument_id)
    aggregate = "READY" if not non_ready else (
        "UNAVAILABLE" if len(non_ready) == len(source.LIVE_INSTRUMENT_IDS) else "PARTIAL"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": aggregate,
        "instrument_ids": list(source.LIVE_INSTRUMENT_IDS),
        "instrument_results": results,
        "non_ready_instrument_ids": non_ready,
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }


def _root_raw_relative(instrument_id: str, trade_date: str) -> Path:
    checked = source._instrument_id(instrument_id)
    day = source._iso_date(trade_date, "trade_date")
    return (Path("market") / "supplementary" / "dataset_id=futures_futoi_raw"
            / "schema_version=v2" / ("instrument_id=" + checked)
            / ("trade_date=" + day) / ("source=" + source.SOURCE_ID) / "part.parquet")


def _root_raw_identity(instrument_id: str) -> dict[str, object]:
    checked = source._instrument_id(instrument_id)
    return {
        "instrument_id": checked, "source_id": source.SOURCE_ID,
        "source_ticker": source.ROOT_TICKERS[checked], "raw_schema_version": "v2",
        "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
        "source_record_key_fields": list(source.ROOT_KEY_FIELDS),
        "source_contract_ref": source.ROOT_CONTRACT_REFS["source_contract_ref"],
        "raw_contract_ref": source.ROOT_CONTRACT_REFS["raw_contract_ref"],
    }


def _decode_root_raw(content: bytes, *, instrument_id: str, trade_date: str) -> dict[str, object]:
    # The exact buffer that was hashed is decoded, never a second pathname read.
    frame = pd.read_parquet(BytesIO(content))
    return source.latest_aligned_factual(
        frame, expected_trade_date=trade_date, expected_instrument_id=instrument_id,
        expected_source_ticker=source.ROOT_TICKERS[instrument_id], raw_schema_version="v2",
    )


def _raw_root_factual(root: Path, *, instrument_id: str, trade_date: str) -> dict[str, object]:
    identity = _root_raw_identity(instrument_id)
    relative = _root_raw_relative(instrument_id, trade_date)
    ref = source.ROOT_REF_PREFIX + relative.as_posix()
    proof = None
    try:
        path = source._checked_root_path(root, relative)
        if not path.exists():
            return {
                "status": "UNAVAILABLE", "trade_date": trade_date, "factual": None,
                "provenance": None, "reason": "selected_v2_raw_partition_missing",
                "raw_schema_version": "v2", "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
            }
        if not path.is_file():
            _fail("selected v2 raw partition is not a regular file")
        content = path.read_bytes()
        proof = {
            **identity, "source_state_kind": "validated_existing_canonical_raw_partition",
            "original_raw_partition_ref": ref, "raw_partition_ref": ref,
            "raw_partition_sha256": sha256(content).hexdigest(),
            "factual_validation": "NOT_VALIDATED",
        }
        factual = _decode_root_raw(content, instrument_id=instrument_id, trade_date=trade_date)
        normalized = _normalized_factual(factual, field="raw_v2." + trade_date, raw_schema_version="v2")
        proof["factual_validation"] = "PASS"
    except Exception as exc:
        return {
            "status": "UNAVAILABLE", "trade_date": trade_date, "factual": None,
            "provenance": proof, "reason": "selected_v2_raw_partition_failed_validation",
            "error_class": type(exc).__name__, "error": str(exc),
            "raw_schema_version": "v2", "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
        }
    return {
        "status": "AVAILABLE", "trade_date": trade_date, "factual": normalized,
        "provenance": proof, "raw_schema_version": "v2",
        "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
    }


def _replay_root_raw_factual(
    root: Path, provenance: Mapping[str, object], *, instrument_id: str, trade_date: str,
) -> dict[str, object]:
    """Distinguish full native publication evidence from raw-only validation."""
    identity = _root_raw_identity(instrument_id)
    relative = _root_raw_relative(instrument_id, trade_date)
    if not isinstance(provenance, Mapping) or any(provenance.get(k) != v for k, v in identity.items()):
        _fail("v2 raw replay identity/version/contract mismatch")
    if "secid" in provenance:
        _fail("v2 raw replay must not contain contract secid")
    if "accepted_state_kind" in provenance:
        if (provenance.get("accepted_state_kind") != "source_native_exact_date_raw_quality_pass"
                or "source_state_kind" in provenance):
            _fail("v2 native publication evidence cannot be relabelled raw-only")
        # All quality/manifest/run bindings are mandatory; any failure propagates.
        return source.replay_root_factual(
            root, provenance, instrument_id=instrument_id, trade_date=trade_date,
        )
    required = {
        **identity, "source_state_kind": "validated_existing_canonical_raw_partition",
        "original_raw_partition_ref": source.ROOT_REF_PREFIX + relative.as_posix(),
        "factual_validation": "PASS",
    }
    if (set(provenance) != set(required) | {"raw_partition_ref", "raw_partition_sha256"}
            or any(provenance.get(k) != v for k, v in required.items())):
        _fail("v2 raw-only evidence shape/state/path mismatch")
    digest = provenance["raw_partition_sha256"]
    if (not isinstance(digest, str) or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)):
        _fail("v2 raw replay digest is invalid")
    ref = provenance["raw_partition_ref"]
    if ref == required["original_raw_partition_ref"]:
        path = source._checked_root_path(root, relative)
        if not path.is_file():
            _fail("v2 raw replay source file is missing")
        content = path.read_bytes()
        if sha256(content).hexdigest() != digest:
            _fail("v2 raw replay SHA mismatch")
    else:
        # Only this exact content-addressed object is allowed. The old canonical
        # partition may have been replaced or removed; it is not read in replay.
        content = source._root_proof_bytes(root, ref, digest, ".parquet")
    return _decode_root_raw(content, instrument_id=instrument_id, trade_date=trade_date)


def _root_factual_for_date(
    root: Path, *, instrument_id: str, trade_date: str, previous: Mapping[str, object],
) -> dict[str, object]:
    source._instrument_id(instrument_id)
    source._iso_date(trade_date, "trade_date")
    if not isinstance(previous, Mapping):
        _fail("v2 previous context must be an explicit mapping")
    factual = previous.get("factual")
    owns_date = previous.get("expected_trade_date") == trade_date or (
        isinstance(factual, Mapping) and factual.get("trade_date") == trade_date
    )
    if not owns_date:
        return _raw_factual(root, instrument_id=instrument_id, trade_date=trade_date, raw_schema_version="v2")
    try:
        if (previous.get("status") != "AVAILABLE" or not isinstance(factual, Mapping)
                or factual.get("trade_date") != trade_date
                or previous.get("expected_trade_date") != trade_date
                or previous.get("raw_schema_version", "v2") != "v2"):
            _fail("selected v2 previous context is not exact and fresh")
        proof = previous.get("provenance")
        if not isinstance(proof, Mapping) or proof.get("accepted_state_kind") != "source_native_exact_date_raw_quality_pass":
            _fail("selected v2 previous context requires native publication evidence")
        replayed = _replay_root_raw_factual(root, proof, instrument_id=instrument_id, trade_date=trade_date)
        normalized = _normalized_factual(replayed, field="previous_v2." + trade_date, raw_schema_version="v2")
        if factual != normalized:
            _fail("selected v2 previous factual differs from frozen raw")
        return {
            "status": "AVAILABLE", "trade_date": trade_date, "factual": normalized,
            "provenance": deepcopy(proof), "source_kind": "previous_session_context",
            "raw_schema_version": "v2", "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
        }
    except Exception as exc:
        # Do not hide an explicit failed attempt with an older canonical file.
        return {
            "status": "UNAVAILABLE", "trade_date": trade_date, "factual": None,
            "provenance": previous.get("provenance"),
            "reason": "selected_v2_previous_failed_revalidation",
            "error_class": type(exc).__name__, "error": str(exc),
            "raw_schema_version": "v2", "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
        }

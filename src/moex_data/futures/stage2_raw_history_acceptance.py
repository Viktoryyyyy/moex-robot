from __future__ import annotations

import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd

from . import materialize_futoi_instrument as futoi_materializer
from . import materialize_raw_5m as quote_core
from .contract_io import expand_contract_path, load_simple_yaml_mapping

DATA_LAKE_PATH: Final[str] = "configs/datasets/futures_data_lake.v1.yaml"
QUOTE_CONTRACT_PATH: Final[str] = "contracts/datasets/futures_raw_5m.v1.yaml"
FUTOI_CONTRACT_PATH: Final[str] = "contracts/datasets/futures_futoi_raw.v1.yaml"
ACCEPTANCE_CONTRACT_PATH: Final[str] = "contracts/datasets/futures_raw_history_acceptance.v1.yaml"

ACCEPTANCE_DATASET_ID: Final[str] = "futures_raw_history_acceptance"
QUOTE_DATASET_ID: Final[str] = "futures_raw_5m"
FUTOI_DATASET_ID: Final[str] = "futures_futoi_raw"
QUOTE_SOURCE_ID: Final[str] = "moex_algopack_fo_tradestats_5m"
FUTOI_SOURCE_ID: Final[str] = "moex_algopack_futoi"
EXPECTED_BOARD: Final[str] = "RFUD"
EXPECTED_MARKET: Final[str] = "forts"
EXPECTED_ENGINE: Final[str] = "futures"
MOEX_SOURCE_TIMEZONE: Final[str] = "Europe/Moscow"

_ALLOWED_QUOTES: Final[frozenset[str]] = frozenset(
    {"usdrubf_futures_family", "cnyrubf_futures_family"}
)
_ALLOWED_FUTOI: Final[frozenset[str]] = frozenset(
    {"si_futures_family", "cr_futures_family"}
)


class RawHistoryAcceptanceError(ValueError):
    pass


@dataclass(frozen=True)
class HistoryExpectation:
    target_dataset_id: str
    instrument_id: str
    source_id: str
    date_start: str
    date_end: str
    expected_partitions: int
    expected_rows: int
    expected_secid: str | None = None
    expected_source_ticker: str | None = None
    expected_missing_dates: int | None = None


def _fail(message: str) -> None:
    raise RawHistoryAcceptanceError(message)


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        _fail(field_name + " must be a mapping")
    return value


def _require_text(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        _fail(field_name + " is required")
    return text


def _require_token(value: object, field_name: str) -> str:
    text = _require_text(value, field_name)
    if text in (".", "..") or "/" in text or "\\" in text or any(
        marker in text for marker in ("*", "{", "}", "$(", "`")
    ):
        _fail(field_name + " must be an explicit safe token")
    return text


def _require_int(value: object, field_name: str) -> int:
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise RawHistoryAcceptanceError(field_name + " must be int") from exc
    if number < 0:
        _fail(field_name + " must be non-negative")
    return number


def _require_date(value: object, field_name: str) -> str:
    text = _require_text(value, field_name)
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise RawHistoryAcceptanceError(field_name + " must be YYYY-MM-DD") from exc


def _date_range(date_start: str, date_end: str) -> tuple[str, ...]:
    start = date.fromisoformat(date_start)
    end = date.fromisoformat(date_end)
    if start > end:
        _fail("date_start must be <= date_end")
    values: list[str] = []
    current = start
    while current <= end:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return tuple(values)


def _data_root() -> str:
    root = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not root:
        _fail("MOEX_DATA_ROOT is required")
    return root


def _stage2(values: Mapping[str, object]) -> Mapping[str, object]:
    section = _mapping(values.get("stage2_forts_source_bindings"), "stage2_forts_source_bindings")
    if str(section.get("status")) != "raw_historical_backfills_completed_acceptance_pending":
        _fail("Stage 2 is not in raw historical acceptance-pending state")
    readiness = _mapping(section.get("readiness_flags"), "stage2_forts_source_bindings.readiness_flags")
    for field in (
        "historical_quotes_backfill_completed",
        "priority_futoi_raw_backfill_completed",
        "raw_physical_audit_completed",
    ):
        if readiness.get(field) is not True:
            _fail("required Stage 2 readiness flag is not true: " + field)
    if readiness.get("accepted_pointer_ready") is not False:
        _fail("accepted_pointer_ready must remain false during raw history acceptance")
    if readiness.get("scheduler_ready") is not False:
        _fail("scheduler_ready must remain false during raw history acceptance")
    if readiness.get("d1_materialization_ready") is not False:
        _fail("d1_materialization_ready must remain false during raw history acceptance")
    if readiness.get("research_ready") is not False:
        _fail("research_ready must remain false during raw history acceptance")
    return section


def _expectation(repo_root: Path, target_dataset_id: str, instrument_id: str) -> HistoryExpectation:
    values = load_simple_yaml_mapping(repo_root, DATA_LAKE_PATH)
    stage2 = _stage2(values)
    if target_dataset_id == QUOTE_DATASET_ID:
        if instrument_id not in _ALLOWED_QUOTES:
            _fail("instrument is not in Stage 2 historical quote acceptance scope")
        source = _mapping(stage2.get("quote_source"), "quote_source")
        if str(source.get("dataset_contract_ref")) != QUOTE_CONTRACT_PATH:
            _fail("quote dataset contract binding is not canonical")
        if str(source.get("source_id")) != QUOTE_SOURCE_ID:
            _fail("quote source binding is not canonical")
        scope = source.get("historical_backfill_instrument_ids")
        if not isinstance(scope, tuple) or instrument_id not in scope:
            _fail("instrument is not explicitly authorized historical quote scope")
        coverage = _mapping(source.get("proven_coverage"), "quote_source.proven_coverage")
        item = _mapping(coverage.get(instrument_id), "quote_source.proven_coverage." + instrument_id)
        if str(item.get("physical_quality_status")) != "pass":
            _fail("repository quote physical quality evidence is not pass")
        if str(item.get("market_spotcheck_status")) != "pass":
            _fail("repository quote market spotcheck evidence is not pass")
        return HistoryExpectation(
            target_dataset_id=QUOTE_DATASET_ID,
            instrument_id=instrument_id,
            source_id=QUOTE_SOURCE_ID,
            date_start=_require_date(item.get("first_available"), "first_available"),
            date_end=_require_date(item.get("last_available"), "last_available"),
            expected_partitions=_require_int(item.get("partitions"), "partitions"),
            expected_rows=_require_int(item.get("rows"), "rows"),
            expected_secid=_require_token(item.get("secid"), "secid"),
        )

    if target_dataset_id == FUTOI_DATASET_ID:
        if instrument_id not in _ALLOWED_FUTOI:
            _fail("instrument is not in Stage 2 historical FUTOI acceptance scope")
        source = _mapping(stage2.get("futoi_source"), "futoi_source")
        if str(source.get("dataset_contract_ref")) != FUTOI_CONTRACT_PATH:
            _fail("FUTOI dataset contract binding is not canonical")
        if str(source.get("source_id")) != FUTOI_SOURCE_ID:
            _fail("FUTOI source binding is not canonical")
        if str(source.get("public_iss_evidence_status")) != "invalidated":
            _fail("FUTOI public ISS evidence must remain invalidated")
        backfills = _mapping(source.get("historical_priority_backfills"), "futoi_source.historical_priority_backfills")
        item = _mapping(backfills.get(instrument_id), "futoi_source.historical_priority_backfills." + instrument_id)
        if str(item.get("physical_quality_status")) != "pass":
            _fail("repository FUTOI physical quality evidence is not pass")
        if _require_int(item.get("bad_partitions"), "bad_partitions") != 0:
            _fail("repository FUTOI bad_partitions must be zero")

        binding = futoi_materializer._registry_binding(
            repo_root / futoi_materializer.REGISTRY_PATH, instrument_id
        )
        if str(binding.get("futoi.source_id")) != FUTOI_SOURCE_ID:
            _fail("registry FUTOI source_id does not match canonical source")
        if str(binding.get("futoi.availability_status")) != "available":
            _fail("registry FUTOI availability_status is not available")
        if str(binding.get("futoi.probe_status")) != "completed":
            _fail("registry FUTOI probe_status is not completed")
        if str(binding.get("board")).casefold() != EXPECTED_BOARD.casefold():
            _fail("registry FUTOI board identity mismatch")
        if str(binding.get("market")).casefold() != EXPECTED_MARKET.casefold():
            _fail("registry FUTOI market identity mismatch")
        if str(binding.get("engine")).casefold() != EXPECTED_ENGINE.casefold():
            _fail("registry FUTOI engine identity mismatch")
        registry_ticker = _require_token(binding.get("futoi.ticker"), "registry.futoi.ticker")
        configured_ticker = _require_token(item.get("ticker"), "ticker")
        if registry_ticker.casefold() != configured_ticker.casefold():
            _fail("repository FUTOI ticker evidence does not match registry binding")

        return HistoryExpectation(
            target_dataset_id=FUTOI_DATASET_ID,
            instrument_id=instrument_id,
            source_id=FUTOI_SOURCE_ID,
            date_start=_require_date(item.get("first_available"), "first_available"),
            date_end=_require_date(item.get("last_available"), "last_available"),
            expected_partitions=_require_int(item.get("partitions"), "partitions"),
            expected_rows=_require_int(item.get("rows"), "rows"),
            expected_secid=_require_token(binding.get("secid"), "registry.secid"),
            expected_source_ticker=registry_ticker,
            expected_missing_dates=_require_int(item.get("skipped_empty_source_dates"), "skipped_empty_source_dates"),
        )

    _fail("target_dataset_id is not part of Stage 2 raw history acceptance scope")


def _contract_path(repo_root: Path, target_dataset_id: str) -> str:
    path = QUOTE_CONTRACT_PATH if target_dataset_id == QUOTE_DATASET_ID else FUTOI_CONTRACT_PATH
    values = load_simple_yaml_mapping(repo_root, path)
    if str(values.get("dataset_id")) != target_dataset_id:
        _fail("target dataset contract identity mismatch")
    return _require_text(values.get("path_pattern"), "path_pattern")


def _partition_path(
    *,
    repo_root: Path,
    pattern: str,
    expectation: HistoryExpectation,
    trade_date: str,
) -> Path:
    placeholders = {
        "DATASET_ID": expectation.target_dataset_id,
        "INSTRUMENT_ID": expectation.instrument_id,
        "YYYY-MM-DD": trade_date,
        "SOURCE_ID": expectation.source_id,
    }
    try:
        return expand_contract_path(pattern, _data_root(), placeholders)
    except Exception as exc:
        raise RawHistoryAcceptanceError(str(exc)) from exc


def _acceptance_path(repo_root: Path, expectation: HistoryExpectation, run_id: str) -> Path:
    contract = load_simple_yaml_mapping(repo_root, ACCEPTANCE_CONTRACT_PATH)
    if str(contract.get("dataset_id")) != ACCEPTANCE_DATASET_ID:
        _fail("acceptance contract identity mismatch")
    pattern = _require_text(contract.get("path_pattern"), "acceptance.path_pattern")
    try:
        return expand_contract_path(
            pattern,
            _data_root(),
            {
                "TARGET_DATASET_ID": expectation.target_dataset_id,
                "INSTRUMENT_ID": expectation.instrument_id,
                "RUN_ID": run_id,
            },
        )
    except Exception as exc:
        raise RawHistoryAcceptanceError(str(exc)) from exc


def acceptance_report_path(
    *, repo_root: str | Path, target_dataset_id: str, instrument_id: str, run_id: str
) -> Path:
    root = Path(repo_root)
    expectation = _expectation(
        root,
        _require_token(target_dataset_id, "target_dataset_id"),
        _require_token(instrument_id, "instrument_id"),
    )
    return _acceptance_path(root, expectation, _require_token(run_id, "run_id"))


def _write_json_immutable(path: Path, values: Mapping[str, object]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp"
    ) as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        temporary_name = handle.name
    try:
        os.link(temporary_name, path)
    except FileExistsError as exc:
        raise RawHistoryAcceptanceError(
            "acceptance report already exists for explicit run_id"
        ) from exc
    finally:
        Path(temporary_name).unlink(missing_ok=True)


def _require_stored_identity(
    frame: pd.DataFrame, field: str, expected: str, *, casefold: bool = False
) -> None:
    if field not in frame.columns:
        _fail("partition missing stored identity field: " + field)
    values = frame[field].astype("string").str.strip()
    if bool(values.isna().any()) or bool(values.eq("").any()):
        _fail("partition contains missing stored identity: " + field)
    matches = values.str.casefold().eq(expected.casefold()) if casefold else values.eq(expected)
    if not bool(matches.all()):
        _fail("partition stored identity mismatch: " + field)


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        _fail("partition missing numeric field: " + column)
    return pd.to_numeric(frame[column], errors="coerce")


def _require_finite_numeric(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    for column in columns:
        values = _numeric_series(frame, column)
        if bool(values.isna().any()) or not bool(np.isfinite(values.to_numpy(dtype="float64")).all()):
            _fail("partition contains invalid or non-finite numeric value: " + column)


def _require_optional_finite_nonnegative(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    for column in columns:
        values = _numeric_series(frame, column)
        present = values.dropna()
        if not bool(np.isfinite(present.to_numpy(dtype="float64")).all()):
            _fail("quote partition contains non-finite " + column)
        if bool((present < 0).any()):
            _fail("quote partition contains negative " + column)


def _moex_publication_utc(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if bool(parsed.isna().any()):
        _fail("FUTOI partition contains invalid publication systime")
    try:
        if parsed.dt.tz is None:
            return parsed.dt.tz_localize(
                MOEX_SOURCE_TIMEZONE, ambiguous="raise", nonexistent="raise"
            ).dt.tz_convert("UTC")
        return parsed.dt.tz_convert("UTC")
    except Exception as exc:
        raise RawHistoryAcceptanceError(
            "FUTOI publication systime timezone normalization failed: " + str(exc)
        ) from exc


def _validate_quote_partition(
    repo_root: Path,
    frame: pd.DataFrame,
    expectation: HistoryExpectation,
    trade_date: str,
    run_id: str,
) -> tuple[int, tuple[str, ...]]:
    if expectation.expected_secid is None:
        _fail("quote expected secid is missing")
    for field, expected, casefold in (
        ("instrument_id", expectation.instrument_id, False),
        ("source_id", expectation.source_id, False),
        ("secid", expectation.expected_secid, False),
        ("board", EXPECTED_BOARD, True),
        ("market", EXPECTED_MARKET, True),
        ("engine", EXPECTED_ENGINE, True),
        ("source", quote_core.SOURCE_CANDIDATE_APIM_TRADESTATS, False),
    ):
        _require_stored_identity(frame, field, expected, casefold=casefold)

    for field in ("trade_date", "session_date", "ts"):
        if field not in frame.columns:
            _fail("quote partition missing temporal field: " + field)
    stored_trade_date = pd.to_datetime(frame["trade_date"], errors="coerce")
    stored_session_date = pd.to_datetime(frame["session_date"], errors="coerce")
    timestamps = pd.to_datetime(frame["ts"], errors="coerce")
    if bool(stored_trade_date.isna().any()) or bool(stored_session_date.isna().any()) or bool(timestamps.isna().any()):
        _fail("quote partition contains invalid temporal identity")
    if not bool(stored_trade_date.dt.date.astype(str).eq(trade_date).all()):
        _fail("quote partition trade_date mismatch")
    if not bool(stored_session_date.dt.date.astype(str).eq(trade_date).all()):
        _fail("quote partition session_date mismatch")
    if not bool(timestamps.dt.date.astype(str).eq(trade_date).all()):
        _fail("quote partition ts date mismatch")

    _require_finite_numeric(frame, ("open", "high", "low", "close", "volume"))
    _require_optional_finite_nonnegative(frame, ("volume", "value", "num_trades"))

    request = quote_core.build_materialization_request(
        repo_root=repo_root,
        dataset_id=QUOTE_DATASET_ID,
        contract_id="futures_raw_5m.v1",
        trade_date=trade_date,
        secid=expectation.expected_secid,
        source_path=None,
        run_id=run_id,
        instrument_id=expectation.instrument_id,
        source_id=expectation.source_id,
        source_candidate=quote_core.SOURCE_CANDIDATE_APIM_TRADESTATS,
        source_endpoint=quote_core.SOURCE_ENDPOINT_APIM_FO_TRADESTATS,
        market=quote_core.TARGET_MARKET,
        board=quote_core.TARGET_BOARD,
        engine=EXPECTED_ENGINE,
        series_type="native",
        granularity="5m",
    )
    validated, _ = quote_core._validate_source_table(frame, request)
    secids = tuple(sorted(set(validated["secid"].astype(str).tolist())))
    return int(len(validated)), secids


def _validate_futoi_partition(
    frame: pd.DataFrame,
    expectation: HistoryExpectation,
    trade_date: str,
) -> tuple[int, tuple[str, ...]]:
    contract_required = (
        "instrument_id", "trade_date", "ts", "moment", "systime", "sess_id", "seqnum",
        "secid", "board", "market", "engine", "source_id", "source_ticker", "clgroup",
        "pos", "pos_long", "pos_short", "pos_long_num", "pos_short_num",
        "availability_ts_utc", "ingest_ts",
    )
    missing = [column for column in contract_required if column not in frame.columns]
    if missing:
        _fail("FUTOI partition missing required columns: " + ",".join(missing))
    if frame.empty:
        _fail("FUTOI partition is empty")

    for field, expected, casefold in (
        ("instrument_id", expectation.instrument_id, False),
        ("source_id", expectation.source_id, False),
        ("board", EXPECTED_BOARD, True),
        ("market", EXPECTED_MARKET, True),
        ("engine", EXPECTED_ENGINE, True),
    ):
        _require_stored_identity(frame, field, expected, casefold=casefold)
    if expectation.expected_secid is None:
        _fail("FUTOI expected secid is missing from registry binding")
    _require_stored_identity(frame, "secid", expectation.expected_secid, casefold=True)
    if not bool(frame["trade_date"].astype(str).eq(trade_date).all()):
        _fail("FUTOI partition trade_date mismatch")
    if expectation.expected_source_ticker is None:
        _fail("FUTOI expected source ticker is missing")
    _require_stored_identity(
        frame, "source_ticker", expectation.expected_source_ticker, casefold=True
    )
    groups = frame["clgroup"].astype("string").str.upper().str.strip()
    if bool(groups.isna().any()) or not bool(groups.isin({"FIZ", "YUR"}).all()):
        _fail("FUTOI partition clgroup is invalid")

    ts = pd.to_datetime(frame["ts"], errors="coerce")
    moment = pd.to_datetime(frame["moment"], errors="coerce")
    systime = pd.to_datetime(frame["systime"], errors="coerce")
    if bool(ts.isna().any()) or bool(moment.isna().any()):
        _fail("FUTOI partition contains invalid ts/moment")
    if not bool(ts.eq(moment).all()):
        _fail("FUTOI partition violates ts=moment source-reference semantics")
    if not bool(ts.dt.date.astype(str).eq(trade_date).all()):
        _fail("FUTOI partition source-reference date mismatch")
    if bool(systime.isna().any()) or bool((systime < moment).any()):
        _fail("FUTOI partition contains invalid publication systime")

    publication_utc = _moex_publication_utc(frame["systime"])
    availability_utc = pd.to_datetime(frame["availability_ts_utc"], errors="coerce", utc=True)
    ingest_utc = pd.to_datetime(frame["ingest_ts"], errors="coerce", utc=True)
    if bool(publication_utc.isna().any()) or bool(availability_utc.isna().any()) or bool(ingest_utc.isna().any()):
        _fail("FUTOI partition contains invalid publication/availability/ingest timestamp")
    if bool((availability_utc < publication_utc).any()):
        _fail("FUTOI availability timestamp precedes source publication timestamp")
    if bool((ingest_utc < availability_utc).any()):
        _fail("FUTOI ingest timestamp precedes availability timestamp")

    position_fields = ("pos", "pos_long", "pos_short", "pos_long_num", "pos_short_num")
    _require_finite_numeric(frame, position_fields)
    position_values = {field: _numeric_series(frame, field) for field in position_fields}
    for field in ("pos_long_num", "pos_short_num"):
        participant_counts = position_values[field].to_numpy(dtype="float64")
        if bool((participant_counts < 0).any()) or not bool(
            np.equal(participant_counts, np.floor(participant_counts)).all()
        ):
            _fail("FUTOI participant counts must be non-negative integers")
    if not bool(
        position_values["pos"].eq(
            position_values["pos_long"] + position_values["pos_short"]
        ).all()
    ):
        _fail("FUTOI net position must equal pos_long plus pos_short")

    normalized = futoi_materializer._validate_required_source_identifiers(frame)
    counts = futoi_materializer._quality_counts(normalized)
    if int(counts["duplicate_key_count"]) != 0:
        _fail("FUTOI partition contains duplicate canonical source-record keys")
    if int(counts["null_required_count"]) != 0:
        _fail("FUTOI partition contains null required values")
    if int(counts["invalid_position_count"]) != 0:
        _fail("FUTOI partition contains invalid position values")

    secid_text = frame["secid"].astype("string").str.strip()
    secids = tuple(sorted(set(secid_text.astype(str).tolist())))
    return int(len(frame)), secids


def audit_history(
    *,
    repo_root: str | Path,
    target_dataset_id: str,
    instrument_id: str,
    run_id: str,
) -> dict[str, object]:
    root = Path(repo_root)
    checked_dataset = _require_token(target_dataset_id, "target_dataset_id")
    checked_instrument = _require_token(instrument_id, "instrument_id")
    checked_run_id = _require_token(run_id, "run_id")
    expectation = _expectation(root, checked_dataset, checked_instrument)
    pattern = _contract_path(root, checked_dataset)

    missing_dates: list[str] = []
    failed_dates: list[dict[str, str]] = []
    hard_failures: list[str] = []
    secid_scope: set[str] = set()
    actual_partitions = 0
    actual_rows = 0
    dates = _date_range(expectation.date_start, expectation.date_end)

    for trade_date in dates:
        path = _partition_path(
            repo_root=root, pattern=pattern, expectation=expectation, trade_date=trade_date
        )
        if not path.exists():
            missing_dates.append(trade_date)
            continue
        if not path.is_file():
            missing_dates.append(trade_date)
            failed_dates.append(
                {"trade_date": trade_date, "error": "canonical partition path is not a file"}
            )
            continue
        actual_partitions += 1
        try:
            frame = pd.read_parquet(path)
            if checked_dataset == QUOTE_DATASET_ID:
                rows, secids = _validate_quote_partition(
                    root, frame, expectation, trade_date, checked_run_id
                )
            else:
                rows, secids = _validate_futoi_partition(frame, expectation, trade_date)
            actual_rows += rows
            secid_scope.update(secids)
        except Exception as exc:
            failed_dates.append({"trade_date": trade_date, "error": str(exc)})

    if actual_partitions != expectation.expected_partitions:
        hard_failures.append("expected_partition_count_mismatch")
    if actual_rows != expectation.expected_rows:
        hard_failures.append("expected_row_count_mismatch")
    expected_calendar_missing = len(dates) - expectation.expected_partitions
    if len(missing_dates) != expected_calendar_missing:
        hard_failures.append("calendar_missing_partition_count_mismatch")
    if expectation.expected_missing_dates is not None and len(missing_dates) != expectation.expected_missing_dates:
        hard_failures.append("recorded_source_empty_date_count_mismatch")
    if failed_dates:
        hard_failures.append("failed_partition_dates_nonempty")
    if expectation.expected_secid is not None and secid_scope != {expectation.expected_secid}:
        hard_failures.append("secid_scope_mismatch")

    output_path = _acceptance_path(root, expectation, checked_run_id)
    status = "pass" if not hard_failures else "fail"
    return {
        "run_id": checked_run_id,
        "dataset_id": ACCEPTANCE_DATASET_ID,
        "target_dataset_id": checked_dataset,
        "instrument_id": checked_instrument,
        "source_id": expectation.source_id,
        "secid_scope": sorted(secid_scope),
        "requested_from": expectation.date_start,
        "requested_till": expectation.date_end,
        "expected_partition_count": expectation.expected_partitions,
        "actual_partition_count": actual_partitions,
        "expected_row_count": expectation.expected_rows,
        "actual_row_count": actual_rows,
        "expected_calendar_missing_partition_count": expected_calendar_missing,
        "actual_calendar_missing_partition_count": len(missing_dates),
        "missing_partition_dates": missing_dates,
        "failed_partition_dates": failed_dates,
        "hard_check_failures": hard_failures,
        "acceptance_status": status,
        "accepted_pointer_written": False,
        "network_access_used": False,
        "historical_backfill_used": False,
        "implicit_partition_discovery_used": False,
        "latest_autodetect_used": False,
        "producer": "moex_data.futures.stage2_raw_history_acceptance_gate.v1",
        "acceptance_contract_ref": ACCEPTANCE_CONTRACT_PATH,
        "acceptance_report_reference": output_path.as_posix(),
        "evidence_written": False,
    }
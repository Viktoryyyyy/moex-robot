from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Final

import pandas as pd

from . import futoi_live_factual_refresh_source_native as source

PROJECT: Final[str] = source.PROJECT
SCHEMA_VERSION: Final[str] = "futoi_intraday_previous_session_context.v1"
ARTIFACT_FILENAME: Final[str] = "intraday_previous_session_context.json"
SOURCE_LOOKBACK_DAYS: Final[int] = source.SOURCE_LOOKBACK_DAYS
CURRENT_ROLE: Final[str] = "current_intraday"
PREVIOUS_ROLE: Final[str] = "previous_completed_session"


class FutoiIntradayContextError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiIntradayContextError(message)


def _artifact_path(root: Path, instrument_id: str) -> Path:
    return source._current_path(root, instrument_id).parent / ARTIFACT_FILENAME


def _load_previous(root: Path, instrument_id: str) -> dict[str, object] | None:
    path = _artifact_path(root, instrument_id)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        _fail("FUTOI intraday context artifact must be a regular non-symlink file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FutoiIntradayContextError(
            "FUTOI intraday context artifact is not valid JSON"
        ) from exc
    if not isinstance(value, dict):
        _fail("FUTOI intraday context artifact must contain a JSON object")
    if value.get("project") != PROJECT or value.get("instrument_id") != instrument_id:
        _fail("FUTOI intraday context artifact identity mismatch")
    return value


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        _fail("clock must be timezone-aware")
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _resolve_observed_trade_dates(
    through_date: str,
    *,
    instrument_id: str,
    timeout: float,
) -> tuple[list[str], str | None, str | None]:
    checked = source._iso_date(through_date, "through_date")
    checked_instrument = source._instrument_id(instrument_id)
    end = date.fromisoformat(checked)
    start = end - timedelta(days=SOURCE_LOOKBACK_DAYS - 1)
    try:
        raw = source.observed_dates.observed_dates(
            start.isoformat(),
            end.isoformat(),
            instrument_id=checked_instrument,
            timeout=timeout,
        )
        observed = source.observed_dates.normalize_observed_dates(
            raw,
            start.isoformat(),
            end.isoformat(),
        )
    except Exception as exc:
        raise FutoiIntradayContextError(
            "authoritative observed TradeStats date selection failed for "
            + checked_instrument
            + ": "
            + str(exc)
        ) from exc
    if not observed:
        _fail("authoritative observed TradeStats date selection returned no dates")

    if checked in observed:
        current_trade_date: str | None = checked
        index = observed.index(checked)
        previous_trade_date = observed[index - 1] if index > 0 else None
    else:
        current_trade_date = None
        previous_trade_date = observed[-1]
    return list(observed), current_trade_date, previous_trade_date


def _freshness(
    *,
    state: str,
    expected_trade_date: str | None,
    factual: Mapping[str, object] | None,
) -> dict[str, object]:
    return {
        "status": state,
        "policy": "authoritative_observed_tradestats_date_then_exact_futoi",
        "expected_trade_date": expected_trade_date,
        "accepted_trade_date": factual.get("trade_date") if factual is not None else None,
        "source_snapshot_ts": factual.get("snapshot_ts") if factual is not None else None,
        "source_publication_time": (
            factual.get("source_publication_time") if factual is not None else None
        ),
        "availability_ts_utc": (
            factual.get("availability_ts_utc") if factual is not None else None
        ),
        "ingest_ts_utc": factual.get("ingest_ts_utc") if factual is not None else None,
        "trading_date_authority_source_id": source.observed_dates.SOURCE_ID,
        "weekday_weekend_inference": False,
        "calendar_dependency": False,
    }


def _empty_record(
    *,
    role: str,
    expected_trade_date: str | None,
    attempted_at: str,
    status: str,
    error_class: str | None = None,
    error: str | None = None,
) -> dict[str, object]:
    return {
        "role": role,
        "status": status,
        "availability_state": "UNAVAILABLE" if status == "UNAVAILABLE" else status,
        "expected_trade_date": expected_trade_date,
        "trade_date": None,
        "refresh_attempted_at": attempted_at,
        "last_success_at": None,
        "failed_attempt_at": attempted_at if error else None,
        "refresh_error_class": error_class,
        "refresh_error": error,
        "freshness": _freshness(
            state="UNAVAILABLE" if status == "UNAVAILABLE" else status,
            expected_trade_date=expected_trade_date,
            factual=None,
        ),
        "factual": None,
        "provenance": None,
    }


def _retained_record(
    prior: Mapping[str, object] | None,
    *,
    role: str,
    expected_trade_date: str | None,
    attempted_at: str,
    error_class: str,
    error: str,
    require_same_trade_date: bool,
) -> dict[str, object] | None:
    if not isinstance(prior, Mapping):
        return None
    factual = prior.get("factual")
    if not isinstance(factual, Mapping):
        return None
    prior_trade_date = str(factual.get("trade_date") or "")
    if require_same_trade_date and prior_trade_date != str(expected_trade_date or ""):
        return None
    provenance = prior.get("provenance")
    return {
        "role": role,
        "status": "RETAINED_STALE",
        "availability_state": "RETAINED",
        "expected_trade_date": expected_trade_date,
        "trade_date": prior_trade_date or None,
        "refresh_attempted_at": attempted_at,
        "last_success_at": prior.get("last_success_at"),
        "failed_attempt_at": attempted_at,
        "refresh_error_class": error_class,
        "refresh_error": error,
        "freshness": _freshness(
            state="STALE",
            expected_trade_date=expected_trade_date,
            factual=factual,
        ),
        "factual": dict(factual),
        "provenance": dict(provenance) if isinstance(provenance, Mapping) else provenance,
    }


def _materialize_record(
    *,
    root: Path,
    instrument_id: str,
    trade_date: str,
    role: str,
    run_id: str,
    timeout: float,
    attempted_at: str,
    now_fn: Callable[[], datetime],
) -> dict[str, object]:
    identity = source.source_identity(instrument_id)
    binding = source._binding(instrument_id)
    probe = source._probe_exact_date(
        binding,
        date.fromisoformat(trade_date),
        timeout=timeout,
    )
    if probe.get("status") != "FUTOI_DATA":
        raise FutoiIntradayContextError(
            "FUTOI exact source pending on authoritative observed TradeStats date "
            + trade_date
        )
    partition_path, provenance = source._materialize_target(
        root,
        trade_date,
        run_id + "_" + role,
        instrument_id=instrument_id,
        timeout=timeout,
    )
    frame = pd.read_parquet(partition_path)
    factual = source.latest_aligned_factual(
        frame,
        expected_trade_date=trade_date,
        expected_instrument_id=instrument_id,
        expected_source_ticker=identity["source_ticker"],
        expected_secid=identity["secid"],
    )
    completed_at = _iso_utc(now_fn())
    return {
        "role": role,
        "status": "FRESH",
        "availability_state": "AVAILABLE",
        "expected_trade_date": trade_date,
        "trade_date": factual["trade_date"],
        "refresh_attempted_at": attempted_at,
        "last_success_at": completed_at,
        "failed_attempt_at": None,
        "refresh_error_class": None,
        "refresh_error": None,
        "freshness": _freshness(
            state="FRESH",
            expected_trade_date=trade_date,
            factual=factual,
        ),
        "factual": factual,
        "provenance": provenance,
    }


def _record_with_failure_semantics(
    *,
    root: Path,
    instrument_id: str,
    trade_date: str | None,
    role: str,
    run_id: str,
    timeout: float,
    attempted_at: str,
    prior: Mapping[str, object] | None,
    now_fn: Callable[[], datetime],
) -> dict[str, object]:
    if trade_date is None:
        retained = _retained_record(
            prior,
            role=role,
            expected_trade_date=None,
            attempted_at=attempted_at,
            error_class="NoObservedCurrentTradeDate",
            error="current through_date is not an observed authoritative TradeStats date",
            require_same_trade_date=False,
        )
        if retained is not None:
            retained["status"] = "UNAVAILABLE_RETAINED_STALE"
            retained["availability_state"] = "UNAVAILABLE_RETAINED"
            return retained
        return _empty_record(
            role=role,
            expected_trade_date=None,
            attempted_at=attempted_at,
            status="UNAVAILABLE",
            error_class="NoObservedCurrentTradeDate",
            error="current through_date is not an observed authoritative TradeStats date",
        )
    try:
        return _materialize_record(
            root=root,
            instrument_id=instrument_id,
            trade_date=trade_date,
            role=role,
            run_id=run_id,
            timeout=timeout,
            attempted_at=attempted_at,
            now_fn=now_fn,
        )
    except Exception as exc:
        require_same = role == PREVIOUS_ROLE
        retained = _retained_record(
            prior,
            role=role,
            expected_trade_date=trade_date,
            attempted_at=attempted_at,
            error_class=exc.__class__.__name__,
            error=str(exc),
            require_same_trade_date=require_same,
        )
        if retained is not None:
            return retained
        status = (
            "PENDING"
            if "pending on authoritative observed TradeStats date" in str(exc)
            else "ERROR"
        )
        return _empty_record(
            role=role,
            expected_trade_date=trade_date,
            attempted_at=attempted_at,
            status=status,
            error_class=exc.__class__.__name__,
            error=str(exc),
        )


def run_refresh(
    *,
    through_date: str,
    instrument_id: str,
    run_id: str,
    timeout: float = 60.0,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, object]:
    checked_through = source._iso_date(through_date, "through_date")
    checked_instrument = source._instrument_id(instrument_id)
    checked_run = source._safe_token(run_id, "run_id")
    root = source._data_root()
    prior = _load_previous(root, checked_instrument)
    attempted_at = _iso_utc(now_fn())
    observed, current_date, previous_date = _resolve_observed_trade_dates(
        checked_through,
        instrument_id=checked_instrument,
        timeout=timeout,
    )
    prior_current = prior.get(CURRENT_ROLE) if isinstance(prior, Mapping) else None
    prior_previous = prior.get(PREVIOUS_ROLE) if isinstance(prior, Mapping) else None

    current = _record_with_failure_semantics(
        root=root,
        instrument_id=checked_instrument,
        trade_date=current_date,
        role=CURRENT_ROLE,
        run_id=checked_run,
        timeout=timeout,
        attempted_at=attempted_at,
        prior=prior_current if isinstance(prior_current, Mapping) else None,
        now_fn=now_fn,
    )
    previous = _record_with_failure_semantics(
        root=root,
        instrument_id=checked_instrument,
        trade_date=previous_date,
        role=PREVIOUS_ROLE,
        run_id=checked_run,
        timeout=timeout,
        attempted_at=attempted_at,
        prior=prior_previous if isinstance(prior_previous, Mapping) else None,
        now_fn=now_fn,
    )

    both_fresh = current["status"] == "FRESH" and previous["status"] == "FRESH"
    any_factual = isinstance(current.get("factual"), Mapping) or isinstance(
        previous.get("factual"), Mapping
    )
    status = "PASS" if both_fresh else ("PARTIAL" if any_factual else "FAILED")
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": status,
        "source_id": source.SOURCE_ID,
        "instrument_id": checked_instrument,
        "run_id": checked_run,
        "through_date": checked_through,
        "refresh_attempted_at": attempted_at,
        "observed_trade_dates": observed,
        "observed_current_trade_date": current_date,
        "previous_observed_trade_date": previous_date,
        CURRENT_ROLE: current,
        PREVIOUS_ROLE: previous,
        "quality_status": "PASS" if any_factual else "FAILED",
        "acceptance_status": "PASS" if any_factual else "FAILED",
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_required": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
        "calendar_dependency": False,
        "weekday_weekend_inference": False,
    }
    source._atomic_json(_artifact_path(root, checked_instrument), payload)
    return payload


def _failed_instrument_result(
    instrument_id: str,
    *,
    through_date: str,
    attempted_at: str,
    exc: Exception,
    prior: Mapping[str, object] | None,
) -> dict[str, object]:
    current_prior = prior.get(CURRENT_ROLE) if isinstance(prior, Mapping) else None
    previous_prior = prior.get(PREVIOUS_ROLE) if isinstance(prior, Mapping) else None
    current = _retained_record(
        current_prior if isinstance(current_prior, Mapping) else None,
        role=CURRENT_ROLE,
        expected_trade_date=None,
        attempted_at=attempted_at,
        error_class=exc.__class__.__name__,
        error=str(exc),
        require_same_trade_date=False,
    ) or _empty_record(
        role=CURRENT_ROLE,
        expected_trade_date=None,
        attempted_at=attempted_at,
        status="ERROR",
        error_class=exc.__class__.__name__,
        error=str(exc),
    )
    previous = _retained_record(
        previous_prior if isinstance(previous_prior, Mapping) else None,
        role=PREVIOUS_ROLE,
        expected_trade_date=(
            str(previous_prior.get("trade_date"))
            if isinstance(previous_prior, Mapping) and previous_prior.get("trade_date")
            else None
        ),
        attempted_at=attempted_at,
        error_class=exc.__class__.__name__,
        error=str(exc),
        require_same_trade_date=False,
    ) or _empty_record(
        role=PREVIOUS_ROLE,
        expected_trade_date=None,
        attempted_at=attempted_at,
        status="ERROR",
        error_class=exc.__class__.__name__,
        error=str(exc),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": (
            "PARTIAL"
            if isinstance(current.get("factual"), Mapping)
            or isinstance(previous.get("factual"), Mapping)
            else "FAILED"
        ),
        "source_id": source.SOURCE_ID,
        "instrument_id": instrument_id,
        "through_date": through_date,
        "refresh_attempted_at": attempted_at,
        "observed_trade_dates": [],
        "observed_current_trade_date": None,
        "previous_observed_trade_date": None,
        CURRENT_ROLE: current,
        PREVIOUS_ROLE: previous,
        "quality_status": "PARTIAL",
        "acceptance_status": "PARTIAL",
        "error_class": exc.__class__.__name__,
        "error": str(exc),
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_required": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
        "calendar_dependency": False,
        "weekday_weekend_inference": False,
    }


def run_refresh_all(
    *,
    through_date: str,
    run_id: str,
    timeout: float = 60.0,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, object]:
    checked_through = source._iso_date(through_date, "through_date")
    checked_run = source._safe_token(run_id, "run_id")
    root = source._data_root()
    attempted_at = _iso_utc(now_fn())
    results: dict[str, object] = {}
    failed: list[str] = []
    for instrument_id in source.LIVE_INSTRUMENT_IDS:
        try:
            result = run_refresh(
                through_date=checked_through,
                instrument_id=instrument_id,
                run_id=checked_run + "_" + instrument_id,
                timeout=timeout,
                now_fn=now_fn,
            )
        except Exception as exc:
            try:
                prior = _load_previous(root, instrument_id)
            except Exception:
                prior = None
            result = _failed_instrument_result(
                instrument_id,
                through_date=checked_through,
                attempted_at=attempted_at,
                exc=exc,
                prior=prior,
            )
            source._atomic_json(_artifact_path(root, instrument_id), result)
        results[instrument_id] = result
        if result.get("status") != "PASS":
            failed.append(instrument_id)
    aggregate = (
        "PASS"
        if not failed
        else ("FAILED" if len(failed) == len(source.LIVE_INSTRUMENT_IDS) else "PARTIAL")
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": aggregate,
        "run_id": checked_run,
        "through_date": checked_through,
        "instrument_ids": list(source.LIVE_INSTRUMENT_IDS),
        "instrument_results": results,
        "failed_instrument_ids": failed,
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }

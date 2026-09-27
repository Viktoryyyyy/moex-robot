from __future__ import annotations

import json
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Final

import pandas as pd

from . import futoi_live_factual_refresh_source_native as source

PROJECT: Final[str] = source.PROJECT
SCHEMA_VERSION: Final[str] = "futoi_intraday_previous_session_context.v1"
SCHEMA_VERSION_V2: Final[str] = "futoi_intraday_previous_session_context.v2"
ARTIFACT_FILENAME: Final[str] = "intraday_previous_session_context.json"
SOURCE_LOOKBACK_DAYS: Final[int] = source.SOURCE_LOOKBACK_DAYS
CURRENT_ROLE: Final[str] = "current_intraday"
PREVIOUS_ROLE: Final[str] = "previous_completed_session"


class FutoiIntradayContextError(ValueError):
    pass


def _fail(message: str) -> None:
    raise FutoiIntradayContextError(message)


def _artifact_path(root: Path, instrument_id: str, *, raw_schema_version: str = "v1") -> Path:
    if source._raw_version(raw_schema_version) == "v2":
        path = source._current_path(root, instrument_id, raw_schema_version="v2").parent / ARTIFACT_FILENAME
        return source._checked_root_path(root, path.relative_to(root))
    return source._current_path(root, instrument_id).parent / ARTIFACT_FILENAME


def _load_previous(root: Path, instrument_id: str, *, raw_schema_version: str = "v1") -> dict[str, object] | None:
    if source._raw_version(raw_schema_version) == "v2":
        return _load_root_context(root, instrument_id)
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
            retained["failed_attempt_evidence"] = getattr(exc, "attempt_provenance", None)
            return retained
        status = (
            "PENDING"
            if "pending on authoritative observed TradeStats date" in str(exc)
            else "ERROR"
        )
        failed = _empty_record(
            role=role,
            expected_trade_date=trade_date,
            attempted_at=attempted_at,
            status=status,
            error_class=exc.__class__.__name__,
            error=str(exc),
        )
        failed["failed_attempt_evidence"] = getattr(exc, "attempt_provenance", None)
        return failed


def run_refresh(
    *,
    through_date: str,
    instrument_id: str,
    run_id: str,
    timeout: float = 60.0,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    raw_schema_version: str = "v1",
) -> dict[str, object]:
    if source._raw_version(raw_schema_version) == "v2":
        return _run_root_context(
            through_date=through_date, run_id=run_id, timeout=timeout, now_fn=now_fn,
            instrument_id=instrument_id,
            parallel=False,
        )
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
    raw_schema_version: str = "v1",
) -> dict[str, object]:
    if source._raw_version(raw_schema_version) == "v2":
        return _run_root_context_all(
            through_date=through_date, run_id=run_id, timeout=timeout, now_fn=now_fn,
            parallel=False,
        )
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


class NoObservedCurrentTradeDate(FutoiIntradayContextError):
    pass


class NoObservedPreviousTradeDate(FutoiIntradayContextError):
    pass


def _root_role_witness(
    through_date: str, role: str, *, timeout: float, observations: list[dict[str, object]],
) -> str:
    """Resolve one role only; a failed witness never changes another role."""
    end = date.fromisoformat(source._iso_date(through_date, "through_date"))
    if role not in (CURRENT_ROLE, PREVIOUS_ROLE):
        _fail("unknown FUTOI context role")
    secid = source.observed_dates.reference_secid(source.DATE_WITNESS_INSTRUMENT_ID)
    if secid != source.DATE_WITNESS_SECID:
        _fail("v2 USDRUBF date-witness registry binding mismatch")
    lower = end if role == CURRENT_ROLE else end - timedelta(days=SOURCE_LOOKBACK_DAYS - 1)
    candidate = end if role == CURRENT_ROLE else end - timedelta(days=1)
    while candidate >= lower:
        observation = {
            "trade_date": candidate.isoformat(),
            "witness_instrument_id": source.DATE_WITNESS_INSTRUMENT_ID,
            "witness_secid": secid, "date_authority_source_id": source.observed_dates.SOURCE_ID,
            "futoi_availability_proven": False,
        }
        observations.append(observation)
        try:
            found = source.observed_dates._exact_date_has_secid(
                candidate, secid=secid, timeout=timeout, apim_base_url=None,
            )
            if type(found) is not bool:
                _fail("observed TradeStats witness must return a boolean")
        except Exception as exc:
            observation.update(status="WITNESS_ERROR", error_class=type(exc).__name__, error=str(exc))
            raise  # Never skip an uncertain date to find an older convenient date.
        observation["status"] = "OBSERVED_TRADESTATS_DATE" if found else "NO_OBSERVED_TRADESTATS_DATE"
        if found:
            return candidate.isoformat()
        candidate -= timedelta(days=1)
    error_type = NoObservedCurrentTradeDate if role == CURRENT_ROLE else NoObservedPreviousTradeDate
    raise error_type("no observed USDRUBF date witness for " + role + " through " + through_date)


def _root_role_record(
    *, root: Path, instrument_id: str, through_date: str, role: str, run_id: str,
    timeout: float, attempted_at: str, now_fn: Callable[[], datetime],
) -> dict[str, object]:
    observations: list[dict[str, object]] = []
    target = None
    provenance = None
    record = {
        "role": role, "instrument_id": instrument_id, "source_id": source.SOURCE_ID,
        "source_ticker": source.ROOT_TICKERS[instrument_id],
        "raw_schema_version": "v2", "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
        "status": "ERROR", "availability_state": "ERROR", "expected_trade_date": None,
        "trade_date": None, "refresh_attempted_at": attempted_at, "last_success_at": None,
        "failed_attempt_at": None, "refresh_error_class": None, "refresh_error": None,
        "factual": None, "provenance": None, "date_witness_observations": observations,
        "consumer_factual_use_allowed": False,
    }
    try:
        target = _root_role_witness(through_date, role, timeout=timeout, observations=observations)
        record["expected_trade_date"] = target
        # The existing materializer performs the own-ticker exact-date request.
        # Do not run the legacy SECID-bound probe or select a second response.
        _, provenance = source._materialize_target(
            root, target, run_id + "_" + role, instrument_id=instrument_id,
            timeout=timeout, raw_schema_version="v2",
        )
        factual = source.replay_root_factual(
            root, provenance, instrument_id=instrument_id, trade_date=target,
        )
        completed = source._aware_utc(now_fn(), role + ".last_success_at")
        started = source._aware_utc(attempted_at, role + ".refresh_attempted_at")
        receipt = source._aware_utc(factual["availability_ts_utc"], role + ".receipt")
        ingest = source._aware_utc(factual["ingest_ts_utc"], role + ".ingest")
        if not started <= receipt <= ingest <= completed:
            _fail("v2 role refresh/receipt/ingest/validation clocks are inconsistent")
        record.update(status="FRESH", availability_state="AVAILABLE", trade_date=target,
                      last_success_at=completed.isoformat(), factual=factual, provenance=provenance)
    except Exception as exc:
        missing = isinstance(exc, (NoObservedCurrentTradeDate, NoObservedPreviousTradeDate))
        empty = source._is_explicit_empty_source(exc)
        status = "UNAVAILABLE" if missing else ("PENDING" if empty else "ERROR")
        record.update(
            status=status, availability_state=status,
            refresh_error_class=type(exc).__name__, refresh_error=str(exc),
            failed_attempt_evidence=getattr(exc, "attempt_provenance", provenance),
            failure_stage="date_witness" if target is None else "futoi_materialization_or_admission",
        )
        try:
            failed_at = source._aware_utc(now_fn(), role + ".failed_attempt_at")
            if failed_at < source._aware_utc(attempted_at, "refresh_attempted_at"):
                _fail("v2 role failure clock precedes refresh start")
            record["failed_attempt_at"] = failed_at.isoformat()
        except Exception as clock_error:
            record["failure_clock_error"] = type(clock_error).__name__ + ": " + str(clock_error)
    record["freshness"] = {
        **_freshness(state=str(record["status"]), expected_trade_date=target,
                     factual=record["factual"]),
        "policy": "independent_usdrubf_role_witness_then_own_exact_futoi_v2",
        "witness_instrument_id": source.DATE_WITNESS_INSTRUMENT_ID,
        "witness_secid": source.DATE_WITNESS_SECID,
        "scope": "observed_date_match_not_consumer_ttl_or_session_completeness",
    }
    return record


def _root_context_payload(
    *, instrument_id: str, through_date: str, run_id: str, attempted_at: str,
    records: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    fresh = [role for role, record in records.items() if record.get("status") == "FRESH"]
    status = "PASS" if len(fresh) == 2 else ("PARTIAL" if fresh else "FAILED")
    dates = sorted({
        str(record["expected_trade_date"]) for record in records.values()
        if record.get("expected_trade_date") is not None
    })
    return {
        "project": PROJECT, "schema_version": SCHEMA_VERSION_V2,
        "raw_schema_version": "v2", "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
        "source_id": source.SOURCE_ID, "instrument_id": instrument_id,
        "source_ticker": source.ROOT_TICKERS[instrument_id], "run_id": run_id,
        "status": status, "through_date": through_date, "refresh_attempted_at": attempted_at,
        "observed_trade_dates": dates, "observed_trade_dates_scope": "independent_role_witness_set",
        "observed_current_trade_date": records[CURRENT_ROLE].get("expected_trade_date"),
        "previous_observed_trade_date": records[PREVIOUS_ROLE].get("expected_trade_date"),
        **records, "quality_status": status, "acceptance_status": status,
        "retention_policy": "prior_frozen_evidence_only_no_factual_fallback",
        "factual_authority": False, "directional_authority": False, "action_authority": False,
        "standalone_buy_sell_authority": False, "stage5_full_mode_required": False,
        "stage5_full_mode_ready": False, "stage5_pointer_promotion_performed": False,
        "session_completion_proven": False, "historical_pit_research_ready_claimed": False,
        "model_usable": False, "calendar_dependency": False, "weekday_weekend_inference": False,
    }


def _run_root_context(
    *, through_date: str, instrument_id: str, run_id: str, timeout: float,
    now_fn: Callable[[], datetime], parallel: bool,
) -> dict[str, object]:
    checked = source._instrument_id(instrument_id)
    through = source._iso_date(through_date, "through_date")
    run_id = source._safe_token(run_id, "run_id")
    root = source._data_root()
    path = _artifact_path(root, checked, raw_schema_version="v2")
    started = source._aware_utc(now_fn(), "refresh_attempted_at")
    if through != started.tz_convert(source.MARKET_TZ).date().isoformat():
        _fail("v2 intraday through_date must equal the refresh-start Europe/Moscow date")
    attempted_at = started.isoformat()

    def refresh_role(role: str) -> dict[str, object]:
        return _root_role_record(
            root=root, instrument_id=checked, through_date=through, role=role,
            run_id=run_id, timeout=timeout, attempted_at=attempted_at, now_fn=now_fn,
        )

    roles = (CURRENT_ROLE, PREVIOUS_ROLE)
    if parallel:
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="futoi-session-v2") as executor:
            futures = {role: executor.submit(refresh_role, role) for role in roles}
            records = {role: futures[role].result() for role in roles}
    else:
        records = {role: refresh_role(role) for role in roles}
    payload = _root_context_payload(
        instrument_id=checked, through_date=through, run_id=run_id,
        attempted_at=attempted_at, records=records,
    )
    # Retain immutable prior evidence, not an old fact labelled as a new role.
    source._archive_root_context(root, path, payload)
    return payload


def _run_root_context_all(
    *, through_date: str, run_id: str, timeout: float,
    now_fn: Callable[[], datetime], parallel: bool,
) -> dict[str, object]:
    through = source._iso_date(through_date, "through_date")
    checked_run = source._safe_token(run_id, "run_id")
    root = source._data_root()
    attempted_at = source._aware_utc(now_fn(), "refresh_attempted_at").isoformat()

    def refresh_one(instrument_id: str) -> dict[str, object]:
        instrument_run = checked_run + "_" + instrument_id
        try:
            return _run_root_context(
                through_date=through, instrument_id=instrument_id, run_id=instrument_run,
                timeout=timeout, now_fn=now_fn, parallel=parallel,
            )
        except Exception as exc:
            records = {}
            failed_at = None
            failure_clock_error = None
            try:
                stamp = source._aware_utc(now_fn(), "failed_attempt_at")
                if stamp < source._aware_utc(attempted_at, "refresh_attempted_at"):
                    _fail("v2 failure clock precedes refresh start")
                failed_at = stamp.isoformat()
            except Exception as clock_error:
                failure_clock_error = type(clock_error).__name__ + ": " + str(clock_error)
            for role in (CURRENT_ROLE, PREVIOUS_ROLE):
                record = _empty_record(role=role, expected_trade_date=None, attempted_at=attempted_at,
                                       status="ERROR", error_class=type(exc).__name__, error=str(exc))
                record.update(raw_schema_version="v2", source_identity_scope=source.ROOT_IDENTITY_SCOPE,
                              instrument_id=instrument_id, source_id=source.SOURCE_ID,
                              source_ticker=source.ROOT_TICKERS[instrument_id],
                              date_witness_observations=[], consumer_factual_use_allowed=False,
                              failed_attempt_at=failed_at)
                if failure_clock_error:
                    record["failure_clock_error"] = failure_clock_error
                record["freshness"].update(
                    policy="independent_usdrubf_role_witness_then_own_exact_futoi_v2",
                    witness_instrument_id=source.DATE_WITNESS_INSTRUMENT_ID,
                    witness_secid=source.DATE_WITNESS_SECID,
                    scope="observed_date_match_not_consumer_ttl_or_session_completeness",
                )
                records[role] = record
            payload = _root_context_payload(instrument_id=instrument_id, through_date=through,
                                            run_id=instrument_run, attempted_at=attempted_at, records=records)
            payload.update(error_class=type(exc).__name__, error=str(exc))
            try:
                source._archive_root_context(
                    root, _artifact_path(root, instrument_id, raw_schema_version="v2"), payload,
                )
            except Exception as persistence_error:
                payload["failure_persistence_error"] = type(persistence_error).__name__ + ": " + str(persistence_error)
            return payload

    instruments = source.LIVE_INSTRUMENT_IDS
    if parallel:
        with ThreadPoolExecutor(max_workers=len(instruments), thread_name_prefix="futoi-context-v2") as executor:
            futures = {instrument: executor.submit(refresh_one, instrument) for instrument in instruments}
            results = {instrument: futures[instrument].result() for instrument in instruments}
    else:
        results = {instrument: refresh_one(instrument) for instrument in instruments}
    failed = [instrument for instrument, result in results.items() if result["status"] != "PASS"]
    fresh_any = any(result["status"] in ("PASS", "PARTIAL") for result in results.values())
    return {
        "project": PROJECT, "schema_version": SCHEMA_VERSION_V2, "raw_schema_version": "v2",
        "source_identity_scope": source.ROOT_IDENTITY_SCOPE,
        "status": "PASS" if not failed else ("PARTIAL" if fresh_any else "FAILED"),
        "run_id": checked_run, "through_date": through,
        "instrument_ids": list(instruments), "instrument_results": results, "failed_instrument_ids": failed,
        "factual_authority": False, "directional_authority": False, "action_authority": False,
        "standalone_buy_sell_authority": False, "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }


def _load_root_context(root: Path, instrument_id: str) -> dict[str, object] | None:
    """Read a version-bound run envelope; consumer admission is a separate check."""
    path = _artifact_path(root, instrument_id, raw_schema_version="v2")
    if not path.exists():
        return None
    value = source._load_json(path, "v2 FUTOI intraday context")
    expected = {
        "project": PROJECT, "schema_version": SCHEMA_VERSION_V2, "raw_schema_version": "v2",
        "source_identity_scope": source.ROOT_IDENTITY_SCOPE, "instrument_id": instrument_id,
        "source_id": source.SOURCE_ID, "source_ticker": source.ROOT_TICKERS[instrument_id],
    }
    if "secid" in value or any(value.get(k) != v for k, v in expected.items()):
        _fail("v2 FUTOI role envelope version/identity mismatch")
    archived = json.loads(source._root_proof_bytes(
        root, value.get("run_evidence_ref"), value.get("run_evidence_sha256"), ".json",
    ))
    if archived != {k: v for k, v in value.items() if k not in ("run_evidence_ref", "run_evidence_sha256")}:
        _fail("v2 FUTOI role envelope differs from frozen evidence")
    for flag in ("factual_authority", "directional_authority", "action_authority",
                 "standalone_buy_sell_authority", "stage5_full_mode_ready",
                 "stage5_pointer_promotion_performed", "session_completion_proven",
                 "historical_pit_research_ready_claimed", "model_usable"):
        if value.get(flag) is not False:
            _fail("v2 FUTOI role envelope must not grant " + flag)
    for role in (CURRENT_ROLE, PREVIOUS_ROLE):
        record = value.get(role)
        if (not isinstance(record, Mapping) or record.get("role") != role or "secid" in record
                or record.get("raw_schema_version") != "v2"
                or record.get("source_identity_scope") != source.ROOT_IDENTITY_SCOPE
                or record.get("instrument_id") != instrument_id
                or record.get("source_id") != source.SOURCE_ID
                or record.get("source_ticker") != source.ROOT_TICKERS[instrument_id]
                or record.get("consumer_factual_use_allowed") is not False):
            _fail("v2 FUTOI role record version/identity mismatch")
        if record.get("status") == "FRESH":
            factual = record.get("factual")
            if (not isinstance(factual, Mapping) or "secid" in factual
                    or factual.get("raw_schema_version") != "v2"
                    or factual.get("source_identity_scope") != source.ROOT_IDENTITY_SCOPE
                    or factual.get("trade_date") != record.get("expected_trade_date")
                    or factual.get("trade_date") != record.get("trade_date")):
                _fail("v2 FUTOI fresh role factual version/date mismatch")
        elif record.get("factual") is not None:
            _fail("v2 FUTOI failed role must not carry an older factual fallback")
    return value

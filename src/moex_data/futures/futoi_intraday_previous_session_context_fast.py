from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from . import futoi_intraday_previous_session_context as core
from . import futoi_live_factual_refresh_source_native as source


PROJECT = core.PROJECT
SCHEMA_VERSION = core.SCHEMA_VERSION
ARTIFACT_FILENAME = core.ARTIFACT_FILENAME
SOURCE_LOOKBACK_DAYS = core.SOURCE_LOOKBACK_DAYS
CURRENT_ROLE = core.CURRENT_ROLE
PREVIOUS_ROLE = core.PREVIOUS_ROLE
FutoiIntradayContextError = core.FutoiIntradayContextError


def _resolve_observed_trade_dates_fast(
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
        secid = source.observed_dates.reference_secid(checked_instrument)
        found_desc: list[str] = []
        candidate = end
        through_is_observed = False
        while candidate >= start:
            is_observed = source.observed_dates._exact_date_has_secid(
                candidate,
                secid=secid,
                timeout=timeout,
                apim_base_url=None,
            )
            if candidate == end:
                through_is_observed = is_observed
            if is_observed:
                found_desc.append(candidate.isoformat())
                required = 2 if through_is_observed else 1
                if len(found_desc) >= required:
                    break
            candidate -= timedelta(days=1)
    except Exception as exc:
        raise FutoiIntradayContextError(
            "authoritative observed TradeStats date selection failed for "
            + checked_instrument
            + ": "
            + str(exc)
        ) from exc

    if not found_desc:
        core._fail("authoritative observed TradeStats date selection returned no dates")

    if through_is_observed:
        current_trade_date: str | None = checked
        previous_trade_date = found_desc[1] if len(found_desc) > 1 else None
    else:
        current_trade_date = None
        previous_trade_date = found_desc[0]
    return sorted(found_desc), current_trade_date, previous_trade_date


def _materialize_record(
    *,
    root,
    instrument_id: str,
    trade_date: str,
    role: str,
    run_id: str,
    timeout: float,
    attempted_at: str,
    now_fn: Callable[[], datetime],
) -> dict[str, object]:
    identity = source.source_identity(instrument_id)
    try:
        partition_path, provenance = source._materialize_target(
            root,
            trade_date,
            run_id + "_" + role,
            instrument_id=instrument_id,
            timeout=timeout,
        )
    except Exception as exc:
        if source._is_explicit_empty_source(exc):
            raise FutoiIntradayContextError(
                "FUTOI exact source pending on authoritative observed TradeStats date "
                + trade_date
            ) from exc
        raise
    frame = pd.read_parquet(partition_path)
    factual = source.latest_aligned_factual(
        frame,
        expected_trade_date=trade_date,
        expected_instrument_id=instrument_id,
        expected_source_ticker=identity["source_ticker"],
        expected_secid=identity["secid"],
    )
    completed_at = core._iso_utc(now_fn())
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
        "freshness": core._freshness(
            state="FRESH",
            expected_trade_date=trade_date,
            factual=factual,
        ),
        "factual": factual,
        "provenance": provenance,
    }


def _record_with_failure_semantics(
    *,
    root,
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
        retained = core._retained_record(
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
        return core._empty_record(
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
        retained = core._retained_record(
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
        return core._empty_record(
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
    prior = core._load_previous(root, checked_instrument)
    attempted_at = core._iso_utc(now_fn())
    observed, current_date, previous_date = _resolve_observed_trade_dates_fast(
        checked_through,
        instrument_id=checked_instrument,
        timeout=timeout,
    )
    prior_current = prior.get(CURRENT_ROLE) if isinstance(prior, Mapping) else None
    prior_previous = prior.get(PREVIOUS_ROLE) if isinstance(prior, Mapping) else None

    role_inputs = {
        CURRENT_ROLE: (
            current_date,
            prior_current if isinstance(prior_current, Mapping) else None,
        ),
        PREVIOUS_ROLE: (
            previous_date,
            prior_previous if isinstance(prior_previous, Mapping) else None,
        ),
    }

    def refresh_role(role: str) -> dict[str, object]:
        trade_date, prior_role = role_inputs[role]
        return _record_with_failure_semantics(
            root=root,
            instrument_id=checked_instrument,
            trade_date=trade_date,
            role=role,
            run_id=checked_run,
            timeout=timeout,
            attempted_at=attempted_at,
            prior=prior_role,
            now_fn=now_fn,
        )

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="futoi-session") as executor:
        role_futures = {
            role: executor.submit(refresh_role, role)
            for role in (CURRENT_ROLE, PREVIOUS_ROLE)
        }
        current = role_futures[CURRENT_ROLE].result()
        previous = role_futures[PREVIOUS_ROLE].result()

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
        "observed_trade_dates_scope": "minimal_current_previous_witness_set",
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
    source._atomic_json(core._artifact_path(root, checked_instrument), payload)
    return payload


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
    attempted_at = core._iso_utc(now_fn())
    instrument_ids = tuple(source.LIVE_INSTRUMENT_IDS)

    def refresh_one(instrument_id: str) -> dict[str, object]:
        try:
            return run_refresh(
                through_date=checked_through,
                instrument_id=instrument_id,
                run_id=checked_run + "_" + instrument_id,
                timeout=timeout,
                now_fn=now_fn,
            )
        except Exception as exc:
            try:
                prior = core._load_previous(root, instrument_id)
            except Exception:
                prior = None
            result = core._failed_instrument_result(
                instrument_id,
                through_date=checked_through,
                attempted_at=attempted_at,
                exc=exc,
                prior=prior,
            )
            source._atomic_json(core._artifact_path(root, instrument_id), result)
            return result

    with ThreadPoolExecutor(
        max_workers=len(instrument_ids),
        thread_name_prefix="futoi-context",
    ) as executor:
        futures = {
            instrument_id: executor.submit(refresh_one, instrument_id)
            for instrument_id in instrument_ids
        }
        results = {
            instrument_id: futures[instrument_id].result()
            for instrument_id in instrument_ids
        }

    failed = [
        instrument_id
        for instrument_id in instrument_ids
        if results[instrument_id].get("status") != "PASS"
    ]
    aggregate = (
        "PASS"
        if not failed
        else ("FAILED" if len(failed) == len(instrument_ids) else "PARTIAL")
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "status": aggregate,
        "run_id": checked_run,
        "through_date": checked_through,
        "instrument_ids": list(instrument_ids),
        "instrument_results": results,
        "failed_instrument_ids": failed,
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }

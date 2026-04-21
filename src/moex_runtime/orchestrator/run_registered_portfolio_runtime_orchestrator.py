from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from uuid import uuid4

from src.moex_core.contracts.external_pattern_artifact_path_resolver import resolve_external_pattern_artifact_path
from src.moex_runtime.engine.run_registered_runtime_boundary import run_registered_runtime_boundary
from src.moex_strategy_sdk.errors import StrategyRegistrationError

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PORTFOLIO_RUN_REPORT_LOCATOR_REF = "data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json"


def _load_json(repo_relative_path: str) -> Mapping[str, object]:
    path = _REPO_ROOT / repo_relative_path
    if not path.exists():
        raise StrategyRegistrationError("missing registry/config file: " + repo_relative_path)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise StrategyRegistrationError("registry/config file must contain JSON object: " + repo_relative_path)
    return payload


def _load_portfolio_record(portfolio_id: str) -> Mapping[str, object]:
    return _load_json("configs/portfolios/" + portfolio_id + ".json")


def _load_environment_record(environment_id: str) -> Mapping[str, object]:
    return _load_json("configs/environments/" + environment_id + ".json")


def _load_enabled_strategy_ids(portfolio_record: Mapping[str, object]) -> tuple[str, ...]:
    enabled_strategy_ids = portfolio_record.get("enabled_strategy_ids")
    if not isinstance(enabled_strategy_ids, list) or not enabled_strategy_ids:
        raise StrategyRegistrationError("portfolio enabled_strategy_ids must be non-empty list")
    normalized: list[str] = []
    for strategy_id in enabled_strategy_ids:
        if not isinstance(strategy_id, str) or not strategy_id:
            raise StrategyRegistrationError("portfolio enabled_strategy_ids must contain non-empty strings")
        normalized.append(strategy_id)
    return tuple(normalized)


def _new_portfolio_run_id(started_at_utc: datetime) -> str:
    return started_at_utc.strftime("%Y%m%dT%H%M%S_%fZ") + "_" + uuid4().hex[:8]


def _write_portfolio_run_report(*, portfolio_result: Mapping[str, object], environment_record: Mapping[str, object]) -> None:
    report_path = resolve_external_pattern_artifact_path(locator_ref=_PORTFOLIO_RUN_REPORT_LOCATOR_REF, environment_record=environment_record, format_kwargs={"portfolio_id": portfolio_result["portfolio_id"], "portfolio_run_id": portfolio_result["portfolio_run_id"]})
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(portfolio_result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def _build_failure_summary(*, strategy_id: str, failed_at_utc: str, exc: Exception) -> dict[str, object]:
    return {
        "failure_summary_schema_version": 1,
        "failure_scope": "delegated_runtime",
        "failed_strategy_id": strategy_id,
        "failed_at_utc": failed_at_utc,
        "error_type": type(exc).__name__,
        "error_message": str(exc),
    }


def _parse_report_timestamp_utc(timestamp_utc: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(timestamp_utc)
    except ValueError as exc:
        raise StrategyRegistrationError("portfolio runtime report timestamp must be valid ISO-8601") from exc
    if parsed.tzinfo is None:
        raise StrategyRegistrationError("portfolio runtime report timestamp must be timezone-aware UTC ISO-8601")
    return parsed.astimezone(timezone.utc)


def _iter_usable_latest_bar_end_utc(delegated_strategy_results: tuple[Mapping[str, object], ...] | list[Mapping[str, object]]) -> tuple[datetime, ...]:
    parsed_latest_bar_ends: list[datetime] = []
    for delegated_strategy_result in delegated_strategy_results:
        if not isinstance(delegated_strategy_result, Mapping):
            continue
        if delegated_strategy_result.get("ok") is not True:
            continue
        delegated_result = delegated_strategy_result.get("result")
        if not isinstance(delegated_result, Mapping):
            continue
        latest_bar_end = delegated_result.get("latest_bar_end")
        if not isinstance(latest_bar_end, str) or not latest_bar_end:
            continue
        parsed_latest_bar_ends.append(_parse_report_timestamp_utc(latest_bar_end))
    return tuple(parsed_latest_bar_ends)


def _iter_usable_signal_counts(delegated_strategy_results: tuple[Mapping[str, object], ...] | list[Mapping[str, object]]) -> tuple[int, ...]:
    usable_signal_counts: list[int] = []
    for delegated_strategy_result in delegated_strategy_results:
        if not isinstance(delegated_strategy_result, Mapping):
            continue
        if delegated_strategy_result.get("ok") is not True:
            continue
        delegated_result = delegated_strategy_result.get("result")
        if not isinstance(delegated_result, Mapping):
            continue
        signal_count = delegated_result.get("signal_count")
        if not isinstance(signal_count, int) or isinstance(signal_count, bool):
            continue
        if signal_count < 0:
            raise StrategyRegistrationError("delegated signal_count must be non-negative integer for signal activity summary")
        usable_signal_counts.append(signal_count)
    return tuple(usable_signal_counts)


def _classify_signal_activity_status(*, reporting_strategy_count: int, active_strategy_count: int, total_signal_count: int) -> str:
    if reporting_strategy_count == 0:
        if active_strategy_count != 0 or total_signal_count != 0:
            raise StrategyRegistrationError("unavailable signal activity status requires zero active strategies and zero total signals")
        return "unavailable"
    if active_strategy_count == 0 and total_signal_count == 0:
        return "idle"
    if active_strategy_count > 0 and total_signal_count > 0:
        return "active"
    raise StrategyRegistrationError("signal activity summary counts must produce a valid normalized status")


def _build_signal_activity_summary(*, enabled_strategy_ids: tuple[str, ...], delegated_strategy_results: tuple[Mapping[str, object], ...] | list[Mapping[str, object]]) -> dict[str, object]:
    usable_signal_counts = _iter_usable_signal_counts(delegated_strategy_results)
    reporting_strategy_count = len(usable_signal_counts)
    active_strategy_count = sum(1 for signal_count in usable_signal_counts if signal_count > 0)
    total_signal_count = sum(usable_signal_counts)
    if active_strategy_count > reporting_strategy_count:
        raise StrategyRegistrationError("active strategy count must not exceed reporting strategy count for signal activity summary")
    if reporting_strategy_count == 0:
        signal_coverage_status = "unavailable"
    elif reporting_strategy_count == len(enabled_strategy_ids):
        signal_coverage_status = "complete"
    else:
        signal_coverage_status = "partial"
    return {
        "signal_activity_summary_schema_version": 1,
        "signal_activity_scope": "portfolio_runtime_signals",
        "signal_coverage_status": signal_coverage_status,
        "reporting_strategy_count": reporting_strategy_count,
        "active_strategy_count": active_strategy_count,
        "total_signal_count": total_signal_count,
        "signal_activity_status": _classify_signal_activity_status(reporting_strategy_count=reporting_strategy_count, active_strategy_count=active_strategy_count, total_signal_count=total_signal_count),
    }


def _build_data_freshness_summary(*, enabled_strategy_ids: tuple[str, ...], delegated_strategy_results: tuple[Mapping[str, object], ...] | list[Mapping[str, object]]) -> dict[str, object]:
    parsed_latest_bar_ends = _iter_usable_latest_bar_end_utc(delegated_strategy_results)
    if not parsed_latest_bar_ends:
        freshness_coverage_status = "unavailable"
        oldest_latest_bar_end_utc = None
        newest_latest_bar_end_utc = None
        latest_bar_end_span_seconds = None
    else:
        oldest_latest_bar_end = min(parsed_latest_bar_ends)
        newest_latest_bar_end = max(parsed_latest_bar_ends)
        if len(parsed_latest_bar_ends) == len(enabled_strategy_ids):
            freshness_coverage_status = "complete"
        else:
            freshness_coverage_status = "partial"
        oldest_latest_bar_end_utc = oldest_latest_bar_end.isoformat()
        newest_latest_bar_end_utc = newest_latest_bar_end.isoformat()
        latest_bar_end_span_seconds = float((newest_latest_bar_end - oldest_latest_bar_end).total_seconds())
    return {
        "data_freshness_summary_schema_version": 1,
        "freshness_scope": "portfolio_runtime_inputs",
        "freshness_coverage_status": freshness_coverage_status,
        "oldest_latest_bar_end_utc": oldest_latest_bar_end_utc,
        "newest_latest_bar_end_utc": newest_latest_bar_end_utc,
        "latest_bar_end_span_seconds": latest_bar_end_span_seconds,
    }


def _build_run_timing_summary(*, started_at_utc: str, completed_at_utc: str) -> dict[str, object]:
    started_at = _parse_report_timestamp_utc(started_at_utc)
    completed_at = _parse_report_timestamp_utc(completed_at_utc)
    wall_clock_duration_seconds = (completed_at - started_at).total_seconds()
    if wall_clock_duration_seconds < 0:
        raise StrategyRegistrationError("portfolio runtime report completed_at_utc must not be earlier than started_at_utc")
    return {
        "run_timing_summary_schema_version": 1,
        "timing_scope": "portfolio_run_wall_clock",
        "wall_clock_duration_seconds": float(wall_clock_duration_seconds),
    }


def _classify_delegated_outcome_status(*, enabled_strategy_count: int, delegated_success_count: int, delegated_failure_count: int) -> str:
    if enabled_strategy_count == 0:
        return "none_enabled"
    if delegated_success_count == enabled_strategy_count and delegated_failure_count == 0:
        return "all_succeeded"
    if delegated_success_count == 0 and delegated_failure_count == enabled_strategy_count:
        return "all_failed"
    if delegated_success_count > 0 and delegated_failure_count > 0:
        return "partial_failure"
    raise StrategyRegistrationError("delegated outcome summary counts must produce a valid normalized status")


def _build_delegated_outcome_summary(*, delegated_strategy_results: tuple[Mapping[str, object], ...] | list[Mapping[str, object]]) -> dict[str, object]:
    enabled_strategy_count = len(delegated_strategy_results)
    delegated_success_count = 0
    delegated_failure_count = 0
    for delegated_strategy_result in delegated_strategy_results:
        if not isinstance(delegated_strategy_result, Mapping):
            raise StrategyRegistrationError("delegated strategy result must be mapping for delegated outcome summary")
        if delegated_strategy_result.get("ok") is True:
            delegated_success_count += 1
            continue
        if delegated_strategy_result.get("ok") is False:
            delegated_failure_count += 1
            continue
        raise StrategyRegistrationError("delegated strategy result ok flag must be boolean for delegated outcome summary")
    if delegated_success_count + delegated_failure_count != enabled_strategy_count:
        raise StrategyRegistrationError("delegated outcome summary counts must exactly match included enabled strategy count")
    return {
        "delegated_outcome_summary_schema_version": 1,
        "delegated_scope": "portfolio_registered_strategies",
        "enabled_strategy_count": enabled_strategy_count,
        "delegated_success_count": delegated_success_count,
        "delegated_failure_count": delegated_failure_count,
        "delegated_outcome_status": _classify_delegated_outcome_status(enabled_strategy_count=enabled_strategy_count, delegated_success_count=delegated_success_count, delegated_failure_count=delegated_failure_count),
    }


def run_registered_portfolio_runtime_orchestrator(*, portfolio_id: str, environment_id: str) -> dict[str, object]:
    portfolio_record = _load_portfolio_record(portfolio_id)
    environment_record = _load_environment_record(environment_id)
    if portfolio_record.get("status") != "active":
        raise StrategyRegistrationError("portfolio registry record must be active")
    enabled_strategy_ids = _load_enabled_strategy_ids(portfolio_record)
    started_at = datetime.now(timezone.utc)
    started_at_utc = started_at.isoformat()
    portfolio_run_id = _new_portfolio_run_id(started_at)
    delegated_strategy_results: list[dict[str, object]] = []
    for strategy_id in enabled_strategy_ids:
        try:
            delegated_result = run_registered_runtime_boundary(strategy_id=strategy_id, portfolio_id=portfolio_id, environment_id=environment_id)
        except Exception as exc:
            failed_at_utc = datetime.now(timezone.utc).isoformat()
            delegated_strategy_results.append({"strategy_id": strategy_id, "ok": False, "error_type": type(exc).__name__, "error": str(exc)})
            portfolio_result = {
                "portfolio_id": portfolio_id,
                "environment_id": environment_id,
                "portfolio_run_schema_version": 7,
                "portfolio_run_id": portfolio_run_id,
                "started_at_utc": started_at_utc,
                "completed_at_utc": failed_at_utc,
                "status": "failed",
                "ok": False,
                "enabled_strategy_ids": enabled_strategy_ids,
                "delegated_strategy_results": tuple(delegated_strategy_results),
                "delegated_outcome_summary": _build_delegated_outcome_summary(delegated_strategy_results=delegated_strategy_results),
                "signal_activity_summary": _build_signal_activity_summary(enabled_strategy_ids=enabled_strategy_ids, delegated_strategy_results=delegated_strategy_results),
                "data_freshness_summary": _build_data_freshness_summary(enabled_strategy_ids=enabled_strategy_ids, delegated_strategy_results=delegated_strategy_results),
                "failure_summary": _build_failure_summary(strategy_id=strategy_id, failed_at_utc=failed_at_utc, exc=exc),
                "run_timing_summary": _build_run_timing_summary(started_at_utc=started_at_utc, completed_at_utc=failed_at_utc),
            }
            _write_portfolio_run_report(portfolio_result=portfolio_result, environment_record=environment_record)
            return portfolio_result
        delegated_strategy_results.append({"strategy_id": strategy_id, "ok": True, "result": delegated_result})
    completed_at_utc = datetime.now(timezone.utc).isoformat()
    portfolio_result = {
        "portfolio_id": portfolio_id,
        "environment_id": environment_id,
        "portfolio_run_schema_version": 7,
        "portfolio_run_id": portfolio_run_id,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "status": "ok",
        "ok": True,
        "enabled_strategy_ids": enabled_strategy_ids,
        "delegated_strategy_results": tuple(delegated_strategy_results),
        "delegated_outcome_summary": _build_delegated_outcome_summary(delegated_strategy_results=delegated_strategy_results),
        "signal_activity_summary": _build_signal_activity_summary(enabled_strategy_ids=enabled_strategy_ids, delegated_strategy_results=delegated_strategy_results),
        "data_freshness_summary": _build_data_freshness_summary(enabled_strategy_ids=enabled_strategy_ids, delegated_strategy_results=delegated_strategy_results),
        "run_timing_summary": _build_run_timing_summary(started_at_utc=started_at_utc, completed_at_utc=completed_at_utc),
    }
    _write_portfolio_run_report(portfolio_result=portfolio_result, environment_record=environment_record)
    return portfolio_result

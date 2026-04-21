from __future__ import annotations

from datetime import datetime, time
from importlib import import_module
from typing import Mapping

from src.moex_runtime.control_plane.runtime_unit_loader import RuntimeUnitConfig, load_environment_record, load_runtime_unit_config
from src.moex_runtime.control_plane.runtime_unit_status_store import build_status_patch, load_runtime_unit_status, resolve_status_path, save_runtime_unit_status
from src.moex_strategy_sdk.errors import StrategyRegistrationError


def _now_local(tz_name: str) -> datetime:
    import pytz

    return datetime.now(pytz.timezone(tz_name))


def _is_within_market_window(runtime_unit: RuntimeUnitConfig) -> bool:
    window = runtime_unit.market_window
    now = _now_local(window["timezone"])
    weekday = now.isoweekday()
    if weekday not in window["allowed_weekdays"]:
        return False
    current_time = now.time()
    start = time.fromisoformat(window["start_time"])
    end = time.fromisoformat(window["end_time"])
    return start <= current_time <= end


def _is_due(runtime_unit: RuntimeUnitConfig, prior_status: Mapping[str, object]) -> bool:
    last_eval = prior_status.get("last_evaluated_at")
    if last_eval is None:
        return True
    try:
        last_dt = datetime.fromisoformat(str(last_eval))
    except Exception:
        return True
    now_dt = datetime.now(last_dt.tzinfo) if last_dt.tzinfo is not None else datetime.now()
    delta_minutes = (now_dt - last_dt).total_seconds() / 60.0
    return delta_minutes >= runtime_unit.cadence_minutes


def _restart_policy_allows(runtime_unit: RuntimeUnitConfig, prior_status: Mapping[str, object]) -> tuple[bool, str]:
    failures = int(prior_status.get("consecutive_failures", 0))
    max_failures = int(runtime_unit.restart_policy["max_consecutive_failures"])
    if failures >= max_failures:
        return False, "restart_blocked"
    return True, "restart_eligible"


def _dispatch(runtime_unit: RuntimeUnitConfig) -> dict[str, object]:
    module_path, func_name = runtime_unit.dispatch_ref.split(":")
    module = import_module(module_path)
    func = getattr(module, func_name)
    return func(**runtime_unit.dispatch_kwargs)


def run_runtime_unit_once(*, runtime_unit_id: str) -> dict[str, object]:
    runtime_unit = load_runtime_unit_config(runtime_unit_id=runtime_unit_id)
    environment_record = load_environment_record(environment_id=runtime_unit.environment_id)
    status_path = resolve_status_path(runtime_unit=runtime_unit, environment_record=environment_record)
    prior_status = load_runtime_unit_status(status_path=status_path, runtime_unit_id=runtime_unit.runtime_unit_id)

    if not runtime_unit.enabled:
        new_status = build_status_patch(
            runtime_unit=runtime_unit,
            prior_status=prior_status,
            coarse_state="disabled",
            restart_policy_state="n/a",
            last_terminal_outcome=None,
            last_error=None,
            started=False,
            finished=False,
            dispatch_result=None,
        )
        return save_runtime_unit_status(status_path=status_path, payload=new_status, runtime_unit_id=runtime_unit.runtime_unit_id)

    if not _is_within_market_window(runtime_unit):
        new_status = build_status_patch(
            runtime_unit=runtime_unit,
            prior_status=prior_status,
            coarse_state="scheduled",
            restart_policy_state="window_closed",
            last_terminal_outcome=None,
            last_error=None,
            started=False,
            finished=False,
            dispatch_result=None,
        )
        return save_runtime_unit_status(status_path=status_path, payload=new_status, runtime_unit_id=runtime_unit.runtime_unit_id)

    if not _is_due(runtime_unit, prior_status):
        new_status = build_status_patch(
            runtime_unit=runtime_unit,
            prior_status=prior_status,
            coarse_state="scheduled",
            restart_policy_state="not_due",
            last_terminal_outcome=None,
            last_error=None,
            started=False,
            finished=False,
            dispatch_result=None,
        )
        return save_runtime_unit_status(status_path=status_path, payload=new_status, runtime_unit_id=runtime_unit.runtime_unit_id)

    restart_allowed, restart_state = _restart_policy_allows(runtime_unit, prior_status)
    if not restart_allowed:
        new_status = build_status_patch(
            runtime_unit=runtime_unit,
            prior_status=prior_status,
            coarse_state="blocked_by_policy",
            restart_policy_state=restart_state,
            last_terminal_outcome="blocked",
            last_error="restart policy exhausted",
            started=False,
            finished=False,
            dispatch_result=None,
        )
        return save_runtime_unit_status(status_path=status_path, payload=new_status, runtime_unit_id=runtime_unit.runtime_unit_id)

    try:
        result = _dispatch(runtime_unit)
        new_status = build_status_patch(
            runtime_unit=runtime_unit,
            prior_status=prior_status,
            coarse_state="succeeded",
            restart_policy_state=restart_state,
            last_terminal_outcome="succeeded",
            last_error=None,
            started=True,
            finished=True,
            dispatch_result=result,
        )
    except Exception as exc:
        new_status = build_status_patch(
            runtime_unit=runtime_unit,
            prior_status=prior_status,
            coarse_state="failed",
            restart_policy_state=restart_state,
            last_terminal_outcome="failed",
            last_error=str(exc),
            started=True,
            finished=True,
            dispatch_result=None,
        )

    return save_runtime_unit_status(status_path=status_path, payload=new_status, runtime_unit_id=runtime_unit.runtime_unit_id)

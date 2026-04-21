from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.moex_strategy_sdk.errors import StrategyRegistrationError


_REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class RuntimeUnitConfig:
    runtime_unit_id: str
    strategy_id: str
    portfolio_id: str
    environment_id: str
    runtime_mode: str
    instrument_scope: tuple[str, ...]
    timeframe: str
    enabled: bool
    cadence_minutes: int
    market_window: Mapping[str, object]
    restart_policy: Mapping[str, object]
    dispatch_ref: str
    dispatch_kwargs: Mapping[str, object]
    status_locator_ref: str
    status: str


def _load_json(repo_relative_path: str) -> Mapping[str, object]:
    path = _REPO_ROOT / repo_relative_path
    if not path.exists():
        raise StrategyRegistrationError("missing runtime control-plane file: " + repo_relative_path)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise StrategyRegistrationError("runtime control-plane file must contain JSON object: " + repo_relative_path)
    return payload


def _require_exact_keys(record_name: str, payload: Mapping[str, object], required_keys: set[str]) -> None:
    actual_keys = set(payload.keys())
    missing = sorted(required_keys - actual_keys)
    extra = sorted(actual_keys - required_keys)
    if missing:
        raise StrategyRegistrationError(record_name + " missing field(s): " + ", ".join(missing))
    if extra:
        raise StrategyRegistrationError(record_name + " has unexpected field(s): " + ", ".join(extra))


def _require_non_empty_string(record_name: str, field_name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StrategyRegistrationError(record_name + " " + field_name + " must be non-empty string")
    return value


def _validate_int(record_name: str, field_name: str, value: object, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise StrategyRegistrationError(record_name + " " + field_name + " must be int >= " + str(minimum))
    return value


def _validate_market_window(payload: Mapping[str, object]) -> Mapping[str, object]:
    required = {"timezone", "start_time", "end_time", "allowed_weekdays"}
    _require_exact_keys("runtime unit market_window", payload, required)
    timezone_name = _require_non_empty_string("runtime unit market_window", "timezone", payload["timezone"])
    start_time = _require_non_empty_string("runtime unit market_window", "start_time", payload["start_time"])
    end_time = _require_non_empty_string("runtime unit market_window", "end_time", payload["end_time"])
    allowed_weekdays = payload["allowed_weekdays"]
    if not isinstance(allowed_weekdays, list) or not allowed_weekdays:
        raise StrategyRegistrationError("runtime unit market_window allowed_weekdays must be non-empty list")
    normalized_weekdays: list[int] = []
    for item in allowed_weekdays:
        weekday = _validate_int("runtime unit market_window", "allowed_weekdays item", item, minimum=1)
        if weekday > 7:
            raise StrategyRegistrationError("runtime unit market_window allowed_weekdays item must be <= 7")
        normalized_weekdays.append(weekday)
    return {"timezone": timezone_name, "start_time": start_time, "end_time": end_time, "allowed_weekdays": tuple(normalized_weekdays)}


def _validate_restart_policy(payload: Mapping[str, object]) -> Mapping[str, object]:
    required = {"max_consecutive_failures", "stale_after_minutes", "require_operator_reset_after_exhausted"}
    _require_exact_keys("runtime unit restart_policy", payload, required)
    max_consecutive_failures = _validate_int("runtime unit restart_policy", "max_consecutive_failures", payload["max_consecutive_failures"], minimum=1)
    stale_after_minutes = _validate_int("runtime unit restart_policy", "stale_after_minutes", payload["stale_after_minutes"], minimum=1)
    require_operator_reset_after_exhausted = payload["require_operator_reset_after_exhausted"]
    if not isinstance(require_operator_reset_after_exhausted, bool):
        raise StrategyRegistrationError("runtime unit restart_policy require_operator_reset_after_exhausted must be bool")
    return {
        "max_consecutive_failures": max_consecutive_failures,
        "stale_after_minutes": stale_after_minutes,
        "require_operator_reset_after_exhausted": require_operator_reset_after_exhausted,
    }


def load_runtime_unit_config(*, runtime_unit_id: str) -> RuntimeUnitConfig:
    payload = _load_json("configs/runtime_units/" + runtime_unit_id + ".json")
    required = {
        "runtime_unit_id",
        "strategy_id",
        "portfolio_id",
        "environment_id",
        "runtime_mode",
        "instrument_scope",
        "timeframe",
        "enabled",
        "cadence_minutes",
        "market_window",
        "restart_policy",
        "dispatch_ref",
        "dispatch_kwargs",
        "status_locator_ref",
        "status",
    }
    _require_exact_keys("runtime unit record", payload, required)
    loaded_runtime_unit_id = _require_non_empty_string("runtime unit record", "runtime_unit_id", payload["runtime_unit_id"])
    if loaded_runtime_unit_id != runtime_unit_id:
        raise StrategyRegistrationError("runtime unit record runtime_unit_id does not match requested runtime_unit_id")
    instrument_scope_raw = payload["instrument_scope"]
    if not isinstance(instrument_scope_raw, list) or not instrument_scope_raw:
        raise StrategyRegistrationError("runtime unit record instrument_scope must be non-empty list")
    instrument_scope: list[str] = []
    for item in instrument_scope_raw:
        instrument_scope.append(_require_non_empty_string("runtime unit record", "instrument_scope item", item))
    enabled = payload["enabled"]
    if not isinstance(enabled, bool):
        raise StrategyRegistrationError("runtime unit record enabled must be bool")
    dispatch_kwargs = payload["dispatch_kwargs"]
    if not isinstance(dispatch_kwargs, dict) or not dispatch_kwargs:
        raise StrategyRegistrationError("runtime unit record dispatch_kwargs must be non-empty object")
    return RuntimeUnitConfig(
        runtime_unit_id=loaded_runtime_unit_id,
        strategy_id=_require_non_empty_string("runtime unit record", "strategy_id", payload["strategy_id"]),
        portfolio_id=_require_non_empty_string("runtime unit record", "portfolio_id", payload["portfolio_id"]),
        environment_id=_require_non_empty_string("runtime unit record", "environment_id", payload["environment_id"]),
        runtime_mode=_require_non_empty_string("runtime unit record", "runtime_mode", payload["runtime_mode"]),
        instrument_scope=tuple(instrument_scope),
        timeframe=_require_non_empty_string("runtime unit record", "timeframe", payload["timeframe"]),
        enabled=enabled,
        cadence_minutes=_validate_int("runtime unit record", "cadence_minutes", payload["cadence_minutes"], minimum=1),
        market_window=_validate_market_window(_require_mapping("runtime unit record", "market_window", payload["market_window"])),
        restart_policy=_validate_restart_policy(_require_mapping("runtime unit record", "restart_policy", payload["restart_policy"])),
        dispatch_ref=_require_non_empty_string("runtime unit record", "dispatch_ref", payload["dispatch_ref"]),
        dispatch_kwargs=dict(dispatch_kwargs),
        status_locator_ref=_require_non_empty_string("runtime unit record", "status_locator_ref", payload["status_locator_ref"]),
        status=_require_non_empty_string("runtime unit record", "status", payload["status"]),
    )


def load_environment_record(*, environment_id: str) -> Mapping[str, object]:
    payload = _load_json("configs/environments/" + environment_id + ".json")
    required = {
        "environment_id",
        "mode",
        "is_research",
        "is_backtest",
        "is_live",
        "broker_adapter_ref",
        "market_data_adapter_ref",
        "calendar_source_ref",
        "artifact_root_refs",
        "allowed_override_keys",
        "required_env_vars",
        "status",
    }
    _require_exact_keys("environment record", payload, required)
    loaded_environment_id = _require_non_empty_string("environment record", "environment_id", payload["environment_id"])
    if loaded_environment_id != environment_id:
        raise StrategyRegistrationError("environment record environment_id does not match requested environment_id")
    return payload


def _require_mapping(record_name: str, field_name: str, value: object) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise StrategyRegistrationError(record_name + " " + field_name + " must be object")
    return value

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from src.moex_core.contracts.external_pattern_artifact_path_resolver import resolve_external_pattern_artifact_path
from src.moex_runtime.control_plane.runtime_unit_loader import RuntimeUnitConfig
from src.moex_strategy_sdk.errors import StrategyRegistrationError


ALLOWED_COARSE_STATES = {
    "disabled",
    "scheduled",
    "running",
    "blocked_by_policy",
    "succeeded",
    "failed",
    "restart_eligible",
    "restart_blocked",
}

ALLOWED_TERMINAL_OUTCOMES = {"succeeded", "failed", "blocked", "never_run"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _normalize_status(payload: Mapping[str, object], *, runtime_unit_id: str) -> dict[str, object]:
    normalized = dict(payload)
    normalized.setdefault("runtime_unit_id", runtime_unit_id)
    normalized.setdefault("enabled", False)
    normalized.setdefault("current_coarse_state", "scheduled")
    normalized.setdefault("last_evaluated_at", None)
    normalized.setdefault("last_started_at", None)
    normalized.setdefault("last_finished_at", None)
    normalized.setdefault("last_terminal_outcome", "never_run")
    normalized.setdefault("last_error", None)
    normalized.setdefault("consecutive_failures", 0)
    normalized.setdefault("restart_policy_state", "eligible")
    normalized.setdefault("last_dispatch_result", None)
    if normalized["runtime_unit_id"] != runtime_unit_id:
        raise StrategyRegistrationError("runtime control-plane status runtime_unit_id mismatch")
    if not isinstance(normalized["enabled"], bool):
        raise StrategyRegistrationError("runtime control-plane status enabled must be bool")
    current_coarse_state = normalized["current_coarse_state"]
    if current_coarse_state not in ALLOWED_COARSE_STATES:
        raise StrategyRegistrationError("runtime control-plane status current_coarse_state is invalid")
    last_terminal_outcome = normalized["last_terminal_outcome"]
    if last_terminal_outcome not in ALLOWED_TERMINAL_OUTCOMES:
        raise StrategyRegistrationError("runtime control-plane status last_terminal_outcome is invalid")
    consecutive_failures = normalized["consecutive_failures"]
    if isinstance(consecutive_failures, bool) or not isinstance(consecutive_failures, int) or consecutive_failures < 0:
        raise StrategyRegistrationError("runtime control-plane status consecutive_failures must be non-negative int")
    restart_policy_state = normalized["restart_policy_state"]
    if not isinstance(restart_policy_state, str) or not restart_policy_state:
        raise StrategyRegistrationError("runtime control-plane status restart_policy_state must be non-empty string")
    return normalized


def resolve_status_path(*, runtime_unit: RuntimeUnitConfig, environment_record: Mapping[str, object]) -> Path:
    return resolve_external_pattern_artifact_path(
        locator_ref=runtime_unit.status_locator_ref,
        environment_record=environment_record,
        format_kwargs={"runtime_unit_id": runtime_unit.runtime_unit_id},
    )


def load_runtime_unit_status(*, status_path: Path, runtime_unit_id: str) -> dict[str, object]:
    if not status_path.exists():
        return _normalize_status({}, runtime_unit_id=runtime_unit_id)
    with status_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise StrategyRegistrationError("runtime control-plane status payload must be object")
    return _normalize_status(payload, runtime_unit_id=runtime_unit_id)


def save_runtime_unit_status(*, status_path: Path, payload: Mapping[str, object], runtime_unit_id: str) -> dict[str, object]:
    normalized = _normalize_status(payload, runtime_unit_id=runtime_unit_id)
    _ensure_parent(status_path)
    tmp_path = status_path.with_suffix(status_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(normalized, handle, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.replace(status_path)
    return normalized


def build_status_patch(*, runtime_unit: RuntimeUnitConfig, prior_status: Mapping[str, object], coarse_state: str, restart_policy_state: str, last_terminal_outcome: str | None, last_error: str | None, started: bool, finished: bool, dispatch_result: Mapping[str, object] | None) -> dict[str, object]:
    now_iso = _now_iso()
    patch = dict(prior_status)
    patch["runtime_unit_id"] = runtime_unit.runtime_unit_id
    patch["enabled"] = runtime_unit.enabled
    patch["current_coarse_state"] = coarse_state
    patch["last_evaluated_at"] = now_iso
    patch["restart_policy_state"] = restart_policy_state
    patch["last_dispatch_result"] = dict(dispatch_result) if dispatch_result is not None else None
    if started:
        patch["last_started_at"] = now_iso
    if finished:
        patch["last_finished_at"] = now_iso
    if last_terminal_outcome is not None:
        patch["last_terminal_outcome"] = last_terminal_outcome
    patch["last_error"] = last_error
    previous_failures = patch.get("consecutive_failures", 0)
    if last_terminal_outcome == "failed":
        patch["consecutive_failures"] = int(previous_failures) + 1
    elif last_terminal_outcome in {"succeeded", "blocked"}:
        patch["consecutive_failures"] = 0
    return _normalize_status(patch, runtime_unit_id=runtime_unit.runtime_unit_id)

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from src.moex_core.contracts.external_pattern_artifact_path_resolver import resolve_external_pattern_artifact_path
from src.moex_runtime.control_plane.runtime_control_plane import run_runtime_unit_once
from src.moex_runtime.control_plane.runtime_unit_loader import load_environment_record
from src.moex_strategy_sdk.errors import StrategyRegistrationError


_REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class SchedulerConfig:
    scheduler_id: str
    environment_id: str
    runtime_unit_ids: tuple[str, ...]
    summary_locator_ref: str
    status: str


def _load_json(repo_relative_path: str) -> Mapping[str, object]:
    path = _REPO_ROOT / repo_relative_path
    if not path.exists():
        raise StrategyRegistrationError("missing scheduler config: " + repo_relative_path)
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise StrategyRegistrationError("scheduler config must be JSON object")
    return payload


def _require_non_empty_string(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StrategyRegistrationError(name + " must be non-empty string")
    return value


def _load_scheduler_config(*, scheduler_id: str) -> SchedulerConfig:
    payload = _load_json("configs/runtime_schedulers/" + scheduler_id + ".json")
    required = {"scheduler_id", "environment_id", "runtime_unit_ids", "summary_locator_ref", "status"}
    actual = set(payload.keys())
    missing = required - actual
    extra = actual - required
    if missing:
        raise StrategyRegistrationError("scheduler config missing fields: " + ",".join(sorted(missing)))
    if extra:
        raise StrategyRegistrationError("scheduler config has unexpected fields: " + ",".join(sorted(extra)))
    loaded_id = _require_non_empty_string("scheduler_id", payload["scheduler_id"])
    if loaded_id != scheduler_id:
        raise StrategyRegistrationError("scheduler_id mismatch")
    unit_ids_raw = payload["runtime_unit_ids"]
    if not isinstance(unit_ids_raw, list) or not unit_ids_raw:
        raise StrategyRegistrationError("runtime_unit_ids must be non-empty list")
    unit_ids: list[str] = []
    for item in unit_ids_raw:
        unit_ids.append(_require_non_empty_string("runtime_unit_id", item))
    return SchedulerConfig(
        scheduler_id=loaded_id,
        environment_id=_require_non_empty_string("environment_id", payload["environment_id"]),
        runtime_unit_ids=tuple(unit_ids),
        summary_locator_ref=_require_non_empty_string("summary_locator_ref", payload["summary_locator_ref"]),
        status=_require_non_empty_string("status", payload["status"]),
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _classify_outcome(status_payload: Mapping[str, object]) -> str:
    state = status_payload.get("current_coarse_state")
    restart_state = status_payload.get("restart_policy_state")
    if state == "disabled":
        return "skipped_disabled"
    if state == "blocked_by_policy":
        return "skipped_blocked"
    if state == "scheduled" and restart_state in {"not_due", "window_closed"}:
        return "skipped_not_due"
    if state == "succeeded":
        return "dispatch_authorized"
    if state == "failed":
        return "dispatch_failed"
    return "evaluation_failed"


def _resolve_summary_path(*, scheduler_id: str, environment_record: Mapping[str, object], summary_locator_ref: str) -> Path:
    return resolve_external_pattern_artifact_path(
        locator_ref=summary_locator_ref,
        environment_record=environment_record,
        format_kwargs={"scheduler_id": scheduler_id},
    )


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _save_summary(*, path: Path, payload: Mapping[str, object]) -> dict[str, object]:
    _ensure_parent(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.replace(path)
    return dict(payload)


def run_scheduler_pass(*, scheduler_id: str) -> dict[str, object]:
    cfg = _load_scheduler_config(scheduler_id=scheduler_id)
    env = load_environment_record(environment_id=cfg.environment_id)

    ordered_units: Sequence[str] = tuple(sorted(cfg.runtime_unit_ids))

    counts = {
        "skipped_disabled": 0,
        "skipped_not_due": 0,
        "skipped_blocked": 0,
        "dispatch_authorized": 0,
        "dispatch_failed": 0,
        "evaluation_failed": 0,
    }

    per_unit: list[dict[str, object]] = []

    started_at = _now_iso()

    for unit_id in ordered_units:
        try:
            status = run_runtime_unit_once(runtime_unit_id=unit_id)
            outcome = _classify_outcome(status)
            if outcome not in counts:
                counts["evaluation_failed"] += 1
                per_unit.append({"runtime_unit_id": unit_id, "outcome": "evaluation_failed"})
            else:
                counts[outcome] += 1
                per_unit.append({"runtime_unit_id": unit_id, "outcome": outcome})
        except Exception as exc:
            counts["evaluation_failed"] += 1
            per_unit.append({"runtime_unit_id": unit_id, "outcome": "evaluation_failed", "error": str(exc)})

    finished_at = _now_iso()

    final_status = "completed"
    if counts["dispatch_failed"] > 0 or counts["evaluation_failed"] > 0:
        final_status = "completed_with_failures"

    summary = {
        "scheduler_id": cfg.scheduler_id,
        "scheduler_pass_started_at": started_at,
        "scheduler_pass_finished_at": finished_at,
        "total_discovered_runtime_units": len(ordered_units),
        "total_evaluated_runtime_units": len(ordered_units),
        "counts": counts,
        "final_scheduler_pass_status": final_status,
        "per_unit_outcomes": per_unit,
    }

    summary_path = _resolve_summary_path(
        scheduler_id=cfg.scheduler_id,
        environment_record=env,
        summary_locator_ref=cfg.summary_locator_ref,
    )

    return _save_summary(path=summary_path, payload=summary)

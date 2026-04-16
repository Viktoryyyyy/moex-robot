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
            delegated_strategy_results.append({"strategy_id": strategy_id, "ok": False, "error_type": type(exc).__name__, "error": str(exc)})
            portfolio_result = {"portfolio_id": portfolio_id, "environment_id": environment_id, "portfolio_run_schema_version": 1, "portfolio_run_id": portfolio_run_id, "started_at_utc": started_at_utc, "completed_at_utc": datetime.now(timezone.utc).isoformat(), "status": "failed", "ok": False, "enabled_strategy_ids": enabled_strategy_ids, "delegated_strategy_results": tuple(delegated_strategy_results)}
            _write_portfolio_run_report(portfolio_result=portfolio_result, environment_record=environment_record)
            return portfolio_result
        delegated_strategy_results.append({"strategy_id": strategy_id, "ok": True, "result": delegated_result})
    portfolio_result = {"portfolio_id": portfolio_id, "environment_id": environment_id, "portfolio_run_schema_version": 1, "portfolio_run_id": portfolio_run_id, "started_at_utc": started_at_utc, "completed_at_utc": datetime.now(timezone.utc).isoformat(), "status": "ok", "ok": True, "enabled_strategy_ids": enabled_strategy_ids, "delegated_strategy_results": tuple(delegated_strategy_results)}
    _write_portfolio_run_report(portfolio_result=portfolio_result, environment_record=environment_record)
    return portfolio_result

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from src.moex_runtime.engine.run_registered_runtime_boundary import run_registered_runtime_boundary
from src.moex_strategy_sdk.errors import StrategyRegistrationError

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _load_portfolio_record(portfolio_id: str) -> Mapping[str, object]:
    path = _REPO_ROOT / "configs" / "portfolios" / (portfolio_id + ".json")
    if not path.exists():
        raise StrategyRegistrationError("missing portfolio registry/config file: configs/portfolios/" + portfolio_id + ".json")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise StrategyRegistrationError("portfolio registry/config file must contain JSON object")
    return payload


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


def run_registered_portfolio_runtime_orchestrator(*, portfolio_id: str, environment_id: str) -> dict[str, object]:
    portfolio_record = _load_portfolio_record(portfolio_id)
    if portfolio_record.get("status") != "active":
        raise StrategyRegistrationError("portfolio registry record must be active")
    enabled_strategy_ids = _load_enabled_strategy_ids(portfolio_record)
    delegated_strategy_results: list[dict[str, object]] = []
    for strategy_id in enabled_strategy_ids:
        try:
            delegated_result = run_registered_runtime_boundary(strategy_id=strategy_id, portfolio_id=portfolio_id, environment_id=environment_id)
        except Exception as exc:
            delegated_strategy_results.append({"strategy_id": strategy_id, "ok": False, "error_type": type(exc).__name__, "error": str(exc)})
            return {"portfolio_id": portfolio_id, "environment_id": environment_id, "status": "failed", "ok": False, "enabled_strategy_ids": enabled_strategy_ids, "delegated_strategy_results": tuple(delegated_strategy_results)}
        delegated_strategy_results.append({"strategy_id": strategy_id, "ok": True, "result": delegated_result})
    return {"portfolio_id": portfolio_id, "environment_id": environment_id, "status": "ok", "ok": True, "enabled_strategy_ids": enabled_strategy_ids, "delegated_strategy_results": tuple(delegated_strategy_results)}

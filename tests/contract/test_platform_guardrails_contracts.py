from __future__ import annotations

from pathlib import Path

import pytest

from src.moex_core.contracts.external_pattern_artifact_path_resolver import resolve_external_pattern_artifact_path
from src.moex_runtime.control_plane.runtime_unit_loader import RuntimeUnitConfig
from src.moex_runtime.control_plane.runtime_unit_status_store import resolve_status_path
from src.moex_strategy_sdk.errors import StrategyRegistrationError


def _runtime_unit(*, runtime_unit_id: str, status_locator_ref: str) -> RuntimeUnitConfig:
    return RuntimeUnitConfig(
        runtime_unit_id=runtime_unit_id,
        strategy_id="ema_3_19_15m",
        portfolio_id="paper",
        environment_id="paper_env",
        runtime_mode="paper",
        instrument_scope=("Si",),
        timeframe="15m",
        enabled=True,
        cadence_minutes=15,
        market_window={
            "timezone": "Asia/Almaty",
            "start_time": "10:00:00",
            "end_time": "18:00:00",
            "allowed_weekdays": (1, 2, 3, 4, 5),
        },
        restart_policy={
            "max_consecutive_failures": 3,
            "stale_after_minutes": 30,
            "require_operator_reset_after_exhausted": True,
        },
        dispatch_ref="pkg.module:run",
        dispatch_kwargs={"runtime_unit_id": runtime_unit_id},
        status_locator_ref=status_locator_ref,
        status="active",
    )


def test_artifact_contract_rejects_missing_required_artifact_root_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MOEX_ARTIFACT_ROOT", raising=False)
    with pytest.raises(StrategyRegistrationError, match="missing required artifact root env var"):
        resolve_external_pattern_artifact_path(
            locator_ref="runtime/status/{runtime_unit_id}.json",
            environment_record={"artifact_root_refs": ["MOEX_ARTIFACT_ROOT"]},
            format_kwargs={"runtime_unit_id": "unit_a"},
        )


def test_artifact_contract_rejects_conflicting_artifact_root_bindings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_ARTIFACT_ROOT", str(tmp_path))
    with pytest.raises(StrategyRegistrationError, match="exactly one artifact_root_ref"):
        resolve_external_pattern_artifact_path(
            locator_ref="runtime/status/{runtime_unit_id}.json",
            environment_record={"artifact_root_refs": ["MOEX_ARTIFACT_ROOT", "OTHER_ROOT"]},
            format_kwargs={"runtime_unit_id": "unit_a"},
        )


def test_runtime_state_locator_requires_runtime_unit_identity_placeholder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_ARTIFACT_ROOT", str(tmp_path))
    runtime_unit = _runtime_unit(runtime_unit_id="unit_a", status_locator_ref="runtime/status/status.json")
    with pytest.raises(StrategyRegistrationError, match="must include \\{runtime_unit_id\\}"):
        resolve_status_path(
            runtime_unit=runtime_unit,
            environment_record={"artifact_root_refs": ["MOEX_ARTIFACT_ROOT"]},
        )


def test_runtime_state_locator_resolves_distinct_paths_per_runtime_unit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOEX_ARTIFACT_ROOT", str(tmp_path))
    unit_a = _runtime_unit(runtime_unit_id="unit_a", status_locator_ref="runtime/status/{runtime_unit_id}.json")
    unit_b = _runtime_unit(runtime_unit_id="unit_b", status_locator_ref="runtime/status/{runtime_unit_id}.json")

    path_a = resolve_status_path(runtime_unit=unit_a, environment_record={"artifact_root_refs": ["MOEX_ARTIFACT_ROOT"]})
    path_b = resolve_status_path(runtime_unit=unit_b, environment_record={"artifact_root_refs": ["MOEX_ARTIFACT_ROOT"]})

    assert path_a != path_b
    assert path_a == tmp_path / "runtime" / "status" / "unit_a.json"
    assert path_b == tmp_path / "runtime" / "status" / "unit_b.json"

from __future__ import annotations

from pathlib import Path

from src.moex_runtime.scheduler import multi_unit_scheduler_loop as scheduler_module


def test_scheduler_pass_orders_runtime_units_deterministically(tmp_path: Path, monkeypatch) -> None:
    execution_order: list[str] = []

    monkeypatch.setattr(
        scheduler_module,
        "_load_scheduler_config",
        lambda *, scheduler_id: scheduler_module.SchedulerConfig(
            scheduler_id=scheduler_id,
            environment_id="paper_env",
            runtime_unit_ids=("unit_c", "unit_a", "unit_b"),
            summary_locator_ref="scheduler/{scheduler_id}.json",
            status="active",
        ),
    )
    monkeypatch.setattr(scheduler_module, "load_environment_record", lambda *, environment_id: {"environment_id": environment_id})
    monkeypatch.setattr(
        scheduler_module,
        "resolve_external_pattern_artifact_path",
        lambda *, locator_ref, environment_record, format_kwargs: tmp_path / locator_ref.format(**format_kwargs),
    )

    def _run_runtime_unit_once(*, runtime_unit_id: str) -> dict[str, object]:
        execution_order.append(runtime_unit_id)
        return {"current_coarse_state": "succeeded", "restart_policy_state": "restart_eligible"}

    monkeypatch.setattr(scheduler_module, "run_runtime_unit_once", _run_runtime_unit_once)

    summary = scheduler_module.run_scheduler_pass(scheduler_id="nightly")

    assert execution_order == ["unit_a", "unit_b", "unit_c"]
    assert [item["runtime_unit_id"] for item in summary["per_unit_outcomes"]] == ["unit_a", "unit_b", "unit_c"]
    assert summary["counts"]["dispatch_authorized"] == 3
    assert summary["final_scheduler_pass_status"] == "completed"

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from moex_data import step10_rub_refresh_dispatcher as dispatcher
from moex_data import step10_rub_refresh_entrypoint as entrypoint


def test_futoi_factual_failure_is_explicit_and_non_blocking(monkeypatch) -> None:
    def fail(**kwargs):
        del kwargs
        raise RuntimeError("futoi unavailable")

    monkeypatch.setattr(entrypoint.futoi_factual, "run_refresh", fail)
    result = entrypoint._run_futoi_factual_non_blocking(
        through_date="2026-08-28",
        run_id="stage10_test",
        timeout=1.0,
    )

    assert result["status"] == "FAILED_NON_BLOCKING"
    assert result["factual_authority"] is False
    assert result["directional_authority"] is False
    assert result["action_authority"] is False
    assert result["stage5_pointer_promotion_performed"] is False


def test_stage5_full_mode_remains_fail_closed() -> None:
    assert dispatcher.STAGE5_FULL_MODE_READY is False


def test_futoi_refresh_order_is_before_calendar_in_blocked_mode() -> None:
    order: list[object] = [
        "futoi_governance",
        "stage5_full_mode_readiness",
        "calendar",
        "stage7_raw_and_derived",
    ]
    entrypoint._insert_futoi_refresh_order(order, dispatcher.BLOCKED_MODE)
    assert order == [
        "futoi_governance",
        "stage5_full_mode_readiness",
        "futoi_raw_factual_refresh",
        "calendar",
        "stage7_raw_and_derived",
    ]


def test_futoi_refresh_order_is_between_calendar_and_stage5_in_full_mode() -> None:
    order: list[object] = [
        "calendar",
        "stage5_raw_and_derived",
        "stage7_raw_and_derived",
    ]
    entrypoint._insert_futoi_refresh_order(order, dispatcher.FULL_MODE)
    assert order == [
        "calendar",
        "futoi_raw_factual_refresh",
        "stage5_raw_and_derived",
        "stage7_raw_and_derived",
    ]


def test_augment_manifest_persists_entrypoint_schema_version(tmp_path, monkeypatch) -> None:
    run_id = "stage10_manifest_test"
    manifest_path = (
        tmp_path
        / "runs"
        / "step10_rub_daily_refresh"
        / ("run_id=" + run_id)
        / "run_manifest.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(entrypoint.step10, "_data_root", lambda: tmp_path)

    result: dict[str, object] = {
        "run_id": run_id,
        "dispatcher_mode": dispatcher.BLOCKED_MODE,
        "deterministic_refresh_order": ["calendar", "stage7_raw_and_derived"],
    }
    entrypoint._augment_manifest(result, {"status": "PASS"})

    persisted = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert result["entrypoint_schema_version"] == entrypoint.SCHEMA_VERSION
    assert persisted["entrypoint_schema_version"] == entrypoint.SCHEMA_VERSION
    assert persisted["futoi_factual_refresh"] == {"status": "PASS"}
    assert persisted["futoi_factual_refresh_blocks_stage7"] is False


def test_dispatcher_failure_preserves_futoi_result_in_manifest_and_exception(
    tmp_path, monkeypatch
) -> None:
    run_id = "stage10_failure_test"
    manifest_path = (
        tmp_path
        / "runs"
        / "step10_rub_daily_refresh"
        / ("run_id=" + run_id)
        / "run_manifest.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps({"status": "failed", "run_id": run_id}) + "\n",
        encoding="utf-8",
    )
    futoi_result = {"status": "PASS", "trade_date": "2026-08-28"}

    monkeypatch.setattr(entrypoint.step10, "_data_root", lambda: tmp_path)
    monkeypatch.setattr(entrypoint.step10, "load_env_file", lambda value: None)
    monkeypatch.setattr(
        entrypoint,
        "_run_futoi_factual_non_blocking",
        lambda **kwargs: dict(futoi_result),
    )

    def dispatcher_failure(**kwargs):
        del kwargs
        raise RuntimeError("stage7 smoke failed")

    monkeypatch.setattr(entrypoint.dispatcher, "run_refresh", dispatcher_failure)

    with pytest.raises(entrypoint.Step10EntrypointError, match="stage7 smoke failed") as exc_info:
        entrypoint.run_refresh(
            through_date="2026-08-28",
            run_id=run_id,
            timeout=1.0,
            now_utc=datetime(2026, 8, 30, 8, 0, tzinfo=timezone.utc),
        )

    assert exc_info.value.futoi_result == futoi_result
    assert exc_info.value.dispatcher_failure_manifest_augmented is True
    persisted = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert persisted["status"] == "failed"
    assert persisted["entrypoint_schema_version"] == entrypoint.SCHEMA_VERSION
    assert persisted["futoi_factual_refresh"] == futoi_result
    assert persisted["futoi_factual_refresh_blocks_stage7"] is False


def test_main_reports_futoi_result_on_dispatcher_failure(monkeypatch, capsys) -> None:
    futoi_result = {"status": "FAILED_NON_BLOCKING", "error": "futoi unavailable"}

    def fail(**kwargs):
        del kwargs
        raise entrypoint.Step10EntrypointError(
            "stage7 failed",
            futoi_result=futoi_result,
            dispatcher_failure_manifest_augmented=True,
        )

    monkeypatch.setattr(entrypoint, "run_refresh", fail)
    status = entrypoint.main(
        ["--through-date", "2026-08-28", "--run-id", "stage10_main_failure"]
    )

    assert status == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == "stage7 failed"
    assert payload["futoi_factual_refresh"] == futoi_result
    assert payload["futoi_factual_refresh_blocks_stage7"] is False
    assert payload["dispatcher_failure_manifest_augmented"] is True


def test_stage10_contract_declares_independent_futoi_factual_refresh() -> None:
    text = Path("configs/datasets/step10_rub_daily_refresh.v1.yaml").read_text(encoding="utf-8")
    assert "futoi_raw_factual_refresh_allowed: true" in text
    assert "futoi_raw_factual_refresh_failure_blocks_stage7: false" in text
    assert "stage5_materialization_allowed: false" in text
    assert "current_full_mode_readiness: false" in text

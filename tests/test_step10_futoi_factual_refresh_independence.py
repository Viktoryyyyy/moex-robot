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


def test_dispatcher_validates_transaction_result_context() -> None:
    context = dispatcher._validated_result_context(
        {
            "entrypoint_schema_version": entrypoint.SCHEMA_VERSION,
            "futoi_factual_refresh": {"status": "PASS"},
            "futoi_factual_refresh_blocks_stage7": False,
        }
    )
    assert context["entrypoint_schema_version"] == entrypoint.SCHEMA_VERSION
    assert context["futoi_factual_refresh"] == {"status": "PASS"}
    assert context["futoi_factual_refresh_blocks_stage7"] is False

    with pytest.raises(
        entrypoint.step10.Step10RefreshError,
        match="must remain non-blocking for Stage 7",
    ):
        dispatcher._validated_result_context(
            {
                "entrypoint_schema_version": entrypoint.SCHEMA_VERSION,
                "futoi_factual_refresh": {"status": "PASS"},
                "futoi_factual_refresh_blocks_stage7": True,
            }
        )


def test_entrypoint_passes_futoi_context_inside_dispatcher_transaction(monkeypatch) -> None:
    futoi_result = {"status": "PASS", "trade_date": "2026-08-28"}
    captured: dict[str, object] = {}

    monkeypatch.setattr(entrypoint.step10, "load_env_file", lambda value: None)
    monkeypatch.setattr(
        entrypoint,
        "_run_futoi_factual_non_blocking",
        lambda **kwargs: dict(futoi_result),
    )

    def fake_dispatcher(**kwargs):
        captured.update(kwargs)
        context = dict(kwargs["result_context"])
        return {
            "status": "succeeded",
            "entrypoint_schema_version": context["entrypoint_schema_version"],
            "futoi_factual_refresh": context["futoi_factual_refresh"],
            "futoi_factual_refresh_blocks_stage7": context[
                "futoi_factual_refresh_blocks_stage7"
            ],
        }

    monkeypatch.setattr(entrypoint.dispatcher, "run_refresh", fake_dispatcher)

    result = entrypoint.run_refresh(
        through_date="2026-08-28",
        run_id="stage10_transaction_context_test",
        timeout=1.0,
        now_utc=datetime(2026, 8, 30, 8, 0, tzinfo=timezone.utc),
    )

    assert captured["result_context"] == {
        "entrypoint_schema_version": entrypoint.SCHEMA_VERSION,
        "futoi_factual_refresh": futoi_result,
        "futoi_factual_refresh_blocks_stage7": False,
    }
    assert result["entrypoint_schema_version"] == entrypoint.SCHEMA_VERSION
    assert result["futoi_factual_refresh"] == futoi_result
    assert result["futoi_factual_refresh_blocks_stage7"] is False
    assert not hasattr(entrypoint, "_augment_manifest")
    assert not hasattr(entrypoint, "_persist_dispatcher_failure_context")


def test_entrypoint_failure_preserves_futoi_result_in_error(monkeypatch) -> None:
    futoi_result = {"status": "PASS", "trade_date": "2026-08-28"}
    captured: dict[str, object] = {}

    monkeypatch.setattr(entrypoint.step10, "load_env_file", lambda value: None)
    monkeypatch.setattr(
        entrypoint,
        "_run_futoi_factual_non_blocking",
        lambda **kwargs: dict(futoi_result),
    )

    def dispatcher_failure(**kwargs):
        captured.update(kwargs)
        raise RuntimeError("stage7 smoke failed")

    monkeypatch.setattr(entrypoint.dispatcher, "run_refresh", dispatcher_failure)

    with pytest.raises(entrypoint.Step10EntrypointError, match="stage7 smoke failed") as exc_info:
        entrypoint.run_refresh(
            through_date="2026-08-28",
            run_id="stage10_failure_test",
            timeout=1.0,
            now_utc=datetime(2026, 8, 30, 8, 0, tzinfo=timezone.utc),
        )

    assert exc_info.value.futoi_result == futoi_result
    assert captured["result_context"] == {
        "entrypoint_schema_version": entrypoint.SCHEMA_VERSION,
        "futoi_factual_refresh": futoi_result,
        "futoi_factual_refresh_blocks_stage7": False,
    }


def test_main_reports_futoi_result_on_dispatcher_failure(monkeypatch, capsys) -> None:
    futoi_result = {"status": "FAILED_NON_BLOCKING", "error": "futoi unavailable"}

    def fail(**kwargs):
        del kwargs
        raise entrypoint.Step10EntrypointError(
            "stage7 failed",
            futoi_result=futoi_result,
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
    assert payload["dispatcher_transaction_context_bound"] is True


def test_stage10_contract_declares_independent_futoi_factual_refresh() -> None:
    text = Path("configs/datasets/step10_rub_daily_refresh.v1.yaml").read_text(encoding="utf-8")
    assert "futoi_raw_factual_refresh_allowed: true" in text
    assert "futoi_raw_factual_refresh_failure_blocks_stage7: false" in text
    assert "stage5_materialization_allowed: false" in text
    assert "current_full_mode_readiness: false" in text

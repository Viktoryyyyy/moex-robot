from __future__ import annotations

from pathlib import Path

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


def test_stage10_contract_declares_independent_futoi_factual_refresh() -> None:
    text = Path("configs/datasets/step10_rub_daily_refresh.v1.yaml").read_text(encoding="utf-8")
    assert "futoi_raw_factual_refresh_allowed: true" in text
    assert "futoi_raw_factual_refresh_failure_blocks_stage7: false" in text
    assert "stage5_materialization_allowed: false" in text
    assert "current_full_mode_readiness: false" in text

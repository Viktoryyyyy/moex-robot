from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from src.moex_research.runners import usdrubf_shadow_scheduler as scheduler


def _args(tmp_path: Path, **overrides: object) -> argparse.Namespace:
    payload: dict[str, object] = {
        "state_root": str(tmp_path),
        "interval_seconds": 300,
        "max_cycles": 2,
        "max_prior_lookback_days": 7,
        "news_timeout_seconds": 10.0,
        "news_max_events": 20,
    }
    payload.update(overrides)
    return argparse.Namespace(**payload)


def _cycle_result(
    root: Path,
    *,
    as_of: str,
    decision_agent_mode: str = "SAFE_WAIT",
    futoi_quality: str = "BLOCKED",
) -> dict[str, object]:
    suffix = as_of.replace(":", "").replace("+", "p")
    market_path = root / f"market_state.{suffix}.json"
    change_path = root / f"change_detection.{suffix}.json"
    return {
        "status": "COMPLETED",
        "as_of_timestamp": as_of,
        "decision_agent_mode": decision_agent_mode,
        "futoi_quality": futoi_quality,
        "news_mode": "LIVE_RSS_DETERMINISTIC_NEUTRAL",
        "macro_mode": "LIVE_CBR",
        "significant_change": False,
        "action_candidate": False,
        "market_state_path": str(market_path),
        "change_detection_path": str(change_path),
    }


def _clock(*values: str):
    iterator = iter(datetime.fromisoformat(value) for value in values)
    return lambda: next(iterator)


def test_config_requires_absolute_state_root_and_bounded_cadence(tmp_path: Path) -> None:
    with pytest.raises(scheduler.ShadowSchedulerError, match="explicit absolute"):
        scheduler._validate_config(_args(tmp_path, state_root="relative/state"))

    with pytest.raises(scheduler.ShadowSchedulerError, match="60..3600"):
        scheduler._validate_config(_args(tmp_path, interval_seconds=59))

    with pytest.raises(scheduler.ShadowSchedulerError, match="60..3600"):
        scheduler._validate_config(_args(tmp_path, interval_seconds=3601))

    cfg = scheduler._validate_config(_args(tmp_path, interval_seconds=60, max_cycles=0))
    assert cfg.state_root == tmp_path
    assert cfg.interval_seconds == 60
    assert cfg.max_cycles == 0


def test_single_instance_lock_is_nonblocking_and_reusable(tmp_path: Path) -> None:
    now = lambda: datetime(2026, 8, 13, 10, 0, tzinfo=timezone.utc)

    with scheduler._single_instance_lock(tmp_path, now_fn=now):
        lock_payload = json.loads((tmp_path / scheduler.LOCK_FILENAME).read_text(encoding="utf-8"))
        assert lock_payload["project"] == "MOEX_Bot"
        assert lock_payload["mode"] == "controlled_shadow_scheduler"
        with pytest.raises(scheduler.SchedulerAlreadyRunning):
            with scheduler._single_instance_lock(tmp_path, now_fn=now):
                pass

    with scheduler._single_instance_lock(tmp_path, now_fn=now):
        pass


def test_scheduler_runs_two_cycles_with_same_root_and_persists_status(tmp_path: Path) -> None:
    cfg = scheduler._validate_config(_args(tmp_path, interval_seconds=60, max_cycles=2))
    seen_live_args: list[argparse.Namespace] = []
    prior_calls: list[Path] = []
    sleep_calls: list[float] = []
    results = iter(
        [
            _cycle_result(tmp_path, as_of="2026-08-13T10:00:01+00:00"),
            _cycle_result(tmp_path, as_of="2026-08-13T10:05:01+00:00"),
        ]
    )
    priors = iter(
        [
            (False, None),
            (True, "2026-08-13T10:00:01+00:00"),
        ]
    )

    def cycle_runner(args: argparse.Namespace) -> dict[str, object]:
        seen_live_args.append(args)
        return next(results)

    def prior_loader(root: Path) -> tuple[bool, str | None]:
        prior_calls.append(root)
        return next(priors)

    final = scheduler.run_scheduler(
        cfg,
        cycle_runner=cycle_runner,
        prior_state_loader=prior_loader,
        sleep_fn=sleep_calls.append,
        now_fn=_clock(
            "2026-08-13T10:00:00+00:00",
            "2026-08-13T10:00:00+00:00",
            "2026-08-13T10:00:02+00:00",
            "2026-08-13T10:05:00+00:00",
            "2026-08-13T10:05:02+00:00",
        ),
    )

    assert final["scheduler_status"] == "COMPLETED"
    assert final["cycle_count"] == 2
    assert final["successful_cycles"] == 2
    assert final["failed_cycles"] == 0
    assert final["prior_state_present"] is True
    assert final["prior_as_of_timestamp"] == "2026-08-13T10:00:01+00:00"
    assert final["last_cycle_as_of_timestamp"] == "2026-08-13T10:05:01+00:00"
    assert prior_calls == [tmp_path, tmp_path]
    assert sleep_calls == [60.0]

    assert len(seen_live_args) == 2
    for live_args in seen_live_args:
        assert Path(live_args.state_root) == tmp_path
        assert live_args.safe_wait_agent is True
        assert live_args.enable_futoi is False
        assert live_args.cbr_macro is True
        assert live_args.live_news is True
        assert live_args.flowise_endpoint is None

    status = json.loads((tmp_path / scheduler.STATUS_FILENAME).read_text(encoding="utf-8"))
    assert status == final


def test_scheduler_fails_closed_on_non_safe_wait_cycle(tmp_path: Path) -> None:
    cfg = scheduler._validate_config(_args(tmp_path, interval_seconds=60, max_cycles=1))
    sleep_calls: list[float] = []

    final = scheduler.run_scheduler(
        cfg,
        cycle_runner=lambda args: _cycle_result(
            tmp_path,
            as_of="2026-08-13T10:00:01+00:00",
            decision_agent_mode="FLOWISE",
        ),
        prior_state_loader=lambda root: (False, None),
        sleep_fn=sleep_calls.append,
        now_fn=_clock(
            "2026-08-13T10:00:00+00:00",
            "2026-08-13T10:00:00+00:00",
            "2026-08-13T10:00:02+00:00",
        ),
    )

    assert final["scheduler_status"] == "BLOCKED"
    assert final["cycle_count"] == 1
    assert final["successful_cycles"] == 0
    assert final["failed_cycles"] == 1
    assert final["last_error_class"] == "ShadowSchedulerError"
    assert "SAFE_WAIT" in str(final["last_error"])
    assert sleep_calls == []


def test_scheduler_blocks_non_advancing_restart_state(tmp_path: Path) -> None:
    cfg = scheduler._validate_config(_args(tmp_path, interval_seconds=60, max_cycles=1))

    final = scheduler.run_scheduler(
        cfg,
        cycle_runner=lambda args: _cycle_result(
            tmp_path,
            as_of="2026-08-13T10:00:01+00:00",
        ),
        prior_state_loader=lambda root: (True, "2026-08-13T10:00:01+00:00"),
        sleep_fn=lambda seconds: None,
        now_fn=_clock(
            "2026-08-13T10:00:00+00:00",
            "2026-08-13T10:00:00+00:00",
            "2026-08-13T10:00:02+00:00",
        ),
    )

    assert final["scheduler_status"] == "BLOCKED"
    assert final["failed_cycles"] == 1
    assert "must advance beyond persisted state" in str(final["last_error"])

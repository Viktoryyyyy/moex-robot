from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from moex_data import step10_rub_refresh_dispatcher as dispatcher
from moex_data import step10_rub_refresh_scheduler as step10


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "configs" / "datasets").mkdir(parents=True)
    (repo / "configs" / "datasets" / "step9_rub_analysis_bundle.v1.yaml").write_text("version: 1\n", encoding="utf-8")
    return repo


def _stage7_base(instrument_id: str, trade_date: str = "2026-08-17") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_id": instrument_id,
                "trade_date": trade_date,
                "period_start_date": trade_date,
                "period_end_date": trade_date,
                "timeframe": "1D",
                "availability_ts_utc": "2026-08-18T03:00:00+00:00",
            }
        ]
    )


def _blocked_governance() -> dict[str, object]:
    return {
        "contract_ref": step10.FUTOI_GOVERNANCE_RELATIVE_PATH.as_posix(),
        "status": "FUTOI_GOVERNED_BLOCKED",
        "required_gate_ids": ["canonical_live_smoke"],
        "blocked_gate_ids": ["canonical_live_smoke"],
        "all_required_gates_pass": False,
        "factual_live_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "promotion_allowed": False,
    }


def _allowed_governance() -> dict[str, object]:
    return {
        "contract_ref": step10.FUTOI_GOVERNANCE_RELATIVE_PATH.as_posix(),
        "status": "LIVE_ACCEPTED",
        "required_gate_ids": ["canonical_live_smoke"],
        "blocked_gate_ids": [],
        "all_required_gates_pass": True,
        "factual_live_authority": True,
        "directional_authority": False,
        "action_authority": False,
        "promotion_allowed": True,
    }


def test_blocked_futoi_still_promotes_stage7_and_never_runs_stage5(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    repo = _repo(tmp_path)
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())
    pointer = root / "state" / "dummy.json"
    pointer.parent.mkdir(parents=True)
    pointer.write_bytes(b"old")
    calls: list[str] = []

    monkeypatch.setattr(step10, "_futoi_stage5_promotion_governance", lambda _repo: _blocked_governance())
    monkeypatch.setattr(step10, "_snapshot_pointers", lambda _root: {pointer: b"old"})
    monkeypatch.setattr(
        step10,
        "_load_stage7_base",
        lambda _root, _as_of: (
            "2026-08-17",
            {name: _stage7_base(name) for name in step10.STAGE7_INSTRUMENTS},
        ),
    )
    monkeypatch.setattr(step10, "_calendar_dates", lambda **_kwargs: ["2026-08-18", "2026-08-19"])

    def fake_stage7(**kwargs):
        calls.append("stage7")
        assert kwargs["trading_dates"] == ["2026-08-18", "2026-08-19"]
        assert kwargs["rebuild_weekly"] is False
        return [{"id": i} for i in range(8)]

    monkeypatch.setattr(step10, "_stage7_refresh", fake_stage7)
    monkeypatch.setattr(step10, "_latest_source_dates", lambda _root, _as_of: ("2026-08-17", "2026-08-17"))

    def fake_sources(**kwargs):
        calls.append("stage3_stage4")
        assert kwargs["latest_trade_date"] == "2026-08-19"
        return {"status": "refreshed", "trade_date": "2026-08-19"}

    monkeypatch.setattr(step10, "_run_stage3_stage4", fake_sources)
    monkeypatch.setattr(
        step10,
        "_pointer_from_output",
        lambda _root, output, _run: (root / "state" / ("s7_" + str(output["id"])), {"id": output["id"]}),
    )

    promoted: list[int] = []

    def fake_promote(records):
        calls.append("promote_stage7")
        promoted.append(len(records))

    monkeypatch.setattr(step10, "_transactional_pointer_replace", fake_promote)
    monkeypatch.setattr(step10, "_capture_written_pointer_state", lambda _records: {})

    def fake_smoke(_as_of):
        calls.append("smoke")
        return {"status": "passed", "daily_block_count": 20, "weekly_block_count": 24}

    monkeypatch.setattr(step10, "_stage9_smoke", fake_smoke)
    monkeypatch.setattr(step10, "_stage5_refresh", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("Stage 5 must not run while FUTOI is governed blocked")))

    result = dispatcher.run_refresh(
        through_date="2026-08-19",
        run_id="dispatcher_blocked",
        repo_root=repo,
        env_file=None,
        now_utc=datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "succeeded"
    assert result["dispatcher_mode"] == dispatcher.BLOCKED_MODE
    assert result["stage5"]["status"] == "governed_blocked_not_run"
    assert result["stage5"]["canonical_pointer_promotion"] is False
    assert result["stage7"]["status"] == "refreshed"
    assert result["futoi_block_does_not_block_stage7"] is True
    assert promoted == [8]
    assert calls == ["stage7", "stage3_stage4", "promote_stage7", "smoke"]


def test_blocked_futoi_sunday_boundary_promotes_stage7_weekly_without_new_trade_date(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    repo = _repo(tmp_path)
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())
    pointer = root / "state" / "dummy.json"
    pointer.parent.mkdir(parents=True)
    pointer.write_bytes(b"old")

    monkeypatch.setattr(step10, "_futoi_stage5_promotion_governance", lambda _repo: _blocked_governance())
    monkeypatch.setattr(step10, "_snapshot_pointers", lambda _root: {pointer: b"old"})
    monkeypatch.setattr(
        step10,
        "_load_stage7_base",
        lambda _root, _as_of: (
            "2026-08-21",
            {name: _stage7_base(name, "2026-08-21") for name in step10.STAGE7_INSTRUMENTS},
        ),
    )
    monkeypatch.setattr(step10, "_calendar_dates", lambda **_kwargs: ["2026-08-21"])

    def fake_stage7(**kwargs):
        assert kwargs["trading_dates"] == []
        assert kwargs["rebuild_weekly"] is True
        assert kwargs["weekly_boundary_end"] == "2026-08-23"
        return [{"id": i} for i in range(8)]

    monkeypatch.setattr(step10, "_stage7_refresh", fake_stage7)
    monkeypatch.setattr(step10, "_latest_source_dates", lambda _root, _as_of: ("2026-08-21", "2026-08-21"))
    monkeypatch.setattr(
        step10,
        "_pointer_from_output",
        lambda _root, output, _run: (root / "state" / ("weekly_" + str(output["id"])), {"id": output["id"]}),
    )
    promoted: list[int] = []
    monkeypatch.setattr(step10, "_transactional_pointer_replace", lambda records: promoted.append(len(records)))
    monkeypatch.setattr(step10, "_capture_written_pointer_state", lambda _records: {})
    monkeypatch.setattr(step10, "_stage9_smoke", lambda _as_of: {"status": "passed", "daily_block_count": 20, "weekly_block_count": 24})

    result = dispatcher.run_refresh(
        through_date="2026-08-23",
        run_id="dispatcher_sunday",
        repo_root=repo,
        env_file=None,
        now_utc=datetime(2026, 8, 24, 1, 0, tzinfo=timezone.utc),
    )

    assert promoted == [8]
    assert result["new_trading_dates"] == []
    assert result["stage5"]["status"] == "governed_blocked_not_run"
    assert result["stage7"]["status"] == "refreshed"


def test_allowed_futoi_delegates_to_full_stage10(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo(tmp_path)
    monkeypatch.setattr(step10, "_futoi_stage5_promotion_governance", lambda _repo: _allowed_governance())
    observed: dict[str, object] = {}

    def fake_full(**kwargs):
        observed.update(kwargs)
        return {"status": "succeeded", "stage": 10}

    monkeypatch.setattr(step10, "run_refresh", fake_full)
    result = dispatcher.run_refresh(
        through_date="2026-08-19",
        run_id="dispatcher_full",
        repo_root=repo,
        env_file=None,
        now_utc=datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc),
    )

    assert result["dispatcher_mode"] == dispatcher.FULL_MODE
    assert result["dispatcher_futoi_governance_checked"] is True
    assert observed["through_date"] == "2026-08-19"
    assert observed["run_id"] == "dispatcher_full"

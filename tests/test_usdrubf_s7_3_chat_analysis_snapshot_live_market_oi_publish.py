from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as overlay


def _live_ready() -> dict[str, object]:
    return {
        "schema_version": "synchronized_live_market_oi_context.v1",
        "status": "READY",
        "quality": {"status": "PASS", "analysis_usable": True},
        "synchronization": {
            "status": "PASS",
            "synchronized": True,
            "as_of_utc": "2026-09-02T15:49:09+00:00",
        },
    }


def test_refresh_snapshot_publishes_live_market_oi_inside_canonical_write(monkeypatch, tmp_path) -> None:
    now = datetime(2026, 9, 2, 15, 49, 10, tzinfo=timezone.utc)
    state_dir = tmp_path / "state"
    output_path = state_dir / overlay.base.CURRENT_FILENAME
    written: dict[str, object] = {}

    monkeypatch.setattr(overlay.base, "_data_root", lambda: tmp_path)
    monkeypatch.setattr(overlay.base, "snapshot_state_dir", lambda _root: state_dir)
    monkeypatch.setattr(overlay.base, "_single_refresh_lock", lambda _state_dir: nullcontext())
    monkeypatch.setattr(overlay.base, "_load_previous", lambda _path: None)
    monkeypatch.setattr(overlay.base, "install_timestamp_policy", lambda: None)
    monkeypatch.setattr(overlay.base, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        overlay.current_context.context,
        "run_refresh_all",
        lambda **_kwargs: {"instrument_results": {}},
    )
    monkeypatch.setattr(overlay.current_context.current, "current_producers", lambda: [])
    monkeypatch.setattr(
        overlay.futoi,
        "build_snapshot",
        lambda **_kwargs: {
            "identity": {"generated_at_utc": now.isoformat()},
            "components": {},
            "authority": {},
            "analysis_views": {},
            "analysis_workflow": {},
        },
    )
    monkeypatch.setattr(overlay.current_context, "_attach_futoi_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        overlay.futoi,
        "_recompute_readiness",
        lambda snapshot: snapshot.setdefault(
            "readiness", {"status": "READY", "component_statuses": {}}
        ),
    )

    def atomic_write(path, snapshot):
        written["path"] = path
        written["snapshot"] = snapshot

    monkeypatch.setattr(overlay.base, "_atomic_write", atomic_write)

    snapshot, path = overlay.refresh_snapshot(
        now_fn=lambda: now,
        live_loader=_live_ready,
    )

    assert path == output_path
    assert written["path"] == output_path
    assert written["snapshot"] is snapshot
    component = snapshot["components"][overlay.COMPONENT]
    assert component["status"] == "READY"
    assert component["data"]["quality"]["analysis_usable"] is True
    assert snapshot["authority"]["live_market_oi_factual_authority"] is True

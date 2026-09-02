from __future__ import annotations

from datetime import datetime, timezone

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as overlay


def test_live_overlay_loads_canonical_project_env_before_fetch(monkeypatch) -> None:
    calls: list[tuple[object, bool]] = []
    monkeypatch.setattr(
        overlay.base,
        "load_dotenv",
        lambda path, override=False: calls.append((path, override)),
    )

    def reader(*, now_fn):
        return (
            {
                "components": {"existing": {"status": "READY"}},
                "authority": {},
                "analysis_views": {},
                "analysis_workflow": {},
            },
            object(),
        )

    def live_loader() -> dict[str, object]:
        assert calls == [(overlay.base.PROJECT_ENV_PATH, False)]
        return {
            "schema_version": "synchronized_live_market_oi_context.v1",
            "status": "READY",
            "quality": {"status": "PASS", "analysis_usable": True},
            "synchronization": {
                "status": "PASS",
                "synchronized": True,
                "as_of_utc": "2026-09-02T10:00:12+00:00",
            },
        }

    snapshot = overlay.load_live_analysis_snapshot(
        now_fn=lambda: datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
        reader=reader,
        live_loader=live_loader,
    )

    assert calls == [(overlay.base.PROJECT_ENV_PATH, False)]
    assert snapshot["components"][overlay.COMPONENT]["status"] == "READY"

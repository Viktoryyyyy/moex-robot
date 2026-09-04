from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as overlay


NOW = datetime(2026, 9, 4, 7, 0, 10, tzinfo=timezone.utc)
TS = "2026-09-04T07:00:00+00:00"


def _instrument(
    logical_id: str,
    secid: str,
    last: float,
    *,
    source_id: str = "moex_apim_forts_rfud_live_marketdata",
    expiry: str | None = None,
) -> dict[str, object]:
    item: dict[str, object] = {
        "logical_id": logical_id,
        "secid": secid,
        "last": last,
        "timestamp": TS,
        "received_at_utc": NOW.isoformat(),
        "age_seconds": 10.0,
        "stale": False,
        "source_id": source_id,
    }
    if expiry is not None:
        item["expiry_date"] = expiry
    return item


def _live_ready() -> dict[str, object]:
    return {
        "schema_version": overlay.live_market.SCHEMA_VERSION,
        "status": "READY",
        "snapshot_received_at_utc": NOW.isoformat(),
        "quality": {
            "status": "PASS",
            "analysis_usable": True,
            "factual_context_usable": True,
        },
        "synchronization": {
            "status": "PASS",
            "synchronized": True,
            "as_of_utc": TS,
        },
        "instruments": {
            "usdrubf": _instrument("usdrubf", "USDRUBF", 91.0),
            "si_front": _instrument("si_front", "SiU6", 92000.0, expiry="2026-09-17"),
            "si_next": _instrument("si_next", "SiZ6", 93000.0, expiry="2026-12-17"),
            "cnyrubf": _instrument("cnyrubf", "CNYRUBF", 12.1),
            "cr_front": _instrument("cr_front", "CRU6", 12.2, expiry="2026-09-17"),
            "cr_next": _instrument("cr_next", "CRZ6", 12.3, expiry="2026-12-17"),
            "cnyrub_tom": _instrument(
                "cnyrub_tom",
                "CNYRUB_TOM",
                12.0,
                source_id="moex_apim_cets_cnyrub_tom_live_marketdata",
            ),
        },
    }


def test_refresh_writes_basis_carry_from_single_live_fetch_into_canonical_snapshot(monkeypatch, tmp_path) -> None:
    state_dir = tmp_path / "state"
    output_path = state_dir / overlay.base.CURRENT_FILENAME
    written: dict[str, object] = {}
    live_calls = 0

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
    monkeypatch.setattr(
        overlay.current_context.delta_context,
        "build_all",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(overlay.current_context.current, "current_producers", lambda: {})
    monkeypatch.setattr(
        overlay.parallel_prefetch,
        "prefetch_producers",
        lambda producers, **_kwargs: producers,
    )
    monkeypatch.setattr(
        overlay.futoi,
        "build_snapshot",
        lambda **_kwargs: {
            "identity": {"generated_at_utc": NOW.isoformat()},
            "components": {},
            "authority": {},
            "analysis_views": {},
            "analysis_workflow": {},
        },
    )
    monkeypatch.setattr(
        overlay.current_context,
        "_attach_futoi_context",
        lambda *_args, **_kwargs: None,
    )

    def atomic_write(path, snapshot):
        written["path"] = path
        written["snapshot"] = snapshot

    monkeypatch.setattr(overlay.base, "_atomic_write", atomic_write)

    def live_loader() -> dict[str, object]:
        nonlocal live_calls
        live_calls += 1
        return _live_ready()

    snapshot, path = overlay.refresh_snapshot(now_fn=lambda: NOW, live_loader=live_loader)

    assert live_calls == 1
    assert path == output_path
    assert written["path"] == output_path
    assert written["snapshot"] is snapshot
    assert snapshot["components"][overlay.COMPONENT]["status"] == "READY"
    basis_component = snapshot["components"][overlay.BASIS_CARRY_COMPONENT]
    assert basis_component["status"] == "PARTIAL"
    assert basis_component["data"]["source_component_ref"] == "components.synchronized_live_market_oi"
    assert basis_component["data"]["live_input_policy"]["additional_live_fetch_performed"] is False
    assert basis_component["data"]["pairs"]["cny_rub"]["status"] == "READY"
    assert basis_component["data"]["pairs"]["usd_rub"]["live_spot_available"] is False
    assert snapshot["analysis_views"]["fresh_basis_carry_component_ref"] == overlay.BASIS_CARRY_COMPONENT
    assert snapshot["analysis_workflow"]["fresh_basis_carry"]["additional_live_fetch_allowed"] is False
    assert snapshot["authority"]["live_basis_carry_directional_authority"] is False
    assert snapshot["authority"]["live_basis_carry_action_authority"] is False
    assert basis_component["data"]["stage5_full_mode_ready"] is False
    assert basis_component["data"]["stage5_pointer_promotion_performed"] is False
    assert snapshot["readiness"]["status"] == "PARTIAL"

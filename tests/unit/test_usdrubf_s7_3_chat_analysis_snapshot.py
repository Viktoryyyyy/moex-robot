from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as snapshot


NOW = datetime(2026, 8, 29, 10, 30, tzinfo=timezone.utc)


def _stage9(scope: str):
    def produce(now: datetime) -> snapshot.ProducedComponent:
        return snapshot.ProducedComponent(
            data={
                "identity": {"scope": scope, "as_of": now.isoformat()},
                "server_core": {
                    "freshness_alignment": {
                        "newest_selected_causal_ts_utc": (now - timedelta(minutes=5)).isoformat()
                    },
                    "blocks": [
                        {
                            "block_id": "stage4.basis.usd_rub",
                            "selected_observation": {"basis": 1.0},
                        },
                        {
                            "block_id": "stage3.spot.cny_tom",
                            "selected_observation": {"close": 11.0},
                        },
                    ],
                },
            },
            data_as_of=now - timedelta(minutes=5),
        )

    return produce


def _simple(name: str, *, minutes_old: int = 1):
    def produce(now: datetime) -> snapshot.ProducedComponent:
        return snapshot.ProducedComponent(
            data={"name": name},
            data_as_of=now - timedelta(minutes=minutes_old),
        )

    return produce


def _market(now: datetime) -> snapshot.ProducedComponent:
    return snapshot.ProducedComponent(
        data={
            "market_data_as_of": (now - timedelta(minutes=5)).isoformat(),
            "active_levels": [{"level_id": "L1"}],
            "level_interactions": [{"level_id": "L1", "state": "AWAY"}],
            "ema_3_19": {
                "direction": "BULLISH_USD",
                "standalone_directional_authority": False,
                "s7_2_verdict": "REJECT_AS_STANDALONE_DIRECTIONAL_SIGNAL",
            },
        },
        data_as_of=now - timedelta(minutes=5),
    )


def _producers():
    return {
        "stage9_daily": _stage9("daily"),
        "stage9_weekly": _stage9("weekly"),
        "live_market_structure": _market,
        "cbr_macro": _simple("macro"),
        "official_news": _simple("news"),
        "cnyrub_spot_live": _simple("cny_spot"),
        "cnyrubf_live": _simple("cny_futures"),
    }


def test_snapshot_is_data_only_and_maps_analysis_to_separate_chats() -> None:
    result = snapshot.build_snapshot(now=NOW, producers=_producers())

    assert result["schema_version"] == "rub_chat_analysis_snapshot.v1"
    assert result["readiness"]["status"] == "READY"
    assert result["authority"]["data_only"] is True
    assert result["authority"]["server_generates_scenario"] is False
    assert result["authority"]["server_generates_buy_sell_out"] is False
    assert result["authority"]["server_generates_invalidation"] is False
    assert result["authority"]["ema_standalone_directional_authority"] is False
    assert result["analysis_workflow"]["weekly_regime"]["consumer"] == "SEPARATE_ANALYSIS_CHAT"
    assert result["analysis_workflow"]["scenario"]["server_generated"] is False
    assert result["analysis_views"]["levels"] == [{"level_id": "L1"}]
    assert result["analysis_views"]["carry"][0]["block_id"] == "stage4.basis.usd_rub"
    assert result["components"]["oil"]["status"] == "GOVERNED_BLOCKED"


def test_component_failure_retains_previous_data_without_pretending_refresh() -> None:
    previous = snapshot.build_snapshot(now=NOW, producers=_producers())
    broken = _producers()

    def fail(_: datetime) -> snapshot.ProducedComponent:
        raise RuntimeError("temporary source failure")

    broken["official_news"] = fail
    later = NOW + timedelta(minutes=10)
    result = snapshot.build_snapshot(now=later, previous=previous, producers=broken)
    news = result["components"]["official_news"]

    assert result["readiness"]["status"] == "PARTIAL"
    assert news["status"] == "RETAINED_PREVIOUS"
    assert news["last_success_at"] == NOW.isoformat()
    assert news["data"] == previous["components"]["official_news"]["data"]
    assert news["refresh_error_class"] == "RuntimeError"
    assert "official_news" in result["readiness"]["retained_previous_components"]


def test_first_component_failure_is_explicitly_unavailable() -> None:
    broken = _producers()

    def fail(_: datetime) -> snapshot.ProducedComponent:
        raise RuntimeError("no market data")

    broken["live_market_structure"] = fail
    result = snapshot.build_snapshot(now=NOW, producers=broken)

    market = result["components"]["live_market_structure"]
    assert market["status"] == "UNAVAILABLE"
    assert market["data"] is None
    assert "live_market_structure" in result["readiness"]["unavailable_components"]


def test_refresh_publishes_atomic_current_under_data_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    result, path = snapshot.refresh_snapshot(now_fn=lambda: NOW, producers=_producers())

    assert path == tmp_path / "state/rub_intelligence/chat_analysis_snapshot/current.json"
    assert path.is_file()
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored["identity"]["generated_at_utc"] == NOW.isoformat()
    assert stored["readiness"]["status"] == "READY"
    assert not list(path.parent.glob(".current.*.tmp"))
    assert result["refresh_policy"]["expected_refresh_interval_seconds"] == 600


def test_read_current_marks_old_snapshot_stale(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    snapshot.refresh_snapshot(now_fn=lambda: NOW, producers=_producers())

    read, _ = snapshot.read_current_snapshot(
        now_fn=lambda: NOW + timedelta(seconds=snapshot.STALE_AFTER_SECONDS + 1)
    )
    assert read["read_freshness"]["status"] == "STALE"
    assert read["read_freshness"]["snapshot_age_seconds"] == snapshot.STALE_AFTER_SECONDS + 1


def test_publication_time_is_collection_completion(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    completed = NOW + timedelta(seconds=34)
    clock = iter((NOW, completed))
    result, path = snapshot.refresh_snapshot(now_fn=lambda: next(clock), producers=_producers())
    assert result["identity"]["generated_at_utc"] == completed.isoformat()
    assert result["identity"]["refresh_started_at_utc"] == NOW.isoformat()
    assert result["components"]["official_news"]["last_success_at"] == completed.isoformat()
    read, _ = snapshot.read_current_snapshot(now_fn=lambda: completed + timedelta(seconds=1))
    assert read["read_freshness"]["snapshot_age_seconds"] == 1
    assert json.loads(path.read_text(encoding="utf-8"))["identity"] == result["identity"]


def test_finalization_preserves_retained_component_success_time() -> None:
    result = snapshot.build_snapshot(now=NOW, producers=_producers())
    retained = result["components"]["official_news"]
    retained["status"] = "RETAINED_PREVIOUS"
    retained["last_success_at"] = (NOW - timedelta(days=1)).isoformat()
    snapshot.finalize_snapshot_timing(result, started=NOW, completed=NOW + timedelta(seconds=34))
    assert retained["last_success_at"] == (NOW - timedelta(days=1)).isoformat()


def test_finalization_rejects_backwards_clock() -> None:
    result = snapshot.build_snapshot(now=NOW, producers=_producers())
    with pytest.raises(snapshot.ChatAnalysisSnapshotError, match="completion precedes start"):
        snapshot.finalize_snapshot_timing(result, started=NOW, completed=NOW - timedelta(seconds=1))


def test_fresh_file_does_not_keep_expired_instrument_usable(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    result, path = snapshot.refresh_snapshot(now_fn=lambda: NOW, producers=_producers())
    result["components"]["synchronized_live_market_oi"] = {
        "status": "PARTIAL", "data": {
            "instruments": {"usdrubf": {"timestamp": NOW.isoformat(), "stale": False, "price_oi_usable": True}},
            "synchronization": {"synchronized": False},
            "quality": {"factual_context_usable": True, "price_oi_usable_by_instrument": {"usdrubf": True}},
        },
    }
    snapshot._atomic_write(path, result)
    original_bytes = path.read_bytes()
    read, _ = snapshot.read_current_snapshot(now_fn=lambda: NOW + timedelta(seconds=61))
    assert read["read_freshness"]["status"] == "FRESH"
    live = read["components"]["synchronized_live_market_oi"]
    assert live["status"] == "UNAVAILABLE"
    assert live["data"]["instruments"]["usdrubf"]["price_oi_usable"] is False
    assert path.read_bytes() == original_bytes


def test_snapshot_state_dir_rejects_symlink_escape(monkeypatch, tmp_path: Path) -> None:
    outside = tmp_path.parent / (tmp_path.name + "_outside")
    outside.mkdir()
    state = tmp_path / "state"
    state.mkdir()
    (state / "rub_intelligence").symlink_to(outside, target_is_directory=True)
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))

    with pytest.raises(snapshot.ChatAnalysisSnapshotError, match="escaped MOEX_DATA_ROOT"):
        snapshot.current_snapshot_path(snapshot._data_root())

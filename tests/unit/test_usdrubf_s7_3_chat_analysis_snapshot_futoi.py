from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as snapshot


NOW = datetime(2026, 8, 30, 10, 0, tzinfo=timezone.utc)


def _simple(name: str):
    def produce(now: datetime) -> base.ProducedComponent:
        return base.ProducedComponent(data={"name": name}, data_as_of=now - timedelta(minutes=1))

    return produce


def _market(now: datetime) -> base.ProducedComponent:
    return base.ProducedComponent(
        data={"active_levels": [], "level_interactions": []},
        data_as_of=now - timedelta(minutes=1),
    )


def _stage9(scope: str):
    def produce(now: datetime) -> base.ProducedComponent:
        return base.ProducedComponent(
            data={
                "identity": {"scope": scope},
                "server_core": {
                    "freshness_alignment": {
                        "newest_selected_causal_ts_utc": (now - timedelta(minutes=1)).isoformat()
                    },
                    "blocks": [],
                },
            },
            data_as_of=now - timedelta(minutes=1),
        )

    return produce


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


def _governance(*, accepted: bool) -> dict[str, object]:
    return {
        "project": "MOEX_Bot",
        "contract_id": "usdrubf_futoi_live_acceptance_governance_v1",
        "status": "FUTOI_LIVE_ACCEPTED_FACTUAL_CONTEXT_ONLY" if accepted else "FUTOI_GOVERNED_BLOCKED",
        "gates": [
            {"gate_id": "g1", "required": True, "status": "PASS"},
            {"gate_id": "g2", "required": True, "status": "PASS" if accepted else "BLOCKED"},
        ],
        "authority": {
            "factual_live_authority": accepted,
            "directional_authority": False,
            "action_authority": False,
        },
    }


def _candidate() -> dict[str, object]:
    ts = "2026-08-29T15:00:00+00:00"
    return {
        "schema_version": "futoi_live_factual_refresh_source_native.v1",
        "project": "MOEX_Bot",
        "status": "PASS",
        "source_id": "moex_algopack_futoi",
        "instrument_id": "si_futures_family",
        "data_as_of": ts,
        "last_success_at": "2026-08-30T00:30:10+00:00",
        "freshness": {"status": "FRESH", "expected_trade_date": "2026-08-29"},
        "quality_status": "PASS",
        "acceptance_status": "PASS",
        "factual": {
            "trade_date": "2026-08-29",
            "snapshot_ts": ts,
            "fiz": {"long": 100, "short": 80, "net": 20},
            "yur": {"long": 80, "short": 100, "net": -20},
            "total_open_interest": 180,
        },
        "provenance": {"accepted_partition_sha256": "a" * 64},
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
    }


def test_governed_blocked_candidate_is_visible_but_not_factual_evidence(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "_load_governance", lambda: _governance(accepted=False))
    monkeypatch.setattr(snapshot, "_load_candidate", lambda _root: _candidate())

    result = snapshot.build_snapshot(
        now=NOW,
        producers=_producers(),
        data_root=tmp_path,
    )
    futoi = result["components"]["futoi_live"]

    assert futoi["status"] == "GOVERNED_BLOCKED"
    assert futoi["data"]["candidate_status"] == "PASS"
    assert futoi["data"]["factual"]["fiz"]["net"] == 20
    assert futoi["data"]["factual_authority"] is False
    assert futoi["data"]["consumer_factual_use_allowed"] is False
    assert futoi["data"]["directional_authority"] is False
    assert futoi["data"]["action_authority"] is False
    assert result["authority"]["futoi_factual_authority"] is False
    assert result["readiness"]["status"] == "READY"


def test_factual_use_requires_all_governance_gates_and_explicit_authority(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "_load_governance", lambda: _governance(accepted=True))
    monkeypatch.setattr(snapshot, "_load_candidate", lambda _root: _candidate())

    result = snapshot.build_snapshot(
        now=NOW,
        producers=_producers(),
        data_root=tmp_path,
    )
    futoi = result["components"]["futoi_live"]

    assert futoi["status"] == "READY"
    assert futoi["data"]["factual_authority"] is True
    assert futoi["data"]["consumer_factual_use_allowed"] is True
    assert futoi["data"]["directional_authority"] is False
    assert futoi["data"]["action_authority"] is False
    assert futoi["data"]["standalone_buy_sell_authority"] is False
    assert result["authority"]["futoi_factual_authority"] is True
    assert result["authority"]["futoi_directional_authority"] is False
    assert result["authority"]["futoi_action_authority"] is False


def test_accepted_governance_retains_previous_factual_context_on_reader_failure(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "_load_governance", lambda: _governance(accepted=True))
    monkeypatch.setattr(snapshot, "_load_candidate", lambda _root: _candidate())
    previous = snapshot.build_snapshot(now=NOW, producers=_producers(), data_root=tmp_path)

    def fail(_root: Path):
        raise RuntimeError("candidate read failed")

    monkeypatch.setattr(snapshot, "_load_candidate", fail)
    result = snapshot.build_snapshot(
        now=NOW + timedelta(minutes=10),
        previous=previous,
        producers=_producers(),
        data_root=tmp_path,
    )
    futoi = result["components"]["futoi_live"]

    assert futoi["status"] == "RETAINED_PREVIOUS"
    assert futoi["data"]["factual_authority"] is True
    assert futoi["refresh_error_class"] == "RuntimeError"
    assert result["readiness"]["status"] == "PARTIAL"
    assert "futoi_live" in result["readiness"]["retained_previous_components"]


def test_blocked_governance_missing_candidate_stays_blocked_not_neutral(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "_load_governance", lambda: _governance(accepted=False))

    def fail(_root: Path):
        raise RuntimeError("candidate absent")

    monkeypatch.setattr(snapshot, "_load_candidate", fail)
    result = snapshot.build_snapshot(now=NOW, producers=_producers(), data_root=tmp_path)
    futoi = result["components"]["futoi_live"]

    assert futoi["status"] == "GOVERNED_BLOCKED"
    assert futoi["data"]["candidate_status"] == "UNAVAILABLE"
    assert futoi["data"]["missing_or_blocked_must_not_be_interpreted_as_neutral"] is True
    assert futoi["data"]["factual_authority"] is False

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as snapshot


NOW = datetime(2026, 8, 30, 10, 0, tzinfo=timezone.utc)
SI = snapshot.futoi_source.SI_INSTRUMENT_ID
CR = snapshot.futoi_source.CR_INSTRUMENT_ID
ACCEPTED_STATUS = "FUTOI_LIVE_ACCEPTED_FACTUAL_CONTEXT_ONLY"
BLOCKED_STATUS = "FUTOI_GOVERNED_BLOCKED"


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


def _instrument_governance(
    *,
    accepted: bool,
    factual_authority: bool | None = None,
    smoke_accepted: bool | None = None,
    local_blockers: list[str] | None = None,
) -> dict[str, object]:
    factual = accepted if factual_authority is None else factual_authority
    smoke = accepted if smoke_accepted is None else smoke_accepted
    blockers = ([] if accepted else ["instrument_not_accepted"]) if local_blockers is None else local_blockers
    return {
        "status": ACCEPTED_STATUS if accepted else BLOCKED_STATUS,
        "canonical_live_smoke_accepted": smoke,
        "factual_live_authority": factual,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "local_blockers": blockers,
    }


def _governance(
    *,
    si_accepted: bool = False,
    cr_accepted: bool = False,
    cr_factual_authority: bool | None = None,
    cr_smoke_accepted: bool | None = None,
    cr_local_blockers: list[str] | None = None,
) -> dict[str, object]:
    all_pass = si_accepted or cr_accepted or cr_factual_authority is True
    return {
        "project": "MOEX_Bot",
        "contract_id": "usdrubf_futoi_live_acceptance_governance_v1",
        "status": BLOCKED_STATUS,
        "instrument_scope": [SI, CR],
        "gates": [
            {"gate_id": "g1", "required": True, "status": "PASS"},
            {"gate_id": "g2", "required": True, "status": "PASS" if all_pass else "BLOCKED"},
        ],
        "authority": {
            "factual_live_authority": False,
            "directional_authority": False,
            "action_authority": False,
        },
        "instrument_acceptance": {
            SI: _instrument_governance(accepted=si_accepted),
            CR: _instrument_governance(
                accepted=cr_accepted,
                factual_authority=cr_factual_authority,
                smoke_accepted=cr_smoke_accepted,
                local_blockers=cr_local_blockers,
            ),
        },
    }


def _candidate(instrument_id: str) -> dict[str, object]:
    ts = "2026-08-29T15:00:00+00:00"
    ticker, secid = ("si", "SiU6") if instrument_id == SI else ("cr", "CRU6")
    return {
        "schema_version": "futoi_live_factual_refresh_source_native.v1",
        "project": "MOEX_Bot",
        "status": "PASS",
        "source_id": "moex_algopack_futoi",
        "instrument_id": instrument_id,
        "data_as_of": ts,
        "last_success_at": "2026-08-30T00:30:10+00:00",
        "freshness": {"status": "FRESH", "accepted_trade_date": "2026-08-29"},
        "quality_status": "PASS",
        "acceptance_status": "PASS",
        "factual": {
            "trade_date": "2026-08-29",
            "snapshot_ts": ts,
            "source_ticker": ticker,
            "secid": secid,
            "fiz": {"long": 100, "short": 80, "net": 20},
            "yur": {"long": 80, "short": 100, "net": -20},
            "total_open_interest": 180,
        },
        "provenance": {"raw_partition_sha256": "a" * 64},
        "factual_authority": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }


def _patch_candidates(monkeypatch) -> None:
    monkeypatch.setattr(snapshot, "_load_candidate", lambda _root, instrument_id: _candidate(instrument_id))


def test_governed_blocked_si_and_cr_candidates_are_visible_separately(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "_load_governance", lambda: _governance())
    _patch_candidates(monkeypatch)

    result = snapshot.build_snapshot(now=NOW, producers=_producers(), data_root=tmp_path)
    si = result["components"]["futoi_live"]
    cr = result["components"]["futoi_live_cr"]

    assert si["status"] == "GOVERNED_BLOCKED"
    assert cr["status"] == "GOVERNED_BLOCKED"
    assert si["data"]["instrument_id"] == SI
    assert cr["data"]["instrument_id"] == CR
    assert si["data"]["factual"]["source_ticker"] == "si"
    assert cr["data"]["factual"]["source_ticker"] == "cr"
    assert si["data"]["factual_authority"] is False
    assert cr["data"]["factual_authority"] is False
    assert result["analysis_views"]["futoi_component_ref"] == "futoi_live"
    assert result["analysis_views"]["futoi_component_refs"] == {
        SI: "futoi_live",
        CR: "futoi_live_cr",
    }
    assert result["analysis_workflow"]["futoi_positioning"]["component_ref"] == "futoi_live"
    assert result["analysis_workflow"]["futoi_positioning"]["instrument_id"] == SI
    assert result["authority"]["futoi_factual_authority"] is False
    assert result["authority"]["futoi_by_instrument"][CR]["component_ref"] == "futoi_live_cr"
    assert result["readiness"]["status"] == "READY"


def test_existing_si_consumer_semantics_remain_backward_compatible(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "_load_governance", lambda: _governance(si_accepted=True, cr_accepted=False))
    _patch_candidates(monkeypatch)

    result = snapshot.build_snapshot(now=NOW, producers=_producers(), data_root=tmp_path)

    assert result["components"]["futoi_live"]["status"] == "READY"
    assert result["components"]["futoi_live"]["data"]["instrument_id"] == SI
    assert result["authority"]["futoi_factual_authority"] is True
    assert result["authority"]["futoi_directional_authority"] is False
    assert result["authority"]["futoi_action_authority"] is False


def test_cr_cannot_inherit_si_factual_authority(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "_load_governance", lambda: _governance(si_accepted=True, cr_accepted=False))
    _patch_candidates(monkeypatch)

    result = snapshot.build_snapshot(now=NOW, producers=_producers(), data_root=tmp_path)

    assert result["components"]["futoi_live"]["data"]["factual_authority"] is True
    assert result["components"]["futoi_live_cr"]["data"]["factual_authority"] is False
    assert result["authority"]["futoi_by_instrument"][SI]["factual_authority"] is True
    assert result["authority"]["futoi_by_instrument"][CR]["factual_authority"] is False
    assert result["authority"]["futoi_by_instrument"][CR]["directional_authority"] is False
    assert result["authority"]["futoi_by_instrument"][CR]["action_authority"] is False
    assert result["authority"]["futoi_by_instrument"][CR]["standalone_buy_sell_authority"] is False


def test_cr_local_smoke_and_blockers_are_required_even_if_global_gates_and_factual_flag_pass(
    monkeypatch, tmp_path: Path
) -> None:
    governance = _governance(
        si_accepted=True,
        cr_accepted=False,
        cr_factual_authority=True,
        cr_smoke_accepted=False,
        cr_local_blockers=["cr_canonical_live_smoke_not_yet_accepted"],
    )
    monkeypatch.setattr(snapshot, "_load_governance", lambda: governance)
    _patch_candidates(monkeypatch)

    result = snapshot.build_snapshot(now=NOW, producers=_producers(), data_root=tmp_path)
    cr = result["components"]["futoi_live_cr"]

    assert cr["status"] == "GOVERNED_BLOCKED"
    assert cr["data"]["governance"]["all_required_gates_pass"] is True
    assert cr["data"]["governance"]["factual_live_authority"] is True
    assert cr["data"]["governance"]["canonical_live_smoke_accepted"] is False
    assert cr["data"]["governance"]["instrument_local_acceptance_pass"] is False
    assert cr["data"]["consumer_factual_use_allowed"] is False
    assert cr["data"]["factual_authority"] is False


def test_accepted_governance_retains_previous_factual_context_per_instrument(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "_load_governance", lambda: _governance(si_accepted=True, cr_accepted=True))
    _patch_candidates(monkeypatch)
    previous = snapshot.build_snapshot(now=NOW, producers=_producers(), data_root=tmp_path)

    def fail(_root: Path, instrument_id: str):
        raise RuntimeError(instrument_id + " candidate read failed")

    monkeypatch.setattr(snapshot, "_load_candidate", fail)
    result = snapshot.build_snapshot(
        now=NOW + timedelta(minutes=10),
        previous=previous,
        producers=_producers(),
        data_root=tmp_path,
    )

    assert result["components"]["futoi_live"]["status"] == "RETAINED_PREVIOUS"
    assert result["components"]["futoi_live_cr"]["status"] == "RETAINED_PREVIOUS"
    assert result["components"]["futoi_live"]["data"]["instrument_id"] == SI
    assert result["components"]["futoi_live_cr"]["data"]["instrument_id"] == CR
    assert result["readiness"]["status"] == "PARTIAL"


def test_blocked_governance_missing_cr_candidate_stays_blocked_not_neutral(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "_load_governance", lambda: _governance())

    def candidate(_root: Path, instrument_id: str):
        if instrument_id == CR:
            raise RuntimeError("CR candidate absent")
        return _candidate(instrument_id)

    monkeypatch.setattr(snapshot, "_load_candidate", candidate)
    result = snapshot.build_snapshot(now=NOW, producers=_producers(), data_root=tmp_path)
    cr = result["components"]["futoi_live_cr"]

    assert cr["status"] == "GOVERNED_BLOCKED"
    assert cr["data"]["instrument_id"] == CR
    assert cr["data"]["candidate_status"] == "UNAVAILABLE"
    assert cr["data"]["missing_or_blocked_must_not_be_interpreted_as_neutral"] is True
    assert cr["data"]["factual_authority"] is False
    assert cr["data"]["directional_authority"] is False
    assert cr["data"]["action_authority"] is False
    assert cr["data"]["stage5_full_mode_ready"] is False
    assert cr["data"]["stage5_pointer_promotion_performed"] is False

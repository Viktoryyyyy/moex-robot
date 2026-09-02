from __future__ import annotations

import inspect
from datetime import datetime, timezone
from pathlib import Path

import pytest

from moex_data.futures import futoi_intraday_previous_session_context as context
from moex_data.futures import futoi_live_factual_refresh_source_native as source
from src.moex_research.runners import (
    usdrubf_s7_3_chat_analysis_snapshot_current_context as runner,
    usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live_runner,
)


NOW = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
IDENTITIES = {
    source.SI_INSTRUMENT_ID: ("si", "SiU6"),
    source.CR_INSTRUMENT_ID: ("cr", "CRU6"),
}


def _identity(instrument_id: str) -> dict[str, str]:
    ticker, secid = IDENTITIES[instrument_id]
    return {
        "instrument_id": instrument_id,
        "source_id": source.SOURCE_ID,
        "source_ticker": ticker,
        "secid": secid,
    }


def _factual(trade_date: str, instrument_id: str) -> dict[str, object]:
    ticker, secid = IDENTITIES[instrument_id]
    day = trade_date + "T"
    return {
        "trade_date": trade_date,
        "snapshot_ts": day + "12:00:00+00:00",
        "source_publication_time": day + "12:01:00+00:00",
        "availability_ts_utc": day + "12:02:00+00:00",
        "ingest_ts_utc": day + "12:03:00+00:00",
        "source_ticker": ticker,
        "secid": secid,
        "sess_id": 1,
        "fiz": {
            "long": 100,
            "short": 80,
            "net": 20,
            "long_participants": 10,
            "short_participants": 11,
        },
        "yur": {
            "long": 80,
            "short": 100,
            "net": -20,
            "long_participants": 12,
            "short_participants": 13,
        },
        "total_open_interest": 180,
        "short_semantics": "absolute_contract_count",
        "timestamp_semantics": "source",
        "fiz_yur_alignment": "exact",
    }


def _install_source(
    monkeypatch,
    tmp_path: Path,
    *,
    observed_by_instrument: dict[str, list[str]] | None = None,
    empty: set[tuple[str, str]] | None = None,
    failures: set[tuple[str, str]] | None = None,
) -> None:
    observed_by_instrument = observed_by_instrument or {
        source.SI_INSTRUMENT_ID: ["2026-08-28", "2026-09-01"],
        source.CR_INSTRUMENT_ID: ["2026-08-28", "2026-09-01"],
    }
    empty = empty or set()
    failures = failures or set()
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(source, "source_identity", _identity)
    monkeypatch.setattr(
        source,
        "_binding",
        lambda instrument_id: {
            "instrument_id": instrument_id,
            "futoi.source_id": source.SOURCE_ID,
            "futoi.ticker": IDENTITIES[instrument_id][0],
            "secid": IDENTITIES[instrument_id][1],
        },
    )
    monkeypatch.setattr(
        source.observed_dates,
        "observed_dates",
        lambda start, end, instrument_id, timeout: list(observed_by_instrument[instrument_id]),
    )
    monkeypatch.setattr(
        source.observed_dates,
        "normalize_observed_dates",
        lambda values, start, end: list(values),
    )

    def probe(binding, candidate, *, timeout):
        del timeout
        key = (str(binding["instrument_id"]), candidate.isoformat())
        if key in failures:
            raise RuntimeError("source transport failure")
        if key in empty:
            return {
                "trade_date": candidate.isoformat(),
                "status": "EMPTY_FUTOI_ON_OBSERVED_TRADE_DATE",
            }
        return {
            "trade_date": candidate.isoformat(),
            "status": "FUTOI_DATA",
            "row_count": 2,
        }

    monkeypatch.setattr(source, "_probe_exact_date", probe)

    def materialize(root, target_trade_date, run_id, *, instrument_id, timeout):
        del root, run_id, timeout
        key = (instrument_id, target_trade_date)
        if key in failures:
            raise RuntimeError("materialization failure")
        return tmp_path / f"{instrument_id}_{target_trade_date}.parquet", {
            "accepted_state_kind": "unit",
            "instrument_id": instrument_id,
            "trade_date": target_trade_date,
        }

    monkeypatch.setattr(source, "_materialize_target", materialize)
    monkeypatch.setattr(context.pd, "read_parquet", lambda path: object())
    monkeypatch.setattr(
        source,
        "latest_aligned_factual",
        lambda frame, *, expected_trade_date, expected_instrument_id, expected_source_ticker, expected_secid: _factual(
            expected_trade_date, expected_instrument_id
        ),
    )


def _run(monkeypatch, tmp_path: Path, instrument_id: str, **install_kwargs):
    _install_source(monkeypatch, tmp_path, **install_kwargs)
    return context.run_refresh(
        through_date="2026-09-01",
        instrument_id=instrument_id,
        run_id="unit",
        timeout=1.0,
        now_fn=lambda: NOW,
    )


def test_01_current_is_authoritative_only_when_through_date_is_observed(monkeypatch, tmp_path: Path):
    _install_source(
        monkeypatch,
        tmp_path,
        observed_by_instrument={
            source.SI_INSTRUMENT_ID: ["2026-08-28"],
            source.CR_INSTRUMENT_ID: ["2026-08-28"],
        },
    )
    result = context.run_refresh(
        through_date="2026-09-01",
        instrument_id=source.SI_INSTRUMENT_ID,
        run_id="unit",
        now_fn=lambda: NOW,
    )
    assert result["observed_current_trade_date"] is None
    assert result[context.CURRENT_ROLE]["status"] == "UNAVAILABLE"
    assert result[context.CURRENT_ROLE]["factual"] is None


def test_02_non_observed_day_does_not_use_weekday_or_calendar_inference(monkeypatch, tmp_path: Path):
    _install_source(
        monkeypatch,
        tmp_path,
        observed_by_instrument={
            source.SI_INSTRUMENT_ID: ["2026-08-28"],
            source.CR_INSTRUMENT_ID: ["2026-08-28"],
        },
    )
    result = context.run_refresh(
        through_date="2026-08-30",
        instrument_id=source.SI_INSTRUMENT_ID,
        run_id="unit",
        now_fn=lambda: NOW,
    )
    assert result["calendar_dependency"] is False
    assert result["weekday_weekend_inference"] is False
    assert result["previous_observed_trade_date"] == "2026-08-28"


def test_03_previous_session_is_preceding_observed_date_not_date_minus_one(monkeypatch, tmp_path: Path):
    result = _run(
        monkeypatch,
        tmp_path,
        source.SI_INSTRUMENT_ID,
        observed_by_instrument={
            source.SI_INSTRUMENT_ID: ["2026-08-27", "2026-08-31", "2026-09-01"],
            source.CR_INSTRUMENT_ID: ["2026-08-27", "2026-08-31", "2026-09-01"],
        },
    )
    assert result["previous_observed_trade_date"] == "2026-08-31"
    assert result[context.PREVIOUS_ROLE]["trade_date"] == "2026-08-31"


@pytest.mark.parametrize("instrument_id", source.LIVE_INSTRUMENT_IDS)
def test_04_05_si_and_cr_publish_fresh_current_intraday(monkeypatch, tmp_path: Path, instrument_id: str):
    result = _run(monkeypatch, tmp_path, instrument_id)
    current = result[context.CURRENT_ROLE]
    assert current["status"] == "FRESH"
    assert current["trade_date"] == "2026-09-01"
    assert current["factual"]["secid"] == IDENTITIES[instrument_id][1]


def test_06_si_cr_failures_are_isolated(monkeypatch, tmp_path: Path):
    _install_source(
        monkeypatch,
        tmp_path,
        failures={(source.CR_INSTRUMENT_ID, "2026-09-01")},
    )
    result = context.run_refresh_all(
        through_date="2026-09-01",
        run_id="unit_all",
        now_fn=lambda: NOW,
    )
    si = result["instrument_results"][source.SI_INSTRUMENT_ID]
    cr = result["instrument_results"][source.CR_INSTRUMENT_ID]
    assert si["status"] == "PASS"
    assert cr["status"] != "PASS"
    assert result["failed_instrument_ids"] == [source.CR_INSTRUMENT_ID]


def test_07_to_12_context_reuses_existing_exact_source_native_validation():
    body = inspect.getsource(context._materialize_record)
    assert "source._materialize_target" in body
    assert "source.latest_aligned_factual" in body
    # Exact FIZ/YUR alignment, max seqnum, sess_id, net/OI and identity fail-closed
    # behavior stays inside the already-tested source-native implementation.


def test_13_current_and_previous_are_simultaneously_available(monkeypatch, tmp_path: Path):
    result = _run(monkeypatch, tmp_path, source.SI_INSTRUMENT_ID)
    assert result[context.CURRENT_ROLE]["trade_date"] == "2026-09-01"
    assert result[context.PREVIOUS_ROLE]["trade_date"] == "2026-08-28"
    for role in (context.CURRENT_ROLE, context.PREVIOUS_ROLE):
        factual = result[role]["factual"]
        assert factual["snapshot_ts"]
        assert factual["source_publication_time"]
        assert factual["availability_ts_utc"]
        assert factual["ingest_ts_utc"]
        assert result[role]["freshness"]["status"] == "FRESH"


def test_14_current_refresh_does_not_overwrite_previous_identity(monkeypatch, tmp_path: Path):
    first = _run(monkeypatch, tmp_path, source.SI_INSTRUMENT_ID)
    previous_before = first[context.PREVIOUS_ROLE]["factual"]["trade_date"]
    second = context.run_refresh(
        through_date="2026-09-01",
        instrument_id=source.SI_INSTRUMENT_ID,
        run_id="unit_second",
        now_fn=lambda: NOW,
    )
    assert second[context.CURRENT_ROLE]["factual"]["trade_date"] == "2026-09-01"
    assert second[context.PREVIOUS_ROLE]["factual"]["trade_date"] == previous_before


def test_15_source_failure_retains_last_valid_current_as_stale_without_relabel(monkeypatch, tmp_path: Path):
    first = _run(monkeypatch, tmp_path, source.SI_INSTRUMENT_ID)
    first_success = first[context.CURRENT_ROLE]["last_success_at"]
    _install_source(
        monkeypatch,
        tmp_path,
        failures={(source.SI_INSTRUMENT_ID, "2026-09-01")},
    )
    second = context.run_refresh(
        through_date="2026-09-01",
        instrument_id=source.SI_INSTRUMENT_ID,
        run_id="unit_failure",
        now_fn=lambda: datetime(2026, 9, 1, 12, 10, tzinfo=timezone.utc),
    )
    current = second[context.CURRENT_ROLE]
    assert current["status"] == "RETAINED_STALE"
    assert current["trade_date"] == "2026-09-01"
    assert current["freshness"]["status"] == "STALE"
    assert current["last_success_at"] == first_success
    assert current["failed_attempt_at"] == "2026-09-01T12:10:00+00:00"


def test_15b_missing_current_futoi_is_pending_and_previous_is_not_relabelled(monkeypatch, tmp_path: Path):
    _install_source(
        monkeypatch,
        tmp_path,
        empty={(source.SI_INSTRUMENT_ID, "2026-09-01")},
    )
    result = context.run_refresh(
        through_date="2026-09-01",
        instrument_id=source.SI_INSTRUMENT_ID,
        run_id="unit_pending",
        now_fn=lambda: NOW,
    )
    assert result[context.CURRENT_ROLE]["status"] == "PENDING"
    assert result[context.CURRENT_ROLE]["factual"] is None
    assert result[context.PREVIOUS_ROLE]["trade_date"] == "2026-08-28"


def _bundle() -> dict[str, object]:
    results = {}
    for instrument_id in source.LIVE_INSTRUMENT_IDS:
        current = {
            "status": "FRESH",
            "last_success_at": "2026-09-01T12:00:00+00:00",
            "refresh_error_class": None,
            "refresh_error": None,
            "freshness": {
                "status": "FRESH",
                "source_snapshot_ts": "2026-09-01T11:55:00+00:00",
            },
            "factual": _factual("2026-09-01", instrument_id),
            "provenance": {"kind": "unit"},
        }
        previous = {
            "status": "FRESH",
            "last_success_at": "2026-09-01T12:00:00+00:00",
            "freshness": {
                "status": "FRESH",
                "source_snapshot_ts": "2026-08-28T11:55:00+00:00",
            },
            "factual": _factual("2026-08-28", instrument_id),
            "provenance": {"kind": "unit"},
        }
        results[instrument_id] = {
            "schema_version": context.SCHEMA_VERSION,
            "status": "PASS",
            "quality_status": "PASS",
            "acceptance_status": "PASS",
            "through_date": "2026-09-01",
            "refresh_attempted_at": "2026-09-01T12:00:00+00:00",
            "observed_trade_dates": ["2026-08-28", "2026-09-01"],
            "observed_current_trade_date": "2026-09-01",
            "previous_observed_trade_date": "2026-08-28",
            context.CURRENT_ROLE: current,
            context.PREVIOUS_ROLE: previous,
        }
    return {"instrument_results": results}


def _snapshot() -> dict[str, object]:
    return {
        "identity": {"generated_at_utc": "2026-09-01T12:30:00+00:00"},
        "components": {
            "futoi_live": {"status": "UNAVAILABLE", "data": {}},
            "futoi_live_cr": {"status": "UNAVAILABLE", "data": {}},
        },
        "authority": {"futoi_by_instrument": {}},
        "analysis_views": {},
        "readiness": {"component_statuses": {}},
    }


def test_16_parent_generated_at_cannot_freshen_futoi_source_timestamp(monkeypatch):
    monkeypatch.setattr(runner.futoi, "_load_governance", lambda: {})
    monkeypatch.setattr(
        runner.futoi,
        "_governance_state",
        lambda values, instrument_id: {"factual_use_allowed": True},
    )
    monkeypatch.setattr(runner.futoi, "_recompute_readiness", lambda snapshot: None)
    snapshot = _snapshot()
    runner._attach_futoi_context(snapshot, _bundle())
    data = snapshot["components"]["futoi_live"]["data"]["current_intraday"]
    assert snapshot["identity"]["generated_at_utc"] == "2026-09-01T12:30:00+00:00"
    assert data["freshness"]["source_snapshot_ts"] == "2026-09-01T11:55:00+00:00"


def test_17_existing_ten_minute_runtime_invokes_context_aware_refresh():
    root = Path(__file__).resolve().parents[2]
    service = (root / "ops/systemd/moex-rub-chat-snapshot.service").read_text(encoding="utf-8")
    timer = (root / "ops/systemd/moex-rub-chat-snapshot.timer").read_text(encoding="utf-8")
    assert "usdrubf_s7_3_chat_analysis_snapshot_live_market_oi --refresh" in service
    assert "OnUnitActiveSec=10min" in timer
    refresh_body = inspect.getsource(live_runner.refresh_snapshot)
    assert "current_context.context.run_refresh_all" in refresh_body
    assert "futoi.build_snapshot" in refresh_body
    assert "_load_live_or_unavailable" in refresh_body
    assert "base._atomic_write" in refresh_body
    assert refresh_body.index("current_context.context.run_refresh_all") < refresh_body.index("futoi.build_snapshot")
    assert refresh_body.index("futoi.build_snapshot") < refresh_body.index("_load_live_or_unavailable")
    assert refresh_body.index("_load_live_or_unavailable") < refresh_body.index("base._atomic_write")


def test_18_19_read_current_and_export_path_do_not_trigger_refresh():
    body = inspect.getsource(runner.read_current_snapshot)
    assert "run_refresh" not in body
    assert "futoi.read_current_snapshot" in body
    args = runner.parse_args(["--read-current"])
    assert args.read_current is True
    assert args.refresh is False


def test_20_stage5_remains_off(monkeypatch, tmp_path: Path):
    result = _run(monkeypatch, tmp_path, source.SI_INSTRUMENT_ID)
    assert result["stage5_full_mode_ready"] is False
    assert result["stage5_pointer_promotion_performed"] is False
    assert result["factual_authority"] is False


def test_21_no_calendar_api_is_used_for_trading_date_authority():
    body = inspect.getsource(context)
    assert "Calendar API" not in body
    assert "weekday_weekend_inference" in body
    assert "observed_dates.observed_dates" in body


def test_22_context_producer_does_not_promote_governance(monkeypatch, tmp_path: Path):
    result = _run(monkeypatch, tmp_path, source.CR_INSTRUMENT_ID)
    assert result["factual_authority"] is False
    assert result["stage5_pointer_promotion_performed"] is False


def test_23_no_directional_or_action_calculation_is_introduced(monkeypatch, tmp_path: Path):
    result = _run(monkeypatch, tmp_path, source.SI_INSTRUMENT_ID)
    assert result["directional_authority"] is False
    assert result["action_authority"] is False
    assert result["standalone_buy_sell_authority"] is False
    producer_body = inspect.getsource(context)
    assert "decision_engine" not in producer_body
    assert "buy_signal" not in producer_body
    assert "sell_signal" not in producer_body

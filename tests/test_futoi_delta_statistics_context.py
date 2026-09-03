from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import futoi_delta_statistics_context as delta
from moex_data.futures import futoi_intraday_previous_session_context as session_context


OBSERVED = [
    "2026-08-12",
    "2026-08-13",
    "2026-08-14",
    "2026-08-17",
    "2026-08-18",
    "2026-08-19",
    "2026-08-20",
    "2026-08-21",
    "2026-08-22",
    "2026-08-23",
    "2026-08-24",
    "2026-08-25",
    "2026-08-26",
    "2026-08-27",
    "2026-08-28",
    "2026-08-29",
    "2026-08-30",
    "2026-08-31",
    "2026-09-01",
    "2026-09-02",
    "2026-09-03",
]


def _factual(trade_date: str, *, net: int, long_participants: int = 70) -> dict[str, object]:
    oi = 1000
    fiz_long = (oi + net) // 2
    fiz_short = oi - fiz_long
    yur_long = oi - fiz_long
    yur_short = fiz_long
    return {
        "trade_date": trade_date,
        "snapshot_ts": trade_date + "T18:00:00+00:00",
        "source_publication_time": trade_date + "T18:00:01+00:00",
        "availability_ts_utc": trade_date + "T18:00:02+00:00",
        "ingest_ts_utc": trade_date + "T18:00:03+00:00",
        "fiz": {
            "long": fiz_long,
            "short": fiz_short,
            "net": net,
            "long_participants": long_participants,
            "short_participants": 30,
        },
        "yur": {
            "long": yur_long,
            "short": yur_short,
            "net": -net,
            "long_participants": 40,
            "short_participants": 60,
        },
        "total_open_interest": oi,
    }


def _record(trade_date: str, *, net: int, status: str = "FRESH") -> dict[str, object]:
    return {
        "status": status,
        "trade_date": trade_date,
        "factual": _factual(trade_date, net=net),
        "provenance": {"trade_date": trade_date},
        "refresh_error_class": None,
        "refresh_error": None,
    }


def _raw_context(*, current_status: str = "FRESH") -> dict[str, object]:
    return {
        "observed_current_trade_date": "2026-09-03",
        "previous_observed_trade_date": "2026-09-02",
        session_context.CURRENT_ROLE: _record(
            "2026-09-03" if current_status == "FRESH" else "2026-09-02",
            net=400 if current_status == "FRESH" else 350,
            status=current_status,
        ),
        session_context.PREVIOUS_ROLE: _record("2026-09-02", net=350),
    }


def _eod(rows: int = 510) -> pd.DataFrame:
    dates = pd.date_range(end="2026-08-17", periods=rows, freq="D")
    return pd.DataFrame(
        {
            "instrument_id": ["si_futures_family"] * rows,
            "trade_date": dates.strftime("%Y-%m-%d"),
            "snapshot_ts_utc": dates.tz_localize("UTC") + pd.Timedelta(hours=18),
            "availability_ts_utc": dates.tz_localize("UTC") + pd.Timedelta(hours=19),
            "phys_net": [200] * rows,
            "phys_long": [600] * rows,
            "phys_short_abs": [400] * rows,
            "phys_long_num": [60] * rows,
            "phys_short_num": [40] * rows,
            "legal_net": [-200] * rows,
            "legal_long": [400] * rows,
            "legal_short_abs": [600] * rows,
            "legal_long_num": [40] * rows,
            "legal_short_num": [60] * rows,
            "total_open_interest": [1000] * rows,
            "phys_net_share_of_oi": [0.2] * rows,
            "legal_net_share_of_oi": [-0.2] * rows,
            "source_partition_ref": ["${MOEX_DATA_ROOT}/frozen"] * rows,
            "source_canonical_partition_ref": ["${MOEX_DATA_ROOT}/raw"] * rows,
            "source_frozen_partition_sha256": ["a" * 64] * rows,
        }
    )


def _patch_sources(monkeypatch: pytest.MonkeyPatch, *, raw_5d_available: bool = True) -> None:
    monkeypatch.setattr(
        delta,
        "_observed_witness",
        lambda *_args, **_kwargs: {
            "status": "PASS",
            "authority_source_id": delta.OBSERVED_DATE_AUTHORITY_SOURCE_ID,
            "current_observed_trade_date": "2026-09-03",
            "previous_observed_trade_date": "2026-09-02",
            "observed_trade_dates": list(OBSERVED),
            "calendar_dependency": False,
            "weekday_weekend_inference": False,
            "provenance": {"pointer_ref": "stage7"},
        },
    )
    monkeypatch.setattr(
        delta,
        "_accepted_eod",
        lambda *_args, **_kwargs: (_eod(), {"pointer_ref": "stage5-eod"}),
    )

    def raw(_root, *, instrument_id, trade_date):
        assert instrument_id == "si_futures_family"
        assert trade_date == "2026-08-29"
        if not raw_5d_available:
            return {
                "status": "UNAVAILABLE",
                "trade_date": trade_date,
                "factual": None,
                "provenance": None,
                "reason": "canonical_raw_partition_missing",
            }
        return {
            "status": "AVAILABLE",
            "trade_date": trade_date,
            "factual": delta._normalized_factual(
                _factual(trade_date, net=250), field="lag5"
            ),
            "provenance": {"raw_partition_ref": trade_date},
        }

    monkeypatch.setattr(delta, "_raw_factual", raw)


def test_exact_observed_session_lags_and_all_required_deltas(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _patch_sources(monkeypatch)
    result = delta.build_instrument_context(
        root=tmp_path,
        instrument_id="si_futures_family",
        raw_context=_raw_context(),
        as_of=datetime(2026, 9, 3, 15, tzinfo=timezone.utc),
    )

    assert result["lag_targets"] == {
        "delta_1d": "2026-09-02",
        "delta_5d": "2026-08-29",
        "delta_20d": "2026-08-12",
    }
    assert result["deltas"]["delta_1d"]["values"]["fiz.net"] == 50
    assert result["deltas"]["delta_5d"]["values"]["fiz.net"] == 150
    assert result["deltas"]["delta_20d"]["values"]["fiz.net"] == 200
    assert result["deltas"]["delta_1d"]["values"]["yur.net"] == -50
    assert result["deltas"]["delta_1d"]["values"]["total_open_interest"] == 0
    assert result["current"]["factual"]["fiz"]["net_share_of_oi"] == pytest.approx(0.4)
    assert result["current"]["factual"]["yur"]["net_share_of_oi"] == pytest.approx(-0.4)
    assert result["current"]["factual"]["participant_count_semantics"]["unique_participant_count"] is None
    assert result["current"]["factual"]["participant_count_semantics"]["long_plus_short_must_not_be_interpreted_as_unique_participants"] is True


def test_stale_current_is_unavailable_and_never_used_as_current(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _patch_sources(monkeypatch)
    result = delta.build_instrument_context(
        root=tmp_path,
        instrument_id="si_futures_family",
        raw_context=_raw_context(current_status="RETAINED_STALE"),
        as_of=datetime(2026, 9, 3, 15, tzinfo=timezone.utc),
    )

    assert result["current"]["status"] == "UNAVAILABLE"
    assert result["current"]["source_record_status"] == "RETAINED_STALE"
    assert result["current"]["retained_factual_trade_date"] == "2026-09-02"
    assert result["deltas"]["delta_1d"]["status"] == "UNAVAILABLE"
    assert result["deltas"]["delta_5d"]["status"] == "UNAVAILABLE"
    assert result["deltas"]["delta_20d"]["status"] == "UNAVAILABLE"
    assert result["statistics"]["status"] == "UNAVAILABLE"


def test_missing_exact_5th_observed_target_is_not_substituted(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _patch_sources(monkeypatch, raw_5d_available=False)
    result = delta.build_instrument_context(
        root=tmp_path,
        instrument_id="si_futures_family",
        raw_context=_raw_context(),
        as_of=datetime(2026, 9, 3, 15, tzinfo=timezone.utc),
    )

    assert result["lag_targets"]["delta_5d"] == "2026-08-29"
    assert result["deltas"]["delta_5d"]["status"] == "UNAVAILABLE"
    assert result["deltas"]["delta_5d"]["values"] is None
    assert result["deltas"]["delta_1d"]["status"] == "AVAILABLE"
    assert result["deltas"]["delta_20d"]["status"] == "AVAILABLE"


def test_statistics_reuse_252_504_population_and_weak_percentile_semantics(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _patch_sources(monkeypatch)
    result = delta.build_instrument_context(
        root=tmp_path,
        instrument_id="si_futures_family",
        raw_context=_raw_context(),
        as_of=datetime(2026, 9, 3, 15, tzinfo=timezone.utc),
    )
    stats = result["statistics"]

    assert stats["status"] == "AVAILABLE"
    assert stats["semantics"]["windows_observations"] == [252, 504]
    assert stats["semantics"]["future_rows_used"] is False
    for window in ("252", "504"):
        assert stats["variables"]["fiz.net_share_of_oi"]["windows"][window]["status"] == "AVAILABLE"
        assert 0.0 <= stats["variables"]["fiz.net_share_of_oi"]["windows"][window]["percentile"] <= 1.0
        assert stats["variables"]["total_open_interest"]["windows"][window]["zscore"] is None
        assert stats["variables"]["total_open_interest"]["windows"][window]["percentile"] == pytest.approx(1.0)


def test_statistics_ignore_future_history_rows() -> None:
    history = _eod()
    future = history.iloc[[-1]].copy()
    future["trade_date"] = "2026-09-10"
    future["phys_net"] = -800
    future["legal_net"] = 800
    future["phys_net_share_of_oi"] = -0.8
    future["legal_net_share_of_oi"] = 0.8
    combined = pd.concat([history, future], ignore_index=True)
    current = {
        "status": "AVAILABLE",
        "factual": delta._normalized_factual(_factual("2026-09-03", net=400), field="current"),
    }

    baseline = delta._statistics(current=current, eod=history, witness_dates=OBSERVED)
    with_future = delta._statistics(current=current, eod=combined, witness_dates=OBSERVED)
    assert baseline == with_future


def test_zero_variance_yields_null_zscore_and_weak_percentile_one() -> None:
    current = {
        "status": "AVAILABLE",
        "factual": delta._normalized_factual(_factual("2026-09-03", net=200), field="current"),
    }
    stats = delta._statistics(current=current, eod=_eod(), witness_dates=OBSERVED)
    for field in delta.STAT_FIELDS:
        for window in ("252", "504"):
            assert stats["variables"][field]["windows"][window]["zscore"] is None
            assert stats["variables"][field]["windows"][window]["percentile"] == pytest.approx(1.0)


def test_invalid_fiz_yur_balance_fails_closed() -> None:
    factual = _factual("2026-09-03", net=400)
    factual["yur"]["net"] = -399
    with pytest.raises(delta.FutoiDeltaStatisticsError, match="net identity failed"):
        delta._normalized_factual(factual, field="bad")


def test_contract_keeps_all_authority_and_stage5_promotion_off() -> None:
    contract = json.loads(
        (Path(__file__).resolve().parents[1] / delta.CONTRACT_REF).read_text(encoding="utf-8")
    )
    assert contract["observed_date_semantics"]["moex_calendar_api_allowed"] is False
    assert contract["observed_date_semantics"]["weekday_weekend_inference"] is False
    assert contract["authority"]["factual_authority"] is False
    assert contract["authority"]["directional_authority"] is False
    assert contract["authority"]["action_authority"] is False
    assert contract["authority"]["standalone_buy_sell_authority"] is False
    assert contract["authority"]["stage5_full_mode_ready"] is False
    assert contract["authority"]["stage5_pointer_promotion_performed"] is False

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from moex_research.intelligence import usdrubf_reversal_candle_monitor as monitor


def _rules() -> dict[str, object]:
    return json.loads(
        (Path(__file__).parents[1] / monitor.DEFAULT_RULES_PATH).read_text(encoding="utf-8")
    )


def _alert_metrics() -> dict[str, object]:
    return {
        "trade_date": "2026-10-06",
        "high": 86.29,
        "close": 85.37,
        "relative_volume_20": 1.31,
        "volume_vs_previous": 0.94,
        "daily_flow_pct": 5.77,
        "daily_oi_pct": 2.52,
        "close_location": 0.19,
        "upper_wick_fraction": 0.44,
        "close_below_session_vwap": True,
        "peak_to_close_atr": 0.61,
        "new_5_session_high": True,
        "previous_session_up": True,
        "previous_session_close_location": 0.94,
        "post_peak_60m_flow_pct": 2.37,
        "break_previous_6_bar_low_with_negative_delta_within_60m": True,
        "fiz_long_change_pp": 2.22,
    }


def test_reference_case_alerts_without_bearish_futoi_or_daily_flow() -> None:
    decision = monitor.evaluate(_alert_metrics(), _rules())
    assert decision["status"] == "ALERT"
    assert decision["mandatory_gates"] == {
        "new_5_session_high": True,
        "price_rejection": True,
        "intraday_confirmation": True,
    }
    assert decision["score"] >= decision["alert_score_threshold"]
    assert decision["score_checks"]["fiz_long_change_pp_lte_minus_1"] is False
    assert decision["score_checks"]["daily_flow_pct_lte_minus_2"] is False


def test_negative_flow_without_price_rejection_never_alerts() -> None:
    metrics = _alert_metrics()
    metrics.update(
        upper_wick_fraction=0.1,
        close_location=0.7,
        daily_flow_pct=-20.0,
        daily_oi_pct=-5.0,
        post_peak_60m_flow_pct=-30.0,
        fiz_long_change_pp=-4.0,
    )
    decision = monitor.evaluate(metrics, _rules())
    assert decision["score"] >= decision["alert_score_threshold"]
    assert decision["mandatory_gates"]["price_rejection"] is False
    assert decision["status"] == "NO_ALERT"


def test_futoi_requires_balanced_recent_endpoints() -> None:
    rows = []
    for tradedate, tradetime, fiz_long in (
        ("2026-10-05", "23:45:00", 100),
        ("2026-10-06", "23:45:00", 90),
    ):
        rows.extend(
            [
                {"tradedate": tradedate, "tradetime": tradetime, "systime": tradedate + " 23:45:05", "clgroup": "FIZ", "pos": 20, "pos_long": fiz_long, "pos_short": fiz_long - 20},
                {"tradedate": tradedate, "tradetime": tradetime, "systime": tradedate + " 23:45:05", "clgroup": "YUR", "pos": -20, "pos_long": 80, "pos_short": 100},
            ]
        )
    value, quality = monitor.futoi_change(
        pd.DataFrame(rows),
        previous_date="2026-10-05",
        target_date="2026-10-06",
        previous_end=pd.Timestamp("2026-10-05 23:50:00"),
        target_end=pd.Timestamp("2026-10-06 23:50:00"),
        maximum_age_minutes=10,
    )
    assert value == -100 * 10 / 180
    assert quality["status"] == "READY"

    rows[-1]["pos"] = -18
    value, quality = monitor.futoi_change(
        pd.DataFrame(rows),
        previous_date="2026-10-05",
        target_date="2026-10-06",
        previous_end=pd.Timestamp("2026-10-05 23:50:00"),
        target_end=pd.Timestamp("2026-10-06 23:50:00"),
        maximum_age_minutes=10,
    )
    assert value is None
    assert quality["status"] == "UNAVAILABLE"


def test_monitor_persists_data_blocked_and_never_notifies(tmp_path: Path) -> None:
    result = monitor.run_monitor(
        trade_date="2026-10-06",
        repo_root=Path(__file__).parents[1],
        state_root=tmp_path / "state",
        env_file=None,
        now_utc=datetime(2026, 10, 7, 0, 30, tzinfo=timezone.utc),
        tradestats_frame=pd.DataFrame({"unexpected": [1]}),
        futoi_frame=pd.DataFrame(),
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["notify"] is False
    current = json.loads((tmp_path / "state" / "current.json").read_text(encoding="utf-8"))
    assert current["status"] == "DATA_BLOCKED"
    assert (tmp_path / "state" / "history" / "2026-10-06.json").is_file()

from __future__ import annotations

import pandas as pd

from src.research.ema.lib_ema_search import generate_ema_signals, normalize_ohlc_dataframe, run_point_backtest


def test_no_lookahead_position_uses_prior_bar_signal_only() -> None:
    bars = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2026-01-01 10:00:00",
                    "2026-01-01 10:05:00",
                    "2026-01-01 10:10:00",
                    "2026-01-01 10:15:00",
                ]
            ),
            "open": [100.0, 101.0, 102.0, 103.0],
            "high": [101.0, 102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0, 102.0],
            "close": [100.0, 102.0, 104.0, 99.0],
        }
    )

    out = generate_ema_signals(bars, ema_fast_span=1, ema_slow_span=2, mode="trend_long_short")

    expected_position = out["signal"].shift(1).fillna(0.0)
    assert out["position"].equals(expected_position)
    assert float(out.iloc[0]["position"]) == 0.0


def test_backtest_semantics_preserve_reversal_fee_and_forced_terminal_close() -> None:
    bars = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2026-01-01 10:00:00",
                    "2026-01-01 10:05:00",
                    "2026-01-01 10:10:00",
                ]
            ),
            "open": [100.0, 110.0, 90.0],
            "close": [101.0, 109.0, 95.0],
            "position": [0.0, 1.0, -1.0],
        }
    )

    out = run_point_backtest(bars, commission_points=2.0)

    assert out["trades"].tolist() == [0.0, 1.0, 2.0]
    assert out["fee"].tolist() == [0.0, 2.0, 4.0]
    assert out["terminal_fee"].tolist() == [0.0, 0.0, 2.0]
    assert out["next_open"].iloc[0] == 110.0
    assert out["next_open"].iloc[1] == 90.0
    assert pd.isna(out["next_open"].iloc[2])
    assert out["pnl_bar"].round(8).tolist() == [0.0, -22.0, -11.0]


def test_time_ordering_normalization_outputs_monotonic_timestamps() -> None:
    raw = pd.DataFrame(
        {
            "when": [
                "2026-01-01 10:10:00",
                "2026-01-01 10:00:00",
                "2026-01-01 10:05:00",
            ],
            "o": [102.0, 100.0, 101.0],
            "h": [103.0, 101.0, 102.0],
            "l": [101.0, 99.0, 100.0],
            "c": [102.5, 100.5, 101.5],
        }
    )

    normalized = normalize_ohlc_dataframe(
        df=raw,
        schema={
            "timestamp": "when",
            "open": "o",
            "high": "h",
            "low": "l",
            "close": "c",
        },
    )

    assert normalized["ts"].is_monotonic_increasing
    assert normalized["ts"].dt.strftime("%H:%M:%S").tolist() == ["10:00:00", "10:05:00", "10:10:00"]

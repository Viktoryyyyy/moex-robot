from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from src.moex_research.runners.usdrubf_s7_2_historical_component_benchmark import (
    HistoricalComponentBenchmarkError,
    _benchmark_observations,
    _filter_prediction_rows,
    _parse_horizons,
    _phase3_manifest_source,
    build_historical_replay_rows,
    build_structure_forward_summary,
)


D0 = pd.Timestamp("2026-08-07")
D1 = pd.Timestamp("2026-08-10")


def _session(day: str, *, variant: str) -> pd.DataFrame:
    if variant == "prior":
        values = [
            (79.8, 80.0, 79.7, 79.9),
            (79.9, 80.2, 79.8, 80.1),
            (80.1, 80.3, 79.6, 79.7),
        ]
    else:
        values = [
            (79.9, 80.0, 79.8, 79.9),
            (79.9, 80.1, 79.9, 80.0),
            (80.0, 80.4, 80.0, 80.35),
        ]
    times = ["10:00", "10:05", "10:10"]
    rows = []
    trade_date = pd.Timestamp(day)
    for clock, (open_, high, low, close) in zip(times, values):
        rows.append(
            {
                "end": pd.Timestamp(f"{day} {clock}"),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 100.0,
                "trade_date": trade_date,
            }
        )
    return pd.DataFrame(rows)


def _phase3_partition(path: Path, day: str, base: float) -> None:
    frame = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp(f"{day} 10:00:00"),
                "instrument_id": "forts.usdrubf",
                "secid": "USDRUBF",
                "open": base,
                "high": base + 0.2,
                "low": base - 0.1,
                "close": base + 0.1,
                "volume": 100.0,
            },
            {
                "ts": pd.Timestamp(f"{day} 18:50:00"),
                "instrument_id": "forts.usdrubf",
                "secid": "USDRUBF",
                "open": base + 0.1,
                "high": base + 0.4,
                "low": base,
                "close": base + 0.3,
                "volume": 200.0,
            },
        ]
    )
    frame.to_parquet(path, index=False)


def _phase3_manifest(tmp_path: Path, *, declared_count: int = 2) -> Path:
    p0 = tmp_path / "part_2026-08-07.parquet"
    p1 = tmp_path / "part_2026-08-10.parquet"
    _phase3_partition(p0, "2026-08-07", 79.7)
    _phase3_partition(p1, "2026-08-10", 80.0)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "panel_id": "usdrubf_phase2_d1_panel.v1",
                "panel_schema_version": "usdrubf_phase2_d1_panel.v1",
                "instrument_id": "forts.usdrubf",
                "secid": "USDRUBF",
                "run_id": "phase3_fixture_exact_history",
                "input_partition_count": declared_count,
                "input_partitions": [str(p0), str(p1)],
            }
        ),
        encoding="utf-8",
    )
    return manifest


def test_replay_uses_previous_complete_session_and_current_bridge_semantics() -> None:
    daily = pd.DataFrame(
        [
            {"end": pd.Timestamp("2026-08-07 18:50"), "close": 79.7},
            {"end": pd.Timestamp("2026-08-10 18:50"), "close": 80.35},
        ]
    )
    intraday = pd.concat(
        [
            _session("2026-08-07", variant="prior"),
            _session("2026-08-10", variant="current"),
        ],
        ignore_index=True,
    )

    replay = build_historical_replay_rows(daily, intraday, horizons=(1,))

    assert len(replay) == 1
    row = replay.iloc[0]
    assert row["trade_date"] == "2026-08-10"
    assert row["prior_trade_date"] == "2026-08-07"
    assert row["price"] == pytest.approx(80.35)
    assert row["ema_direction"] == "NEUTRAL"
    assert row["trend"] == "NEUTRAL"
    assert row["future_price_h1"] is None or pd.isna(row["future_price_h1"])
    parsed = datetime.fromisoformat(str(row["as_of_timestamp"]))
    assert parsed.tzinfo is not None
    assert str(row["structure_signature"]).startswith("HIGH:")
    assert "|LOW:" in str(row["structure_signature"])


def test_phase3_manifest_binds_exact_listed_partitions_without_scan(tmp_path: Path) -> None:
    manifest = _phase3_manifest(tmp_path)

    daily, intraday, provenance = _phase3_manifest_source(manifest)

    assert len(daily) == 2
    assert len(intraday) == 4
    assert provenance["source_mode"] == "phase3_panel_manifest"
    assert provenance["panel_run_id"] == "phase3_fixture_exact_history"
    assert provenance["panel_instrument_id"] == "forts.usdrubf"
    assert provenance["panel_secid"] == "USDRUBF"
    assert provenance["input_partition_count"] == 2
    assert provenance["input_partition_paths_recorded_in_manifest"] is True
    assert provenance["directory_scan_used"] is False
    expected_sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
    assert provenance["panel_manifest_sha256"] == expected_sha


def test_phase3_manifest_rejects_declared_partition_count_mismatch(tmp_path: Path) -> None:
    manifest = _phase3_manifest(tmp_path, declared_count=3)
    with pytest.raises(HistoricalComponentBenchmarkError, match="count mismatch"):
        _phase3_manifest_source(manifest)


def test_phase3_manifest_rejects_wrong_identity_before_replay(tmp_path: Path) -> None:
    manifest = _phase3_manifest(tmp_path)
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["instrument_id"] = "wrong.instrument"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(HistoricalComponentBenchmarkError, match="instrument_id mismatch"):
        _phase3_manifest_source(manifest)


def test_prediction_filter_preserves_labels_created_from_rows_after_end_date() -> None:
    replay = pd.DataFrame(
        [
            {
                "trade_date": "2026-08-07",
                "future_price_h1": 80.0,
            },
            {
                "trade_date": "2026-08-10",
                "future_price_h1": 81.0,
            },
            {
                "trade_date": "2026-08-11",
                "future_price_h1": 82.0,
            },
        ]
    )

    selected = _filter_prediction_rows(
        replay,
        start=pd.Timestamp("2026-08-10"),
        end=pd.Timestamp("2026-08-10"),
    )

    assert selected["trade_date"].tolist() == ["2026-08-10"]
    assert selected.iloc[0]["future_price_h1"] == 81.0


def test_prediction_filter_rejects_empty_window() -> None:
    replay = pd.DataFrame([{"trade_date": "2026-08-07", "future_price_h1": 80.0}])
    with pytest.raises(HistoricalComponentBenchmarkError, match="zero prediction rows"):
        _filter_prediction_rows(
            replay,
            start=pd.Timestamp("2026-08-10"),
            end=None,
        )


def test_ema_bias_only_and_always_active_are_distinct_benchmarks() -> None:
    replay = pd.DataFrame(
        [
            {
                "as_of_timestamp": "2026-08-10T18:50:00+03:00",
                "price": 80.0,
                "trend": "BULLISH_USD",
                "market_regime": "PREVIOUS_SESSION_RANGE_UNCONFIRMED",
                "ema_confidence": 1.0,
                "future_price_h1": 81.0,
            }
        ]
    )

    bias_only = _benchmark_observations(replay, horizons=(1,), always_active=False)
    always_active = _benchmark_observations(replay, horizons=(1,), always_active=True)

    assert bias_only[0].final_bias == "BULLISH_USD"
    assert bias_only[0].trade_state == "WAIT"
    assert bias_only[0].exposure == "OUT"
    assert always_active[0].trade_state == "HOLD"
    assert always_active[0].exposure == "LONG_USD"


def test_structure_summary_reports_outcomes_without_inventing_directional_rule() -> None:
    replay = pd.DataFrame(
        [
            {
                "market_regime": "R1",
                "structure_signature": "HIGH:AWAY|LOW:AWAY",
                "price": 80.0,
                "future_price_h1": 80.8,
            },
            {
                "market_regime": "R1",
                "structure_signature": "HIGH:AWAY|LOW:AWAY",
                "price": 80.0,
                "future_price_h1": 79.2,
            },
        ]
    )

    summary = build_structure_forward_summary(
        replay,
        horizons=(1,),
        neutral_band_bps=0.0,
    )

    regime = summary[
        (summary["grouping_field"] == "market_regime")
        & (summary["group_value"] == "R1")
    ].iloc[0]
    assert regime["count"] == 2
    assert regime["bullish_rate"] == 0.5
    assert regime["bearish_rate"] == 0.5
    assert regime["neutral_rate"] == 0.0


def test_horizon_parser_is_strict_and_deterministic() -> None:
    assert _parse_horizons("10,1,5,3") == (1, 3, 5, 10)
    with pytest.raises(HistoricalComponentBenchmarkError, match="unique"):
        _parse_horizons("1,1")
    with pytest.raises(HistoricalComponentBenchmarkError, match="positive"):
        _parse_horizons("0,1")

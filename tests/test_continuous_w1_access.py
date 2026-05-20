from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

from moex_data.futures import continuous_bars_access as access
from moex_data.futures.continuous_bars_access import load_futures_continuous_bars

FAMILY_CODE = "Si"
ROLL_POLICY_ID = "expiration_minus_1_trading_session_v1"
ADJUSTMENT_POLICY_ID = "unadjusted_v1"


def make_w1_rows(week_start="2026-05-04", week_end="2026-05-10"):
    return pd.DataFrame([
        {
            "week_start": week_start,
            "week_end": week_end,
            "iso_year": int(pd.Timestamp(week_start).isocalendar().year),
            "iso_week": int(pd.Timestamp(week_start).isocalendar().week),
            "continuous_symbol": "Si_CONT",
            "family_code": FAMILY_CODE,
            "source_trade_dates": [week_start],
            "source_contracts": ["SiM6"],
            "open": 1000.0,
            "high": 1010.0,
            "low": 990.0,
            "close": 1005.0,
            "volume": 100,
            "roll_policy_id": ROLL_POLICY_ID,
            "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
            "adjustment_factor": 1.0,
            "has_roll_boundary": False,
            "roll_map_id": "Si.rollmap." + week_start,
            "source_d1_row_count": 1,
            "schema_version": "futures_continuous_w1.v1",
            "ingest_ts": "2026-05-10T20:00:00Z",
        }
    ])


def register_w1(root, store, frame, week_start="2026-05-04"):
    path = root / "futures" / "continuous_w1" / ("roll_policy=" + ROLL_POLICY_ID) / ("adjustment_policy=" + ADJUSTMENT_POLICY_ID) / ("family=" + FAMILY_CODE) / ("week_start=" + week_start) / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    store[str(path)] = frame


def test_valid_w1_route(tmp_path, monkeypatch):
    store = {}

    def fake_read_parquet(path):
        key = str(path)
        if key not in store:
            raise AssertionError("unexpected parquet path:" + key)
        return store[key].copy()

    monkeypatch.setattr(access.pd, "read_parquet", fake_read_parquet)
    register_w1(tmp_path, store, make_w1_rows())

    result = load_futures_continuous_bars(
        data_root=tmp_path,
        family_code=FAMILY_CODE,
        roll_policy_id=ROLL_POLICY_ID,
        adjustment_policy_id=ADJUSTMENT_POLICY_ID,
        timeframe="W1",
        start="2026-05-04",
        end="2026-05-10",
        columns=["week_start", "close", "schema_version"],
    )

    assert result.columns.tolist() == ["week_start", "close", "schema_version"]
    assert len(result) == 1
    assert result["week_start"].tolist() == ["2026-05-04"]
    assert result["schema_version"].unique().tolist() == ["futures_continuous_w1.v1"]


def test_w1_route_allows_sparse_materialized_weeks(tmp_path, monkeypatch):
    store = {}

    def fake_read_parquet(path):
        key = str(path)
        if key not in store:
            raise AssertionError("unexpected parquet path:" + key)
        return store[key].copy()

    monkeypatch.setattr(access.pd, "read_parquet", fake_read_parquet)
    register_w1(tmp_path, store, make_w1_rows("2026-05-04", "2026-05-10"), "2026-05-04")
    register_w1(tmp_path, store, make_w1_rows("2026-05-18", "2026-05-24"), "2026-05-18")

    result = load_futures_continuous_bars(
        data_root=tmp_path,
        family_code=FAMILY_CODE,
        roll_policy_id=ROLL_POLICY_ID,
        adjustment_policy_id=ADJUSTMENT_POLICY_ID,
        timeframe="W1",
        start="2026-05-04",
        end="2026-05-19",
        columns=["week_start", "schema_version"],
    )

    assert result["week_start"].tolist() == ["2026-05-04", "2026-05-18"]
    assert result["schema_version"].unique().tolist() == ["futures_continuous_w1.v1"]

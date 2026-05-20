from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

from moex_data.futures import continuous_w1_builder as builder


def make_d1_rows():
    rows = []
    for trade_date, open_value, high, low, close, volume in [
        ("2026-05-04", 100.0, 105.0, 99.0, 104.0, 10),
        ("2026-05-05", 104.0, 110.0, 103.0, 108.0, 20),
    ]:
        rows.append({
            "trade_date": trade_date,
            "session_date": trade_date,
            "continuous_symbol": "Si_CONT",
            "family_code": "Si",
            "source_contracts": ["SiM6"],
            "open": open_value,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "roll_policy_id": builder.ROLL_POLICY_ID,
            "adjustment_policy_id": builder.ADJUSTMENT_POLICY_ID,
            "adjustment_factor": 1.0,
            "has_roll_boundary": False,
            "roll_map_id": "Si.rollmap.2026-05",
            "schema_version": builder.SCHEMA_D1,
            "ingest_ts": "2026-05-05T20:00:00Z",
        })
    return pd.DataFrame(rows)


def test_aggregate_w1_from_d1_only():
    d1 = builder.normalize_d1(make_d1_rows())
    w1 = builder.aggregate_w1(d1, "2026-05-10T20:00:00Z")
    blockers = builder.validate_w1(w1, d1, "Si", builder.ROLL_POLICY_ID, builder.ADJUSTMENT_POLICY_ID)

    assert blockers == []
    assert len(w1) == 1
    row = w1.iloc[0]
    assert row["week_start"] == "2026-05-04"
    assert row["week_end"] == "2026-05-10"
    assert row["open"] == 100.0
    assert row["high"] == 110.0
    assert row["low"] == 99.0
    assert row["close"] == 108.0
    assert row["volume"] == 30
    assert row["source_trade_dates"] == ["2026-05-04", "2026-05-05"]
    assert row["source_contracts"] == ["SiM6"]
    assert row["source_d1_row_count"] == 2
    assert row["schema_version"] == "futures_continuous_w1.v1"

from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures import stage2_raw_history_acceptance as acceptance


def _expectation() -> acceptance.HistoryExpectation:
    return acceptance.HistoryExpectation(
        target_dataset_id=acceptance.FUTOI_DATASET_ID,
        instrument_id="si_futures_family",
        source_id=acceptance.FUTOI_SOURCE_ID,
        date_start="2026-08-17",
        date_end="2026-08-17",
        expected_partitions=1,
        expected_rows=2,
        expected_secid="SiU6",
        expected_source_ticker="si",
        expected_missing_dates=0,
    )


def _frame() -> pd.DataFrame:
    ts = pd.to_datetime(["2026-08-17 10:00:00", "2026-08-17 10:00:00"])
    return pd.DataFrame(
        {
            "instrument_id": ["si_futures_family", "si_futures_family"],
            "trade_date": ["2026-08-17", "2026-08-17"],
            "ts": ts,
            "moment": ts,
            "systime": pd.to_datetime(["2026-08-17 10:01:00", "2026-08-17 10:01:00"]),
            "sess_id": [1, 1],
            "seqnum": [10, 11],
            "secid": ["SiU6", "SiU6"],
            "board": ["RFUD", "RFUD"],
            "market": ["forts", "forts"],
            "engine": ["futures", "futures"],
            "source_id": [acceptance.FUTOI_SOURCE_ID, acceptance.FUTOI_SOURCE_ID],
            "source_ticker": ["si", "si"],
            "clgroup": ["FIZ", "YUR"],
            "pos": [10, -10],
            "pos_long": [20, 30],
            "pos_short": [-10, -40],
            "pos_long_num": [2.0, 3.0],
            "pos_short_num": [1.0, 4.0],
            "availability_ts_utc": pd.to_datetime(
                ["2026-08-17T07:02:00Z", "2026-08-17T07:02:00Z"]
            ),
            "ingest_ts": pd.to_datetime(
                ["2026-08-17T07:03:00Z", "2026-08-17T07:03:00Z"]
            ),
        }
    )


def test_futoi_participant_counts_must_be_integral() -> None:
    frame = _frame()
    frame.loc[0, "pos_long_num"] = 1.5
    with pytest.raises(
        acceptance.RawHistoryAcceptanceError,
        match="participant counts must be non-negative integers",
    ):
        acceptance._validate_futoi_partition(frame, _expectation(), "2026-08-17")

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from moex_data.futures import stage2_raw_history_acceptance as acceptance


def _quote_expectation() -> acceptance.HistoryExpectation:
    return acceptance.HistoryExpectation(
        target_dataset_id=acceptance.QUOTE_DATASET_ID,
        instrument_id="usdrubf_futures_family",
        source_id=acceptance.QUOTE_SOURCE_ID,
        date_start="2026-08-17",
        date_end="2026-08-17",
        expected_partitions=1,
        expected_rows=1,
        expected_secid="USDRUBF",
    )


def _quote_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "instrument_id": ["usdrubf_futures_family"],
            "trade_date": ["2026-08-17"],
            "ts": pd.to_datetime(["2026-08-17 10:00:00"]),
            "session_date": ["2026-08-17"],
            "secid": ["USDRUBF"],
            "board": ["RFUD"],
            "market": ["FORTS"],
            "engine": ["futures"],
            "source_id": [acceptance.QUOTE_SOURCE_ID],
            "open": [80.0],
            "high": [80.2],
            "low": [79.9],
            "close": [80.1],
            "volume": [10.0],
            "value": [800.0],
            "num_trades": [5],
            "source": ["MOEX_ALGOPACK_FO_TRADESTATS"],
            "ingest_ts": ["2026-08-18T00:00:00+00:00"],
        }
    )


def _futoi_expectation() -> acceptance.HistoryExpectation:
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


def _futoi_frame() -> pd.DataFrame:
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
            "pos_long_num": [2, 3],
            "pos_short_num": [1, 4],
            "availability_ts_utc": pd.to_datetime(
                ["2026-08-17T10:02:00Z", "2026-08-17T10:02:00Z"]
            ),
            "ingest_ts": pd.to_datetime(
                ["2026-08-17T10:03:00Z", "2026-08-17T10:03:00Z"]
            ),
        }
    )


def test_quote_ts_must_match_partition_trade_date() -> None:
    frame = _quote_frame()
    frame["ts"] = pd.to_datetime(["2026-08-16 10:00:00"])
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="ts date mismatch"):
        acceptance._validate_quote_partition(
            Path("."), frame, _quote_expectation(), "2026-08-17", "test_run"
        )


def test_quote_session_date_must_match_partition_trade_date() -> None:
    frame = _quote_frame()
    frame["session_date"] = "2026-08-16"
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="session_date mismatch"):
        acceptance._validate_quote_partition(
            Path("."), frame, _quote_expectation(), "2026-08-17", "test_run"
        )


def test_quote_required_numeric_cannot_be_infinite() -> None:
    frame = _quote_frame()
    frame["volume"] = np.inf
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="non-finite numeric value: volume"):
        acceptance._validate_quote_partition(
            Path("."), frame, _quote_expectation(), "2026-08-17", "test_run"
        )


def test_quote_optional_numeric_may_be_null_but_not_infinite() -> None:
    frame = _quote_frame()
    frame["value"] = np.nan
    acceptance._validate_quote_partition(
        Path("."), frame, _quote_expectation(), "2026-08-17", "test_run"
    )
    frame["value"] = np.inf
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="non-finite value"):
        acceptance._validate_quote_partition(
            Path("."), frame, _quote_expectation(), "2026-08-17", "test_run"
        )


def test_futoi_availability_cannot_precede_publication() -> None:
    frame = _futoi_frame()
    frame["availability_ts_utc"] = pd.to_datetime(
        ["2026-08-17T09:59:00Z", "2026-08-17T09:59:00Z"]
    )
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="precedes source publication"):
        acceptance._validate_futoi_partition(frame, _futoi_expectation(), "2026-08-17")


def test_futoi_ingest_cannot_precede_availability() -> None:
    frame = _futoi_frame()
    frame["ingest_ts"] = pd.to_datetime(
        ["2026-08-17T10:01:30Z", "2026-08-17T10:01:30Z"]
    )
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="precedes availability"):
        acceptance._validate_futoi_partition(frame, _futoi_expectation(), "2026-08-17")


def test_futoi_secid_must_match_registry_expectation() -> None:
    frame = _futoi_frame()
    frame["secid"] = "CRU6"
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="stored identity mismatch: secid"):
        acceptance._validate_futoi_partition(frame, _futoi_expectation(), "2026-08-17")


def test_futoi_net_position_must_equal_long_plus_short() -> None:
    frame = _futoi_frame()
    frame.loc[0, "pos"] = 999
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="net position must equal"):
        acceptance._validate_futoi_partition(frame, _futoi_expectation(), "2026-08-17")


def test_futoi_position_fields_must_be_finite() -> None:
    frame = _futoi_frame()
    frame.loc[0, "pos_long_num"] = np.inf
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="non-finite numeric value: pos_long_num"):
        acceptance._validate_futoi_partition(frame, _futoi_expectation(), "2026-08-17")

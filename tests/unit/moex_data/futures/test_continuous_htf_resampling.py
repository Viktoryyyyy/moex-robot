import pandas as pd
import pytest

from moex_data.futures.continuous_htf_resampling import (
    HTFResamplingError,
    OUTPUT_SCHEMA_VERSION,
    resample_continuous_htf,
)


def make_source_bars():
    ends = pd.date_range("2026-05-04 10:05:00", "2026-05-04 14:00:00", freq="5min")
    rows = []
    for idx, end in enumerate(ends):
        base = 1000.0 + idx
        rows.append(
            {
                "trade_date": "2026-05-04",
                "end": end,
                "session_date": "2026-05-04",
                "continuous_symbol": "Si_CONT",
                "family_code": "Si",
                "source_secid": "SiM6",
                "source_contract": "SiM6",
                "open": base,
                "high": base + 1.0,
                "low": base - 1.0,
                "close": base + 0.5,
                "volume": 10 + idx,
                "roll_policy_id": "expiration_minus_1_session.v1",
                "adjustment_policy_id": "unadjusted.v1",
                "adjustment_factor": 1.0,
                "is_roll_boundary": False,
                "roll_map_id": "Si.rollmap.2026-05-04",
                "schema_version": "futures_continuous_5m.v1",
                "ingest_ts": "2026-05-04 23:00:00",
            }
        )
    return pd.DataFrame(rows)


def make_calendar():
    return pd.DataFrame(
        [
            {
                "calendar_id": "moex_futures_session_calendar.v1",
                "calendar_date": "2026-05-04",
                "session_date": "2026-05-04",
                "session_seq": 1,
                "is_trading_day": True,
                "is_planned_non_trading_day": False,
                "session_interval_id": "2026-05-04-main",
                "session_start": pd.Timestamp("2026-05-04 10:00:00"),
                "session_end": pd.Timestamp("2026-05-04 14:00:00"),
                "exchange_timezone": "Europe/Moscow",
                "expected_bar_granularity": "5m",
                "expected_first_bar_end": pd.Timestamp("2026-05-04 10:05:00"),
                "expected_last_bar_end": pd.Timestamp("2026-05-04 14:00:00"),
                "expected_bar_count": 48,
                "source_name": "MOEX ISS futures calendar",
                "source_endpoint_or_provider": "/iss/calendars/futures.json",
                "source_retrieved_at": "2026-05-04 23:00:00",
                "source_range_start": "2026-05-04",
                "source_range_end": "2026-05-04",
                "calendar_status": "canonical",
                "schema_version": "moex_futures_session_calendar.v1",
            }
        ]
    )


@pytest.mark.parametrize(
    ("timeframe", "expected_rows", "expected_first_count"),
    [
        ("15m", 16, 3),
        ("30m", 8, 6),
        ("1h", 4, 12),
        ("4h", 1, 48),
    ],
)
def test_successful_resampling_for_supported_timeframes(timeframe, expected_rows, expected_first_count):
    source = make_source_bars()
    result = resample_continuous_htf(source, make_calendar(), timeframe)

    assert len(result) == expected_rows
    assert result["schema_version"].unique().tolist() == [OUTPUT_SCHEMA_VERSION]
    assert result["timeframe"].unique().tolist() == [timeframe]
    assert result.iloc[0]["source_bar_count"] == expected_first_count
    assert result.iloc[0]["first_source_end"] == pd.Timestamp("2026-05-04 10:05:00")
    assert result.iloc[0]["open"] == source.iloc[0]["open"]
    assert result.iloc[0]["high"] == source.iloc[:expected_first_count]["high"].max()
    assert result.iloc[0]["low"] == source.iloc[:expected_first_count]["low"].min()
    assert result.iloc[0]["close"] == source.iloc[expected_first_count - 1]["close"]
    assert result.iloc[0]["volume"] == source.iloc[:expected_first_count]["volume"].sum()


def test_unsupported_timeframe_fails_closed():
    with pytest.raises(HTFResamplingError, match="unsupported_timeframe"):
        resample_continuous_htf(make_source_bars(), make_calendar(), "2h")


def test_missing_source_column_fails_closed():
    source = make_source_bars().drop(columns=["source_contract"])

    with pytest.raises(HTFResamplingError, match="missing_source_columns:source_contract"):
        resample_continuous_htf(source, make_calendar(), "15m")


def test_duplicate_timestamps_fail_closed():
    source = pd.concat([make_source_bars(), make_source_bars().iloc[[0]]], ignore_index=True)

    with pytest.raises(HTFResamplingError, match="duplicate_source_timestamps"):
        resample_continuous_htf(source, make_calendar(), "15m")


def test_non_monotonic_timestamps_fail_closed():
    source = make_source_bars()
    source = pd.concat([source.iloc[[1]], source.iloc[[0]], source.iloc[2:]], ignore_index=True)

    with pytest.raises(HTFResamplingError, match="non_monotonic_source_timestamps"):
        resample_continuous_htf(source, make_calendar(), "15m")


def test_missing_expected_bar_gap_fails_closed():
    source = make_source_bars().drop(index=[0]).reset_index(drop=True)

    with pytest.raises(HTFResamplingError, match="missing_expected_5m_source_bar_inside_bucket"):
        resample_continuous_htf(source, make_calendar(), "15m")


def test_fully_missing_expected_bucket_fails_closed():
    source = make_source_bars()
    missing_bucket_ends = pd.to_datetime(
        ["2026-05-04 10:20:00", "2026-05-04 10:25:00", "2026-05-04 10:30:00"]
    )
    source = source.loc[~source["end"].isin(missing_bucket_ends)].reset_index(drop=True)

    with pytest.raises(HTFResamplingError, match="missing_expected_5m_source_bar_inside_bucket"):
        resample_continuous_htf(source, make_calendar(), "15m")


def test_cross_session_boundary_fails_closed():
    source = make_source_bars()
    source.loc[1, "session_date"] = "2026-05-05"

    with pytest.raises(HTFResamplingError, match="source_bars_outside_calendar_intervals"):
        resample_continuous_htf(source, make_calendar(), "15m")


def test_mixed_source_secid_inside_bucket_fails_closed():
    source = make_source_bars()
    source.loc[1, "source_secid"] = "SiU6"

    with pytest.raises(HTFResamplingError, match="mixed_source_secid_inside_bucket"):
        resample_continuous_htf(source, make_calendar(), "15m")


def test_mixed_roll_metadata_inside_bucket_fails_closed():
    source = make_source_bars()
    source.loc[1, "roll_map_id"] = "Si.rollmap.changed"

    with pytest.raises(HTFResamplingError, match="mixed_roll_map_id_inside_bucket"):
        resample_continuous_htf(source, make_calendar(), "15m")

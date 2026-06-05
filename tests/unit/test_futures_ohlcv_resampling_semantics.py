from __future__ import annotations

from datetime import date, datetime

import pytest

from moex_core.calendars.futures_session import FuturesSessionError
from moex_core.calendars.moex_iss_calendar import build_futures_calendar_from_rows
from moex_data.futures.resampling import resample_ohlcv_5m_partition
from moex_data.futures.validation import FuturesValidationError

CALENDAR_REF = "contracts/datasets/futures_calendar_session.v1.yaml"


def _calendar():
    return build_futures_calendar_from_rows(
        (
            {"trade_date": "2026-06-01", "is_trading_day": False, "reason": "holiday"},
            {"trade_date": "2026-06-02", "is_trading_day": True, "reason": "normal"},
            {"trade_date": "2026-06-03", "is_trading_day": True, "reason": "normal"},
            {"trade_date": "2026-06-04", "is_trading_day": True, "reason": "normal"},
            {"trade_date": "2026-06-05", "is_trading_day": True, "reason": "normal"},
        ),
        calendar_contract_ref=CALENDAR_REF,
    )


def _config(*, approved_timeframes=("5m", "10m", "15m", "30m", "1h", "4h", "1D", "1W")):
    return {
        "config_id": "futures_historical_data_core.v1",
        "approved_timeframes": approved_timeframes,
        "continuous_series_status": "blocked_placeholder",
    }


def _identity(**overrides):
    values = {
        "FAMILY": "RI",
        "SECID": "RIM6",
        "BOARD": "RFUD",
        "MARKET": "FORTS",
        "SERIES_TYPE": "native",
    }
    values.update(overrides)
    return values


def _request(timeframe, **overrides):
    values = {
        "dataset_id": "futures_ohlcv_derived_timeframe",
        "contract_id": "futures_ohlcv_derived_timeframe.v1",
        "timeframe": timeframe,
        "partition_key": "2026-06-02",
        "storage_ref": "${MOEX_DATA_ROOT}/futures/ohlcv_derived/timeframe={TIMEFRAME}/family={FAMILY}/secid={SECID}/board={BOARD}/market={MARKET}/series_type={SERIES_TYPE}/part.parquet",
        "parent_manifest_ref": "${MOEX_DATA_ROOT}/futures/manifests/ohlcv_5m/family={FAMILY}/secid={SECID}/part.json",
        "calendar_contract_ref": CALENDAR_REF,
        "manifest_ref": "${MOEX_DATA_ROOT}/futures/manifests/ohlcv_derived/timeframe={TIMEFRAME}/family={FAMILY}/secid={SECID}/part.json",
        "quality_report_ref": "${MOEX_DATA_ROOT}/futures/quality/ohlcv_derived/timeframe={TIMEFRAME}/family={FAMILY}/secid={SECID}/part.json",
        **_identity(),
    }
    values.update(overrides)
    return values


def _parent_manifest(**overrides):
    values = {
        "dataset_id": "futures_ohlcv_5m",
        "timeframe": "5m",
        "partition_key": "2026-06-02",
        "storage_ref": "${MOEX_DATA_ROOT}/futures/ohlcv_5m/family={FAMILY}/secid={SECID}/board={BOARD}/market={MARKET}/series_type={SERIES_TYPE}/part.parquet",
        "row_count": 2,
        "expected_bar_count": 2,
        "observed_bar_count": 2,
        "missing_bar_count": 0,
        "calendar_contract_ref": CALENDAR_REF,
        "quality_report_ref": "${MOEX_DATA_ROOT}/futures/quality/ohlcv_5m/family={FAMILY}/secid={SECID}/part.json",
        **_identity(),
    }
    values.update(overrides)
    return values


def _row(ts, *, open_, high, low, close, volume=1, value=10, trades=1, open_interest=None, **overrides):
    ts_value = datetime.fromisoformat(ts)
    values = {
        "ts": ts_value,
        "trade_date": ts_value.date(),
        "session_date": ts_value.date(),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "value": value,
        "trades": trades,
        **_identity(),
    }
    if open_interest is not None:
        values["open_interest"] = open_interest
    values.update(overrides)
    return values


def _two_rows():
    return (
        _row(
            "2026-06-02T10:00:00",
            open_=100,
            high=102,
            low=99,
            close=101,
            volume=3,
            value=30,
            trades=1,
            open_interest=50,
        ),
        _row(
            "2026-06-02T10:05:00",
            open_=101,
            high=105,
            low=100,
            close=104,
            volume=4,
            value=40,
            trades=2,
            open_interest=55,
        ),
    )


def _run(timeframe, rows=None, request=None, manifest=None, config=None):
    rows = _two_rows() if rows is None else rows
    return resample_ohlcv_5m_partition(
        rows,
        _request(timeframe) if request is None else request,
        parent_manifest_values=_parent_manifest(
            row_count=len(rows), expected_bar_count=len(rows), observed_bar_count=len(rows)
        )
        if manifest is None
        else manifest,
        core_config_values=_config() if config is None else config,
        calendar=_calendar(),
    )


@pytest.mark.parametrize("timeframe", ("10m", "15m", "30m", "1h", "4h"))
def test_deterministic_resampling_supports_intraday_timeframes(timeframe):
    result = _run(timeframe)

    assert result.request.timeframe == timeframe
    assert len(result.rows) == 1
    assert result.manifest.timeframe == timeframe
    assert result.quality_report.parent_manifest_ref == _request(timeframe)["parent_manifest_ref"]


def test_deterministic_resampling_supports_1d():
    rows = (
        _row("2026-06-02T10:00:00", open_=100, high=101, low=99, close=100, volume=1),
        _row("2026-06-02T18:45:00", open_=100, high=106, low=98, close=105, volume=2),
    )
    result = _run("1D", rows=rows)

    assert len(result.rows) == 1
    assert result.rows[0]["timeframe"] == "1D"
    assert result.rows[0]["session_date"] == date(2026, 6, 2)


def test_deterministic_resampling_supports_1w_with_trading_week_anchor_and_no_non_trading_bars():
    rows = (
        _row("2026-06-02T10:00:00", open_=100, high=102, low=99, close=101, volume=1),
        _row("2026-06-04T10:00:00", open_=101, high=106, low=100, close=105, volume=2),
    )
    manifest = (
        _parent_manifest(partition_key="2026-06-02", row_count=1, expected_bar_count=1, observed_bar_count=1),
        _parent_manifest(partition_key="2026-06-04", row_count=1, expected_bar_count=1, observed_bar_count=1),
    )
    result = _run("1W", rows=rows, manifest=manifest)

    assert len(result.rows) == 1
    assert result.rows[0]["week_start_date"] == date(2026, 6, 1)
    assert result.rows[0]["session_date"] == date(2026, 6, 4)
    assert result.rows[0]["session_date"] != date(2026, 6, 1)


def test_ohlcv_aggregation_semantics():
    row = _run("10m").rows[0]

    assert row["open"] == 100
    assert row["high"] == 105
    assert row["low"] == 99
    assert row["close"] == 104
    assert row["volume"] == 7
    assert row["value"] == 70
    assert row["trades"] == 3
    assert row["open_interest"] == 55


def test_unsupported_timeframe_is_rejected():
    with pytest.raises(FuturesValidationError):
        _run("2m", request=_request("2m"))


def test_absent_10m_in_approved_config_is_rejected():
    with pytest.raises(FuturesValidationError):
        _run("15m", config=_config(approved_timeframes=("5m", "15m", "30m", "1h", "4h", "1D", "1W")))


def test_derived_timeframe_without_parent_5m_manifest_is_rejected():
    with pytest.raises(FuturesValidationError):
        resample_ohlcv_5m_partition(
            _two_rows(),
            _request("10m"),
            parent_manifest_values=(),
            core_config_values=_config(),
            calendar=_calendar(),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("FAMILY", "SI"),
        ("SECID", "SIM6"),
        ("BOARD", "TQBR"),
        ("MARKET", "EQ"),
        ("SERIES_TYPE", "continuous"),
    ),
)
def test_parent_rows_with_mixed_identity_are_rejected(field, value):
    rows = (_two_rows()[0], _row("2026-06-02T10:05:00", open_=101, high=105, low=100, close=104, **{field: value}))

    with pytest.raises(FuturesValidationError):
        _run("10m", rows=rows)


def test_continuous_series_is_rejected():
    with pytest.raises(FuturesValidationError):
        _run("10m", request=_request("10m", SERIES_TYPE="continuous"))


def test_cross_session_aggregation_is_rejected():
    rows = (
        _row("2026-06-02T10:00:00", open_=100, high=101, low=99, close=100),
        _row("2026-06-03T10:05:00", open_=100, high=102, low=98, close=101),
    )

    with pytest.raises((FuturesValidationError, FuturesSessionError)):
        _run("10m", rows=rows)


def test_non_monotonic_parent_timestamps_are_rejected():
    rows = (
        _row("2026-06-02T10:05:00", open_=101, high=105, low=100, close=104),
        _row("2026-06-02T10:00:00", open_=100, high=102, low=99, close=101),
    )

    with pytest.raises(FuturesValidationError):
        _run("10m", rows=rows)


def test_duplicate_parent_ts_secid_is_rejected():
    rows = (
        _row("2026-06-02T10:00:00", open_=100, high=102, low=99, close=101),
        _row("2026-06-02T10:00:00", open_=101, high=105, low=100, close=104),
    )

    with pytest.raises(FuturesValidationError):
        _run("10m", rows=rows)


def test_invalid_ohlc_parent_rows_are_rejected():
    rows = (
        _row("2026-06-02T10:00:00", open_=100, high=99, low=101, close=100),
        _row("2026-06-02T10:05:00", open_=101, high=105, low=100, close=104),
    )

    with pytest.raises(FuturesValidationError):
        _run("10m", rows=rows)


@pytest.mark.parametrize("field", ("manifest_ref", "quality_report_ref"))
def test_missing_derived_manifest_quality_refs_are_rejected(field):
    request = _request("10m")
    request.pop(field)

    with pytest.raises(FuturesValidationError):
        _run("10m", request=request)

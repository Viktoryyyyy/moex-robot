from datetime import date, datetime

import pytest

from moex_core.calendars.moex_iss_calendar import build_futures_calendar_from_rows
from moex_data.futures.raw_ohlcv_5m import (
    validate_raw_5m_materialization_request_values,
    validate_raw_5m_partition_rows,
)
from moex_data.futures.validation import FuturesValidationError


def _token(*codes: int) -> str:
    return "".join(chr(code) for code in codes)


def _universe() -> dict[str, object]:
    return {
        "universe_id": "futures_universe.v1",
        "dynamic_scan_allowed": False,
        "instruments": [
            {"FAMILY": "TEST_FAMILY_A", "SECID": "TEST_A_1", "BOARD": "RFUD", "MARKET": "FORTS", "SERIES_TYPE": "native"},
        ],
    }


def _request(**overrides: object):
    values = {
        "dataset_id": "futures_ohlcv_5m",
        "contract_id": "futures_ohlcv_5m.v1",
        "timeframe": "5m",
        "FAMILY": "TEST_FAMILY_A",
        "SECID": "TEST_A_1",
        "BOARD": "RFUD",
        "MARKET": "FORTS",
        "SERIES_TYPE": "native",
        "partition_key": "2026-06-02",
        "storage_ref": "${MOEX_DATA_ROOT}/futures/ohlcv_5m/family={FAMILY}/secid={SECID}/part.parquet",
        "calendar_contract_ref": "contracts/datasets/futures_calendar_session.v1.yaml",
        "manifest_ref": "${MOEX_DATA_ROOT}/futures/manifests/run_id={RUN_ID}/manifest.json",
        "quality_report_ref": "${MOEX_DATA_ROOT}/futures/quality/run_id={RUN_ID}/quality_report.json",
        "source_contract_ref": "contracts/datasets/futures_source_contracts.v1.yaml",
    }
    values.update(overrides)
    return validate_raw_5m_materialization_request_values(values, universe_values=_universe())


def _calendar():
    return build_futures_calendar_from_rows(
        [
            {"trade_date": "2026-06-02", "is_trading_day": True, "reason": "N"},
            {"trade_date": "2026-06-03", "is_trading_day": False, "reason": "H"},
        ],
        calendar_contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
    )


def _rows(**overrides: object) -> list[dict[str, object]]:
    base = []
    for minute, close in ((0, 100.5), (5, 101.0)):
        row = {
            "ts": datetime(2026, 6, 2, 10, minute),
            "trade_date": date(2026, 6, 2),
            "session_date": date(2026, 6, 2),
            "FAMILY": "TEST_FAMILY_A",
            "SECID": "TEST_A_1",
            "BOARD": "RFUD",
            "MARKET": "FORTS",
            "SERIES_TYPE": "native",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": close,
            "volume": 10,
            "value": 1000.0,
            "trades": 2,
        }
        row.update(overrides)
        base.append(row)
    return base


def test_raw_5m_partition_rejects_absolute_server_path():
    with pytest.raises(FuturesValidationError, match="absolute"):
        _request(storage_ref="/home/trader/moex_bot/data/futures/ohlcv_5m/part.parquet")


@pytest.mark.parametrize(
    "marker",
    [
        _token(108, 97, 116, 101, 115, 116),
        _token(99, 117, 114, 114, 101, 110, 116),
        _token(97, 117, 116, 111, 100, 101, 116, 101, 99, 116),
        "*",
    ],
)
def test_raw_5m_partition_rejects_latest_current_autodetect_glob_markers(marker):
    with pytest.raises(FuturesValidationError):
        _request(storage_ref="${MOEX_DATA_ROOT}/futures/" + marker + "/part.parquet")


def test_raw_5m_partition_rejects_non_trading_day_partition():
    request = _request(partition_key="2026-06-03")
    rows = _rows(trade_date=date(2026, 6, 3), session_date=date(2026, 6, 3), ts=datetime(2026, 6, 3, 10, 0))
    with pytest.raises(Exception, match="trading day"):
        validate_raw_5m_partition_rows(rows, request=request, calendar=_calendar())


@pytest.mark.parametrize("field_name", ["trade_date", "session_date"])
def test_raw_5m_partition_rejects_unresolved_session_date_trade_date(field_name):
    rows = _rows()
    rows[0].pop(field_name)
    with pytest.raises(Exception):
        validate_raw_5m_partition_rows(rows, request=_request(), calendar=_calendar())


def test_raw_5m_partition_rejects_duplicate_ts_secid():
    rows = _rows()
    rows[1]["ts"] = rows[0]["ts"]
    with pytest.raises(FuturesValidationError, match="duplicate"):
        validate_raw_5m_partition_rows(rows, request=_request(), calendar=_calendar())


def test_raw_5m_partition_rejects_non_monotonic_timestamps():
    rows = _rows()
    rows[1]["ts"] = datetime(2026, 6, 2, 9, 55)
    with pytest.raises(FuturesValidationError, match="non-monotonic"):
        validate_raw_5m_partition_rows(rows, request=_request(), calendar=_calendar())


def test_raw_5m_partition_rejects_invalid_ohlc():
    with pytest.raises(FuturesValidationError, match="invalid OHLC"):
        validate_raw_5m_partition_rows(_rows(high=98.0), request=_request(), calendar=_calendar())


@pytest.mark.parametrize("field_name", ["volume", "value", "trades"])
def test_raw_5m_partition_rejects_negative_volume_value_trades(field_name):
    with pytest.raises(FuturesValidationError, match="non-negative"):
        validate_raw_5m_partition_rows(_rows(**{field_name: -1}), request=_request(), calendar=_calendar())

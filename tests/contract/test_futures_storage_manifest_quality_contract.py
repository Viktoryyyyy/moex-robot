from __future__ import annotations

from datetime import datetime

import pytest

from moex_core.calendars.moex_iss_calendar import MoexIssCalendarError, build_futures_calendar_from_rows
from moex_data.futures.manifests import (
    REQUIRED_MANIFEST_FIELDS,
    validate_futures_partition_manifest_values,
    validate_storage_ref,
)
from moex_data.futures.validation import FuturesValidationError
from moex_data.quality.futures_ohlcv import validate_futures_quality_report_values, validate_ohlcv_rows


def _calendar():
    return build_futures_calendar_from_rows(
        (
            {"trade_date": "2026-06-01", "is_trading_day": False, "reason": "holiday"},
            {"trade_date": "2026-06-02", "is_trading_day": True, "reason": "normal"},
        ),
        calendar_contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
    )


def _manifest(**overrides):
    values = {
        "dataset_id": "futures_ohlcv_5m",
        "timeframe": "5m",
        "SERIES_TYPE": "native",
        "FAMILY": "TEST_FAMILY_A",
        "SECID": "TEST_FAMILY_A_1",
        "BOARD": "RFUD",
        "MARKET": "FORTS",
        "partition_key": "2026-06-02",
        "storage_ref": "${MOEX_DATA_ROOT}/futures/ohlcv_5m/family=TEST_FAMILY_A/secid=TEST_FAMILY_A_1/board=RFUD/market=FORTS/series_type=native/trade_date=2026-06-02/part.parquet",
        "row_count": 3,
        "expected_bar_count": 4,
        "observed_bar_count": 3,
        "missing_bar_count": 1,
        "calendar_contract_ref": "contracts/datasets/futures_calendar_session.v1.yaml",
        "quality_report_ref": "${MOEX_DATA_ROOT}/futures/quality/family=TEST_FAMILY_A/secid=TEST_FAMILY_A_1/trade_date=2026-06-02/report.json",
    }
    values.update(overrides)
    return values


def _quality(**overrides):
    values = {
        "dataset_id": "futures_ohlcv_5m",
        "timeframe": "5m",
        "SERIES_TYPE": "native",
        "FAMILY": "TEST_FAMILY_A",
        "SECID": "TEST_FAMILY_A_1",
        "BOARD": "RFUD",
        "MARKET": "FORTS",
        "partition_key": "2026-06-02",
        "status": "pass",
        "downstream_consumption_allowed": True,
        "row_count": 3,
        "duplicate_ts_secid_count": 0,
        "non_monotonic_timestamp_count": 0,
        "invalid_ohlc_count": 0,
        "negative_volume_count": 0,
        "negative_value_count": 0,
        "negative_trades_count": 0,
        "parent_manifest_ref": "${MOEX_DATA_ROOT}/futures/manifests/manifest.json",
    }
    values.update(overrides)
    return values


def _rows():
    return (
        {
            "ts": datetime(2026, 6, 2, 10, 0),
            "SECID": "TEST_FAMILY_A_1",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 10,
            "value": 1000.0,
            "trades": 2,
        },
        {
            "ts": datetime(2026, 6, 2, 10, 5),
            "SECID": "TEST_FAMILY_A_1",
            "open": 100.5,
            "high": 102.0,
            "low": 100.0,
            "close": 101.5,
            "volume": 11,
            "value": 1100.0,
            "trades": 3,
        },
    )


def test_storage_ref_must_stay_under_moex_data_root_external_pattern():
    storage_ref = validate_storage_ref(_manifest()["storage_ref"])

    assert storage_ref.startswith("${MOEX_DATA_ROOT}/")


@pytest.mark.parametrize(
    "storage_ref",
    (
        "/home/trader/moex_bot/data/futures/ohlcv_5m/part.parquet",
        "${MOEX_DATA_ROOT}/futures/latest/part.parquet",
        "${MOEX_DATA_ROOT}/futures/current/part.parquet",
        "${MOEX_DATA_ROOT}/futures/autodetect/part.parquet",
        "${MOEX_DATA_ROOT}/futures/*/part.parquet",
    ),
)
def test_storage_ref_rejects_absolute_server_paths_and_forbidden_markers(storage_ref):
    with pytest.raises(FuturesValidationError):
        validate_storage_ref(storage_ref)


@pytest.mark.parametrize("field", REQUIRED_MANIFEST_FIELDS)
def test_manifest_requires_mandatory_fields(field):
    values = _manifest()
    values.pop(field)

    with pytest.raises(FuturesValidationError):
        validate_futures_partition_manifest_values(values)


def test_manifest_contract_accepts_required_shape():
    manifest = validate_futures_partition_manifest_values(_manifest(), calendar=_calendar())

    assert manifest.dataset_id == "futures_ohlcv_5m"
    assert manifest.timeframe == "5m"
    assert manifest.series_type == "native"
    assert manifest.row_count == 3


def test_non_trading_day_partition_is_rejected():
    with pytest.raises(MoexIssCalendarError):
        validate_futures_partition_manifest_values(_manifest(partition_key="2026-06-01"), calendar=_calendar())


@pytest.mark.parametrize("status", ("pass", "warn", "fail"))
def test_quality_report_supports_pass_warn_fail(status):
    report = validate_futures_quality_report_values(
        _quality(status=status, downstream_consumption_allowed=(status != "fail"))
    )

    assert report.status == status


def test_quality_report_fail_blocks_downstream_consumption():
    with pytest.raises(FuturesValidationError):
        validate_futures_quality_report_values(_quality(status="fail", downstream_consumption_allowed=True))


def test_quality_report_rejects_duplicate_ts_secid():
    rows = (_rows()[0], _rows()[0])

    with pytest.raises(FuturesValidationError):
        validate_ohlcv_rows(rows, timeframe="5m")


def test_quality_report_rejects_non_monotonic_timestamps():
    rows = (_rows()[1], _rows()[0])

    with pytest.raises(FuturesValidationError):
        validate_ohlcv_rows(rows, timeframe="5m")


def test_quality_report_rejects_invalid_ohlc():
    row = dict(_rows()[0])
    row["high"] = 98.0

    with pytest.raises(FuturesValidationError):
        validate_ohlcv_rows((row,), timeframe="5m")


@pytest.mark.parametrize("field", ("volume", "value", "trades"))
def test_quality_report_rejects_negative_volume_value_trades(field):
    row = dict(_rows()[0])
    row[field] = -1

    with pytest.raises(FuturesValidationError):
        validate_ohlcv_rows((row,), timeframe="5m")


def test_quality_report_rejects_unsupported_timeframe():
    with pytest.raises(FuturesValidationError):
        validate_futures_quality_report_values(_quality(timeframe="2m"))


def test_quality_report_rejects_parent_manifest_inconsistency():
    manifest = validate_futures_partition_manifest_values(_manifest())

    with pytest.raises(FuturesValidationError):
        validate_futures_quality_report_values(_quality(row_count=2), parent_manifest=manifest)

from datetime import date, datetime

import pytest

from moex_core.calendars.moex_iss_calendar import build_futures_calendar_from_rows
from moex_data.futures.materialization import materialize_raw_5m_boundary
from moex_data.futures.raw_ohlcv_5m import validate_raw_5m_materialization_request_values
from moex_data.futures.validation import FuturesValidationError


def _token(*codes: int) -> str:
    return "".join(chr(code) for code in codes)


def _universe() -> dict[str, object]:
    return {
        "universe_id": "futures_universe.v1",
        "dynamic_scan_allowed": False,
        "instruments": [
            {"FAMILY": "TEST_FAMILY_A", "SECID": "TEST_A_1", "BOARD": "RFUD", "MARKET": "FORTS", "SERIES_TYPE": "native"},
            {"FAMILY": "TEST_FAMILY_B", "SECID": "TEST_B_1", "BOARD": "RFUD", "MARKET": "FORTS", "SERIES_TYPE": "native"},
            {"FAMILY": "TEST_CONT", "SECID": "TEST_CONT_1", "BOARD": "RFUD", "MARKET": "FORTS", "SERIES_TYPE": "continuous"},
        ],
    }


def _request(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
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
    return values


def _source_contract() -> dict[str, object]:
    return {
        "source_id": "moex_iss_forts_candles_5m",
        "source_system": "MOEX_ISS",
        "market": "FORTS",
        "board": "RFUD",
        "native_timeframe": "5m",
        "output_contract_ref": "contracts/datasets/futures_ohlcv_5m.v1.yaml",
    }


def _calendar():
    return build_futures_calendar_from_rows(
        [{"trade_date": "2026-06-02", "is_trading_day": True, "reason": "N"}],
        calendar_contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
    )


def _rows(family: str = "TEST_FAMILY_A", secid: str = "TEST_A_1") -> list[dict[str, object]]:
    return [
        {
            "ts": datetime(2026, 6, 2, 10, 0),
            "trade_date": date(2026, 6, 2),
            "session_date": date(2026, 6, 2),
            "FAMILY": family,
            "SECID": secid,
            "BOARD": "RFUD",
            "MARKET": "FORTS",
            "SERIES_TYPE": "native",
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
            "trade_date": date(2026, 6, 2),
            "session_date": date(2026, 6, 2),
            "FAMILY": family,
            "SECID": secid,
            "BOARD": "RFUD",
            "MARKET": "FORTS",
            "SERIES_TYPE": "native",
            "open": 100.5,
            "high": 102.0,
            "low": 100.0,
            "close": 101.0,
            "volume": 11,
            "value": 1100.0,
            "trades": 3,
        },
    ]


class _Adapter:
    def __init__(self, rows):
        self._rows = rows

    def read_rows(self, request):
        return self._rows


@pytest.mark.parametrize("field_name", ["FAMILY", "SECID", "BOARD", "MARKET", "SERIES_TYPE"])
def test_raw_5m_materialization_rejects_missing_identity_field(field_name):
    values = _request()
    values.pop(field_name)
    with pytest.raises(FuturesValidationError):
        validate_raw_5m_materialization_request_values(values, universe_values=_universe())


def test_raw_5m_materialization_rejects_instrument_not_in_universe():
    with pytest.raises(FuturesValidationError, match="not declared"):
        validate_raw_5m_materialization_request_values(
            _request(FAMILY="UNKNOWN", SECID="UNKNOWN_1"), universe_values=_universe()
        )


def test_raw_5m_materialization_rejects_unsupported_timeframe_for_raw_boundary():
    with pytest.raises(FuturesValidationError, match="only 5m"):
        validate_raw_5m_materialization_request_values(_request(timeframe="10m"), universe_values=_universe())


def test_raw_5m_materialization_rejects_continuous_series():
    with pytest.raises(FuturesValidationError, match="continuous"):
        validate_raw_5m_materialization_request_values(
            _request(FAMILY="TEST_CONT", SECID="TEST_CONT_1", SERIES_TYPE="continuous"),
            universe_values=_universe(),
        )


@pytest.mark.parametrize("field_name", ["manifest_ref", "quality_report_ref"])
def test_raw_5m_materialization_requires_manifest_and_quality_report_refs(field_name):
    values = _request()
    values.pop(field_name)
    with pytest.raises(FuturesValidationError):
        validate_raw_5m_materialization_request_values(values, universe_values=_universe())


def test_raw_5m_materialization_remains_generic_across_two_fixture_instruments():
    for family, secid in (("TEST_FAMILY_A", "TEST_A_1"), ("TEST_FAMILY_B", "TEST_B_1")):
        result = materialize_raw_5m_boundary(
            _request(FAMILY=family, SECID=secid),
            universe_values=_universe(),
            source_contract_values=_source_contract(),
            calendar=_calendar(),
            source_adapter=_Adapter(_rows(family, secid)),
        )
        assert result.request.identity.family == family
        assert result.partition_validation.row_count == 2

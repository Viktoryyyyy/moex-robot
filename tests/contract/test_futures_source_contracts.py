import pytest

from moex_data.iss.source_contracts import validate_futures_source_contract_set, validate_futures_source_contract_values
from moex_data.futures.validation import FuturesValidationError


def _source(**overrides):
    values = {
        "source_id": "moex_iss_forts_candles_5m",
        "source_system": "MOEX_ISS",
        "market": "FORTS",
        "board": "RFUD",
        "native_timeframe": "5m",
        "output_contract_ref": "contracts/datasets/futures_ohlcv_5m.v1.yaml",
    }
    values.update(overrides)
    return values


def test_source_contract_accepts_explicit_moex_iss_source():
    source = validate_futures_source_contract_values(_source())

    assert source.source_id == "moex_iss_forts_candles_5m"
    assert source.native_timeframe == "5m"
    assert source.output_contract_ref == "contracts/datasets/futures_ohlcv_5m.v1.yaml"


def test_source_contract_set_rejects_duplicate_source_ids():
    with pytest.raises(FuturesValidationError):
        validate_futures_source_contract_set({"sources": (_source(), _source())})


def test_source_contract_rejects_unsupported_timeframe():
    with pytest.raises(FuturesValidationError):
        validate_futures_source_contract_values(_source(native_timeframe="2m"))


def test_source_contract_rejects_absolute_output_ref():
    with pytest.raises(FuturesValidationError):
        validate_futures_source_contract_values(_source(output_contract_ref="/tmp/futures.yaml"))

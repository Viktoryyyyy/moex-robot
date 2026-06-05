import pytest

from moex_data.futures.instrument_registry import validate_futures_instrument_registry_values
from moex_data.futures.validation import FuturesValidationError, REQUIRED_IDENTIFIER_FIELDS


def _identity(**overrides):
    values = {
        "FAMILY": "TEST_FAMILY_A",
        "SECID": "TEST_CONTRACT_A",
        "BOARD": "RFUD",
        "MARKET": "FORTS",
        "SERIES_TYPE": "native",
    }
    values.update(overrides)
    return values


def _registry(*items):
    return {"registry_id": "futures_instrument_registry.v1", "instruments": tuple(items or (_identity(),))}


def test_registry_accepts_universal_identifier_fields():
    registry = validate_futures_instrument_registry_values(_registry())

    assert registry.registry_id == "futures_instrument_registry.v1"
    assert len(registry.instruments) == 1
    assert tuple(REQUIRED_IDENTIFIER_FIELDS) == ("FAMILY", "SECID", "BOARD", "MARKET", "SERIES_TYPE")
    assert registry.instruments[0].family == "TEST_FAMILY_A"
    assert registry.instruments[0].series_type == "native"


@pytest.mark.parametrize("field_name", ("FAMILY", "SECID", "BOARD", "MARKET", "SERIES_TYPE"))
def test_registry_rejects_missing_required_identifier(field_name):
    values = _identity()
    values.pop(field_name)

    with pytest.raises(FuturesValidationError):
        validate_futures_instrument_registry_values(_registry(values))


def test_registry_rejects_duplicate_identity():
    values = _identity()

    with pytest.raises(FuturesValidationError):
        validate_futures_instrument_registry_values(_registry(values, values))


def test_registry_rejects_unsupported_series_type():
    with pytest.raises(FuturesValidationError):
        validate_futures_instrument_registry_values(_registry(_identity(SERIES_TYPE="synthetic")))

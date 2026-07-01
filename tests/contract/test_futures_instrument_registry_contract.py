import pytest

from moex_data.futures.instrument_registry import validate_futures_instrument_registry_values
from moex_data.futures.validation import FuturesValidationError, REQUIRED_IDENTIFIER_FIELDS


def _identity(**overrides):
    values = {
        "INSTRUMENT_ID": "fixture.instrument.a",
        "SECID": "FIXTURE_A",
        "BOARD": "RFUD",
        "MARKET": "FORTS",
        "SERIES_TYPE": "native",
    }
    values.update(overrides)
    return values


def _registry(*items):
    return {"registry_id": "futures_instrument_registry.v1", "instruments": tuple(items or (_identity(),))}


def test_registry_accepts_generic_identifier_fields():
    registry = validate_futures_instrument_registry_values(_registry())

    assert registry.registry_id == "futures_instrument_registry.v1"
    assert len(registry.instruments) == 1
    assert tuple(REQUIRED_IDENTIFIER_FIELDS) == ("INSTRUMENT_ID", "SECID", "BOARD", "MARKET", "SERIES_TYPE")
    assert registry.instruments[0].instrument_id == "fixture.instrument.a"
    assert registry.instruments[0].series_type == "native"


def test_generic_registry_allows_empty_declaration_set():
    registry = validate_futures_instrument_registry_values({"registry_id": "futures_instrument_registry.v1", "instruments": ()})

    assert registry.instruments == ()


@pytest.mark.parametrize("field_name", ("INSTRUMENT_ID", "SECID", "BOARD", "MARKET", "SERIES_TYPE"))
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

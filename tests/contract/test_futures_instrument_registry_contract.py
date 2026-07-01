import pytest

from moex_data.futures.instrument_registry import validate_futures_instrument_registry_values
from moex_data.futures.validation import FuturesValidationError, REQUIRED_IDENTIFIER_FIELDS


def _identity(**overrides):
    values = {
        "INSTRUMENT_ID": "forts.test.contract_a",
        "SOURCE_ID": "moex.test.source_a",
        "SECID": "TEST_CONTRACT_A",
        "BOARD": "RFUD",
        "MARKET": "FORTS",
        "ENGINE": "futures",
        "SERIES_TYPE": "native",
        "FAMILY": "TEST_FAMILY_A",
    }
    values.update(overrides)
    return values


def _registry(*items):
    return {"registry_id": "futures_instrument_registry.v1", "instruments": tuple(items or (_identity(),))}


def test_registry_accepts_canonical_identifier_fields():
    registry = validate_futures_instrument_registry_values(_registry())

    assert registry.registry_id == "futures_instrument_registry.v1"
    assert len(registry.instruments) == 1
    assert tuple(REQUIRED_IDENTIFIER_FIELDS) == (
        "INSTRUMENT_ID",
        "SOURCE_ID",
        "SECID",
        "BOARD",
        "MARKET",
        "ENGINE",
        "SERIES_TYPE",
    )
    assert registry.instruments[0].instrument_id == "forts.test.contract_a"
    assert registry.instruments[0].source_id == "moex.test.source_a"
    assert registry.instruments[0].family == "TEST_FAMILY_A"
    assert registry.instruments[0].series_type == "native"


def test_generic_registry_allows_empty_declaration_set():
    registry = validate_futures_instrument_registry_values({"registry_id": "futures_instrument_registry.v1", "instruments": ()})

    assert registry.instruments == ()


@pytest.mark.parametrize(
    "field_name",
    ("INSTRUMENT_ID", "SOURCE_ID", "SECID", "BOARD", "MARKET", "ENGINE", "SERIES_TYPE"),
)
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


def test_legacy_identity_rows_remain_supported_for_existing_raw_5m_boundary():
    registry = validate_futures_instrument_registry_values(
        {
            "registry_id": "futures_instrument_registry.v1",
            "instruments": (
                {"FAMILY": "TEST_FAMILY_A", "SECID": "TEST_CONTRACT_A", "BOARD": "RFUD", "MARKET": "FORTS", "SERIES_TYPE": "native"},
            ),
        }
    )

    assert registry.instruments[0].instrument_id == "TEST_FAMILY_A"
    assert registry.instruments[0].source_id == "TEST_CONTRACT_A"
    assert registry.instruments[0].engine == "legacy"

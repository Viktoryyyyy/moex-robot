import pytest

from moex_data.futures.validation import FuturesValidationError, validate_dataset_contract_values, validate_timeframe


APPROVED_TIMEFRAMES = ("5m", "10m", "15m", "30m", "1h", "4h", "1D", "1W")
DERIVED_TIMEFRAMES = ("10m", "15m", "30m", "1h", "4h", "1D", "1W")


def _contract(**overrides):
    values = {
        "contract_id": "futures_ohlcv_5m.v1",
        "dataset_id": "futures_ohlcv_5m",
        "artifact_class": "external_pattern",
        "storage_root_ref": "MOEX_DATA_ROOT",
        "path_pattern": "${MOEX_DATA_ROOT}/futures/ohlcv_5m/family={FAMILY}/secid={SECID}/board={BOARD}/market={MARKET}/series_type={SERIES_TYPE}/part.parquet",
        "partitioning": ("FAMILY", "SECID", "BOARD", "MARKET", "SERIES_TYPE"),
        "timeframe": "5m",
        "required_identifier_fields": ("FAMILY", "SECID", "BOARD", "MARKET", "SERIES_TYPE"),
    }
    values.update(overrides)
    return values


def test_all_approved_timeframes_are_declared():
    assert tuple(validate_timeframe(item) for item in APPROVED_TIMEFRAMES) == APPROVED_TIMEFRAMES


def test_derived_contract_requires_all_derived_timeframes_including_10m():
    contract = validate_dataset_contract_values(
        _contract(
            contract_id="futures_ohlcv_derived_timeframe.v1",
            dataset_id="futures_ohlcv_derived_timeframe",
            path_pattern="${MOEX_DATA_ROOT}/futures/ohlcv_derived/timeframe={TIMEFRAME}/family={FAMILY}/secid={SECID}/board={BOARD}/market={MARKET}/series_type={SERIES_TYPE}/part.parquet",
            allowed_timeframes=DERIVED_TIMEFRAMES,
            parent_manifest_required=True,
        )
    )

    assert contract.allowed_timeframes == DERIVED_TIMEFRAMES
    assert contract.parent_manifest_required is True


def test_derived_contract_rejects_absent_10m():
    with pytest.raises(FuturesValidationError):
        validate_dataset_contract_values(_contract(allowed_timeframes=("15m", "30m", "1h", "4h", "1D", "1W")))


def test_dataset_contract_rejects_unsupported_timeframe():
    with pytest.raises(FuturesValidationError):
        validate_timeframe("2m")


def test_dataset_contract_rejects_absolute_server_path_and_dynamic_markers():
    with pytest.raises(FuturesValidationError):
        validate_dataset_contract_values(_contract(path_pattern="/home/trader/moex_bot/data/part.parquet"))
    for marker in ("latest", "current", "autodetect"):
        with pytest.raises(FuturesValidationError):
            validate_dataset_contract_values(_contract(path_pattern="${MOEX_DATA_ROOT}/futures/" + marker + "/part.parquet"))


def test_dataset_contract_rejects_missing_universal_identifiers():
    with pytest.raises(FuturesValidationError):
        validate_dataset_contract_values(_contract(required_identifier_fields=("FAMILY", "SECID", "BOARD", "MARKET")))

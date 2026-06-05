import pytest

from moex_data.futures.validation import (
    FuturesValidationError,
    validate_continuous_series_policy_values,
    validate_identifier_values,
    reject_continuous_without_complete_roll_policy,
)


def _policy(**overrides):
    values = {
        "schema_id": "futures_continuous_series_policy.v1",
        "continuous_series_status": "blocked_placeholder",
        "materialization_allowed": False,
        "roll_policy_required": True,
        "expiration_family_mapping_required": True,
    }
    values.update(overrides)
    return values


def _continuous_identity():
    return validate_identifier_values(
        {
            "FAMILY": "TEST_FAMILY_A",
            "SECID": "TEST_CONTINUOUS_A",
            "BOARD": "RFUD",
            "MARKET": "FORTS",
            "SERIES_TYPE": "continuous",
        }
    )


def test_continuous_policy_remains_blocked_placeholder():
    policy = validate_continuous_series_policy_values(_policy())

    assert policy.continuous_series_status == "blocked_placeholder"
    assert policy.materialization_allowed is False
    assert policy.roll_policy_required is True
    assert policy.expiration_family_mapping_required is True


def test_continuous_policy_rejects_unblocked_materialization():
    with pytest.raises(FuturesValidationError):
        validate_continuous_series_policy_values(_policy(materialization_allowed=True))
    with pytest.raises(FuturesValidationError):
        validate_continuous_series_policy_values(_policy(continuous_series_status="active"))


def test_continuous_without_complete_roll_policy_fails_closed():
    with pytest.raises(FuturesValidationError):
        reject_continuous_without_complete_roll_policy(_continuous_identity(), {})


def test_continuous_with_complete_roll_policy_boundary_is_accepted_only_as_guard():
    reject_continuous_without_complete_roll_policy(
        _continuous_identity(),
        {
            "roll_policy_contract_ref": "contracts/datasets/futures_continuous_series_policy.v1.yaml",
            "expiration_family_mapping_contract_ref": "contracts/instruments/futures_universe.v1.yaml",
        },
    )

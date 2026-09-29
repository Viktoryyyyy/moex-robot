"""Exact arithmetic at the versioned one-percent admission boundary."""
from copy import deepcopy

import pytest

from moex_data.futures import futoi_pair_balance as balance


def sides(long, short):
    return ({"long": long, "short": short, "net": long - short},
            {"long": 0, "short": 0, "net": 0})


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("denominator", [10000, 100 * (2**53 + 1)])
@pytest.mark.parametrize("offset,accepted", [(-1, True), (0, True), (1, False)])
def test_inclusive_boundary_uses_exact_integer_contracts(direction, denominator, offset, accepted):
    residual = denominator // 100 + offset
    long, short = denominator, denominator - residual
    if direction == -1:
        long, short = short, long
    fiz, yur = sides(long, short)
    check = balance.evaluate(fiz, yur)
    assert check["denominator_contracts"] == denominator
    assert check["signed_net_imbalance"] == direction * residual
    assert check["accepted"] is accepted
    if accepted:
        assert balance.admit(fiz, yur, balance.RELATIVE) == check
    else:
        with pytest.raises(ValueError, match="exceeds 1%"):
            balance.admit(fiz, yur, balance.RELATIVE)


def test_zero_contracts_do_not_divide_by_zero_or_create_a_percentage():
    check = balance.admit(*sides(0, 0), balance.RELATIVE)
    assert check["accepted"] is True
    assert check["imbalance_percent_decimal"] is None
    with pytest.raises(ValueError, match="exceeds 1%"):
        balance.admit(*sides(1, 0), balance.RELATIVE)


@pytest.mark.parametrize("field,value", [("long", True), ("net", 1.0), ("short", -1), ("net", 999)])
def test_per_row_identity_and_exact_positions_are_still_required(field, value):
    fiz, yur = sides(10000, 9900)
    fiz[field] = value
    with pytest.raises(ValueError):
        balance.admit(fiz, yur, balance.RELATIVE)


def fact():
    fiz, yur = sides(10000, 9900)
    return {"raw_schema_version": "v2", "source_identity_scope": "source_ticker_root",
            "fiz": fiz, "yur": yur, "balance_check": balance.admit(fiz, yur, balance.RELATIVE)}


@pytest.mark.parametrize("field,value", [
    ("policy", balance.STRICT), ("policy", "future_policy"),
    ("limit_percent", 2), ("limit_percent", True), ("denominator_contracts", 20000),
    ("signed_net_imbalance", 0), ("absolute_net_imbalance", 0),
    ("accepted", 1), ("imbalance_percent_decimal", "0"),
    ("contract_ref", "unrelated.json"), ("denominator_basis", "two_sided_gross"),
])
def test_factual_metadata_is_checked_against_original_values(field, value):
    value_fact = fact()
    value_fact["balance_check"][field] = value
    with pytest.raises(ValueError):
        balance.validate_factual(value_fact)


@pytest.mark.parametrize("version,scope", [("v1", "source_ticker_root"), ("v2", "contract"), (None, None)])
def test_tolerance_cannot_escape_versioned_root_scope(version, scope):
    value = fact()
    value.update(raw_schema_version=version, source_identity_scope=scope)
    with pytest.raises(ValueError):
        balance.validate_factual(value)


def test_legacy_is_strict_and_original_values_are_never_adjusted():
    value = fact()
    original = deepcopy(value)
    assert balance.validate_factual(value) == balance.RELATIVE
    assert value == original
    value.pop("balance_check")
    with pytest.raises(ValueError, match="balance to zero"):
        balance.validate_factual(value)
    value["fiz"] = sides(10000, 10000)[0]
    assert balance.validate_factual(value) == balance.STRICT
    value["balance_check"] = None
    with pytest.raises(ValueError):
        balance.validate_factual(value)
    assert balance.from_provenance({}) == balance.STRICT
    with pytest.raises(ValueError):
        balance.from_provenance({"pair_balance_policy": None})


def test_policy_migration_is_not_a_new_economic_observation():
    from moex_data.rub_si_futoi_dated_context import _economic_identity
    fiz, yur = sides(10000, 10000)
    old = {"raw_schema_version": "v2", "source_identity_scope": "source_ticker_root",
           "fiz": fiz, "yur": yur}
    current = {**deepcopy(old), "balance_check": balance.admit(fiz, yur, balance.RELATIVE)}
    assert balance.validate_factual(old) == balance.STRICT
    assert balance.validate_factual(current) == balance.RELATIVE
    assert _economic_identity(old) == _economic_identity(current)
    assert "balance_check" in current  # Retained original records remain untouched.

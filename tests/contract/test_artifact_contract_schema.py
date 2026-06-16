from __future__ import annotations

import importlib

import pytest

from src.moex_strategy_sdk.artifact_contracts import (
    ALLOWED_CONTRACT_CLASSES,
    ArtifactContract,
    validate_artifact_contract,
)
from src.moex_strategy_sdk.errors import ArtifactContractValidationError


def _valid_contract(**overrides: object) -> ArtifactContract:
    payload = {
        "artifact_id": "test_output",
        "artifact_role": "output",
        "contract_class": "repo_relative",
        "producer": "test_producer",
        "consumers": ("test_consumer",),
        "format": "csv",
        "schema_version": 1,
        "partitioning_rule": None,
        "retention_policy": None,
        "locator_ref": "data/test_output.csv",
    }
    payload.update(overrides)
    return ArtifactContract(**payload)


def test_artifact_contract_accepts_canonical_role_and_consumers() -> None:
    contract = _valid_contract(consumers=["consumer_a", "consumer_b"])

    assert contract.artifact_id == "test_output"
    assert contract.artifact_role == "output"
    assert contract.contract_class == "repo_relative"
    assert contract.producer == "test_producer"
    assert contract.consumers == ("consumer_a", "consumer_b")
    assert contract.artifact_class == "output"
    assert contract.consumer == "consumer_a"
    assert validate_artifact_contract(contract) is contract


def test_artifact_contract_accepts_legacy_constructor_aliases_as_read_only_views() -> None:
    contract = ArtifactContract(
        artifact_id="legacy_output",
        artifact_class="output",
        contract_class="external_pattern",
        producer="legacy_producer",
        consumer="legacy_consumer",
        format="table",
        schema_version="legacy.v1",
    )

    assert contract.artifact_role == "output"
    assert contract.consumers == ("legacy_consumer",)
    assert contract.artifact_class == "output"
    assert contract.consumer == "legacy_consumer"
    assert validate_artifact_contract(contract) is contract


def test_supported_contract_classes_are_exact() -> None:
    assert ALLOWED_CONTRACT_CLASSES == frozenset(
        {"repo_relative", "external_pattern", "cli_argument", "env_contract"}
    )

    for contract_class in ALLOWED_CONTRACT_CLASSES:
        contract = _valid_contract(contract_class=contract_class)
        assert validate_artifact_contract(contract).contract_class == contract_class


@pytest.mark.parametrize(
    "overrides",
    (
        {"artifact_id": None},
        {"artifact_id": ""},
        {"artifact_role": None},
        {"artifact_role": ""},
        {"producer": None},
        {"producer": ""},
        {"consumers": None},
        {"consumers": ()},
        {"consumers": ("",)},
        {"contract_class": "unsupported"},
        {"schema_version": None},
        {"schema_version": 0},
        {"schema_version": ""},
        {"schema_version": True},
        {"schema_version": []},
    ),
)
def test_validate_artifact_contract_rejects_invalid_contracts(overrides: dict[str, object]) -> None:
    with pytest.raises(ArtifactContractValidationError):
        validate_artifact_contract(_valid_contract(**overrides))


@pytest.mark.parametrize(
    "module_name",
    (
        "src.strategies.ema_3_19_15m.artifact_contracts",
        "src.strategies.usdrubf_large_day_mr.artifact_contracts",
    ),
)
def test_existing_strategy_artifact_contract_modules_import(module_name: str) -> None:
    module = importlib.import_module(module_name)

    assert module.ARTIFACT_CONTRACTS
    for contract in module.ARTIFACT_CONTRACTS:
        assert validate_artifact_contract(contract) is contract


def test_output_contract_resolution_fields_match_registry_loader_lookup_contract() -> None:
    module = importlib.import_module("src.strategies.usdrubf_large_day_mr.artifact_contracts")

    matches = [
        contract
        for contract in module.ARTIFACT_CONTRACTS
        if contract.artifact_id == "usdrubf_large_day_mr_backtest_day_metrics"
        and contract.artifact_role == "output"
        and contract.producer == "moex_backtest"
    ]

    assert len(matches) == 1
    assert matches[0].consumers == ("backtest_verdict_layer",)

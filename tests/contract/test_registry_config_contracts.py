from pathlib import Path

import pytest

from moex_core.contracts import (
    EXPECTED_REGISTRY_CONFIG_PATHS,
    EXPECTED_REGISTRY_CONTRACT_PATHS,
    REGISTRY_KINDS,
    RegistryContractError,
    validate_registry_entry_values,
    validate_registry_package,
    validate_registry_package_values,
)


REPO_ROOT = Path(__file__).resolve().parents[2]

ENTRY_IDS = {
    "instrument": "instrument.Si.v1",
    "dataset": "dataset.futures_derived_d1.v1",
    "feature": "feature.d1_tsmom_inputs.v1",
    "strategy": "strategy.d1_tsmom.v1",
    "portfolio": "portfolio.single_strategy_research.v1",
    "environment": "environment.research_contract_only.v1",
}

CONFIG_PATHS = {
    "instrument": "configs/instruments/instrument_registry.v1.yaml",
    "dataset": "configs/datasets/dataset_registry.v1.yaml",
    "feature": "configs/features/feature_registry.v1.yaml",
    "strategy": "configs/strategies/strategy_registry.v1.yaml",
    "portfolio": "configs/portfolios/portfolio_registry.v1.yaml",
    "environment": "configs/environments/environment_registry.v1.yaml",
}

DEPENDENCIES = {
    "instrument": {},
    "dataset": {"instrument": (ENTRY_IDS["instrument"],)},
    "feature": {"dataset": (ENTRY_IDS["dataset"],)},
    "strategy": {
        "dataset": (ENTRY_IDS["dataset"],),
        "feature": (ENTRY_IDS["feature"],),
        "instrument": (ENTRY_IDS["instrument"],),
    },
    "portfolio": {
        "strategy": (ENTRY_IDS["strategy"],),
        "instrument": (ENTRY_IDS["instrument"],),
    },
    "environment": {
        "dataset": (ENTRY_IDS["dataset"],),
        "strategy": (ENTRY_IDS["strategy"],),
        "portfolio": (ENTRY_IDS["portfolio"],),
    },
}


def _entry(kind: str, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "entry_id": ENTRY_IDS[kind],
        "registry_kind": kind,
        "config_id": f"{kind}_registry.contract_test.v1",
        "artifact_class": "repo_relative",
        "repo_path": CONFIG_PATHS[kind],
        "dependencies": DEPENDENCIES[kind],
        "enabled": kind in ("instrument", "dataset", "feature"),
        "registry_mutation_allowed": False,
        "promotion_ref_or_none": None,
    }
    values.update(overrides)
    return values


def _package_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "entries": tuple(_entry(kind) for kind in REGISTRY_KINDS),
    }
    values.update(overrides)
    return values


def test_registry_contract_and_config_paths_are_declared_and_present():
    assert REGISTRY_KINDS == (
        "instrument",
        "dataset",
        "feature",
        "strategy",
        "portfolio",
        "environment",
    )
    for relative_path in EXPECTED_REGISTRY_CONTRACT_PATHS + EXPECTED_REGISTRY_CONFIG_PATHS:
        assert not relative_path.startswith("/")
        assert (REPO_ROOT / relative_path).exists(), relative_path


def test_valid_minimal_registry_package_passes():
    package = validate_registry_package_values(_package_values())

    assert validate_registry_package(package) is package
    assert tuple(entry.registry_kind for entry in package.entries) == REGISTRY_KINDS
    assert tuple(entry.artifact_class for entry in package.entries) == tuple("repo_relative" for _ in REGISTRY_KINDS)
    assert tuple(entry.registry_mutation_allowed for entry in package.entries) == tuple(False for _ in REGISTRY_KINDS)


@pytest.mark.parametrize("missing_field", ("entry_id", "registry_kind", "repo_path", "dependencies"))
def test_missing_required_fields_fail_closed(missing_field):
    values = _entry("dataset")
    values.pop(missing_field)

    with pytest.raises(RegistryContractError):
        validate_registry_entry_values(values)


def test_dangling_dependency_ref_fails_closed():
    broken_dataset = _entry("dataset", dependencies={"instrument": ("instrument.not_declared.v1",)})
    entries = (_entry("instrument"), broken_dataset) + tuple(_entry(kind) for kind in REGISTRY_KINDS[2:])

    with pytest.raises(RegistryContractError):
        validate_registry_package_values({"entries": entries})


def test_wrong_dependency_kind_fails_closed():
    with pytest.raises(RegistryContractError):
        validate_registry_entry_values(_entry("feature", dependencies={"instrument": (ENTRY_IDS["instrument"],)}))


@pytest.mark.parametrize(
    "repo_path",
    (
        "/abs/configs/datasets/dataset_registry.v1.yaml",
        "configs/datasets/dataset_registry.late" + "st.yaml",
        "configs/datasets/dataset_registry.cur" + "rent.yaml",
        "configs/datasets/dataset_registry.auto" + "detect.yaml",
    ),
)
def test_implicit_or_absolute_config_refs_fail_closed(repo_path):
    with pytest.raises(RegistryContractError):
        validate_registry_entry_values(_entry("dataset", repo_path=repo_path))


def test_registry_mutation_flag_is_rejected_when_enabled():
    with pytest.raises(RegistryContractError):
        validate_registry_entry_values(_entry("strategy", registry_mutation_allowed=True))


def test_strategy_promotion_approval_is_not_embedded_in_registry():
    values = _entry("strategy")
    values["approval_metric"] = "accepted"

    with pytest.raises(RegistryContractError):
        validate_registry_entry_values(values)


def test_registry_order_and_membership_are_closed_sets():
    with pytest.raises(RegistryContractError):
        validate_registry_package_values({"entries": tuple(_entry(kind) for kind in reversed(REGISTRY_KINDS))})
    with pytest.raises(RegistryContractError):
        validate_registry_package_values({"entries": tuple(_entry(kind) for kind in REGISTRY_KINDS[:-1])})

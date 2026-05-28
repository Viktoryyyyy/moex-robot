from pathlib import Path

import pytest

from moex_research.contracts.strategy_testing_docs import STRATEGY_TESTING_CONTRACT_DOC_PATHS
from moex_research.contracts.validation import (
    REQUIRED_CONTRACT_DOC_MARKERS,
    ContractValidationError,
    validate_contract_doc_paths,
    validate_contract_doc_text,
    validate_required_contract_markers,
    validate_strategy_testing_contract_set,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def _contract_docs():
    return {
        path: (REPO_ROOT / path).read_text(encoding="utf-8")
        for path in STRATEGY_TESTING_CONTRACT_DOC_PATHS
    }


def test_canonical_contract_path_list_has_exactly_11_docs():
    assert len(STRATEGY_TESTING_CONTRACT_DOC_PATHS) == 11
    assert validate_contract_doc_paths(STRATEGY_TESTING_CONTRACT_DOC_PATHS) is STRATEGY_TESTING_CONTRACT_DOC_PATHS


def test_validation_passes_for_current_contract_docs():
    docs = _contract_docs()

    assert validate_strategy_testing_contract_set(docs) is docs


@pytest.mark.parametrize("marker", REQUIRED_CONTRACT_DOC_MARKERS)
def test_missing_marker_fails_closed(marker):
    valid_text = next(iter(_contract_docs().values()))
    broken_text = valid_text.replace(marker, "", 1)

    with pytest.raises(ContractValidationError):
        validate_contract_doc_text(broken_text)


def test_unknown_doc_path_fails_closed():
    paths = STRATEGY_TESTING_CONTRACT_DOC_PATHS + ("contracts/strategy_testing/unknown.v1.md",)

    with pytest.raises(ContractValidationError):
        validate_contract_doc_paths(paths)


def test_missing_doc_path_fails_closed():
    docs = dict(_contract_docs())
    docs.pop(STRATEGY_TESTING_CONTRACT_DOC_PATHS[-1])

    with pytest.raises(ContractValidationError):
        validate_strategy_testing_contract_set(docs)


def test_helper_functions_are_pure_and_do_not_reference_server_runtime_terms():
    source_root = REPO_ROOT / "src" / "moex_research" / "contracts"
    helper_text = "\n".join(
        path.read_text(encoding="utf-8") for path in source_root.glob("*.py")
    ).casefold()

    forbidden_terms = (
        "server",
        "runtime",
        "live",
        "backtest.run",
        "promotion_verdict(",
        "registryentry(",
        "subprocess",
        "requests",
        "urllib",
        "glob(",
    )
    for term in forbidden_terms:
        assert term not in helper_text


def test_validate_required_contract_markers_is_alias_safe():
    valid_text = next(iter(_contract_docs().values()))

    assert validate_required_contract_markers(valid_text) == valid_text

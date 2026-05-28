from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from .strategy_testing_docs import STRATEGY_TESTING_CONTRACT_DOC_PATHS


class ContractValidationError(ValueError):
    pass


REQUIRED_CONTRACT_DOC_MARKERS: Final[tuple[str, ...]] = (
    "contract_id:",
    "schema_version:",
    "artifact_class:",
    "producer:",
    "consumer:",
    "## required_fields",
    "## validation_rules",
    "## forbidden_patterns",
)


def validate_required_contract_markers(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        raise ContractValidationError("contract document text is required")

    missing_markers = tuple(marker for marker in REQUIRED_CONTRACT_DOC_MARKERS if marker not in text)
    if missing_markers:
        raise ContractValidationError("contract document is missing required markers")
    return text


def validate_contract_doc_text(text: str) -> str:
    return validate_required_contract_markers(text)


def validate_contract_doc_paths(paths: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(paths, tuple):
        raise ContractValidationError("contract document paths must be a tuple")
    if paths != STRATEGY_TESTING_CONTRACT_DOC_PATHS:
        raise ContractValidationError("contract document paths must match the canonical strategy testing set")
    if len(paths) != len(set(paths)):
        raise ContractValidationError("contract document paths must be unique")
    return paths


def validate_strategy_testing_contract_set(contract_docs: Mapping[str, str]) -> Mapping[str, str]:
    if not isinstance(contract_docs, Mapping):
        raise ContractValidationError("contract documents must be a mapping")

    expected_paths = validate_contract_doc_paths(STRATEGY_TESTING_CONTRACT_DOC_PATHS)
    provided_paths = tuple(contract_docs.keys())
    if provided_paths != expected_paths:
        raise ContractValidationError("contract document mapping must match the canonical strategy testing set")

    for path in expected_paths:
        validate_contract_doc_text(contract_docs[path])
    return contract_docs


__all__ = [
    "ContractValidationError",
    "REQUIRED_CONTRACT_DOC_MARKERS",
    "validate_contract_doc_paths",
    "validate_contract_doc_text",
    "validate_required_contract_markers",
    "validate_strategy_testing_contract_set",
]

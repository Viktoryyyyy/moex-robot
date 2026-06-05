from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from moex_data.futures.validation import FuturesValidationError, clean_mapping_text, validate_timeframe


@dataclass(frozen=True)
class FuturesSourceContract:
    source_id: str
    source_system: str
    market: str
    board: str
    native_timeframe: str
    output_contract_ref: str


@dataclass(frozen=True)
class FuturesSourceContractSet:
    sources: tuple[FuturesSourceContract, ...]


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FuturesValidationError(field_name + " is required")
    return value.strip()


def validate_futures_source_contract_values(values: Mapping[str, object]) -> FuturesSourceContract:
    if not isinstance(values, Mapping):
        raise FuturesValidationError("source contract must be a mapping")
    clean_mapping_text(values)
    native_timeframe = _text(values.get("native_timeframe"), "native_timeframe")
    if native_timeframe != "raw":
        validate_timeframe(native_timeframe)
    output_ref = _text(values.get("output_contract_ref"), "output_contract_ref")
    if output_ref.startswith("/"):
        raise FuturesValidationError("output_contract_ref must be repo-relative")
    return FuturesSourceContract(
        source_id=_text(values.get("source_id"), "source_id"),
        source_system=_text(values.get("source_system"), "source_system"),
        market=_text(values.get("market"), "market"),
        board=_text(values.get("board"), "board"),
        native_timeframe=native_timeframe,
        output_contract_ref=output_ref,
    )


def validate_futures_source_contract_set(values: Mapping[str, object]) -> FuturesSourceContractSet:
    if not isinstance(values, Mapping):
        raise FuturesValidationError("source contract set must be a mapping")
    clean_mapping_text(values)
    raw_sources = values.get("sources")
    if not isinstance(raw_sources, tuple):
        raise FuturesValidationError("sources must be a tuple")
    sources = tuple(validate_futures_source_contract_values(item) for item in raw_sources)
    if not sources:
        raise FuturesValidationError("sources must be non-empty")
    source_ids = tuple(source.source_id for source in sources)
    if len(set(source_ids)) != len(source_ids):
        raise FuturesValidationError("duplicate source_id")
    return FuturesSourceContractSet(sources=sources)

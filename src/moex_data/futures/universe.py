from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .instrument_registry import FuturesInstrumentRegistry, validate_futures_instrument_registry_values
from .validation import FuturesValidationError, clean_mapping_text, validate_no_dynamic_scan


@dataclass(frozen=True)
class FuturesUniverse:
    universe_id: str
    instruments: FuturesInstrumentRegistry


def validate_futures_universe_values(values: Mapping[str, object]) -> FuturesUniverse:
    if not isinstance(values, Mapping):
        raise FuturesValidationError("universe must be a mapping")
    clean_mapping_text(values)
    validate_no_dynamic_scan(values)
    universe_id = values.get("universe_id")
    if not isinstance(universe_id, str) or not universe_id.strip():
        raise FuturesValidationError("universe_id is required")
    registry = validate_futures_instrument_registry_values(
        {"registry_id": universe_id.strip(), "instruments": values.get("instruments")}
    )
    return FuturesUniverse(universe_id=universe_id.strip(), instruments=registry)

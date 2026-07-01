from collections.abc import Mapping
from dataclasses import dataclass

from .validation import FuturesInstrumentIdentity, FuturesValidationError, validate_identifier_values


@dataclass(frozen=True)
class FuturesInstrumentRegistry:
    registry_id: str
    instruments: tuple[FuturesInstrumentIdentity, ...]


def validate_futures_instrument_registry_values(values: Mapping[str, object]) -> FuturesInstrumentRegistry:
    if not isinstance(values, Mapping):
        raise FuturesValidationError("registry must be a mapping")
    registry_id = values.get("registry_id")
    if not isinstance(registry_id, str) or not registry_id.strip():
        raise FuturesValidationError("registry_id is required")
    raw_items = values.get("instruments")
    if not isinstance(raw_items, tuple):
        raise FuturesValidationError("instruments must be a tuple")
    items = tuple(validate_identifier_values(item) for item in raw_items)
    seen = set()
    for item in items:
        key = (item.instrument_id, item.secid, item.board, item.market, item.series_type)
        if key in seen:
            raise FuturesValidationError("duplicate identity")
        seen.add(key)
    return FuturesInstrumentRegistry(registry_id=registry_id.strip(), instruments=items)

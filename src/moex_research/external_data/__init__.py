"""Point-in-time external-data foundation for USDRUBF market-phase research."""

from .models import ExternalDataError
from .registry import SOURCE_REGISTRY, SOURCE_SLOTS, require_phase8_2_ready

__all__ = [
    "ExternalDataError",
    "SOURCE_REGISTRY",
    "SOURCE_SLOTS",
    "require_phase8_2_ready",
]

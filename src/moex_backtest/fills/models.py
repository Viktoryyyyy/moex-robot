from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final


class FillModelId(str, Enum):
    NEXT_BAR_OPEN = "next_bar_open"
    NEXT_BAR_CLOSE = "next_bar_close"
    NEXT_TICK = "next_tick"


ALLOWED_FILL_MODEL_IDS: Final[frozenset[str]] = frozenset(item.value for item in FillModelId)


@dataclass(frozen=True)
class FillModelSpec:
    fill_model_id: str

    def __post_init__(self) -> None:
        if self.fill_model_id not in ALLOWED_FILL_MODEL_IDS:
            raise ValueError("unsupported fill_model_id")

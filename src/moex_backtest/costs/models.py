from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final


class CostModelId(str, Enum):
    ZERO_COST = "zero_cost"
    FIXED_BPS = "fixed_bps"
    COMMISSION_BPS = "commission_bps"


class SlippageModelId(str, Enum):
    ZERO_SLIPPAGE = "zero_slippage"
    FIXED_BPS = "fixed_bps"
    SPREAD_BPS = "spread_bps"


ALLOWED_COST_MODEL_IDS: Final[frozenset[str]] = frozenset(item.value for item in CostModelId)
ALLOWED_SLIPPAGE_MODEL_IDS: Final[frozenset[str]] = frozenset(item.value for item in SlippageModelId)


@dataclass(frozen=True)
class CostSlippageModelSpec:
    cost_model_id: str
    slippage_model_id: str

    def __post_init__(self) -> None:
        if self.cost_model_id not in ALLOWED_COST_MODEL_IDS:
            raise ValueError("unsupported cost_model_id")
        if self.slippage_model_id not in ALLOWED_SLIPPAGE_MODEL_IDS:
            raise ValueError("unsupported slippage_model_id")

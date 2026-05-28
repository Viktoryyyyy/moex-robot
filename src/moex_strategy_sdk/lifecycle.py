from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RuntimeLifecycleStatus(str, Enum):
    BLOCKED = "blocked"
    ENABLED = "enabled"


@dataclass(frozen=True)
class StrategyLifecycle:
    runtime_status: RuntimeLifecycleStatus = RuntimeLifecycleStatus.BLOCKED
    live_status: RuntimeLifecycleStatus = RuntimeLifecycleStatus.BLOCKED

    @property
    def runtime_blocked(self) -> bool:
        return self.runtime_status == RuntimeLifecycleStatus.BLOCKED

    @property
    def live_blocked(self) -> bool:
        return self.live_status == RuntimeLifecycleStatus.BLOCKED

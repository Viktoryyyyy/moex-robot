class RuntimeLifecycleStatus:
    BLOCKED = "blocked"


class StrategyLifecycle:
    def __init__(self):
        self.runtime_status = RuntimeLifecycleStatus.BLOCKED
        self.live_status = RuntimeLifecycleStatus.BLOCKED

    @property
    def runtime_blocked(self):
        return self.runtime_status == RuntimeLifecycleStatus.BLOCKED

    @property
    def live_blocked(self):
        return self.live_status == RuntimeLifecycleStatus.BLOCKED

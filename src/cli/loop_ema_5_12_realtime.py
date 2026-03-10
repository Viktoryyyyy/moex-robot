#!/usr/bin/env python3
import os

from src.realtime.gate_preflight import preflight
from src.infra.single_instance import acquire_lock, release_lock
from src.strategy.realtime.ema_5_12.runner_ema_5_12 import run_ema_5_12_loop


def main() -> None:
    if not os.getenv("MOEX_API_KEY"):
        print("[CRIT] MOEX_API_KEY missing")
        raise SystemExit(2)

    try:
        gate = preflight()
    except Exception as e:
        print("[Gate] status=BLOCK reason=" + str(e))
        raise SystemExit(2)

    if gate.risk == 1:
        print("[Gate] status=BLOCK reason=phase_transition_risk==1")
        raise SystemExit(2)

    lock = acquire_lock("ema_5_12_realtime")
    try:
        run_ema_5_12_loop()
    finally:
        release_lock(lock)


if __name__ == "__main__":
    main()

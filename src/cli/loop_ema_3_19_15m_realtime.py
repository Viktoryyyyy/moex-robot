import time
from datetime import date

from src.realtime.ema_d_day_context_preflight import preflight
from src.infra.single_instance import acquire_lock, release_lock


SECID = "Si"
EMA_FAST_WINDOW = 3
EMA_SLOW_WINDOW = 19
BAR_INTERVAL_SECONDS = 15 * 60


def main() -> None:
    import os
    if not os.getenv("MOEX_API_KEY"):
        print("[CRIT] MOEX_API_KEY missing")
        raise SystemExit(2)

    try:
        ctx = preflight()
    except Exception as e:
        print("[Context] status=BLOCK reason=" + str(e))
        raise SystemExit(2)
    if not ctx.allowed:
        print("[Context] status=BLOCK reason=allowed!=true")
        raise SystemExit(2)

    lock = acquire_lock("ema_3_19_15m_realtime")
    try:
        trade_date = date.today()
        print("[EMA_3_19_15M] status=ALLOW target_day=" + ctx.target_day + " source_trade_date=" + ctx.source_trade_date + " band=" + ctx.band)
        print("[EMA_3_19_15M] runtime stub active")
        print("[EMA_3_19_15M] secid=" + SECID + " ema_fast=" + str(EMA_FAST_WINDOW) + " ema_slow=" + str(EMA_SLOW_WINDOW) + " bar_interval_seconds=" + str(BAR_INTERVAL_SECONDS) + " trade_date=" + trade_date.isoformat())
        while True:
            try:
                ctx = preflight()
            except Exception as e:
                print("[Context] status=BLOCK reason=" + str(e))
                raise SystemExit(2)
            if not ctx.allowed:
                print("[Context] status=BLOCK reason=allowed!=true")
                raise SystemExit(2)
            time.sleep(5)
    finally:
        release_lock(lock)


if __name__ == "__main__":
    main()

from __future__ import annotations

import math
from collections.abc import Mapping
from copy import deepcopy

from moex_data import synchronized_live_market_oi_context as core
from moex_data import synchronized_live_market_oi_context_apim as apim


PARTIAL_STATUS = "PARTIAL"


def _positive_number(value: object) -> bool:
    return bool(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) > 0
    )


def _future_price_oi_usable(item: Mapping[str, object]) -> bool:
    oi = item.get("oi")
    return bool(
        _positive_number(item.get("last"))
        and isinstance(oi, int)
        and not isinstance(oi, bool)
        and oi >= 0
        and item.get("stale") is False
        and item.get("price_oi_same_source_row") is True
    )


def _spot_price_usable(item: Mapping[str, object]) -> bool:
    return bool(_positive_number(item.get("last")) and item.get("stale") is False)


def _reclassify(snapshot: Mapping[str, object]) -> dict[str, object]:
    result = deepcopy(dict(snapshot))
    instruments = result.get("instruments")
    synchronization = result.get("synchronization")
    quality = result.get("quality")
    if not isinstance(instruments, dict):
        raise core.SynchronizedLiveMarketOIError("snapshot instruments are missing")
    if not isinstance(synchronization, dict):
        raise core.SynchronizedLiveMarketOIError("snapshot synchronization is missing")
    if not isinstance(quality, dict):
        raise core.SynchronizedLiveMarketOIError("snapshot quality is missing")

    futures_usable: dict[str, bool] = {}
    future_timestamps = []
    for logical_id in core.FUTURES_LOGICAL_ORDER:
        item = instruments.get(logical_id)
        if not isinstance(item, dict):
            raise core.SynchronizedLiveMarketOIError(f"snapshot instrument {logical_id} is missing")
        usable = _future_price_oi_usable(item)
        item["price_oi_usable"] = usable
        futures_usable[logical_id] = usable
        future_timestamps.append(core._aware_utc(item.get("timestamp"), f"{logical_id}.timestamp"))

    spot = instruments.get("cnyrub_tom")
    if not isinstance(spot, dict):
        raise core.SynchronizedLiveMarketOIError("snapshot instrument cnyrub_tom is missing")
    spot_usable = _spot_price_usable(spot)

    futures_oldest = min(future_timestamps)
    futures_newest = max(future_timestamps)
    futures_max_skew = (futures_newest - futures_oldest).total_seconds()
    futures_all_fresh = all(instruments[key].get("stale") is False for key in core.FUTURES_LOGICAL_ORDER)
    futures_synchronized = bool(
        futures_all_fresh and futures_max_skew <= core.MAX_SKEW_SECONDS
    )

    full_synchronized = synchronization.get("synchronized") is True
    all_futures_usable = all(futures_usable.values())
    full_analysis_usable = bool(full_synchronized and all_futures_usable and spot_usable)
    factual_context_usable = bool(any(futures_usable.values()) or spot_usable)

    synchronization["futures_status"] = "PASS" if futures_synchronized else "FAIL"
    synchronization["futures_synchronized"] = futures_synchronized
    synchronization["futures_all_fresh"] = futures_all_fresh
    synchronization["futures_oldest_timestamp_utc"] = core._iso(futures_oldest)
    synchronization["futures_as_of_utc"] = core._iso(futures_newest)
    synchronization["futures_max_skew_seconds"] = round(futures_max_skew, 3)

    quality["analysis_usable"] = full_analysis_usable
    quality["price_oi_all_futures_usable"] = all_futures_usable
    quality["price_oi_usable_by_instrument"] = futures_usable
    quality["spot_price_usable"] = spot_usable
    quality["factual_context_usable"] = factual_context_usable
    quality["full_cross_market_synchronization_required"] = True
    quality["status"] = (
        "PASS" if full_analysis_usable else PARTIAL_STATUS if factual_context_usable else "FAIL"
    )
    result["status"] = (
        "READY" if full_analysis_usable else PARTIAL_STATUS if factual_context_usable else "UNAVAILABLE"
    )
    return result


def fetch_live_snapshot(**kwargs: object) -> dict[str, object]:
    return _reclassify(apim.fetch_live_snapshot(**kwargs))


SCHEMA_VERSION = apim.SCHEMA_VERSION
SynchronizedLiveMarketOIError = core.SynchronizedLiveMarketOIError

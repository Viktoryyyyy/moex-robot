"""Downgrade persisted live facts at consumption time without fetching or promotion."""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone


LIVE = "synchronized_live_market_oi"
BASIS = "live_basis_carry"
MAX_LIVE_AGE_SECONDS = 60
MAX_FUTURE_SKEW_SECONDS = 5


def _time(value: object) -> datetime:
    result = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return result.astimezone(timezone.utc)


def apply_read_freshness(snapshot: Mapping[str, object], *, now: datetime) -> dict[str, object]:
    """Return an independent view. Never upgrade a persisted quality/authority gate."""
    now = _time(now)
    result = deepcopy(dict(snapshot))
    components = result.get("components", {})
    live = components.get(LIVE) if isinstance(components, dict) else None
    data = live.get("data") if isinstance(live, dict) else None
    instruments = data.get("instruments", {}) if isinstance(data, dict) else {}
    if not isinstance(instruments, dict):
        instruments = {}
    blocked: set[str] = set()
    for key, item in instruments.items():
        if not isinstance(item, dict):
            blocked.add(key)
            continue
        try:
            signed_age = (now - _time(item.get("timestamp"))).total_seconds()
            age = max(0.0, signed_age)
            reason = (
                "future_source_timestamp" if signed_age < -MAX_FUTURE_SKEW_SECONDS
                else "source_age_exceeds_threshold" if age > MAX_LIVE_AGE_SECONDS
                else None
            )
        except (ValueError, TypeError, OverflowError):
            age, reason = None, "invalid_source_timestamp"
        if reason is None and item.get("stale") is not False:
            reason = "persisted_source_not_fresh"
        item["age_seconds"] = age
        item["freshness_reference_utc"] = now.isoformat()
        item["stale"] = reason is not None
        item["read_freshness_reason"] = reason
        if reason:
            blocked.add(key)
            item["price_oi_usable"] = False
            if "spot_price_usable" in item:
                item["spot_price_usable"] = False

    if isinstance(data, dict):
        sync = data.get("synchronization", {})
        quality = data.get("quality", {})
        if not isinstance(sync, dict) or not isinstance(quality, dict):
            sync, quality = {}, {}
            data["synchronization"], data["quality"] = sync, quality
            blocked.update(instruments)
        usable_map = quality.get("price_oi_usable_by_instrument", {})
        if not isinstance(usable_map, dict):
            usable_map = {}
        for key in list(usable_map):
            usable_map[key] = bool(
                usable_map[key] is True and key in instruments and key not in blocked
            )
        quality["price_oi_usable_by_instrument"] = usable_map
        futures_ok = bool(usable_map) and all(usable_map.values())
        quality["price_oi_all_futures_usable"] = bool(
            quality.get("price_oi_all_futures_usable") is True and futures_ok
        )
        spot_ok = "cnyrub_tom" in instruments and "cnyrub_tom" not in blocked
        quality["spot_price_usable"] = bool(quality.get("spot_price_usable") is True and spot_ok)
        for field in ("futures_synchronized", "futures_all_fresh"):
            sync[field] = bool(sync.get(field) is True and futures_ok)
        sync["futures_status"] = "PASS" if sync["futures_synchronized"] else "FAIL"
        full_fresh = bool(instruments) and not blocked
        for field in ("synchronized", "all_instruments_fresh"):
            sync[field] = bool(sync.get(field) is True and full_fresh)
        sync["status"] = "PASS" if sync["synchronized"] else "FAIL"
        sync["freshness_reference_utc"] = now.isoformat()
        quality["analysis_usable"] = bool(
            quality.get("analysis_usable") is True and sync["synchronized"]
            and quality["price_oi_all_futures_usable"] and quality["spot_price_usable"]
        )
        factual = bool(any(usable_map.values()) or quality["spot_price_usable"])
        quality["factual_context_usable"] = bool(
            (quality.get("factual_context_usable") is True or quality.get("analysis_usable") is True)
            and factual and live.get("status") in {"READY", "PARTIAL"}
        )
        data["status"] = (
            "READY" if quality["analysis_usable"] and live.get("status") == "READY"
            else "PARTIAL" if quality["factual_context_usable"] else "UNAVAILABLE"
        )
        quality["status"] = {"READY": "PASS", "PARTIAL": "PARTIAL", "UNAVAILABLE": "FAIL"}[data["status"]]
        if live.get("status") in {"READY", "PARTIAL"}:
            live["status"] = data["status"]

    basis = components.get(BASIS) if isinstance(components, dict) else None
    derived = basis.get("data") if isinstance(basis, dict) else None
    if isinstance(derived, dict):
        pairs = derived.get("pairs", {})
        total_ready = 0
        if isinstance(pairs, dict):
            for pair in pairs.values():
                if not isinstance(pair, dict):
                    continue
                legs = pair.get("legs", {})
                for key, leg in legs.items() if isinstance(legs, dict) else ():
                    if not isinstance(leg, dict):
                        continue
                    source = instruments.get(key)
                    if isinstance(source, dict):
                        leg["age_seconds"] = source.get("age_seconds")
                        leg["stale"] = source.get("stale")
                    if key not in instruments or key in blocked:
                        leg["status"] = "UNAVAILABLE"
                        leg["unavailable_reason"] = "source_not_fresh_at_read"
                metrics = pair.get("metrics", [])
                for metric in metrics:
                    required = metric.get("legs", [])
                    freshness = metric.get("freshness")
                    if isinstance(freshness, dict):
                        freshness["age_seconds_by_leg"] = {
                            key: instruments[key].get("age_seconds")
                            if isinstance(instruments.get(key), dict) else None for key in required
                        }
                    if metric.get("status") == "READY" and (
                        not required or any(key not in instruments or key in blocked for key in required)
                        or basis.get("status") not in {"READY", "PARTIAL"}
                    ):
                        metric["status"] = "UNAVAILABLE"
                        metric["value"] = None
                        metric["synchronized"] = False
                        metric["unavailable_reason"] = "source_not_fresh_at_read"
                    if isinstance(freshness, dict) and metric.get("status") != "READY":
                        freshness["status"] = "UNAVAILABLE"
                count = sum(metric.get("status") == "READY" for metric in metrics)
                pair["ready_metric_count"] = count
                pair["unavailable_metric_ids"] = [m.get("metric_id") for m in metrics if m.get("status") != "READY"]
                pair["status"] = "READY" if metrics and count == len(metrics) else "PARTIAL" if count else "UNAVAILABLE"
                spot_key = "usd_tom" if pair.get("pair_id") == "USD/RUB" else "cnyrub_tom"
                pair["live_spot_available"] = bool(
                    pair.get("live_spot_available") is True and spot_key in instruments and spot_key not in blocked
                )
                total_ready += count
        derived["ready_metric_count"] = total_ready
        if not total_ready:
            derived["status"] = derived["current_live_scope_status"] = "UNAVAILABLE"
        elif derived.get("status") == "READY" and blocked:
            derived["status"] = "PARTIAL"
        if blocked and derived.get("current_live_scope_status") == "READY":
            derived["current_live_scope_status"] = "PARTIAL" if total_ready else "UNAVAILABLE"
        derived["requested_metric_availability_status"] = derived.get("status")
        if basis.get("status") in {"READY", "PARTIAL"}:
            basis["status"] = derived.get("current_live_scope_status", derived.get("status"))

    authority = result.get("authority")
    if isinstance(authority, dict):
        if not isinstance(data, dict) or data.get("quality", {}).get("factual_context_usable") is not True:
            authority["live_market_oi_factual_authority"] = False
        if not isinstance(derived, dict) or not derived.get("ready_metric_count"):
            authority["live_basis_carry_factual_authority"] = False
    readiness = result.get("readiness")
    if isinstance(readiness, dict) and isinstance(components, dict):
        statuses = {key: item.get("status") for key, item in components.items() if isinstance(item, dict)}
        readiness["component_statuses"] = statuses
        for field, status in (("unavailable_components", "UNAVAILABLE"), ("partial_components", "PARTIAL"), ("retained_previous_components", "RETAINED_PREVIOUS")):
            readiness[field] = sorted(key for key, value in statuses.items() if value == status)
        if any(value in {"UNAVAILABLE", "PARTIAL", "RETAINED_PREVIOUS"} for value in statuses.values()):
            readiness["status"] = "PARTIAL"
    result["live_read_freshness"] = {
        "read_at_utc": now.isoformat(),
        "maximum_source_age_seconds": MAX_LIVE_AGE_SECONDS,
        "blocked_instruments": sorted(blocked),
        "additional_live_fetch_performed": False,
        "policy": "downgrade_only_source_event_time_at_read",
    }
    return result

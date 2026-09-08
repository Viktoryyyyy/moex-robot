"""Read-time admission shared by the matrix and lossless factual projection."""
from datetime import datetime
from math import isfinite

from moex_data.rub_snapshot_read_freshness import MAX_LIVE_AGE_SECONDS, MAX_FUTURE_SKEW_SECONDS


def _dict(value):
    return value if isinstance(value, dict) else {}


def market_data(snapshot):
    return _dict(_dict(_dict(snapshot.get('components')).get('synchronized_live_market_oi')).get('data'))


def reference(snapshot):
    try:
        value = (snapshot['live_read_freshness']['read_at_utc'] if 'live_read_freshness' in snapshot
                 else snapshot['identity']['generated_at_utc'])
        now = datetime.fromisoformat(value)
        return now if now.utcoffset() is not None else None
    except (KeyError, TypeError, ValueError):
        return None


def fresh(item, now):
    try:
        age = (now - datetime.fromisoformat(item['timestamp'])).total_seconds()
        return item.get('stale') is False and -MAX_FUTURE_SKEW_SECONDS <= age <= MAX_LIVE_AGE_SECONDS
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def spot_usable(snapshot):
    data = market_data(snapshot)
    item = _dict(_dict(data.get('instruments')).get('cnyrub_tom'))
    quality = _dict(data.get('quality'))
    value = item.get('last')
    return (isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and value > 0
            and fresh(item, reference(snapshot)) and item.get('spot_price_usable') is not False
            and quality.get('spot_price_usable') is True)


def basis_metrics(snapshot):
    """One deterministic occurrence per admitted ID; conflicting duplicates revoke it."""
    components = _dict(snapshot.get('components'))
    component = components.get('live_basis_carry', {})
    if not isinstance(component, dict) or component.get('status') not in {'READY', 'PARTIAL'}:
        return []
    data = component.get('data')
    pairs = data.get('pairs') if isinstance(data, dict) else None
    if not isinstance(pairs, dict): return []
    instruments = _dict(market_data(snapshot).get('instruments'))
    now = reference(snapshot)
    grouped = {}
    for key in sorted(k for k in pairs if isinstance(k, str)):
        pair = pairs[key]
        metrics = pair.get('metrics') if isinstance(pair, dict) else None
        if not isinstance(metrics, list): continue
        for index, metric in enumerate(metrics):
            if not isinstance(metric, dict): continue
            identity = metric.get('metric_id')
            if not isinstance(identity, str) or not identity: continue
            path = f'components.live_basis_carry.data.pairs.{key}.metrics.{index}'
            grouped.setdefault(identity, []).append((path, metric))
    result = []
    for identity in sorted(grouped):
        copies = grouped[identity]
        path, metric = copies[0]
        if any(other != metric for _, other in copies): continue
        legs = metric.get('legs')
        value = metric.get('value')
        if (metric.get('status') == 'READY' and isinstance(legs, list) and legs
                and isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)
                and all(isinstance(leg, str) and fresh(instruments.get(leg, {}), now) for leg in legs)):
            result.append((path, metric))
    return result

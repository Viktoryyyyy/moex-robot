"""Bounded native TradeStats acquisition and preparation-only observed H1 replay."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from urllib.parse import urlsplit, parse_qs

from moex_data import rub_dated_context as dated, rub_hourly_observation as hourly
from moex_data import rub_dated_source_admission as envelope
from moex_data import synchronized_live_market_oi_context as transport

ENDPOINT = '/iss/datashop/algopack/fo/tradestats/USDRUBF.json'
PURPOSE = 'timeframe:observed_1H.USDRUBF'
COLUMNS = ('secid', 'tradedate', 'tradetime', 'pr_open', 'pr_high', 'pr_low', 'pr_close', 'vol')
MAX_PAGES = 4
MAX_ROWS = 1000
MAX_BYTES = 1_000_000


def _table(payload, name):
    block = payload[name]; columns = block['columns']; rows = block['data']
    if (not isinstance(columns, list) or not all(isinstance(c, str) for c in columns)
            or len({c.lower() for c in columns}) != len(columns) or not isinstance(rows, list)
            or any(not isinstance(r, list) or len(r) != len(columns) for r in rows)):
        raise ValueError('invalid_native_table')
    return [c.lower() for c in columns], rows


def native_rows(pages, source_date):
    """Fail closed on ambiguous identity/cursor; numerical defects stay per hour."""
    if not isinstance(pages, list) or not 1 <= len(pages) <= MAX_PAGES:
        raise ValueError('unbounded_native_pages')
    if len(json.dumps(pages, allow_nan=False).encode()) > MAX_BYTES:
        raise ValueError('unbounded_native_bytes')
    result = []; total = None; last_receipt = None
    for page in pages:
        requested = dated.stamp(page['request_started_at_utc']); received = dated.stamp(page['received_at_utc'])
        if requested > received: raise ValueError('noncausal_native_request')
        if last_receipt is not None and requested < last_receipt: raise ValueError('noncausal_native_pagination')
        last_receipt = received
        url = urlsplit(page['source_url']); query = parse_qs(url.query)
        if (url.scheme, url.netloc, url.path) != ('https', 'apim.moex.com', ENDPOINT):
            raise ValueError('native_endpoint_mismatch')
        if any(key in query and query[key] != [source_date] for key in ('from', 'till')):
            raise ValueError('native_response_date_mismatch')
        params = page['request_params']
        if params != {'from': source_date, 'till': source_date, 'start': len(result), 'iss.meta': 'off'}:
            raise ValueError('native_query_mismatch')
        payload = page['payload']; columns, rows = _table(payload, 'data')
        if not set(COLUMNS) <= set(columns): raise ValueError('native_schema_mismatch')
        cursor_columns, cursor_rows = _table(payload, 'data.cursor')
        if len(cursor_rows) != 1: raise ValueError('native_cursor_missing')
        cursor = dict(zip(cursor_columns, cursor_rows[0]))
        index, current_total, size = (cursor[k] for k in ('index', 'total', 'pagesize'))
        if any(type(v) is not int or v < 0 for v in (index, current_total, size)):
            raise ValueError('native_cursor_invalid')
        if total is None: total = current_total
        if total > MAX_ROWS or total != current_total or index != len(result) or (total and not size):
            raise ValueError('native_cursor_inconsistent')
        if len(rows) != min(size, total - index): raise ValueError('native_cursor_row_count_mismatch')
        for values in rows:
            raw = dict(zip(columns, values))
            if raw['secid'] != 'USDRUBF' or raw['tradedate'] != source_date:
                raise ValueError('native_identity_or_date_mismatch')
            end = datetime.fromisoformat(source_date + 'T' + raw['tradetime'])
            if end.utcoffset() is not None or end.second or end.microsecond or end.minute % 5:
                raise ValueError('native_bar_time_invalid')
            end = end.replace(tzinfo=transport.MOSCOW).astimezone(timezone.utc)
            result.append((end, {c: raw[c] for c in COLUMNS}, received))
    if len(result) != total: raise ValueError('native_pagination_incomplete')
    return result


def select(pages, source_date, *, now):
    rows = native_rows(pages, source_date)
    groups = {}; diagnostics = {}
    for end, raw, receipt in rows:
        start = (end - timedelta(microseconds=1)).replace(minute=0, second=0, microsecond=0)
        groups.setdefault(start, []).append((end, raw, receipt))
    for start in sorted(groups, reverse=True):
        group = groups[start]; label = start.isoformat()
        try:
            bars = [{'end': end.isoformat(), **{target: raw[name] for target, name in
                    (('open', 'pr_open'), ('high', 'pr_high'), ('low', 'pr_low'), ('close', 'pr_close'), ('volume', 'vol'))}}
                    for end, raw, _ in group]
            hour = hourly.aggregate(bars)
            if any(end > receipt for end, _, receipt in group): raise ValueError('native_bar_after_receipt')
            for end, _, _ in group: dated._age(end, dated.stamp(now))
            return {'columns': list(COLUMNS), 'data': [[raw[c] for c in COLUMNS] for _, raw, _ in group]}, hour, diagnostics
        except (KeyError, TypeError, ValueError) as exc:
            diagnostics[label] = {'observed_bar_count': len(group), 'required_bar_count': 12, 'reason': str(exc)}
    return None, None, diagnostics


def acquire(*, now_fn=lambda: datetime.now(timezone.utc), http_get=None, env=None):
    import os
    import requests
    clock = dated.stamp(now_fn()); earliest = (clock - timedelta(seconds=dated.MAX_AGE_SECONDS)).astimezone(transport.MOSCOW).date()
    candidate = clock.astimezone(transport.MOSCOW).date(); attempts = {}
    active_env = os.environ if env is None else env
    for offset in range(5):
        day = candidate - timedelta(days=offset)
        if day < earliest: break
        source_date = day.isoformat(); pages = []
        try:
            base = transport._api_base_url(None, active_env); headers = transport._auth_headers(active_env)
            start = 0
            for _ in range(MAX_PAGES):
                params = {'from': source_date, 'till': source_date, 'start': start, 'iss.meta': 'off'}
                requested = dated.stamp(now_fn())
                payload, url, received = transport._fetch_json(url=base + ENDPOINT, params=params, headers=headers,
                    timeout=12.0, http_get=requests.get if http_get is None else http_get, now_fn=now_fn)
                pages.append({'payload': payload, 'source_url': url, 'request_params': params,
                              'request_started_at_utc': requested.isoformat(), 'received_at_utc': received.isoformat()})
                columns, rows = _table(payload, 'data.cursor'); cursor = dict(zip(columns, rows[0]))
                if type(cursor['pagesize']) is not int or cursor['pagesize'] <= 0:
                    if cursor['total'] != 0: raise ValueError('native_cursor_invalid')
                start += len(payload['data']['data'])
                if start >= cursor['total']: break
                if not payload['data']['data']: raise ValueError('native_pagination_no_progress')
            selected, hour, skipped = select(pages, source_date, now=now_fn())
            attempts[source_date] = {'status': 'SELECTED' if hour else 'EMPTY' if not native_rows(pages, source_date) else 'NO_COMPLETE_HOUR',
                                     'skipped_hours': skipped}
            if hour:
                return {'source_date': source_date, 'pages': pages, 'selected': selected, 'latest_attempts': attempts}
        except Exception as exc:
            attempts[source_date] = {'status': 'INVALID' if isinstance(exc, (ValueError, KeyError, TypeError, IndexError)) else 'ERROR',
                                     'reason': str(exc)[:160] if isinstance(exc, ValueError) else type(exc).__name__}
    return {'latest_attempts': attempts}


def make_frame(acquisition, *, now):
    pages = acquisition['pages']; source_date = acquisition['source_date']
    selected, hour, _ = select(pages, source_date, now=now)
    if hour is None: raise ValueError('no_admissible_observed_hour')
    frame = {'schema_version': envelope.SCHEMA, 'origin': envelope.ORIGIN, 'kind': 'slow',
             'purpose': PURPOSE, 'source_id': hourly.SOURCE, 'scope': 'preparation_only',
             'revision_semantics': 'observed_now_not_historical_pit', 'current_usable': False,
             'historical_pit_usable': False, 'model_usable': False,
             'identity': {'secid': 'USDRUBF', 'source_date': source_date, 'source_url': 'https://apim.moex.com' + ENDPOINT},
             'units': {'price': 'RUB_per_USD', 'volume': 'contracts'},
             'raw_source_payload': selected, 'raw_source_digest': dated.digest(selected), 'source_pages': deepcopy(pages),
             'source_pages_digest': dated.digest(pages),
             'source_observation_at_utc': hour['hour_end_utc'], 'accepted_at_utc': now.isoformat(), 'generation_at_utc': now.isoformat(),
             'request_started_at_utc': pages[0]['request_started_at_utc'],
             'received_at_utc': max((p['received_at_utc'] for p in pages), key=dated.stamp)}
    from moex_data.rub_dated_market_source import _revision
    frame['revision_id'] = _revision(frame)
    return frame


def replay(frame):
    accepted = dated.stamp(frame['accepted_at_utc']); envelope.validate_envelope(frame, now=accepted)
    source_date = frame['identity']['source_date']
    if (frame['purpose'] != PURPOSE or frame['source_id'] != hourly.SOURCE or
            frame['identity'] != {'secid': 'USDRUBF', 'source_date': source_date, 'source_url': 'https://apim.moex.com' + ENDPOINT}
            or frame['units'] != {'price': 'RUB_per_USD', 'volume': 'contracts'} or dated.stamp(frame['generation_at_utc']) != accepted):
        raise ValueError('hour_source_identity_units_or_generation_mismatch')
    pages = frame['source_pages']
    if dated.digest(pages) != frame['source_pages_digest']: raise ValueError('native_source_pages_digest_mismatch')
    selected, hour, _ = select(pages, source_date, now=accepted)
    if (selected != frame['raw_source_payload'] or hour is None or hour['hour_end_utc'] != frame['source_observation_at_utc']
            or frame['request_started_at_utc'] != pages[0]['request_started_at_utc']
            or frame['received_at_utc'] != max((p['received_at_utc'] for p in pages), key=dated.stamp)
            or any(dated.stamp(p['received_at_utc']) > accepted for p in pages)):
        raise ValueError('hour_source_replay_mismatch')
    return {'values': {**hour, 'source_id': hourly.SOURCE, 'requested_secid': 'USDRUBF',
                      'receipt_upper_bound_utc': frame['received_at_utc'], 'receipt_semantics': 'dated_native_response_completion_upper_bound',
                      'current_usable': False, 'historical_pit_usable': False, 'model_usable': False,
                      'accepted_at_utc': frame['accepted_at_utc'], 'request_started_at_utc': frame['request_started_at_utc'],
                      'revision_id': frame['revision_id'], 'raw_source_digest': frame['raw_source_digest']}}


def capture(previous, acquisition, *, now):
    admitted, _ = dated.validated(previous, now)
    frames = {ref: deepcopy(frame) for ref, frame, *_ in admitted.values()}; selections = {key: value[0] for key, value in admitted.items()}
    try:
        frame = make_frame(acquisition, now=now); value = replay(frame); prior = admitted.get(PURPOSE)
        newer = not prior or dated.stamp(prior[2]['values']['hour_end_utc']) < dated.stamp(value['values']['hour_end_utc'])
        revised = prior and prior[1].get('origin') == envelope.ORIGIN and prior[1].get('revision_id') != frame['revision_id'] and prior[2]['values']['hour_end_utc'] == value['values']['hour_end_utc']
        if newer or revised:
            ref = dated.digest(frame); frames[ref] = frame; selections[PURPOSE] = ref
    except (KeyError, TypeError, ValueError, AttributeError, IndexError): pass
    return {'schema_version': dated.SCHEMA, 'frames': {ref: frame for ref, frame in frames.items() if ref in selections.values()},
            'selections': selections, 'last_hour_source_attempts': deepcopy(acquisition.get('latest_attempts', {}))}

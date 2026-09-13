"""Bounded, received-now history of the selected contract; never a roll series."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import time
from urllib.parse import quote

from . import moex_brent_factual as source

SCHEMA = 'brent_daily_weekly_context.v1'
MAX_PAGES = 4
MAX_ROWS = 400
BUDGET_SECONDS = 8


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return sha256(encoded(value)).hexdigest()


def payload_digest(value):
    # Non-finite native cells are retained as rejected ordinal positions.
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=True).encode()).hexdigest()


def week(day):
    start = day - timedelta(days=day.weekday())
    return start, start + timedelta(days=6)


def bounds(latest, now):
    start = week(now.astimezone(source.MOSCOW).date())[0] - timedelta(weeks=7)
    return max(start, source._date(latest['identity_evidence']['first_trade_date'])), source._date(latest['source_trade_date'])


def route(secid, start, end, offset):
    return source._route('/history' + source.MARKET + '/boards/RFUD/securities/' + quote(secid, safe='') + '.json',
        **{'from': start, 'till': end, 'start': offset, 'limit': 100, 'iss.only': 'history,history.cursor'})


def immutable(root, category, body):
    identity = sha256(body).hexdigest()
    path = Path(root) / 'audit' / 'brent_daily_context' / category / (identity + '.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open('xb') as handle:
            handle.write(body)
    except FileExistsError:
        source._require(path.read_bytes() == body, 'immutable Brent audit collision')
    return identity


def _page_rows(batch, latest, accepted):
    """Replay exact range and pagination; invalid prices retain source positions."""
    start, end = source._date(batch['from_date']), source._date(batch['till_date'])
    source._require(start <= end < accepted.astimezone(source.MOSCOW).date(), 'history range invalid')
    pages = batch['pages']
    source._require(isinstance(pages, list) and 0 < len(pages) <= MAX_PAGES, 'history pages missing or excessive')
    offset = 0; total = None; result = []; last_receipt = None
    for page in pages:
        requested, received = source._utc(page['requested_at']), source._utc(page['received_at'])
        source._require(requested <= received <= accepted and (last_receipt is None or requested >= last_receipt), 'history page chronology invalid')
        last_receipt = received
        source._require(page['source_route'] == route(latest['secid'], start.isoformat(), end.isoformat(), offset), 'history page route mismatch')
        raw = page['raw_body'].encode('utf-8')
        source._require(0 < len(raw) <= source.MAX_BODY_BYTES and sha256(raw).hexdigest() == page['raw_source_id'], 'history raw digest mismatch')
        payload = json.loads(raw, parse_constant=str)
        source._require(payload == page['payload'], 'history raw replay mismatch')
        source._require(page['payload_digest'] == payload_digest(payload), 'history payload digest mismatch')
        rows = source._rows(payload, 'history'); cursors = source._rows(payload, 'history.cursor')
        source._require(len(cursors) == 1, 'history cursor missing')
        cursor = cursors[0]
        source._require(all(type(cursor.get(k)) is int for k in ('INDEX', 'TOTAL', 'PAGESIZE')), 'history cursor types invalid')
        if total is None: total = cursor['TOTAL']
        source._require(cursor['INDEX'] == offset and cursor['TOTAL'] == total and 0 <= total <= MAX_ROWS
            and cursor['PAGESIZE'] > 0 and len(rows) == min(cursor['PAGESIZE'], total-offset), 'history pagination incomplete')
        source._require(total == 0 or bool(rows), 'history pagination made no progress')
        result.extend(rows); offset += len(rows)
    source._require(offset == total, 'history pagination incomplete')
    seen = set()
    for row in result:
        day = source._date(row.get('TRADEDATE'))
        source._require(start <= day <= end and day not in seen, 'history duplicate or out-of-range date')
        seen.add(day)
        source._require(row.get('SECID') == latest['secid'] and row.get('BOARDID') == 'RFUD' and row.get('ASSETCODE') == 'BR', 'history source identity mismatch')
    return sorted(result, key=lambda row: row['TRADEDATE'])


def replay(evidence, latest, now):
    source._require(isinstance(evidence, dict) and evidence.get('schema_version') == SCHEMA, 'history evidence missing')
    identity = {k: latest[k] for k in ('secid', 'expiry', 'board', 'price_unit', 'source_unit_text')}
    source._require(evidence['identity'] == identity and identity['price_unit'] == 'USD/barrel'
        and identity['source_unit_text'] == source.UNIT_TEXT, 'history identity/unit mismatch')
    accepted = source._utc(evidence['accepted_at'])
    source._require(source._utc(evidence['first_accepted_at']) <= accepted <= now, 'history acceptance chronology invalid')
    source._require(evidence['audit_version_ref'] == payload_digest({k: v for k, v in evidence.items() if k != 'audit_version_ref'}), 'history version digest mismatch')
    batches = evidence['batches']
    source._require(isinstance(batches, list) and 1 <= len(batches) <= 8, 'history batch count invalid')
    source._require(evidence['full_check_date_moscow'] == source._utc(batches[0]['pages'][-1]['received_at']).astimezone(source.MOSCOW).date().isoformat(), 'history full-check date mismatch')
    rows = []
    for i, batch in enumerate(batches):
        incoming = _page_rows(batch, latest, accepted)
        if i:
            source._require(batch['till_date'] >= batches[i-1]['till_date'] and batch['from_date'] <= batches[i-1]['till_date'], 'history incremental coverage gap')
            rows = [r for r in rows if r['TRADEDATE'] < batch['from_date']]
        rows.extend(incoming)
    source._require(len(rows) <= MAX_ROWS and rows, 'history empty or excessive')
    source._require(evidence['source_revision_id'] == payload_digest({'identity': identity, 'rows': rows}), 'history revision mismatch')
    source._require(rows[-1]['TRADEDATE'] == latest['source_trade_date'] and source._ohlc(rows[-1]) == latest['ohlc'], 'history latest anchor mismatch')
    source._require(source._utc(evidence['received_at']) == source._utc(batches[-1]['pages'][-1]['received_at']) <= accepted, 'history receipt mismatch')
    return rows


def acquire(latest, *, previous=None, audit_root, clock=lambda: datetime.now(timezone.utc), transport=None, monotonic=time.monotonic):
    """Preserve raw versions. Same-date unchanged anchors need no extra history call."""
    started = monotonic(); now = source._utc(clock()); start, end = bounds(latest, now)
    prior = previous if isinstance(previous, dict) else {}
    old = prior.get('evidence'); valid_old = False
    try:
        replay(old, prior.get('anchor') or latest, now)
        valid_old = True
    except (ValueError, TypeError, KeyError, OverflowError, AttributeError, IndexError):
        pass
    try:
        replay(old, latest, now)
        if old['full_check_date_moscow'] == now.astimezone(source.MOSCOW).date().isoformat() and prior.get('last_attempt', {}).get('status') != 'FAILED':
            return {'evidence': deepcopy(old), 'anchor': deepcopy(prior.get('anchor')), 'last_attempt': {'status': 'CACHE_REUSED', 'at': now.isoformat(), 'request_count': 0,
                'reason': 'same_date_same_anchor_no_history_request', 'received_bytes': 0, 'elapsed_seconds': 0}}
    except (ValueError, TypeError, KeyError, OverflowError, AttributeError, IndexError):
        pass
    # Recheck the full bounded range daily and on anchor revisions. A bound
    # advance may reuse an independently replayable prefix with week overlap.
    batches = []; full_date = now.astimezone(source.MOSCOW).date().isoformat()
    if isinstance(old, dict) and old.get('full_check_date_moscow') == full_date:
        try:
            prior_latest = prior['anchor']
            replay(old, prior_latest, now)
            if prior_latest['secid'] == latest['secid'] and prior_latest['source_trade_date'] < latest['source_trade_date'] and len(old['batches']) < 8:
                batches = deepcopy(old['batches'])
                start = min(week(end)[0], source._date(prior_latest['source_trade_date']))
        except (ValueError, TypeError, KeyError, OverflowError, AttributeError, IndexError):
            pass
    pages = []; offset = 0; byte_count = 0; request_count = 0
    try:
        source._require(start <= end, 'history has no dates in review range')
        for _ in range(MAX_PAGES):
            source._require(monotonic()-started <= BUDGET_SECONDS, 'history collection budget exceeded')
            url = route(latest['secid'], start.isoformat(), end.isoformat(), offset)
            requested = source._utc(clock()); request_count += 1
            raw = (transport or source._fetch)(url); received = source._utc(clock())
            source._require(isinstance(raw, bytes) and 0 < len(raw) <= source.MAX_BODY_BYTES, 'history raw body size invalid')
            byte_count += len(raw)
            raw_id = immutable(audit_root, 'raw', raw)
            payload = json.loads(raw, parse_constant=str)
            pages.append({'source_route': url, 'requested_at': requested.isoformat(), 'received_at': received.isoformat(),
                'raw_source_id': raw_id, 'raw_body': raw.decode('utf-8'), 'payload_digest': payload_digest(payload), 'payload': payload})
            rows = source._rows(payload, 'history'); cursor = source._rows(payload, 'history.cursor')
            source._require(len(cursor) == 1 and type(cursor[0].get('TOTAL')) is int, 'history cursor missing')
            offset += len(rows)
            if offset >= cursor[0]['TOTAL']: break
            source._require(bool(rows), 'history pagination made no progress')
        accepted = source._utc(clock())
        source._require(monotonic()-started <= BUDGET_SECONDS, 'history collection budget exceeded')
        batch = {'from_date': start.isoformat(), 'till_date': end.isoformat(), 'pages': pages}
        incoming = _page_rows(batch, latest, accepted)
        previous_rows = replay(old, prior['anchor'], now) if batches else []
        combined = [row for row in previous_rows if row['TRADEDATE'] < start.isoformat()] + incoming
        identity = {k: latest[k] for k in ('secid', 'expiry', 'board', 'price_unit', 'source_unit_text')}
        evidence = {'schema_version': SCHEMA, 'identity': identity, 'batches': batches+[batch],
            'source_revision_id': payload_digest({'identity': identity, 'rows': combined}),
            'received_at': pages[-1]['received_at'], 'accepted_at': accepted.isoformat(), 'full_check_date_moscow': full_date}
        if valid_old and old.get('source_revision_id') == evidence['source_revision_id']:
            evidence['first_accepted_at'] = old.get('first_accepted_at', old['accepted_at'])
        else: evidence['first_accepted_at'] = evidence['accepted_at']
        version_bytes = encoded(evidence)
        evidence['audit_version_ref'] = sha256(version_bytes).hexdigest()
        replay(evidence, latest, accepted)
        immutable(audit_root, 'versions', version_bytes)
        return {'evidence': evidence, 'anchor': {k: deepcopy(latest[k]) for k in ('secid', 'expiry', 'board', 'price_unit', 'source_unit_text', 'source_trade_date', 'ohlc')},
            'last_attempt': {'status': 'RECEIVED', 'at': accepted.isoformat(), 'request_count': len(pages), 'received_bytes': byte_count,
                'elapsed_seconds': round(monotonic()-started, 6), 'reason': None}}
    except (ValueError, TypeError, KeyError, OverflowError, AttributeError, IndexError, OSError) as exc:
        return {'evidence': deepcopy(old), 'anchor': deepcopy(prior.get('anchor')),
            'last_attempt': {'status': 'FAILED', 'at': source._utc(clock()).isoformat(), 'request_count': request_count,
                'received_bytes': byte_count, 'reason': str(exc), 'elapsed_seconds': round(monotonic()-started, 6)}}


def describe(context, latest, *, now):
    """Recompute from bounded native evidence at the package clock, without IO."""
    result = {'schema_version': SCHEMA, 'status': 'UNAVAILABLE', 'reason': None,
        'secid': latest.get('secid'), 'expiry': latest.get('expiry'), 'price_unit': 'USD/barrel', 'price_field': 'CLOSE',
        'contract_scope': 'history_of_contract_selected_now_not_historical_front',
        'live_quote': False, 'historical_pit_eligible': False, 'model_usable': False,
        'session_completion_proven': False, 'data_finality_proven': False,
        'as_of_utc': now.isoformat(), 'daily': [], 'weekly': [], 'comparisons': {}}
    try:
        source._require(isinstance(context, dict), 'history context missing')
        result['last_attempt'] = deepcopy(context.get('last_attempt'))
        if context.get('last_attempt', {}).get('status') == 'FAILED':
            try:
                prior = context['evidence']
                replay(prior, context['anchor'], now)
                result['retained_history_evidence'] = {'status': 'NOT_ADMITTED_AFTER_REFRESH_FAILURE',
                    'secid': prior['identity']['secid'], 'source_revision_id': prior['source_revision_id'],
                    'received_at': prior['received_at'], 'accepted_at': prior['accepted_at'], 'audit_version_ref': prior['audit_version_ref']}
            except (ValueError, TypeError, KeyError, OverflowError, AttributeError, IndexError):
                pass
        evidence = context['evidence']; rows = replay(evidence, latest, now)
        source._require(context.get('last_attempt', {}).get('status') != 'FAILED', 'latest_history_refresh_failed: '+str(context.get('last_attempt', {}).get('reason')))
        start, end = bounds(latest, now)
        source._require(evidence['batches'][0]['from_date'] <= start.isoformat(), 'history requested range does not cover review window')
        rows = [r for r in rows if r['TRADEDATE'] >= start.isoformat()]
        entries = []
        for row in rows:
            entry = {'trade_date': row['TRADEDATE'], 'secid': latest['secid'], 'status': 'AVAILABLE', 'reason': None}
            try: entry['ohlc'] = source._ohlc(row)
            except (ValueError, TypeError, OverflowError) as exc: entry.update(status='UNAVAILABLE', reason=str(exc), ohlc=None)
            entries.append(entry)
        result.update(status='AVAILABLE', requested_from_date=start.isoformat(), requested_till_date=end.isoformat(),
            source_range_complete=True, observed_date_count=len(entries), daily=entries[-30:],
            source_revision_id=evidence['source_revision_id'], audit_version_ref=evidence['audit_version_ref'],
            received_at=evidence['received_at'], accepted_at=evidence['accepted_at'], first_accepted_at=evidence['first_accepted_at'],
            last_successful_history_check_at=evidence['received_at'],
            full_range_last_checked_at=evidence['batches'][0]['pages'][-1]['received_at'],
            availability_semantics='current_received_revision_no_past_availability_claim',
            source_requests=[{k: p[k] for k in ('source_route', 'requested_at', 'received_at', 'raw_source_id')} for b in evidence['batches'] for p in b['pages']])
        for lag in (1, 5, 20):
            comparison = {'status': 'UNAVAILABLE', 'reason': 'insufficient_observed_dates', 'observed_transitions': lag,
                'secid': latest['secid'], 'latest_date': latest['source_trade_date'], 'latest_close': latest['price'],
                'reference_date': None, 'reference_close': None, 'change_abs': None, 'change_pct': None,
                'change_abs_unit': 'USD/barrel', 'change_pct_unit': 'percent'}
            if len(entries) > lag:
                reference = entries[-1-lag]; comparison['reference_date'] = reference['trade_date']
                if any(entry['status'] != 'AVAILABLE' for entry in entries[-1-lag:]): comparison['reason'] = 'invalid_source_row_in_observed_lag'
                else:
                    base = reference['ohlc']['close']; price = entries[-1]['ohlc']['close']
                    absolute, percent = price-base, (price/base-1)*100
                    if not math.isfinite(absolute) or not math.isfinite(percent): comparison['reason'] = 'nonfinite_computed_change'
                    else: comparison.update(status='AVAILABLE', reason=None, reference_close=base, change_abs=absolute, change_pct=percent)
            result['comparisons'][str(lag)+'obs'] = comparison
        result['previous_comparable_close'] = deepcopy(result['comparisons']['1obs'])
        current = week(now.astimezone(source.MOSCOW).date())[0]
        for index in range(7, -1, -1):
            first = current-timedelta(weeks=index); last = first+timedelta(days=6)
            observed = [e for e in entries if first.isoformat() <= e['trade_date'] <= last.isoformat()]
            item = {'period_start': first.isoformat(), 'period_end': last.isoformat(), 'is_current_review_week': index == 0,
                'calendar_interval_ended': last < now.astimezone(source.MOSCOW).date(), 'data_finality_proven': False,
                'source_range_complete': start <= first and end >= min(last, now.astimezone(source.MOSCOW).date()-timedelta(days=1)),
                'session_completion_proven': False, 'observed_dates': [e['trade_date'] for e in observed],
                'observed_date_count': len(observed), 'first_observed_date': observed[0]['trade_date'] if observed else None,
                'last_observed_date': observed[-1]['trade_date'] if observed else None,
                'status': 'UNAVAILABLE', 'reason': 'no_source_observations_in_period', 'ohlc': None}
            if observed:
                if any(e['status'] != 'AVAILABLE' for e in observed): item['reason'] = 'invalid_source_row_in_week'
                else: item.update(status='AVAILABLE', reason=None, ohlc={'open': observed[0]['ohlc']['open'],
                    'high': max(e['ohlc']['high'] for e in observed), 'low': min(e['ohlc']['low'] for e in observed), 'close': observed[-1]['ohlc']['close']})
            result['weekly'].append(item)
    except (ValueError, TypeError, KeyError, OverflowError, AttributeError, IndexError) as exc:
        result.update(status='UNAVAILABLE', reason=str(exc), daily=[], weekly=[], comparisons={})
        result['comparisons'] = {str(lag)+'obs': {'status': 'UNAVAILABLE', 'reason': str(exc),
            'observed_transitions': lag, 'secid': latest.get('secid'), 'change_abs': None, 'change_pct': None} for lag in (1,5,20)}
    return result

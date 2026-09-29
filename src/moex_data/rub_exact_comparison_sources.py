"""Bounded exact-target source custody; historical GETs run only during capture.

This supplemental admission does not create accepted Stage3/4 runs. Its frozen
original responses are replayed independently of those legacy admissions.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
import base64
import json
import os

from moex_data import rub_contract_price_market_oi_observed as custody
from moex_data import synchronized_live_market_oi_context as transport

SCHEMA = 'exact_comparison_source.v1'
CONTRACT = 'contracts/intelligence/exact_comparison_sources_v1.json'
CACHE = 'state/evidence/exact_comparison_sources_v1'
MAX_PAGES = 8
MAX_ROWS = 2000
MAX_PAGE_BYTES = 1_000_000
require = custody._require
stamp = custody._stamp
digest = custody.common._digest


class BudgetExceeded(ValueError):
    pass


class Budget:
    def __init__(self, initial=()):
        self.total = 0; self.seen = set(); self.failed = False
        for raw in initial: self.charge(raw)

    def charge(self, raw):
        key = sha256(raw).hexdigest()
        if self.failed or self.total + (0 if key in self.seen else len(raw)) > 16_000_000:
            self.failed = True
            raise BudgetExceeded('exact_source_total_byte_limit')
        if key not in self.seen: self.total += len(raw); self.seen.add(key)

    def source(self, value):
        for page in value['pages']:
            require(custody._encoded_size(page['response_base64']) <= MAX_PAGE_BYTES, 'exact_source_response_byte_limit')
            self.charge(base64.b64decode(page['response_base64'], validate=True))


def contract():
    value = custody._source_object((Path(__file__).resolve().parents[2]/CONTRACT).read_bytes())
    require(value['schema_version'] == 'exact_comparison_sources_admission.v1'
            and value['dated_lifetime_seconds'] == 345600 and value['max_target_dates'] == 7
            and value['max_sources'] == 37 and value['max_pages_per_source'] == MAX_PAGES
            and value['max_rows_per_source'] == MAX_ROWS and value['max_page_bytes'] == MAX_PAGE_BYTES
            and value['max_total_bytes'] == 16_000_000 and value['price_oi_lags'] == [1, 5, 20]
            and value['basis_carry_lags'] == [1, 5] and value['action_authority'] is False,
            'exact_source_contract_invalid')
    return value


def endpoint(secid):
    import re
    if secid == 'CNYRUB_TOM':
        return 'https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/CNYRUB_TOM/candles.json'
    require(secid in ('USDRUBF', 'CNYRUBF') or re.fullmatch(r'(Si|CR)[HMUZ][0-9]', secid) is not None,
            'exact_source_SECID_scope')
    return 'https://apim.moex.com/iss/datashop/algopack/fo/tradestats/'+secid+'.json'


def params(day, secid, start):
    value = {'from': day, 'till': day, 'start': start, 'iss.meta': 'off'}
    if secid == 'CNYRUB_TOM': value['interval'] = 1
    return value


def _page(raw, *, requested, received, url, query, status):
    require(isinstance(raw, bytes) and len(raw) <= MAX_PAGE_BYTES, 'exact_source_response_byte_limit')
    return {'response_base64': base64.b64encode(raw).decode('ascii'), 'response_sha256': sha256(raw).hexdigest(),
            'requested_at_utc': requested.isoformat(), 'received_at_utc': received.isoformat(),
            'source_url': url, 'request_params': query, 'http_status': status}


def acquire(day, secid, *, now_fn, http_get=None, env=None, budget=None):
    """Use the existing authenticated adapter and retain even rejected responses."""
    import requests
    active = os.environ if env is None else env
    budget = Budget() if budget is None else budget
    started = stamp(now_fn()); pages = []; error = None; start = 0
    custody._day(day); endpoint(secid)
    require(0 < (started.astimezone(transport.MOSCOW).date()-datetime.fromisoformat(day).date()).days <= 45,
            'exact_source_completed_date_scope')
    try:
        headers = {'User-Agent': 'moex_bot_step3_cets_tom/1.0'} if secid == 'CNYRUB_TOM' else transport._auth_headers(active)
        for _ in range(MAX_PAGES):
            requested = stamp(now_fn()); query = params(day, secid, start)
            # Capture bytes before the ordinary adapter parses or rejects them.
            def retained_get(url, **kwargs):
                response = (requests.get if http_get is None else http_get)(url, **kwargs)
                budget.charge(response.content)
                pages.append(_page(response.content, requested=requested, received=stamp(now_fn()),
                                   url=str(response.url), query=query, status=response.status_code))
                return response
            payload, _, _ = transport._fetch_json(url=endpoint(secid), params=query, headers=headers,
                timeout=12.0, http_get=retained_get, now_fn=now_fn)
            table = custody._table(payload, 'candles' if secid == 'CNYRUB_TOM' else 'data')
            start += len(table)
            require(start <= MAX_ROWS, 'exact_source_row_limit')
            if secid == 'CNYRUB_TOM':
                if not table: break
            else:
                cursor = custody._table(payload, 'data.cursor')
                require(len(cursor) == 1, 'exact_source_cursor_shape')
                if start >= cursor[0]['TOTAL']: break
                require(bool(table), 'exact_source_pagination_no_progress')
        else: raise ValueError('exact_source_page_limit')
    except BudgetExceeded: raise
    except Exception as exc:
        # Exception text may contain request headers or arbitrary server content.
        error = type(exc).__name__
    completed = stamp(now_fn())
    require(started <= completed, 'exact_source_capture_clock_reversed')
    return {'schema_version': SCHEMA, 'trade_date': day, 'secid': secid,
            'capture_started_at_utc': started.isoformat(), 'capture_completed_at_utc': completed.isoformat(),
            'pages': pages, 'acquisition_error': error}


def retained(root, day, secid, *, now_fn, http_get=None, env=None, budget=None):
    """One bounded cache key per exact source/date. Corruption never triggers fallback."""
    endpoint(secid); custody._day(day)
    path = root/CACHE/(day+'_'+secid+'.json')
    require(not path.is_symlink() and path.resolve().is_relative_to(root.resolve()), 'exact_source_cache_path')
    previous = None; budget = Budget() if budget is None else budget
    if path.exists():
        require(path.stat().st_size <= 12_000_000, 'exact_source_cache_byte_limit')
        previous = custody._source_object(path.read_bytes())
        require(set(previous) == {'source', 'source_sha256', 'last_attempt_at_utc'} and digest(previous['source']) == previous['source_sha256'],
                'exact_source_cache_hash')
        source = previous['source']
        require(stamp(source['capture_completed_at_utc']) <= stamp(previous['last_attempt_at_utc']) <= stamp(now_fn()), 'exact_source_cache_clock')
        require(source['trade_date'] == day and source['secid'] == secid, 'exact_source_cache_identity')
        # A received source revision is immutable; failed transport may be retried.
        if source['acquisition_error'] is None or (stamp(now_fn())-stamp(previous['last_attempt_at_utc'])).total_seconds() < 900:
            budget.source(source)
            return source
    source = acquire(day, secid, now_fn=now_fn, http_get=http_get, env=env, budget=budget)
    last_attempt = source['capture_completed_at_utc']
    if previous is not None and source['acquisition_error'] == previous['source']['acquisition_error'] and not source['pages']:
        source = previous['source']
    encoded = custody._json({'source': source, 'source_sha256': digest(source), 'last_attempt_at_utc':last_attempt}).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    with temporary.open('xb') as handle: handle.write(encoded)
    temporary.replace(path)
    return source


def replay(source, *, day, secid, now):
    """Verify full response custody then parse exactly those verified bytes."""
    require(set(source) == {'schema_version', 'trade_date', 'secid', 'capture_started_at_utc',
            'capture_completed_at_utc', 'pages', 'acquisition_error'} and source['schema_version'] == SCHEMA
            and source['trade_date'] == day and source['secid'] == secid, 'exact_source_envelope_identity')
    started, completed = stamp(source['capture_started_at_utc']), stamp(source['capture_completed_at_utc'])
    require(started <= completed <= stamp(now), 'exact_source_causal_capture')
    require(0 < (started.astimezone(transport.MOSCOW).date()-datetime.fromisoformat(custody._day(day)).date()).days <= 45,
            'exact_source_completed_date_scope')
    pages = source['pages']; require(isinstance(pages, list) and len(pages) <= MAX_PAGES, 'exact_source_page_limit')
    require(source['acquisition_error'] is None or isinstance(source['acquisition_error'], str), 'exact_source_error_shape')
    decoded = []; previous = started
    for page in pages:
        require(set(page) == {'response_base64', 'response_sha256', 'requested_at_utc', 'received_at_utc',
                'source_url', 'request_params', 'http_status'}, 'exact_source_page_shape')
        require(custody._encoded_size(page['response_base64']) <= MAX_PAGE_BYTES, 'exact_source_response_byte_limit')
        raw = base64.b64decode(page['response_base64'], validate=True)
        require(sha256(raw).hexdigest() == page['response_sha256'], 'exact_source_response_hash')
        request, receipt = stamp(page['requested_at_utc']), stamp(page['received_at_utc'])
        require(previous <= request <= receipt <= completed, 'exact_source_request_clock')
        previous = receipt; decoded.append((page, raw, receipt))
    # A complete failure envelope is evidence of a refusal, not a source fact.
    if source['acquisition_error'] is not None:
        raise ValueError('exact_source_acquisition_'+source['acquisition_error'])
    require(bool(pages), 'exact_source_pages_missing')
    rows = []; total = None; seen = set(); spot = secid == 'CNYRUB_TOM'
    for index, (page, raw, receipt) in enumerate(decoded):
        require(page['http_status'] == 200, 'exact_source_http_'+str(page['http_status']))
        expected = params(day, secid, len(rows)); url = urlsplit(page['source_url']); route = urlsplit(endpoint(secid))
        require((url.scheme, url.netloc, url.path) == (route.scheme, route.netloc, route.path)
                and not url.fragment and page['request_params'] == expected
                and parse_qs(url.query) == {k: [str(v)] for k,v in expected.items()}, 'exact_source_request_identity')
        payload = custody._source_object(raw)
        require(not payload.get('ERROR_MESSAGE'), 'exact_source_ERROR_MESSAGE')
        table = custody._table(payload, 'candles' if spot else 'data')
        if spot:
            require(index == len(decoded)-1 or bool(table), 'exact_source_early_terminal_page')
            require(index != len(decoded)-1 or not table, 'exact_source_missing_terminal_page')
        else:
            cursor = custody._table(payload, 'data.cursor'); require(len(cursor) == 1, 'exact_source_cursor_shape')
            c = cursor[0]
            require(set(c) == {'INDEX', 'TOTAL', 'PAGESIZE'} and all(type(v) is int and v >= 0 for v in c.values()), 'exact_source_cursor_values')
            if total is None: total = c['TOTAL']
            require(total <= MAX_ROWS and c['TOTAL'] == total and c['INDEX'] == len(rows)
                    and (c['PAGESIZE'] > 0 or total == 0) and len(table) == min(c['PAGESIZE'], total-len(rows)), 'exact_source_cursor_inventory')
        for row in table:
            if spot:
                end = datetime.fromisoformat(row['END']); begin = datetime.fromisoformat(row['BEGIN'])
                require(end.tzinfo is None and begin.tzinfo is None and end.date().isoformat() == day
                        and begin.date().isoformat() == day and timedelta(0) <= end-begin < timedelta(minutes=1), 'exact_CETS_date_or_interval')
                event = end.replace(tzinfo=transport.MOSCOW).astimezone(timezone.utc)
            else:
                require(row['SECID'] == secid and row['TRADEDATE'] == day, 'exact_source_row_identity')
                local = datetime.fromisoformat(day+'T'+row['TRADETIME'])
                require(local.tzinfo is None and not local.second and not local.microsecond and local.minute % 5 == 0, 'exact_source_bar_grid')
                event = local.replace(tzinfo=transport.MOSCOW).astimezone(timezone.utc)
            require(event not in seen, 'exact_source_duplicate_bar'); seen.add(event)
            require(event <= receipt, 'exact_source_event_after_receipt')
            rows.append({'event': event, 'raw': row, 'receipt': receipt, 'response_sha256': page['response_sha256']})
        require(len(rows) <= MAX_ROWS, 'exact_source_row_limit')
    require(spot or len(rows) == total, 'exact_source_incomplete_pagination')
    if not rows: raise ValueError('exact_source_EMPTY')
    return sorted(rows, key=lambda r: r['event'])


def quote(row, *, spot=False):
    raw = row['raw']; prefix = '' if spot else 'PR_'
    values = {k: raw[prefix+k.upper()] for k in ('open', 'high', 'low', 'close')}
    custody._ohlc(values)
    publication = None
    if not spot:
        publication = datetime.fromisoformat(raw['SYSTIME'])
        if publication.tzinfo is None: publication = publication.replace(tzinfo=transport.MOSCOW)
        publication = publication.astimezone(timezone.utc)
        require(row['event'] <= publication <= row['receipt'], 'exact_source_publication_clock')
    return values['close'], publication


def price_pair(rows, *, day, secid, source):
    # Select the latest observation first. Bad latest OI must not expose older OI.
    row = rows[-1]; price, publication = quote(row)
    require(all(not isinstance(row['raw'][k], float) or abs(row['raw'][k]) <= 2**53
                for k in ('OI_OPEN','OI_HIGH','OI_LOW','OI_CLOSE')), 'exact_source_float_OI_precision_unproven')
    custody._ohlc({k.lower(): row['raw'][k] for k in ('OI_OPEN', 'OI_HIGH', 'OI_LOW', 'OI_CLOSE')}, 'oi_')
    oi = custody._number(row['raw']['OI_CLOSE'], integer=True)
    return custody._pair(secid, day, row['event'].isoformat(), publication.isoformat(), row['receipt'].isoformat(),
        price, int(oi), {'source_envelope_sha256': digest(source), 'response_sha256': row['response_sha256']},
        source_kind='EXACT_TARGET_OFFICIAL_TRADESTATS_V1')

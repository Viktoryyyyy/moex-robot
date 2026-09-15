"""One read-only acceptance gate for a factual release; never a forecast grant."""
import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import tempfile
from urllib.parse import urlsplit

from moex_data import rub_factual_release as release
from moex_data.rub_macro_evidence_inventory import DENIED
from moex_data.rub_snapshot_read_freshness import apply_read_freshness

REQUIRED_FACTORS = ('rosstat_monthly_cpi', 'cbr_liquidity_verified')
AUTHORITY = ('session_completion_proven', 'historical_dataset_accepted',
             'model_validated', 'training_authorized', 'broker_execution')
ADMISSION = {
    'basis_carry': ('basis_carry', 'factual_context_usable'),
    'rosstat_monthly_cpi': ('rosstat_macro', 'monthly_cpi_factual_context_usable'),
    'cbr_liquidity_verified': ('cbr_rates', 'liquidity_factual_context_usable'),
    'rosstat_cpi': ('rosstat_macro', 'factual_context_usable'),
    'cbr_rates_verified': ('cbr_rates', 'factual_context_usable'),
    'external_cny': ('external_cny', 'factual_context_usable'),
    'oil': ('brent', 'factual_context_usable'),
}


def _require(condition, message):
    if not condition:
        raise AssertionError(message)


def _factors(value):
    return {row['factor'] for row in value['facts']}


def _contract_source_keys(evidence, now):
    """Independent eligible-key oracle, including an all-output omission check."""
    from collections import Counter
    from datetime import date
    from math import isfinite
    import re
    try:
        if evidence['schema_version'] != 'rub_contract_observed_dates.v1': return None
        rows = evidence['rows']; capture = datetime.fromisoformat(evidence['captured_at_utc'])
        if not isinstance(rows, list) or len(rows) > 240 or not capture <= now: return None
        capture_date = capture.astimezone(timezone(timedelta(hours=3))).date()
        keys = []; roles = {}
        for row in rows:
            secid = row['secid']; day = date.fromisoformat(row['source_date'])
            if not re.fullmatch(r'(Si|CR)[HMUZ][0-9]', secid): return None
            family = 'Si' if secid.startswith('Si') else 'CR'
            binding = row['binding_at_source']; provenance = row['source_provenance']
            role = binding['role']; instrument = family.lower()+'_'+role+'_contract'
            ref = datetime.fromisoformat(row['binding_observed_at_utc'])
            first, last, low, high, finished = [datetime.fromisoformat(row[key]) for key in
                ('source_first_bar_at_utc', 'source_last_bar_at_utc', 'source_receipt_lower_bound_utc',
                 'source_receipt_upper_bound_utc', 'parent_finished_at_utc')]
            expiry = date.fromisoformat(binding['last_trade_date'])
            if not (role in ('front', 'next') and binding['root'] == family
                    and binding['secid'] == provenance['secid'] == secid
                    and binding['instrument_id'] == provenance['instrument_id'] == row['instrument_id_at_source'] == instrument
                    and binding['source_id'] == 'moex_iss_forts_securities_reference'
                    and date.fromisoformat(binding['as_of_date']) == day <= expiry
                    and datetime.fromisoformat(binding['mapping_fixed_ts_utc']) == ref
                    == datetime.fromisoformat(binding['availability_ts_utc'])
                    == datetime.fromisoformat(provenance['binding_availability_ts_utc'])
                    and capture_date-timedelta(days=45) <= day <= capture_date
                    and ref.astimezone(timezone(timedelta(hours=3))).date() >= day
                    and first <= last <= high <= finished <= capture and first <= low <= high and ref <= low
                    and first.astimezone(timezone(timedelta(hours=3))).date() == last.astimezone(timezone(timedelta(hours=3))).date() == day): return None
            if any(type(row[key]) not in (int, float) or not isfinite(row[key]) or row[key] <= 0 for key in ('open','high','low','close')): return None
            if (type(row['volume']) not in (int,float) or not isfinite(row['volume']) or row['volume'] < 0
                    or not row['low'] <= min(row['open'], row['close']) <= max(row['open'], row['close']) <= row['high']
                    or type(row['source_bar_count']) is not int or row['source_bar_count'] <= 0
                    or type(row['intraday_gap_count']) is not int or not 0 <= row['intraday_gap_count'] < row['source_bar_count']
                    or row['session_completion_proven'] is not False): return None
            if (provenance['source_id'] != 'moex_algopack_fo_tradestats_5m' or provenance['dataset_id'] != 'futures_raw_5m'
                    or provenance['acceptance_contract_id'] != 'step3_canonical_raw_acceptance.v1'
                    or provenance['quality_status'] != 'pass' or provenance['refresh_status'] != 'succeeded'
                    or provenance['hash_semantics'] != 'computed_at_current_revalidation_not_original_acceptance_digest'): return None
            run = provenance['acceptance_run_id']; root = '${MOEX_DATA_ROOT}/'
            if not re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]*_stage3', run) or not re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]*', provenance['run_id']): return None
            if (provenance['accepted_marker_ref'] != root+'state/acceptance/step3_canonical_raw/run_id='+run+'/accepted_pointers.json'
                    or provenance['parent_manifest_ref'] != root+'runs/step10_rub_daily_refresh/run_id='+run[:-7]+'/run_manifest.json'): return None
            prefix = root+'runs/step3_canonical_raw/run_id='+run+'/'
            for field in ('partition_ref','manifest_ref','quality_report_ref'):
                path = provenance[field]
                if not path.startswith(prefix) or '\\' in path or any(p in ('','.','..') for p in path[len(prefix):].split('/')): return None
            for field in ('partition_sha256','manifest_sha256','quality_report_sha256',
                          'accepted_marker_sha256_at_revalidation','pilot_evidence_sha256_at_revalidation','parent_manifest_sha256_at_revalidation'):
                if not re.fullmatch('[0-9a-f]{64}', provenance[field]): return None
            group = roles.setdefault((run, day, family), {})
            if role in group: return None
            group[role] = expiry
            keys.append((secid, row['source_date']))
        if len(keys) != len(set(keys)) or any(count > 30 for count in Counter(key[0] for key in keys).values()): return None
        if any('front' in group and 'next' in group and group['front'] >= group['next'] for group in roles.values()): return None
        return Counter(keys)
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        return None


def _fx_arithmetic_completeness(block, *, now):
    """Independent arithmetic oracle over retained source rows, not output counts."""
    evidence = block.get('observed_context_evidence')
    if evidence is None:
        return
    context = block.get('observed_context')
    _require(isinstance(context, dict), 'FX observed context omitted')
    # Validate contracted build clocks independently of the shared descriptor.
    # A malformed retained row invalidates the window; it cannot shorten a lag.
    try:
        build_times = [datetime.fromisoformat(row['build_ts_utc']) for row in evidence['rows']]
        if any(stamp.utcoffset() is None for stamp in build_times):
            raise ValueError('timezone required')
    except (KeyError, TypeError, ValueError, OverflowError):
        _require(context.get('status') == 'UNAVAILABLE'
                 and context.get('observations') == [] and context.get('comparisons') == {},
                 'FX required build timestamp refusal')
        return
    # The separately retained selected row witnesses the common Stage7 build.
    # Do not delegate this test to the producer or shared describe/apply helper.
    try:
        selected_build = datetime.fromisoformat(block['selected_observation']['build_ts_utc'])
        build_matches = selected_build.utcoffset() is not None and all(
            stamp == selected_build for stamp in build_times)
    except (KeyError, TypeError, ValueError, OverflowError):
        build_matches = False
    if not build_matches:
        _require(context.get('status') == 'UNAVAILABLE'
                 and context.get('observations') == [] and context.get('comparisons') == {},
                 'FX retained build witness refusal')
        return
    if context.get('status') != 'AVAILABLE':
        return
    timeframe = block['timeframe']
    rows = [row for row, built in zip(evidence['rows'], build_times)
            if datetime.fromisoformat(row['availability_ts_utc']) <= now and built <= now]
    _require(bool(rows) and context['observations'] == rows and len(rows) <= (30 if timeframe == '1D' else 8),
             'FX bounded causal observations')
    _require(context['historical_pit_usable'] is False and context['session_completion_proven'] is False
             and context['first_accepted_at_utc'] is None, 'FX scope and unknown first acceptance')
    lags = (1, 5, 20) if timeframe == '1D' else (1,)
    _require(set(context['comparisons']) == {str(lag) for lag in lags}, 'FX exact lag coverage')
    for lag in lags:
        comparison = context['comparisons'][str(lag)]
        if len(rows) <= lag:
            _require(comparison['status'] == 'UNAVAILABLE', 'FX insufficient lag refusal')
            continue
        first, last = rows[-1-lag], rows[-1]
        _require(comparison['status'] == 'AVAILABLE'
                 and comparison['target_date'] == first['period_end_date']
                 and comparison['source_date'] == last['period_end_date']
                 and comparison['target_close'] == first['close']
                 and comparison['source_close'] == last['close']
                 and comparison['absolute_change'] == last['close'] - first['close']
                 and comparison['percent_change'] == (last['close'] / first['close'] - 1) * 100
                 and comparison['observed_date_witness'] == [row['period_end_date'] for row in rows[-1-lag:]],
                 'FX exact observed lag arithmetic and witness')
    if timeframe == '1D':
        today = now.astimezone(timezone(timedelta(hours=3))).date()
        monday = today - timedelta(days=today.weekday())
        part = [row for row in rows if monday.isoformat() <= row['trade_date'] <= today.isoformat()]
        wtd = context['week_to_date']
        _require(wtd['source_dates'] == [row['trade_date'] for row in part]
                 and wtd['source_period_count'] == len(part)
                 and wtd['week_start_date'] == monday.isoformat()
                 and wtd['as_of_moscow_date'] == today.isoformat()
                 and wtd['week_completion_proven'] is False, 'FX dynamic WTD dates and scope')
        if part:
            _require([wtd[key] for key in ('open', 'high', 'low', 'close')] == [part[0]['open'],
                max(row['high'] for row in part), min(row['low'] for row in part), part[-1]['close']], 'FX WTD OHLC')


def projection_completeness(snapshot, value, *, now):
    """Independent reverse oracle over the read-time input, not exported fact counts."""
    now = now.astimezone(timezone.utc)
    from moex_data.rub_si_futoi_dated_context import verify_projection as verify_si_dated
    verify_si_dated(snapshot, value, now=now)
    from moex_data.rub_si_futoi_observed_statistics import verify_projection as verify_si_statistics
    verify_si_statistics(snapshot, value, now=now)
    from math import isfinite
    view = apply_read_freshness(snapshot, now=now)
    components = view.get('components', {})
    from moex_research.external_data.brent_daily_context import describe as oil_context
    from moex_research.external_data.moex_brent_factual import factual_usable as oil_usable
    oil_component = components.get('oil', {})
    oil = oil_component.get('data') or {}
    oil_facts = [fact for fact in value['facts'] if fact['factor'] == 'oil']
    _require(len(oil_facts) == int(oil_usable(oil_component)), 'oil fact cardinality must match reconciled admission')
    if oil_facts:
        _require(oil_facts[0]['values'].get('daily_weekly_context') == oil_context(oil.get('daily_weekly_context'), oil, now=now), 'oil history bidirectional projection completeness')
    market = components.get('synchronized_live_market_oi', {}).get('data', {})
    spot = market.get('instruments', {}).get('cnyrub_tom', {})
    price = spot.get('last')
    expected_spot = (market.get('quality', {}).get('spot_price_usable') is True
        and spot.get('spot_price_usable') is not False and spot.get('stale') is False
        and isinstance(price, (int, float)) and not isinstance(price, bool) and isfinite(price) and price > 0)
    facts = {fact['factor']: fact for fact in value['facts']}
    rows = {row['block_id']: row for row in value['matrix']}
    _require(('cnyrub_tom' in facts) == expected_spot == rows['cnyrub_tom']['usable_for_full_forecast'], 'spot completeness')
    if expected_spot: _require(facts['cnyrub_tom']['values']['last'] == price, 'spot values')
    fields = ('last', 'oi', 'open', 'high', 'low', 'close', 'volume', 'trades', 'units',
        'price_unit', 'quote_unit', 'oi_unit', 'expiry_date', 'expiry_metadata', 'contract_size',
        'price_scale', 'normalization', 'wap', 'wap_method')
    for key, item in market.get('instruments', {}).items():
        quote_allowed = item.get('quote_usable') is True and item.get('stale') is False
        quote = value['market_usability'][key]['quote']
        _require((quote is not None) == quote_allowed, 'independent quote completeness ' + key)
        if quote_allowed: _require(quote['values'] == {field: item.get(field) for field in ('bid', 'ask', 'spread')}, 'quote values')
        matching = []
        basis_component = components.get('live_basis_carry', {})
        try:
            metadata_causal = all(0 <= (now - datetime.fromisoformat(item[field])).total_seconds() <= 96 * 3600
                                  for field in ('timestamp', 'received_at_utc'))
        except (KeyError, TypeError, ValueError): metadata_causal = False
        expected_metadata_source = 'moex_apim_cets_cnyrub_tom_live_marketdata' if key == 'cnyrub_tom' else 'moex_apim_forts_rfud_live_marketdata'
        metadata_source_allowed = key in ('usdrubf', 'si_front', 'si_next', 'cnyrubf', 'cr_front', 'cr_next', 'cnyrub_tom') and item.get('source_id') == expected_metadata_source
        if metadata_causal and metadata_source_allowed:
            for pair in (basis_component.get('data') or {}).get('pairs', {}).values():
                leg = pair.get('legs', {}).get(key, {})
                metadata_status = leg.get('status') == 'READY' or (leg.get('status') == 'UNAVAILABLE' and
                    leg.get('unavailable_reason') in ('source_leg_stale', 'source_leg_freshness_exceeds_threshold', 'source_not_fresh_at_read'))
                if metadata_status and leg.get('raw_value') == item.get('last') and all(
                    item.get(field) is not None and leg.get(field) == item[field] for field in ('secid', 'timestamp', 'received_at_utc', 'source_id')):
                    matching.append({field: leg[field] for field in ('raw_unit', 'normalization_divisor', 'normalized_unit', 'expiry_date', 'expiry_metadata') if field in leg})
        expected_metadata = matching[0] if matching and all(item == matching[0] for item in matching) else None
        actual_metadata = value['market_usability'][key]['contract_metadata']
        _require((actual_metadata or {}).get('values') == expected_metadata, 'contract metadata completeness ' + key)
        usable = expected_spot if key == 'cnyrub_tom' else item.get('price_oi_usable') is True
        _require((key in facts) == usable, 'market completeness ' + key)
        if not usable: continue
        admitted_fields = fields + (('bid', 'ask', 'spread') if item.get('quote_usable') is True else ())
        expected_values = {name: item[name] for name in admitted_fields if name in item and not (key == 'cnyrub_tom' and name == 'oi')}
        _require(facts[key]['values'] == expected_values, 'market values ' + key)
    structure = components.get('live_market_structure', {})
    levels = (structure.get('data') or {}).get('structural_levels', {})
    try:
        age = (now - datetime.fromisoformat(levels['data_as_of'])).total_seconds()
        allowed = structure.get('status') == 'READY' and levels.get('status') == 'FRESH' and 0 <= age <= 1200
    except (KeyError, ValueError, TypeError): allowed = False
    _require((value['market_structure']['status'] == 'AVAILABLE') == allowed, 'structure completeness')
    if allowed:
        actual_levels = value['market_structure']['values']
        for key in ('active_levels', 'level_interactions', 'price_context', 'methodology'):
            _require(actual_levels.get(key) == levels.get(key), 'structure values ' + key)
        _require('prior_completed_session' not in actual_levels.get('observed_extrema', {}), 'no unproven completed session')
        expected_extrema = deepcopy(levels.get('observed_extrema', {}))
        if 'prior_completed_session' in expected_extrema:
            old = expected_extrema.pop('prior_completed_session')
            expected_extrema['prior_observed_date'] = {key: item for key, item in old.items() if key != 'partial_session'}
            expected_extrema['prior_observed_date'].update(session_completion_proven=False, session_completion_state='UNKNOWN')
        _require(actual_levels.get('observed_extrema', {}) == expected_extrema, 'observed extrema completeness and values')
        source_data = structure.get('data') or {}
        deterministic = value['market_structure']['deterministic_context']
        for key in ('market_regime',):
            _require(deterministic.get(key) == source_data.get(key), 'deterministic structure context')
        ema = source_data.get('ema_3_19') or {}; details = ema.get('details') or {}
        try: causal_ema = datetime.fromisoformat(ema['available_at']) <= now
        except (KeyError, ValueError, TypeError): causal_ema = False
        expected_ema = ema.get('quality_status') == 'OK' and causal_ema and all(
            isinstance(details.get(key), (int, float)) and not isinstance(details.get(key), bool) and isfinite(details[key])
            for key in ('ema_fast', 'ema_slow'))
        actual_ema = deterministic['ema_3_19']
        _require(deterministic.get('trend') == (source_data.get('trend') if expected_ema else None), 'EMA-derived trend admission')
        _require((actual_ema['status'] == 'AVAILABLE') == expected_ema, 'EMA independent admission')
        if expected_ema:
            _require(actual_ema['values'] == {key: details[key] for key in ('ema_fast', 'ema_slow', 'bar_count', 'source') if key in details}
                and actual_ema['available_at'] == ema['available_at'], 'EMA values and date')
            _require(actual_ema['relation'] == ema.get('direction') and actual_ema['standalone_directional_authority'] is False,
                'EMA relation and scope')
        else:
            _require(actual_ema == {'status': 'UNAVAILABLE', 'reason': 'ema_quality_values_or_causal_time_not_admitted'},
                'EMA excluded values')
        _require('confidence' not in actual_ema, 'EMA no model probability')
    position = snapshot.get('user_position_context')
    position = position if isinstance(position, dict) else {}
    price = position.get('average_entry_price'); direction = position.get('direction')
    try: dated = datetime.fromisoformat(position['user_input_updated_at']) <= now
    except (KeyError, TypeError, ValueError): dated = False
    valid_position = (position.get('status') == 'AVAILABLE' and position.get('explicit_user_input') is True
        and position.get('instrument') == 'USDRUBF' and dated and (
            direction == 'FLAT' and price is None or direction in ('LONG', 'SHORT')
            and isinstance(price, (int, float)) and not isinstance(price, bool) and isfinite(price) and price > 0))
    if valid_position:
        expected_position = {key: position[key] for key in ('instrument', 'direction', 'average_entry_price', 'user_input_updated_at')}
        expected_position.update(status='AVAILABLE', explicit_user_input=True)
    else:
        invalid = position.get('explicit_user_input') is True or position.get('availability') == 'INVALID_EXPLICIT_USER_INPUT'
        expected_position = {'status': 'UNAVAILABLE', 'availability': 'INVALID_EXPLICIT_USER_INPUT' if invalid else 'NO_EXPLICIT_USER_INPUT',
            'direction': None, 'average_entry_price': None, 'explicit_user_input': False}
    _require(value.get('user_position_context') == expected_position, 'explicit position completeness and exclusions')
    expected_blocks = {}
    from moex_data.rub_contract_observed_context import describe as describe_contract_dates
    contract_evidence = None
    for name in ('stage9_daily', 'stage9_weekly'):
        component = components.get(name, {})
        core = (component.get('data') or {}).get('server_core', {})
        if component.get('status') == 'READY' and 'contract_price_evidence' in core:
            contract_evidence = core['contract_price_evidence']
            break
    expected_contracts = describe_contract_dates(contract_evidence, now=now)
    _require(value.get('contract_price_context') == expected_contracts, 'contract observed date completeness')
    from collections import Counter
    source_keys = _contract_source_keys(contract_evidence, now)
    emitted_keys = Counter((row['secid'], row['source_date']) for contract in expected_contracts['contracts'] for row in contract['observations'])
    _require(emitted_keys == (source_keys or Counter()), 'contract independent source-to-output completeness')
    if expected_contracts['status'] == 'AVAILABLE':
        # Independent retained-binding/capture oracle: do not trust shared describe
        # to establish admissibility of its own output.
        from datetime import date
        captured = datetime.fromisoformat(contract_evidence['captured_at_utc'])
        capture_day = captured.astimezone(timezone(timedelta(hours=3))).date()
        _require(captured <= now, 'contract independent capture causality')
        for contract in expected_contracts['contracts']:
            for row in contract['observations']:
                binding = row['binding_at_source']; provenance = row['source_provenance']
                observed_date = date.fromisoformat(row['source_date'])
                family = 'Si' if contract['secid'].startswith('Si') else 'CR'
                role = 'front' if row['instrument_id_at_source'].endswith('_front_contract') else 'next'
                reference = datetime.fromisoformat(row['binding_observed_at_utc'])
                first = datetime.fromisoformat(row['source_first_bar_at_utc'])
                last = datetime.fromisoformat(row['source_last_bar_at_utc'])
                receipt_min = datetime.fromisoformat(row['source_receipt_lower_bound_utc'])
                receipt_max = datetime.fromisoformat(row['source_receipt_upper_bound_utc'])
                finished = datetime.fromisoformat(row['parent_finished_at_utc'])
                _require(binding['root'] == family and binding['role'] == role
                    and row['secid'] == binding['secid'] == provenance['secid'] == contract['secid']
                    and binding['instrument_id'] == row['instrument_id_at_source'] == provenance['instrument_id']
                    == family.lower() + '_' + role + '_contract'
                    and binding['source_id'] == 'moex_iss_forts_securities_reference'
                    and date.fromisoformat(binding['as_of_date']) == observed_date
                    and date.fromisoformat(binding['last_trade_date']) >= observed_date
                    and datetime.fromisoformat(binding['mapping_fixed_ts_utc']) == reference
                    == datetime.fromisoformat(binding['availability_ts_utc'])
                    == datetime.fromisoformat(provenance['binding_availability_ts_utc'])
                    and reference.astimezone(timezone(timedelta(hours=3))).date() >= observed_date,
                    'contract independent retained binding identity and dates')
                _require(capture_day - timedelta(days=45) <= observed_date <= capture_day
                    and first <= last <= receipt_max <= finished <= captured
                    and first <= receipt_min <= receipt_max and reference <= receipt_min,
                    'contract independent capture window and causal bounds')
            for week in contract['observed_weeks']:
                originals = [row for row in contract['observations'] if row['source_date'] in week['source_dates']]
                _require(len(originals) == len(week['source_dates']) and [week[k] for k in ('open', 'high', 'low', 'close')] ==
                    [originals[0]['open'], max(row['high'] for row in originals), min(row['low'] for row in originals), originals[-1]['close']],
                    'contract sparse weekly arithmetic')
    for name in ('stage9_daily', 'stage9_weekly'):
        component = components.get(name, {})
        if component.get('status') != 'READY': continue
        for block in (component.get('data') or {}).get('server_core', {}).get('blocks', []):
            try:
                selected_clock = datetime.fromisoformat(block['selected_causal_ts_utc'])
                build_clock = datetime.fromisoformat(block['selected_observation']['build_ts_utc'])
                causal = (selected_clock.utcoffset() is not None and build_clock.utcoffset() is not None
                          and selected_clock <= now and build_clock <= now)
            except (KeyError, ValueError, TypeError, AttributeError, OverflowError): causal = False
            if block.get('status') == 'ready' and block.get('stage') == 7 and block.get('timeframe') in ('1H', '1D', '1W') and causal:
                _fx_arithmetic_completeness(block, now=now)
                expected_block = deepcopy(block)
                expected_block.pop('observed_context_evidence', None)
                expected_blocks.setdefault((block.get('block_id'), block.get('selected_causal_ts_utc')), expected_block)
    actual_blocks = {(entry['values'].get('block_id'), entry['values'].get('selected_causal_ts_utc')): entry['values'] for entry in value['timeframe_context']}
    # Independent source-hour oracle: do not ask the projection's admission helper.
    hour = None
    try:
        component = components['live_market_structure']; data = component['data']; evidence = data['hourly_observation']
        bars = evidence['source_bars']; ends = [datetime.fromisoformat(row['end']).astimezone(timezone.utc) for row in bars]
        end = ends[-1]; start = end - timedelta(hours=1)
        numeric = all(isinstance(row[key], (int, float)) and not isinstance(row[key], bool)
                      and isfinite(row[key]) and (row[key] >= 0 if key == 'volume' else row[key] > 0)
                      for row in bars for key in ('open', 'high', 'low', 'close', 'volume'))
        ohlc = all(row['low'] <= min(row['open'], row['close']) <= max(row['open'], row['close']) <= row['high'] for row in bars)
        from zoneinfo import ZoneInfo
        dates = {item.astimezone(ZoneInfo('Europe/Moscow')).date().isoformat() for item in ends}
        receipt = datetime.fromisoformat(evidence['receipt_upper_bound_utc'])
        allowed_hour = (component.get('status') == 'READY' and evidence.get('schema_version') == 'rub_observed_clock_hour.v1'
            and evidence.get('status') == 'AVAILABLE' and data.get('instrument') == data.get('requested_secid') == evidence.get('requested_secid') == 'USDRUBF'
            and data.get('source_id') == evidence.get('source_id') == 'moex_algopack_fo_tradestats_5m'
            and data.get('source_contract_ref') == evidence.get('source_contract_ref') == 'contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml'
            and evidence.get('receipt_semantics') == 'current_loader_return_upper_bound'
            and len(bars) == 12 and not end.minute and not end.second and not end.microsecond
            and ends == [start + timedelta(minutes=5 * index) for index in range(1, 13)]
            and dates == {data.get('trade_date')} and end <= receipt <= now and 0 <= (now - end).total_seconds() <= 96 * 3600
            and numeric and ohlc and all(set(row) == {'end', 'open', 'high', 'low', 'close', 'volume'} for row in bars))
        expected_hour = {'instrument': 'USDRUBF', 'timeframe': '1H', 'timezone': 'UTC',
            'hour_start_utc': start.isoformat(), 'hour_end_utc': end.isoformat(), 'source_observed_moscow_date': next(iter(dates)),
            'source_bar_count': 12, 'values': {'open': bars[0]['open'], 'high': max(row['high'] for row in bars),
                'low': min(row['low'] for row in bars), 'close': bars[-1]['close'], 'volume': sum(row['volume'] for row in bars)},
            'scope': 'complete_observed_clock_hour_not_accepted_HTF_dataset_or_session_completion', 'session_completion_proven': False}
        if allowed_hour and evidence.get('observation') == expected_hour:
            hour = {**expected_hour, 'source_id': evidence['source_id'], 'requested_secid': 'USDRUBF',
                'receipt_upper_bound_utc': receipt.isoformat(), 'receipt_semantics': evidence['receipt_semantics']}
            from moex_data.rub_consumption_clock import hour as project_hour_clock
            project_hour_clock(hour, now)
    except (KeyError, TypeError, ValueError, IndexError, AttributeError): pass
    if hour is not None:
        expected_blocks[('observed_1H.USDRUBF', hour['hour_end_utc'])] = {'block_id': 'observed_1H.USDRUBF',
            'selected_causal_ts_utc': hour['hour_end_utc'], 'selected_causal_time_semantics': 'observed_hour_end_not_availability', **hour}
    from moex_data.rub_dated_context import validated as validated_hour_witnesses
    hour_witnesses, _ = validated_hour_witnesses(view.get('accepted_dated_slow'), now)
    for secid in ('USDRUBF', 'CNYRUBF'):
        acquired_hour = hour_witnesses.get('timeframe:observed_1H.' + secid)
        if acquired_hour and acquired_hour[1].get('origin') == 'source_observation_acquired_now':
            hour = deepcopy(acquired_hour[2]['values'])
            from moex_data.rub_consumption_clock import hour as project_hour_clock
            project_hour_clock(hour, now)
            key = ('observed_1H.' + secid, hour['hour_end_utc'])
            if key not in expected_blocks:
                entries = [entry for entry in value['timeframe_context'] if
                           (entry['values'].get('block_id'), entry['values'].get('selected_causal_ts_utc')) == key]
                _require(len(entries) == 1 and all(entries[0].get(name) == expected for name, expected in
                    {'origin': acquired_hour[1]['origin'], 'accepted_at_utc': acquired_hour[1]['accepted_at_utc'],
                     'acceptance_evidence_id': acquired_hour[0], 'revision_id': acquired_hour[1]['revision_id']}.items()),
                    'dated H1 timeframe original acceptance evidence')
            expected_blocks.setdefault(key, {'block_id': key[0], 'selected_causal_ts_utc': key[1],
                'selected_causal_time_semantics': 'observed_hour_end_not_availability', **hour})
    _require(actual_blocks == expected_blocks, 'timeframe completeness')
    from moex_data.rub_dated_context import describe as dated_context
    _require(value.get('dated_context') == dated_context(view, now=now), 'dated witness projection completeness and values')
    from moex_data.rub_dated_context import validated as validated_witnesses
    expected_dated = {}
    for field in ('accepted_dated_slow', 'accepted_dated_market'):
        admitted, _ = validated_witnesses(view.get(field), now)
        expected_dated.update(admitted)
    actual_dated = value['dated_context']['observations']
    range_evidence = expected_dated.get('structure:observed_range_levels.USDRUBF')
    actual_range = value.get('observed_range_levels', {})
    if range_evidence:
        ref, frame, source, times, ages = range_evidence
        _require(actual_range == {'status': 'AVAILABLE', 'values': source['values'], 'origin': frame['origin'],
            'accepted_at_utc': frame['accepted_at_utc'], 'acceptance_evidence_id': ref, 'revision_id': frame['revision_id'],
            'source_times': times, 'ages': ages, 'checked_at_utc': now.isoformat(), 'raw_source_digest': frame['raw_source_digest'],
            'current_usable': False, 'historical_pit_usable': False, 'model_usable': False}, 'observed range exact reverse projection')
    else:
        _require(actual_range.get('status') == 'UNAVAILABLE' and 'values' not in actual_range, 'observed range cannot invent evidence')
    _require(set(actual_dated) == set(expected_dated), 'dated reverse factor completeness')
    for key, (ref, frame, source, times, ages) in expected_dated.items():
        item = actual_dated[key]
        expected = deepcopy(source)
        if key.startswith('market:'):
            allowed_fields = fields + (('bid', 'ask', 'spread') if source.get('quote_usable') is True else ())
            expected = {name: source[name] for name in allowed_fields if name in source and not (key == 'market:cnyrub_tom' and name == 'oi')}
            _require(item['source_identity']['secid'] == source['secid'], 'dated exact SECID')
        elif key.startswith('basis:'):
            from moex_data.rub_consumption_clock import metric as project_metric_clock
            if frame.get('origin') == 'source_observation_acquired_now':
                from moex_data.rub_dated_market_source import replay
                instruments = {leg: replay(evidence) for leg, evidence in frame['leg_evidence'].items()}
                project_metric_clock(expected, instruments, now)
            else:
                project_metric_clock(expected, frame['components']['synchronized_live_market_oi']['data']['instruments'], now)
                expected['status_semantics'] = 'original_accepted_derivation_not_current_admission'
        elif key == 'structure':
            from moex_data.rub_consumption_clock import structure as project_structure_clock
            project_structure_clock(expected['values'], now)
        elif key.startswith('timeframe:'):
            from moex_data.rub_consumption_clock import hour as project_hour_clock
            project_hour_clock(expected['values'], now)
        _require(item['values'] == expected and item['acceptance_evidence_id'] == ref
                 and item['accepted_at_utc'] == frame['accepted_at_utc'] and item['source_times'] == times
                 and item['ages'] == ages and item['current_usable'] is False, 'dated reverse values and admission')
    _require(value['futoi_context']['futoi_live_cr']['previous_observation'] is None
        and value['futoi_context']['futoi_live_cr']['comparisons'] is None, 'CR no history grant')
    si_component = components.get('futoi_live', {}); si = si_component.get('data') or {}
    admission = si_component.get('status') == 'READY' and si.get('consumer_factual_use_allowed') is True and si.get('factual_authority') is True and si.get('governance', {}).get('factual_use_allowed') is True
    previous = si.get('previous_completed_session') or {}
    prior_available = view.get('temporal_applicability', {}).get('components', {}).get('futoi_live', {}).get('previous', {}).get('dated_observation_available') is True
    expected_previous = previous.get('factual') if si.get('governance', {}).get('factual_use_allowed') is True and prior_available and previous.get('consumer_factual_use_allowed') is not False else None
    _require(value['futoi_context']['futoi_live']['previous_observation'] == expected_previous, 'Si previous completeness')
    engine = si.get('delta_statistics') or {}; current = (si.get('current_intraday') or {}).get('factual') or {}
    normalized = (engine.get('current') or {}).get('factual') or {}
    # A separate comparison of source-native fields, not the producer projection helper.
    same = bool(current) and all(normalized.get(field) == current.get(field) and current.get(field) is not None
        for field in ('trade_date', 'snapshot_ts', 'source_publication_time', 'availability_ts_utc', 'ingest_ts_utc', 'total_open_interest'))
    for side in ('fiz', 'yur'):
        left = normalized.get(side) or {}; right = current.get(side) or {}
        same = same and all(left.get(field) == right.get(field) and right.get(field) is not None for field in ('long', 'short', 'net', 'long_participants', 'short_participants'))
        try: same = same and left.get('net_share_of_oi') == right['net'] / current['total_open_interest']
        except (KeyError, TypeError, ZeroDivisionError): same = False
    expected_comparisons = (admission and same and engine.get('instrument_id') == si.get('instrument_id') == 'si_futures_family'
        and engine.get('current', {}).get('status') == 'AVAILABLE' and engine.get('consumer_factual_use_allowed') is not False
        and engine.get('observed_date_witness', {}).get('status') == 'PASS'
        and engine.get('observed_date_witness', {}).get('current_observed_trade_date') == current.get('trade_date'))
    comparisons = value['futoi_context']['futoi_live']['comparisons']
    _require((comparisons is not None) == expected_comparisons, 'Si comparisons completeness')
    if expected_comparisons:
        _require(set(comparisons.get('deltas', {})) == set(engine.get('deltas', {})), 'Si all delta horizons')
        prior_engine = (engine.get('previous_observed_session') or {}).get('factual') or {}
        prior_source = previous.get('factual') or {}
        prior_match = bool(prior_source) and all(prior_engine.get(field) == prior_source.get(field) and prior_source.get(field) is not None
            for field in ('trade_date', 'snapshot_ts', 'source_publication_time', 'availability_ts_utc', 'ingest_ts_utc', 'total_open_interest'))
        for side in ('fiz', 'yur'):
            left = prior_engine.get(side) or {}; right = prior_source.get(side) or {}
            prior_match = prior_match and all(left.get(field) == right.get(field) and right.get(field) is not None
                for field in ('long', 'short', 'net', 'long_participants', 'short_participants'))
            try: prior_match = prior_match and left.get('net_share_of_oi') == right['net'] / prior_source['total_open_interest']
            except (KeyError, TypeError, ZeroDivisionError): prior_match = False
        for name, item in engine.get('deltas', {}).items():
            expected_delta = deepcopy(item)
            if name == 'delta_1d' and (expected_previous is None or not prior_match):
                expected_delta.update(status='UNAVAILABLE', reason='previous_observation_not_admitted_or_baseline_mismatch')
            if expected_delta.get('status') != 'AVAILABLE': expected_delta['values'] = None
            _require(comparisons['deltas'][name] == expected_delta, 'Si admitted delta values and exclusions')
        # A2 replaces only statistics. Its exact observed-slot inventory, source
        # exclusions and independent Decimal arithmetic were verified above;
        # the obsolete EOD-plus-current sample is retained only in source audit.
        _require(comparisons.get('statistics_policy') == 'observed_slots_admitted_subset_min2_descriptive.v1',
                 'Si observed statistics selection required')
    _require(all(event['direction'] == 'UNKNOWN' and event['classification_status'] == 'NOT_ANALYZED'
        for event in value['news_context']['events']), 'news no neutrality')
    news = components.get('official_news', {}); data = news.get('data') or {}
    pool = data.get('retained_event_pool', data.get('legacy_selected_event_pool', data.get('events', [])))
    pool = pool if isinstance(pool, list) else []
    causal = {}; conflicts = set(); publication_keys = {}; identities = set()
    from urllib.parse import urlunsplit
    import re
    for event in pool:
        try:
            stamps = [datetime.fromisoformat(event[key]) for key in ('published_at', 'available_at', 'ingested_at')]
            allowed = (news.get('status') in {'READY', 'PARTIAL', 'RETAINED_PREVIOUS'}
                and all(stamp.utcoffset() is not None for stamp in stamps)
                and stamps[0] <= stamps[1] <= stamps[2] <= now
                and all(isinstance(event.get(key), str) and event[key] for key in ('event_id', 'source_id', 'source_reference')))
            if not allowed: continue
            normalized = deepcopy(event)
            normalized.setdefault('quality_status', 'OK'); normalized.setdefault('content_hash', None)
            hashed = normalized['content_hash']; identity = None
            if isinstance(hashed, str) and re.fullmatch('[0-9a-f]{64}', hashed):
                try:
                    ref = event['source_reference'].strip()
                    if not ref: raise ValueError('empty publication reference')
                    parts = urlsplit(ref)
                    if parts.scheme in ('http', 'https') and parts.netloc:
                        ref = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, parts.query, ''))
                    identity = (ref, stamps[0].astimezone(timezone.utc), hashed)
                except ValueError: pass
            if event.get('publication_identity_policy') == 'exact_reference_publication_utc_content_hash.v2' and identity is None:
                continue
            event_id = event['event_id']
            # Conflicting causal IDs refuse before age/quality selection, including
            # a conflicting copy that would otherwise fall outside those windows.
            if event_id in causal and causal[event_id] != normalized: conflicts.add(event_id)
            causal[event_id] = normalized
            publication_keys[event_id] = identity if identity is not None else ('legacy', event_id)
        except (KeyError, TypeError, ValueError, OverflowError): pass
    eligible = [event for event_id, event in causal.items() if event_id not in conflicts
        and event['quality_status'] == 'OK' and (now-datetime.fromisoformat(event['published_at'])).total_seconds() <= 604800]
    order = lambda event: (datetime.fromisoformat(event['published_at']), event['source_id'], event['event_id'])
    pools = [[], []]
    for event in sorted(eligible, key=order, reverse=True):
        identity = publication_keys[event['event_id']]
        if identity in identities: continue
        identities.add(identity)
        pools[int((now - datetime.fromisoformat(event['published_at'])).total_seconds() > 86400)].append(event)
    selected = []
    for band, candidates in enumerate(pools):
        budget = 20 if band == 0 else min(4, 20-len(selected))
        groups = {}
        for event in candidates: groups.setdefault(event['source_id'], []).append(event)
        count = 0
        while groups and count < budget:
            for source in sorted(groups, key=lambda key: order(groups[key][0]), reverse=True):
                if count >= budget: break
                selected.append(groups[source].pop(0)); count += 1
                if not groups[source]: del groups[source]
    selected.sort(key=order)
    _require([event.get('event_id') for event in value['news_context']['events']] == [event['event_id'] for event in selected], 'news completeness')
    for actual, original in zip(value['news_context']['events'], selected):
        _require(all(actual.get(key) == original.get(key) for key in ('event_id', 'source_id', 'source_reference', 'published_at', 'available_at', 'ingested_at', 'content_hash', 'headline')), 'news source values')
        _require(actual.get('primary_provenance') == {key: original.get(key) for key in ('source_id', 'source_tier', 'source_reference', 'published_at', 'available_at', 'ingested_at', 'content_hash')}, 'news primary provenance')
        seconds = (now-datetime.fromisoformat(original['published_at'])).total_seconds()
        _require(actual.get('publication_age_seconds_at_as_of') == seconds, 'news publication age')
        _require(actual.get('selection_band') == ('fresh' if seconds <= 86400 else 'background'), 'news retention band')
        _require('source_provenance' not in actual, 'news compact provenance bound')
    basis = components.get('live_basis_carry', {})
    grouped = {}
    if basis.get('status') in {'READY', 'PARTIAL'}:
        for key, pair in (basis.get('data') or {}).get('pairs', {}).items():
            for index, metric in enumerate(pair.get('metrics', [])):
                if isinstance(metric, dict) and isinstance(metric.get('metric_id'), str) and metric['metric_id']:
                    grouped.setdefault(metric['metric_id'], []).append((key, index, metric))
    expected = {}
    for identity, copies in grouped.items():
        metric = copies[0][2]; number = metric.get('value')
        if (all(copy[2] == metric for copy in copies) and metric.get('status') == 'READY'
                and isinstance(number, (int, float)) and not isinstance(number, bool) and isfinite(number)):
            expected[identity] = metric
    _require(set(rows['basis_carry']['admitted_metric_ids']) == set(expected), 'basis matrix completeness')
    _require(('basis_carry' in facts) == bool(expected), 'basis fact completeness')
    actual = facts.get('basis_carry', {}).get('values', {}).get('metrics', [])
    _require(len(actual) == len(expected), 'basis exactly once')
    _require({entry['values']['metric_id'] for entry in actual} == set(expected), 'basis IDs')
    for entry in actual:
        metric = entry['values']; node = view
        for part in entry['snapshot_path'].split('.'):
            node = node[int(part)] if isinstance(node, list) else node[part]
        _require(metric == node == expected[metric['metric_id']], 'basis lossless values and source path')


def run(snapshot, *, now, code_revision, output, required_factors=REQUIRED_FACTORS):
    """Archive and verify one input, plus labelled in-memory failure simulations.

    No source retrieval, source-file mutation, training or model evaluation occurs.
    PASS accepts this factual contract only; the release remains INCOMPLETE.
    """
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix='factual-acceptance-', dir=output))
    before = deepcopy(snapshot)
    report = {'schema_version': 'rub_factual_release_acceptance.v1',
              'status': 'FAIL', 'code_revision': code_revision,
              'as_of_utc': now.isoformat(), 'required_factors': list(required_factors),
              'artifact_directory': str(directory.resolve()), 'checks': [],
              'failures': [], 'release_status': None, 'fact_count': 0,
              'forecast_acceptance': False, 'simulations_are_observed_outages': False}

    def check(name, operation):
        try:
            result = operation()
        except Exception as exc:
            # Do not copy arbitrary input or request error text into reports.
            report['checks'].append({'name': name, 'status': 'FAIL', 'detail': type(exc).__name__})
            report['failures'].append(name)
            return None
        report['checks'].append({'name': name, 'status': 'PASS', 'detail': 'verified'})
        return result

    def assemble():
        value = release.build(snapshot, now=now, code_revision=code_revision)
        report.update(release_status=value['status'], fact_count=len(value['facts']))
        return value

    value = check('build_with_pinned_revision_and_aware_time', assemble)
    if value is not None:
        def authority():
            _require(value['schema_version'] == release.SCHEMA and value['status'] == 'INCOMPLETE', 'release contract')
            _require(all(value[key] is False for key in AUTHORITY), 'authority')
            _require(set(value['horizons']) == {'D1', 'W1'}, 'horizons')
            for horizon in value['horizons'].values():
                _require(horizon['kind'] == 'FACTUAL_CONTEXT_NOT_FORECAST'
                         and horizon['status'] == 'INCOMPLETE'
                         and horizon['target_trading_date'] is None
                         and horizon['target_date_proven'] is False
                         and horizon['model_probability'] is None
                         and horizon['forecast_generated'] is False, 'horizon authority')
            inventory = value['macro_evidence_inventory']
            _require(all(inventory[key] is False for key in DENIED), 'macro authority')
            _require(bool(value['blocking_required_factors']), 'unresolved requirements')
        check('factual_only_D1_W1_and_authority', authority)

        def required():
            _require(bool(required_factors) and set(required_factors) <= _factors(value), 'required observed facts')
        check('required_observed_facts', required)

        def matrix():
            rows = {row['block_id']: row for row in value['matrix']}
            _require(len(rows) == len(value['matrix']), 'unique matrix blocks')
            _require(len(_factors(value)) == len(value['facts']), 'unique factors')
            for fact in value['facts']:
                block, gate = ADMISSION.get(fact['factor'], (fact['factor'], 'usable_for_full_forecast'))
                _require(rows[block][gate] is True, 'fact admission agrees with matrix')
                node = snapshot
                for part in fact['snapshot_path'].split('.'):
                    node = node[part]
                _require(isinstance(node, dict), 'source path resolves')
            expected = {row['block_id'] for row in value['matrix']
                        if row['requirement'] == 'required' and not row['usable_for_full_forecast']}
            _require(set(value['blocking_required_factors']) == expected, 'blocking factors agree')
            for event in value['macro_evidence_inventory']['scheduled_events']:
                _require(event['event_status'] == 'SCHEDULED'
                         and event['actual_event_time'] is None, 'planned event is not actual')
        check('all_facts_matrix_paths_and_scheduled_events', matrix)
        check('spot_and_basis_projection_completeness', lambda: projection_completeness(snapshot, value, now=now))

        def uncertainty():
            rows = [row for row in value['macro_evidence_inventory']['facts']
                    if row['component'] in REQUIRED_FACTORS]
            _require(len(rows) == 5, 'five core macro observations')
            for row in rows:
                item = row['uncertainty']
                _require(row['quality_status'] == 'USABLE_WITH_LIMITATIONS'
                         and item['usable_as_dated_context'] is True, 'limited context')
                _require(item['numeric_error_bounds'] is None and item['probabilities'] is None,
                         'no invented uncertainty bounds')
                _require(all(item[key] is False for key in
                             ('forecast_use_allowed', 'historical_pit_acceptance', 'action_authority')), 'uncertainty authority')
                _require(row['source_publication_time'] is None, 'unknown publication time')
            monthly = next(row for row in rows if row['component'] == 'rosstat_monthly_cpi')
            from decimal import Decimal
            for base, index in monthly['indices'].items():
                if index is not None:
                    _require(Decimal(monthly['changes_percent'][base]) == Decimal(index) - 100, 'explicit CPI base')
            for row in rows:
                if row['component'] == 'cbr_liquidity_verified':
                    _require(row['arithmetic_residual']['informational_only'] is True
                             and row['uncertainty']['revision_kind'] == 'latest_revised', 'liquidity limitation')
        check('core_macro_uncertainty_and_comparison_bases', uncertainty)

        def cached():
            if 'factual_release' in snapshot:
                _require(snapshot['factual_release'] == release.describe(snapshot), 'cached projection differs')
            current = release.describe(apply_read_freshness(snapshot, now=now))
            _require(all(value[key] == item for key, item in current.items()), 'current projection differs')
        check('cached_and_current_projection_consistency', cached)

        def archive():
            first = release.export(snapshot, now=now, code_revision=code_revision, output=directory / 'first')
            second = release.export(snapshot, now=now, code_revision=code_revision, output=directory / 'replay')
            for name in ('input_snapshot.json', 'release.json', 'manifest.json'):
                _require((first / name).read_bytes() == (second / name).read_bytes(), 'deterministic artifacts')
            manifest = json.loads((first / 'manifest.json').read_bytes())
            for name, field in (('input_snapshot.json', 'input_snapshot_sha256'), ('release.json', 'release_sha256')):
                _require(sha256((first / name).read_bytes()).hexdigest() == manifest[field], 'artifact digest')
            frozen = json.loads((first / 'input_snapshot.json').read_bytes())
            _require(release.build(frozen, now=now, code_revision=code_revision) == value, 'frozen replay')
            report['release_directory'] = str(first.resolve())
        check('frozen_export_hashes_and_exact_replay', archive)

        def expired():
            result = release.build(snapshot, now=now + timedelta(seconds=1201), code_revision=code_revision)
            _require(not set(REQUIRED_FACTORS).intersection(_factors(result)), 'expired facts excluded')
            _require(not any(row['component'] in REQUIRED_FACTORS
                             for row in result['macro_evidence_inventory']['facts']), 'expired inventory excluded')
        check('simulated_receipt_expiry', expired)

        for factor in REQUIRED_FACTORS:
            for defect in ('changed_value', 'missing_evidence', 'refresh_failed'):
                def failure(factor=factor, defect=defect):
                    changed = deepcopy(snapshot)
                    component = changed['components'][factor]
                    data = component['data']
                    if defect == 'changed_value':
                        if factor == 'rosstat_monthly_cpi':
                            data['indices']['previous_month'] = '999999.99'
                        else:
                            data['observations'][0]['value'] = '999999.99'
                    elif defect == 'missing_evidence':
                        key = 'document_manifest_path' if factor == 'rosstat_monthly_cpi' else 'manifest_path'
                        data[key] = str(directory / 'absent-evidence.json')
                    else:
                        component['refresh_error'] = 'acceptance_simulated_failure'
                    result = release.build(changed, now=now, code_revision=code_revision)
                    _require(factor not in _factors(result), 'failed source excluded')
                    _require([row for row in result['facts'] if row['factor'] != factor]
                             == [row for row in value['facts'] if row['factor'] != factor], 'unrelated facts preserved')
                    _require(result['macro_evidence_inventory']['facts'] ==
                             [row for row in value['macro_evidence_inventory']['facts'] if row['component'] != factor],
                             'only failed macro source excluded')
                check('simulated_' + factor + '_' + defect, failure)

        def missing():
            empty = {'identity': deepcopy(snapshot['identity']), 'components': {}}
            result = release.build(empty, now=now, code_revision=code_revision)
            _require(result['facts'] == [] and result['status'] == 'INCOMPLETE'
                     and bool(result['blocking_required_factors']), 'missing does not become neutral')
        check('simulated_missing_inputs_stay_incomplete', missing)
    check('input_not_mutated', lambda: _require(snapshot == before, 'input mutated'))
    report['status'] = 'FAIL' if report['failures'] else 'PASS'
    (directory / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--snapshot', type=Path)
    source.add_argument('--api-url', help='Local factual API endpoint; redirects are rejected')
    parser.add_argument('--env-file', type=Path, help='Local API token environment file (never copied to artifacts)')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--as-of', help='Aware consumption time; required for frozen input')
    args = parser.parse_args(argv)
    # Bind CLI evidence to the executing checkout, not an arbitrary user label.
    repo = Path(__file__).resolve().parents[2]
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=repo, text=True).strip():
        parser.error('acceptance CLI requires a clean committed checkout')
    if args.snapshot:
        if not args.as_of:
            parser.error('--as-of is required with --snapshot')
        snapshot = json.loads(args.snapshot.read_text(encoding='utf-8'))
        now = datetime.fromisoformat(args.as_of)
    else:
        url = urlsplit(args.api_url)
        if url.scheme != 'http' or url.hostname not in ('127.0.0.1', 'localhost', '::1') or url.username or url.password \
                or url.path != '/v1/rub/factual-snapshot' or url.query or url.fragment:
            parser.error('--api-url must be the local factual-snapshot endpoint')
        if args.as_of:
            parser.error('live acceptance uses current consumption time; --as-of is only for frozen input')
        import requests
        from src.misc.rub_factual_snapshot_http_server import load_api_token
        env = {'MOEX_ENV_FILE': str(args.env_file)} if args.env_file else None
        token = load_api_token(env)
        with requests.Session() as session:
            session.trust_env = False
            response = session.get(args.api_url, headers={'Authorization': 'Bearer ' + token},
                                   timeout=60, allow_redirects=False)
        if response.status_code != 200:
            parser.error('local factual API did not return HTTP 200')
        snapshot = response.json()
        now = datetime.now(timezone.utc)
    report = run(snapshot, now=now, code_revision=revision, output=args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())

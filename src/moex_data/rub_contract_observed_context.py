"""Factual-only sparse date aggregates of currently revalidated accepted Stage3 runs.

Archived markers do not retain original content hashes. Never present hashes read
now as cryptographic proof of original acceptance, or these dates as a full series.
"""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from pathlib import Path
import re

SCHEMA = 'rub_contract_observed_dates.v1'
SOURCE = 'moex_algopack_fo_tradestats_5m'
MOSCOW = timezone(timedelta(hours=3))
MAX_RUNS = 256
LOOKBACK_DAYS = 45


def _stamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None: raise ValueError('timestamp_timezone_required')
    return parsed


def _number(value, positive=False):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and (value > 0 if positive else value >= 0)


def _retained_chain(provenance):
    """Validate portable custody metadata without reopening mutable current pointers."""
    run = provenance['acceptance_run_id']
    if not isinstance(run, str) or not re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]*_stage3', run):
        raise ValueError('invalid_retained_acceptance_run')
    producer = provenance['run_id']
    if not isinstance(producer, str) or not re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]*', producer):
        raise ValueError('invalid_retained_producer_run')
    prefix = '${MOEX_DATA_ROOT}/'
    if (provenance['accepted_marker_ref'] != prefix+'state/acceptance/step3_canonical_raw/run_id='+run+'/accepted_pointers.json'
            or provenance['parent_manifest_ref'] != prefix+'runs/step10_rub_daily_refresh/run_id='+run[:-7]+'/run_manifest.json'):
        raise ValueError('retained_acceptance_parent_reference_mismatch')
    artifact_prefix = prefix+'runs/step3_canonical_raw/run_id='+run+'/'
    for field in ('partition_ref', 'manifest_ref', 'quality_report_ref'):
        value = provenance[field]
        if (not isinstance(value, str) or not value.startswith(artifact_prefix) or '\\' in value
                or any(part in ('', '.', '..') for part in value[len(artifact_prefix):].split('/'))):
            raise ValueError('retained_artifact_run_reference_mismatch')
    for field in ('partition_sha256', 'manifest_sha256', 'quality_report_sha256',
                  'accepted_marker_sha256_at_revalidation', 'pilot_evidence_sha256_at_revalidation',
                  'parent_manifest_sha256_at_revalidation'):
        if not isinstance(provenance.get(field), str) or not re.fullmatch('[0-9a-f]{64}', provenance[field]):
            raise ValueError('current_revalidation_digest_missing')


def aggregate(frame, spec, *, available, binding_available):
    """Require original per-row identity, observed times and finite coherent OHLCV."""
    import pandas as pd
    if len(frame) != spec.row_count or frame.empty:
        raise ValueError('contract_partition_row_count_mismatch')
    for key, expected in (('instrument_id', spec.instrument_id), ('secid', spec.secid),
                          ('trade_date', spec.trade_date), ('source_id', SOURCE)):
        if key not in frame or not frame[key].eq(expected).all():
            raise ValueError('contract_row_identity_mismatch:' + key)
    timestamps = pd.to_datetime(frame['ts'], errors='raise')
    if timestamps.dt.tz is None: timestamps = timestamps.dt.tz_localize('Europe/Moscow')
    timestamps = timestamps.dt.tz_convert('UTC')
    receipts = pd.to_datetime(frame['ingest_ts'], errors='raise')
    if receipts.dt.tz is None or receipts.isna().any(): raise ValueError('contract_invalid_source_receipt')
    receipts = receipts.dt.tz_convert('UTC')
    if (receipts > available).any() or (timestamps > receipts).any(): raise ValueError('contract_noncausal_source_receipt')
    if (receipts < binding_available).any(): raise ValueError('contract_binding_after_source_receipt')
    if timestamps.isna().any() or timestamps.duplicated().any() or not timestamps.is_monotonic_increasing:
        raise ValueError('contract_duplicate_invalid_or_unordered_times')
    if (timestamps > available).any() or not timestamps.dt.tz_convert('Europe/Moscow').dt.date.eq(date.fromisoformat(spec.trade_date)).all():
        raise ValueError('contract_source_date_or_availability_mismatch')
    rows = frame.to_dict('records')
    for row in rows:
        if any(not _number(row.get(key), True) for key in ('open', 'high', 'low', 'close')):
            raise ValueError('contract_invalid_ohlc')
        if not row['low'] <= min(row['open'], row['close']) <= max(row['open'], row['close']) <= row['high']:
            raise ValueError('contract_incoherent_ohlc')
        if not _number(row.get('volume')): raise ValueError('contract_invalid_volume')
    result = {'secid': spec.secid, 'instrument_id_at_source': spec.instrument_id,
        'source_date': spec.trade_date, 'source_bar_count': len(rows),
        'source_first_bar_at_utc': timestamps.iloc[0].isoformat(), 'source_last_bar_at_utc': timestamps.iloc[-1].isoformat(),
        'source_receipt_lower_bound_utc': receipts.min().isoformat(),
        'source_receipt_upper_bound_utc': receipts.max().isoformat(),
        'open': rows[0]['open'], 'high': max(row['high'] for row in rows), 'low': min(row['low'] for row in rows),
        'close': rows[-1]['close'], 'volume': sum(row['volume'] for row in rows),
        'session_completion_proven': False, 'intraday_gap_count': int((timestamps.diff().dropna() != pd.Timedelta(minutes=5)).sum())}
    if not isfinite(result['volume']): raise ValueError('contract_nonfinite_volume_sum')
    return result


def collect(root, *, now):
    """Bounded local read only. Require successful parent, never marker alone."""
    from moex_data import step3_raw_acceptance as stage3, step9_rub_analysis_bundle as step9
    import pandas as pd
    base = root / 'state/acceptance/step3_canonical_raw'
    result = {'schema_version': SCHEMA, 'captured_at_utc': now.isoformat(), 'rows': [], 'refusals': [],
        'scan_lookback_calendar_days': LOOKBACK_DAYS, 'max_retained_dates_per_secid': 30}
    if not base.exists(): return result
    try:
        base.resolve(strict=True).relative_to(root)
        if base.is_symlink(): raise ValueError('symlink_archive_inventory')
    except (ValueError, OSError):
        result['refusals'] = [{'reason': 'archive_inventory_escaped_root_or_symlink'}]
        return result
    paths = sorted(base.glob('run_id=*/accepted_pointers.json'))
    earliest = now.astimezone(MOSCOW).date() - timedelta(days=LOOKBACK_DAYS)
    result['inventory_marker_count'] = len(paths)
    candidates = []
    def refuse(run, reason):
        if len(result['refusals']) < MAX_RUNS:
            result['refusals'].append({'run_id': run, 'reason': reason[:160]})
        else:
            result['refusals_omitted_count'] = result.get('refusals_omitted_count', 0) + 1
    # Filter small metadata before bounding expensive support/parquet revalidation.
    # Growth of old archives must not erase the recent accepted-date context.
    for path in paths:
        run = path.parent.name.removeprefix('run_id=')
        try:
            pilot_path = stage3.pilot_evidence_path(run)
            pilot_path = step9._resolve_root_ref(step9.ROOT_REF_PREFIX + pilot_path.relative_to(root).as_posix(), 'pilot_inventory', root)
            metadata = step9._load_json(pilot_path, 'pilot_inventory')
            observed = date.fromisoformat(metadata['trade_date'])
            if earliest <= observed <= now.astimezone(MOSCOW).date(): candidates.append((observed, path))
        except (ValueError, KeyError, TypeError, OSError):
            refuse(run, 'pilot_inventory_metadata_unavailable_or_invalid')
    if len(candidates) > MAX_RUNS:
        result['recent_candidates_omitted_by_bound'] = len(candidates)-MAX_RUNS
    paths = [path for _, path in sorted(candidates)[-MAX_RUNS:]]
    selected = {}
    for marker_path in paths:
        run = marker_path.parent.name.removeprefix('run_id=')
        try:
            from moex_data.rub_accepted_stage3_resolver import resolve
            resolved = resolve(root, marker_path, now=now, earliest=earliest)
            if resolved is None: continue
            marker_path, pilot_path, parent_path = (resolved[key] for key in ('marker_path', 'pilot_path', 'parent_path'))
            pilot, finished, binding, specs = (resolved[key] for key in ('pilot', 'finished', 'binding', 'specs'))
            # Stage3 revalidates support documents; freshly read hashes are labelled as such.
            run_rows = []
            for spec in specs:
                if spec.dataset_id != 'futures_raw_5m': continue
                for path in (spec.partition_path, spec.manifest_path, spec.quality_path):
                    step9._resolve_root_ref(step9.ROOT_REF_PREFIX + path.relative_to(root).as_posix(), 'archive_artifact', root)
                values = aggregate(pd.read_parquet(spec.partition_path), spec, available=finished, binding_available=binding)
                hashes = stage3._pointer_values(spec, acceptance_run_id=run, binding_availability_ts_utc=binding.isoformat())
                values.update(binding_observed_at_utc=binding.isoformat(), parent_finished_at_utc=finished.isoformat(),
                    binding_at_source=deepcopy(next(item for item in pilot['bindings'] if item['instrument_id'] == spec.instrument_id)),
                    source_provenance={**hashes, 'accepted_marker_ref': step9.ROOT_REF_PREFIX + marker_path.relative_to(root).as_posix(),
                        'hash_semantics': 'computed_at_current_revalidation_not_original_acceptance_digest',
                        'accepted_marker_sha256_at_revalidation': step9._sha256_file(marker_path),
                        'pilot_evidence_sha256_at_revalidation': step9._sha256_file(pilot_path),
                        'parent_manifest_ref': step9.ROOT_REF_PREFIX + parent_path.relative_to(root).as_posix(),
                        'parent_manifest_sha256_at_revalidation': step9._sha256_file(parent_path)})
                run_rows.append(values)
            for row in run_rows:
                key = (row['secid'], row['source_date'])
                if key not in selected or _stamp(selected[key]['parent_finished_at_utc']) < finished: selected[key] = row
        except (ValueError, KeyError, TypeError, OSError, AttributeError) as exc:
            refuse(run, str(exc))
    for secid in sorted({key[0] for key in selected}):
        result['rows'].extend(sorted((row for row in selected.values() if row['secid'] == secid), key=lambda row: row['source_date'])[-30:])
    return result


def describe(evidence, *, now):
    """Sparse accepted dates are descriptive context, never exact trading lags."""
    result = {'schema_version': SCHEMA, 'status': 'UNAVAILABLE', 'contracts': [],
        'scope': 'CURRENT_REVALIDATED_ACCEPTED_RUN', 'historical_pit_usable': False,
        'historical_original_digest_available': False, 'historical_original_digest_unavailable': True,
        'first_accepted_at_utc': None, 'session_completion_proven': False, 'model_usable': False,
        'coverage_limitation': 'sparse_accepted_run_dates_not_complete_observed_date_universe', 'as_of_utc': now.isoformat()}
    if not isinstance(evidence, dict) or evidence.get('schema_version') != SCHEMA: return result
    rows = evidence.get('rows')
    if not isinstance(rows, list) or len(rows) > 240: return result
    result['source_scan_refusals'] = deepcopy(evidence.get('refusals', []))
    result['source_inventory'] = {key: deepcopy(evidence[key]) for key in ('inventory_marker_count',
        'scan_lookback_calendar_days', 'max_retained_dates_per_secid', 'recent_candidates_omitted_by_bound',
        'refusals_omitted_count') if key in evidence}
    result['capture_as_of_utc'] = evidence.get('captured_at_utc')
    try:
        captured = _stamp(evidence['captured_at_utc'])
        if captured > now: return result
        capture_day = captured.astimezone(MOSCOW).date()
        earliest = capture_day - timedelta(days=LOOKBACK_DAYS)
        seen = set(); causal = []; bindings_by_run = {}
        for row in rows:
            secid = row['secid']; day = date.fromisoformat(row['source_date'])
            if not earliest <= day <= capture_day: raise ValueError('contract_date_outside_capture_window')
            if not re.fullmatch(r'(Si|CR)[HMUZ][0-9]', secid): raise ValueError('invalid_contract_secid')
            prefix = 'si_' if secid.startswith('Si') else 'cr_'
            if row['instrument_id_at_source'] not in (prefix+'front_contract', prefix+'next_contract'):
                raise ValueError('contract_role_identity_mismatch')
            provenance = row['source_provenance']
            _retained_chain(provenance)
            if (provenance['secid'] != secid or provenance['instrument_id'] != row['instrument_id_at_source']
                    or provenance['source_id'] != SOURCE or provenance['dataset_id'] != 'futures_raw_5m'
                    or provenance['acceptance_contract_id'] != 'step3_canonical_raw_acceptance.v1'):
                raise ValueError('contract_source_provenance_mismatch')
            binding = row['binding_at_source']
            family = 'Si' if prefix == 'si_' else 'CR'
            role = 'front' if row['instrument_id_at_source'] == prefix+'front_contract' else 'next'
            bound_at = _stamp(row['binding_observed_at_utc'])
            expiry = date.fromisoformat(binding['last_trade_date'])
            if (binding['root'] != family or binding['role'] != role
                    or binding['instrument_id'] != row['instrument_id_at_source'] or binding['secid'] != secid
                    or binding['source_id'] != 'moex_iss_forts_securities_reference'
                    or date.fromisoformat(binding['as_of_date']) != day or expiry < day
                    or _stamp(binding['mapping_fixed_ts_utc']) != bound_at
                    or _stamp(binding['availability_ts_utc']) != bound_at
                    or _stamp(provenance['binding_availability_ts_utc']) != bound_at
                    or bound_at.astimezone(MOSCOW).date() < day):
                raise ValueError('contract_retained_binding_mismatch')
            if (provenance.get('quality_status') != 'pass' or provenance.get('refresh_status') != 'succeeded'
                    or provenance.get('hash_semantics') != 'computed_at_current_revalidation_not_original_acceptance_digest'):
                raise ValueError('contract_retained_admission_mismatch')
            run_key = (provenance['acceptance_run_id'], day, family)
            by_role = bindings_by_run.setdefault(run_key, {})
            if role in by_role: raise ValueError('duplicate_retained_binding_role')
            by_role[role] = (secid, expiry)
            if any(not isinstance(provenance.get(key), str) or not re.fullmatch('[0-9a-f]{64}', provenance[key])
                   for key in ('partition_sha256', 'manifest_sha256', 'quality_report_sha256')):
                raise ValueError('current_revalidation_digest_missing')
            if any(_stamp(row[key]).astimezone(MOSCOW).date() != day for key in ('source_first_bar_at_utc', 'source_last_bar_at_utc')):
                raise ValueError('contract_observed_date_mismatch')
            key = (secid, day)
            if key in seen: raise ValueError('duplicate_contract_date')
            seen.add(key)
            if (type(row['source_bar_count']) is not int or row['source_bar_count'] <= 0
                    or type(row['intraday_gap_count']) is not int or not 0 <= row['intraday_gap_count'] < row['source_bar_count']
                    or row.get('session_completion_proven') is not False):
                raise ValueError('invalid_retained_bar_count_or_scope')
            if any(not _number(row[key], True) for key in ('open', 'high', 'low', 'close')) or not _number(row['volume']): raise ValueError('invalid_contract_values')
            if not row['low'] <= min(row['open'], row['close']) <= max(row['open'], row['close']) <= row['high']: raise ValueError('invalid_contract_ohlc')
            if not (_stamp(row['source_first_bar_at_utc']) <= _stamp(row['source_last_bar_at_utc']) <= _stamp(row['source_receipt_upper_bound_utc']) <= _stamp(row['parent_finished_at_utc']) <= captured
                    and _stamp(row['source_first_bar_at_utc']) <= _stamp(row['source_receipt_lower_bound_utc'])
                    and _stamp(row['binding_observed_at_utc']) <= _stamp(row['source_receipt_lower_bound_utc'])
                    <= _stamp(row['source_receipt_upper_bound_utc'])): raise ValueError('invalid_retained_causality')
            causal.append(deepcopy(row))
        for by_role in bindings_by_run.values():
            if 'front' in by_role and 'next' in by_role:
                if by_role['front'][0] == by_role['next'][0] or by_role['front'][1] >= by_role['next'][1]:
                    raise ValueError('retained_front_next_binding_order_mismatch')
        for secid in sorted({row['secid'] for row in causal}):
            part = sorted((row for row in causal if row['secid'] == secid), key=lambda row: row['source_date'])
            if len(part) > 30: raise ValueError('contract_date_bound_exceeded')
            first, last = date.fromisoformat(part[0]['source_date']), date.fromisoformat(part[-1]['source_date'])
            if (last-first).days > LOOKBACK_DAYS: raise ValueError('contract_history_bound_exceeded')
            days = [row['source_date'] for row in part]
            weeks = {}
            for row in part:
                day = date.fromisoformat(row['source_date']); monday = day-timedelta(days=day.weekday())
                weeks.setdefault(monday.isoformat(), []).append(row)
            week_views = []
            for monday, group in sorted(weeks.items()):
                week_views.append({'week_start_date': monday, 'source_dates': [row['source_date'] for row in group],
                    'open': group[0]['open'], 'high': max(row['high'] for row in group),
                    'low': min(row['low'] for row in group), 'close': group[-1]['close'],
                    'scope': 'observed_archive_dates_only_not_accepted_W1', 'week_completion_proven': False})
            result['contracts'].append({'secid': secid, 'price_unit': 'RUB_per_1000_USD' if secid.startswith('Si') else 'RUB_per_CNY',
                'observations': part, 'observed_date_count': len(part), 'source_start_date': days[0], 'source_end_date': days[-1],
                'observed_weeks': week_views[-8:], 'exact_1_5_20_trading_date_comparisons': None,
                'comparison_refusal': 'complete_observed_date_universe_unproven',
                'calendar_dates_without_accepted_run': [(first+timedelta(days=i)).isoformat() for i in range((last-first).days+1)
                    if (first+timedelta(days=i)).isoformat() not in days],
                'gap_semantics': 'unrepresented_calendar_dates_not_proven_missing_sessions'})
        if result['contracts']: result['status'] = 'AVAILABLE'
    except (ValueError, KeyError, TypeError, OverflowError):
        result.update(status='UNAVAILABLE', contracts=[], reason='invalid_retained_contract_evidence')
    return result

"""Factual-only sparse date aggregates of currently revalidated accepted Stage3 runs.

Archived markers do not retain original content hashes. Never present hashes read
now as cryptographic proof of original acceptance, or these dates as a full series.
"""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from math import isfinite
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
            marker_path = step9._resolve_root_ref(step9.ROOT_REF_PREFIX + marker_path.relative_to(root).as_posix(), 'accepted_marker', root)
            marker = step9._load_json(marker_path, 'accepted_marker')
            if (marker.get('project') != 'MOEX_Bot' or marker.get('step') != 3 or marker.get('status') != 'accepted'
                    or marker.get('run_id') != run or marker.get('acceptance_contract_id') != stage3.CONTRACT_ID
                    or marker.get('artifact_semantics') != 'immutable_run_scoped'
                    or marker.get('accepted_pointer_count') != 10 or marker.get('expected_pointer_count') != 10):
                raise ValueError('accepted_marker_identity_or_status_mismatch')
            pilot_path = step9._resolve_root_ref(marker['pilot_evidence_ref'], 'pilot_evidence', root)
            if pilot_path != stage3.pilot_evidence_path(run).resolve(): raise ValueError('pilot_evidence_run_path_mismatch')
            pilot = step9._load_json(pilot_path, 'pilot_evidence')
            observed = date.fromisoformat(pilot['trade_date'])
            if observed < earliest or observed > now.astimezone(MOSCOW).date(): continue
            if not run.endswith('_stage3'): raise ValueError('successful_parent_proof_unavailable')
            parent_run = run[:-7]
            parent_path = step9._resolve_root_ref(step9.ROOT_REF_PREFIX + 'runs/step10_rub_daily_refresh/run_id=' + parent_run + '/run_manifest.json', 'parent', root)
            parent = step9._load_json(parent_path, 'parent')
            refresh = parent.get('source_refresh', {})
            if (parent.get('project') != 'MOEX_Bot' or parent.get('stage') != 10 or parent.get('run_id') != parent_run
                    or parent.get('status') != 'succeeded' or parent.get('current_pointer_rollback_status') not in (None, 'not_needed')
                    or refresh.get('status') != 'refreshed' or refresh.get('stage3_run_id') != run
                    or refresh.get('trade_date') != pilot['trade_date']):
                raise ValueError('parent_failed_rolled_back_or_identity_mismatch')
            finished = _stamp(parent['finished_at_utc']); binding = _stamp(pilot['reference_observed_at_utc'])
            if not binding <= finished <= now: raise ValueError('future_binding_or_parent_completion')
            if pilot.get('run_artifacts_immutable') is not True or pilot.get('run_id_reuse_allowed') is not False:
                raise ValueError('immutable_run_proof_missing')
            specs = stage3.validate_pilot_evidence(pilot, run_id=run)
            pointers = marker.get('pointers')
            if not isinstance(pointers, list) or len(pointers) != 10: raise ValueError('accepted_marker_pointer_count')
            for spec in specs:
                matches = [item for item in pointers if item.get('dataset_id') == spec.dataset_id and item.get('instrument_id') == spec.instrument_id]
                if len(matches) != 1: raise ValueError('accepted_marker_pointer_identity')
                item = matches[0]
                if (step9._resolve_root_ref(item['manifest_ref'], 'marker_manifest', root) != spec.manifest_path
                        or step9._resolve_root_ref(item['quality_report_ref'], 'marker_quality', root) != spec.quality_path):
                    raise ValueError('accepted_marker_support_reference_mismatch')
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
        if _stamp(evidence['captured_at_utc']) > now: return result
        seen = set(); causal = []
        for row in rows:
            secid = row['secid']; day = date.fromisoformat(row['source_date'])
            if not re.fullmatch(r'(Si|CR)[HMUZ][0-9]', secid): raise ValueError('invalid_contract_secid')
            prefix = 'si_' if secid.startswith('Si') else 'cr_'
            if row['instrument_id_at_source'] not in (prefix+'front_contract', prefix+'next_contract'):
                raise ValueError('contract_role_identity_mismatch')
            provenance = row['source_provenance']
            if (provenance['secid'] != secid or provenance['instrument_id'] != row['instrument_id_at_source']
                    or provenance['source_id'] != SOURCE or provenance['dataset_id'] != 'futures_raw_5m'
                    or provenance['acceptance_contract_id'] != 'step3_canonical_raw_acceptance.v1'):
                raise ValueError('contract_source_provenance_mismatch')
            if any(not isinstance(provenance.get(key), str) or not re.fullmatch('[0-9a-f]{64}', provenance[key])
                   for key in ('partition_sha256', 'manifest_sha256', 'quality_report_sha256')):
                raise ValueError('current_revalidation_digest_missing')
            if any(_stamp(row[key]).astimezone(MOSCOW).date() != day for key in ('source_first_bar_at_utc', 'source_last_bar_at_utc')):
                raise ValueError('contract_observed_date_mismatch')
            key = (secid, day)
            if key in seen: raise ValueError('duplicate_contract_date')
            seen.add(key)
            if any(not _number(row[key], True) for key in ('open', 'high', 'low', 'close')) or not _number(row['volume']): raise ValueError('invalid_contract_values')
            if not row['low'] <= min(row['open'], row['close']) <= max(row['open'], row['close']) <= row['high']: raise ValueError('invalid_contract_ohlc')
            if not (_stamp(row['source_first_bar_at_utc']) <= _stamp(row['source_last_bar_at_utc']) <= _stamp(row['source_receipt_upper_bound_utc']) <= _stamp(row['parent_finished_at_utc']) <= now
                    and _stamp(row['binding_observed_at_utc']) <= _stamp(row['source_receipt_lower_bound_utc'])
                    <= _stamp(row['source_receipt_upper_bound_utc'])): continue
            causal.append(deepcopy(row))
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

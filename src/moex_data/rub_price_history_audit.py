"""Read-only price/timestamp audit; observed gaps never certify a session calendar."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path

SCHEMA = 'rub_price_history_audit.v1'
PRICE_FIELDS = ('open', 'high', 'low', 'close')
MOSCOW = timezone(timedelta(hours=3))


def instant(value):
    if not isinstance(value, str):
        raise ValueError('timestamp must be a string')
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError('timezone-aware timestamp required')
    return parsed.astimezone(timezone.utc)


def calendar_date(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError('canonical date required')
    return date.fromisoformat(value)


def prices(row):
    values = []
    for field in PRICE_FIELDS:
        value = row[field]
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise ValueError('invalid price type')
        number = Decimal(str(value))
        if not number.is_finite() or number <= 0:
            raise ValueError('price must be finite and positive')
        values.append(number)
    opening, high, low, close = values
    if not low <= min(opening, close) <= max(opening, close) <= high:
        raise ValueError('invalid OHLC bounds')
    return tuple(values)


def aggregate(rows):
    return (rows[0]['prices'][0], max(r['prices'][1] for r in rows),
            min(r['prices'][2] for r in rows), rows[-1]['prices'][3])


def audit(dataset, *, as_of):
    """Inspect explicit prices and causal metadata without importing any builder."""
    cutoff = instant(as_of)
    issues = []
    observed_gaps = []
    bars, daily = [], []
    counts = {'bars': 0, 'daily': 0, 'weekly': 0}
    incomplete = {'daily': [], 'weekly': []}

    def issue(scope, index, reason):
        issues.append({'scope': scope, 'row': index, 'reason': reason})

    if not isinstance(dataset, dict):
        dataset = {}
    if dataset.get('schema_version') != 'rub_exchange_history_research.v1':
        issue('dataset', None, 'unsupported_schema')
    if dataset.get('secid') not in ('SiU6', 'CRU6'):
        issue('dataset', None, 'unsupported_contract')
    try:
        built = instant(dataset['built_at_utc'])
        if built > cutoff:
            issue('dataset', None, 'build_after_as_of')
    except (KeyError, ValueError, TypeError, OverflowError):
        built = None
        issue('dataset', None, 'invalid_build_timestamp')

    def rows(name):
        value = dataset.get(name)
        if not isinstance(value, list) or not value:
            issue(name, None, 'missing_or_empty_rows')
            return []
        counts[name] = len(value)
        return value

    def available(row, scope, index):
        value = instant(row['availability_ts'])
        if value > cutoff:
            issue(scope, index, 'availability_after_as_of')
        if built is not None and value > built:
            issue(scope, index, 'availability_after_build')
        return value

    seen = set()
    previous = None
    for index, row in enumerate(rows('bars')):
        try:
            end = instant(row['interval_end'])
            if end in seen:
                issue('bars', index, 'duplicate_timestamp')
            seen.add(end)
            if previous is not None and end <= previous:
                issue('bars', index, 'timestamps_not_strictly_increasing')
            previous = end
            if end.second or end.microsecond or end.minute % 5:
                issue('bars', index, 'timestamp_not_on_five_minute_grid')
            receipt = available(row, 'bars', index)
            if receipt < end:
                issue('bars', index, 'availability_precedes_interval_end')
            if not isinstance(row.get('source_id'), str) or not row['source_id'].strip():
                issue('bars', index, 'missing_source_identity')
            bars.append({'end': end, 'availability': receipt, 'prices': prices(row),
                         'calendar_date': end.astimezone(MOSCOW).date().isoformat()})
        except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError):
            issue('bars', index, 'invalid_price_or_timestamp_record')
    bars.sort(key=lambda row: row['end'])
    for left, right in zip(bars, bars[1:]):
        seconds = (right['end'] - left['end']).total_seconds()
        if seconds > 300:
            observed_gaps.append({'left_interval_end': left['end'].isoformat(),
                'right_interval_end': right['end'].isoformat(), 'elapsed_seconds': seconds,
                'kind': 'OBSERVED_INTERVAL_ONLY', 'missing_bar_count': None,
                'session_state': 'UNKNOWN'})

    day_labels, assigned_dates = set(), set()
    for index, row in enumerate(rows('daily')):
        try:
            label = calendar_date(row['trading_day'])
            if label in day_labels:
                issue('daily', index, 'duplicate_period')
            day_labels.add(label)
            value = prices(row)
            receipt = available(row, 'daily', index)
            source_dates = row['source_calendar_dates']
            if (not isinstance(source_dates, list) or not source_dates
                    or any(not isinstance(v, str) for v in source_dates)
                    or source_dates != sorted(set(source_dates))):
                raise ValueError('invalid source dates')
            for item in source_dates:
                calendar_date(item)
            if assigned_dates.intersection(source_dates):
                issue('daily', index, 'calendar_date_assigned_to_multiple_periods')
            assigned_dates.update(source_dates)
            children = [bar for bar in bars if bar['calendar_date'] in source_dates]
            if {bar['calendar_date'] for bar in children} != set(source_dates):
                issue('daily', index, 'declared_source_date_without_price_bars')
            if not children:
                issue('daily', index, 'no_observed_children')
            else:
                if value != aggregate(children):
                    issue('daily', index, 'OHLC_does_not_match_observed_children')
                if receipt < max(bar['availability'] for bar in children):
                    issue('daily', index, 'availability_precedes_child_evidence')
            missing = row.get('missing_calendar_dates')
            if not isinstance(missing, list):
                issue('daily', index, 'missing_coverage_metadata')
            if row.get('session_coverage_complete') is not True:
                incomplete['daily'].append(label.isoformat())
            elif missing:
                issue('daily', index, 'complete_flag_with_declared_missing_dates')
            daily.append({'day': label, 'prices': value, 'availability': receipt})
        except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError):
            issue('daily', index, 'invalid_price_or_timestamp_record')
    unassigned = sorted({bar['calendar_date'] for bar in bars} - assigned_dates)
    if unassigned:
        issue('daily', None, 'observed_calendar_dates_not_assigned')

    week_labels, assigned_days = set(), set()
    for index, row in enumerate(rows('weekly')):
        try:
            start = calendar_date(row['week_start'])
            if start in week_labels:
                issue('weekly', index, 'duplicate_period')
            week_labels.add(start)
            value = prices(row)
            receipt = available(row, 'weekly', index)
            # A declared seven-day bucket is not proof of expected sessions.
            children = sorted((day for day in daily if start <= day['day'] < start + timedelta(days=7)),
                              key=lambda day: day['day'])
            child_days = {day['day'] for day in children}
            if assigned_days.intersection(child_days):
                issue('weekly', index, 'daily_period_assigned_to_multiple_weeks')
            assigned_days.update(child_days)
            if not children:
                issue('weekly', index, 'no_observed_children')
            else:
                if value != aggregate(children):
                    issue('weekly', index, 'OHLC_does_not_match_observed_children')
                if receipt < max(day['availability'] for day in children):
                    issue('weekly', index, 'availability_precedes_child_evidence')
            if row.get('session_coverage_complete') is not True:
                incomplete['weekly'].append(start.isoformat())
        except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError):
            issue('weekly', index, 'invalid_price_or_timestamp_record')
    if {day['day'] for day in daily} - assigned_days:
        issue('weekly', None, 'observed_daily_periods_not_assigned')

    return {'schema_version': SCHEMA, 'secid': dataset.get('secid'), 'as_of': cutoff.isoformat(),
        'validation_status': 'FAIL' if issues else 'PASS', 'counts': counts, 'issues': issues,
        'observed_interval_gaps': observed_gaps, 'unassigned_calendar_dates': unassigned,
        'declared_incomplete_periods': incomplete, 'coverage_acceptance': 'UNVERIFIED',
        'calendar_acceptance_granted': False, 'historical_pit_ready': False,
        'model_acceptance_granted': False, 'accepted_pointer_promotion': False,
        'limitations': ['Only stored prices, identity and causal metadata checked.',
            'Input lineage hashes and source calendar evidence are not replayed.',
            'Observed intervals and upstream completeness flags do not establish missing sessions.',
            'Historical acquisition does not establish availability before receipt.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--as-of', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.dataset.is_symlink() or not args.dataset.is_file() or args.output.is_symlink():
        parser.error('regular input and exclusive regular output required')
    raw = args.dataset.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != args.expected_sha256:
        parser.error('dataset SHA mismatch')
    report = audit(json.loads(raw), as_of=args.as_of)
    report.update(dataset_sha256=digest, dataset_path=str(args.dataset.resolve()),
                  validator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
    print(json.dumps({'validation_status': report['validation_status'], 'issues': len(report['issues']),
                      'coverage_acceptance': report['coverage_acceptance']}))
    return 1 if report['issues'] else 0


if __name__ == '__main__':
    raise SystemExit(main())

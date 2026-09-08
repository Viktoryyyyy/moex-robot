from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import unittest

from moex_data.rub_price_history_audit import audit

AS_OF = '2026-09-08T10:00:00+03:00'


def fixture():
    receipt = '2026-09-07T10:00:00+03:00'
    price = dict(open='10', high='12', low='9', close='11')
    bars = [dict(price, interval_end='2026-09-06T07:00:00+03:00',
                 availability_ts=receipt, source_id='official_archive'),
            dict(price, interval_end='2026-09-06T07:10:00+03:00',
                 availability_ts=receipt, source_id='official_archive')]
    daily = [dict(price, trading_day='2026-09-06', availability_ts=receipt,
                  source_calendar_dates=['2026-09-06'], missing_calendar_dates=[],
                  session_coverage_complete=False)]
    weekly = [dict(price, week_start='2026-09-06', availability_ts=receipt,
                   session_coverage_complete=False)]
    return dict(schema_version='rub_exchange_history_research.v1', secid='SiU6',
                built_at_utc='2026-09-07T11:00:00+03:00', bars=bars, daily=daily, weekly=weekly)


class PriceHistoryAuditTests(unittest.TestCase):
    def test_observed_gap_is_not_claimed_missing_or_closed(self):
        source = fixture()
        original = deepcopy(source)
        result = audit(source, as_of=AS_OF)
        self.assertEqual(result['validation_status'], 'PASS')
        self.assertEqual(len(result['observed_interval_gaps']), 1)
        gap = result['observed_interval_gaps'][0]
        self.assertEqual(gap['elapsed_seconds'], 600)
        self.assertIsNone(gap['missing_bar_count'])
        self.assertEqual(gap['session_state'], 'UNKNOWN')
        self.assertEqual(result['coverage_acceptance'], 'UNVERIFIED')
        self.assertFalse(result['calendar_acceptance_granted'])
        self.assertFalse(result['model_acceptance_granted'])
        self.assertFalse(result['historical_pit_ready'])
        self.assertEqual(source, original)

    def test_corruption_is_detected(self):
        cases = [
            ('bars', 'interval_end', '2026-09-06T07:01:00+03:00', 'timestamp_not_on_five_minute_grid'),
            ('bars', 'availability_ts', '2026-09-06T06:59:00+03:00', 'availability_precedes_interval_end'),
            ('bars', 'availability_ts', '2026-09-09T10:00:00+03:00', 'availability_after_as_of'),
            ('bars', 'availability_ts', '2026-09-07T12:00:00+03:00', 'availability_after_build'),
            ('bars', 'availability_ts', '2026-09-07T10:00:00', 'invalid_price_or_timestamp_record'),
            ('bars', 'high', '8', 'invalid_price_or_timestamp_record'),
            ('bars', 'close', 'NaN', 'invalid_price_or_timestamp_record'),
            ('bars', 'low', True, 'invalid_price_or_timestamp_record'),
            ('bars', 'source_id', '', 'missing_source_identity'),
            ('daily', 'close', '10.5', 'OHLC_does_not_match_observed_children'),
            ('daily', 'availability_ts', '2026-09-07T09:59:00+03:00', 'availability_precedes_child_evidence'),
            ('weekly', 'close', '10.5', 'OHLC_does_not_match_observed_children'),
            ('weekly', 'availability_ts', '2026-09-07T09:59:00+03:00', 'availability_precedes_child_evidence'),
        ]
        for scope, field, value, reason in cases:
            with self.subTest(scope=scope, field=field, value=value):
                source = fixture()
                source[scope][0][field] = value
                result = audit(source, as_of=AS_OF)
                self.assertEqual(result['validation_status'], 'FAIL')
                self.assertIn(reason, [issue['reason'] for issue in result['issues']])

    def test_duplicate_timestamp_with_equivalent_offset(self):
        source = fixture()
        source['bars'][1]['interval_end'] = '2026-09-06T04:00:00+00:00'
        reasons = [i['reason'] for i in audit(source, as_of=AS_OF)['issues']]
        self.assertIn('duplicate_timestamp', reasons)

    def test_duplicate_daily_and_weekly_periods(self):
        for scope in ('daily', 'weekly'):
            with self.subTest(scope=scope):
                source = fixture()
                source[scope].append(deepcopy(source[scope][0]))
                self.assertIn('duplicate_period', [i['reason'] for i in audit(source, as_of=AS_OF)['issues']])

    def test_unassigned_dates_and_false_complete_claim(self):
        source = fixture()
        source['daily'][0]['source_calendar_dates'] = ['2026-09-05']
        source['daily'][0]['missing_calendar_dates'] = ['2026-09-04']
        source['daily'][0]['session_coverage_complete'] = True
        reasons = [i['reason'] for i in audit(source, as_of=AS_OF)['issues']]
        self.assertIn('observed_calendar_dates_not_assigned', reasons)
        self.assertIn('complete_flag_with_declared_missing_dates', reasons)

    def test_upstream_ready_does_not_promote_calendar(self):
        source = fixture()
        for scope in ('daily', 'weekly'):
            source[scope][0]['session_coverage_complete'] = True
        result = audit(source, as_of=AS_OF)
        self.assertEqual(result['validation_status'], 'PASS')
        self.assertEqual(result['coverage_acceptance'], 'UNVERIFIED')
        self.assertFalse(result['accepted_pointer_promotion'])

    def test_irrelevant_payload_is_not_read_or_copied(self):
        source = fixture()
        expected = audit(source, as_of=AS_OF)
        source['private_upstream_payload'] = object()
        for scope in ('bars', 'daily', 'weekly'):
            source[scope][0]['unused_source_measurement'] = object()
        self.assertEqual(expected, audit(source, as_of=AS_OF))
        json.dumps(expected)

    def test_empty_lists_and_invalid_identity_cannot_pass(self):
        source = fixture()
        source.update(bars=[], daily=[], weekly=[], secid='SiZ6')
        self.assertEqual(audit(source, as_of=AS_OF)['validation_status'], 'FAIL')
        source = fixture()
        source['secid'] = []
        self.assertEqual(audit(source, as_of=AS_OF)['validation_status'], 'FAIL')

    def test_weekly_overlap_and_unassigned_day(self):
        source = fixture()
        second = deepcopy(source['weekly'][0])
        second['week_start'] = '2026-09-05'
        source['weekly'].append(second)
        self.assertIn('daily_period_assigned_to_multiple_weeks',
                      [i['reason'] for i in audit(source, as_of=AS_OF)['issues']])
        source = fixture()
        source['weekly'][0]['week_start'] = '2026-09-07'
        self.assertIn('observed_daily_periods_not_assigned',
                      [i['reason'] for i in audit(source, as_of=AS_OF)['issues']])

    def test_cli_hash_gate_and_exclusive_report(self):
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'dataset.json'
            output = root / 'report.json'
            raw = json.dumps(fixture()).encode()
            source.write_bytes(raw)
            base = [sys.executable, '-m', 'moex_data.rub_price_history_audit',
                    '--dataset', str(source), '--as-of', AS_OF, '--output', str(output),
                    '--expected-sha256']
            bad = subprocess.run(base + ['0' * 64], capture_output=True)
            self.assertNotEqual(bad.returncode, 0)
            self.assertFalse(output.exists())
            good = subprocess.run(base + [hashlib.sha256(raw).hexdigest()], capture_output=True)
            self.assertEqual(good.returncode, 0, good.stderr.decode())
            before = output.read_bytes()
            again = subprocess.run(base + [hashlib.sha256(raw).hexdigest()], capture_output=True)
            self.assertNotEqual(again.returncode, 0)
            self.assertEqual(output.read_bytes(), before)
            self.assertEqual(source.read_bytes(), raw)


if __name__ == '__main__':
    unittest.main()

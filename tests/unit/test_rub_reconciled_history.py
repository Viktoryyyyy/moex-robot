import unittest
from datetime import datetime, timedelta

from moex_data.rub_reconciled_history import combine, previews


class ReconciledHistoryTests(unittest.TestCase):
    def bars(self):
        start = datetime.fromisoformat('2026-08-18T09:00:00+03:00')
        return [dict(interval_end=(start+timedelta(minutes=5*i)).isoformat(),
            open='1',high='2',low='1',close='2',volume='3',value='4',num_trades='1',
            source_id='raw',availability_ts='2026-09-06T18:00:00+03:00') for i in range(1,13)]

    def render(self, bars, empty=None):
        return previews(bars, empty or set(), datetime.fromisoformat('2026-09-06T19:00:00+03:00'))

    def test_right_label_and_causal_availability(self):
        result=self.render(self.bars())
        hour=next(r for r in result if r['timeframe']=='H1')
        self.assertEqual(hour['period'],'2026-08-18T10:00:00+03:00')
        self.assertEqual(hour['coverage_status'],'FULL_CLOCK_HOUR')
        self.assertEqual(hour['availability_ts'],'2026-09-06T19:00:00+03:00')
        self.assertEqual(hour['volume'],'36')

    def test_unknown_additive_not_silently_summed(self):
        bars=self.bars();bars[5]['value']=None;bars[5]['num_trades']=None
        for row in self.render(bars):
            self.assertIsNone(row['value']);self.assertIsNone(row['num_trades'])
            self.assertFalse(row['model_acceptance_granted'])

    def test_partial_hour_and_empty_coverage(self):
        bars=self.bars();missing=datetime.fromisoformat(bars.pop(5)['interval_end'])
        hour=next(r for r in self.render(bars) if r['timeframe']=='H1')
        self.assertEqual(hour['coverage_status'],'OBSERVED_WINDOW_ONLY')
        hour=next(r for r in self.render(bars,{missing}) if r['timeframe']=='H1')
        self.assertEqual(hour['coverage_status'],'FULL_CLOCK_HOUR')
        self.assertEqual(hour['source_row_count'],11)

    def test_daily_weekly_never_claim_session_completeness(self):
        for row in self.render(self.bars()):
            if row['timeframe']!='H1':
                self.assertEqual(row['coverage_status'],'OBSERVED_WINDOW_ONLY')
            self.assertFalse(row['session_calendar_attested'])
            if row['timeframe']=='W1':self.assertFalse(row['week_complete'])

    def test_wrong_contract_and_future_bar_rejected(self):
        row=dict(ts='2026-08-18T10:00:00+03:00',secid='SiU6',ingest_ts='2026-08-18T09:00:00+03:00')
        with self.assertRaises(ValueError):combine([row],[],'CRU6')
        with self.assertRaises(ValueError):combine([row],[],'SiU6')


if __name__=='__main__':unittest.main()

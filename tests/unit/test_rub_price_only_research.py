import copy
import unittest
from datetime import date,timedelta

from moex_features.rub_price_only_research import build,calculate


class PriceOnlyTests(unittest.TestCase):
    def rows(self,count=25):
        rows=[];day=date(2026,7,20)
        while len(rows)<count:
            if day.weekday()<5:
                value=100+len(rows)
                rows.append(dict(period=str(day),open=str(value),high=str(value+1),low=str(value-1),close=str(value),availability_ts=str(day)+'T23:59:59+03:00'))
            day+=timedelta(days=1)
        return rows

    def dataset(self):
        return dict(schema_version='rub_exchange_history_research.v1',secid='SiU6',weekly=[],daily=[dict(r,trading_day=r['period'],session_coverage_complete=True,iss_daily_prices_match=True,volume='999') for r in self.rows()])

    def test_ema_seed_atr_and_prior_range_percentile(self):
        result=calculate(self.rows(),'D1')
        self.assertIsNone(result[18]['ema20']);self.assertEqual(result[19]['ema20'],109.5)
        self.assertAlmostEqual(result[20]['ema20'],110.5)
        self.assertEqual(result[13]['atr14'],2)
        self.assertIsNone(result[19]['range_percentile20']);self.assertEqual(result[20]['range_percentile20'],50)
        self.assertIsNotNone(result[20]['realized_volatility20_annualized'])

    def test_future_append_does_not_change_prefix(self):
        rows=self.rows(30)
        self.assertEqual(calculate(rows[:25],'D1'),calculate(rows,'D1')[:25])

    def test_pivot_waits_for_two_later_bars(self):
        rows=self.rows(6)
        for r in rows:r.update(open='100',close='100',high='101',low='99')
        rows[2]['high']='105'
        result=calculate(rows,'D1')
        self.assertIsNone(result[3]['confirmed_swing_high'])
        self.assertEqual(result[4]['confirmed_swing_high']['pivot_period'],rows[2]['period'])
        self.assertEqual(result[4]['confirmed_swing_high']['confirmed_period'],rows[4]['period'])
        self.assertEqual(result[4]['confirmed_swing_high']['availability_ts'],rows[4]['availability_ts'])

    def test_volume_fields_have_no_effect(self):
        d=self.dataset();before=build(d,'2026-09-06T23:00:00+03:00')
        for row in d['daily']:row.update(volume=None,value='invalid',num_trades=-100,iss_daily_volume_match=False)
        self.assertEqual(before,build(d,'2026-09-06T23:00:00+03:00'))
        self.assertFalse(before['volume_dependent_features_enabled'])

    def test_asof_does_not_use_future_acquisition(self):
        d=self.dataset()
        for row in d['daily']:row['availability_ts']='2026-09-06T20:00:00+03:00'
        result=build(d,'2026-08-31T23:59:59+03:00')
        self.assertEqual(result['features']['D1']['rows'],[])
        with self.assertRaises(ValueError):build(d,'2026-09-06T20:00:00')

    def test_price_failure_resets_warmup(self):
        d=self.dataset();d['daily'][10]['iss_daily_prices_match']=False
        result=build(d,'2026-09-06T23:00:00+03:00')
        self.assertEqual(result['features']['D1']['latest']['observations'],14)
        self.assertIsNone(result['features']['D1']['latest']['ema20'])

    def test_missing_period_resets_warmup(self):
        d=self.dataset();d['daily'].pop(10)
        result=build(d,'2026-09-06T23:00:00+03:00')
        self.assertEqual(result['features']['D1']['latest']['observations'],14)

    def test_duplicate_and_bad_ohlc_rejected(self):
        d=self.dataset();d['daily'].append(copy.deepcopy(d['daily'][0]))
        with self.assertRaises(ValueError):build(d,'2026-09-06T23:00:00+03:00')
        rows=self.rows();rows[-1]['high']='1'
        with self.assertRaises(ValueError):calculate(rows,'D1')


if __name__=='__main__':unittest.main()

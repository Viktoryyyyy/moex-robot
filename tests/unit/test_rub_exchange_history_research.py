import copy
import unittest
from datetime import datetime,timedelta
from unittest.mock import patch

from moex_data.rub_exchange_history_research import candles, repair, trading_day, required_dates, combine, regroup


class ExchangeHistoryTests(unittest.TestCase):
    def fixture(self):
        start=datetime.fromisoformat('2026-08-07T06:50:00+03:00')
        def evidence(interval,count):
            rows=[]
            for i in range(count):
                begin=start+timedelta(minutes=i*interval)
                rows.append([begin.isoformat(),(begin+timedelta(minutes=interval)-timedelta(seconds=1)).isoformat(),1,1,1,1,10*interval])
            return dict(secid='SiU6',day='2026-08-07',edge='open',
                url='https://apim.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/SiU6/candles.json',
                query={'interval':interval,'from':start.isoformat(),'till':'2026-08-07T07:29:59+03:00'},
                checked_at_utc='2026-09-06T18:00:00+00:00',
                payload={'candles':{'columns':['begin','end','open','high','low','close','volume'],'data':rows}})
        minute=evidence(1,40);ten=evidence(10,4)
        bars=[]
        for clock in ('07:00','07:05','07:10'):
            bars.append(dict(interval_end='2026-08-07T'+clock+':00+03:00',open='1',high='1',low='1',close='1',
                volume='49' if clock=='07:05' else '50',value='500',num_trades='10',source_id='raw',availability_ts='2026-09-06T12:00:00+00:00'))
        dataset=dict(secid='SiU6',bars=bars,empty_intervals=[])
        audit_result=dict(dates=[dict(day='2026-08-07',issues=['EDGE_OHLCV_MISMATCH:2026-08-07T07:05:00+03:00'])])
        return dataset,[copy.deepcopy(minute)],[minute,ten],audit_result

    def test_replacement_preserves_raw_and_unknown_fields(self):
        d,b,c,a=self.fixture();original=copy.deepcopy(d)
        with patch('moex_data.rub_exchange_history_research.audit',side_effect=[a,{'dates':[]}]):
            revised,changes,_=repair(d,b,c)
        self.assertEqual(d,original);self.assertEqual(len(changes),1)
        row=revised['bars'][1]
        self.assertEqual(row['volume'],'50');self.assertIsNone(row['value']);self.assertIsNone(row['num_trades'])
        self.assertEqual(changes[0]['original']['volume'],'49')
        self.assertEqual(row['source_id'],'moex_iss_forts_rfud_1m')

    def test_changed_repeat_blocks(self):
        d,b,c,a=self.fixture();c[0]['payload']['candles']['data'][10][-1]=11
        with patch('moex_data.rub_exchange_history_research.audit',return_value=a):
            with self.assertRaisesRegex(ValueError,'stable'):repair(d,b,c)

    def test_ten_minute_conflict_blocks(self):
        d,b,c,a=self.fixture();c[1]['payload']['candles']['data'][1][-1]=99
        with patch('moex_data.rub_exchange_history_research.audit',return_value=a):
            with self.assertRaisesRegex(ValueError,'10m'):repair(d,b,c)

    def test_neighbor_conflict_blocks(self):
        d,b,c,a=self.fixture();d['bars'][0]['volume']='49'
        with patch('moex_data.rub_exchange_history_research.audit',return_value=a):
            with self.assertRaisesRegex(ValueError,'adjacent'):repair(d,b,c)

    def test_duplicate_or_wrong_source_rejected(self):
        _,_,c,_=self.fixture();entry=c[0]
        with self.assertRaises(ValueError):candles(entry,'CRU6',1)
        entry['payload']['candles']['data'].append(entry['payload']['candles']['data'][0])
        with self.assertRaises(ValueError):candles(entry,'SiU6',1)

    def test_weekend_monday_and_friday_evening_semantics(self):
        self.assertEqual(trading_day('2026-07-25'),'2026-07-27')
        self.assertEqual(trading_day('2026-07-26'),'2026-07-27')
        self.assertEqual(trading_day('2026-07-24'),'2026-07-24')
        self.assertEqual(required_dates('2026-07-20'),['2026-07-18','2026-07-19','2026-07-20'])
        self.assertEqual(required_dates('2026-08-17'),['2026-08-17'])

    def test_unknown_additive_remains_unknown(self):
        d,_,_,_=self.fixture();d['bars'][1]['value']=None
        result=combine(d['bars']);self.assertIsNone(result['value']);self.assertEqual(result['volume'],'149')

    def test_complete_week_and_trailing_weekend_are_distinct(self):
        d,_,cross,_=self.fixture();template=d['bars'][0]
        dates=['2026-08-'+str(i) for i in range(17,22)]+['2026-09-05']
        d['bars']=[dict(template,interval_end=day+'T12:00:00+03:00') for day in dates]
        coverage={'dates':[dict(day=day,status='CALENDAR_DATE_OHLCV_COVERAGE_VERIFIED') for day in dates]}
        entry=copy.deepcopy(cross[0]);entry['query']={'from':'2026-07-20','till':'2026-09-05','interval':24}
        entry['payload']['candles']['data']=[[day+' 00:00:00',day+' 23:49:55',1,1,1,1,50] for day in dates[:-1]]
        daily,weekly=regroup(d,coverage,[entry],datetime.fromisoformat('2026-09-06T18:00:00+00:00'))
        self.assertTrue(weekly[0]['session_coverage_complete'])
        self.assertFalse(weekly[1]['session_coverage_complete'])
        trailing=daily[-1]
        self.assertEqual(trailing['trading_day'],'2026-09-07')
        self.assertEqual(trailing['missing_calendar_dates'],['2026-09-06','2026-09-07'])
        self.assertFalse(trailing['model_acceptance_granted'])
        self.assertEqual(trailing['acceptance'],'RESEARCH_ONLY')

    def test_daily_reference_requires_completed_query_date(self):
        _,_,c,_=self.fixture();entry=c[0]
        entry['query']={'from':'2026-09-06','till':'2026-09-06','interval':24}
        with self.assertRaisesRegex(ValueError,'unfinished'):candles(entry,'SiU6',24)


if __name__=='__main__':unittest.main()

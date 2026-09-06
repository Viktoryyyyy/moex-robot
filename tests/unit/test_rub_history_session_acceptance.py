import unittest
from moex_data.rub_history_session_acceptance import schedule, edge_check, audit


class SessionAcceptanceTests(unittest.TestCase):
    def fixture(self):
        start,close=schedule('2026-07-22')
        entry=dict(secid='SiU6',day='2026-07-22',edge='open',
            url='https://apim.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/SiU6/candles.json',
            query={'interval':1,'from':'2026-07-22 06:45:00','till':'2026-07-22 07:10:00'},
            checked_at_utc='2026-09-06T18:00:00+00:00',
            payload={'candles':{'columns':['begin','end','open','high','low','close','volume'],
                'data':[['2026-07-22 07:00:00','2026-07-22 07:00:59',1,2,1,2,10]]}})
        from moex_data.rub_history_gap_reconciliation import timestamp
        bars={timestamp('2026-07-22T07:05:00+03:00'):dict(open='1',high='2',low='1',close='2',volume='10')}
        return entry,bars,start,close

    def test_reviewed_closures(self):
        for day in ('2026-08-01','2026-08-02','2026-08-15','2026-08-16'):
            self.assertIsNone(schedule(day))
        self.assertEqual(schedule('2026-08-08')[0].hour,9)

    def test_no_calendar_extrapolation(self):
        with self.assertRaises(ValueError):schedule('2026-09-14')

    def test_empty_auction_and_adjacent_match(self):
        empty,issues,matches=edge_check(*self.fixture())
        self.assertEqual(issues,[]);self.assertEqual(matches,1)
        self.assertIn('06:55',[t.strftime('%H:%M') for t in empty])

    def test_price_conflict_blocks(self):
        entry,bars,start,close=self.fixture()
        next(iter(bars.values()))['close']='1'
        self.assertTrue(any('MISMATCH' in x for x in edge_check(entry,bars,start,close)[1]))

    def test_wrong_source_or_duplicate_minute_rejected(self):
        entry,bars,start,close=self.fixture();entry['url']+='bad'
        with self.assertRaises(ValueError):edge_check(entry,bars,start,close)
        entry,bars,start,close=self.fixture();entry['payload']['candles']['data']*=2
        with self.assertRaises(ValueError):edge_check(entry,bars,start,close)

    def test_traded_missing_bar_blocks(self):
        entry,bars,start,close=self.fixture()
        issues=edge_check(entry,{},start,close)[1]
        self.assertTrue(any('MISSING_TRADED_EDGE_BAR' in x for x in issues))
        self.assertIn('NO_ADJACENT_RAW_CORROBORATION',issues)

    def test_missing_queries_fail_closed(self):
        with self.assertRaises(ValueError):audit(dict(secid='SiU6',bars=[],empty_intervals=[]),[])


if __name__=='__main__':unittest.main()

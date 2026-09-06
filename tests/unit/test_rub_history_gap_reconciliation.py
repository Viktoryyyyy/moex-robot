import copy
import unittest
from datetime import datetime,timedelta

from moex_data.rub_history_gap_reconciliation import reconcile


def evidence(*,span=1,trades=False):
    start=datetime.fromisoformat('2026-08-03T10:00:00+03:00')
    left=start+timedelta(minutes=5);right=left+timedelta(minutes=5*(span+1))
    columns=['begin','end','open','high','low','close','volume']
    data=[]
    for i in range(int((right-start).total_seconds()/60)):
        if 5 <= i < 5*(span+1) and not trades:continue
        begin=start+timedelta(minutes=i)
        data.append([begin.isoformat(),(begin+timedelta(seconds=59)).isoformat(),100,102,99,101,2])
    return dict(secid='SiU6',left_end=left.isoformat(),right_end=right.isoformat(),
        checked_at_utc='2026-09-06T17:00:00+00:00',
        url='https://apim.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/SiU6/candles.json',
        query={'from':start.isoformat(),'till':(right-timedelta(seconds=1)).isoformat(),'interval':1},
        neighbors=[dict(end=end.isoformat(),open=100,high=102,low=99,close=101,volume=10) for end in (left,right)],
        minute_payload={'candles':{'columns':columns,'data':data}})


class GapReconciliationTests(unittest.TestCase):
    def test_adjacent_empty_slots_are_verified_together(self):
        result=reconcile(evidence(span=2))
        self.assertEqual(len(result['intervals']),2)
        self.assertTrue(all(row['status']=='CORROBORATED_EMPTY' for row in result['intervals']))

    def test_actual_trades_create_explicit_secondary_source_candidate(self):
        entry=evidence(trades=True);before=copy.deepcopy(entry)
        result=reconcile(entry);candidate=result['intervals'][0]
        self.assertEqual(candidate['status'],'OHLCV_RECOVERY_CANDIDATE')
        self.assertEqual(candidate['ohlcv']['volume'],'10')
        self.assertEqual(candidate['availability_ts'],entry['checked_at_utc'])
        self.assertIn('open_interest',candidate['unknown_fields'])
        self.assertFalse(result['raw_data_changed'])
        self.assertFalse(result['model_acceptance_granted'])
        self.assertEqual(entry,before)

    def test_partial_minutes_do_not_create_repair(self):
        entry=evidence(trades=True);del entry['minute_payload']['candles']['data'][7]
        result=reconcile(entry)
        self.assertEqual(result['status'],'UNRESOLVED')
        self.assertNotIn('ohlcv',result['intervals'][0])

    def test_adjacent_mismatch_rejects_even_empty_gap(self):
        entry=evidence();entry['neighbors'][0]['volume']=11
        with self.assertRaisesRegex(ValueError,'OHLCV mismatch'):reconcile(entry)

    def test_duplicate_minute_rejected(self):
        entry=evidence();entry['minute_payload']['candles']['data'].append(entry['minute_payload']['candles']['data'][0])
        with self.assertRaisesRegex(ValueError,'duplicate'):reconcile(entry)

    def test_query_and_source_scope_are_checked(self):
        for field in ('url','query','checked_at_utc'):
            with self.subTest(field=field):
                entry=evidence()
                entry[field]={'url':entry['url'].replace('SiU6','CRU6'),
                    'query':{**entry['query'],'interval':10},
                    'checked_at_utc':'2026-08-03T06:00:00+00:00'}[field]
                with self.assertRaises(ValueError):reconcile(entry)

    def test_nonfinite_prices_are_rejected(self):
        entry=evidence();entry['minute_payload']['candles']['data'][0][2]=float('nan')
        with self.assertRaises(ValueError):reconcile(entry)


if __name__=='__main__':unittest.main()

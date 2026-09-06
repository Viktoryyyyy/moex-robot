from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

from moex_data.empty_tradestats_interval import EmptyIntervalError, reconcile_empty_interval
from src.moex_research.intelligence.usdrubf_live_shadow_bridge import (
    LiveShadowBridgeError, build_closed_15m_bars, build_live_decision_input,
)

END = datetime.fromisoformat('2026-09-06T12:05:00+03:00')
COLUMNS = ['begin', 'end', 'open', 'high', 'low', 'close', 'volume']


def bar(end, price=85.88, volume=10):
    return dict(end=end, open=price, high=price+.01, low=price-.01, close=price, volume=volume)


def fixture():
    bars = [bar(END-timedelta(minutes=20)), bar(END-timedelta(minutes=15)),
            bar(END-timedelta(minutes=10)), bar(END-timedelta(minutes=5), volume=130),
            bar(END+timedelta(minutes=5), volume=14)]
    rows = []
    for start, volume in [(END-timedelta(minutes=10), 130), (END, 14)]:
        rows.append([start.strftime('%Y-%m-%d %H:%M:%S'),
                     (start+timedelta(seconds=59)).strftime('%Y-%m-%d %H:%M:%S'),
                     85.88, 85.89, 85.86999999999999, 85.88, volume])
    # Avoid binary rounding in source-like fixtures.
    for item in bars:
        item['high'],item['low'] = 85.89,85.87
    for row in rows: row[4] = 85.87
    return bars, {'candles': {'columns': COLUMNS, 'data': rows}}


class EmptyIntervalTests(unittest.TestCase):
    def test_exact_neighbors_corroborate_without_creating_prices(self):
        bars,payload=fixture()
        original=deepcopy((bars,payload))
        proof=reconcile_empty_interval(missing_end=END,neighboring_bars=bars,minute_payload=payload)
        self.assertEqual(proof['status'],'CORROBORATED_EMPTY')
        self.assertFalse(proof['synthetic_ohlc_created'])
        self.assertFalse(proof['oi_inferred'])
        self.assertEqual((bars,payload),original)

    def test_absence_without_matching_neighbors_is_not_proof(self):
        for fault in ['empty','wrong_volume','wrong_price','duplicate','missing_column','trade_in_gap']:
            with self.subTest(fault=fault):
                bars,payload=fixture();block=payload['candles']
                if fault=='empty': block['data']=[]
                if fault=='wrong_volume': block['data'][0][-1]=129
                if fault=='wrong_price': block['data'][0][2]=85.879
                if fault=='duplicate': block['data'].append(block['data'][0][:])
                if fault=='missing_column': block['columns']=COLUMNS[:-1]
                if fault=='trade_in_gap':
                    row=block['data'][1][:]
                    row[0]='2026-09-06 12:00:00';row[1]='2026-09-06 12:00:59'
                    block['data'].append(row)
                with self.assertRaises((EmptyIntervalError,ValueError)):
                    reconcile_empty_interval(missing_end=END,neighboring_bars=bars,minute_payload=payload)

    def test_existing_stats_row_cannot_be_declared_empty(self):
        bars,payload=fixture();bars.append(bar(END))
        with self.assertRaises(EmptyIntervalError):
            reconcile_empty_interval(missing_end=END,neighboring_bars=bars,minute_payload=payload)

    def test_sparse_15m_uses_only_actual_prices_and_volume(self):
        bars,payload=fixture()
        with self.assertRaises(LiveShadowBridgeError): build_closed_15m_bars(bars)
        proof=reconcile_empty_interval(missing_end=END,neighboring_bars=bars,minute_payload=payload)
        checked=END+timedelta(hours=1);proof['checked_at']=checked.isoformat()
        result=build_closed_15m_bars(bars,empty_intervals={END:proof})
        self.assertEqual(len(result),2)
        self.assertEqual(result[-1]['volume'],144)
        self.assertEqual(result[-1]['open'],85.88)
        self.assertEqual(result[-1]['source_available_at'],checked)
        self.assertEqual(result[-1]['corroborated_empty_5m_ends'],[END.isoformat()])
        self.assertEqual(len(bars),5)

    def test_future_evidence_cannot_enter_decision(self):
        bars,payload=fixture()
        proof=reconcile_empty_interval(missing_end=END,neighboring_bars=bars,minute_payload=payload)
        checked=END+timedelta(hours=1);proof['checked_at']=checked.isoformat()
        prior=[{**b,'end':b['end']-timedelta(days=1)} for b in bars]
        with self.assertRaisesRegex(LiveShadowBridgeError,'not available'):
            build_live_decision_input(current_session_bars=bars,prior_session_bars=prior,
                                      wall_clock_as_of=checked-timedelta(seconds=1),empty_intervals={END:proof})
        inputs=build_live_decision_input(current_session_bars=bars,prior_session_bars=prior,
                                        wall_clock_as_of=checked,empty_intervals={END:proof})
        self.assertEqual(inputs.as_of_timestamp,checked)
        self.assertEqual(inputs.ema_3_19_ai.available_at,checked)

    def test_equivalent_utc_bar_timestamps_use_the_same_evidence(self):
        bars,payload=fixture()
        proof=reconcile_empty_interval(missing_end=END,neighboring_bars=bars,minute_payload=payload)
        proof['checked_at']=(END+timedelta(hours=1)).isoformat()
        utc_bars=[{**b,'end':b['end'].astimezone(timezone.utc)} for b in bars]
        result=build_closed_15m_bars(utc_bars,empty_intervals={END:proof})
        self.assertEqual(result[-1]['volume'],144)


if __name__=='__main__': unittest.main()

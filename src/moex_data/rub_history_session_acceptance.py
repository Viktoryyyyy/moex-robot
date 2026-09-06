"""Bounded calendar-date coverage audit; never grants exchange D1 acceptance."""
import argparse
import hashlib
import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from moex_data.rub_history_gap_reconciliation import timestamp, aggregate, FIELDS

SOURCES = ['https://www.moex.com/n101980?nt=112', 'https://www.moex.com/n95564',
           'https://www.moex.com/n96571', 'https://www.moex.com/n103333?nt=107']
CLOSED = {'2026-08-01', '2026-08-02', '2026-08-15', '2026-08-16'}


def schedule(day):
    if not '2026-07-20' <= day <= '2026-09-05':
        raise ValueError('outside reviewed calendar scope')
    parsed = date.fromisoformat(day)
    if day in CLOSED:
        return None
    weekend = parsed.weekday() >= 5
    return (timestamp(day + ('T09:50:00+03:00' if weekend else 'T06:50:00+03:00')),
            timestamp(day + ('T19:00:00+03:00' if weekend else 'T23:50:00+03:00')))


def edge_check(entry, bars, start, close):
    secid = entry['secid']
    expected = f'https://apim.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/{secid}/candles.json'
    if entry['url'] != expected or entry['query']['interval'] != 1:
        raise ValueError('wrong minute source')
    left, right = timestamp(entry['query']['from']), timestamp(entry['query']['till'])
    if not timedelta(minutes=15) <= right-left <= timedelta(minutes=30):
        raise ValueError('unbounded edge query')
    if timestamp(entry['checked_at_utc'], aware=True) <= right:
        raise ValueError('unfinished query window')
    anchor = start if entry['edge'] == 'open' else close
    if entry['edge'] not in ('open', 'close') or not left < anchor < right:
        raise ValueError('query does not straddle session boundary')
    block=entry['payload']['candles'];columns=block['columns']
    if len(set(columns)) != len(columns) or not set(FIELDS+('begin','end')).issubset(columns):
        raise ValueError('invalid minute schema')
    minutes={}
    for values in block['data']:
        if len(values)!=len(columns):raise ValueError('invalid row width')
        row=dict(zip(columns,values));begin=timestamp(row['begin'])
        if begin in minutes or begin.second or begin.microsecond or not left <= begin <= right:
            raise ValueError('duplicate or out of window minute')
        if timestamp(row['end']) != begin+timedelta(seconds=59):raise ValueError('partial minute')
        values={f:Decimal(str(row[f])) for f in FIELDS}
        if any(not n.is_finite() or n<=0 for n in values.values()):raise ValueError('invalid number')
        if not values['low'] <= min(values['open'],values['close']) <= max(values['open'],values['close']) <= values['high']:
            raise ValueError('invalid OHLC')
        minutes[begin]=values
    empty=[];issues=[];matched=0
    if any(not start <= t < close for t in minutes):issues.append('TRADES_OUTSIDE_SCHEDULE')
    end=left.replace(second=0,microsecond=0)+timedelta(minutes=5)
    if end.minute%5:raise ValueError('unaligned query')
    while end <= right+timedelta(seconds=1):
        selected=[v for t,v in sorted(minutes.items()) if end-timedelta(minutes=5)<=t<end]
        if end in bars:
            if not selected or any(aggregate(selected)[f] != Decimal(bars[end][f]) for f in FIELDS):
                issues.append('EDGE_OHLCV_MISMATCH:'+end.isoformat())
            else:matched+=1
        elif start < end <= close:
            if selected:issues.append('MISSING_TRADED_EDGE_BAR:'+end.isoformat())
            else:empty.append(end)
        end+=timedelta(minutes=5)
    if matched==0:issues.append('NO_ADJACENT_RAW_CORROBORATION')
    return empty,issues,matched


def audit(dataset, evidence):
    if dataset['secid'] not in ('SiU6','CRU6'):raise ValueError('unsupported contract')
    rows=dataset['bars'];bars={timestamp(r['interval_end'],aware=True):r for r in rows}
    if len(bars)!=len(rows):raise ValueError('duplicate source bars')
    empty={timestamp(t,aware=True) for t in dataset['empty_intervals']}
    if empty & bars.keys():raise ValueError('empty interval overlaps bar')
    days=[]
    for offset in range(48):
        day=(date(2026,7,20)+timedelta(days=offset)).isoformat()
        session=schedule(day);observed={t for t in bars if t.date().isoformat()==day}
        if session is None:
            days.append(dict(day=day,status='UNEXPECTED_CLOSED_DAY_DATA' if observed else 'VERIFIED_CLOSED_DATE'))
            continue
        start,close=session
        entries=[e for e in evidence if e['secid']==dataset['secid'] and e['day']==day]
        if len(entries)!=2 or {e['edge'] for e in entries}!={'open','close'}:
            raise ValueError('exactly two boundary queries required per open date')
        covered=set(empty);issues=[];matches=0
        for entry in entries:
            extra,problems,count=edge_check(entry,bars,start,close)
            covered.update(extra);issues.extend(problems);matches+=count
        expected={start+timedelta(minutes=5*i) for i in range(1,int((close-start).total_seconds()/300)+1)}
        missing=expected-observed-covered
        if observed-expected:issues.append('BAR_OUTSIDE_CALENDAR')
        if missing:issues.append('UNCOVERED_INTERVALS')
        days.append(dict(day=day,status='CALENDAR_DATE_OHLCV_COVERAGE_VERIFIED' if not issues else 'BLOCKED',
            issues=issues,missing_intervals=sorted(t.isoformat() for t in missing),
            corroborated_edge_bars=matches,edge_empty_count=len((covered-empty)&expected),
            exchange_trading_day=(date.fromisoformat(day)+timedelta(days=7-date.fromisoformat(day).weekday())).isoformat()
                if date.fromisoformat(day).weekday()>=5 else day))
    return dict(secid=dataset['secid'],sources=SOURCES,dates=days,
        verified_calendar_dates=sum(d['status']=='CALENDAR_DATE_OHLCV_COVERAGE_VERIFIED' for d in days),
        closed_dates=sum(d['status']=='VERIFIED_CLOSED_DATE' for d in days),
        exchange_d1_accepted=False,exchange_w1_accepted=False,model_acceptance_granted=False,
        reason='Weekend sessions belong to the following trading day; calendar-date previews require regrouping.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset',type=Path,required=True)
    parser.add_argument('--evidence',type=Path,required=True)
    args=parser.parse_args()
    raw=args.dataset.read_bytes();proof=args.evidence.read_bytes()
    result=audit(json.loads(raw),json.loads(proof))
    result.update(dataset_sha256=hashlib.sha256(raw).hexdigest(),evidence_sha256=hashlib.sha256(proof).hexdigest())
    print(json.dumps(result,indent=2))

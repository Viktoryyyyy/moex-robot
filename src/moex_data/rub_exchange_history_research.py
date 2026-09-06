"""Evidence-bound ISS conflict replacements and exchange-day research aggregates."""
import argparse
import copy
import hashlib
import json
from datetime import date, timedelta, datetime, timezone
from decimal import Decimal
from pathlib import Path

from moex_data.rub_history_gap_reconciliation import FIELDS, aggregate, timestamp
from moex_data.rub_history_session_acceptance import audit, CLOSED


def sha(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def candles(entry, secid, interval):
    url=f'https://apim.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/{secid}/candles.json'
    query=entry['query']
    if entry['secid']!=secid or entry['url']!=url or query['interval']!=interval:
        raise ValueError('wrong source identity')
    left=timestamp(query['from']);right=timestamp(query['till'])
    if interval==24:right=right+timedelta(days=1)-timedelta(seconds=1)
    if right<left or timestamp(entry['checked_at_utc'],aware=True)<=right:
        raise ValueError('unfinished source window')
    block=entry.get('payload',entry.get('minute_payload'))['candles'];columns=block['columns']
    if len(set(columns))!=len(columns) or not set(FIELDS+('begin','end')).issubset(columns):
        raise ValueError('invalid candle schema')
    output={}
    for values in block['data']:
        if len(values)!=len(columns):raise ValueError('invalid candle width')
        row=dict(zip(columns,values));begin=timestamp(row['begin']);end=timestamp(row['end'])
        duration=timedelta(days=1) if interval==24 else timedelta(minutes=interval)
        valid_end = (begin <= end < begin+duration and begin.hour==0 and begin.minute==0) if interval==24 else end==begin+duration-timedelta(seconds=1)
        if begin in output or not left<=begin<=right or not valid_end:
            raise ValueError('duplicate, partial or out-of-window candle')
        if begin.second or begin.microsecond or (interval<24 and begin.minute%interval):
            raise ValueError('misaligned candle')
        numbers={k:Decimal(str(row[k])) for k in FIELDS}
        if any(not v.is_finite() or v<=0 for v in numbers.values()):raise ValueError('invalid OHLCV')
        if not numbers['low']<=min(numbers['open'],numbers['close'])<=max(numbers['open'],numbers['close'])<=numbers['high']:
            raise ValueError('invalid OHLC ordering')
        output[begin]=numbers
    return output


def select(entries,secid,interval,day):
    selected=[e for e in entries if e['secid']==secid and e['query']['interval']==interval and e['query']['from'][:10]==day]
    if len(selected)!=1:raise ValueError('one exact crosscheck query required')
    return selected[0]


def repair(dataset, boundary, crosscheck):
    result=copy.deepcopy(dataset);secid=result['secid']
    original=audit(dataset,boundary)
    bars={timestamp(r['interval_end']):r for r in result['bars']}
    originals=copy.deepcopy(bars);changes=[]
    for day in original['dates']:
        for issue in day.get('issues',[]):
            if not issue.startswith('EDGE_OHLCV_MISMATCH:'):
                raise ValueError('only source conflicts may be repaired by this policy')
            end=timestamp(issue.split(':',1)[1]);start=end-timedelta(minutes=5)
            repeated=select(crosscheck,secid,1,day['day']);tens=select(crosscheck,secid,10,day['day'])
            minute=candles(repeated,secid,1);ten=candles(tens,secid,10)
            prior=next(e for e in boundary if e['secid']==secid and e['day']==day['day'] and e['edge']=='open')
            earlier=candles(prior,secid,1)
            stamps=[start+timedelta(minutes=i) for i in range(5)]
            if any(t not in minute or minute[t]!=earlier.get(t) for t in stamps):
                raise ValueError('five complete, stable minutes required')
            for adjacent in (end-timedelta(minutes=5),end+timedelta(minutes=5)):
                selected=[v for t,v in sorted(minute.items()) if adjacent-timedelta(minutes=5)<=t<adjacent]
                if adjacent not in originals or not selected or any(aggregate(selected)[k]!=Decimal(originals[adjacent][k]) for k in FIELDS):
                    raise ValueError('adjacent captured OHLCV does not corroborate replacement')
            ten_start=start.replace(minute=(start.minute//10)*10)
            selected=[v for t,v in sorted(minute.items()) if ten_start<=t<ten_start+timedelta(minutes=10)]
            if len(selected)!=10 or ten.get(ten_start)!=aggregate(selected):
                raise ValueError('independent 10m candle does not match complete minutes')
            replacement=aggregate([minute[t] for t in stamps])
            change=dict(interval_end=end.isoformat(),original=copy.deepcopy(bars[end]),
                replacement={k:str(v) for k,v in replacement.items()},
                evidence_sha256=[sha(prior),sha(repeated),sha(tens)],
                policy='stable_iss_minutes_and_10m_research_override_v1')
            bars[end].update(change['replacement'],source_id='moex_iss_forts_rfud_1m',
                value=None,num_trades=None,original_bar_sha256=sha(change['original']),
                conflict_resolution_sha256=sha(change),
                availability_ts=max(timestamp(bars[end]['availability_ts']),timestamp(repeated['checked_at_utc']),timestamp(tens['checked_at_utc'])).isoformat())
            changes.append(change)
    result['bars']=[bars[t] for t in sorted(bars)]
    checked=audit(result,boundary)
    if any(d['status']=='BLOCKED' for d in checked['dates']):raise ValueError('unresolved coverage after replacements')
    return result,changes,checked


def trading_day(day):
    parsed=date.fromisoformat(day)
    return (parsed+timedelta(days=7-parsed.weekday()) if parsed.weekday()>=5 else parsed).isoformat()


def required_dates(day):
    parsed=date.fromisoformat(day)
    required=[day]
    if parsed.weekday()==0:
        required += [(parsed-timedelta(days=i)).isoformat() for i in (2,1)
                     if (parsed-timedelta(days=i)).isoformat() not in CLOSED]
    return sorted(required)


def combine(rows):
    values=aggregate([{k:Decimal(r[k]) for k in FIELDS} for r in rows])
    result={k:str(v) for k,v in values.items()}
    for field in ('value','num_trades'):
        result[field]=None if any(r.get(field) is None for r in rows) else str(sum(Decimal(r[field]) for r in rows))
    return result


def regroup(dataset, coverage, crosscheck, available):
    secid=dataset['secid'];groups={}
    statuses={d['day']:d['status'] for d in coverage['dates']}
    for row in sorted(dataset['bars'],key=lambda r:timestamp(r['interval_end'])):
        groups.setdefault(trading_day(row['interval_end'][:10]),[]).append(row)
    daily_entry=select(crosscheck,secid,24,'2026-07-20')
    references={t.date().isoformat():r for t,r in candles(daily_entry,secid,24).items()}
    daily=[]
    for day,rows in sorted(groups.items()):
        missing=[d for d in required_dates(day) if statuses.get(d)!='CALENDAR_DATE_OHLCV_COVERAGE_VERIFIED']
        values=combine(rows);reference=references.get(day)
        prices_match=reference is not None and all(Decimal(values[k])==reference[k] for k in FIELDS if k!='volume')
        volume_match=reference is not None and Decimal(values['volume'])==reference['volume']
        daily.append(dict(timeframe='D1',trading_day=day,**values,source_row_count=len(rows),
            source_calendar_dates=sorted({r['interval_end'][:10] for r in rows}),
            missing_calendar_dates=missing,session_coverage_complete=not missing,
            iss_daily_prices_match=prices_match,iss_daily_volume_match=volume_match,
            reference_volume=None if reference is None else str(reference['volume']),
            acceptance='RESEARCH_ONLY',model_acceptance_granted=False,
            availability_ts=max([available,timestamp(daily_entry['checked_at_utc'])]+[timestamp(r['availability_ts']) for r in rows]).isoformat()))
    weeks={}
    for row in daily:
        d=date.fromisoformat(row['trading_day']);key=(d-timedelta(days=d.weekday())).isoformat()
        weeks.setdefault(key,[]).append(row)
    weekly=[]
    for week,rows in sorted(weeks.items()):
        required={(date.fromisoformat(week)+timedelta(days=i)).isoformat() for i in range(5)}
        complete=required=={r['trading_day'] for r in rows} and all(r['session_coverage_complete'] for r in rows)
        weekly.append(dict(timeframe='W1',week_start=week,**combine(rows),
            trading_day_count=len(rows),session_coverage_complete=complete,
            acceptance='RESEARCH_ONLY',model_acceptance_granted=False,
            availability_ts=max(timestamp(r['availability_ts']) for r in rows).isoformat()))
    return daily,weekly


def build(dataset,boundary,crosscheck):
    revised,changes,coverage=repair(dataset,boundary,crosscheck)
    available=max(timestamp(e['checked_at_utc'],aware=True) for e in boundary)
    daily,weekly=regroup(revised,coverage,crosscheck,available)
    return dict(schema_version='rub_exchange_history_research.v1',secid=dataset['secid'],
        input_sha256=dict(dataset=sha(dataset),boundary=sha(boundary),crosscheck=sha(crosscheck)),
        built_at_utc=datetime.now(timezone.utc).isoformat(),changes=changes,bars=revised['bars'],
        coverage=coverage,daily=daily,weekly=weekly,
        model_acceptance_granted=False,accepted_pointer_promotion=False,
        limitation='ISS daily volume semantics remain unreconciled; coherent intraday research replacements do not certify canonical corrections.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('dataset','boundary','crosscheck','output'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args()
    result=build(*[json.loads(getattr(args,n).read_text()) for n in ('dataset','boundary','crosscheck')])
    with args.output.open('x',encoding='utf-8') as stream:json.dump(result,stream,indent=2)
    print(json.dumps(dict(secid=result['secid'],replacements=len(result['changes']),daily=len(result['daily']),
        complete_daily=sum(r['session_coverage_complete'] for r in result['daily']),weekly=len(result['weekly']),
        complete_weekly=sum(r['session_coverage_complete'] for r in result['weekly']))))

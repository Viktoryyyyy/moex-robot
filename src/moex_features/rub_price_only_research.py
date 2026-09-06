"""Causal price-only research features; volume reconciliation is out of scope."""
import argparse
import hashlib
import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path

from moex_data.rub_history_gap_reconciliation import timestamp

POLICY = 'rub_price_only_research.v1'


def next_period(previous, current, frame):
    expected=date.fromisoformat(previous)+timedelta(days=7 if frame=='W1' else 1)
    if frame=='D1':
        while expected.weekday()>=5:expected+=timedelta(days=1)
    return expected.isoformat()==current


def calculate(rows, frame):
    """Rows are a contiguous, already eligible prefix; formulas use prices only."""
    output=[];closes=[];ranges=[];returns=[];tr=[];ema=None;atr=None;high_swing=None;low_swing=None
    for i,row in enumerate(rows):
        opening,high,low,close=[float(row[k]) for k in ('open','high','low','close')]
        if any(not math.isfinite(v) or v<=0 for v in (opening,high,low,close)) or not low<=min(opening,close)<=max(opening,close)<=high:
            raise ValueError('invalid OHLC')
        previous=closes[-1] if closes else None
        current_range=high-low
        tr.append(max(current_range,abs(high-previous),abs(low-previous)) if previous is not None else current_range)
        if previous is not None:returns.append(math.log(close/previous))
        closes.append(close)
        if len(closes)==20:ema=sum(closes)/20
        elif len(closes)>20:ema=close*(2/21)+ema*(19/21)
        if len(tr)==14:atr=sum(tr)/14
        elif len(tr)>14:atr=(atr*13+tr[-1])/14
        rv=None
        if len(returns)>=20:
            sample=returns[-20:];mean=sum(sample)/20
            rv=math.sqrt(sum((v-mean)**2 for v in sample)/19)*math.sqrt(252 if frame=='D1' else 52)
        percentile=None
        if len(ranges)>=20:
            sample=ranges[-20:]
            percentile=100*(sum(v<current_range for v in sample)+0.5*sum(v==current_range for v in sample))/20
        # Confirm the pivot only after two later closed bars. Never backdate it.
        if i>=4:
            pivot=i-2;window=rows[i-4:i+1];candidate=rows[pivot]
            known=max(timestamp(r['availability_ts'],aware=True) for r in rows[:i+1]).isoformat()
            if all(float(candidate['high'])>float(r['high']) for j,r in enumerate(window) if j!=2):
                high_swing=dict(price=float(candidate['high']),pivot_period=candidate['period'],confirmed_period=row['period'],availability_ts=known)
            if all(float(candidate['low'])<float(r['low']) for j,r in enumerate(window) if j!=2):
                low_swing=dict(price=float(candidate['low']),pivot_period=candidate['period'],confirmed_period=row['period'],availability_ts=known)
        bos=None
        if high_swing and previous is not None and previous<=high_swing['price']<close:bos='up'
        if low_swing and previous is not None and previous>=low_swing['price']>close:bos='down'
        output.append(dict(period=row['period'],timeframe=frame,close=close,
            return_1=close/previous-1 if previous is not None else None,
            return_5=close/closes[-6]-1 if len(closes)>=6 else None,
            ema20=ema,atr14=atr,realized_volatility20_annualized=rv,
            range_percentile20=percentile,confirmed_swing_high=high_swing,
            confirmed_swing_low=low_swing,break_of_structure=bos,
            observations=len(closes),availability_ts=max(timestamp(r['availability_ts'],aware=True) for r in rows[:i+1]).isoformat(),
            unavailable=[name for name,value in [('ema20',ema),('atr14',atr),('realized_volatility20',rv),('range_percentile20',percentile)] if value is None]))
        ranges.append(current_range)
    return output


def build(dataset, as_of):
    cutoff=timestamp(as_of,aware=True)
    if dataset.get('schema_version')!='rub_exchange_history_research.v1' or dataset.get('secid') not in ('SiU6','CRU6'):
        raise ValueError('explicit Si/CR exchange research dataset required')
    frames={};excluded=[]
    for frame,key,label in [('D1','daily','trading_day'),('W1','weekly','week_start')]:
        rows=sorted(dataset[key],key=lambda r:r[label]);seen=set();segment=[];result=[];previous=None
        for source in rows:
            period=source[label]
            if period in seen:raise ValueError('duplicate period')
            seen.add(period)
            d=date.fromisoformat(period)
            if (frame=='W1' and d.weekday()!=0) or (frame=='D1' and d.weekday()>=5):raise ValueError('invalid exchange period label')
            end=d+timedelta(days=4 if frame=='W1' else 0)
            closed=timestamp(end.isoformat()+'T23:50:00+03:00')
            available=timestamp(source['availability_ts'],aware=True)
            price_match=source.get('iss_daily_prices_match') is True
            if frame=='W1':
                required={(d+timedelta(days=i)).isoformat() for i in range(5)}
                children=[r for r in dataset['daily'] if r['trading_day'] in required]
                price_match=len(children)==5 and all(r.get('iss_daily_prices_match') is True and r.get('session_coverage_complete') is True for r in children)
                if children:available=max([available]+[timestamp(r['availability_ts'],aware=True) for r in children])
            reasons=[]
            if source.get('session_coverage_complete') is not True:reasons.append('incomplete_session_coverage')
            if not price_match:reasons.append('unverified_daily_prices')
            if available>cutoff or closed>cutoff:reasons.append('not_available_at_as_of')
            if reasons:
                result+=calculate(segment,frame);segment=[];previous=None
                excluded.append(dict(timeframe=frame,period=period,reasons=reasons));continue
            if previous is not None and not next_period(previous,period,frame):
                result+=calculate(segment,frame);segment=[]
            # Deliberate field projection: no volume, turnover, trade count or OI enters formulas.
            segment.append(dict(period=period,availability_ts=max(available,closed).isoformat(),**{k:source[k] for k in ('open','high','low','close')}))
            previous=period
        result+=calculate(segment,frame)
        frames[frame]=dict(rows=result,latest=result[-1] if result else None)
    return dict(schema_version=POLICY,secid=dataset['secid'],as_of=cutoff.isoformat(),
        profile='price_only_research',volume_reconciliation='excluded_by_user_scope',
        volume_dependent_features_enabled=False,model_acceptance_granted=False,
        historical_pit_ready=False,accepted_pointer_promotion=False,
        excluded_periods=excluded,features=frames,
        remaining_dependencies=['continuous_contract_history','longer_D1_W1_history','weekly_OI','FUTOI','out_of_sample_validation'])


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset',type=Path,required=True)
    parser.add_argument('--as-of',required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();raw=args.dataset.read_bytes()
    result=build(json.loads(raw),args.as_of);result['dataset_sha256']=hashlib.sha256(raw).hexdigest()
    with args.output.open('x',encoding='utf-8') as stream:json.dump(result,stream,indent=2)
    print(json.dumps({frame:{'rows':len(v['rows']),'latest':v['latest']} for frame,v in result['features'].items()},indent=2))

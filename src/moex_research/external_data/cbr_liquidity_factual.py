"""Latest received CBR banking liquidity, with explicit revision/precision limits."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext
from hashlib import sha256
from html import unescape
import json
from pathlib import Path
import re

from . import cbr
from . import cbr_rates_factual as evidence

COMPONENT = 'cbr_liquidity_verified'
POLICY = 'cbr_liquidity_received_reference.v1'
SCOPE = 'CURRENT_RECEIVED_BANKING_LIQUIDITY'
MAX_BYTES = 2_000_000
MAX_RECEIPT_SECONDS = 1200
QUERY_DAYS = 30
MAX_OBSERVATION_DAYS = 7
DENIED = ('historical_pit_acceptance','forecast_use_allowed','action_authority','full_macro_complete')
LIMITATIONS = [
    'Latest revised source values; earlier publication vintages are not established.',
    'Values retain displayed decimal precision; rounded source columns need not sum exactly. No error bars are inferred.',
    'Beginning-of-day observation basis is not a publication timestamp; source publication hour is unknown.',
]
HEADERS = (
    ('Date', 'Liquidity deficit (+)/surplus (-)', 'Liquidity deficit (+)/surplus (-) subject to correspondent account balances fluctuations', 'including', 'As reference:'),
    ('the CBR standard monetary policy instruments', 'non-standard CBR monetary policy instruments (including SM)*', 'Correspondent account balances of credit institutions with the Bank of Russia', 'Required Reserves to be Averaged on Correspondent Accounts**'),
    ('the CBR claims on the banking sector', 'including', 'the CBR liabilities to the banking sector', 'including'),
    ('auctions', 'standing facilities', 'deposits', 'the CBR coupon bonds'),
    ('REPOs and buy/sell FX swaps', 'secured loans', 'REPOs and buy/sell FX swaps', 'secured loans', 'auctions', 'standing facilities'),
    cbr.LIQUIDITY_NUMBER_HEADERS,
)
METRICS = (
    ('liquidity_deficit_surplus_rub_bn', 1, HEADERS[0][1]),
    ('liquidity_deficit_surplus_ex_correspondent_accounts_rub_bn', 2, HEADERS[0][2]),
    ('bank_correspondent_accounts_rub_bn', 13, HEADERS[1][2]),
    ('required_reserves_averaging_rub_bn', 14, HEADERS[1][3]),
)


def _url(day):
    return cbr._url(cbr.LIQUIDITY_ROUTE,day-timedelta(days=QUERY_DAYS),day)


def _number(value):
    if not isinstance(value,str) or len(value)>128 or not re.fullmatch(r'-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?',value):
        raise ValueError('invalid displayed numeric value')
    return format(Decimal(value.replace(',','')),'f')


def parse(raw, *, source_url, received_at):
    received = evidence._utc(received_at)
    day = received.astimezone(evidence.MOSCOW).date()
    if source_url != _url(day) or not isinstance(raw,bytes) or not 0<len(raw)<=MAX_BYTES:
        raise ValueError('source identity or raw size invalid')
    text = raw.decode('utf-8')
    if 'billions of rubles (at the beginning of the day)' not in ' '.join(unescape(re.sub('<[^>]*>',' ',text)).split()):
        raise ValueError('source units or observation basis absent')
    parser = cbr._TableParser()
    parser.feed(text)
    matches = [table for table in parser.tables if cbr.LIQUIDITY_NUMBER_HEADERS in table]
    if len(matches)!=1 or tuple(matches[0][:6])!=HEADERS:
        raise ValueError('liquidity table headings changed or ambiguous')
    rows = matches[0][6:]
    if not 1<=len(rows)<=QUERY_DAYS+1:
        raise ValueError('liquidity row count invalid')
    identities, selected = set(), None
    for row in rows:
        if len(row)!=15 or not re.fullmatch(r'\d{2}\.\d{2}\.\d{4}',row[0]):
            raise ValueError('malformed liquidity row')
        observed = datetime.strptime(row[0],'%d.%m.%Y').date()
        if not day-timedelta(days=QUERY_DAYS)<=observed<=day or observed in identities:
            raise ValueError('liquidity observation date range or duplicate')
        identities.add(observed)
        values = {metric:_number(row[index]) for metric,index,_ in METRICS}
        if any(Decimal(values[metric])<0 for metric,_,_ in METRICS[2:]):
            raise ValueError('negative correspondent accounts or reserves')
        if selected is None or observed>selected[0]:
            selected = observed,values
    observed, values = selected
    if (day-observed).days>MAX_OBSERVATION_DAYS:
        raise ValueError('latest liquidity observation too old')
    return [{'metric_id':metric,'value':values[metric],'units':'RUB_bn','source_label':label,
        'displayed_decimal_places': max(0,-Decimal(values[metric]).as_tuple().exponent),
        'observation_date':observed.isoformat(),'observation_time_basis':'BEGINNING_OF_DAY',
        'source_publication_time':None,'system_available_at':received.isoformat()}
        for metric,_,label in METRICS]


def _normalized(raw, *, receipt, now):
    now = evidence._utc(now)
    if not isinstance(receipt,dict) or set(receipt)!={'source_url','requested_at','received_at','raw_sha256'}:
        raise ValueError('receipt shape invalid')
    requested,received = evidence._utc(receipt['requested_at']),evidence._utc(receipt['received_at'])
    if not requested<=received<=now or (now-received).total_seconds()>MAX_RECEIPT_SECONDS:
        raise ValueError('receipt expired or chronology invalid')
    if requested.astimezone(evidence.MOSCOW).date()!=received.astimezone(evidence.MOSCOW).date():
        raise ValueError('acquisition crossed query date')
    if sha256(raw).hexdigest()!=evidence._digest(receipt['raw_sha256']):
        raise ValueError('raw hash mismatch')
    observations = parse(raw,source_url=receipt['source_url'],received_at=received)
    observed = date.fromisoformat(observations[0]['observation_date'])
    if (now.astimezone(evidence.MOSCOW).date()-observed).days>MAX_OBSERVATION_DAYS:
        raise ValueError('latest liquidity observation too old at reference')
    values = {o['metric_id']:Decimal(o['value']) for o in observations}
    with localcontext() as context:
        context.prec = sum(len(v.as_tuple().digits) for v in values.values())+4
        residual = values[METRICS[0][0]]-(values[METRICS[1][0]]-values[METRICS[2][0]]+values[METRICS[3][0]])
    return {'policy':POLICY,'scope':SCOPE,'receipt':receipt,'observations':observations,
        'latest_observation_date':observed.isoformat(),'received_at':received.isoformat(),
        'system_available_at':received.isoformat(),'source_publication_time':None,
        'source_url':receipt['source_url'],'raw_sha256':receipt['raw_sha256'],
        'source_revision_status':'latest_revised','quality_status':'USABLE_WITH_LIMITATIONS',
        'arithmetic_residual': {'value':format(residual,'f'),'units':'RUB_bn',
            'formula':'column_2 - (column_3 - column_14 + column_15)',
            'observation_date':observed.isoformat(),'source_url':receipt['source_url'],
            'raw_sha256':receipt['raw_sha256'],'informational_only':True,
            'note':'Printed values may not add exactly; residual is not an error bound and does not correct source values.'},
        'limitations':list(LIMITATIONS),'factual_authority':True,'consumer_factual_use_allowed':True,
        **dict.fromkeys(DENIED,False)}


def load(*, root, now_fn=lambda:datetime.now(timezone.utc), fetch=evidence._fetch):
    requested = evidence._utc(now_fn())
    url = _url(requested.astimezone(evidence.MOSCOW).date())
    raw = fetch(url)
    received = evidence._utc(now_fn())
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX_BYTES:
        raise ValueError('invalid raw response')
    digest = sha256(raw).hexdigest()
    receipt = dict(source_url=url,requested_at=requested.isoformat(),received_at=received.isoformat(),raw_sha256=digest)
    data = _normalized(raw,receipt=receipt,now=received)
    directory = Path(root)/'raw/external/cbr_liquidity'
    if any(p.is_symlink() for p in (directory,*directory.parents)):
        raise ValueError('symlink evidence directory refused')
    directory.mkdir(parents=True,exist_ok=True)
    evidence._freeze(directory/(digest+'.html'),raw)
    encoded = json.dumps(data,sort_keys=True,separators=(',',':')).encode()
    digest = sha256(encoded).hexdigest()
    path = directory/(digest+'.json')
    evidence._freeze(path,encoded)
    return {**data,'manifest_path':str(path),'manifest_sha256':digest}


def reconcile(component, *, now):
    result = deepcopy(component) if isinstance(component,dict) else {}
    data = result.get('data')
    if not isinstance(data,dict): data = result['data'] = {}
    try:
        if result.get('status')!='READY' or result.get('refresh_error'):
            raise ValueError('latest refresh not ready')
        digest = evidence._digest(data['manifest_sha256'])
        path = Path(data['manifest_path'])
        if path.name!=digest+'.json': raise ValueError('manifest name mismatch')
        frozen = json.loads(evidence._read(path,digest))
        if not isinstance(frozen,dict) or set(data)-{'manifest_path','manifest_sha256','read_freshness_reason'}!=set(frozen):
            raise ValueError('manifest shape mismatch')
        if any(json.dumps(data[k],sort_keys=True)!=json.dumps(value,sort_keys=True) for k,value in frozen.items()):
            raise ValueError('normalized evidence modified')
        if not isinstance(frozen.get('receipt'),dict): raise ValueError('receipt object required')
        raw_hash = evidence._digest(frozen['receipt']['raw_sha256'])
        raw = evidence._read(path.parent/(raw_hash+'.html'),raw_hash)
        replayed = _normalized(raw,receipt=frozen['receipt'],now=now)
        if json.dumps(replayed,sort_keys=True)!=json.dumps(frozen,sort_keys=True):
            raise ValueError('raw replay mismatch')
        data['read_freshness_reason'] = None
    except (ValueError,TypeError,KeyError,OSError,OverflowError) as exc:
        result['status']='UNAVAILABLE'
        data.update(dict.fromkeys(DENIED,False))
        data['factual_authority']=data['consumer_factual_use_allowed']=False
        data['quality_status']='UNAVAILABLE'
        data['read_freshness_reason']=str(exc)
    return result


def apply(snapshot, *, now):
    components = snapshot.get('components',{})
    if isinstance(components,dict) and COMPONENT in components:
        components[COMPONENT]=reconcile(components[COMPONENT],now=now)

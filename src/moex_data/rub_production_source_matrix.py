"""Evidence-backed production coverage and unanalyzed-news presentation."""
import argparse
import copy
from datetime import datetime
import hashlib
import json
from pathlib import Path

from moex_research.external_data import moex_brent_factual as brent


def unanalyzed_news(events):
    result=[]
    for event in events:
        item=copy.deepcopy(event)
        if item.get('classification_status')!='NOT_ANALYZED':
            item['upstream_placeholder']={k:item.get(k) for k in ('direction','confidence','rub_relevance','importance','horizon')}
        item.update(direction='UNKNOWN',confidence=None,rub_relevance=None,importance='UNKNOWN',horizon='UNKNOWN',
            classification_status='NOT_ANALYZED',mechanism='RUB impact has not been analyzed; acquisition does not establish neutrality.')
        result.append(item)
    return result


def build(snapshot):
    components=snapshot['components'];rows=[]
    def add(block,role,collected,usable,reason,evidence):
        rows.append(dict(block_id=block,requirement=role,collection_present=bool(collected),
            usable_for_full_forecast=bool(usable),reason=reason,evidence_path=evidence))
    market=components.get('synchronized_live_market_oi',{}).get('data',{}).get('instruments',{})
    for key in ('si_front','si_next','cr_front','cr_next','usdrubf','cnyrubf','cnyrub_tom'):
        item=market.get(key,{})
        # A record's mere presence or collector READY never grants freshness.
        usable=item.get('price_oi_usable') is True if key!='cnyrub_tom' else item.get('last') is not None and item.get('stale') is False
        add(key,'required',bool(item),usable,item.get('read_freshness_reason') or ('ready' if usable else 'freshness_or_identity_not_proven'),
            'components.synchronized_live_market_oi.data.instruments.'+key)
    add('usd_spot','conditional_usd_basis',False,False,'current_live_schema_unsupported','components.live_basis_carry')
    for key in ('futoi_live','futoi_live_cr'):
        component=components.get(key,{});data=component.get('data',{})
        usable=component.get('status')=='READY' and data.get('consumer_factual_use_allowed') is True and data.get('factual_authority') is True
        add(key,'required',bool(data.get('current_intraday')),usable,'consumer_acceptance_required' if not usable else 'ready','components.'+key)
    basis=components.get('live_basis_carry',{})
    basis = basis if isinstance(basis, dict) else {}
    basis_data = basis.get('data')
    basis_data = basis_data if isinstance(basis_data, dict) else {}
    pairs = basis_data.get('pairs')
    pairs = pairs if isinstance(pairs, dict) else {}
    admitted_metrics = sorted({metric['metric_id']
        for pair in pairs.values() if isinstance(pair, dict)
        for metric in (pair.get('metrics') if isinstance(pair.get('metrics'), list) else [])
        if isinstance(metric, dict) and metric.get('status') == 'READY'
        and isinstance(metric.get('metric_id'), str) and metric['metric_id']})
    basis_factual = bool(admitted_metrics) and basis.get('status') in {'READY', 'PARTIAL'}
    add('basis_carry','required',bool(basis_data),basis.get('status')=='READY' and basis_factual,
        'ready_current_live_scope' if basis.get('status')=='READY' and basis_factual
        else 'partial_factual_metrics_only' if basis_factual else 'synchronized_comparable_inputs_required',
        'components.live_basis_carry')
    rows[-1].update(factual_context_usable=basis_factual,
        factual_authority_scope='individual_READY_metrics_only',
        admitted_metric_ids=admitted_metrics if basis_factual else [])
    macro=components.get('cbr_macro',{}).get('data',{}).get('state',{})
    add('cbr_rates','required',bool(macro.get('observations')),False,'key_rate_and_ruonia_present_but_full_macro_acceptance_pending','components.cbr_macro')
    for block in ('minfin_fx_operations','event_calendar'):
        add(block,'required',False,False,'accepted_block_not_present','components.stage9_daily.data.external_context_required')
    from moex_research.external_data import rosstat_cpi_factual as rosstat
    rosstat_component = components.get('rosstat_cpi', {})
    try:
        if 'live_read_freshness' in snapshot:
            rosstat_freshness = snapshot['live_read_freshness']
            if not isinstance(rosstat_freshness, dict):
                raise ValueError('invalid freshness block')
            reference = rosstat_freshness['read_at_utc']
        else:
            reference = snapshot['identity']['generated_at_utc']
        view = rosstat.reconcile(rosstat_component, now=datetime.fromisoformat(reference))
        rosstat_usable = view.get('status') == 'READY' and (view.get('data') or {}).get('consumer_factual_use_allowed') is True
    except (ValueError, TypeError, KeyError):
        rosstat_usable = False
    add('rosstat_macro','required',bool(rosstat_component.get('data')),False,
        'weekly_cpi_only_full_macro_and_calendar_pending' if rosstat_usable else 'accepted_weekly_cpi_unavailable',
        'components.rosstat_cpi')
    rows[-1].update(factual_context_usable=rosstat_usable, price_context_scope='latest_listed_weekly_estimate')
    news=components.get('official_news',{}).get('data',{})
    add('official_news','required',bool(news.get('events')),False,'acquired_events_are_not_impact_analysis','components.official_news')
    oil=components.get('oil',{})
    oil=oil if isinstance(oil,dict) else {}
    oil_data=oil.get('data')
    oil_data=oil_data if isinstance(oil_data,dict) else {}
    freshness_present='live_read_freshness' in snapshot
    freshness=snapshot.get('live_read_freshness')
    freshness_valid=not freshness_present or (isinstance(freshness,dict) and 'read_at_utc' in freshness)
    freshness=freshness if isinstance(freshness,dict) else {}
    reason=oil_data.get('reason','consumer_acceptance_required')
    try:
        if not freshness_valid:
            raise ValueError("invalid optional freshness block")
        reference=freshness['read_at_utc'] if freshness_present else snapshot['identity']['generated_at_utc']
        oil_view=brent.reconcile_component(oil,now=datetime.fromisoformat(reference))
        view_data=oil_view.get('data')
        if isinstance(view_data,dict):
            reason=view_data.get('read_freshness_reason') or reason
    except (ValueError,TypeError,OverflowError,KeyError):
        oil_view={'status':'UNAVAILABLE'}
        reason='invalid_snapshot_freshness_reference'
    usable=brent.factual_usable(oil_view)
    collected=oil_data.get('source_id')==brent.SOURCE_ID and bool(oil_data.get('secid')) and isinstance(oil_data.get('ohlc'),dict)
    add('brent','required',collected,usable,'accepted_latest_published_close_not_live' if usable else reason,'components.oil')
    rows[-1].update(factual_context_usable=bool(usable),price_context_scope='latest_published_history_only',intraday_fresh=False)
    external = components.get('external_cny', {})
    external_data = external.get('data') or {}
    from moex_research.external_data import fred_cny_factual as fred_cny
    try:
        reference = freshness['read_at_utc'] if freshness_present else snapshot['identity']['generated_at_utc']
        external_view = fred_cny.reconcile(external, now=datetime.fromisoformat(reference))
        external_usable = external_view.get('status') == 'READY' and (external_view.get('data') or {}).get('consumer_factual_use_allowed') is True
    except (ValueError, TypeError, KeyError):
        external_usable = False
    add('external_cny','required',bool(external_data.get('manifest_sha256')),False,
        'dated_reference_only_forecast_alignment_pending' if external_usable else 'accepted_external_CNY_or_CNH_required',
        'components.external_cny')
    rows[-1].update(factual_context_usable=external_usable, price_context_scope='latest_published_daily_reference', intraday_fresh=False)
    for block in ('wti','urals','dxy','ust'):
        add(block,'enrichment',False,False,'no_accepted_snapshot_block','components.stage9_daily.data.external_context_required')
    add('volume_features','excluded',False,False,'excluded_by_user_instruction',None)
    return dict(schema_version='rub_production_source_matrix.v1',
        snapshot_generated_at=snapshot['identity']['generated_at_utc'],
        freshness_evaluated_at=freshness.get('read_at_utc'),
        rows=rows,blocking_required_blocks=[r['block_id'] for r in rows if r['requirement']=='required' and not r['usable_for_full_forecast']],
        data_acceptance_complete=False,analysis_ready=False,model_validated=False,
        training_authorized=False,volume_investigation_in_scope=False,
        news_view=unanalyzed_news(news.get('events',[])),
        news_acquisition_summary=news.get('summary',{}),
        note='Snapshot-bound inventory; research pilots outside this snapshot are not accepted sources. No closed-market freshness upgrade is inferred.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--snapshot',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();raw=args.snapshot.read_bytes();result=build(json.loads(raw));result['snapshot_sha256']=hashlib.sha256(raw).hexdigest()
    with args.output.open('x',encoding='utf-8') as stream:json.dump(result,stream,indent=2)
    print(json.dumps({k:v for k,v in result.items() if k not in ('rows','news_view','news_acquisition_summary')},indent=2))

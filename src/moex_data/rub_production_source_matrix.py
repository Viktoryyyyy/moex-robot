"""Evidence-backed production coverage and unanalyzed-news presentation."""
import argparse
import copy
import hashlib
import json
from pathlib import Path


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
    add('basis_carry','required',bool(basis.get('data')),basis.get('status')=='READY','synchronized_comparable_inputs_required','components.live_basis_carry')
    macro=components.get('cbr_macro',{}).get('data',{}).get('state',{})
    add('cbr_rates','required',bool(macro.get('observations')),False,'key_rate_and_ruonia_present_but_full_macro_acceptance_pending','components.cbr_macro')
    for block in ('minfin_fx_operations','rosstat_macro','event_calendar'):
        add(block,'required',False,False,'accepted_block_not_present','components.stage9_daily.data.external_context_required')
    news=components.get('official_news',{}).get('data',{})
    add('official_news','required',bool(news.get('events')),False,'acquired_events_are_not_impact_analysis','components.official_news')
    oil=components.get('oil',{})
    add('brent','required',False,False,oil.get('data',{}).get('reason','accepted_source_missing'),'components.oil')
    add('external_cny','required',False,False,'accepted_external_CNY_or_CNH_required','components.stage9_daily.data.external_context_required')
    for block in ('wti','urals','dxy','ust'):
        add(block,'enrichment',False,False,'no_accepted_snapshot_block','components.stage9_daily.data.external_context_required')
    add('volume_features','excluded',False,False,'excluded_by_user_instruction',None)
    return dict(schema_version='rub_production_source_matrix.v1',
        snapshot_generated_at=snapshot['identity']['generated_at_utc'],
        freshness_evaluated_at=snapshot.get('live_read_freshness',{}).get('read_at_utc'),
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

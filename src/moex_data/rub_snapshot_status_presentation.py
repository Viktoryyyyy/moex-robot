"""Consumer-facing status dimensions projected from existing admissions."""
from copy import deepcopy
from pathlib import Path
import json

SCHEMA='snapshot_status_presentation.v1'
CONTRACT='contracts/intelligence/snapshot_status_presentation_v1.json'


def apply(snapshot, *, now):
    policy=json.loads((Path(__file__).resolve().parents[2]/CONTRACT).read_bytes())
    if policy.get('schema_version')!=SCHEMA or any(policy.get(k) is not False for k in ('new_admission','ttl_extension','action_authority')):
        raise ValueError('snapshot_status_presentation_policy_invalid')
    components=snapshot.get('components',{})
    bundles={s:(components.get('stage9_'+s,{}).get('data') or {}) for s in ('daily','weekly')}
    current=bundles['daily'].get('sections',{}).get('current_market',{}).get('items',{})
    summary={}
    for name,component in components.items():
        if not isinstance(component,dict):continue
        data=component.get('data') or {};legacy=component.get('status')
        item={'processing_status':legacy,'processing_status_semantics':policy['legacy_component_status'],
              'source_data_as_of':component.get('data_as_of'),'use_scope':'component_specific_admission_required',
              'use_status':'NOT_EVALUATED_HERE','current_quote_use_allowed':False}
        if name=='cnyrub_spot_live':
            observation=data.get('observation') or {}
            item.update(use_scope='observed_partial_day',use_status='AVAILABLE_PARTIAL_DAY_OBSERVATION' if legacy=='READY' and observation else 'UNAVAILABLE',
                source_trade_date=observation.get('trade_date'),source_event_at=observation.get('candle_end'),
                session_completion_proven=False,current_quote_source_ref='components.synchronized_live_market_oi.data.instruments.cnyrub_tom')
        elif name in ('oil','external_cny'):
            allowed=legacy=='READY' and data.get('consumer_factual_use_allowed') is True
            item.update(use_scope='latest_published_dated_reference',use_status='AVAILABLE_DATED' if allowed else 'UNAVAILABLE',
                source_trade_date=data.get('source_trade_date') or data.get('observation_date'),session_completion_proven=False)
        elif name=='cbr_macro':
            item.update(use_scope=data.get('coverage_scope'),use_status='AVAILABLE' if data.get('full_macro_complete') is True else 'PARTIAL',
                missing_required_blocks=deepcopy(data.get('missing_required_blocks',[])))
        elif name in ('stage9_daily','stage9_weekly'):
            item.update(use_scope='analysis_bundle',use_status=data.get('readiness',{}).get('bundle_status','UNAVAILABLE'),
                section_statuses=deepcopy(data.get('readiness',{}).get('section_statuses',{})))
        elif name=='synchronized_live_market_oi':
            keys=('usdrubf','si_front','si_next','cnyrubf','cr_front','cr_next','cnyrub_tom','usd_tom')
            item.update(use_scope='per_instrument_current_market_admission',
                use_status=bundles['daily'].get('sections',{}).get('current_market',{}).get('status','UNAVAILABLE'),
                instruments={key:{'status':current.get(key,{}).get('status','UNAVAILABLE'),
                    'reason':current.get(key,{}).get('reason'),'market_state':deepcopy(current.get(key,{}).get('market_state'))} for key in keys})
        elif name=='live_basis_carry':
            basis=current.get('basis_carry',{})
            item.update(use_scope='individually_admitted_current_metrics',use_status=basis.get('status','UNAVAILABLE'),coverage=deepcopy(basis.get('coverage')))
        elif name in ('futoi_live','futoi_live_cr'):
            item.update(use_scope='current_pair_existing_admission',use_status=current.get(name,{}).get('status','UNAVAILABLE'),
                reason=current.get(name,{}).get('reason'),session_completion_proven=False)
        component['status_presentation']=deepcopy(item)
        summary[name]=item
    full=all(b.get('readiness',{}).get('analysis_bundle_complete') is True for b in bundles.values())
    snapshot['status_presentation']={'schema_version':SCHEMA,'contract_ref':CONTRACT,'checked_at_utc':now.isoformat(),
        'display_status':'READY' if full else 'PARTIAL','analysis_bundle_complete':full,
        'component_statuses_are_not_current_market_admission':True,'components':summary,
        'daily_weekly':{s:deepcopy(b.get('readiness',{})) for s,b in bundles.items()},'action_authority':False}
    from moex_data.rub_cny_basis_calendar import coverage
    snapshot['status_presentation']['currency_calendar_coverage']=coverage(now=now)
    snapshot.setdefault('readiness',{})['display_status']=snapshot['status_presentation']['display_status']
    return snapshot['status_presentation']

from datetime import datetime,timezone
from moex_data import rub_snapshot_status_presentation as presentation
from moex_data import rub_cny_basis_calendar as calendar

NOW=datetime(2026,9,29,17,49,tzinfo=timezone.utc)

def test_collector_READY_does_not_label_old_cny_or_oil_current():
    snapshot={'components':{
        'cnyrub_spot_live':{'status':'READY','data':{'partial_day':True,'observation':{'trade_date':'2026-09-29','candle_end':'2026-09-29T18:55:00+03:00'}}},
        'oil':{'status':'READY','data':{'consumer_factual_use_allowed':True,'source_trade_date':'2026-09-28'}},
        'stage9_daily':{'status':'PARTIAL','data':{'readiness':{'bundle_status':'PARTIAL','analysis_bundle_complete':False}}},
        'stage9_weekly':{'status':'PARTIAL','data':{'readiness':{'bundle_status':'PARTIAL','analysis_bundle_complete':False}}}}}
    result=presentation.apply(snapshot,now=NOW)
    cny=result['components']['cnyrub_spot_live'];oil=result['components']['oil']
    assert cny['processing_status']=='READY' and cny['use_status']=='AVAILABLE_PARTIAL_DAY_OBSERVATION'
    assert cny['current_quote_use_allowed'] is False and not cny['session_completion_proven']
    assert oil['use_status']=='AVAILABLE_DATED' and oil['source_trade_date']=='2026-09-28'
    assert result['display_status']=='PARTIAL' and not result['analysis_bundle_complete']

def test_year_boundary_is_explicit_and_never_fills_unverified_2027():
    before=calendar.coverage(now=datetime(2026,12,31,10,tzinfo=timezone.utc))
    after=calendar.coverage(now=datetime(2027,1,1,10,tzinfo=timezone.utc))
    assert before['current_date_covered'] and before['days_until_coverage_end']==0
    assert not after['current_date_covered'] and after['following_year_status']=='NOT_VERIFIED'
    assert calendar.select(['2026-12-30','2027-01-04'])['target_dates']==[]
    assert calendar.select(['2026-12-30','2027-01-04'])['reason']=='CNY_calendar_coverage_unknown'

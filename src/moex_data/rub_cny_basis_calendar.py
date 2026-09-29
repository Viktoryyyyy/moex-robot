"""CNY spot target eligibility, separate from futures observation/quality proofs."""
from pathlib import Path
from moex_data import rub_contract_price_market_oi_observed as custody

CONTRACT = 'contracts/intelligence/cny_basis_calendar_2026_v1.json'
SHA256 = 'a5ff624b92eff0e6eb621ffa21e5c5511a210c9152270da3f8e52bbef70d2fe9'
SCHEMA = 'cny_basis_date_selection.v1'


def select(dates):
    """Choose anchor/lags before looking at prices or archive availability.

    This finite reviewed plan is not a weekday heuristic or an assertion of
    actual session completion. Unknown coverage fails only the spot selection.
    """
    value = custody._source_object((Path(__file__).resolve().parents[2]/CONTRACT).read_bytes())
    custody._require(custody.common._digest(value) == SHA256, 'CNY_calendar_contract_hash')
    known = all(value['coverage_start'] <= day <= value['coverage_end'] for day in dates)
    common = [day for day in dates if day in value['trading_dates']] if known else []
    return {'schema_version': SCHEMA, 'calendar_contract': CONTRACT, 'calendar_sha256': SHA256,
            'calendar_kind': value['policy_kind'], 'eligible_observed_dates': common,
            'excluded_nontrading_dates': [day for day in dates if day not in common] if known else [],
            'target_dates': [common[-1-lag] for lag in (0,1,5) if len(common)>lag],
            'reason': None if common else 'CNY_calendar_coverage_unknown' if not known else 'CNY_no_common_eligible_date',
            'session_completion_proven': False}


def metric_dates(e, pair, name):
    selection = e.get('_usd_reference_selection') if pair=='usd_rub' else e.get('_cny_spot_selection')
    if pair in ('cny_rub','usd_rub') and 'spot' in name and selection is not None:
        return selection['eligible_observed_dates']
    return e['observed_dates']


def decorate_metric(e, pair, name, item):
    selection = e.get('_usd_reference_selection') if pair=='usd_rub' else e.get('_cny_spot_selection')
    if pair in ('cny_rub','usd_rub') and 'spot' in name and selection is not None:
        dates = selection['eligible_observed_dates']
        item.update(anchor_trade_date=dates[-1] if dates else None,
                    horizon_basis='exact_USD_reference_common_eligible_observation_indices' if pair=='usd_rub' else 'exact_CNY_common_eligible_observation_indices',
                    date_selection=selection)
        if selection.get('reason'):
            item['reason'] = selection['reason']
            for change in item['changes'].values(): change['reason'] = selection['reason']
    return item


def horizon_basis(e):
    return ('per_metric_common_eligible_indices_CNY_spot_calendar_others_futures_witness'
            if '_cny_spot_selection' in e else 'exact_common_witness_indices_not_archive_success_positions')


def coverage(*, now):
    """Expose finite reviewed coverage; never infer a future trading calendar."""
    from datetime import date
    value = custody._source_object((Path(__file__).resolve().parents[2]/CONTRACT).read_bytes())
    custody._require(custody.common._digest(value) == SHA256, 'CNY_calendar_contract_hash')
    day = now.astimezone(custody.archive.MOSCOW).date()
    start, end = date.fromisoformat(value['coverage_start']), date.fromisoformat(value['coverage_end'])
    return {'schema_version':'currency_calendar_coverage.v1','contract_ref':CONTRACT,'contract_sha256':SHA256,
        'coverage_start':start.isoformat(),'coverage_end':end.isoformat(),'checked_date_moscow':day.isoformat(),
        'current_date_covered':start <= day <= end,'days_until_coverage_end':(end-day).days,
        'following_year':end.year+1,'following_year_status':'NOT_VERIFIED',
        'unsupported_date_behavior':'refuse_selected_currency_comparison_without_weekday_or_prior_year_fallback',
        'session_completion_proven':False}

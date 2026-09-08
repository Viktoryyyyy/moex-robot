from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest

from moex_data import rub_macro_evidence_inventory as inventory
from moex_research.external_data import cbr_rates_factual as cbr
from moex_research.external_data import rosstat_cpi_factual as rosstat
from test_cbr_rates_factual import raw as cbr_raw
from test_rosstat_cpi_factual import component as rosstat_component, freeze

NOW = datetime(2026, 9, 8, 7, tzinfo=timezone.utc)


def snapshot(tmp_path):
    rosstat_root = tmp_path / 'rosstat'
    rosstat_root.mkdir()
    data = cbr.load(root=tmp_path, now_fn=lambda: NOW,
        fetch=lambda url: cbr_raw('ruonia' if 'ruonia' in url else 'key_rate'))
    return {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {
        'cbr_rates_verified': {'status': 'READY', 'data': data},
        'rosstat_cpi': rosstat_component(rosstat_root), 'unrelated': {'data': {'preserve': True}}}}


def test_real_frozen_replay_projects_distinct_times_and_schedule(tmp_path):
    value = snapshot(tmp_path)
    before = deepcopy(value)
    result = inventory.describe(value, now=NOW)
    assert result == inventory.describe(value, now=NOW)
    assert value == before
    assert len(result['required_blocks']) == 4
    assert len(result['facts']) == 3
    facts = {x['series_id']: x for x in result['facts']}
    rate, ruonia = facts['cbr_key_rate_pct'], facts['cbr_ruonia_rate_pct']
    assert rate['effective_date'] == '2026-06-01'
    assert rate['source_publication_date'] is None
    assert rate['observation_date'] is None
    assert ruonia['observation_date'] == '2026-09-07'
    assert ruonia['source_publication_date'] == '2026-09-08'
    assert ruonia['source_publication_time'] is None
    assert ruonia['units'] == rate['units'] == 'PERCENT_PER_ANNUM'
    assert ruonia['system_available_at'] == NOW.isoformat()
    cpi = facts['ROSSTAT_WEEKLY_CPI_ESTIMATE']
    assert cpi['source_publication_date'] == '2026-09-02'
    assert cpi['indices']['previous_registration'] == '99.99'
    assert cpi['weekly_change_percent'] == '-0.01'
    event, = result['scheduled_events']
    assert event['event_status'] == 'SCHEDULED'
    assert event['scheduled_date'] == '2026-09-09'
    assert event['scheduled_time'] is event['actual_event_time'] is event['source_publication_time'] is None
    assert event['timezone'] == 'Europe/Moscow'
    assert event['weekly_calendar_coverage_end'] == '2026-09-16'
    assert event['calendar_overdue_policy'] == 'after_scheduled_date_end'
    assert event['index_manifest_sha256'] == value['components']['rosstat_cpi']['data']['index_manifest_sha256']
    for fact in result['facts']:
        assert len(fact['evidence']['raw_sha256']) == 64
        assert fact['source_publication_time'] is None
        assert fact['consensus'] is fact['surprise'] is None
    assert all(result[k] is False for k in inventory.DENIED)
    assert all(row['full_block_accepted'] is False for row in result['required_blocks'])
    assert 'accepted_latest_minfin_fx_component' in result['missing_evidence']
    assert 'banking_liquidity_requirement_and_vintage_policy_pending' in result['policy_gaps']


@pytest.mark.parametrize('provider', ['cbr_rates_verified', 'rosstat_cpi'])
@pytest.mark.parametrize('defect', ['expired', 'retained', 'refresh_failed', 'modified_raw', 'missing_manifest', 'modified_value'])
def test_source_failure_removes_only_corresponding_evidence(tmp_path, provider, defect):
    value = snapshot(tmp_path)
    component = value['components'][provider]
    data = component['data']
    now = NOW
    if defect == 'expired': now += timedelta(seconds=1201)
    elif defect == 'retained': component['status'] = 'RETAINED_PREVIOUS'
    elif defect == 'refresh_failed': component['refresh_error'] = 'failed'
    elif defect == 'modified_value':
        if provider == 'rosstat_cpi': data['indices']['previous_registration'] = '123'
        else: data['observations']['key_rate']['value'] = 99
    else:
        key = 'document_manifest_path' if provider == 'rosstat_cpi' else 'manifest_path'
        path = Path(data[key])
        if defect == 'missing_manifest': path.unlink()
        else:
            digest = data['raw_sha256'] if provider == 'rosstat_cpi' else data['receipts']['key_rate']['raw_sha256']
            path.with_name(digest + '.html').write_bytes(b'changed')
    result = inventory.describe(value, now=now)
    assert not any(f['component'] == provider for f in result['facts'])
    if provider == 'rosstat_cpi' or defect == 'expired': assert result['scheduled_events'] == []
    if defect != 'expired':
        other = 'cbr_rates_verified' if provider == 'rosstat_cpi' else 'rosstat_cpi'
        assert any(f['component'] == other for f in result['facts'])
    assert all(result[k] is False for k in inventory.DENIED)


@pytest.mark.parametrize('bad', [None, [], 'bad', 4, True, {}, {'status': 'READY', 'data': []}, {'status': 'READY', 'data': {}}])
def test_malformed_known_components_are_normalized_without_mutation(bad):
    value = {'components': {'cbr_rates_verified': bad, 'rosstat_cpi': bad, 'other': {'data': {'x': 1}}}}
    before = deepcopy(value)
    copy = inventory.reconcile_components(value, now=NOW)
    assert value == before
    for name in inventory.PROVIDERS:
        assert copy['components'][name]['status'] == 'UNAVAILABLE'
        assert copy['components'][name]['data']['factual_authority'] is False
    assert copy['components']['other'] == before['components']['other']
    result = inventory.describe(value, now=NOW)
    assert result['facts'] == result['scheduled_events'] == []


@pytest.mark.parametrize('bad_time', [None, NOW.replace(tzinfo=None), NOW.isoformat(), 1])
def test_invalid_reference_does_not_admit_evidence(tmp_path, bad_time):
    result = inventory.describe(snapshot(tmp_path), now=bad_time)
    assert result['reference_time_valid'] is False
    assert result['as_of_utc'] is None
    assert result['facts'] == result['scheduled_events'] == []


def test_missing_components_keep_four_blocks_and_policy_gaps():
    result = inventory.describe({}, now=NOW)
    assert len(result['required_blocks']) == 4
    assert result['facts'] == result['scheduled_events'] == []
    assert 'accepted_global_macro_calendar' in result['missing_evidence']
    assert 'fresh_replayable_weekly_cpi_schedule' in result['missing_evidence']


def test_failure_reason_is_preserved():
    value = {'components': {'rosstat_cpi': {'status': 'UNAVAILABLE', 'data': {'read_freshness_reason': 'specific_source_failure'}}}}
    result = inventory.reconcile_components(value, now=None)
    assert result['components']['rosstat_cpi']['data']['read_freshness_reason'] == 'specific_source_failure'


def test_january_cpi_absent_bases_remain_null(tmp_path):
    now = datetime(2026, 1, 15, 7, tzinfo=timezone.utc)
    url = 'https://rosstat.gov.ru/storage/mediabank/1_14-01-2026.html'
    raw = (Path(__file__).parents[1] / 'fixtures/rosstat/weekly_cpi_january_excerpt.html').read_bytes()
    archive = ('<div class="toggle-card"><div class="toggle-card__title">' + rosstat.TITLE + '</div>'
        '<div class="document-list__item document-list__item--row"><a href="' + url + '">HTML</a>'
        '<div class="document-list__item-title">с 1 по 12 января 2026 года</div>'
        '<div class="document-list__item-info">1 Кб, 14.01.2026</div></div></div>'
        '<div class="toggle-card"><div class="toggle-card__title">ГРАФИК размещения срочных информаций и справок на сайте Росстата в I полугодии 2026 года</div>'
        '<table><tr><td>1</td><td>Об оценке индекса потребительских цен с 1 по 12 января 2026 года</td><td>14 января</td></tr>'
        '<tr><td>2</td><td>Об оценке индекса потребительских цен с 13 по 19 января 2026 года</td><td>21 января</td></tr></table></div>').encode()
    i, ih = freeze(tmp_path, archive, rosstat.INDEX_URL, '2026-01-15T06:59:00+00:00', '2026-01-15T06:59:01+00:00')
    d, dh = freeze(tmp_path, raw, url, '2026-01-15T06:59:02+00:00', '2026-01-15T06:59:03+00:00')
    data = rosstat._replay(dict(index_manifest_path=i, index_manifest_sha256=ih, document_manifest_path=d, document_manifest_sha256=dh), now=now)
    result = inventory.describe({'components': {'rosstat_cpi': {'status': 'READY', 'data': data}}}, now=now)
    fact, = result['facts']
    assert fact['indices'] == {'previous_registration': None, 'month_start': '101.26', 'year_start': None}
    assert fact['weekly_change_percent'] is None
    assert result['scheduled_events'][0]['scheduled_date'] == '2026-01-21'

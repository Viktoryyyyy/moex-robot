from copy import deepcopy

from moex_research.external_data import rosstat_cpi_history as history
from moex_research.external_data import rosstat_cpi_vintages as store


def weekly(received, value, start='2026-09-01', end='2026-09-07'):
    return {
        'series_id': 'ROSSTAT_WEEKLY_CPI_ESTIMATE', 'geography': 'RU',
        'observation_start': start, 'observation_end': end,
        'indices': {'previous_registration': value, 'month_start': value, 'year_start': '104.72'},
        'weekly_change_percent': '0.05', 'units': 'index_percent_base_100',
        'document_format': 'three_explicit_bases', 'monthly_final': False,
        'source_publication_time': None, 'listed_publication_date': '2026-09-09',
        'source_url': 'https://rosstat.gov.ru/storage/mediabank/test.html',
        'raw_sha256': 'a' * 64, 'document_manifest_path': '/evidence/doc.json',
        'document_manifest_sha256': 'b' * 64, 'index_manifest_path': '/evidence/index.json',
        'index_manifest_sha256': 'c' * 64, 'received_at': received, 'system_available_at': received,
    }


def test_as_of_does_not_see_later_revision(tmp_path):
    initial = weekly('2026-09-09T16:00:05+00:00', '100.05')
    revised = deepcopy(initial)
    revised['received_at'] = revised['system_available_at'] = '2026-09-09T18:00:05+00:00'
    revised['indices']['previous_registration'] = '100.06'
    store.record(tmp_path, initial)
    store.record(tmp_path, revised)

    before = history.as_of(tmp_path, series_id='ROSSTAT_WEEKLY_CPI_ESTIMATE',
                           observation_key='2026-09-01__2026-09-07', as_of='2026-09-09T17:00:00+00:00')
    after = history.as_of(tmp_path, series_id='ROSSTAT_WEEKLY_CPI_ESTIMATE',
                          observation_key='2026-09-01__2026-09-07', as_of='2026-09-09T19:00:00+00:00')
    assert before['revision_seq'] == 0
    assert before['value_current']['indices']['previous_registration'] == '100.05'
    assert after['revision_seq'] == 1
    assert after['value_current']['indices']['previous_registration'] == '100.06'


def test_latest_as_of_selects_latest_available_observation_not_future_one(tmp_path):
    old = weekly('2026-09-09T16:00:05+00:00', '100.05')
    new = weekly('2026-09-16T16:00:05+00:00', '100.10', start='2026-09-08', end='2026-09-14')
    new['listed_publication_date'] = '2026-09-16'
    store.record(tmp_path, old)
    store.record(tmp_path, new)

    before = history.latest_as_of(tmp_path, series_id='ROSSTAT_WEEKLY_CPI_ESTIMATE',
                                  as_of='2026-09-15T12:00:00+00:00')
    after = history.latest_as_of(tmp_path, series_id='ROSSTAT_WEEKLY_CPI_ESTIMATE',
                                 as_of='2026-09-16T17:00:00+00:00')
    assert before['observation_key'] == '2026-09-01__2026-09-07'
    assert after['observation_key'] == '2026-09-08__2026-09-14'


def test_history_preserves_all_revisions(tmp_path):
    first = weekly('2026-09-09T16:00:05+00:00', '100.05')
    second = deepcopy(first)
    second['received_at'] = second['system_available_at'] = '2026-09-09T18:00:05+00:00'
    second['indices']['previous_registration'] = '100.06'
    store.record(tmp_path, first)
    store.record(tmp_path, second)
    rows = history.history(tmp_path, series_id='ROSSTAT_WEEKLY_CPI_ESTIMATE',
                           observation_key='2026-09-01__2026-09-07')
    assert [row['revision_seq'] for row in rows] == [0, 1]
    assert all(row['vintage_path'] and row['vintage_sha256'] for row in rows)

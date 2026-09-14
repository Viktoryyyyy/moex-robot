from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import pytest

from moex_research.external_data import rosstat_cpi_vintages as source


def weekly(received='2026-09-09T16:00:05+00:00', value='100.05'):
    return {
        'series_id': 'ROSSTAT_WEEKLY_CPI_ESTIMATE',
        'geography': 'RU',
        'observation_start': '2026-09-01',
        'observation_end': '2026-09-07',
        'indices': {'previous_registration': value, 'month_start': value, 'year_start': '104.72'},
        'weekly_change_percent': str(Decimal(value) - Decimal('100')),
        'units': 'index_percent_base_100',
        'document_format': 'three_explicit_bases',
        'monthly_final': False,
        'source_publication_time': None,
        'listed_publication_date': '2026-09-09',
        'source_url': 'https://rosstat.gov.ru/storage/mediabank/137_09-09-2026.html',
        'raw_sha256': 'a' * 64,
        'document_manifest_path': '/evidence/weekly.json',
        'document_manifest_sha256': 'b' * 64,
        'index_manifest_path': '/evidence/index.json',
        'index_manifest_sha256': 'c' * 64,
        'received_at': received,
        'system_available_at': received,
    }


def monthly(received='2026-09-11T16:00:05+00:00'):
    return {
        'series_id': 'ROSSTAT_MONTHLY_CPI',
        'geography': 'RU',
        'observation_month': '2026-08',
        'indices': {'previous_month': '99.92', 'previous_december': '104.67', 'same_month_previous_year': '106.33'},
        'changes_percent': {'previous_month': '-0.08', 'previous_december': '4.67', 'same_month_previous_year': '6.33'},
        'units': 'index_percent_base_100',
        'estimate_kind': 'PUBLISHED_MONTHLY_INDEX',
        'decimal_places': 2,
        'precision_scope': 'printed_granularity_not_error_bound',
        'source_publication_time': None,
        'listed_publication_date': None,
        'source_url': 'https://rosstat.gov.ru/storage/mediabank/140_11-09-2026.html',
        'raw_sha256': 'd' * 64,
        'document_manifest_path': '/evidence/monthly.json',
        'document_manifest_sha256': 'e' * 64,
        'index_manifest_path': '/evidence/index.json',
        'index_manifest_sha256': 'f' * 64,
        'received_at': received,
        'system_available_at': received,
    }


def test_initial_weekly_vintage_is_immutable_and_pit_dated(tmp_path):
    ref = source.record(tmp_path, weekly())
    artifact = json.loads(Path(ref['vintage_path']).read_text())
    assert artifact['revision_seq'] == 0
    assert artifact['revision_status'] == 'INITIAL'
    assert artifact['published_at'] is None
    assert artifact['published_date'] == '2026-09-09'
    assert artifact['available_at'] == '2026-09-09T16:00:05+00:00'
    assert artifact['value_initial'] == artifact['value_current']
    assert artifact['value_previous'] is artifact['value_revised'] is None
    assert artifact['pit']['eligible_from'] == artifact['available_at']
    assert artifact['pit']['no_lookahead'] is True
    assert source.validate_reference(ref, weekly()) is True


def test_repeated_poll_same_semantics_reuses_vintage_and_only_refreshes_pointer(tmp_path):
    first = source.record(tmp_path, weekly())
    later_data = weekly(received='2026-09-09T16:10:05+00:00')
    second = source.record(tmp_path, later_data)
    assert second['vintage_id'] == first['vintage_id']
    assert second['vintage_path'] == first['vintage_path']
    files = list(Path(first['vintage_path']).parent.glob('vintage_id=*.json'))
    assert len(files) == 1
    pointer = json.loads((Path(first['vintage_path']).parents[1] / 'current.json').read_text())
    assert pointer['last_verified_at'] == '2026-09-09T16:10:05+00:00'
    assert pointer['available_at'] == '2026-09-09T16:00:05+00:00'


def test_changed_same_observation_creates_revision_preserving_initial_value(tmp_path):
    first_data = weekly()
    first = source.record(tmp_path, first_data)
    changed = weekly(received='2026-09-09T16:20:05+00:00', value='100.06')
    second = source.record(tmp_path, changed)
    artifact = json.loads(Path(second['vintage_path']).read_text())
    initial = json.loads(Path(first['vintage_path']).read_text())
    assert second['vintage_id'] != first['vintage_id']
    assert artifact['revision_seq'] == 1
    assert artifact['revision_status'] == 'REVISED'
    assert artifact['value_initial'] == initial['value_current']
    assert artifact['value_previous'] == initial['value_current']
    assert artifact['value_revised'] == artifact['value_current']
    assert artifact['revision_at'] == '2026-09-09T16:20:05+00:00'


def test_revert_to_older_value_is_new_revision_not_semantic_dedup(tmp_path):
    first = source.record(tmp_path, weekly())
    second = source.record(tmp_path, weekly(received='2026-09-09T16:20:05+00:00', value='100.06'))
    third = source.record(tmp_path, weekly(received='2026-09-09T16:30:05+00:00', value='100.05'))
    assert len({first['vintage_id'], second['vintage_id'], third['vintage_id']}) == 3
    artifact = json.loads(Path(third['vintage_path']).read_text())
    assert artifact['revision_seq'] == 2
    assert artifact['revision_status'] == 'REVISED'


def test_new_month_is_new_initial_observation_not_revision(tmp_path):
    first = source.record(tmp_path, monthly())
    next_month = monthly(received='2026-10-09T16:00:05+00:00')
    next_month['observation_month'] = '2026-09'
    next_month['source_url'] = 'https://rosstat.gov.ru/storage/mediabank/x.html'
    second = source.record(tmp_path, next_month)
    assert first['observation_key'] == '2026-08'
    assert second['observation_key'] == '2026-09'
    assert second['revision_seq'] == 0


def test_monthly_values_are_stored_as_published_not_derived_from_weekly(tmp_path):
    ref = source.record(tmp_path, monthly())
    artifact = json.loads(Path(ref['vintage_path']).read_text())
    assert artifact['frequency'] == 'MONTHLY'
    assert artifact['value_current']['indices']['previous_month'] == '99.92'
    assert artifact['value_current']['changes_percent']['previous_month'] == '-0.08'
    assert 'weekly_change_percent' not in artifact['value_current']


def test_tampered_vintage_reference_fails_closed(tmp_path):
    data = weekly()
    ref = source.record(tmp_path, data)
    path = Path(ref['vintage_path'])
    path.write_text('{}')
    with pytest.raises(source.RosstatVintageError, match='hash mismatch'):
        source.validate_reference(ref, data)


@pytest.mark.parametrize('change', ['naive_time', 'missing_provenance', 'unknown_series', 'future_availability'])
def test_invalid_normalized_input_fails_closed(tmp_path, change):
    data = deepcopy(weekly())
    if change == 'naive_time': data['received_at'] = data['system_available_at'] = '2026-09-09T16:00:05'
    elif change == 'missing_provenance': data.pop('document_manifest_sha256')
    elif change == 'unknown_series': data['series_id'] = 'ROSSTAT_UNKNOWN'
    else: data['system_available_at'] = '2026-09-09T16:00:06+00:00'
    with pytest.raises(source.RosstatVintageError):
        source.record(tmp_path, data)

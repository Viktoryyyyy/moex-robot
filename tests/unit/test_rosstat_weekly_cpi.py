from hashlib import sha256
import json
from pathlib import Path

import pytest

from moex_research.external_data import rosstat_https as transport
from moex_research.external_data import rosstat_weekly_cpi as source

RAW = (Path(__file__).parents[1] / 'fixtures/rosstat/weekly_cpi_excerpt.html').read_bytes()
NOW = '2026-09-08T06:16:42+00:00'


def test_real_document_excerpt_preserves_period_units_and_three_bases():
    result = source.parse(RAW, received_at=NOW)
    assert result['observation_start'] == '2026-08-25'
    assert result['observation_end'] == '2026-08-31'
    assert result['indices'] == {'previous_registration': '99.99', 'month_start': '99.92', 'year_start': '104.67'}
    assert result['weekly_change_percent'] == '-0.01'
    assert result['units'] == 'index_percent_base_100'
    assert result['monthly_final'] is False


def test_real_cross_month_document_uses_end_month_base():
    raw = (Path(__file__).parents[1] / 'fixtures/rosstat/weekly_cpi_cross_month_excerpt.html').read_bytes()
    result = source.parse(raw, received_at=NOW)
    assert (result['observation_start'], result['observation_end']) == ('2026-07-28', '2026-08-03')
    assert result['indices'] == {'previous_registration': '99.98', 'month_start': '100.00', 'year_start': '104.84'}
    with pytest.raises(ValueError):
        source.parse(raw.replace('С начала августа'.encode(), 'С начала июля'.encode()), received_at=NOW)


def test_real_january_initial_release_does_not_invent_absent_bases():
    raw = (Path(__file__).parents[1] / 'fixtures/rosstat/weekly_cpi_january_excerpt.html').read_bytes()
    result = source.parse(raw, received_at=NOW)
    assert result['observation_end'] == '2026-01-12'
    assert result['indices'] == {'previous_registration': None, 'month_start': '101.26', 'year_start': None}
    assert result['weekly_change_percent'] is None
    assert result['document_format'] == 'january_initial_month_index'
    with pytest.raises(ValueError): source.parse(raw.replace(b'101,26%', b'101,27%'), received_at=NOW)


@pytest.mark.parametrize('period,start,end', [
    ('со 2 по 8 июня 2026', '2026-06-02', '2026-06-08'),
    ('с 17 по 24 февраля 2026', '2026-02-17', '2026-02-24'),
    ('с 27 октября по 2 ноября 2026', '2026-10-27', '2026-11-02'),
])
def test_calendar_grammar_supports_observed_holiday_forms(period, start, end):
    assert tuple(d.isoformat() for d in source.period_dates(period)) == (start, end)


def test_so_prefix_in_title_and_summary_uses_same_period_identity():
    raw = RAW.decode().replace('с 25 по 31 августа', 'со 2 по 8 августа').encode()
    assert source.parse(raw, received_at=NOW)['observation_start'] == '2026-08-02'


@pytest.mark.parametrize('period', ['с 30 февраля по 2 марта 2026', 'с 28 июля по 3 сентября 2026',
    'с 3 по 2 августа 2026', 'с 30 декабря по 2 января 2026', 'с 1 по 20 января 2026'])
def test_invalid_and_unproven_year_rollover_periods_fail(period):
    with pytest.raises(ValueError): source.period_dates(period)


@pytest.mark.parametrize('old,new', [
    ('99,99%', '100,99%'), ('99,92%', '100,92%'),
    ('99,99%', '99,99 рублей'), ('104,67%', 'NaN%'),
    ('104,67%', '0,00%'), ('31 августа 2026 года', '30 августа 2026 года'),
    ('31 августа 2026 года', '32 августа 2026 года'),
    ('К предыдущей', 'К иной'), ('С начала<br/>августа', 'С начала<br/>июля'),
    ('Об оценке индекса', 'Об окончательном индексе'),
    ('с 25 по 31', 'с 25 июля по 31'),
])
def test_changed_identity_period_units_and_conflicting_table_fail(old, new):
    text = RAW.decode()
    assert old in text
    with pytest.raises(ValueError):
        source.parse(text.replace(old, new).encode(), received_at=NOW)


@pytest.mark.parametrize('raw', [b'', b'not html', RAW + RAW, b'\xff', b'x' * (transport.MAX_BYTES + 1)])
def test_invalid_and_ambiguous_document_fails(raw):
    with pytest.raises(ValueError): source.parse(raw, received_at=NOW)


@pytest.mark.parametrize('now', ['2026-08-30T00:00:00+00:00', '2026-09-08T00:00:00'])
def test_future_observation_and_naive_receipt_fail(now):
    with pytest.raises(ValueError): source.parse(RAW, received_at=now)


def freeze(tmp_path, **changes):
    raw_hash = sha256(RAW).hexdigest()
    (tmp_path / (raw_hash + '.html')).write_bytes(RAW)
    receipt = {'policy': transport.POLICY, 'source_url': 'https://rosstat.gov.ru/storage/mediabank/134_02-09-2026.html',
        'raw_sha256': raw_hash, 'certificate_sha256': transport.CERTIFICATES,
        'tls_chain_and_hostname_verified': True, 'semantic_validation_status': 'NOT_PARSED',
        'factual_authority': False, 'historical_pit_acceptance': False, 'action_authority': False,
        'requested_at_utc': '2026-09-08T06:16:39+00:00', 'received_at_utc': '2026-09-08T06:16:40+00:00', **changes}
    raw = json.dumps(receipt, sort_keys=True).encode()
    digest = sha256(raw).hexdigest()
    path = tmp_path / (digest + '.json')
    path.write_bytes(raw)
    return path, digest


def test_replay_is_deterministic_receipt_bound_and_does_not_promote_authority(tmp_path):
    path, digest = freeze(tmp_path)
    result = source.replay(path, manifest_sha256=digest, now=NOW)
    assert result == source.replay(path, manifest_sha256=digest, now='2026-09-09T00:00:00+00:00')
    assert result['system_available_at'] == '2026-09-08T06:16:40+00:00'
    assert result['source_publication_time'] is None
    assert result['consensus'] is result['surprise'] is None
    assert result['semantic_validation_status'] == 'DOCUMENT_PARSED'
    for key in ('factual_authority', 'consumer_factual_use_allowed', 'historical_pit_acceptance',
                'action_authority', 'latest_publication_verified'):
        assert result[key] is False


@pytest.mark.parametrize('changes', [
    {'source_url': 'https://example.org/x.html'},
    {'source_url': 'https://rosstat.gov.ru/compendium/document/50798'},
    {'certificate_sha256': {}}, {'tls_chain_and_hostname_verified': False},
    {'policy': 'unknown'}, {'factual_authority': True},
    {'requested_at_utc': '2026-09-09T00:00:00+00:00'},
    {'received_at_utc': '2026-09-09T00:00:00+00:00'},
    {'received_at_utc': '2026-09-08T06:16:40'},
])
def test_unverified_receipt_cannot_be_replayed(tmp_path, changes):
    path, digest = freeze(tmp_path, **changes)
    with pytest.raises(ValueError): source.replay(path, manifest_sha256=digest, now=NOW)


@pytest.mark.parametrize('target', ['manifest', 'raw', 'missing'])
def test_modified_or_missing_evidence_fails(tmp_path, target):
    path, digest = freeze(tmp_path)
    raw_path = tmp_path / (sha256(RAW).hexdigest() + '.html')
    if target == 'missing': raw_path.unlink()
    else: (path if target == 'manifest' else raw_path).write_bytes(b'changed')
    with pytest.raises((ValueError, OSError)): source.replay(path, manifest_sha256=digest, now=NOW)

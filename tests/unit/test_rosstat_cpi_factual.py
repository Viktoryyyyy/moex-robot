from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path

import pytest

from moex_research.external_data import rosstat_cpi_factual as source
from moex_research.external_data import rosstat_https as transport

NOW = datetime(2026, 9, 8, 7, 0, tzinfo=timezone.utc)
URL = 'https://rosstat.gov.ru/storage/mediabank/134_02-09-2026.html'
RAW = (Path(__file__).parents[1] / 'fixtures/rosstat/weekly_cpi_excerpt.html').read_bytes()


def row(url=URL, label='с 25 по 31 августа 2026 года', day='02.09.2026'):
    return f'<div class="document-list__item document-list__item--row"><a href="{url}">HTML</a><div class="document-list__item-title">{label}</div><div class="document-list__item-info">135.09 Кб, {day}</div></div>'


def index(*rows):
    return ('<div class="toggle-card"><div class="toggle-card__title">' + source.TITLE + '</div>' + ''.join(rows or [row()]) + '</div>').encode()


def test_select_uses_latest_publication_not_dom_order_or_other_sections():
    raw = index(row(day='26.08.2026', url=URL.replace('134_', '133_')), row())
    raw += row(day='09.09.2026').encode()  # announcement outside weekly archive
    selected = source.select(raw, now=NOW)
    assert selected['source_url'] == URL
    assert selected['listed_publication_date'] == '2026-09-02'


@pytest.mark.parametrize('new', [
    row(day='03.09.2026', url=URL.replace('.html', '.pdf')),
    row(day='03.09.2026', url='https://example.org/x.html'),
    row(day='03.09.2026', label='с 28 июля по 3 августа 2026 года'),
    row(day='09.09.2026'), row(day='n/a'), row(),
])
def test_bad_or_ambiguous_newest_is_not_replaced_by_older_good(new):
    with pytest.raises(ValueError): source.select(index(row(), new), now=NOW)


@pytest.mark.parametrize('raw', [b'', b'<html>missing</html>', index()+index()])
def test_missing_or_duplicate_archive_fails(raw):
    with pytest.raises(ValueError): source.select(raw, now=NOW)


def test_expired_publication_fails():
    with pytest.raises(ValueError): source.select(index(), now='2026-09-13T00:00:00+00:00')


def freeze(tmp_path, raw, url, requested, received):
    raw_hash = sha256(raw).hexdigest()
    (tmp_path / (raw_hash + '.html')).write_bytes(raw)
    receipt = {'policy': transport.POLICY, 'source_url': url, 'raw_sha256': raw_hash,
        'certificate_sha256': transport.CERTIFICATES, 'tls_chain_and_hostname_verified': True,
        'semantic_validation_status': 'NOT_PARSED', 'factual_authority': False,
        'historical_pit_acceptance': False, 'action_authority': False,
        'requested_at_utc': requested, 'received_at_utc': received}
    encoded = json.dumps(receipt).encode()
    digest = sha256(encoded).hexdigest()
    path = tmp_path / (digest + '.json')
    path.write_bytes(encoded)
    return str(path), digest


def component(tmp_path, archive=None):
    i, ih = freeze(tmp_path, archive or index(), source.INDEX_URL, '2026-09-08T06:59:00+00:00', '2026-09-08T06:59:01+00:00')
    d, dh = freeze(tmp_path, RAW, URL, '2026-09-08T06:59:02+00:00', '2026-09-08T06:59:03+00:00')
    refs = dict(index_manifest_path=i, index_manifest_sha256=ih, document_manifest_path=d, document_manifest_sha256=dh)
    return {'status': 'READY', 'data': source._replay(refs, now=NOW)}


def test_verified_pair_admits_only_weekly_dated_context(tmp_path):
    original = component(tmp_path)
    result = source.reconcile(original, now=NOW)
    assert result['status'] == 'READY'
    data = result['data']
    assert data['indices']['previous_registration'] == '99.99'
    assert data['consumer_factual_use_allowed'] is True
    assert data['system_available_at'] == '2026-09-08T06:59:03+00:00'
    for key in ('historical_pit_acceptance', 'action_authority', 'calendar_accepted', 'full_rosstat_macro_accepted', 'forecast_alignment_accepted'):
        assert data[key] is False
    assert data['source_publication_time'] is None
    assert 'read_freshness_reason' not in original['data']


@pytest.mark.parametrize('defect', ['expired', 'refresh', 'retained', 'blocked', 'numbers', 'authority', 'raw', 'index', 'manifest'])
def test_consumer_downgrades_expired_failed_tampered_or_unaccepted(tmp_path, defect):
    value = component(tmp_path)
    now = NOW
    if defect == 'expired': now = '2026-09-08T07:20:00+00:00'
    elif defect == 'refresh': value['refresh_error'] = 'failed'
    elif defect == 'retained': value['status'] = 'RETAINED_PREVIOUS'
    elif defect == 'blocked': value['data']['factual_authority'] = False
    elif defect == 'numbers': value['data']['indices']['year_start'] = '999'
    elif defect == 'authority': value['data']['historical_pit_acceptance'] = True
    elif defect == 'raw': (tmp_path / (sha256(RAW).hexdigest() + '.html')).write_bytes(b'changed')
    elif defect == 'index': (tmp_path / (sha256(index()).hexdigest() + '.html')).write_bytes(b'changed')
    else: Path(value['data']['document_manifest_path']).unlink()
    result = source.reconcile(value, now=now)
    assert result['status'] == 'UNAVAILABLE'
    assert result['data']['consumer_factual_use_allowed'] is False
    assert result['data']['action_authority'] is False


def test_archive_document_period_conflict_fails(tmp_path):
    with pytest.raises(ValueError, match='periods disagree'):
        component(tmp_path, archive=index(row(label='с 24 по 31 августа 2026 года')))


def test_source_matrix_and_release_keep_full_macro_blocked(tmp_path):
    from moex_data.rub_factual_release import describe
    snapshot = {'identity': {'generated_at_utc': NOW.isoformat()}, 'components': {'rosstat_cpi': component(tmp_path)}}
    release = describe(snapshot)
    assert 'rosstat_macro' in release['blocking_required_factors']
    assert any(f['factor'] == 'rosstat_cpi' for f in release['facts'])
    row = next(r for r in release['matrix'] if r['block_id'] == 'rosstat_macro')
    assert row['factual_context_usable'] is True
    assert row['usable_for_full_forecast'] is False


def test_canonical_refresh_failure_and_readtime_revoke(tmp_path):
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    def fail(now): raise ValueError('source failed')
    original = component(tmp_path)
    result = runner._component_payload('rosstat_cpi', fail, now=NOW, previous={'components': {'rosstat_cpi': original}})
    assert result['status'] == 'UNAVAILABLE'
    assert result['data']['consumer_factual_use_allowed'] is False
    snapshot = {'components': {'rosstat_cpi': original}}
    result = apply_read_freshness(snapshot, now=datetime(2026, 9, 8, 7, 21, tzinfo=timezone.utc))
    assert result['components']['rosstat_cpi']['data']['factual_authority'] is False


def test_canonical_default_producer_calls_loader(monkeypatch, tmp_path):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
    monkeypatch.setattr(source, 'load', lambda **kwargs: {'received_at': NOW.isoformat()})
    assert runner.default_producers()['rosstat_cpi'](NOW).data_as_of == NOW.isoformat()


@pytest.mark.parametrize('freshness', [None, 'invalid', 7, [], True, {}, {'read_at_utc': ''}, {'read_at_utc': None}])
def test_malformed_freshness_blocks_rosstat_without_crashing_matrix(tmp_path, freshness):
    from moex_data.rub_production_source_matrix import build
    snapshot = {'identity': {'generated_at_utc': NOW.isoformat()},
                'live_read_freshness': freshness, 'components': {'rosstat_cpi': component(tmp_path)}}
    result = build(snapshot)
    row = next(r for r in result['rows'] if r['block_id'] == 'rosstat_macro')
    assert row['factual_context_usable'] is False

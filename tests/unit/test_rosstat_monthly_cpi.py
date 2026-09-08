from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path

import pytest
from moex_research.external_data import rosstat_monthly_cpi as source
from moex_research.external_data import rosstat_https as transport

NOW = datetime(2026, 9, 8, 7, tzinfo=timezone.utc)
URL = 'https://rosstat.gov.ru/storage/mediabank/120_12-08-2026.html'
FIXTURES = Path(__file__).parents[1] / 'fixtures/rosstat'
RAW = (FIXTURES / 'monthly_cpi_july_excerpt.html').read_bytes()


def index(extra=''):
    return (f'<div class="toggle-card"><div class="toggle-card__title">{source.TITLE}</div>'
        f'<div class="content"><a href="{URL}">в июле 2026 года</a>{extra}</div></div>').encode()


def freeze(root, raw, url, requested, received):
    root.mkdir(parents=True, exist_ok=True)
    digest = sha256(raw).hexdigest(); (root / (digest+'.html')).write_bytes(raw)
    receipt = {'policy': transport.POLICY, 'source_url': url, 'raw_sha256': digest,
        'certificate_sha256': transport.CERTIFICATES, 'tls_chain_and_hostname_verified': True,
        'semantic_validation_status': 'NOT_PARSED', 'factual_authority': False,
        'historical_pit_acceptance': False, 'action_authority': False,
        'requested_at_utc': requested.isoformat(), 'received_at_utc': received.isoformat()}
    encoded = json.dumps(receipt).encode(); digest = sha256(encoded).hexdigest()
    path = root / (digest+'.json'); path.write_bytes(encoded)
    return str(path), digest


def component(tmp_path, now=NOW):
    i, ih = freeze(tmp_path, index(), source.INDEX_URL, now-timedelta(seconds=4), now-timedelta(seconds=3))
    d, dh = freeze(tmp_path, RAW, URL, now-timedelta(seconds=2), now-timedelta(seconds=1))
    refs = {'index_manifest_path': i, 'index_manifest_sha256': ih,
        'document_manifest_path': d, 'document_manifest_sha256': dh}
    return {'status': 'READY', 'data': source._replay(refs, now=now)}


@pytest.mark.parametrize('file,now,expected', [
    ('july', NOW, ['100.54','104.75','105.98']),
    ('january', '2026-02-14T12:00:00+00:00', ['101.62','101.62','106.00']),
    ('december', '2026-01-17T12:00:00+00:00', ['100.32','105.59','105.59']),
])
def test_real_monthly_formats_have_correct_bases_not_cumulative_average(file, now, expected):
    parsed = source.parse((FIXTURES / f'monthly_cpi_{file}_excerpt.html').read_bytes(), received_at=now)
    assert list(parsed['indices'].values()) == expected
    assert parsed['units'] == 'index_percent_base_100'
    assert parsed['decimal_places'] == 2
    assert parsed['precision_scope'] == 'printed_granularity_not_error_bound'
    assert parsed['frequency'] == 'MONTHLY'
    assert parsed['estimate_kind'] == 'PUBLISHED_MONTHLY_INDEX'
    assert 'monthly_final' not in parsed
    if file == 'july': assert parsed['changes_percent']['same_month_previous_year'] == '5.98'


@pytest.mark.parametrize('old,new', [('100,54%', '100,55%'),('104,75%', '104,76%'),
    ('июлю 2025 г.', 'июлю 2024 г.'), ('105,98', 'NaN'), ('105,98','-5,00'),
    ('Июль 2026 г. к', 'Июнь 2026 г. к')])
def test_wrong_values_bases_or_precision_block(old,new):
    with pytest.raises(ValueError): source.parse(RAW.decode().replace(old,new).encode(),received_at=NOW)


@pytest.mark.parametrize('extra', [
    '<a href="https://rosstat.gov.ru/storage/mediabank/new.pdf">в августе 2026 года</a>',
    '<a href="https://example.org/new.html">в августе 2026 года</a>',
    f'<a href="{URL}">в июле 2026 года</a>',
    '<a href="https://rosstat.gov.ru/storage/mediabank/new.html">в сентябре 2026 года</a>',
])
def test_newest_bad_archive_entry_never_falls_back(extra):
    with pytest.raises(ValueError): source.select(index(extra),now=NOW)


def test_calendar_is_optional_but_archive_scope_and_month_are_required():
    assert source.select(index(),now=NOW)['observation_month'] == '2026-07'
    with pytest.raises(ValueError): source.select(index()+index(),now=NOW)
    with pytest.raises(ValueError): source.select(index().replace(source.TITLE.encode(),b'other'),now=NOW)


def test_received_fact_is_replayable_without_publication_or_historical_invention(tmp_path):
    original = component(tmp_path); before = deepcopy(original)
    result = source.reconcile(original,now=NOW)
    assert result['status']=='READY'
    assert result['data']['listed_publication_date'] is None
    assert result['data']['source_publication_time'] is None
    assert result['data']['consumer_factual_use_allowed'] is True
    assert all(result['data'][key] is False for key in source.DENIED)
    assert original==before


@pytest.mark.parametrize('defect',['expired','raw','manifest','value','retained','failed','missing','shape'])
def test_bad_evidence_revokes_received_factual_scope(tmp_path,defect):
    value=component(tmp_path); data=value['data']; now=NOW
    path=Path(data['document_manifest_path'])
    if defect=='expired': now+=timedelta(seconds=1201)
    elif defect=='raw': path.with_name(data['raw_sha256']+'.html').write_bytes(b'changed')
    elif defect=='manifest': path.write_bytes(b'{}')
    elif defect=='missing': path.unlink()
    elif defect=='value': data['indices']['previous_month']='999'
    elif defect=='retained': value['status']='RETAINED_PREVIOUS'
    elif defect=='failed': value['refresh_error']='failed'
    elif defect=='shape': value={'status':'READY','data':[]}
    result=source.reconcile(value,now=now)
    assert result['status']=='UNAVAILABLE'
    assert result['data']['consumer_factual_use_allowed'] is False


def test_monthly_lag_limit_is_not_receipt_freshness():
    with pytest.raises(ValueError): source.parse(RAW,received_at='2026-10-03T00:00:00+00:00')


def test_other_table_headings_cannot_validate_cpi_values():
    split = RAW.decode().replace('<tr><td>Индекс потребительских цен', '</table><table><tr><td>Индекс потребительских цен')
    with pytest.raises(ValueError, match='table identity'):
        source.parse(split.encode(), received_at=NOW)

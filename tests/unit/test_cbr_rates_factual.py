from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest

from moex_research.external_data import cbr_rates_factual as m

NOW = datetime(2026, 9, 8, 8, tzinfo=timezone.utc)


def table(headers, rows):
    return ('<table><tr>' + ''.join(f'<th>{x}</th>' for x in headers) + '</tr>' + ''.join(
        '<tr>' + ''.join(f'<td>{x}</td>' for x in row) + '</tr>' for row in rows) + '</table>').encode()


def raw(source, value='14.42', observation='07.09.2026', publication='08.09.2026', status='Standard'):
    if source == 'ruonia':
        return table(m.cbr.RUONIA_HEADERS, [(observation, value, '483.45', '57', '23', '13', '14', '14.5', '15', status, publication)])
    return table(m.cbr.KEY_RATE_HEADERS, [('01.06.2026', '15')])


def load(tmp_path):
    return m.load(root=tmp_path, now_fn=lambda: NOW,
        fetch=lambda url: raw('ruonia' if 'ruonia' in url else 'key_rate'))


def test_load_replay_and_no_historical_admission(tmp_path):
    data = load(tmp_path)
    assert data['received_at'] == NOW.isoformat()
    assert data['system_available_at'] == data['received_at']
    assert data['observations']['ruonia']['source_publication_time'] is None
    assert data['observations']['ruonia']['source_publication_date'] == '2026-09-08'
    assert data['observations']['ruonia']['value'] == 14.42
    assert all(data[k] is False for k in ('historical_pit_acceptance', 'forecast_use_allowed', 'full_macro_complete', 'action_authority'))
    assert len(list((tmp_path / 'raw/external/cbr_rates').glob('*.html'))) == 2
    view = m.reconcile({'status': 'READY', 'data': data}, now=NOW)
    assert view['status'] == 'READY'
    assert m.reconcile(view, now=NOW)['status'] == 'READY'


def test_canonical_producer_retention_and_factual_inventory(tmp_path, monkeypatch):
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
    from moex_data.rub_factual_release import describe
    from moex_data.rub_snapshot_read_freshness import apply_read_freshness
    data = load(tmp_path)
    monkeypatch.setenv('MOEX_DATA_ROOT', str(tmp_path))
    monkeypatch.setattr(m, 'load', lambda **kwargs: data)
    assert runner.default_producers()['cbr_rates_verified'](NOW).data == data
    snapshot = {'identity': {'generated_at_utc': NOW.isoformat()},
        'components': {'cbr_rates_verified': {'status': 'READY', 'data': data}}}
    release = describe(apply_read_freshness(snapshot, now=NOW))
    assert any(item['factor'] == 'cbr_rates_verified' for item in release['facts'])
    assert next(row for row in release['matrix'] if row['block_id'] == 'cbr_rates')['factual_context_usable'] is True
    def fail(now): raise ValueError('source failure')
    retained = runner._component_payload('cbr_rates_verified', fail, now=NOW, previous=snapshot)
    assert retained['status'] == 'UNAVAILABLE'
    assert retained['data']['consumer_factual_use_allowed'] is False
    expired = apply_read_freshness(snapshot, now=NOW + timedelta(seconds=1201))
    assert expired['components']['cbr_rates_verified']['data']['factual_authority'] is False


@pytest.mark.parametrize('seconds,expected', [(1200, 'READY'), (1201, 'UNAVAILABLE'), (-1, 'UNAVAILABLE')])
def test_receipt_freshness(tmp_path, seconds, expected):
    assert m.reconcile({'status': 'READY', 'data': load(tmp_path)}, now=NOW + timedelta(seconds=seconds))['status'] == expected


@pytest.mark.parametrize('field,value', [('value', -1), ('unit', 'RUB'), ('source_publication_time', NOW.isoformat()), ('metric_id', 'other')])
def test_modified_fact_revokes(tmp_path, field, value):
    data = load(tmp_path)
    data['observations']['ruonia'][field] = value
    view = m.reconcile({'status': 'READY', 'data': data}, now=NOW)
    assert view['status'] == 'UNAVAILABLE'
    assert view['data']['factual_authority'] is False


@pytest.mark.parametrize('target', ['raw', 'manifest'])
def test_modified_file_revokes(tmp_path, target):
    data = load(tmp_path)
    path = Path(data['manifest_path'])
    if target == 'raw':
        path = path.parent / (data['receipts']['ruonia']['raw_sha256'] + '.html')
    path.write_bytes(path.read_bytes() + b'changed')
    assert m.reconcile({'status': 'READY', 'data': data}, now=NOW)['status'] == 'UNAVAILABLE'


@pytest.mark.parametrize('target', ['receipts', 'observations', 'receipt', 'observation', 'manifest'])
def test_hash_consistent_malformed_manifest_types_revoke(tmp_path, target):
    import json
    data = load(tmp_path)
    evidence = json.loads(Path(data['manifest_path']).read_bytes())
    if target == 'manifest':
        evidence = ['receipts', 'observations']
    elif target in ('receipts', 'observations'):
        evidence[target] = ['ruonia', 'key_rate']
        data[target] = evidence[target]
    else:
        container = 'receipts' if target == 'receipt' else 'observations'
        evidence[container]['ruonia'] = ['malformed']
        data[container] = evidence[container]
    encoded = json.dumps(evidence, sort_keys=True, separators=(',', ':')).encode()
    digest = m.sha256(encoded).hexdigest()
    path = Path(data['manifest_path']).parent / (digest + '.json')
    path.write_bytes(encoded)
    data.update(manifest_path=str(path), manifest_sha256=digest)
    view = m.reconcile({'status': 'READY', 'data': data}, now=NOW)
    assert view['status'] == 'UNAVAILABLE'
    assert view['data']['consumer_factual_use_allowed'] is False


@pytest.mark.parametrize('kwargs', [{'value': '-1'}, {'value': 'nan'}, {'value': 'inf'}, {'value': '101'}, {'observation': '01.08.2026'}, {'observation': '09.09.2026', 'publication': '09.09.2026'}, {'publication': '09.09.2026'}, {'status': 'Unknown'}])
def test_invalid_source_rejected(kwargs):
    with pytest.raises(ValueError):
        m.parse(raw('ruonia', **kwargs), source='ruonia', url=m._url('ruonia', NOW.date()), received_at=NOW, now=NOW)


def test_stale_observation_rejected():
    with pytest.raises(ValueError):
        m.parse(raw('ruonia', observation='31.08.2026', publication='01.09.2026'), source='ruonia', url=m._url('ruonia', NOW.date()), received_at=NOW, now=NOW)


def test_malformed_latest_row_cannot_fall_back():
    body = raw('ruonia').replace(b'</table>', b'<tr><td>broken-date</td></tr></table>')
    with pytest.raises(ValueError):
        m.parse(body, source='ruonia', url=m._url('ruonia', NOW.date()), received_at=NOW, now=NOW)


@pytest.mark.parametrize('corrupt_date', ['broken-date', '', '32.08.2026'])
def test_corrupted_first_key_rate_date_cannot_fall_back(corrupt_date):
    body = table(m.cbr.KEY_RATE_HEADERS, [(corrupt_date, '14'), ('01.06.2026', '15')])
    with pytest.raises(ValueError):
        m.parse(body, source='key_rate', url=m._url('key_rate', NOW.date()), received_at=NOW, now=NOW)


@pytest.mark.parametrize('outside_date', ['09.09.2026', '01.01.2010'])
def test_key_rate_same_value_outside_query_rejected_before_dedup(outside_date):
    body = table(m.cbr.KEY_RATE_HEADERS, [(outside_date, '15'), ('01.06.2026', '15')])
    with pytest.raises(ValueError, match='raw key-rate row outside requested dates'):
        m.parse(body, source='key_rate', url=m._url('key_rate', NOW.date()), received_at=NOW, now=NOW)


def test_exact_official_grouped_key_rate_headings():
    rows = [*m.KEY_RATE_SUBHEADERS, ('27.07.2026', '14'), ('01.06.2026', '15')]
    body = table(m.KEY_RATE_WIDE_HEADER, rows)
    assert m.parse(body, source='key_rate', url=m._url('key_rate', NOW.date()), received_at=NOW, now=NOW)['value'] == 14
    for broken in ([*m.KEY_RATE_SUBHEADERS, ('broken', '14'), ('01.06.2026', '15')],
                   [('unknown subheader',), *rows], [*m.KEY_RATE_SUBHEADERS[:-1], ('27.07.2026', '14')]):
        with pytest.raises(ValueError):
            m.parse(table(m.KEY_RATE_WIDE_HEADER, broken), source='key_rate', url=m._url('key_rate', NOW.date()), received_at=NOW, now=NOW)


def test_no_promotion_after_failure(tmp_path):
    data = load(tmp_path)
    for component in ({'status': 'UNAVAILABLE', 'data': data}, {'status': 'READY', 'refresh_error': 'failed', 'data': data}):
        view = m.reconcile(component, now=NOW)
        assert view['status'] == 'UNAVAILABLE'
        assert m.reconcile(view, now=NOW)['status'] == 'UNAVAILABLE'


def test_source_identity_rejected():
    with pytest.raises(ValueError):
        m.parse(raw('ruonia'), source='ruonia', url='https://evil.example/', received_at=NOW, now=NOW)


def test_date_boundary_and_clock_reverse_rejected(tmp_path):
    for end in (NOW - timedelta(seconds=1), NOW + timedelta(days=1)):
        clock = iter([NOW, end])
        with pytest.raises(ValueError):
            m.load(root=tmp_path, now_fn=lambda: next(clock), fetch=lambda url: raw('ruonia'))


def test_apply_changes_only_verified_component(tmp_path):
    snapshot = {'components': {'cbr_rates_verified': {'status': 'READY', 'data': load(tmp_path)}, 'cbr_macro': {'data': {'legacy': True}}}}
    legacy = deepcopy(snapshot['components']['cbr_macro'])
    m.apply(snapshot, now=NOW + timedelta(seconds=1201))
    assert snapshot['components']['cbr_rates_verified']['status'] == 'UNAVAILABLE'
    assert snapshot['components']['cbr_macro'] == legacy

from email.message import Message
from hashlib import sha256
import json
from pathlib import Path
import ssl

import pytest
from moex_research.external_data import rosstat_https as source

URL = 'https://rosstat.gov.ru/storage/mediabank/134_02-09-2026.html'
INDEX_URL = 'https://rosstat.gov.ru/compendium/document/50798'


@pytest.mark.parametrize('url', [
    'http://rosstat.gov.ru/storage/mediabank/a.html',
    'https://rosstat.gov.ru.example.org/storage/mediabank/a.html',
    'https://user@rosstat.gov.ru/storage/mediabank/a.html',
    'https://rosstat.gov.ru:8443/storage/mediabank/a.html',
    'https://rosstat.gov.ru/storage/mediabank/../a.html',
    'https://rosstat.gov.ru/storage/mediabank/%2e%2e/a.html',
    URL + '?redirect=https://example.org', URL + '#fragment',
    'https://rosstat.gov.ru/private',
])
def test_other_hosts_and_ambiguous_routes_refused(url):
    with pytest.raises(ValueError): source.validate_url(url)


def test_scoped_context_preserves_chain_hostname_and_tls_checks():
    before = ssl._create_default_https_context
    context = source.tls_context()
    assert context.check_hostname is True
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.minimum_version == ssl.TLSVersion.TLSv1_2
    assert ssl._create_default_https_context is before
    assert context.cert_store_stats()['x509_ca'] == 2


def test_altered_or_missing_certificates_fail_closed(tmp_path):
    with pytest.raises(OSError): source.tls_context(tmp_path)
    for name in source.CERTIFICATES:
        (tmp_path / name).write_bytes((source.CERTIFICATE_DIR / name).read_bytes())
    (tmp_path / 'rosstat_root.pem').write_bytes(b'changed')
    with pytest.raises(ValueError, match='hash'): source.tls_context(tmp_path)


def test_redirects_refused_even_within_same_host():
    with pytest.raises(ValueError, match='redirect'):
        source.NoRedirect().redirect_request(None, None, 302, '', {}, URL)


class Response:
    status = 200
    headers = Message()
    headers['Content-Type'] = 'text/html; charset=utf-8'
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def geturl(self): return URL
    def read(self, size): return b'<html>official candidate</html>'


def test_capture_freezes_receipt_without_granting_data_authority(monkeypatch, tmp_path):
    class Opener:
        def open(self, request, timeout): return Response()
    monkeypatch.setattr(source, 'build_opener', lambda *a: Opener())
    result = source.capture(URL, output=tmp_path)
    assert Path(result['manifest_path']).exists()
    assert (tmp_path / (result['raw_sha256'] + '.html')).read_bytes() == Response().read(100)
    assert result['tls_chain_and_hostname_verified'] is True
    assert result['factual_authority'] is False
    assert result['historical_pit_acceptance'] is False
    assert result['semantic_validation_status'] == 'NOT_PARSED'


@pytest.mark.parametrize('defect', ['route', 'status', 'type', 'empty', 'large'])
def test_invalid_document_cannot_be_captured(monkeypatch, tmp_path, defect):
    response = Response()
    if defect == 'route': response.geturl = lambda: 'https://example.org/'
    elif defect == 'status': response.status = 503
    elif defect == 'type':
        response.headers = Message()
        response.headers['Content-Type'] = 'application/json'
    elif defect == 'empty': response.read = lambda n: b''
    elif defect == 'large': response.read = lambda n: b'x' * n
    class Opener:
        def open(self, request, timeout): return response
    monkeypatch.setattr(source, 'build_opener', lambda *a: Opener())
    with pytest.raises(ValueError): source.capture(URL, output=tmp_path)
    assert not list(tmp_path.iterdir())


def _receipt(root, *, url, raw, requested):
    raw_sha = sha256(raw).hexdigest()
    (root / (raw_sha + '.html')).write_bytes(raw)
    receipt = {
        'policy': source.POLICY,
        'source_url': url,
        'requested_at_utc': requested,
        'received_at_utc': requested,
        'raw_sha256': raw_sha,
        'certificate_sha256': dict(source.CERTIFICATES),
        'tls_chain_and_hostname_verified': True,
        'factual_authority': False,
        'historical_pit_acceptance': False,
        'semantic_validation_status': 'NOT_PARSED',
        'action_authority': False,
    }
    encoded = json.dumps(receipt, sort_keys=True, separators=(',', ':')).encode()
    manifest_sha = sha256(encoded).hexdigest()
    path = root / (manifest_sha + '.json')
    path.write_bytes(encoded)
    return path, root / (raw_sha + '.html')


def test_prune_source_receipts_removes_only_superseded_target_polling_evidence(tmp_path):
    old_manifest, old_raw = _receipt(tmp_path, url=INDEX_URL, raw=b'<html>old index</html>',
                                     requested='2026-09-14T08:00:00+00:00')
    keep_manifest, keep_raw = _receipt(tmp_path, url=INDEX_URL, raw=b'<html>current index</html>',
                                       requested='2026-09-14T08:10:00+00:00')
    release_manifest, release_raw = _receipt(tmp_path, url=URL, raw=b'<html>weekly release</html>',
                                             requested='2026-09-14T08:10:01+00:00')
    malformed = tmp_path / ('f' * 64 + '.json')
    malformed.write_bytes(b'not-json')

    result = source.prune_source_receipts(tmp_path, source_url=INDEX_URL,
                                          keep_manifests=(keep_manifest,))

    assert result['manifests_removed'] == 1
    assert result['raw_removed'] == 1
    assert result['bytes_removed'] > 0
    assert not old_manifest.exists() and not old_raw.exists()
    assert keep_manifest.exists() and keep_raw.exists()
    assert release_manifest.exists() and release_raw.exists()
    assert malformed.exists()


def test_prune_source_receipts_keeps_raw_referenced_by_retained_receipt(tmp_path):
    raw = b'<html>shared index body</html>'
    old_manifest, raw_path = _receipt(tmp_path, url=INDEX_URL, raw=raw,
                                      requested='2026-09-14T08:00:00+00:00')
    keep_manifest, same_raw_path = _receipt(tmp_path, url=INDEX_URL, raw=raw,
                                            requested='2026-09-14T08:10:00+00:00')
    assert same_raw_path == raw_path

    result = source.prune_source_receipts(tmp_path, source_url=INDEX_URL,
                                          keep_manifests=(keep_manifest,))

    assert result['manifests_removed'] == 1
    assert result['raw_removed'] == 0
    assert not old_manifest.exists()
    assert keep_manifest.exists()
    assert raw_path.exists()


def test_prune_source_receipts_refuses_keep_manifest_outside_target(tmp_path):
    evidence = tmp_path / 'evidence'; evidence.mkdir()
    outside = tmp_path / 'outside'; outside.mkdir()
    keep, _ = _receipt(outside, url=INDEX_URL, raw=b'<html>x</html>',
                       requested='2026-09-14T08:00:00+00:00')
    with pytest.raises(ValueError, match='outside target'):
        source.prune_source_receipts(evidence, source_url=INDEX_URL, keep_manifests=(keep,))

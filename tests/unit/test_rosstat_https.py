from email.message import Message
from pathlib import Path
import ssl
import pytest
from moex_research.external_data import rosstat_https as source

URL = 'https://rosstat.gov.ru/storage/mediabank/134_02-09-2026.html'


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

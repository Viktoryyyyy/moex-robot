"""Host-scoped verified Rosstat transport; acquisition grants no macro authority."""
import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import ssl
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

CERTIFICATE_DIR = Path(__file__).resolve().parents[3] / 'contracts/source_tls'
CERTIFICATES = {
    'rosstat_root.pem': '0819977502d9aed2234830f6ffb91f82f401d3674c6e51dd19e16d8b3dbf0eb4',
    'rosstat_sub2024.pem': '6f9d829c8e6712444fce3624658d8788672849c5d5b7b53fd9cf7e83eac4193e',
}
MAX_BYTES = 2_000_000
POLICY = 'rosstat_verified_https.v1'
_HASH = re.compile(r'[0-9a-f]{64}')


def validate_url(url):
    parts = urlsplit(url)
    if (parts.scheme != 'https' or parts.hostname != 'rosstat.gov.ru'
        or parts.port not in (None, 443) or parts.username or parts.password
        or parts.fragment or parts.query or '%' in parts.path or '..' in parts.path
        or not (parts.path.startswith('/storage/mediabank/')
            or parts.path in ('/statistics/price', '/compendium/document/50798'))):
        raise ValueError('URL outside explicit Rosstat document scope')


def tls_context(certificate_dir=CERTIFICATE_DIR):
    certificates = []
    for name, expected in CERTIFICATES.items():
        path = Path(certificate_dir) / name
        if path.is_symlink():
            raise ValueError('certificate symlink refused')
        raw = path.read_bytes()
        if sha256(raw).hexdigest() != expected:
            raise ValueError('certificate hash mismatch')
        certificates.append(raw.decode('ascii').strip())
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_verify_locations(cadata='\n'.join(certificates) + '\n')
    return context


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError('Rosstat source redirect refused')


def fetch(url, *, timeout=10):
    validate_url(url)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 30:
        raise ValueError('timeout must be within (0, 30] seconds')
    opener = build_opener(NoRedirect(), HTTPSHandler(context=tls_context()))
    requested = datetime.now(timezone.utc)
    with opener.open(Request(url, headers={'User-Agent': 'moex-robot-rosstat-factual/1',
                                          'Accept': 'text/html'}), timeout=timeout) as response:
        if response.geturl() != url or response.status != 200:
            raise ValueError('unexpected Rosstat response route or status')
        if response.headers.get_content_type() != 'text/html':
            raise ValueError('Rosstat HTML document required')
        raw = response.read(MAX_BYTES + 1)
    received = datetime.now(timezone.utc)
    if received < requested or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('invalid Rosstat receipt or document size')
    return raw, {'policy': POLICY, 'source_url': url, 'requested_at_utc': requested.isoformat(),
        'received_at_utc': received.isoformat(), 'raw_sha256': sha256(raw).hexdigest(),
        'certificate_sha256': dict(CERTIFICATES), 'tls_chain_and_hostname_verified': True,
        'factual_authority': False, 'historical_pit_acceptance': False,
        'semantic_validation_status': 'NOT_PARSED', 'action_authority': False}


def _freeze(path, raw):
    if path.is_symlink():
        raise ValueError('evidence symlink refused')
    try:
        with path.open('xb') as stream:
            stream.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise ValueError('evidence collision')


def capture(url, *, output, timeout=10):
    raw, evidence = fetch(url, timeout=timeout)
    directory = Path(output)
    if directory.is_symlink():
        raise ValueError('evidence directory symlink refused')
    directory.mkdir(parents=True, exist_ok=True)
    _freeze(directory / (evidence['raw_sha256'] + '.html'), raw)
    encoded = json.dumps(evidence, sort_keys=True, separators=(',', ':')).encode()
    digest = sha256(encoded).hexdigest()
    manifest = directory / (digest + '.json')
    _freeze(manifest, encoded)
    return {**evidence, 'manifest_path': str(manifest), 'manifest_sha256': digest}


def prune_source_receipts(output, *, source_url, keep_manifests=()):
    """Remove superseded polling receipts for one URL without touching release documents.

    Only valid content-addressed manifests using this transport policy are eligible.
    Raw HTML is removed only when no retained valid manifest references its hash.
    Unknown, malformed, non-file and symlink entries are left untouched.
    """
    validate_url(source_url)
    directory = Path(output)
    if not directory.exists():
        return {'manifests_removed': 0, 'raw_removed': 0, 'bytes_removed': 0}
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('evidence directory must be a regular directory')
    resolved = directory.resolve(strict=True)
    keep_names = set()
    for value in keep_manifests:
        path = Path(value)
        if not path.exists():
            raise ValueError('retained evidence manifest missing')
        if path.is_symlink() or not path.is_file():
            raise ValueError('retained evidence manifest must be a regular file')
        actual = path.resolve(strict=True)
        if actual.parent != resolved or not _HASH.fullmatch(actual.stem) or actual.suffix != '.json':
            raise ValueError('retained evidence manifest outside target directory')
        keep_names.add(actual.name)

    receipts = []
    for path in resolved.iterdir():
        if path.is_symlink() or not path.is_file() or path.suffix != '.json' or not _HASH.fullmatch(path.stem):
            continue
        try:
            encoded = path.read_bytes()
            if sha256(encoded).hexdigest() != path.stem:
                continue
            receipt = json.loads(encoded)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(receipt, dict):
            continue
        raw_sha = receipt.get('raw_sha256')
        if not isinstance(raw_sha, str) or not _HASH.fullmatch(raw_sha):
            continue
        receipts.append((path, receipt, raw_sha))

    removable = [(path, raw_sha) for path, receipt, raw_sha in receipts
                 if receipt.get('policy') == POLICY and receipt.get('source_url') == source_url
                 and path.name not in keep_names]
    removable_names = {path.name for path, _ in removable}
    retained_raw = {raw_sha for path, _, raw_sha in receipts if path.name not in removable_names}
    candidate_raw = {raw_sha for _, raw_sha in removable if raw_sha not in retained_raw}

    bytes_removed = 0
    manifests_removed = 0
    raw_removed = 0
    for path, _ in removable:
        try:
            bytes_removed += path.stat().st_size
            path.unlink()
            manifests_removed += 1
        except FileNotFoundError:
            pass
    for raw_sha in candidate_raw:
        path = resolved / (raw_sha + '.html')
        if path.is_symlink() or not path.is_file():
            continue
        try:
            raw = path.read_bytes()
        except OSError:
            continue
        if sha256(raw).hexdigest() != raw_sha:
            continue
        try:
            bytes_removed += path.stat().st_size
            path.unlink()
            raw_removed += 1
        except FileNotFoundError:
            pass
    return {'manifests_removed': manifests_removed, 'raw_removed': raw_removed,
            'bytes_removed': bytes_removed}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(capture(args.url, output=args.output), indent=2))

"""CLI transport/revision plumbing; factual acceptance has separate replay tests."""
from datetime import datetime
import json
from pathlib import Path

import pytest

from moex_data import rub_factual_release_acceptance as cli

HEAD = 'b' * 40
URL = 'http://127.0.0.1:8765/v1/rub/factual-snapshot'
SECRET = 'test-only-sensitive-bearer-value'


@pytest.fixture
def clean_git(monkeypatch):
    calls = []
    def git(args, *, cwd, text):
        calls.append(args)
        assert cwd == Path(cli.__file__).resolve().parents[2]
        assert text is True
        if args == ['git', 'rev-parse', 'HEAD']:
            return HEAD + '\n'
        assert args == ['git', 'status', '--porcelain', '--untracked-files=normal']
        return ''
    monkeypatch.setattr(cli.subprocess, 'check_output', git)
    return calls


@pytest.mark.parametrize('status,exit_code', [('PASS', 0), ('FAIL', 1)])
def test_frozen_cli_pins_executing_revision_and_returns_gate_status(tmp_path, monkeypatch, clean_git, capsys, status, exit_code):
    path = tmp_path / 'snapshot.json'
    snapshot = {'identity': {'generated_at_utc': '2026-09-08T13:00:00+00:00'}}
    path.write_text(json.dumps(snapshot), encoding='utf-8')
    seen = []
    def run(value, **kwargs):
        seen.append(kwargs)
        assert value == snapshot
        return {'status': status, 'code_revision': kwargs['code_revision']}
    monkeypatch.setattr(cli, 'run', run)
    assert cli.main(['--snapshot', str(path), '--as-of', '2026-09-08T13:01:00+00:00',
                     '--output', str(tmp_path / 'out')]) == exit_code
    assert seen == [{'now': datetime.fromisoformat('2026-09-08T13:01:00+00:00'),
                     'code_revision': HEAD, 'output': tmp_path / 'out'}]
    assert len(clean_git) == 2
    assert json.loads(capsys.readouterr().out)['status'] == status


def test_dirty_checkout_cannot_produce_acceptance(tmp_path, monkeypatch):
    monkeypatch.setattr(cli.subprocess, 'check_output',
                        lambda args, **kwargs: HEAD if args[1] == 'rev-parse' else ' M changed.py\n')
    monkeypatch.setattr(cli, 'run', lambda *a, **k: pytest.fail('gate must not run'))
    with pytest.raises(SystemExit) as exc:
        cli.main(['--api-url', URL, '--output', str(tmp_path)])
    assert exc.value.code == 2


@pytest.mark.parametrize('url', ['http://example.org/v1/rub/factual-snapshot',
    'http://127.0.0.1.example.org/v1/rub/factual-snapshot',
    'http://name:password@localhost/v1/rub/factual-snapshot',
    'http://localhost/v1/rub/factual-snapshot?next=example.org',
    'https://localhost/v1/rub/factual-snapshot'])
def test_noncanonical_endpoint_rejected_before_any_request(tmp_path, monkeypatch, clean_git, url):
    import requests
    monkeypatch.setattr(requests, 'Session', lambda: pytest.fail('no request for rejected URL'))
    with pytest.raises(SystemExit) as exc:
        cli.main(['--api-url', url, '--output', str(tmp_path)])
    assert exc.value.code == 2


@pytest.mark.parametrize('response_status,exit_code', [(200, 0), (302, 2)])
def test_live_cli_disables_proxies_and_redirects_without_printing_token(tmp_path, monkeypatch, clean_git, capsys, response_status, exit_code):
    import requests
    from src.misc import rub_factual_snapshot_http_server as server
    env_file = tmp_path / 'private.env'
    # Deliberately present proxy settings: the local bearer must ignore them.
    monkeypatch.setenv('HTTP_PROXY', 'http://untrusted-proxy.example:8080')
    monkeypatch.setenv('ALL_PROXY', 'http://untrusted-proxy.example:8080')
    monkeypatch.delenv('NO_PROXY', raising=False)
    def token(env):
        assert env == {'MOEX_ENV_FILE': str(env_file)}
        return SECRET
    monkeypatch.setattr(server, 'load_api_token', token)
    snapshot = {'identity': {'generated_at_utc': '2026-09-08T13:00:00+00:00'}}
    calls = []
    class Session:
        trust_env = True
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def get(self, url, **kwargs):
            assert self.trust_env is False
            assert url == URL
            assert kwargs == {'headers': {'Authorization': 'Bearer ' + SECRET},
                              'timeout': 60, 'allow_redirects': False}
            calls.append(url)
            class Response:
                status_code = response_status
                def json(self):
                    assert response_status == 200
                    return snapshot
            return Response()
    monkeypatch.setattr(requests, 'Session', Session)
    def run(value, **kwargs):
        assert response_status == 200
        assert value == snapshot and kwargs['code_revision'] == HEAD
        assert kwargs['now'].utcoffset() is not None
        return {'status': 'PASS', 'code_revision': HEAD}
    monkeypatch.setattr(cli, 'run', run)
    args = ['--api-url', URL, '--env-file', str(env_file), '--output', str(tmp_path / 'out')]
    if response_status == 200:
        assert cli.main(args) == exit_code
    else:
        with pytest.raises(SystemExit) as exc:
            cli.main(args)
        assert exc.value.code == exit_code
    assert calls == [URL]
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    assert not env_file.exists()

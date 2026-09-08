from __future__ import annotations

import ast
import http.client
import inspect
import json
import threading
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

import pytest

from src.misc import rub_factual_snapshot_http_server as api
from src.moex_research.consumers.usdrubf_chat_snapshot_consumer import (
    validate_analysis_chat_snapshot,
)


TOKEN = "test-bearer-token"


def _snapshot(*, readiness: str = "READY", freshness: str = "FRESH") -> dict[str, object]:
    return {
        "schema_version": "rub_chat_analysis_snapshot.v1",
        "identity": {
            "project": "MOEX_Bot",
            "mode": "s7_3_chat_analysis_snapshot",
            "generated_at_utc": "2026-08-31T15:20:00+00:00",
            "snapshot_kind": "server_persisted_data_only_context_for_separate_analysis_chats",
        },
        "refresh_policy": {
            "expected_refresh_interval_seconds": 600,
            "snapshot_stale_after_seconds": 1200,
            "component_failure_policy": "retain_previous_component_if_available_else_unavailable",
            "atomic_publish": True,
        },
        "readiness": {
            "status": readiness,
            "component_statuses": {
                "official_news": "READY",
                "oil": "GOVERNED_BLOCKED",
            },
            "unavailable_components": [],
            "retained_previous_components": [],
        },
        "components": {
            "official_news": {
                "status": "READY",
                "data_as_of": "2026-08-31T15:18:00+00:00",
                "data": {"items": []},
            },
            "oil": {
                "status": "GOVERNED_BLOCKED",
                "data_as_of": None,
                "data": {
                    "missing_oil_must_not_be_interpreted_as_neutral": True,
                    "action_authority": False,
                },
            },
        },
        "read_freshness": {
            "read_at_utc": "2026-08-31T15:20:05+00:00",
            "snapshot_age_seconds": 5,
            "status": freshness,
        },
        "authority": {
            "data_only": True,
            "server_generates_market_analysis": False,
            "server_generates_scenario": False,
            "server_generates_buy_sell_out": False,
            "server_generates_invalidation": False,
            "ema_standalone_directional_authority": False,
            "news_directional_action_authority": False,
            "broker_execution": False,
            "telegram_delivery": False,
        },
    }


@contextmanager
def _running_server(loader, *, release_loader=None):
    server = api.SnapshotHTTPServer(
        (api.DEFAULT_HOST, 0),
        api.SnapshotRequestHandler,
        api_token=TOKEN,
        snapshot_loader=loader,
        release_loader=release_loader,
    )
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _request(port: int, path: str, *, token: str | None = TOKEN, method: str = "GET"):
    headers = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    connection = http.client.HTTPConnection(api.DEFAULT_HOST, port, timeout=2)
    connection.request(method, path, headers=headers)
    response = connection.getresponse()
    raw = response.read()
    headers_out = dict(response.getheaders())
    connection.close()
    return response.status, headers_out, json.loads(raw.decode("utf-8"))


def test_snapshot_endpoint_preserves_canonical_payload_exactly() -> None:
    expected = _snapshot()
    with _running_server(lambda: deepcopy(expected)) as port:
        status, headers, body = _request(port, api.SNAPSHOT_PATH)

    assert status == 200
    assert body == expected
    assert headers["Cache-Control"] == "no-store"
    assert body["identity"]["generated_at_utc"] == "2026-08-31T15:20:00+00:00"  # type: ignore[index]
    assert body["read_freshness"]["read_at_utc"] == "2026-08-31T15:20:05+00:00"  # type: ignore[index]
    assert body["components"]["official_news"]["data_as_of"] == "2026-08-31T15:18:00+00:00"  # type: ignore[index]


def test_compact_endpoint_matches_current_export_same_reader_and_clock(tmp_path):
    import runpy
    from moex_data.rub_factual_release import export_current
    from src.moex_research.consumers.usdrubf_chat_snapshot_consumer import load_factual_release
    helper = runpy.run_path(str(Path(__file__).with_name('test_rub_factual_projection.py')))
    original = helper['core_snapshot'](); now = helper['NOW']; metadata = _snapshot()
    for key in ('schema_version', 'refresh_policy', 'readiness', 'authority'): original[key] = metadata[key]
    original['identity']['project'] = 'MOEX_Bot'
    original['read_freshness'] = {'status': 'FRESH', 'snapshot_age_seconds': 0, 'read_at_utc': now.isoformat()}
    original['fast_market_read'] = {'read_at': now.isoformat(), 'completed_at': now.isoformat(), 'error': None}
    calls = []
    def reader(*, now_fn):
        calls.append(now_fn())
        return deepcopy(original), tmp_path / 'unused'
    def compact_loader():
        return load_factual_release(now_fn=lambda: now, reader=reader, code_revision='a' * 40)
    with _running_server(lambda: deepcopy(original), release_loader=compact_loader) as port:
        status, headers, body = _request(port, api.RELEASE_PATH)
        assert status == 200 and headers['Cache-Control'] == 'no-store'
        assert _request(port, api.RELEASE_PATH, token=None)[0] == 401
        assert _request(port, api.RELEASE_PATH + '?as_of=yesterday')[0] == 400
    assert calls == [now]
    exported = export_current(output=tmp_path, now_fn=lambda: now, reader=reader, code_revision='a' * 40)
    assert json.loads(exported.read_bytes()) == body
    assert body['generations']['fast_market']['completed_at'] == now.isoformat()
    assert body['as_of_utc'] == now.isoformat() and body['status'] == 'PARTIAL'
    assert body['authority']['model_ready'] is False


def test_compact_failure_returns_503_without_old_snapshot_fallback():
    def failed(): raise ValueError('invalid source evidence')
    with _running_server(_snapshot, release_loader=failed) as port:
        status, _, body = _request(port, api.RELEASE_PATH)
    assert status == 503 and body == {'error': 'factual_release_unavailable'}


def test_api_preserves_complete_spot_and_partial_basis_projection(tmp_path, monkeypatch):
    import runpy
    from moex_data.rub_factual_release import describe
    from moex_data.rub_factual_release_acceptance import projection_completeness
    from moex_research.runners.usdrubf_s7_3_chat_analysis_snapshot import current_snapshot_path, read_current_snapshot
    from moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as runner
    monkeypatch.setattr(runner, '_data_root', lambda: tmp_path)
    helpers = runpy.run_path(str(Path(__file__).with_name('test_rub_factual_projection.py')))
    original = helpers['snapshot'](); now = helpers['NOW']
    original['schema_version'] = runner.SCHEMA_VERSION
    original['identity']['project'] = runner.PROJECT
    path = current_snapshot_path(tmp_path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(original), encoding='utf-8')
    def loader():
        return read_current_snapshot(now_fn=lambda: now)[0]
    with _running_server(loader) as port:
        status, _, body = _request(port, api.SNAPSHOT_PATH)
    assert status == 200
    projection_completeness(original, body['factual_release'], now=now)
    assert body['factual_release']['facts'] == describe(body)['facts']
    assert json.loads(path.read_text()) == original


@pytest.mark.parametrize(
    ("readiness", "freshness"),
    [("PARTIAL", "FRESH"), ("READY", "STALE"), ("PARTIAL", "STALE")],
)
def test_snapshot_endpoint_returns_degraded_state_without_hiding_it(
    readiness: str, freshness: str
) -> None:
    expected = _snapshot(readiness=readiness, freshness=freshness)
    with _running_server(lambda: deepcopy(expected)) as port:
        status, _, body = _request(port, api.SNAPSHOT_PATH)
        ready_status, _, ready_body = _request(port, api.READINESS_PATH)

    assert status == 200
    assert body == expected
    assert ready_status == 503
    assert ready_body["status"] == "NOT_READY"
    assert ready_body["snapshot_readiness"] == readiness
    assert ready_body["snapshot_freshness"] == freshness


def test_readiness_is_ready_only_for_ready_and_fresh_snapshot() -> None:
    with _running_server(lambda: _snapshot()) as port:
        status, _, body = _request(port, api.READINESS_PATH)

    assert status == 200
    assert body == {
        "status": "READY",
        "schema_version": "rub_chat_analysis_snapshot.v1",
        "snapshot_generated_at_utc": "2026-08-31T15:20:00+00:00",
        "snapshot_read_at_utc": "2026-08-31T15:20:05+00:00",
        "snapshot_readiness": "READY",
        "snapshot_freshness": "FRESH",
    }


def test_missing_snapshot_fails_predictably() -> None:
    def missing():
        raise RuntimeError("current snapshot does not exist")

    with _running_server(missing) as port:
        status, _, body = _request(port, api.SNAPSHOT_PATH)

    assert status == 503
    assert body == {"error": "snapshot_unavailable"}


def test_malformed_snapshot_is_rejected_by_governed_consumer_validation() -> None:
    malformed = _snapshot()
    malformed["schema_version"] = "wrong.schema"

    def loader():
        validate_analysis_chat_snapshot(malformed)
        return malformed

    with _running_server(loader) as port:
        status, _, body = _request(port, api.SNAPSHOT_PATH)

    assert status == 503
    assert body == {"error": "snapshot_unavailable"}


def test_non_strict_json_snapshot_fails_predictably_for_data_and_readiness() -> None:
    malformed = _snapshot()
    malformed["components"]["official_news"]["data"]["invalid"] = float("nan")  # type: ignore[index]

    with _running_server(lambda: malformed) as port:
        status, _, body = _request(port, api.SNAPSHOT_PATH)
        ready_status, _, ready_body = _request(port, api.READINESS_PATH)

    assert status == 503
    assert body == {"error": "snapshot_unavailable"}
    assert ready_status == 503
    assert ready_body == {"status": "NOT_READY", "reason": "snapshot_unavailable"}


@pytest.mark.parametrize("token", [None, "wrong-token", " test-bearer-token", "test-bearer-token "])
def test_snapshot_endpoint_requires_exact_bearer_token(token: str | None) -> None:
    calls = 0

    def loader():
        nonlocal calls
        calls += 1
        return _snapshot()

    with _running_server(loader) as port:
        status, headers, body = _request(port, api.SNAPSHOT_PATH, token=token)

    assert status == 401
    assert body == {"error": "unauthorized"}
    assert headers["WWW-Authenticate"] == "Bearer"
    assert calls == 0


def test_authenticated_snapshot_request_calls_only_configured_snapshot_loader_once() -> None:
    calls = 0

    def loader():
        nonlocal calls
        calls += 1
        return _snapshot()

    with _running_server(loader) as port:
        status, _, _ = _request(port, api.SNAPSHOT_PATH)

    assert status == 200
    assert calls == 1


def test_transport_source_has_no_upstream_refresh_or_action_imports() -> None:
    source = inspect.getsource(api)
    tree = ast.parse(source)
    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    assert "refresh_snapshot" not in source
    assert not any(module == "requests" or module.startswith("requests.") for module in imported_modules)
    assert not any("external_data" in module for module in imported_modules)
    assert not any("broker" in module.lower() for module in imported_modules)
    assert not any("telegram" in module.lower() for module in imported_modules)
    assert "src.moex_research.consumers.usdrubf_chat_snapshot_consumer" in imported_modules


def test_mutating_http_methods_are_not_available_and_do_not_load_snapshot() -> None:
    calls = 0

    def loader():
        nonlocal calls
        calls += 1
        return _snapshot()

    with _running_server(loader) as port:
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            status, _, body = _request(port, api.SNAPSHOT_PATH, method=method)
            assert status == 405
            assert body == {"error": "method_not_allowed"}

    assert calls == 0


def test_load_api_token_prefers_direct_environment() -> None:
    assert api.load_api_token({api.TOKEN_ENV: TOKEN}) == TOKEN


def test_load_api_token_reads_only_governed_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "project.env"
    env_file.write_text(f"OTHER_SECRET=ignored\n{api.TOKEN_ENV}={TOKEN}\n", encoding="utf-8")

    assert api.load_api_token({api.ENV_FILE_ENV: str(env_file)}) == TOKEN


@pytest.mark.parametrize(
    "environ",
    [
        {},
        {api.TOKEN_ENV: ""},
        {api.TOKEN_ENV: " token"},
        {api.TOKEN_ENV: "token "},
        {api.TOKEN_ENV: "token with space"},
        {api.TOKEN_ENV: "tökën"},
        {api.ENV_FILE_ENV: "relative.env"},
    ],
)
def test_load_api_token_fails_closed_for_invalid_configuration(environ: dict[str, str]) -> None:
    with pytest.raises(api.SnapshotAPIConfigurationError):
        api.load_api_token(environ)


def test_create_server_refuses_non_loopback_binding() -> None:
    with pytest.raises(api.SnapshotAPIConfigurationError, match="must bind to loopback"):
        api.create_server(api_token=TOKEN, host="0.0.0.0", port=8765)


def test_create_server_rejects_non_ascii_token_before_binding() -> None:
    with pytest.raises(api.SnapshotAPIConfigurationError, match="must be ASCII"):
        api.create_server(api_token="tökën", port=8765)

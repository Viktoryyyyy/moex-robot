"""Synthetic inputs; real refresh, saved JSON, canonical reader, HTTP and MCP."""
import asyncio
from copy import deepcopy
from datetime import timedelta
import json
import threading

import pytest
import requests

from test_stage9_analysis_bundle_v2 import NOW, source_io, live, shifted_market
from src.moex_research.consumers import usdrubf_chat_snapshot_consumer as consumer
from src.misc import rub_factual_snapshot_http_server as api
from src.misc import mcp_rub_factual_snapshot_bridge as bridge
from src.misc import moex_analyst_web_chat as chat
from moex_data import rub_market_factual_delivery as delivery


@pytest.mark.parametrize('failure,corrupt', [(None, None), (('si', '2026-09-24'), None), (('cr', '2026-09-24'), None),
    (None, ('price', 'invalid')), (None, ('price', '2026-09-24T07:35:00')),
    (None, ('futoi', 'invalid')), (None, ('futoi', '2026-09-24T07:35:00')),
    (None, ('price', 123)), (None, ('futoi', 123))])
def test_real_refresh_saved_reader_http_mcp_final_delivery(tmp_path, monkeypatch, failure, corrupt):
    source_io(tmp_path, monkeypatch, failed=failure)
    saved, path = live.refresh_snapshot(now_fn=lambda: NOW, live_loader=lambda: shifted_market(NOW))
    if corrupt:
        kind, stamp = corrupt
        if kind == 'price':
            saved['components']['synchronized_live_market_oi']['data']['instruments']['cr_next']['timestamp'] = stamp
        else:
            saved['components']['futoi_live']['data']['current_intraday']['factual']['snapshot_ts'] = stamp
        live.base._atomic_write(path, saved)
    raw = path.read_bytes()
    def load():
        return consumer.load_market_factual(now_fn=lambda: NOW, reader=live.base.read_current_snapshot, code_revision='a'*40)
    server = api.SnapshotHTTPServer(('127.0.0.1', 0), api.SnapshotRequestHandler,
        api_token='test-token', snapshot_loader=lambda: saved, market_loader=load)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.01}, daemon=True)
    thread.start()
    monkeypatch.setattr(bridge, 'UPSTREAM_BASE_URL', 'http://127.0.0.1:'+str(server.server_port))
    monkeypatch.setattr(bridge, '_bridge', bridge.RubFactualSnapshotHTTPBridge(api_token='test-token'))
    try:
        async def call():
            result = await bridge.mcp.call_tool('get_rub_market_factual', {})
            # FastMCP keeps the exact HTTP JSON as the structured result.
            return result[1]
        result = asyncio.run(call())
        assert result['schema_version'] == delivery.SCHEMA
        assert len(delivery.encoded(result)) < delivery.MAX_BYTES
        assert 'base64' not in json.dumps(result)
        assert result['prices']['si_front']['values']['last'] == saved['components']['synchronized_live_market_oi']['data']['instruments']['si_front']['last']
        assert result['basis_carry']['admitted_metric_count'] > 0
        assert result['authority']['analysis_bundle_complete'] is False
        for root in ('si', 'cr'):
            item = result['futoi'][root]
            assert (item['status'] == 'AVAILABLE') is (failure != (root, '2026-09-24') and not (corrupt and corrupt[0] == 'futoi' and root == 'si'))
            assert item['source_identity']['source_ticker'] == root
            assert item['source_identity']['raw_schema_version'] == 'v2'
            assert 'secid' not in item['source_identity']
            if item['status'] == 'AVAILABLE':
                assert item['values']['sess_id'] == 2**53+1
                assert item['evidence_verification']['status'] == 'PASS'
                assert item['evidence']['provenance']['publication_audit']['sha256']
            else:
                assert item['values'] is None and item['reason']
        expected = load()
        assert result == expected
        # Only external model I/O is synthetic; the actual chat client routes
        # the tool request through MCP and the canonical HTTP reader again.
        posts = []
        def post(*args, **kwargs):
            posts.append(kwargs['json'])
            class Response:
                status_code = 200
                def json(self):
                    if len(posts) == 1:
                        return {'output': [{'type': 'function_call', 'name': 'get_rub_market_factual',
                            'call_id': 'synthetic-market-call', 'arguments': '{}'}]}
                    assert json.loads(posts[-1]['input'][-1]['output']) == result
                    return {'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'Synthetic model answer'}]}]}
            return Response()
        def tool_caller(url, name):
            assert name == 'get_rub_market_factual'
            return asyncio.run(call())
        client = chat.OpenAIResponsesClient(api_key='synthetic', model=chat.DEFAULT_MODEL,
            mcp_url=chat.DEFAULT_MCP_URL, post=post, tool_caller=tool_caller)
        assert client.answer([{'role': 'user', 'content': 'Price FUTOI basis carry'}]) == 'Synthetic model answer'
        assert 'get_rub_market_factual' in [tool['name'] for tool in posts[0]['tools']]
        assert 'use get_rub_market_factual' in posts[0]['instructions']
        if corrupt:
            freshness = result['prices']['cr_next']['freshness'] if corrupt[0] == 'price' else result['futoi']['si']['freshness']['source']
            assert freshness['source_timestamp'] == corrupt[1]
            assert freshness['age_seconds'] is None and freshness['valid_until_utc'] is None
            assert freshness['reason'] == 'source_timestamp_missing_or_invalid'
            if corrupt[0] == 'price':
                assert result['prices']['cr_next']['values'] is None
                assert all('cr_next' not in row['values']['legs'] for row in result['basis_carry']['metrics'])
        assert requests.get(bridge.UPSTREAM_BASE_URL+api.MARKET_PATH, timeout=5).status_code == 401
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)
    assert path.read_bytes() == raw


@pytest.mark.parametrize('seconds', [61, 1201])
def test_delivery_clock_revokes_expired_read_without_refresh_or_fallback(tmp_path, monkeypatch, seconds):
    source_io(tmp_path, monkeypatch)
    _, path = live.refresh_snapshot(now_fn=lambda: NOW, live_loader=lambda: shifted_market(NOW))
    clock = iter((NOW, NOW+timedelta(seconds=seconds)))
    result = consumer.load_market_factual(now_fn=lambda: next(clock), reader=live.base.read_current_snapshot, code_revision='a'*40)
    assert all(row['values'] is None and row['quote'] is None for row in result['prices'].values())
    assert result['basis_carry']['admitted_metric_count'] == 0
    for root in ('si', 'cr'):
        assert (result['futoi'][root]['values'] is None) is (seconds == 1201)
        if seconds == 1201:
            assert result['futoi'][root]['previous_dated_observation']['values'] is None
    assert result['canonical_read_at_utc'] == NOW.isoformat()
    assert result['as_of_utc'] == (NOW+timedelta(seconds=seconds)).isoformat()


def test_bounded_delivery_preserves_partial_metrics_and_refuses_oversize(tmp_path, monkeypatch):
    source_io(tmp_path, monkeypatch)
    saved, path = live.refresh_snapshot(now_fn=lambda: NOW, live_loader=lambda: shifted_market(NOW))
    changed = deepcopy(saved)
    changed['components']['synchronized_live_market_oi']['data']['instruments']['cr_next']['timestamp'] = (NOW-timedelta(minutes=2)).isoformat()
    live.base._atomic_write(path, changed)
    read, _ = live.base.read_current_snapshot(now_fn=lambda: NOW)
    result = delivery.project(read, now=NOW, code_revision='a'*40)
    assert result['prices']['cr_next']['values'] is None
    assert result['prices']['si_front']['values'] is not None
    assert result['basis_carry']['admitted_metric_count'] > 0
    assert all('cr_next' not in row['values']['legs'] for row in result['basis_carry']['metrics'])
    assert result['basis_carry']['refusals']
    monkeypatch.setattr(delivery, 'MAX_BYTES', 10)
    with pytest.raises(ValueError, match='no silent truncation'):
        delivery.project(read, now=NOW, code_revision='a'*40)


def test_independent_quote_and_exact_basis_values_survive_delivery(tmp_path, monkeypatch):
    source_io(tmp_path, monkeypatch)
    saved, path = live.refresh_snapshot(now_fn=lambda: NOW, live_loader=lambda: shifted_market(NOW))
    row = saved['components']['synchronized_live_market_oi']['data']['instruments']['si_front']
    row.update(price_oi_usable=False, oi=None)
    live.base._atomic_write(path, saved)
    read, _ = live.base.read_current_snapshot(now_fn=lambda: NOW)
    result = delivery.project(read, now=NOW, code_revision='a'*40)
    assert result['prices']['si_front']['values'] is None
    assert result['prices']['si_front']['quote']['values']['bid'] == row['bid']
    assert result['prices']['si_front']['quote_usable'] is True
    original = next(f['values']['metrics'] for f in read['factual_release']['facts'] if f['factor']=='basis_carry')
    expected = {m['values']['metric_id']: m['values'] for m in original}
    assert set(expected) == {m['values']['metric_id'] for m in result['basis_carry']['metrics']}
    for metric in result['basis_carry']['metrics']:
        values = metric['values']; source = expected[values['metric_id']]
        for key in ('value', 'units', 'formula', 'legs', 'source_timestamps', 'source_refs'):
            assert values[key] == source[key]


@pytest.mark.parametrize('status', [401, 503, 302])
def test_market_bridge_fails_without_snapshot_or_dated_fallback(status):
    calls = []
    class Response:
        status_code = status
        def json(self): return {'error': 'synthetic'}
    def get(url, **kwargs):
        calls.append((url, kwargs)); return Response()
    client = bridge.RubFactualSnapshotHTTPBridge(api_token='test', http_get=get)
    with pytest.raises(bridge.RubSnapshotBridgeError): client.get_market_factual()
    assert len(calls) == 1 and calls[0][0].endswith('/market-factual')
    assert calls[0][1]['allow_redirects'] is False
    assert calls[0][1]['timeout'][1] > 13.3


@pytest.mark.parametrize('payload', [{}, {'schema_version': 'legacy_snapshot.v1'}])
def test_market_bridge_refuses_wrong_version_without_fallback(payload):
    calls = []
    class Response:
        status_code = 200
        def json(self): return payload
    def get(url, **kwargs):
        calls.append(url)
        return Response()
    with pytest.raises(bridge.RubSnapshotBridgeError, match='schema mismatch'):
        bridge.RubFactualSnapshotHTTPBridge(api_token='test', http_get=get).get_market_factual()
    assert len(calls) == 1 and calls[0].endswith('/market-factual')

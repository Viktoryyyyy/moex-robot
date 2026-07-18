from __future__ import annotations
import json
from datetime import date, datetime, timedelta, timezone
from email.message import Message
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request
import pytest
from moex_research.external_data import moex_cnyrub_algopack_history as source
NOW = datetime(2026, 7, 17, 9, 0, tzinfo=timezone.utc)
START = date(2024, 8, 1)
END = date(2024, 8, 2)
TOKEN = 'secret-token-value'
COLUMNS = ['tradedate', 'tradetime', 'secid', 'pr_open', 'pr_high', 'pr_low', 'pr_close', 'vol', 'val', 'trades', 'pr_vwap', 'pr_change', 'trades_b', 'trades_s', 'val_b', 'val_s', 'vol_b', 'vol_s', 'disb', 'pr_vwap_b', 'pr_vwap_s', 'sec_pr_open', 'sec_pr_high', 'sec_pr_low', 'sec_pr_close', 'SYSTIME']

def _identity() -> source.CnyrubSecurityIdentity:
    return source.CnyrubSecurityIdentity(source_id='moex_cnyrub_tom_daily', security_id='CNYRUB_TOM', board_id='CETS', engine='currency', market='selt', primary_board=True, active_board=True, history_from=date(2010, 9, 27), history_till=None, metadata_route=source.build_security_metadata_url(), retrieved_at_utc=NOW, raw_payload_sha256='a' * 64, source_revision_status='official_iss_current_revision', historical_model_use_status='source_validation_only')

def _systime(day: str, bucket_time: str, *, extra_seconds: int=10) -> str:
    bucket = datetime.strptime(f'{day} {bucket_time}', '%Y-%m-%d %H:%M:%S')
    available = bucket + timedelta(minutes=5, seconds=extra_seconds)
    return available.strftime('%Y-%m-%d %H:%M:%S')

def _row(day: str, bucket_time: str, *, open_: float=11.0, high: float=12.0, low: float=10.0, close: float=11.5, vol: int=10, vol_b: int=6, vol_s: int=4, val: float=110, val_b: float=66, val_s: float=44, trades: int=5, trades_b: int=3, trades_s: int=2, secid: str='CNYRUB_TOM', systime: str | None=None) -> list[object]:
    return [day, bucket_time, secid, open_, high, low, close, vol, val, trades, close, 0, trades_b, trades_s, val_b, val_s, vol_b, vol_s, 0, close, close, 1, 1, 1, 1, systime or _systime(day, bucket_time)]

def _payload(rows: list[list[object]], *, start: int, total: int, page_size: int=1000, columns: list[str] | None=None) -> bytes:
    return json.dumps({'data': {'columns': columns or COLUMNS, 'data': rows}, 'data.cursor': {'columns': ['INDEX', 'TOTAL', 'PAGESIZE'], 'data': [[start, total, page_size]]}}, separators=(',', ':')).encode('utf-8')

class Response:

    def __init__(self, payload: bytes=b'{"ok":true}') -> None:
        self.payload = payload

    def __enter__(self) -> 'Response':
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload

def _http_error(code: int, *, marker: str | None=None, retry_after: str | None=None) -> HTTPError:
    headers = Message()
    if marker is not None:
        headers['X-MOEX-Error-Code'] = marker
    if retry_after is not None:
        headers['Retry-After'] = retry_after
    return HTTPError(source.build_tradestats_url(START, END), code, 'sanitized', headers, None)

def test_exact_algopack_route_and_query() -> None:
    url = source.build_tradestats_url(START, END, start=17)
    parsed = urlsplit(url)
    assert parsed.scheme == 'https'
    assert parsed.hostname == 'apim.moex.com'
    assert parsed.path == source.ALGOPACK_TRADESTATS_PATH
    assert parse_qs(parsed.query) == {'from': ['2024-08-01'], 'till': ['2024-08-02'], 'start': ['17']}

@pytest.mark.parametrize('url', ['https://evil.example/iss/datashop/algopack/fx/tradestats/CNYRUB_TOM.json?from=2024-08-01&till=2024-08-02&start=0', 'https://apim.moex.com/iss/datashop/algopack/fx/tradestats/USDRUB_TOM.json?from=2024-08-01&till=2024-08-02&start=0', 'https://apim.moex.com/iss/datashop/algopack/fx/tradestats/CNYRUB_TOM.json?from=2024-08-01&till=2024-08-02&start=0&extra=1'], ids=['arbitrary-host', 'wrong-path', 'wrong-query'])
def test_non_allowlisted_route_rejected_before_opener(url: str) -> None:
    calls = 0

    def opener(*_args: object, **_kwargs: object) -> Response:
        nonlocal calls
        calls += 1
        return Response()
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.fetch_algopack_bytes(url, TOKEN, opener=opener)
    assert raised.value.blocker == 'provenance_not_sufficient'
    assert calls == 0

def test_cross_host_redirect_cannot_receive_authorization() -> None:
    request = Request(source.build_tradestats_url(START, END), headers={'Authorization': f'Bearer {TOKEN}'})
    redirected = source._RejectAllRedirects().redirect_request(request, None, 302, 'Found', Message(), 'https://evil.example/collect')
    assert redirected is None

def test_redirect_failure_does_not_expose_token() -> None:
    calls = 0

    def opener(_request: Request, timeout: int) -> Response:
        nonlocal calls
        calls += 1
        assert timeout == 30
        raise _http_error(302)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.fetch_algopack_bytes(source.build_tradestats_url(START, END), TOKEN, opener=opener)
    assert raised.value.blocker == 'provenance_not_sufficient'
    assert TOKEN not in str(raised.value)
    assert TOKEN not in repr(raised.value)
    assert calls == 1

def test_exact_allowlisted_route_sends_bearer_header() -> None:
    captured: dict[str, object] = {}

    def opener(request: Request, timeout: int) -> Response:
        captured['request'] = request
        captured['timeout'] = timeout
        return Response()
    url = source.build_tradestats_url(START, END)
    assert source.fetch_algopack_bytes(url, TOKEN, opener=opener) == b'{"ok":true}'
    request = captured['request']
    assert request.get_header('Authorization') == f'Bearer {TOKEN}'
    assert TOKEN not in request.full_url
    assert captured['timeout'] == 30

@pytest.mark.parametrize('code', [401, 403, 404, 429, 500])
def test_token_never_appears_in_http_exception_text(code: int) -> None:

    def opener(_request: Request, timeout: int) -> Response:
        assert timeout == 30
        raise _http_error(code)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.fetch_algopack_bytes(source.build_tradestats_url(START, END), TOKEN, opener=opener)
    assert TOKEN not in str(raised.value)
    assert TOKEN not in repr(raised.value)

@pytest.mark.parametrize('value', [None, '', '   '])
def test_missing_or_empty_environment_token_is_exact_blocker(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    if value is None:
        monkeypatch.delenv(source.ALGOPACK_TOKEN_ENV, raising=False)
    else:
        monkeypatch.setenv(source.ALGOPACK_TOKEN_ENV, value)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.load_algopack_token()
    assert raised.value.blocker == 'token_env_not_configured'

@pytest.mark.parametrize('code,marker,expected', [(401, None, 'algopack_authentication_failed'), (403, None, 'algopack_subscription_not_entitled'), (404, 'route_not_found', 'official_route_not_reproducible'), (404, 'ticker_not_found', 'cnyrub_tom_not_available')])
def test_exact_non_retryable_http_classification(code: int, marker: str | None, expected: str) -> None:

    def opener(_request: Request, timeout: int) -> Response:
        assert timeout == 30
        raise _http_error(code, marker=marker)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.fetch_algopack_bytes(source.build_tradestats_url(START, END), TOKEN, opener=opener)
    assert raised.value.blocker == expected
    assert not raised.value.retryable

@pytest.mark.parametrize('blocker', ['algopack_authentication_failed', 'algopack_subscription_not_entitled', 'official_route_not_reproducible', 'cnyrub_tom_not_available'])
def test_401_403_404_classes_are_not_retried(blocker: str) -> None:
    calls = 0

    def transport(_url: str, _token: str) -> bytes:
        nonlocal calls
        calls += 1
        raise source.CnyrubAlgoPackError('sanitized', blocker=blocker)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.fetch_algopack_bytes_with_retry(source.build_tradestats_url(START, END), TOKEN, transport=transport, sleeper=lambda _delay: pytest.fail('non-retryable HTTP outcome retried'))
    assert raised.value.blocker == blocker
    assert calls == 1

def test_429_retries_with_valid_bounded_retry_after_then_succeeds() -> None:
    calls: list[str] = []
    delays: list[float] = []

    def transport(url: str, _token: str) -> bytes:
        calls.append(url)
        if len(calls) < 3:
            raise source.CnyrubAlgoPackError('rate limited', blocker='algopack_rate_limit_blocked', retryable=True, retry_after_seconds=7.0)
        return b'ok'
    route = source.build_tradestats_url(START, END)
    assert source.fetch_algopack_bytes_with_retry(route, TOKEN, transport=transport, sleeper=delays.append) == b'ok'
    assert calls == [route, route, route]
    assert delays == [7.0, 7.0]

def test_invalid_retry_after_falls_back_to_bounded_schedule() -> None:
    calls = 0
    delays: list[float] = []

    def transport(_url: str, _token: str) -> bytes:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise source.CnyrubAlgoPackError('rate limited', blocker='algopack_rate_limit_blocked', retryable=True, retry_after_seconds=None)
        return b'ok'
    source.fetch_algopack_bytes_with_retry(source.build_tradestats_url(START, END), TOKEN, transport=transport, sleeper=delays.append)
    assert delays == [0.5]

@pytest.mark.parametrize('factory', [lambda: _http_error(500), lambda: URLError('timeout'), TimeoutError])
def test_5xx_and_transport_timeout_are_bounded_retries(factory: object) -> None:
    calls = 0
    delays: list[float] = []

    def transport(_url: str, _token: str) -> bytes:
        nonlocal calls
        calls += 1
        if calls < 3:
            exc = factory() if callable(factory) else factory
            if isinstance(exc, HTTPError):
                raise source.CnyrubAlgoPackError('unavailable', blocker='algopack_tradestats_not_available', retryable=True)
            raise source.CnyrubAlgoPackError('timeout', blocker='algopack_tradestats_not_available', retryable=True)
        return b'ok'
    assert source.fetch_algopack_bytes_with_retry(source.build_tradestats_url(START, END), TOKEN, transport=transport, sleeper=delays.append) == b'ok'
    assert calls == 3
    assert delays == [0.5, 1.0]

def test_rate_limit_exhaustion_has_exact_blocker() -> None:

    def transport(_url: str, _token: str) -> bytes:
        raise source.CnyrubAlgoPackError('rate limited', blocker='algopack_rate_limit_blocked', retryable=True, retry_after_seconds=0)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.fetch_algopack_bytes_with_retry(source.build_tradestats_url(START, END), TOKEN, transport=transport, sleeper=lambda _delay: None)
    assert raised.value.blocker == 'algopack_rate_limit_blocked'

def test_malformed_json_is_schema_failure_without_retry() -> None:
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.parse_tradestats_page_response(b'not-json', from_date=START, till_date=END, start=0, route=source.build_tradestats_url(START, END), retrieved_at_utc=NOW)
    assert raised.value.blocker == 'algopack_schema_not_stable'

def test_pagination_and_daily_directional_aggregation_uses_max_systime() -> None:
    rows = [_row('2024-08-01', '10:00:00', vol=10, vol_b=6, vol_s=4), _row('2024-08-01', '10:05:00', open_=11.5, high=12.5, low=11.0, close=12.0, vol=20, vol_b=8, vol_s=12, val=230, val_b=92, val_s=138, trades=9, trades_b=4, trades_s=5, systime='2024-08-01 10:10:30'), _row('2024-08-02', '10:00:00', vol=4, vol_b=1, vol_s=3, val=48, val_b=12, val_s=36, trades=2, trades_b=1, trades_s=1)]
    calls: list[int] = []

    def transport(url: str, token: str) -> bytes:
        assert token == TOKEN
        start = int(parse_qs(urlsplit(url).query)['start'][0])
        calls.append(start)
        if start == 0:
            return _payload(rows[:2], start=0, total=3, page_size=2)
        if start == 2:
            return _payload(rows[2:], start=2, total=3, page_size=2)
        raise AssertionError(start)
    daily = source.load_daily_history(_identity(), from_date=START, till_date=END, bearer_token=TOKEN, transport=transport, sleeper=lambda _delay: None, clock=lambda: NOW)
    assert calls == [0, 2]
    assert [item.trade_date for item in daily] == [START, END]
    first = daily[0]
    assert first.volume == 30
    assert first.volume_buy == 14
    assert first.volume_sell == 16
    assert first.source_available_at.isoformat() == '2024-08-01T10:10:30+03:00'
    assert first.candle_end.isoformat() == '2024-08-01T10:10:00+03:00'

@pytest.mark.parametrize('columns,row_systime,expected_reason', [([item for item in COLUMNS if item != 'SYSTIME'], None, 'missing'), (COLUMNS, 'bad-time', 'malformed'), (COLUMNS, '2024-08-01 10:04:59', 'precedes')])
def test_systime_failures_are_point_in_time_blockers(columns: list[str], row_systime: str | None, expected_reason: str) -> None:
    row = _row('2024-08-01', '10:00:00', systime=row_systime)
    if 'SYSTIME' not in columns:
        row = [value for index, value in enumerate(row) if COLUMNS[index] != 'SYSTIME']
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.parse_tradestats_page_response(_payload([row], start=0, total=1, columns=columns), from_date=START, till_date=END, start=0, route=source.build_tradestats_url(START, END), retrieved_at_utc=NOW)
    assert raised.value.blocker == 'point_in_time_cutoff_not_provable'
    assert expected_reason in str(raised.value).lower()

@pytest.mark.parametrize('bucket_time', ['10:01:00', '10:05:01'])
def test_five_minute_grid_is_exact(bucket_time: str) -> None:
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.parse_tradestats_page_response(_payload([_row('2024-08-01', bucket_time)], start=0, total=1), from_date=START, till_date=END, start=0, route=source.build_tradestats_url(START, END), retrieved_at_utc=NOW)
    assert raised.value.blocker == 'numerical_or_chronology_integrity_failure'

@pytest.mark.parametrize('start,total,row_count', [(1, 2, 1), (3, 2, 0), (1, 1, 1)])
def test_cursor_index_start_and_remaining_total_are_exact(start: int, total: int, row_count: int) -> None:
    requested_start = 0 if start != 3 else 3
    rows = [_row('2024-08-01', '10:00:00')] * row_count
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.parse_tradestats_page_response(_payload(rows, start=start, total=total), from_date=START, till_date=END, start=requested_start, route=source.build_tradestats_url(START, END, start=requested_start), retrieved_at_utc=NOW)
    assert raised.value.blocker == 'algopack_schema_not_stable'

def test_cursor_total_must_remain_constant() -> None:
    calls = 0

    def transport(url: str, _token: str) -> bytes:
        nonlocal calls
        calls += 1
        start = int(parse_qs(urlsplit(url).query)['start'][0])
        if calls == 1:
            return _payload([_row('2024-08-01', '10:00:00')], start=start, total=2, page_size=1)
        return _payload([_row('2024-08-01', '10:05:00')], start=start, total=3, page_size=1)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.load_daily_history(_identity(), from_date=START, till_date=END, bearer_token=TOKEN, transport=transport, sleeper=lambda _delay: None, clock=lambda: NOW)
    assert raised.value.blocker == 'algopack_schema_not_stable'

def test_premature_empty_page_is_schema_blocker() -> None:

    def transport(url: str, _token: str) -> bytes:
        start = int(parse_qs(urlsplit(url).query)['start'][0])
        if start == 0:
            return _payload([_row('2024-08-01', '10:00:00')], start=0, total=2, page_size=1)
        return _payload([], start=1, total=2, page_size=1)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.load_daily_history(_identity(), from_date=START, till_date=END, bearer_token=TOKEN, transport=transport, sleeper=lambda _delay: None, clock=lambda: NOW)
    assert raised.value.blocker == 'algopack_schema_not_stable'
    assert 'premature empty' in str(raised.value)

def test_duplicate_provider_identity_across_pages_is_forbidden() -> None:
    row = _row('2024-08-01', '10:00:00')

    def transport(url: str, _token: str) -> bytes:
        start = int(parse_qs(urlsplit(url).query)['start'][0])
        return _payload([row], start=start, total=2, page_size=1)
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.load_daily_history(_identity(), from_date=START, till_date=END, bearer_token=TOKEN, transport=transport, sleeper=lambda _delay: None, clock=lambda: NOW)
    assert raised.value.blocker == 'algopack_schema_not_stable'

def test_prior_session_requires_provider_availability_before_anchor() -> None:
    parsed, _, _, digest = source.parse_tradestats_page_response(_payload([_row('2024-08-01', '10:00:00')], start=0, total=1), from_date=START, till_date=END, start=0, route=source.build_tradestats_url(START, END), retrieved_at_utc=NOW)
    candle = source.aggregate_daily_tradestats(parsed, source_route=source.build_tradestats_url(START, END), retrieved_at_utc=NOW, raw_payload_sha256=digest)[0]
    source.validate_prior_session_candle(candle, target_trade_date=date(2024, 8, 2), prior_trade_date=date(2024, 8, 1))
    late = source.CnyrubAlgoPackDailyCandle(**{**candle.as_record(), 'source_available_at': datetime(2024, 8, 2, 6, 0, tzinfo=source.MOSCOW)})
    with pytest.raises(source.CnyrubAlgoPackError) as raised:
        source.validate_prior_session_candle(late, target_trade_date=date(2024, 8, 2), prior_trade_date=date(2024, 8, 1))
    assert raised.value.blocker == 'point_in_time_cutoff_not_provable'

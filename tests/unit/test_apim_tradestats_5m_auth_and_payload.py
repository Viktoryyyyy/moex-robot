from __future__ import annotations

from collections.abc import Mapping
from datetime import date

import pytest

from moex_data.futures.apim_tradestats_5m import (
    MOEX_APIM_AUTH_ENV_VAR,
    MOEX_APIM_DEFAULT_BASE_URL,
    MoexApimFoTradestats5mAdapter,
    MoexApimTradestatsSourceError,
    RequestsMoexApimTradestatsHttpClient,
    build_moex_apim_fo_tradestats_5m_page_request,
)
from moex_data.futures.raw_ohlcv_5m import Raw5mMaterializationRequest
from moex_data.futures.validation import FuturesInstrumentIdentity


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        payload: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
        text: str = "",
        json_error: Exception | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = dict(headers if headers is not None else {"content-type": "application/json"})
        self.text = text
        self._json_error = json_error

    def json(self) -> Mapping[str, object]:
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class _FakeClient:
    def __init__(self, responses: tuple[_FakeResponse, ...]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, Mapping[str, object], float]] = []

    def get(self, url: str, *, params: Mapping[str, object], timeout: float) -> _FakeResponse:
        self.calls.append((url, dict(params), timeout))
        return self._responses.pop(0)


def _request() -> Raw5mMaterializationRequest:
    return Raw5mMaterializationRequest(
        dataset_id="futures_ohlcv_5m",
        contract_id="futures_ohlcv_5m.v1",
        timeframe="5m",
        identity=FuturesInstrumentIdentity(family="Si", secid="SiM6", board="RFUD", market="FORTS", series_type="native"),
        partition_key=date.fromisoformat("2026-06-02"),
        storage_ref="${MOEX_DATA_ROOT}/futures/raw/ohlcv_5m/timeframe={TIMEFRAME}/family={FAMILY}/secid={SECID}/series_type={SERIES_TYPE}/trade_date={YYYY-MM-DD}/part.parquet",
        calendar_contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
        manifest_ref="${MOEX_DATA_ROOT}/futures/manifests/raw_ohlcv_5m/run_id={RUN_ID}/family={FAMILY}/secid={SECID}/trade_date={YYYY-MM-DD}/manifest.json",
        quality_report_ref="${MOEX_DATA_ROOT}/futures/quality/raw_ohlcv_5m/run_id={RUN_ID}/family={FAMILY}/secid={SECID}/trade_date={YYYY-MM-DD}/quality.json",
        source_contract_ref="contracts/datasets/futures_source_contracts.v1.yaml",
    )


def _payload(table_key: str = "data") -> dict[str, object]:
    table = {
        "columns": [
            "tradedate",
            "tradetime",
            "secid",
            "asset_code",
            "pr_open",
            "pr_high",
            "pr_low",
            "pr_close",
            "vol",
            "val",
            "trades",
        ],
        "data": [["2026-06-02", "10:05", "SiM6", "Si", 80000, 80100, 79900, 80050, 100, 8005000, 12]],
    }
    return {table_key: table}


def test_default_apim_url_uses_apim_host_not_iss_host() -> None:
    page = build_moex_apim_fo_tradestats_5m_page_request(_request())

    assert MOEX_APIM_DEFAULT_BASE_URL == "https://apim.moex.com"
    assert page.url == "https://apim.moex.com/iss/datashop/algopack/fo/tradestats.json"
    assert page.params["secid"] == "SiM6"
    assert page.params["iss.only"] == "tradestats"


def test_requests_client_uses_bearer_auth_from_moex_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _Session:
        def get(self, url: str, *, params: Mapping[str, object], headers: Mapping[str, str], timeout: float) -> _FakeResponse:
            captured["url"] = url
            captured["headers"] = dict(headers)
            captured["params"] = dict(params)
            captured["timeout"] = timeout
            return _FakeResponse(payload=_payload())

    import requests

    monkeypatch.setenv(MOEX_APIM_AUTH_ENV_VAR, "secret-value")
    monkeypatch.setattr(requests, "Session", lambda: _Session())
    client = RequestsMoexApimTradestatsHttpClient()
    client.get("https://apim.moex.com/iss/datashop/algopack/fo/tradestats.json", params={"secid": "SiM6"}, timeout=30.0)

    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers["Authorization"] == "Bearer secret-value"
    assert headers["Accept"] == "application/json"


def test_requests_client_fails_closed_when_moex_api_key_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    import requests

    monkeypatch.delenv(MOEX_APIM_AUTH_ENV_VAR, raising=False)
    monkeypatch.setattr(requests, "Session", lambda: object())
    client = RequestsMoexApimTradestatsHttpClient()

    with pytest.raises(MoexApimTradestatsSourceError, match="MOEX_API_KEY"):
        client.get("https://apim.moex.com/iss/datashop/algopack/fo/tradestats.json", params={}, timeout=30.0)


def test_adapter_parses_live_apim_top_level_data_payload() -> None:
    client = _FakeClient((_FakeResponse(payload=_payload("data")),))
    rows = MoexApimFoTradestats5mAdapter(http_client=client).read_rows(_request())

    assert len(rows) == 1
    assert rows[0]["SECID"] == "SiM6"
    assert rows[0]["close"] == 80050.0
    assert rows[0]["trades"] == 12.0
    assert client.calls[0][0].startswith("https://apim.moex.com/")


def test_adapter_keeps_legacy_tradestats_table_compatibility() -> None:
    client = _FakeClient((_FakeResponse(payload=_payload("tradestats")),))
    rows = MoexApimFoTradestats5mAdapter(http_client=client).read_rows(_request())

    assert len(rows) == 1
    assert rows[0]["open"] == 80000.0


def test_adapter_filters_unrelated_secid_rows_before_normalization() -> None:
    payload = _payload()
    table = payload["data"]
    assert isinstance(table, dict)
    table["data"] = [
        ["2026-06-02", "10:03", "BRM6", "BR", 1, 2, 0.5, 1.5, 10, 15, 1],
        ["2026-06-02", "10:05", "SiM6", "Si", 80000, 80100, 79900, 80050, 100, 8005000, 12],
        ["2026-06-03", "10:07", "CNYM6", "CNY", 1, 2, 0.5, 1.5, 10, 15, 1],
    ]
    client = _FakeClient((_FakeResponse(payload=payload),))

    rows = MoexApimFoTradestats5mAdapter(http_client=client).read_rows(_request())

    assert len(rows) == 1
    assert rows[0]["SECID"] == "SiM6"
    assert rows[0]["close"] == 80050.0


def test_adapter_fails_closed_when_no_requested_secid_rows_remain() -> None:
    payload = _payload()
    table = payload["data"]
    assert isinstance(table, dict)
    table["data"] = [
        ["2026-06-02", "10:05", "BRM6", "BR", 1, 2, 0.5, 1.5, 10, 15, 1],
        ["2026-06-02", "10:10", "CNYM6", "CNY", 1, 2, 0.5, 1.5, 10, 15, 1],
    ]
    client = _FakeClient((_FakeResponse(payload=payload),))

    with pytest.raises(MoexApimTradestatsSourceError, match="returned no rows"):
        MoexApimFoTradestats5mAdapter(http_client=client).read_rows(_request())


def test_adapter_still_fails_closed_for_requested_secid_wrong_trade_date() -> None:
    payload = _payload()
    table = payload["data"]
    assert isinstance(table, dict)
    table["data"] = [["2026-06-03", "10:05", "SiM6", "Si", 80000, 80100, 79900, 80050, 100, 8005000, 12]]
    client = _FakeClient((_FakeResponse(payload=payload),))

    with pytest.raises(MoexApimTradestatsSourceError, match="tradedate"):
        MoexApimFoTradestats5mAdapter(http_client=client).read_rows(_request())


def test_adapter_still_fails_closed_for_requested_secid_invalid_5m_tradetime() -> None:
    payload = _payload()
    table = payload["data"]
    assert isinstance(table, dict)
    table["data"] = [["2026-06-02", "10:03", "SiM6", "Si", 80000, 80100, 79900, 80050, 100, 8005000, 12]]
    client = _FakeClient((_FakeResponse(payload=payload),))

    with pytest.raises(MoexApimTradestatsSourceError, match="5-minute"):
        MoexApimFoTradestats5mAdapter(http_client=client).read_rows(_request())


def test_adapter_still_fails_closed_for_malformed_schema() -> None:
    payload = {"data": {"columns": ["secid", "tradedate"], "data": [["SiM6", "2026-06-02"]]}}
    client = _FakeClient((_FakeResponse(payload=payload),))

    with pytest.raises(MoexApimTradestatsSourceError, match="missing column"):
        MoexApimFoTradestats5mAdapter(http_client=client).read_rows(_request())


def test_non_json_response_reports_status_content_type_and_safe_snippet_without_auth_secret() -> None:
    client = _FakeClient(
        (
            _FakeResponse(
                status_code=200,
                headers={"content-type": "text/html"},
                text="<html>not json</html>",
                json_error=ValueError("not json"),
            ),
        )
    )

    with pytest.raises(MoexApimTradestatsSourceError) as excinfo:
        MoexApimFoTradestats5mAdapter(http_client=client).read_rows(_request())

    message = str(excinfo.value)
    assert "not valid JSON" in message
    assert "content_type=text/html" in message
    assert "<html>not json</html>" in message
    assert "secret-value" not in message
    assert "Authorization" not in message


def test_http_error_reports_status_content_type_without_secret_leakage() -> None:
    client = _FakeClient(
        (
            _FakeResponse(
                status_code=401,
                headers={"content-type": "application/json"},
                text='{"message":"Unauthorized","http_status_code":401}',
            ),
        )
    )

    with pytest.raises(MoexApimTradestatsSourceError) as excinfo:
        MoexApimFoTradestats5mAdapter(http_client=client).read_rows(_request())

    message = str(excinfo.value)
    assert "HTTP 401" in message
    assert "content_type=application/json" in message
    assert "Unauthorized" in message
    assert "Authorization" not in message
    assert "Bearer" not in message

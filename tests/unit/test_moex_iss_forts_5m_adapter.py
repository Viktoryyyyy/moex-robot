from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import pytest

from moex_data.futures.iss_forts_5m import (
    MoexIssFortsCandles5mAdapter,
    MoexIssSourceError,
    build_moex_iss_forts_candles_5m_page_request,
)
from moex_data.futures.raw_ohlcv_5m import Raw5mMaterializationRequest
from moex_data.futures.validation import FuturesInstrumentIdentity


@dataclass(frozen=True)
class FakeResponse:
    status_code: int
    payload: Mapping[str, object]

    def json(self) -> Mapping[str, object]:
        return self.payload


class FakeHttpClient:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, Mapping[str, object]]] = []

    def get(self, url: str, *, params: Mapping[str, object], timeout: float) -> FakeResponse:
        self.calls.append((url, params))
        return self.response


def _request() -> Raw5mMaterializationRequest:
    return Raw5mMaterializationRequest(
        dataset_id="futures_ohlcv_5m",
        contract_id="futures_ohlcv_5m.v1",
        timeframe="5m",
        identity=FuturesInstrumentIdentity(
            family="Si",
            secid="SiM6",
            board="RFUD",
            market="FORTS",
            series_type="native",
        ),
        partition_key=date(2026, 6, 2),
        storage_ref="${MOEX_DATA_ROOT}/futures/ohlcv_5m/family={FAMILY}/secid={SECID}/board={BOARD}/market={MARKET}/series_type={SERIES_TYPE}/trade_date={TRADE_DATE}/part.parquet",
        calendar_contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
        manifest_ref="${MOEX_DATA_ROOT}/futures/manifests/run_date={YYYY-MM-DD}/run_id={RUN_ID}/raw_5m_manifest.json",
        quality_report_ref="${MOEX_DATA_ROOT}/futures/quality/run_date={YYYY-MM-DD}/run_id={RUN_ID}/raw_5m_quality_report.json",
        source_contract_ref="contracts/datasets/futures_source_contracts.v1.yaml",
    )


def _payload(*, rows: list[list[object]], columns: list[str] | None = None) -> Mapping[str, object]:
    return {
        "candles": {
            "columns": columns
            or ["begin", "end", "open", "high", "low", "close", "volume", "value", "trades"],
            "data": rows,
        }
    }


def test_adapter_url_and_parameter_construction():
    page_request = build_moex_iss_forts_candles_5m_page_request(
        _request(),
        base_url="https://iss.moex.com/",
        start=200,
    )

    assert page_request.url == "https://iss.moex.com/iss/engines/futures/markets/forts/boards/rfud/securities/SiM6/candles.json"
    assert page_request.params == {
        "interval": "5",
        "from": "2026-06-02",
        "till": "2026-06-02",
        "start": 200,
        "iss.meta": "off",
    }


def test_adapter_normalizes_iss_rows_to_canonical_raw_5m_schema():
    response = FakeResponse(
        200,
        _payload(rows=[["2026-06-02 10:00:00", "2026-06-02 10:05:00", 100, 101, 99, 100.5, 10, 1005, 2]]),
    )
    client = FakeHttpClient(response)
    adapter = MoexIssFortsCandles5mAdapter(http_client=client)

    rows = adapter.read_rows(_request())

    assert len(rows) == 1
    row = rows[0]
    assert row["FAMILY"] == "Si"
    assert row["SECID"] == "SiM6"
    assert row["BOARD"] == "RFUD"
    assert row["MARKET"] == "FORTS"
    assert row["SERIES_TYPE"] == "native"
    assert row["trade_date"].isoformat() == "2026-06-02"
    assert row["session_date"].isoformat() == "2026-06-02"
    assert row["open"] == 100.0
    assert row["high"] == 101.0
    assert row["low"] == 99.0
    assert row["close"] == 100.5
    assert row["volume"] == 10.0
    assert row["value"] == 1005.0
    assert row["trades"] == 2.0


def test_adapter_uses_zero_trades_when_iss_response_has_no_trades_column():
    response = FakeResponse(
        200,
        _payload(
            columns=["begin", "end", "open", "high", "low", "close", "volume", "value"],
            rows=[["2026-06-02 10:00:00", "2026-06-02 10:05:00", 100, 101, 99, 100.5, 10, 1005]],
        ),
    )
    adapter = MoexIssFortsCandles5mAdapter(http_client=FakeHttpClient(response))

    rows = adapter.read_rows(_request())

    assert rows[0]["trades"] == 0.0


def test_adapter_rejects_http_errors():
    adapter = MoexIssFortsCandles5mAdapter(http_client=FakeHttpClient(FakeResponse(500, _payload(rows=[]))))

    with pytest.raises(MoexIssSourceError, match="HTTP 500"):
        adapter.read_rows(_request())


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"candles": {"columns": ["begin"], "data": []}},
        {"candles": {"columns": ["begin", "open", "high", "low", "close", "volume", "value"], "data": [["2026-06-02 10:00:00"]]}},
    ],
)
def test_adapter_rejects_malformed_iss_response(payload):
    adapter = MoexIssFortsCandles5mAdapter(http_client=FakeHttpClient(FakeResponse(200, payload)))

    with pytest.raises(MoexIssSourceError):
        adapter.read_rows(_request())


def test_adapter_rejects_non_5m_candle_duration():
    response = FakeResponse(
        200,
        _payload(rows=[["2026-06-02 10:00:00", "2026-06-02 10:10:00", 100, 101, 99, 100.5, 10, 1005, 2]]),
    )
    adapter = MoexIssFortsCandles5mAdapter(http_client=FakeHttpClient(response))

    with pytest.raises(MoexIssSourceError, match="5 minutes"):
        adapter.read_rows(_request())


def test_adapter_rejects_dynamic_markers():
    bad_request = Raw5mMaterializationRequest(
        dataset_id="futures_ohlcv_5m",
        contract_id="futures_ohlcv_5m.v1",
        timeframe="5m",
        identity=FuturesInstrumentIdentity(
            family="Si",
            secid="latest",
            board="RFUD",
            market="FORTS",
            series_type="native",
        ),
        partition_key=date(2026, 6, 2),
        storage_ref="${MOEX_DATA_ROOT}/x",
        calendar_contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
        manifest_ref="${MOEX_DATA_ROOT}/x.json",
        quality_report_ref="${MOEX_DATA_ROOT}/q.json",
        source_contract_ref="contracts/datasets/futures_source_contracts.v1.yaml",
    )

    with pytest.raises(MoexIssSourceError):
        build_moex_iss_forts_candles_5m_page_request(bad_request)

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import pytest

from moex_data.futures.apim_tradestats_5m import (
    MoexApimFoTradestats5mAdapter,
    MoexApimTradestatsSourceError,
    build_moex_apim_fo_tradestats_5m_page_request,
)
from moex_data.futures.raw_ohlcv_5m import Raw5mMaterializationRequest
from moex_data.futures.validation import FuturesInstrumentIdentity, FuturesValidationError


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
        "tradestats": {
            "columns": columns
            or ["SECID", "TRADEDATE", "TRADETIME", "PR_OPEN", "PR_HIGH", "PR_LOW", "PR_CLOSE", "VOL", "VAL", "TRADES"],
            "data": rows,
        }
    }


def test_adapter_url_and_parameter_construction():
    page_request = build_moex_apim_fo_tradestats_5m_page_request(
        _request(),
        base_url="https://iss.moex.com/",
        start=200,
    )

    assert page_request.url == "https://iss.moex.com/iss/datashop/algopack/fo/tradestats.json"
    assert page_request.params == {
        "date": "2026-06-02",
        "from": "2026-06-02",
        "till": "2026-06-02",
        "secid": "SiM6",
        "start": 200,
        "iss.meta": "off",
        "iss.only": "tradestats",
    }


def test_adapter_normalizes_apim_rows_to_canonical_raw_5m_schema():
    response = FakeResponse(
        200,
        _payload(rows=[["SiM6", "2026-06-02", "10:05:00", 100, 101, 99, 100.5, 10, 1005, 2]]),
    )
    adapter = MoexApimFoTradestats5mAdapter(http_client=FakeHttpClient(response))

    rows = adapter.read_rows(_request())

    assert len(rows) == 1
    row = rows[0]
    assert row["ts"].isoformat() == "2026-06-02T10:05:00"
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


def test_adapter_rejects_empty_apim_response():
    adapter = MoexApimFoTradestats5mAdapter(http_client=FakeHttpClient(FakeResponse(200, _payload(rows=[]))))

    with pytest.raises(MoexApimTradestatsSourceError, match="returned no rows"):
        adapter.read_rows(_request())


def test_adapter_rejects_malformed_apim_response():
    adapter = MoexApimFoTradestats5mAdapter(http_client=FakeHttpClient(FakeResponse(200, {"tradestats": {"columns": ["SECID"], "data": []}})))

    with pytest.raises(MoexApimTradestatsSourceError):
        adapter.read_rows(_request())


def test_adapter_rejects_non_5m_tradetime():
    response = FakeResponse(
        200,
        _payload(rows=[["SiM6", "2026-06-02", "10:03:00", 100, 101, 99, 100.5, 10, 1005, 2]]),
    )
    adapter = MoexApimFoTradestats5mAdapter(http_client=FakeHttpClient(response))

    with pytest.raises(MoexApimTradestatsSourceError, match="5-minute aligned"):
        adapter.read_rows(_request())


def test_adapter_rejects_wrong_secid():
    response = FakeResponse(
        200,
        _payload(rows=[["SiU6", "2026-06-02", "10:05:00", 100, 101, 99, 100.5, 10, 1005, 2]]),
    )
    adapter = MoexApimFoTradestats5mAdapter(http_client=FakeHttpClient(response))

    with pytest.raises(MoexApimTradestatsSourceError, match="secid"):
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

    with pytest.raises(FuturesValidationError):
        build_moex_apim_fo_tradestats_5m_page_request(bad_request)

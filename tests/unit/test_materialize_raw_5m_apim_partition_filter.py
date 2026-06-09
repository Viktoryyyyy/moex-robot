from __future__ import annotations

from collections.abc import Mapping

import pytest

from moex_data.futures import materialize_raw_5m as materializer


class _FakeResponse:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self._payload = payload
        self.url = "https://apim.moex.com/iss/datashop/algopack/fo/tradestats.json?secid=SiM6"

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Mapping[str, object]:
        return self._payload


def _request() -> materializer.Raw5mMaterializationRequest:
    return materializer.build_materialization_request(
        repo_root=".",
        dataset_id=materializer.TARGET_DATASET_ID,
        contract_id=materializer.TARGET_CONTRACT_ID,
        trade_date=materializer.TARGET_TRADE_DATE,
        family=materializer.TARGET_FAMILY,
        secid=materializer.TARGET_SECID,
        source_path=None,
        run_id="regression",
        source_candidate=materializer.SOURCE_CANDIDATE_APIM_TRADESTATS,
        source_endpoint=materializer.SOURCE_ENDPOINT_APIM_FO_TRADESTATS,
        market=materializer.TARGET_MARKET,
        board=materializer.TARGET_BOARD,
        series_type=materializer.TARGET_SERIES_TYPE,
        granularity=materializer.TARGET_GRANULARITY,
    )


def _payload(rows: list[list[object]]) -> dict[str, object]:
    return {
        "data": {
            "columns": [
                "tradedate",
                "tradetime",
                "secid",
                "pr_open",
                "pr_high",
                "pr_low",
                "pr_close",
                "vol",
                "val",
                "trades",
            ],
            "data": rows,
        }
    }


def _install_response(monkeypatch: pytest.MonkeyPatch, rows: list[list[object]]) -> None:
    def fake_get(url: str, *, params: Mapping[str, object], headers: Mapping[str, str], timeout: float) -> _FakeResponse:
        assert params["date"] == "2026-06-02"
        assert params["from"] == "2026-06-02"
        assert params["till"] == "2026-06-02"
        assert params["secid"] == "SiM6"
        assert params["start"] == 0
        assert params["iss.meta"] == "off"
        assert params["iss.only"] == "tradestats"
        assert timeout == 30.0
        return _FakeResponse(_payload(rows))

    monkeypatch.setattr(materializer.requests, "get", fake_get)


def test_apim_materializer_filters_requested_secid_and_trade_date_before_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_response(
        monkeypatch,
        [
            ["2026-06-02", "10:05", "SiM6", 100.0, 101.0, 99.0, 100.5, 10, 1000.0, 1],
            ["2026-06-03", "10:10", "SiM6", 200.0, 201.0, 199.0, 200.5, 20, 2000.0, 2],
            ["2026-06-02", "10:15", "RIM6", 300.0, 301.0, 299.0, 300.5, 30, 3000.0, 3],
        ],
    )

    request = _request()
    source_table, _source_info = materializer._source_table_for_request(
        request,
        timeout=30.0,
        apim_base_url=None,
        env={"MOEX_API_KEY": "token"},
    )
    output, metrics = materializer._validate_source_table(
        source_table,
        request.trade_date,
        request.family,
        request.secid,
    )

    assert metrics["rows"] == 1
    assert output["trade_date"].tolist() == ["2026-06-02"]
    assert output["secid"].tolist() == ["SiM6"]
    assert str(output["ts"].iloc[0]) == "2026-06-02 10:05:00"


def test_apim_materializer_fails_closed_when_requested_secid_date_has_no_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_response(
        monkeypatch,
        [
            ["2026-06-03", "10:05", "SiM6", 200.0, 201.0, 199.0, 200.5, 20, 2000.0, 2],
            ["2026-06-02", "10:10", "RIM6", 300.0, 301.0, 299.0, 300.5, 30, 3000.0, 3],
        ],
    )

    with pytest.raises(materializer.FuturesRaw5mMaterializationError, match="requested secid/date"):
        materializer._source_table_for_request(
            _request(),
            timeout=30.0,
            apim_base_url=None,
            env={"MOEX_API_KEY": "token"},
        )

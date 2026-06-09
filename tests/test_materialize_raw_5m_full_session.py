import json

import pandas as pd
import pytest

from moex_data.futures import materialize_raw_5m as materializer
from moex_data.futures import materialize_raw_5m_full_session as full_session
from test_materialize_raw_5m_explicit_trade_dates import _call_kwargs, _write_contract_package


def _response_rows(trade_date: str, start: int) -> list[list[object]]:
    rows = [
        [trade_date, "10:00:00", "SiM6", 100, 110, 99, 105, 10, 1000, 2],
        [trade_date, "10:05:00", "SiM6", 105, 111, 104, 108, 12, 1200, 3],
        [trade_date, "10:10:00", "SiM6", 108, 112, 107, 111, 15, 1500, 4],
        [trade_date, "10:15:00", "SiM6", 111, 113, 110, 112, 14, 1400, 3],
        [trade_date, "10:20:00", "SiM6", 112, 114, 111, 113, 13, 1300, 2],
    ]
    if start == 0:
        return rows[:2]
    if start == 2:
        return rows[2:]
    return []


class Response:
    def __init__(self, trade_date: str, start: int, rows: list[list[object]]) -> None:
        self.url = "https://apim.moex.test/iss/datashop/algopack/fo/tradestats.json?start=" + str(start)
        self.trade_date = trade_date
        self.rows = rows

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return {
            "data": {
                "columns": ["tradedate", "tradetime", "secid", "pr_open", "pr_high", "pr_low", "pr_close", "vol", "val", "trades"],
                "data": self.rows,
            }
        }


def test_full_session_materializer_pages_until_empty(tmp_path, monkeypatch):
    trade_date = "2026-06-04"
    repo_root = tmp_path / "repo"
    data_root = tmp_path / "data"
    repo_root.mkdir()
    _write_contract_package(repo_root)
    seen_starts = []

    def fake_get(url, params, headers, timeout):
        start = int(params["start"])
        seen_starts.append(start)
        return Response(trade_date, start, _response_rows(trade_date, start))

    monkeypatch.setattr(full_session.requests, "get", fake_get)
    result = full_session.materialize_single_raw_5m_full_session_partition(**_call_kwargs(repo_root, data_root, trade_date))

    assert result.status == materializer.SUCCEEDED_STATUS
    assert result.rows == 5
    assert seen_starts == [0, 2, 5]
    partition = pd.read_parquet(result.partition_path)
    assert len(partition.index) == 5
    assert set(partition["trade_date"]) == {trade_date}
    assert set(partition["secid"]) == {"SiM6"}
    quality_report = json.loads(result.quality_report_path.read_text(encoding="utf-8"))
    assert quality_report["rows"][0]["rows"] == 5
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["source_contract"]["source_fetch_mode"] == "declared_apim_tradestats"
    assert manifest["source_contract"]["trade_date"] == trade_date


def test_full_session_materializer_fails_if_pagination_does_not_advance(monkeypatch):
    trade_date = "2026-06-04"
    request = materializer.Raw5mMaterializationRequest(
        repo_root=".",
        dataset_id=materializer.TARGET_DATASET_ID,
        contract_id=materializer.TARGET_CONTRACT_ID,
        trade_date=trade_date,
        family=materializer.TARGET_FAMILY,
        secid=materializer.TARGET_SECID,
        source_path=None,
        run_id="run_apim_full_session",
        source_candidate=materializer.SOURCE_CANDIDATE_APIM_TRADESTATS,
        source_endpoint=materializer.SOURCE_ENDPOINT_APIM_FO_TRADESTATS,
        market=materializer.TARGET_MARKET,
        board=materializer.TARGET_BOARD,
        series_type=materializer.TARGET_SERIES_TYPE,
        granularity=materializer.TARGET_GRANULARITY,
    )

    def fake_get(url, params, headers, timeout):
        return Response(trade_date, int(params["start"]), _response_rows(trade_date, 0))

    monkeypatch.setattr(full_session.requests, "get", fake_get)
    with pytest.raises(materializer.FuturesRaw5mMaterializationError, match="APIM pagination did not advance"):
        full_session._fetch_apim_tradestats_full_session_frame(
            request=request,
            timeout=3.0,
            apim_base_url="https://apim.moex.test",
            env={"MOEX_API_URL": "https://apim.moex.test", "MOEX_UA": "pytest"},
        )

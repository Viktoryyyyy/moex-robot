"""Synthetic regressions for the 2026-09-18 TradeStats scope incident.

Fixtures model observed response shapes, not retained production payloads.
No market-data or server writes, live API requests, or historical backfill.
"""

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
import sys

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from moex_data.futures import algopack_availability_probe as availability
from moex_data.futures import liquidity_history_metrics_probe as liquidity
from moex_data.futures import refresh_forts_raw_5m_incremental as observed

FROM = "2025-09-18"
TILL = "2026-09-18"
ENDPOINT = "/iss/datashop/algopack/fo/tradestats"
COLUMNS = ["tradedate", "tradetime", "secid", "vol", "val", "trades"]


@pytest.fixture(autouse=True)
def forbid_live_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("live network forbidden in source-scope regression")
    monkeypatch.setattr(requests.sessions.Session, "request", denied)


def rows(count, secid="USDRUBF", start="2025-09-18T10:00:00"):
    first = datetime.fromisoformat(start)
    return [
        [(first + timedelta(minutes=5 * i)).date().isoformat(),
         (first + timedelta(minutes=5 * i)).time().isoformat(),
         secid, 1, 10, 1]
        for i in range(count)
    ]


def payload(values, block="tradestats"):
    return {block: {"columns": list(COLUMNS), "data": deepcopy(values)}}


def invalid_payload(case):
    if case in ("mixed_7_993", "foreign_0_1000"):
        matching = 7 if case == "mixed_7_993" else 0
        return payload(rows(matching) + rows(1000 - matching, "FOREIGN"))
    value = payload(rows(2))
    table = value["tradestats"]
    if case in ("missing_secid", "missing_date"):
        column = "secid" if case == "missing_secid" else "tradedate"
        index = table["columns"].index(column)
        table["columns"].pop(index)
        for row in table["data"]:
            row.pop(index)
    elif case == "null_secid":
        table["data"][0][2] = None
    elif case == "bad_date":
        table["data"][0][0] = "not-a-date"
    elif case == "out_of_range":
        table["data"][0][0] = "2024-01-01"
    elif case == "metadata_only":
        return {"tradestats.cursor": {"columns": ["INDEX", "TOTAL", "PAGESIZE"],
                                     "data": [[0, 1000, 1000]]}}
    elif case == "error_block":
        return {"tradestats": {"columns": ["ERROR_MESSAGE"], "data": [["denied"]]}}
    else:
        raise AssertionError(case)
    return value


INVALID = [
    "mixed_7_993", "foreign_0_1000", "missing_secid", "missing_date",
    "null_secid", "bad_date", "out_of_range", "metadata_only", "error_block",
]


def probe(monkeypatch, answer):
    calls = []
    def request(base_url, path, params, timeout, use_apim):
        calls.append((base_url, path, dict(params)))
        return deepcopy(answer)
    monkeypatch.setattr(availability, "request_json", request)
    result = availability.probe_endpoint_for_instrument(
        "algopack_fo_tradestats", ENDPOINT + ".json", "USDRUBF", "USDRUBF",
        FROM, TILL, 1.0, "https://apim.invalid", "https://iss.invalid",
    )
    return result, calls


@pytest.mark.parametrize("case", INVALID)
def test_availability_does_not_admit_foreign_or_invalid_data(monkeypatch, case):
    result, _ = probe(monkeypatch, invalid_payload(case))
    assert result["availability_status"] != "available"
    assert result["observed_rows"] == 0


def test_availability_accepts_valid_sparse_first_page_without_history_claim(monkeypatch):
    result, calls = probe(monkeypatch, payload(rows(3, start="2026-07-01T10:00:00")))
    assert result["availability_status"] == "available"
    assert result["observed_rows"] == 3
    assert result["observed_min_ts"] == "2026-07-01"
    assert calls and all(c[1].endswith("/USDRUBF.json") for c in calls)


def test_availability_empty_data_is_not_available(monkeypatch):
    result, _ = probe(monkeypatch, payload([]))
    assert result["availability_status"] != "available"
    assert result["observed_rows"] == 0


def test_only_tradestats_candidate_policy_changes():
    candidates = availability.endpoint_probe_candidates(
        "algopack_fo_tradestats", "USDRUBF", "USDRUBF", ENDPOINT + ".json")
    assert candidates and all(item[0].endswith("/USDRUBF.json") for item in candidates)
    for name in ("obstats", "hi2"):
        base = "/iss/datashop/algopack/fo/" + name
        assert availability.endpoint_probe_candidates(
            "algopack_fo_" + name, "USDRUBF", "USDRUBF", base + ".json"
        ) == [(base + "/USDRUBF.json", {}, True),
              (base + ".json", {"secid": "USDRUBF"}, True)]
    assert availability.endpoint_probe_candidates(
        "moex_futoi", "SiU6", "Si", "unused"
    ) == [("/iss/analyticalproducts/futoi/securities/si.json", {"latest": 1}, True)]


def history(monkeypatch, page_for_start, secid="USDRUBF"):
    calls = []
    def request(base_url, path, params, timeout, use_apim):
        calls.append((path, dict(params)))
        if len(calls) > 12:
            raise RuntimeError("synthetic request budget exhausted")
        assert path.endswith("/" + secid + ".json"), "general route is not history"
        assert params["from"] == FROM and params["till"] == TILL
        return deepcopy(page_for_start(int(params.get("start", 0))))
    monkeypatch.setattr(liquidity, "request_json", request)
    result = liquidity.fetch_tradestats(
        secid, FROM, TILL, 1.0, "https://apim.invalid", "https://iss.invalid")
    return result, calls


@pytest.mark.parametrize("case", INVALID)
def test_history_rejects_invalid_response_before_aggregation(monkeypatch, case):
    (frame, _, status, error), _ = history(
        monkeypatch, lambda offset: invalid_payload(case) if offset == 0 else payload([]))
    assert status == "failed" and frame.empty and error


@pytest.mark.parametrize("block", ["tradestats", "data"])
def test_history_paginates_beyond_1000_without_cursor(monkeypatch, block):
    first = rows(1000)
    second = rows(197, start="2026-09-17T00:00:00")
    pages = {0: payload(first, block), 1000: payload(second, block), 1197: payload([], block)}
    (frame, url, status, error), calls = history(monkeypatch, pages.__getitem__)
    assert status == "completed" and not error
    assert len(frame) == 1197
    assert set(frame["secid"]) == {"USDRUBF"}
    assert set(frame["tradedate"]) >= {FROM, "2026-09-17"}
    assert [c[1]["start"] for c in calls] == [0, 1000, 1197]
    assert url.endswith("/USDRUBF.json")
    daily, _ = liquidity.aggregate_daily_tradestats(frame)
    assert daily["volume"].sum() == 1197


def test_history_preserves_sparse_instrument_without_inventing_dates(monkeypatch):
    values = rows(197, "92U6", start="2026-07-01T00:00:00")
    values[-1][0] = "2026-09-17"
    (frame, _, status, error), _ = history(
        monkeypatch, lambda offset: payload(values) if offset == 0 else payload([]), "92U6")
    assert status == "completed" and not error and len(frame) == 197
    assert frame["tradedate"].min() == "2026-07-01"
    assert frame["tradedate"].max() == "2026-09-17"


def test_history_repeated_page_is_not_partial_success(monkeypatch):
    (frame, _, status, error), calls = history(monkeypatch, lambda offset: payload(rows(2)))
    assert status == "failed" and frame.empty and error
    assert len(calls) <= 8, "reader must detect repetition, not exhaust the fake budget"


def test_history_later_page_timeout_discards_partial_history(monkeypatch):
    def answer(offset):
        if offset:
            raise requests.ReadTimeout("synthetic later-page timeout")
        return payload(rows(1000))
    (frame, _, status, error), _ = history(monkeypatch, answer)
    assert status == "failed" and frame.empty and error


class Response:
    status_code = 200
    def __init__(self, value):
        self.value = value
    def raise_for_status(self):
        return None
    def json(self):
        return deepcopy(self.value)


def dates(monkeypatch, page_for_start, secid="USDRUBF"):
    calls = []
    def get(url, *, params, headers, timeout, **kwargs):
        calls.append((url, dict(params)))
        if len(calls) > 12:
            raise RuntimeError("synthetic request budget exhausted")
        assert url.endswith("/" + secid + ".json"), "observed dates need instrument history"
        assert params["from"] == FROM and params["till"] == TILL
        return Response(page_for_start(int(params.get("start", 0))))
    monkeypatch.setenv("MOEX_API_KEY", "synthetic-test-token")
    monkeypatch.setattr(observed.requests, "get", get)
    result = observed.fetch_observed_tradestats_dates(
        FROM, TILL, secid=secid, timeout=1.0, apim_base_url="https://apim.invalid")
    return result, calls


def test_observed_dates_use_all_instrument_pages_without_global_endpoint_change(monkeypatch):
    pages = {0: payload(rows(1000)),
             1000: payload(rows(2, start="2026-09-17T10:00:00")),
             1002: payload([])}
    result, calls = dates(monkeypatch, pages.__getitem__)
    expected = sorted({r[0] for p in pages.values() for r in p["tradestats"]["data"]})
    assert result == expected and result[0] == FROM and result[-1] == "2026-09-17"
    assert [c[1]["start"] for c in calls] == [0, 1000, 1002]
    assert observed.materializer.core.SOURCE_ENDPOINT_APIM_FO_TRADESTATS == ENDPOINT + ".json"


@pytest.mark.parametrize("case", INVALID)
def test_observed_dates_reject_mixed_invalid_or_missing_identity(monkeypatch, case):
    with pytest.raises(ValueError):
        dates(monkeypatch, lambda offset: invalid_payload(case) if offset == 0 else payload([]))


def test_observed_dates_empty_is_explicit_failure(monkeypatch):
    with pytest.raises(ValueError):
        dates(monkeypatch, lambda offset: payload([]))


def test_observed_dates_later_page_failure_cannot_admit_earlier_dates(monkeypatch):
    def answer(offset):
        if offset:
            raise requests.ReadTimeout("synthetic date-page timeout")
        return payload(rows(1000))
    with pytest.raises(ValueError):
        dates(monkeypatch, answer)

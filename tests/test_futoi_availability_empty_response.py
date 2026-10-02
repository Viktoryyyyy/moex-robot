"""A valid empty data table is different from an unproven source response."""
from copy import deepcopy

import pandas as pd
import pytest
import requests

from moex_data.futures import algopack_availability_probe as probe
from moex_data.futures import all_universe_futoi_raw_backfill_slice as runner


COLUMNS = [
    "sess_id", "seqnum", "tradedate", "tradetime", "ticker", "clgroup",
    "pos", "pos_long", "pos_short", "pos_long_num", "pos_short_num",
    "systime", "trade_session_date",
]
BASE = "https://apim.moex.com"
DAY = "2026-10-01"


def payload(rows=None):
    return {
        "futoi.dates": {"columns": ["from", "till"], "data": [[None, None]]},
        "futoi": {"columns": COLUMNS.copy(), "data": [] if rows is None else rows},
    }


def availability_row(monkeypatch, response):
    monkeypatch.setattr(probe, "request_json", lambda *a, **kw: deepcopy(response))
    row = pd.Series({"secid": "WTZ6", "family_code": "WT", "board": "RFUD"})
    return probe.availability_record_from_probe(
        "moex_futoi", "", row, DAY, "2026-09-17", DAY, 30, BASE, "unused",
    )["row"]


def eligibility():
    return pd.DataFrame([{
        "secid": "WTZ6", "family_code": "WT", "board": "RFUD",
        "classification_status": "included", "registry_snapshot_date": DAY,
        "eligibility_snapshot_date": DAY, "registry_snapshot_id": "registry",
        "eligibility_snapshot_id": "eligibility",
    }])


@pytest.mark.parametrize("timestamp_columns", [["tradedate", "tradetime"], ["moment"]])
def test_valid_empty_response_is_deferred_without_retry(monkeypatch, timestamp_columns):
    response = payload()
    response["futoi"]["columns"] = [
        c for c in COLUMNS if c not in ("tradedate", "tradetime")
    ] + timestamp_columns
    row = availability_row(monkeypatch, response)
    assert row["availability_status"] == "unavailable"
    assert row["probe_status"] == "completed"
    assert row["observed_rows"] == 0
    assert row["error_code"] is None and row["error_message"] is None
    actual = runner.derive_futoi_eligibility(eligibility(), pd.DataFrame([row]), DAY)
    assert actual.futoi_check_status.tolist() == ["deferred_futoi_unavailable"]
    assert actual.futoi_retry_required.tolist() == [False]
    manifest, _ = runner.record_availability_outcomes(
        {"status": "deferred", "secid_list": [], "failed_secid": [], "output_partitions": []},
        pd.DataFrame(), actual, "run", "chunk",
    )
    assert manifest["futoi_coverage_complete"] is False
    assert manifest["deferred_secid"] == ["WTZ6"]
    assert manifest["availability_retry_secid"] == []


@pytest.mark.parametrize("response", [
    {},
    {"futoi": None},
    {"data": payload()["futoi"]},
    {"futoi.dates": payload()["futoi.dates"]},
    {"futoi": {"columns": COLUMNS}},
    {"futoi": {"columns": COLUMNS, "data": None}},
    {"futoi": {"columns": COLUMNS, "data": {}}},
    {"futoi": {"columns": COLUMNS, "data": [[]]}},
    {"futoi": {"columns": COLUMNS, "data": [dict.fromkeys(COLUMNS)]}},
    {"futoi": {"columns": COLUMNS + ["POS"], "data": []}},
    {"futoi": {"columns": COLUMNS + [None], "data": []}},
    {"futoi": {"columns": COLUMNS + [" extra "], "data": []}},
    {"futoi": {"columns": [], "data": []}},
    {"futoi": {"columns": [c for c in COLUMNS if c != "pos"], "data": []}},
    {"futoi": {"columns": [c for c in COLUMNS if c != "tradetime"], "data": []}},
    {"futoi": {"columns": ["ERROR_MESSAGE"], "data": []}},
    {"futoi": {"columns": ["ERROR_MESSAGE"], "data": [["not available"]]}},
])
def test_unproven_empty_or_error_response_keeps_retry_required(monkeypatch, response):
    row = availability_row(monkeypatch, response)
    assert row["availability_status"] != "available"
    assert row["error_code"] and row["error_message"]
    actual = runner.derive_futoi_eligibility(eligibility(), pd.DataFrame([row]), DAY)
    assert actual.futoi_eligible.tolist() == [False]
    assert actual.futoi_retry_required.tolist() == [True]


def test_nonempty_response_still_admits_real_positions(monkeypatch):
    row = availability_row(monkeypatch, payload([
        [7660, 202, DAY, "23:50:00", "WT", "FIZ", 1, 2, -1, 2, 1,
         DAY + " 23:50:10", DAY],
    ]))
    assert row["availability_status"] == "available" and row["observed_rows"] == 1
    actual = runner.derive_futoi_eligibility(eligibility(), pd.DataFrame([row]), DAY)
    assert actual.futoi_eligible.tolist() == [True]


def test_transport_failure_still_requires_retry(monkeypatch):
    def fail(*args, **kwargs):
        raise requests.Timeout("source timeout")
    monkeypatch.setattr(probe, "request_json", fail)
    status, frame, _, code, message = probe.probe_one_path(
        BASE, "/iss/analyticalproducts/futoi/securities/wt.json", {}, 30, True,
    )
    assert status == "error" and frame.empty
    assert code == "Timeout" and message == "source timeout"

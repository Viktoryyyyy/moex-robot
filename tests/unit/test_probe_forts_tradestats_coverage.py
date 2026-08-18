from __future__ import annotations

from datetime import date

import pytest

from moex_data.futures import probe_forts_tradestats_coverage as coverage


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Session:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, dict(params), timeout))
        return _Response(self.payload)


def test_exact_probe_uses_contract_availability_endpoint_and_exact_identity_params() -> None:
    session = _Session({"data": {"columns": ["SECID", "TRADEDATE"], "data": [["USDRUBF", "2026-08-17"]]}})
    assert coverage._exact_has_data(session, "https://apim.moex.com", "USDRUBF", date(2026, 8, 17), 30.0) is True
    assert len(session.calls) == 1
    url, params, timeout = session.calls[0]
    assert url == "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/USDRUBF.json"
    assert params == {
        "date": "2026-08-17",
        "from": "2026-08-17",
        "till": "2026-08-17",
        "secid": "USDRUBF",
        "start": 0,
        "iss.meta": "off",
        "iss.only": "tradestats",
    }
    assert timeout == 30.0


def test_exact_probe_rejects_rows_for_different_secid_or_date() -> None:
    wrong_secid = _Session({"data": {"columns": ["SECID", "TRADEDATE"], "data": [["OTHER", "2026-08-17"]]}})
    assert coverage._exact_has_data(wrong_secid, "https://apim.moex.com", "USDRUBF", date(2026, 8, 17), 30.0) is False

    wrong_date = _Session({"data": {"columns": ["SECID", "TRADEDATE"], "data": [["USDRUBF", "2020-01-03"]]}})
    assert coverage._exact_has_data(wrong_date, "https://apim.moex.com", "USDRUBF", date(2026, 8, 17), 30.0) is False


def test_probe_parser_accepts_same_data_and_tradestats_blocks_as_writer() -> None:
    expected = (["SECID", "TRADEDATE"], [["USDRUBF", "2026-08-17"]])
    assert coverage._rows({"data": {"columns": expected[0], "data": expected[1]}}) == expected
    assert coverage._rows({"tradestats": {"columns": expected[0], "data": expected[1]}}) == expected


def test_identity_fields_are_required_fail_closed() -> None:
    with pytest.raises(coverage.TradestatsCoverageError, match="SECID"):
        coverage._contains_requested_identity(["TRADEDATE"], [["2026-08-17"]], "USDRUBF", date(2026, 8, 17), date(2026, 8, 17))
    with pytest.raises(coverage.TradestatsCoverageError, match="TRADEDATE/DATE"):
        coverage._contains_requested_identity(["SECID"], [["USDRUBF"]], "USDRUBF", date(2026, 8, 17), date(2026, 8, 17))


def test_tradestats_error_message_payload_fails_closed() -> None:
    with pytest.raises(coverage.TradestatsCoverageError, match="ERROR_MESSAGE"):
        coverage._rows({"data": {"columns": ["ERROR_MESSAGE"], "data": [["bad request"]]}})


def test_missing_compatible_data_block_fails_closed() -> None:
    with pytest.raises(coverage.TradestatsCoverageError, match="compatible data block is missing"):
        coverage._rows({"history.cursor": {"columns": [], "data": []}})


def test_probe_coverage_reports_explicit_first_and_last_dates(monkeypatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-token")
    monkeypatch.setattr(coverage, "_month_has_data", lambda *args, **kwargs: args[3].month == 8)

    available = {date(2026, 8, 11), date(2026, 8, 17)}
    monkeypatch.setattr(coverage, "_exact_has_data", lambda _session, _base, _secid, value, _timeout: value in available)

    result = coverage.probe_coverage("USDRUBF", "2026-07-01", "2026-08-17")
    assert result["first_available"] == "2026-08-11"
    assert result["last_available"] == "2026-08-17"
    assert result["latest_autodetect_used"] is False
    assert result["coverage_endpoint_path"] == "/iss/datashop/algopack/fo/tradestats/USDRUBF.json"
    assert result["materialization_endpoint_path"] == "/iss/datashop/algopack/fo/tradestats.json"
    assert result["identity_filter"] == "SECID+TRADEDATE"

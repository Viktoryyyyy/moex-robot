from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures.front_next_binding import FrontNextBindingError, bind_front_next


def test_si_front_next_is_deterministic_by_expiry() -> None:
    frame = pd.DataFrame(
        [
            {"SECID": "SiH7", "BOARDID": "RFUD", "LASTTRADEDATE": "2027-03-18"},
            {"SECID": "SiZ6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-12-17"},
            {"SECID": "SiU6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-09-17"},
            {"SECID": "SiM6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-06-18"},
            {"SECID": "SiU6", "BOARDID": "OTHER", "LASTTRADEDATE": "2026-09-17"},
            {"SECID": "USDRUBF", "BOARDID": "RFUD", "LASTTRADEDATE": "2100-01-01"},
        ]
    )
    result = bind_front_next(frame, root="Si", as_of_date="2026-08-24")
    assert [row["role"] for row in result] == ["front", "next"]
    assert [row["secid"] for row in result] == ["SiU6", "SiZ6"]
    assert [row["instrument_id"] for row in result] == ["si_front_contract", "si_next_contract"]
    assert all(row["as_of_date"] == "2026-08-24" for row in result)


def test_cr_binding_does_not_need_volume_or_open_interest() -> None:
    frame = pd.DataFrame(
        [
            {"SECID": "CRZ6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-12-17"},
            {"SECID": "CRU6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-09-17"},
        ]
    )
    result = bind_front_next(frame, root="CR", as_of_date="2026-08-24")
    assert [row["secid"] for row in result] == ["CRU6", "CRZ6"]


def test_binding_fails_closed_when_two_contracts_are_not_available() -> None:
    frame = pd.DataFrame([{"SECID": "SiU6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-09-17"}])
    with pytest.raises(FrontNextBindingError, match="fewer than two"):
        bind_front_next(frame, root="Si", as_of_date="2026-08-24")


def test_binding_requires_explicit_valid_date() -> None:
    frame = pd.DataFrame(
        [
            {"SECID": "SiU6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-09-17"},
            {"SECID": "SiZ6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-12-17"},
        ]
    )
    with pytest.raises(FrontNextBindingError, match="explicit YYYY-MM-DD"):
        bind_front_next(frame, root="Si", as_of_date="latest")

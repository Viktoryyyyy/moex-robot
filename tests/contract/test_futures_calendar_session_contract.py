from __future__ import annotations

from datetime import datetime

import pytest

from moex_core.calendars.futures_session import (
    FuturesSessionError,
    resolve_futures_bar_session,
    validate_futures_calendar_session_contract_values,
    validate_partition_session_metadata,
)
from moex_core.calendars.moex_iss_calendar import MoexIssCalendarError, build_futures_calendar_from_rows
from moex_data.futures.validation import FuturesValidationError


def _calendar():
    return build_futures_calendar_from_rows(
        (
            {"trade_date": "2026-06-01", "is_trading_day": False, "reason": "holiday"},
            {"trade_date": "2026-06-02", "is_trading_day": True, "reason": "normal"},
            {"trade_date": "2026-06-03", "is_trading_day": True, "reason": "normal"},
        ),
        calendar_contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
    )


def _contract(**overrides):
    values = {
        "contract_id": "futures_calendar_session.v1",
        "artifact_class": "repo_relative",
        "repo_path": "contracts/datasets/futures_calendar_session.v1.yaml",
        "calendar_source": "MOEX_ISS_CALENDAR",
        "market": "FORTS",
        "board": "RFUD",
        "timezone": "Europe/Moscow",
        "session_rules": {
            "timestamp_field": "ts",
            "trade_date_field": "trade_date",
            "session_date_field": "session_date",
            "day_session_start": "10:00",
            "evening_session_end": "23:50",
        },
        "path_rules": {
            "hardcoded_server_path_allowed": False,
            "implicit_file_selection_allowed": False,
            "dynamic_scan_allowed": False,
        },
    }
    values.update(overrides)
    return values


def test_calendar_session_contract_shape_is_valid():
    contract = validate_futures_calendar_session_contract_values(_contract())

    assert contract.contract_id == "futures_calendar_session.v1"
    assert contract.calendar_source == "MOEX_ISS_CALENDAR"
    assert contract.market == "FORTS"
    assert contract.board == "RFUD"


def test_calendar_session_contract_rejects_implicit_selection():
    values = _contract(
        path_rules={
            "hardcoded_server_path_allowed": False,
            "implicit_file_selection_allowed": True,
            "dynamic_scan_allowed": False,
        }
    )

    with pytest.raises(FuturesValidationError):
        validate_futures_calendar_session_contract_values(values)


def test_every_bar_requires_resolvable_session_date_and_trade_date():
    resolved = resolve_futures_bar_session(
        {"ts": datetime(2026, 6, 2, 10, 5), "trade_date": "2026-06-02", "session_date": "2026-06-02"},
        calendar=_calendar(),
        contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
    )

    assert resolved.trade_date.isoformat() == "2026-06-02"
    assert resolved.session_date.isoformat() == "2026-06-02"


@pytest.mark.parametrize(
    "bar",
    (
        {"ts": datetime(2026, 6, 2, 10, 5), "session_date": "2026-06-02"},
        {"ts": datetime(2026, 6, 2, 10, 5), "trade_date": "2026-06-02"},
    ),
)
def test_bar_missing_session_or_trade_date_is_rejected(bar):
    with pytest.raises(FuturesSessionError):
        resolve_futures_bar_session(
            bar,
            calendar=_calendar(),
            contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
        )


def test_non_trading_day_bar_is_rejected():
    with pytest.raises(MoexIssCalendarError):
        resolve_futures_bar_session(
            {"ts": datetime(2026, 6, 1, 10, 5), "trade_date": "2026-06-01", "session_date": "2026-06-01"},
            calendar=_calendar(),
            contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
        )


def test_cross_session_partition_metadata_is_rejected():
    first = resolve_futures_bar_session(
        {"ts": datetime(2026, 6, 2, 10, 5), "trade_date": "2026-06-02", "session_date": "2026-06-02"},
        calendar=_calendar(),
        contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
    )
    second = resolve_futures_bar_session(
        {"ts": datetime(2026, 6, 3, 10, 5), "trade_date": "2026-06-03", "session_date": "2026-06-03"},
        calendar=_calendar(),
        contract_ref="contracts/datasets/futures_calendar_session.v1.yaml",
    )

    with pytest.raises(FuturesSessionError):
        validate_partition_session_metadata("2026-06-02", (first, second))

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time

from .moex_iss_calendar import MoexIssFuturesCalendar, coerce_date
from moex_data.futures.validation import FuturesValidationError, guard_text, require_mapping, require_text


class FuturesSessionError(ValueError):
    pass


@dataclass(frozen=True)
class FuturesCalendarSessionContract:
    contract_id: str
    calendar_source: str
    market: str
    board: str
    timezone: str
    session_date_field: str
    trade_date_field: str
    timestamp_field: str


@dataclass(frozen=True)
class FuturesBarSession:
    ts: datetime
    trade_date: date
    session_date: date
    calendar_contract_ref: str


def _require_time(value: object, field_name: str) -> time:
    if not isinstance(value, str):
        raise FuturesValidationError(field_name + " must be HH:MM text")
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise FuturesValidationError(field_name + " must be HH:MM text") from exc


def validate_futures_calendar_session_contract_values(
    values: Mapping[str, object]
) -> FuturesCalendarSessionContract:
    values = require_mapping(values, "futures_calendar_session_contract")
    contract_id = require_text(values.get("contract_id"), "contract_id")
    if contract_id != "futures_calendar_session.v1":
        raise FuturesValidationError("unexpected calendar/session contract_id")
    if require_text(values.get("artifact_class"), "artifact_class") != "repo_relative":
        raise FuturesValidationError("calendar/session contract must be repo_relative")
    calendar_source = require_text(values.get("calendar_source"), "calendar_source")
    if calendar_source != "MOEX_ISS_CALENDAR":
        raise FuturesValidationError("calendar source must be MOEX_ISS_CALENDAR")
    market = require_text(values.get("market"), "market")
    board = require_text(values.get("board"), "board")
    timezone = require_text(values.get("timezone"), "timezone")
    session_rules = require_mapping(values.get("session_rules"), "session_rules")
    session_date_field = require_text(session_rules.get("session_date_field"), "session_date_field")
    trade_date_field = require_text(session_rules.get("trade_date_field"), "trade_date_field")
    timestamp_field = require_text(session_rules.get("timestamp_field"), "timestamp_field")
    _require_time(session_rules.get("day_session_start"), "day_session_start")
    _require_time(session_rules.get("evening_session_end"), "evening_session_end")
    path_rules = require_mapping(values.get("path_rules"), "path_rules")
    if path_rules.get("hardcoded_server_path_allowed") is not False:
        raise FuturesValidationError("hardcoded server paths must be forbidden")
    if path_rules.get("implicit_file_selection_allowed") is not False:
        raise FuturesValidationError("implicit file selection must be forbidden")
    if path_rules.get("dynamic_scan_allowed") is not False:
        raise FuturesValidationError("dynamic scan must be forbidden")
    return FuturesCalendarSessionContract(
        contract_id=contract_id,
        calendar_source=calendar_source,
        market=market,
        board=board,
        timezone=timezone,
        session_date_field=session_date_field,
        trade_date_field=trade_date_field,
        timestamp_field=timestamp_field,
    )


def resolve_futures_bar_session(
    bar: Mapping[str, object], *, calendar: MoexIssFuturesCalendar, contract_ref: str
) -> FuturesBarSession:
    values = require_mapping(bar, "bar")
    ts_value = values.get("ts")
    if not isinstance(ts_value, datetime):
        raise FuturesSessionError("bar requires datetime ts")
    if "trade_date" not in values:
        raise FuturesSessionError("bar requires trade_date")
    if "session_date" not in values:
        raise FuturesSessionError("bar requires session_date")
    trade_date = calendar.require_trading_day(coerce_date(values.get("trade_date"), "trade_date"))
    session_date = calendar.require_trading_day(coerce_date(values.get("session_date"), "session_date"))
    if ts_value.date() != session_date:
        raise FuturesSessionError("bar ts date must match session_date")
    return FuturesBarSession(
        ts=ts_value,
        trade_date=trade_date,
        session_date=session_date,
        calendar_contract_ref=guard_text(contract_ref, "calendar_contract_ref"),
    )


def validate_partition_session_metadata(partition_key: str, bars: Sequence[FuturesBarSession]) -> None:
    partition_date = coerce_date(partition_key, "partition_key")
    if not bars:
        raise FuturesSessionError("partition requires at least one resolved bar session")
    for bar in bars:
        if bar.trade_date != partition_date or bar.session_date != partition_date:
            raise FuturesSessionError("cross-session partition metadata is rejected")

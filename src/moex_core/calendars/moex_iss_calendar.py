from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime


class MoexIssCalendarError(ValueError):
    pass


@dataclass(frozen=True)
class FuturesTradingDay:
    trade_date: date
    is_trading_day: bool
    reason: str


@dataclass(frozen=True)
class MoexIssFuturesCalendar:
    calendar_contract_ref: str
    days: Mapping[date, FuturesTradingDay]

    def require_trading_day(self, value: date | datetime | str) -> date:
        trade_date = coerce_date(value, "trade_date")
        day = self.days.get(trade_date)
        if day is None:
            raise MoexIssCalendarError("trade_date is not present in futures calendar")
        if not day.is_trading_day:
            raise MoexIssCalendarError("trade_date is not a trading day")
        return trade_date

    def is_trading_day(self, value: date | datetime | str) -> bool:
        trade_date = coerce_date(value, "trade_date")
        day = self.days.get(trade_date)
        return bool(day and day.is_trading_day)


def coerce_date(value: date | datetime | str, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise MoexIssCalendarError(field_name + " must be ISO date") from exc
    raise MoexIssCalendarError(field_name + " must be date, datetime, or ISO date")


def build_futures_calendar_from_rows(
    rows: Iterable[Mapping[str, object]], *, calendar_contract_ref: str
) -> MoexIssFuturesCalendar:
    days: dict[date, FuturesTradingDay] = {}
    for row in rows:
        trade_date = coerce_date(row.get("trade_date"), "trade_date")
        raw_is_trading = row.get("is_trading_day")
        if not isinstance(raw_is_trading, bool):
            raise MoexIssCalendarError("is_trading_day must be boolean")
        reason = row.get("reason", "")
        if not isinstance(reason, str):
            raise MoexIssCalendarError("reason must be text")
        if trade_date in days:
            raise MoexIssCalendarError("duplicate futures calendar date")
        days[trade_date] = FuturesTradingDay(
            trade_date=trade_date,
            is_trading_day=raw_is_trading,
            reason=reason,
        )
    if not days:
        raise MoexIssCalendarError("futures calendar must contain at least one day")
    return MoexIssFuturesCalendar(calendar_contract_ref=calendar_contract_ref, days=days)

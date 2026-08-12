from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .usdrubf_news_macro import MacroObservation


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
RUONIA_SOURCE_ID = "cbr_ruonia_daily"
KEY_RATE_SOURCE_ID = "cbr_key_rate_daily"
RUONIA_METRIC_ID = "cbr_ruonia_rate_pct"
KEY_RATE_METRIC_ID = "cbr_key_rate_pct"
_READY_STATUS = "candidate_for_phase8_2"


class CbrMacroAdapterError(ValueError):
    """Raised when normalized CBR loader output cannot be made PIT-safe."""


def _aware_datetime(value: datetime | str, field: str) -> datetime:
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(text)
        except ValueError as exc:
            raise CbrMacroAdapterError(f"{field} must be ISO datetime") from exc
    if not isinstance(value, datetime):
        raise CbrMacroAdapterError(f"{field} must be datetime or ISO datetime string")
    if value.tzinfo is None or value.utcoffset() is None:
        raise CbrMacroAdapterError(f"{field} must be timezone-aware")
    return value


def _iso_date(value: object, field: str) -> date:
    try:
        parsed = date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise CbrMacroAdapterError(f"{field} must be ISO date") from exc
    return parsed


def _finite_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CbrMacroAdapterError(f"{field} must be numeric")
    result = float(value)
    if result != result or result in {float("inf"), float("-inf")}:
        raise CbrMacroAdapterError(f"{field} must be finite")
    return result


def _day_start(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=MOSCOW_TZ)


def _next_day_start(day: date) -> datetime:
    return _day_start(day + timedelta(days=1))


def _require_record(
    record: Mapping[str, object],
    *,
    source_id: str,
) -> tuple[str, datetime]:
    if record.get("source_id") != source_id:
        raise CbrMacroAdapterError(f"unexpected source_id for {source_id}")
    if record.get("historical_model_use_status") != _READY_STATUS:
        raise CbrMacroAdapterError(f"{source_id} is not governed as {_READY_STATUS}")
    route = record.get("source_route")
    if not isinstance(route, str) or not route.startswith("https://"):
        raise CbrMacroAdapterError(f"{source_id} source_route must be HTTPS")
    retrieved = _aware_datetime(record.get("retrieved_at_utc"), "retrieved_at_utc")
    return route, retrieved


def latest_ruonia_macro_observation(
    records: Iterable[Mapping[str, object]],
    *,
    as_of_timestamp: datetime | str,
) -> MacroObservation:
    """Convert the latest causally eligible RUONIA row into MacroObservation.

    CBR exposes a row-level publication *date* but not a governed intraday
    publication timestamp in the existing loader contract. Phase 8.2 therefore
    excludes same-day publication. This adapter preserves that rule by setting
    the causal availability boundary to 00:00 Europe/Moscow on the next
    calendar day. ``published_at`` is conservatively anchored to the final
    microsecond of the official publication date; no intraday release time is
    inferred.
    """

    as_of = _aware_datetime(as_of_timestamp, "as_of_timestamp")
    local_date = as_of.astimezone(MOSCOW_TZ).date()
    candidates: list[tuple[date, date, Mapping[str, object], str, datetime]] = []

    for record in records:
        route, retrieved = _require_record(record, source_id=RUONIA_SOURCE_ID)
        observation_date = _iso_date(record.get("observation_date"), "observation_date")
        publication_date = _iso_date(record.get("publication_date"), "publication_date")
        if publication_date < observation_date:
            raise CbrMacroAdapterError("RUONIA publication_date precedes observation_date")
        if retrieved > as_of:
            continue
        available_at = _next_day_start(publication_date)
        if publication_date >= local_date or available_at > as_of or retrieved < available_at:
            continue
        candidates.append((publication_date, observation_date, record, route, retrieved))

    if not candidates:
        raise CbrMacroAdapterError("no causally eligible RUONIA observation")

    publication_date, observation_date, record, route, retrieved = max(
        candidates,
        key=lambda item: (item[0], item[1]),
    )
    available_at = _next_day_start(publication_date)
    published_at = available_at - timedelta(microseconds=1)
    return MacroObservation(
        metric_id=RUONIA_METRIC_ID,
        source_id=RUONIA_SOURCE_ID,
        source_reference=route,
        value=_finite_number(record.get("ruonia_rate_pct"), "ruonia_rate_pct"),
        unit="PERCENT_PER_ANNUM",
        observed_or_effective_at=_day_start(observation_date),
        published_at=published_at,
        available_at=available_at,
        ingested_at=retrieved,
        quality_status="OK",
    )


def latest_key_rate_macro_observation(
    records: Iterable[Mapping[str, object]],
    *,
    as_of_timestamp: datetime | str,
) -> MacroObservation:
    """Convert the latest effective CBR key-rate change into MacroObservation.

    The frozen external-data contract treats ``effective_date`` as the causal
    boundary. Announcements before that date do not make the new rate usable
    earlier, and no separate intraday publication time is invented.
    """

    as_of = _aware_datetime(as_of_timestamp, "as_of_timestamp")
    local_date = as_of.astimezone(MOSCOW_TZ).date()
    candidates: list[tuple[date, Mapping[str, object], str, datetime]] = []

    for record in records:
        route, retrieved = _require_record(record, source_id=KEY_RATE_SOURCE_ID)
        effective_date = _iso_date(record.get("effective_date"), "effective_date")
        effective_at = _day_start(effective_date)
        if retrieved > as_of:
            continue
        if effective_date > local_date or effective_at > as_of or retrieved < effective_at:
            continue
        candidates.append((effective_date, record, route, retrieved))

    if not candidates:
        raise CbrMacroAdapterError("no causally eligible key-rate observation")

    effective_date, record, route, retrieved = max(candidates, key=lambda item: item[0])
    effective_at = _day_start(effective_date)
    return MacroObservation(
        metric_id=KEY_RATE_METRIC_ID,
        source_id=KEY_RATE_SOURCE_ID,
        source_reference=route,
        value=_finite_number(record.get("key_rate_pct"), "key_rate_pct"),
        unit="PERCENT_PER_ANNUM",
        observed_or_effective_at=effective_at,
        published_at=effective_at,
        available_at=effective_at,
        ingested_at=retrieved,
        quality_status="OK",
    )


def build_current_cbr_macro_observations(
    *,
    ruonia_records: Iterable[Mapping[str, object]],
    key_rate_records: Iterable[Mapping[str, object]],
    as_of_timestamp: datetime | str,
) -> tuple[MacroObservation, MacroObservation]:
    """Return the two governed CBR macro observations for the current state."""

    return (
        latest_ruonia_macro_observation(
            ruonia_records,
            as_of_timestamp=as_of_timestamp,
        ),
        latest_key_rate_macro_observation(
            key_rate_records,
            as_of_timestamp=as_of_timestamp,
        ),
    )

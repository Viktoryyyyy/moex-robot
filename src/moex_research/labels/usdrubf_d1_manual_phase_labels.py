"""USDRUBF D1 manual B/S/OUT phase label contract materializer.

This module is contract-only research infrastructure. It does not fetch market data
and does not write normalized label artifacts on import.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Union


ALLOWED_LABELS = ("B", "S", "OUT")

LABEL_MEANINGS: Dict[str, str] = {
    "B": "buy_long_regime",
    "S": "sell_short_regime",
    "OUT": "sideways_out_of_market_regime_where_EMA_3_19_loses_effectiveness",
}

RUNTIME_AI_ASSISTANT_MAY_CONSUME = (
    "model_outputs",
    "prediction_confidence",
    "feature_freshness_metadata",
    "data_quality_flags",
)

RUNTIME_AI_ASSISTANT_MUST_NOT_CONSUME = (
    "manual_phase_labels",
    "future-derived_transition_labels",
    "phase_remaining_sessions",
    "next_phase_label_from_manual_labels",
)

NON_RUNTIME_FIELDS = frozenset(
    {
        "session_date",
        "phase_label",
        "phase_label_meaning",
        "source_interval_id",
        "interval_start_date",
        "interval_end_date",
        "transition_exit_day",
        "phase_remaining_sessions",
        "current_regime_ends_within_1d",
        "current_regime_ends_within_3d",
        "current_regime_ends_within_5d",
        "next_regime_if_current_ends",
    }
)

_MANUAL_INTERVAL_ROWS = (
    ("05.08.2024", "27.11.2024", "B"),
    ("28.11.2024", "06.12.2024", "S"),
    ("07.12.2024", "16.01.2025", "OUT"),
    ("17.01.2025", "25.02.2025", "S"),
    ("26.02.2025", "05.03.2025", "B"),
    ("06.03.2025", "17.03.2025", "S"),
    ("18.03.2025", "07.04.2025", "B"),
    ("08.04.2025", "08.04.2025", "OUT"),
    ("09.04.2025", "10.06.2025", "S"),
    ("11.06.2025", "21.07.2025", "OUT"),
    ("22.07.2025", "29.07.2025", "B"),
    ("30.07.2025", "01.08.2025", "S"),
    ("04.08.2025", "02.09.2025", "OUT"),
    ("03.09.2025", "11.09.2025", "B"),
    ("11.09.2025", "23.09.2025", "OUT"),
    ("24.09.2025", "02.10.2025", "S"),
    ("03.10.2025", "03.10.2025", "B"),
    ("06.10.2025", "15.10.2025", "S"),
    ("16.10.2025", "17.10.2025", "B"),
    ("20.10.2025", "18.11.2025", "OUT"),
    ("19.11.2025", "04.12.2025", "S"),
    ("05.12.2025", "17.12.2025", "B"),
    ("18.12.2025", "26.12.2025", "S"),
    ("29.12.2025", "05.01.2026", "B"),
    ("06.01.2026", "22.01.2026", "S"),
    ("23.01.2026", "24.02.2026", "OUT"),
    ("25.02.2026", "19.03.2026", "B"),
    ("20.03.2026", "23.03.2026", "S"),
    ("24.03.2026", "31.03.2026", "OUT"),
    ("01.04.2026", "14.04.2026", "S"),
    ("15.04.2026", "05.05.2026", "OUT"),
    ("06.05.2026", "19.05.2026", "S"),
    ("20.05.2026", "03.06.2026", "B"),
    ("04.06.2026", "09.06.2026", "S"),
    ("10.06.2026", "26.06.2026", "B"),
)


DateLike = Union[str, date, datetime]


@dataclass(frozen=True)
class RawPhaseInterval:
    """Manual inclusive phase interval before session-universe mapping."""

    interval_id: str
    start_date: date
    end_date: date
    label: str

    def __post_init__(self) -> None:
        if self.label not in ALLOWED_LABELS:
            raise ValueError(f"Unsupported phase label: {self.label!r}")
        if self.end_date < self.start_date:
            raise ValueError(
                f"Interval {self.interval_id!r} has end_date before start_date"
            )


@dataclass(frozen=True)
class PhaseLabelRow:
    """One normalized D1 session label row."""

    session_date: date
    phase_label: str
    phase_label_meaning: str
    source_interval_id: str
    interval_start_date: date
    interval_end_date: date
    transition_exit_day: bool
    phase_remaining_sessions: int
    current_regime_ends_within_1d: bool
    current_regime_ends_within_3d: bool
    current_regime_ends_within_5d: bool
    next_regime_if_current_ends: Optional[str]

    def as_dict(self) -> Dict[str, object]:
        return {
            "session_date": self.session_date.isoformat(),
            "phase_label": self.phase_label,
            "phase_label_meaning": self.phase_label_meaning,
            "source_interval_id": self.source_interval_id,
            "interval_start_date": self.interval_start_date.isoformat(),
            "interval_end_date": self.interval_end_date.isoformat(),
            "transition_exit_day": self.transition_exit_day,
            "phase_remaining_sessions": self.phase_remaining_sessions,
            "current_regime_ends_within_1d": self.current_regime_ends_within_1d,
            "current_regime_ends_within_3d": self.current_regime_ends_within_3d,
            "current_regime_ends_within_5d": self.current_regime_ends_within_5d,
            "next_regime_if_current_ends": self.next_regime_if_current_ends,
        }


def parse_date(value: DateLike, *, input_format: str = "%Y-%m-%d") -> date:
    """Parse a date supplied by caller.

    Strings default to ISO YYYY-MM-DD. Use input_format="%d.%m.%Y" for raw
    interval dates.
    """

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.strptime(value, input_format).date()
    raise TypeError(f"Unsupported date value type: {type(value)!r}")


def build_manual_intervals() -> List[RawPhaseInterval]:
    """Return the curated v1 raw manual intervals in declared order."""

    intervals: List[RawPhaseInterval] = []
    for index, (start_raw, end_raw, label) in enumerate(_MANUAL_INTERVAL_ROWS, start=1):
        intervals.append(
            RawPhaseInterval(
                interval_id=f"usdrubf_d1_manual_phase_v1_{index:03d}",
                start_date=parse_date(start_raw, input_format="%d.%m.%Y"),
                end_date=parse_date(end_raw, input_format="%d.%m.%Y"),
                label=label,
            )
        )
    return intervals


MANUAL_PHASE_INTERVALS = tuple(build_manual_intervals())


def normalize_session_dates(session_dates: Iterable[DateLike]) -> List[date]:
    """Return a sorted unique D1 session universe supplied by caller.

    No dates are synthesized. The returned universe can be MOEX calendar sessions
    or D1 OHLC dates supplied by an upstream data contract.
    """

    normalized = {
        parse_date(item, input_format="%Y-%m-%d") if isinstance(item, str) else parse_date(item)
        for item in session_dates
    }
    return sorted(normalized)


def _next_interval_label(
    intervals: Sequence[RawPhaseInterval], interval_index: int
) -> Optional[str]:
    next_index = interval_index + 1
    if next_index >= len(intervals):
        return None
    return intervals[next_index].label


def materialize_phase_label_rows(
    session_dates: Iterable[DateLike],
    intervals: Sequence[RawPhaseInterval] = MANUAL_PHASE_INTERVALS,
) -> List[PhaseLabelRow]:
    """Map raw inclusive intervals to the caller-supplied D1 session universe.

    Deterministic rules:
    - interval boundaries are inclusive;
    - non-trading starts/ends are represented only by sessions present in the
      supplied universe;
    - overlapping sessions keep the earlier interval label;
    - each emitted session has exactly one primary phase label.
    """

    session_universe = normalize_session_dates(session_dates)
    assigned: Dict[date, PhaseLabelRow] = {}

    for interval_index, interval in enumerate(intervals):
        interval_session_dates = [
            session_date
            for session_date in session_universe
            if interval.start_date <= session_date <= interval.end_date
            and session_date not in assigned
        ]
        if not interval_session_dates:
            continue

        last_index = len(interval_session_dates) - 1
        next_label = _next_interval_label(intervals, interval_index)

        for index, session_date in enumerate(interval_session_dates):
            remaining = last_index - index
            assigned[session_date] = PhaseLabelRow(
                session_date=session_date,
                phase_label=interval.label,
                phase_label_meaning=LABEL_MEANINGS[interval.label],
                source_interval_id=interval.interval_id,
                interval_start_date=interval.start_date,
                interval_end_date=interval.end_date,
                transition_exit_day=remaining == 0,
                phase_remaining_sessions=remaining,
                current_regime_ends_within_1d=remaining <= 1,
                current_regime_ends_within_3d=remaining <= 3,
                current_regime_ends_within_5d=remaining <= 5,
                next_regime_if_current_ends=next_label,
            )

    return [assigned[session_date] for session_date in session_universe if session_date in assigned]


def materialize_phase_label_dicts(
    session_dates: Iterable[DateLike],
    intervals: Sequence[RawPhaseInterval] = MANUAL_PHASE_INTERVALS,
) -> List[Dict[str, object]]:
    """Return normalized rows as JSON/CSV-ready dictionaries.

    This function does not write files. Artifact generation is intentionally a
    later task after source-data and calendar blockers are cleared.
    """

    return [
        row.as_dict()
        for row in materialize_phase_label_rows(
            session_dates=session_dates,
            intervals=intervals,
        )
    ]


def runtime_feature_fields(candidate_fields: Iterable[str]) -> Set[str]:
    """Filter out fields that are labels, future-derived targets, or provenance."""

    return {field for field in candidate_fields if field not in NON_RUNTIME_FIELDS}


def assert_single_primary_label_per_session(rows: Iterable[Mapping[str, object]]) -> None:
    """Validate that normalized rows contain no duplicate primary session labels."""

    seen: Set[object] = set()
    for row in rows:
        session_date = row["session_date"]
        if session_date in seen:
            raise ValueError(f"Duplicate primary phase label for session {session_date!r}")
        seen.add(session_date)

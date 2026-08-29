from __future__ import annotations

from datetime import datetime
from typing import Iterable, Mapping, Sequence

import pandas as pd

from src.strategy.realtime.ema_3_19_15m.session_state_ema_3_19_15m import (
    SessionStateEma31915m,
)
from src.strategy.realtime.ema_3_19_15m.signal_engine_ema_3_19_15m import (
    update_signal_state_on_closed_bar,
)

from .usdrubf_decision_engine import DecisionInput, DirectionalContext, ema_context_from_target_position
from .usdrubf_level_structure import InteractionSnapshot, classify_level_history
from .usdrubf_live_shadow_bridge import (
    MOSCOW,
    LiveShadowBridgeError,
    blocked_futoi_context,
    build_previous_session_zones,
    closed_bars,
    empty_macro_state,
)
from .usdrubf_news_macro import MacroState, NewsEvent


HISTORICAL_SPARSE_15M_SOURCE = "historical_sparse_15m_from_native_5m"


def _market_regime(interactions: Sequence[InteractionSnapshot]) -> str:
    states = {item.state for item in interactions}
    if states & {
        "BREAKOUT",
        "RETEST_PENDING",
        "RETEST",
        "RETEST_HOLD",
        "RETEST_FAIL",
        "ACCEPTANCE",
        "FALSE_BREAKOUT",
    }:
        return "PREVIOUS_SESSION_BOUNDARY_BREAKOUT_OR_RETEST"
    if states & {"APPROACH", "TEST", "REPEATED_TEST", "REJECTION", "RANGE_RETURN"}:
        return "PREVIOUS_SESSION_BOUNDARY_INTERACTION"
    return "PREVIOUS_SESSION_RANGE_UNCONFIRMED"


def build_historical_sparse_closed_15m_bars(
    current_session_bars: Iterable[Mapping[str, object]],
    *,
    as_of_timestamp: datetime | str,
) -> tuple[dict[str, object], ...]:
    """Aggregate observed native 5m rows into sparse-safe closed 15m buckets.

    Historical-research semantics only. Missing native 5m rows are never
    synthesized, forward-filled, back-filled, timestamp-shifted, or inferred.
    A non-empty aligned bucket is usable only after its nominal close time
    (bucket label + 10 minutes) is observable by ``as_of_timestamp``.
    """

    normalized = closed_bars(current_session_bars, as_of_timestamp=as_of_timestamp)
    trade_dates = {item["end"].astimezone(MOSCOW).date() for item in normalized}
    if len(trade_dates) != 1:
        raise LiveShadowBridgeError("historical current session bars must belong to one Moscow trade date")

    as_of = pd.Timestamp(as_of_timestamp)
    if as_of.tzinfo is None:
        raise LiveShadowBridgeError("historical sparse as_of_timestamp must be timezone-aware")
    as_of = as_of.tz_convert(MOSCOW)

    rows = []
    for item in normalized:
        end = item["end"]
        if not isinstance(end, datetime):
            raise AssertionError("normalized bar end must be datetime")
        rows.append(
            {
                "end": pd.Timestamp(end).tz_convert(MOSCOW),
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
                "volume": float(item["volume"]),
            }
        )

    frame = pd.DataFrame(rows).sort_values("end", kind="stable").reset_index(drop=True)
    frame["bucket_label"] = frame["end"].dt.floor("15min")

    aggregates: list[dict[str, object]] = []
    for label, group in frame.groupby("bucket_label", sort=True):
        label_ts = pd.Timestamp(label)
        if label_ts + pd.Timedelta(minutes=10) > as_of:
            continue
        ordered = group.sort_values("end", kind="stable")
        source_available_at = ordered.iloc[-1]["end"]
        if isinstance(source_available_at, pd.Timestamp):
            source_available_at = source_available_at.to_pydatetime()
        aggregates.append(
            {
                "end": label_ts.to_pydatetime().isoformat(),
                "open": float(ordered.iloc[0]["open"]),
                "high": float(ordered["high"].max()),
                "low": float(ordered["low"].min()),
                "close": float(ordered.iloc[-1]["close"]),
                "volume": float(ordered["volume"].sum()),
                "source_available_at": source_available_at,
                "constituent_count": int(len(ordered)),
            }
        )

    if not aggregates:
        raise LiveShadowBridgeError("historical sparse 15m aggregation produced zero closed rows")
    return tuple(aggregates)


def build_historical_sparse_ema_context(
    current_session_bars: Iterable[Mapping[str, object]],
    *,
    as_of_timestamp: datetime | str,
) -> DirectionalContext:
    synthetic_15m = build_historical_sparse_closed_15m_bars(
        current_session_bars,
        as_of_timestamp=as_of_timestamp,
    )
    trade_date = datetime.fromisoformat(str(synthetic_15m[0]["end"])).astimezone(MOSCOW).date().isoformat()
    state = SessionStateEma31915m(trade_date=trade_date)
    for bar in synthetic_15m:
        state = update_signal_state_on_closed_bar(state, bar)
    if state.ema_fast is None or state.ema_slow is None:
        raise LiveShadowBridgeError("EMA state is unavailable after historical sparse 15m replay")
    if state.ema_fast > state.ema_slow:
        target = 1
    elif state.ema_fast < state.ema_slow:
        target = -1
    else:
        target = 0

    available_at = synthetic_15m[-1]["source_available_at"]
    if not isinstance(available_at, datetime):
        raise AssertionError("historical sparse EMA source_available_at must be datetime")
    constituent_counts = [int(bar["constituent_count"]) for bar in synthetic_15m]
    return ema_context_from_target_position(
        target,
        available_at=available_at,
        confidence=1.0,
        details={
            "ema_fast": state.ema_fast,
            "ema_slow": state.ema_slow,
            "bar_count": len(synthetic_15m),
            "sparse_bucket_count": sum(1 for value in constituent_counts if value < 3),
            "min_constituent_count": min(constituent_counts),
            "max_constituent_count": max(constituent_counts),
            "source": HISTORICAL_SPARSE_15M_SOURCE,
            "missing_5m_imputation": False,
            "nominal_close_guard": True,
        },
    )


def build_historical_sparse_decision_input(
    *,
    current_session_bars: Iterable[Mapping[str, object]],
    prior_session_bars: Iterable[Mapping[str, object]],
    wall_clock_as_of: datetime | str,
    futoi_context: DirectionalContext | None = None,
    news_events: Sequence[NewsEvent] = (),
    macro_state: MacroState | None = None,
) -> DecisionInput:
    """Build S7.2 historical DecisionInput without relaxing the live bridge."""

    current = closed_bars(current_session_bars, as_of_timestamp=wall_clock_as_of)
    prior = closed_bars(prior_session_bars, as_of_timestamp=wall_clock_as_of)
    decision_as_of = current[-1]["end"]
    if not isinstance(decision_as_of, datetime):
        raise AssertionError("historical decision_as_of must be datetime")
    current_date = decision_as_of.astimezone(MOSCOW).date()
    prior_date = prior[-1]["end"].astimezone(MOSCOW).date()
    if prior_date >= current_date:
        raise LiveShadowBridgeError("prior session must precede current session")

    zones = build_previous_session_zones(prior)
    interactions = tuple(classify_level_history(zone, current) for zone in zones)
    ema = build_historical_sparse_ema_context(
        current,
        as_of_timestamp=decision_as_of,
    )
    futoi = futoi_context or blocked_futoi_context(
        available_at=decision_as_of,
        reason="futoi_context_not_supplied",
    )
    macro = macro_state or empty_macro_state(decision_as_of)
    trend = ema.direction if ema.direction in {"BULLISH_USD", "NEUTRAL", "BEARISH_USD"} else "NEUTRAL"

    return DecisionInput(
        as_of_timestamp=decision_as_of,
        price=float(current[-1]["close"]),
        trend=trend,
        market_regime=_market_regime(interactions),
        active_levels=zones,
        level_interactions=interactions,
        ema_3_19_ai=ema,
        futoi=futoi,
        news_events=tuple(news_events),
        macro_state=macro,
    )

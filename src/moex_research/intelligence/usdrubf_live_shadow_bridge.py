from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from math import isfinite
from typing import Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from src.strategy.realtime.ema_3_19_15m.session_state_ema_3_19_15m import (
    SessionStateEma31915m,
)
from src.strategy.realtime.ema_3_19_15m.signal_engine_ema_3_19_15m import (
    update_signal_state_on_closed_bar,
)

from .usdrubf_decision_engine import (
    DecisionInput,
    DirectionalContext,
    ema_context_from_target_position,
)
from .usdrubf_level_structure import InteractionSnapshot, LevelZone, classify_level_history
from .usdrubf_news_macro import MacroState, NewsEvent


MOSCOW = ZoneInfo("Europe/Moscow")
SECID_KEY = "Si"


class LiveShadowBridgeError(ValueError):
    """Raised when live shadow inputs cannot be built causally and fail closed."""


def _aware_datetime(value: datetime | str, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise LiveShadowBridgeError(f"{field} must be ISO datetime") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise LiveShadowBridgeError(f"{field} must be timezone-aware")
    return value


def _positive_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LiveShadowBridgeError(f"{field} must be numeric")
    result = float(value)
    if not isfinite(result) or result <= 0.0:
        raise LiveShadowBridgeError(f"{field} must be finite and positive")
    return result


def _nonnegative_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LiveShadowBridgeError(f"{field} must be numeric")
    result = float(value)
    if not isfinite(result) or result < 0.0:
        raise LiveShadowBridgeError(f"{field} must be finite and non-negative")
    return result


def _normalize_bar(raw: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(raw, Mapping):
        raise LiveShadowBridgeError("bar must be a mapping")
    end = _aware_datetime(raw.get("end"), "bar.end")
    open_price = _positive_number(raw.get("open"), "bar.open")
    high = _positive_number(raw.get("high"), "bar.high")
    low = _positive_number(raw.get("low"), "bar.low")
    close = _positive_number(raw.get("close"), "bar.close")
    volume = _nonnegative_number(raw.get("volume"), "bar.volume")
    if low > high or not low <= open_price <= high or not low <= close <= high:
        raise LiveShadowBridgeError("bar OHLC values are inconsistent")
    return {
        "end": end,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def closed_bars(
    bars: Iterable[Mapping[str, object]],
    *,
    as_of_timestamp: datetime | str,
) -> tuple[dict[str, object], ...]:
    """Return strictly ordered bars whose end timestamp is observable by as_of."""

    as_of = _aware_datetime(as_of_timestamp, "as_of_timestamp")
    normalized = [_normalize_bar(item) for item in bars]
    normalized.sort(key=lambda item: item["end"])
    result: list[dict[str, object]] = []
    previous_end: datetime | None = None
    for item in normalized:
        end = item["end"]
        if not isinstance(end, datetime):
            raise AssertionError("normalized bar end must be datetime")
        if previous_end is not None and end <= previous_end:
            raise LiveShadowBridgeError("bars must be unique and strictly increasing")
        previous_end = end
        if end <= as_of:
            result.append(item)
    if not result:
        raise LiveShadowBridgeError("no closed bars are available by as_of_timestamp")
    return tuple(result)


def _price_increment(bars: Sequence[Mapping[str, object]]) -> float:
    values: set[Decimal] = set()
    for bar in bars:
        for key in ("open", "high", "low", "close"):
            values.add(Decimal(str(_positive_number(bar.get(key), f"bar.{key}"))))
    ordered = sorted(values)
    increments = [right - left for left, right in zip(ordered, ordered[1:]) if right > left]
    if not increments:
        raise LiveShadowBridgeError("cannot infer a non-zero observed price increment")
    increment = min(increments)
    if increment <= 0:
        raise LiveShadowBridgeError("observed price increment must be positive")
    return float(increment)


def build_previous_session_zones(
    prior_session_bars: Sequence[Mapping[str, object]],
) -> tuple[LevelZone, LevelZone]:
    """Create causal zones from the fully completed prior session only."""

    if not prior_session_bars:
        raise LiveShadowBridgeError("prior session bars are required")
    normalized = [_normalize_bar(item) for item in prior_session_bars]
    normalized.sort(key=lambda item: item["end"])
    for index in range(1, len(normalized)):
        if normalized[index]["end"] <= normalized[index - 1]["end"]:
            raise LiveShadowBridgeError("prior session bars must be strictly increasing")

    prior_date = normalized[-1]["end"].astimezone(MOSCOW).date()
    if any(item["end"].astimezone(MOSCOW).date() != prior_date for item in normalized):
        raise LiveShadowBridgeError("prior session bars must belong to one Moscow trade date")

    increment = _price_increment(normalized)
    session_high = max(float(item["high"]) for item in normalized)
    session_low = min(float(item["low"]) for item in normalized)
    if session_low - increment <= 0:
        raise LiveShadowBridgeError("prior-session low cannot form a positive level zone")

    created_at = normalized[-1]["end"].isoformat()
    suffix = prior_date.strftime("%Y%m%d")
    high_zone = LevelZone(
        level_id=f"previous_session_high_{suffix}",
        level_type="PREVIOUS_SESSION_HIGH",
        center_price=session_high,
        lower_bound=session_high - increment,
        upper_bound=session_high + increment,
        created_at=created_at,
        source_timeframe="5m",
        status="ACTIVE",
    )
    low_zone = LevelZone(
        level_id=f"previous_session_low_{suffix}",
        level_type="PREVIOUS_SESSION_LOW",
        center_price=session_low,
        lower_bound=session_low - increment,
        upper_bound=session_low + increment,
        created_at=created_at,
        source_timeframe="5m",
        status="ACTIVE",
    )
    return high_zone, low_zone


def _bucket_label_15m(value: datetime) -> datetime:
    return value.replace(minute=(value.minute // 15) * 15, second=0, microsecond=0)


def build_closed_15m_bars(
    current_session_bars: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    """Aggregate complete aligned 5m triples exactly like the established 15m runtime."""

    if not current_session_bars:
        raise LiveShadowBridgeError("current session bars are required for 15m aggregation")
    normalized = [_normalize_bar(item) for item in current_session_bars]
    normalized.sort(key=lambda item: item["end"])
    for index in range(1, len(normalized)):
        if normalized[index]["end"] <= normalized[index - 1]["end"]:
            raise LiveShadowBridgeError("current session bars must be strictly increasing")

    aggregates: list[dict[str, object]] = []
    for index in range(2, len(normalized)):
        b0 = normalized[index - 2]
        b1 = normalized[index - 1]
        b2 = normalized[index]
        t0 = b0["end"]
        t1 = b1["end"]
        t2 = b2["end"]
        if not all(isinstance(value, datetime) for value in (t0, t1, t2)):
            raise AssertionError("normalized bar ends must be datetime")
        label = _bucket_label_15m(t2)
        if t2 != label + timedelta(minutes=10):
            continue
        if t0 != label or t1 != label + timedelta(minutes=5):
            raise LiveShadowBridgeError(
                "broken 15m bucket aligned to broker label " + label.isoformat()
            )
        aggregates.append(
            {
                "end": label.isoformat(),
                "open": float(b0["open"]),
                "high": max(float(b0["high"]), float(b1["high"]), float(b2["high"])),
                "low": min(float(b0["low"]), float(b1["low"]), float(b2["low"])),
                "close": float(b2["close"]),
                "volume": float(b0["volume"]) + float(b1["volume"]) + float(b2["volume"]),
                "source_available_at": t2,
            }
        )
    if not aggregates:
        raise LiveShadowBridgeError("no complete aligned 15m bars are available")
    return tuple(aggregates)


def build_ema_context(
    current_session_bars: Sequence[Mapping[str, object]],
) -> DirectionalContext:
    if not current_session_bars:
        raise LiveShadowBridgeError("current session bars are required for EMA context")
    normalized = [_normalize_bar(item) for item in current_session_bars]
    normalized.sort(key=lambda item: item["end"])
    trade_date = normalized[0]["end"].astimezone(MOSCOW).date().isoformat()
    if any(item["end"].astimezone(MOSCOW).date().isoformat() != trade_date for item in normalized):
        raise LiveShadowBridgeError("current session bars must belong to one Moscow trade date")

    synthetic_15m = build_closed_15m_bars(normalized)
    state = SessionStateEma31915m(trade_date=trade_date)
    for bar in synthetic_15m:
        state = update_signal_state_on_closed_bar(state, bar)
    if state.ema_fast is None or state.ema_slow is None:
        raise LiveShadowBridgeError("EMA state is unavailable after closed 15m replay")
    if state.ema_fast > state.ema_slow:
        target = 1
    elif state.ema_fast < state.ema_slow:
        target = -1
    else:
        target = 0
    available_at = synthetic_15m[-1]["source_available_at"]
    if not isinstance(available_at, datetime):
        raise AssertionError("15m source_available_at must be datetime")
    return ema_context_from_target_position(
        target,
        available_at=available_at,
        confidence=1.0,
        details={
            "ema_fast": state.ema_fast,
            "ema_slow": state.ema_slow,
            "bar_count": len(synthetic_15m),
            "source": "closed_15m_bar_replay_from_5m",
        },
    )


def blocked_futoi_context(
    *,
    available_at: datetime | str,
    reason: str,
) -> DirectionalContext:
    return DirectionalContext(
        source_id="futoi",
        available_at=available_at,
        direction="MIXED",
        confidence=0.0,
        quality_status="BLOCKED",
        details={"reason": str(reason)},
    )


def futoi_context_from_pair(pair: object) -> DirectionalContext:
    required = (
        "source_available_at",
        "trade_date",
        "moment",
        "sess_id",
        "fiz_pos",
        "fiz_pos_long",
        "fiz_pos_short",
        "fiz_pos_long_num",
        "fiz_pos_short_num",
        "yur_pos",
        "yur_pos_long",
        "yur_pos_short",
        "yur_pos_long_num",
        "yur_pos_short_num",
    )
    if any(not hasattr(pair, name) for name in required):
        raise LiveShadowBridgeError("FUTOI pair does not match the validated loader contract")
    available_at = _aware_datetime(getattr(pair, "source_available_at"), "futoi.available_at")
    return DirectionalContext(
        source_id="futoi",
        available_at=available_at,
        direction="MIXED",
        confidence=0.0,
        quality_status="OK",
        details={
            "trade_date": getattr(pair, "trade_date").isoformat(),
            "moment": getattr(pair, "moment").isoformat(),
            "sess_id": str(getattr(pair, "sess_id")),
            "fiz_pos": float(getattr(pair, "fiz_pos")),
            "fiz_pos_long": float(getattr(pair, "fiz_pos_long")),
            "fiz_pos_short": float(getattr(pair, "fiz_pos_short")),
            "fiz_pos_long_num": int(getattr(pair, "fiz_pos_long_num")),
            "fiz_pos_short_num": int(getattr(pair, "fiz_pos_short_num")),
            "yur_pos": float(getattr(pair, "yur_pos")),
            "yur_pos_long": float(getattr(pair, "yur_pos_long")),
            "yur_pos_short": float(getattr(pair, "yur_pos_short")),
            "yur_pos_long_num": int(getattr(pair, "yur_pos_long_num")),
            "yur_pos_short_num": int(getattr(pair, "yur_pos_short_num")),
            "interpretation": "participant_positioning_only_no_directional_rule_frozen",
        },
    )


def load_futoi_context(
    *,
    prior_trade_date: date,
    current_trade_date: date,
    fallback_available_at: datetime | str,
    enabled: bool,
) -> DirectionalContext:
    """Load FUTOI only through the existing validated loader when explicitly enabled."""

    if not enabled:
        return blocked_futoi_context(
            available_at=fallback_available_at,
            reason="live_futoi_not_explicitly_enabled",
        )
    try:
        from src.moex_research.runners.usdrubf_phase8_7a_futoi_si_source_validation import (
            load_futoi_daily_pair,
            validate_prior_session_pair,
        )

        pair, _columns = load_futoi_daily_pair(prior_trade_date)
        validate_prior_session_pair(
            pair,
            target_trade_date=current_trade_date,
            prior_trade_date=prior_trade_date,
        )
        return futoi_context_from_pair(pair)
    except Exception as exc:
        blocker = getattr(exc, "blocker", exc.__class__.__name__)
        return blocked_futoi_context(
            available_at=fallback_available_at,
            reason=f"{blocker}: {exc}",
        )


def empty_macro_state(as_of_timestamp: datetime | str) -> MacroState:
    as_of = _aware_datetime(as_of_timestamp, "as_of_timestamp")
    return MacroState(
        as_of_timestamp=as_of.isoformat(),
        observations=(),
        overall_direction="NEUTRAL",
        confidence=0.0,
        dominant_drivers=(),
    )


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


def build_live_decision_input(
    *,
    current_session_bars: Iterable[Mapping[str, object]],
    prior_session_bars: Iterable[Mapping[str, object]],
    wall_clock_as_of: datetime | str,
    futoi_context: DirectionalContext | None = None,
    news_events: Sequence[NewsEvent] = (),
    macro_state: MacroState | None = None,
) -> DecisionInput:
    wall_clock = _aware_datetime(wall_clock_as_of, "wall_clock_as_of")
    current = closed_bars(current_session_bars, as_of_timestamp=wall_clock)
    prior = tuple(_normalize_bar(item) for item in prior_session_bars)
    if not prior:
        raise LiveShadowBridgeError("prior session bars are required")

    decision_as_of = current[-1]["end"]
    if not isinstance(decision_as_of, datetime):
        raise AssertionError("normalized decision_as_of must be datetime")
    current_date = decision_as_of.astimezone(MOSCOW).date()
    prior_date = prior[-1]["end"].astimezone(MOSCOW).date()
    if prior_date >= current_date:
        raise LiveShadowBridgeError("prior session must precede current session")

    zones = build_previous_session_zones(prior)
    interactions = tuple(classify_level_history(zone, current) for zone in zones)
    ema = build_ema_context(current)
    futoi = futoi_context or blocked_futoi_context(
        available_at=decision_as_of,
        reason="futoi_context_not_supplied",
    )
    macro = macro_state or empty_macro_state(decision_as_of)
    trend = (
        ema.direction
        if ema.direction in {"BULLISH_USD", "NEUTRAL", "BEARISH_USD"}
        else "NEUTRAL"
    )

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


def find_prior_session(
    current_trade_date: date,
    *,
    loader: Callable[[str, date], Sequence[Mapping[str, object]]],
    max_lookback_days: int = 7,
) -> tuple[date, tuple[Mapping[str, object], ...]]:
    if not isinstance(current_trade_date, date):
        raise LiveShadowBridgeError("current_trade_date must be date")
    if max_lookback_days < 1 or max_lookback_days > 14:
        raise LiveShadowBridgeError("max_lookback_days must be within 1..14")
    for offset in range(1, max_lookback_days + 1):
        candidate = current_trade_date - timedelta(days=offset)
        bars = tuple(loader(SECID_KEY, candidate))
        if bars:
            return candidate, bars
    raise LiveShadowBridgeError("no prior session with bars found within bounded lookback")


def safe_wait_decision_agent(payload: Mapping[str, object]) -> Mapping[str, object]:
    """Explicit non-production fallback used only for input-bridge shadow smoke tests."""

    market_facts = payload.get("market_facts")
    output_contract = payload.get("output_contract")
    if not isinstance(market_facts, Mapping) or not isinstance(output_contract, Mapping):
        raise LiveShadowBridgeError("decision payload is malformed")
    trend = market_facts.get("trend")
    if trend not in {"BULLISH_USD", "NEUTRAL", "BEARISH_USD"}:
        trend = "NEUTRAL"
    refs = output_contract.get("allowed_evidence_refs")
    evidence_refs: list[str] = []
    if isinstance(refs, Sequence) and not isinstance(refs, (str, bytes)):
        evidence_refs = [str(item) for item in refs if str(item).startswith("level:")]
    return {
        "final_bias": trend,
        "trade_state": "WAIT",
        "confidence": 0.25,
        "target_references": [],
        "invalidation_reference": None,
        "scenario": "Short live shadow input-bridge smoke; no production decision transport is being asserted.",
        "reason": "WAIT is forced by the explicitly selected safe shadow agent.",
        "evidence_refs": evidence_refs,
    }


__all__ = [
    "LiveShadowBridgeError",
    "MOSCOW",
    "SECID_KEY",
    "blocked_futoi_context",
    "build_closed_15m_bars",
    "build_ema_context",
    "build_live_decision_input",
    "build_previous_session_zones",
    "closed_bars",
    "empty_macro_state",
    "find_prior_session",
    "futoi_context_from_pair",
    "load_futoi_context",
    "safe_wait_decision_agent",
]

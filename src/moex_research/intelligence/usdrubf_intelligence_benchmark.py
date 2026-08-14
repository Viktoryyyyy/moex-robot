from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from math import isfinite
from typing import Mapping, Sequence


_ALLOWED_BIAS = {"BULLISH_USD", "NEUTRAL", "BEARISH_USD"}
_ALLOWED_TRADE_STATE = {"WAIT", "ENTER", "HOLD", "ADD", "REDUCE", "EXIT"}
_ACTIVE_TRADE_STATES = {"ENTER", "HOLD", "ADD"}
_ALLOWED_EXPOSURE = {"LONG_USD", "SHORT_USD", "OUT"}


class IntelligenceBenchmarkError(ValueError):
    """Raised when benchmark inputs violate deterministic evaluation boundaries."""


def _aware_datetime(value: datetime | str, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise IntelligenceBenchmarkError(f"{field} must be ISO datetime") from exc
    if not isinstance(value, datetime):
        raise IntelligenceBenchmarkError(f"{field} must be datetime or ISO datetime string")
    if value.tzinfo is None or value.utcoffset() is None:
        raise IntelligenceBenchmarkError(f"{field} must be timezone-aware")
    return value


def _positive_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise IntelligenceBenchmarkError(f"{field} must be numeric")
    result = float(value)
    if not isfinite(result) or result <= 0.0:
        raise IntelligenceBenchmarkError(f"{field} must be finite and positive")
    return result


def _probability(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise IntelligenceBenchmarkError(f"{field} must be numeric")
    result = float(value)
    if not isfinite(result) or not 0.0 <= result <= 1.0:
        raise IntelligenceBenchmarkError(f"{field} must be within 0..1")
    return result


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IntelligenceBenchmarkError(f"{field} must be non-empty")
    return value.strip()


def _rate(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else numerator / denominator


def _mean(values: Sequence[float]) -> float | None:
    return None if not values else sum(values) / len(values)


@dataclass(frozen=True)
class BenchmarkObservation:
    as_of_timestamp: datetime | str
    price: float
    final_bias: str
    trade_state: str
    confidence: float
    future_prices: Mapping[int, float]
    trend: str
    market_regime: str
    instrument: str = "USDRUBF"

    def __post_init__(self) -> None:
        if self.instrument != "USDRUBF":
            raise IntelligenceBenchmarkError("instrument must be USDRUBF")
        object.__setattr__(self, "as_of_timestamp", _aware_datetime(self.as_of_timestamp, "as_of_timestamp"))
        object.__setattr__(self, "price", _positive_number(self.price, "price"))
        if self.final_bias not in _ALLOWED_BIAS:
            raise IntelligenceBenchmarkError("invalid final_bias")
        if self.trade_state not in _ALLOWED_TRADE_STATE:
            raise IntelligenceBenchmarkError("invalid trade_state")
        object.__setattr__(self, "confidence", _probability(self.confidence, "confidence"))
        object.__setattr__(self, "trend", _text(self.trend, "trend"))
        object.__setattr__(self, "market_regime", _text(self.market_regime, "market_regime"))

        normalized: dict[int, float] = {}
        for raw_horizon, raw_price in self.future_prices.items():
            if isinstance(raw_horizon, bool) or not isinstance(raw_horizon, int) or raw_horizon <= 0:
                raise IntelligenceBenchmarkError("future price horizon must be a positive integer")
            normalized[raw_horizon] = _positive_number(raw_price, f"future_prices[{raw_horizon}]")
        object.__setattr__(self, "future_prices", normalized)

    @property
    def exposure(self) -> str:
        if self.trade_state not in _ACTIVE_TRADE_STATES:
            return "OUT"
        if self.final_bias == "BULLISH_USD":
            return "LONG_USD"
        if self.final_bias == "BEARISH_USD":
            return "SHORT_USD"
        return "OUT"

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["as_of_timestamp"] = self.as_of_timestamp.isoformat()  # type: ignore[union-attr]
        payload["future_prices"] = {str(key): value for key, value in sorted(self.future_prices.items())}
        payload["exposure"] = self.exposure
        return payload


def realized_bias(*, start_price: float, future_price: float, neutral_band_bps: float) -> str:
    start = _positive_number(start_price, "start_price")
    future = _positive_number(future_price, "future_price")
    if isinstance(neutral_band_bps, bool) or not isinstance(neutral_band_bps, (int, float)):
        raise IntelligenceBenchmarkError("neutral_band_bps must be numeric")
    band = float(neutral_band_bps)
    if not isfinite(band) or band < 0.0:
        raise IntelligenceBenchmarkError("neutral_band_bps must be finite and non-negative")
    return_bps = (future / start - 1.0) * 10_000.0
    if return_bps > band:
        return "BULLISH_USD"
    if return_bps < -band:
        return "BEARISH_USD"
    return "NEUTRAL"


def _validate_horizons(horizons: Sequence[int]) -> tuple[int, ...]:
    normalized = tuple(horizons)
    if not normalized:
        raise IntelligenceBenchmarkError("at least one horizon is required")
    if any(isinstance(item, bool) or not isinstance(item, int) or item <= 0 for item in normalized):
        raise IntelligenceBenchmarkError("horizons must contain positive integers")
    if len(normalized) != len(set(normalized)):
        raise IntelligenceBenchmarkError("horizons must be unique")
    return tuple(sorted(normalized))


def evaluate_intelligence_quality(
    observations: Sequence[BenchmarkObservation],
    *,
    horizons: Sequence[int] = (1, 3, 5, 10),
    neutral_band_bps: float = 0.0,
    high_confidence_threshold: float = 0.75,
) -> dict[str, object]:
    """Evaluate persisted RUB Intelligence decisions against future prices.

    Future prices are labels only. This function does not build MarketState, alter DecisionInput,
    or participate in live inference. Missing terminal horizons are reported and excluded only from
    that horizon's metric denominator.
    """

    resolved_horizons = _validate_horizons(horizons)
    high_conf = _probability(high_confidence_threshold, "high_confidence_threshold")
    band = float(neutral_band_bps)
    realized_bias(start_price=1.0, future_price=1.0, neutral_band_bps=band)

    rows = tuple(observations)
    timestamps = [item.as_of_timestamp for item in rows]
    if len(timestamps) != len(set(timestamps)):
        raise IntelligenceBenchmarkError("benchmark as_of_timestamp values must be unique")

    results: dict[str, object] = {
        "instrument": "USDRUBF",
        "observation_count": len(rows),
        "horizons": list(resolved_horizons),
        "neutral_band_bps": band,
        "high_confidence_threshold": high_conf,
        "metric_semantics": {
            "bias_accuracy": "final_bias equals realized forward-return class",
            "selected_class_brier": "mean squared error of stated confidence versus correctness of selected final_bias",
            "active_directional_success_rate": "LONG_USD/SHORT_USD exposure matches realized directional class",
            "missed_directional_opportunity_rate": "OUT while realized class is directional",
            "mean_signed_return_bps_when_active": "forward return signed by LONG_USD=+1 or SHORT_USD=-1; no costs or sizing",
        },
        "by_horizon": {},
    }

    by_horizon: dict[str, object] = {}
    for horizon in resolved_horizons:
        eligible = [item for item in rows if horizon in item.future_prices]
        missing_count = len(rows) - len(eligible)
        correct_flags: list[int] = []
        confidence_errors: list[float] = []
        signed_active_returns: list[float] = []
        active_positive = 0
        active_count = 0
        active_directional_correct = 0
        missed_directional = 0
        out_count = 0
        high_conf_count = 0
        high_conf_errors = 0
        prediction_counts = {key: 0 for key in sorted(_ALLOWED_BIAS)}
        realized_counts = {key: 0 for key in sorted(_ALLOWED_BIAS)}
        exposure_counts = {key: 0 for key in sorted(_ALLOWED_EXPOSURE)}

        for item in eligible:
            future_price = item.future_prices[horizon]
            outcome = realized_bias(
                start_price=item.price,
                future_price=future_price,
                neutral_band_bps=band,
            )
            prediction_counts[item.final_bias] += 1
            realized_counts[outcome] += 1
            exposure_counts[item.exposure] += 1

            correct = int(item.final_bias == outcome)
            correct_flags.append(correct)
            confidence_errors.append((item.confidence - correct) ** 2)
            if item.confidence >= high_conf:
                high_conf_count += 1
                if not correct:
                    high_conf_errors += 1

            forward_return_bps = (future_price / item.price - 1.0) * 10_000.0
            if item.exposure == "LONG_USD":
                active_count += 1
                signed_active_returns.append(forward_return_bps)
                if forward_return_bps > 0.0:
                    active_positive += 1
                if outcome == "BULLISH_USD":
                    active_directional_correct += 1
            elif item.exposure == "SHORT_USD":
                active_count += 1
                signed_active_returns.append(-forward_return_bps)
                if forward_return_bps < 0.0:
                    active_positive += 1
                if outcome == "BEARISH_USD":
                    active_directional_correct += 1
            else:
                out_count += 1
                if outcome in {"BULLISH_USD", "BEARISH_USD"}:
                    missed_directional += 1

        by_horizon[str(horizon)] = {
            "eligible_count": len(eligible),
            "missing_label_count": missing_count,
            "label_coverage": _rate(len(eligible), len(rows)),
            "bias_correct_count": sum(correct_flags),
            "bias_accuracy": _rate(sum(correct_flags), len(correct_flags)),
            "selected_class_brier": _mean(confidence_errors),
            "high_confidence_count": high_conf_count,
            "high_confidence_error_rate": _rate(high_conf_errors, high_conf_count),
            "active_count": active_count,
            "active_coverage": _rate(active_count, len(eligible)),
            "active_directional_success_rate": _rate(active_directional_correct, active_count),
            "positive_active_return_rate": _rate(active_positive, active_count),
            "mean_signed_return_bps_when_active": _mean(signed_active_returns),
            "out_count": out_count,
            "missed_directional_opportunity_rate": _rate(missed_directional, out_count),
            "prediction_distribution": prediction_counts,
            "realized_distribution": realized_counts,
            "exposure_distribution": exposure_counts,
        }

    results["by_horizon"] = by_horizon
    return results

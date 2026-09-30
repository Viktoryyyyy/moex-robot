"""Deterministic, bar-resolution forecast outcomes; never execution or P&L."""
from __future__ import annotations

from datetime import datetime
from decimal import Context, Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext

from .usdrubf_forecast_journal import ForecastJournalError, _hash, _token, encode, fields, text, timestamp

FORECAST_VERSION = "usdrubf.forecast.v1"
FORECAST_V2 = "usdrubf.forecast.v2"
FACTS_VERSION = "usdrubf.forecast_facts.v1"
EVALUATOR_VERSION = "usdrubf.forecast_evaluation.v2"


def price(value: object, *, zero: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ForecastJournalError("price/band must be a decimal string or integer")
    if len(str(value)) > 64:
        raise ForecastJournalError("decimal input too large")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ForecastJournalError("invalid decimal") from exc
    if (not number.is_finite() or number < 0 or (not zero and number == 0)
            or number > Decimal("1e12") or len(number.as_tuple().digits) > 30
            or number.as_tuple().exponent < -12 or number.as_tuple().exponent > 12):
        raise ForecastJournalError("decimal outside bounded exact-price domain")
    return number


def array(value: object, *, nonempty: bool = False) -> list:
    if not isinstance(value, list) or len(value) > 100_000 or (nonempty and not value):
        raise ForecastJournalError("bounded explicit list required")
    return value


def interval(value: object) -> tuple[datetime, datetime]:
    if not isinstance(value, list) or len(value) != 2:
        raise ForecastJournalError("interval must contain open and close timestamps")
    start, end = map(timestamp, value)
    if start >= end:
        raise ForecastJournalError("bar interval must be positive")
    return start, end


def predicate(value: object) -> None:
    if value is None:
        return
    value = fields(value, {"op", "level", "closes"})
    if text(value["op"]) not in {"GE", "LE"}:
        raise ForecastJournalError("only GE/LE closed-bar conditions are supported")
    price(value["level"])
    count = value["closes"]
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 100_000:
        raise ForecastJournalError("positive consecutive-close count required")


def validate_forecast(spec: object) -> dict:
    context = None
    if isinstance(spec, dict) and spec.get("schema_version") == FORECAST_V2:
        context = spec.get("context")
        legacy = {k: v for k, v in spec.items() if k != "context"}
        legacy["schema_version"] = FORECAST_VERSION
        validate_forecast(legacy)
        context = fields(context, {"original_text", "interpretation", "external_context",
            "registration_class", "grid_provenance", "baseline", "horizon_label"})
        text(context["original_text"])
        text(context["interpretation"])
        if context["registration_class"] not in {"SYNTHETIC", "RETROSPECTIVE", "PROSPECTIVE_LOCAL"}:
            raise ForecastJournalError("explicit registration class required")
        if context["horizon_label"] not in {"DAY", "WEEK"}:
            raise ForecastJournalError("DAY/WEEK horizon label required")
        grid = fields(context["grid_provenance"], {"source", "completeness_scope"})
        text(grid["source"])
        text(grid["completeness_scope"])
        baseline = fields(context["baseline"], {"input", "pointer", "timestamp_pointer", "status"})
        if baseline["input"] not in spec["inputs"] or baseline["status"] not in {"CANONICAL_FIELD", "EXTERNAL_UNVERIFIED"}:
            raise ForecastJournalError("baseline must reference a linked input")
        text(baseline["pointer"])
        text(baseline["timestamp_pointer"])
        for item in array(context["external_context"]):
            fields(item, {"input", "interpretation"})
            if item["input"] not in spec["inputs"]:
                raise ForecastJournalError("external context must reference frozen input")
            text(item["interpretation"])
        return spec
    spec = fields(spec, {"schema_version", "instrument", "contract", "issued_at",
                         "horizon_start", "horizon_end", "reference_price", "reference_price_at",
                         "bias", "neutral_band_bps", "range", "method_version", "inputs",
                         "observation_grid", "scenarios", "supersedes", "revision_reason"})
    encode(spec)
    if spec["schema_version"] != FORECAST_VERSION or spec["instrument"] != "USDRUBF":
        raise ForecastJournalError("forecast schema/instrument mismatch")
    if spec["contract"] != "USDRUBF":
        raise ForecastJournalError("v1 supports exact USDRUBF contract only")
    text(spec["method_version"])
    issued, start, end = (timestamp(spec[k]) for k in ("issued_at", "horizon_start", "horizon_end"))
    if not timestamp(spec["reference_price_at"]) <= issued <= start < end:
        raise ForecastJournalError("reference <= issued <= horizon start < horizon end required")
    price(spec["reference_price"])
    band = price(spec["neutral_band_bps"], zero=True)
    if band > 10_000:
        raise ForecastJournalError("neutral band cannot exceed 10000 bps")
    if spec["bias"] is not None and text(spec["bias"]) not in {"BULLISH_USD", "BEARISH_USD", "NEUTRAL"}:
        raise ForecastJournalError("unsupported bias")
    input_ids = set()
    for ref in array(spec["inputs"], nonempty=True):
        fields(ref, {"kind", "id", "sha256"})
        if ref["kind"] != "input" or _token(ref["id"]) in input_ids:
            raise ForecastJournalError("distinct input references required")
        _hash(ref["sha256"])
        input_ids.add(ref["id"])
    bounds = spec["range"]
    if bounds is not None:
        fields(bounds, {"lower", "upper"})
        if price(bounds["lower"]) > price(bounds["upper"]):
            raise ForecastJournalError("reversed forecast range")
    grid = [interval(item) for item in array(spec["observation_grid"], nonempty=True)]
    if grid[0][0] != start or grid[-1][1] != end:
        raise ForecastJournalError("grid must explicitly span the forecast horizon")
    if any(right[0] < left[1] for left, right in zip(grid, grid[1:])):
        raise ForecastJournalError("grid must be ordered and nonoverlapping")
    ids = set()
    for item in array(spec["scenarios"]):
        fields(item, {"id", "direction", "activation", "confirmation", "targets", "invalidation"})
        identifier = text(item["id"])
        if identifier in ids:
            raise ForecastJournalError("duplicate scenario ID")
        ids.add(identifier)
        if text(item["direction"]) not in {"BULLISH_USD", "BEARISH_USD"}:
            raise ForecastJournalError("scenario must have an explicit direction")
        predicate(item["activation"])
        predicate(item["confirmation"])
        targets = [price(value) for value in array(item["targets"])]
        if len(set(targets)) != len(targets):
            raise ForecastJournalError("duplicate target level")
        stop = None if item["invalidation"] is None else price(item["invalidation"])
        if stop is not None and targets:
            bullish = item["direction"] == "BULLISH_USD"
            if (bullish and stop >= min(targets)) or (not bullish and stop <= max(targets)):
                raise ForecastJournalError("invalidation and target geometry overlap")
    if spec["supersedes"] is None:
        if spec["revision_reason"] is not None:
            raise ForecastJournalError("revision reason without superseded forecast")
    else:
        previous = fields(spec["supersedes"], {"kind", "id", "sha256"})
        if previous["kind"] != "forecast":
            raise ForecastJournalError("supersedes must reference a forecast")
        _token(previous["id"])
        _hash(previous["sha256"])
        text(spec["revision_reason"])
    return spec


def _satisfied(condition: dict, close: Decimal) -> bool:
    level = price(condition["level"])
    return close >= level if condition["op"] == "GE" else close <= level


def _scenario(spec: dict, bars: list[dict], start: str) -> dict:
    bullish = spec["direction"] == "BULLISH_USD"
    phase = "activation" if spec["activation"] else ("confirmation" if spec["confirmation"] else "active")
    result = {"id": spec["id"], "status": None,
              "activated_at": None if spec["activation"] else start,
              "confirmed_at": start if phase == "active" else None,
              "invalidation_bar": None, "reason": None,
              "targets": [{"level": target, "status": "NOT_REACHED", "first_touch_bar": None}
                          for target in spec["targets"]]}
    consecutive = 0
    for index, bar in enumerate(bars):
        low, high, close = (price(bar[key]) for key in ("low", "high", "close"))
        span = [bar["open_at"], bar["close_at"]]
        stop = None if spec["invalidation"] is None else price(spec["invalidation"])
        stopped = stop is not None and (low <= stop if bullish else high >= stop)
        if index == 0 and phase == "active":
            opening = price(bar["open"])
            if any((opening >= price(t) if bullish else opening <= price(t)) for t in spec["targets"]):
                result["status"] = "NOT_EVALUABLE"
                result["reason"] = "TARGET_ALREADY_PASSED_AT_HORIZON_START"
                break
        if phase != "active":
            # Cancellation occurred by this close, before a close-based trigger can be used.
            if stopped:
                result["status"] = ("CANCELLED_BEFORE_ACTIVATION" if phase == "activation"
                                    else "CANCELLED_BEFORE_CONFIRMATION")
                result["invalidation_bar"] = span
                break
            condition = spec[phase]
            consecutive = consecutive + 1 if _satisfied(condition, close) else 0
            if consecutive >= condition["closes"]:
                consecutive = 0
                if phase == "activation":
                    result["activated_at"] = bar["close_at"]
                    phase = "confirmation" if spec["confirmation"] else "active"
                else:
                    phase = "active"
                if phase == "active":
                    result["confirmed_at"] = bar["close_at"]
                    if any((close >= price(t) if bullish else close <= price(t)) for t in spec["targets"]):
                        result["status"] = "NOT_EVALUABLE"
                        result["reason"] = "TARGET_ALREADY_PASSED_AT_CONFIRMATION_CLOSE"
                        break
            # Intrabar highs/lows on the activation/confirmation bar precede the close.
            continue
        for target in result["targets"]:
            if target["status"] != "NOT_REACHED":
                continue
            touched = high >= price(target["level"]) if bullish else low <= price(target["level"])
            if touched or stopped:
                target["first_touch_bar"] = span if touched else None
                target["status"] = ("UNKNOWN_ORDER" if touched and stopped else
                                    "TARGET_FIRST" if touched else "INVALIDATION_FIRST")
        if stopped:
            result["invalidation_bar"] = span
            states = [t["status"] for t in result["targets"]]
            if "UNKNOWN_ORDER" in states:
                result["status"] = "AMBIGUOUS"
                result["reason"] = "SAME_BAR_TARGET_AND_INVALIDATION_ORDER_UNKNOWN"
            elif states and all(state == "TARGET_FIRST" for state in states):
                result["status"] = "TARGETS_BEFORE_INVALIDATION"
            else:
                result["status"] = "INVALIDATED"
            break
    if result["status"] is None:
        if phase != "active":
            result["status"] = "NOT_ACTIVATED" if phase == "activation" else "NOT_CONFIRMED"
        else:
            states = [t["status"] for t in result["targets"]]
            result["status"] = ("TARGETS_REACHED" if states and all(s == "TARGET_FIRST" for s in states)
                                else "ACTIVE_AT_HORIZON_END")
    if phase != "active" or result["status"] == "NOT_EVALUABLE":
        for target in result["targets"]:
            target["status"] = "NOT_APPLICABLE"
    return result


def evaluate(spec: dict, facts: object, *, evaluated_at: datetime,
             source_as_of: datetime, limitations: list[str]) -> dict:
    spec = validate_forecast(spec)
    if (not isinstance(evaluated_at, datetime) or evaluated_at.utcoffset() is None
            or not isinstance(source_as_of, datetime) or source_as_of.utcoffset() is None):
        raise ForecastJournalError("aware evaluation/source clocks required")
    if evaluated_at < timestamp(spec["horizon_end"]):
        raise ForecastJournalError("forecast horizon has not finished")
    if source_as_of > evaluated_at:
        raise ForecastJournalError("future factual source")
    for item in array(limitations):
        text(item)
    facts = fields(facts, {"schema_version", "instrument", "contract", "bars"})
    if (facts["schema_version"], facts["instrument"], facts["contract"]) != (
            FACTS_VERSION, spec["instrument"], spec["contract"]):
        raise ForecastJournalError("factual schema/instrument/contract mismatch")
    expected = {interval(item): item for item in spec["observation_grid"]}
    seen = {}
    previous_end = None
    for bar in array(facts["bars"]):
        fields(bar, {"open_at", "close_at", "open", "high", "low", "close"})
        span = interval([bar["open_at"], bar["close_at"]])
        if span not in expected or span in seen:
            raise ForecastJournalError("duplicate or out-of-grid factual bar")
        if previous_end is not None and span[0] < previous_end:
            raise ForecastJournalError("factual bars must be chronologically ordered")
        if span[1] > source_as_of:
            raise ForecastJournalError("bar closes after source data_as_of")
        op, high, low, close = (price(bar[key]) for key in ("open", "high", "low", "close"))
        if not low <= min(op, close) <= max(op, close) <= high:
            raise ForecastJournalError("invalid OHLC geometry")
        previous_end = span[1]
        seen[span] = bar
    missing = [raw for span, raw in expected.items() if span not in seen]
    complete = not missing and not limitations
    final = seen.get(interval(spec["observation_grid"][-1]))
    direction = {"status": "NOT_EVALUABLE", "realized_bias": None,
                 "correct": None, "return_bps": None}
    if final is not None and not limitations:
        with localcontext(Context(prec=80, rounding=ROUND_HALF_EVEN)):
            baseline, end = price(spec["reference_price"]), price(final["close"])
            move = (end - baseline) * 10_000
            boundary = price(spec["neutral_band_bps"], zero=True) * baseline
            realized = "BULLISH_USD" if move > boundary else "BEARISH_USD" if move < -boundary else "NEUTRAL"
            direction = {"status": "EVALUATED" if spec["bias"] is not None else "NOT_REQUESTED",
                         "realized_bias": realized,
                         "correct": None if spec["bias"] is None else realized == spec["bias"],
                         "return_bps": str(move / baseline)}
    range_result = {"status": "NOT_REQUESTED" if spec["range"] is None else "NOT_EVALUABLE"}
    if complete and spec["range"] is not None:
        low = min(price(bar["low"]) for bar in facts["bars"])
        high = max(price(bar["high"]) for bar in facts["bars"])
        contained = price(spec["range"]["lower"]) <= low and high <= price(spec["range"]["upper"])
        range_result = {"status": "CONTAINED" if contained else "BREACHED",
                        "observed_low": str(low), "observed_high": str(high)}
    scenarios = []
    for scenario in spec["scenarios"]:
        if complete:
            scenarios.append(_scenario(scenario, facts["bars"], spec["horizon_start"]))
        else:
            scenarios.append({"id": scenario["id"], "status": "NOT_EVALUABLE",
                              "reason": "INCOMPLETE_GRID_OR_SOURCE_QUALITY_LIMITATIONS"})
    return {"schema_version": EVALUATOR_VERSION,
            "baseline_status": spec.get("context", {}).get("baseline", {}).get("status", "LEGACY_CALLER_DECLARED"),
            "coverage": {"status": "COMPLETE_FOR_DECLARED_GRID" if complete else "PARTIAL",
                         "expected_bars": len(expected), "observed_bars": len(seen),
                         "missing_intervals": missing, "quality_limitations": limitations},
            "direction": direction, "range": range_result, "scenarios": scenarios}

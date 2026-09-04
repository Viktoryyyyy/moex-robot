from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Final
from zoneinfo import ZoneInfo

from moex_data import synchronized_live_market_oi_context as live_core


SCHEMA_VERSION: Final[str] = "live_basis_carry_context.v1"
SOURCE_COMPONENT_REF: Final[str] = "components.synchronized_live_market_oi"
STAGE4_CONTRACT_REF: Final[str] = "contracts/datasets/rub_basis_carry_5m.v1.yaml"
STAGE4_CONFIG_REF: Final[str] = "configs/datasets/step4_rub_basis_carry.v1.yaml"
STAGE4_MATERIALIZER_REF: Final[str] = "src/moex_data/analytics/materialize_rub_basis_carry_5m.py"
MOSCOW: Final[ZoneInfo] = ZoneInfo("Europe/Moscow")
MAX_SKEW_SECONDS: Final[int] = live_core.MAX_SKEW_SECONDS
MAX_FRESHNESS_SECONDS: Final[int] = live_core.MAX_FRESHNESS_SECONDS


class LiveBasisCarryContextError(ValueError):
    pass


@dataclass(frozen=True)
class PairSpec:
    key: str
    pair_id: str
    spot: str
    perpetual: str
    front: str
    next: str
    front_divisor: float
    next_divisor: float
    unit: str
    spot_raw_unit: str
    perpetual_raw_unit: str
    front_raw_unit: str
    next_raw_unit: str


PAIR_SPECS: Final[tuple[PairSpec, ...]] = (
    PairSpec(
        "usd_rub", "USD/RUB", "usd_tom", "usdrubf", "si_front", "si_next",
        1000.0, 1000.0, "RUB_per_USD", "RUB_per_USD", "RUB_per_USD",
        "RUB_per_1000_USD", "RUB_per_1000_USD",
    ),
    PairSpec(
        "cny_rub", "CNY/RUB", "cnyrub_tom", "cnyrubf", "cr_front", "cr_next",
        1.0, 1.0, "RUB_per_CNY", "RUB_per_CNY", "RUB_per_CNY",
        "RUB_per_CNY", "RUB_per_CNY",
    ),
)


def _aware_utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise LiveBasisCarryContextError(f"{field} is missing")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LiveBasisCarryContextError(f"{field} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LiveBasisCarryContextError(f"{field} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _finite_number(value: object, *, positive: bool) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    numeric = float(value)
    if not math.isfinite(numeric) or (numeric <= 0 if positive else numeric < 0):
        return None
    return numeric


def _expiry(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _leg(
    instruments: Mapping[str, object],
    logical_id: str,
    *,
    divisor: float,
    raw_unit: str,
    unit: str,
) -> dict[str, object]:
    raw = instruments.get(logical_id)
    base = {
        "logical_id": logical_id,
        "raw_unit": raw_unit,
        "normalized_unit": unit,
        "normalization_divisor": divisor,
    }
    if not isinstance(raw, Mapping):
        return {
            **base,
            "status": "UNAVAILABLE",
            "unavailable_reason": "leg_not_present_in_synchronized_live_component",
            "raw_value": None,
            "normalized_rate": None,
            "timestamp": None,
            "received_at_utc": None,
            "age_seconds": None,
            "stale": None,
            "source_id": None,
            "secid": None,
            "expiry_date": None,
            "expiry_metadata": None,
        }

    value = _finite_number(raw.get("last"), positive=True)
    age = _finite_number(raw.get("age_seconds"), positive=False)
    timestamp: str | None = None
    reason: str | None = None
    if value is None:
        reason = "last_price_missing_or_nonpositive"
    else:
        try:
            timestamp = _iso(_aware_utc(raw.get("timestamp"), f"{logical_id}.timestamp"))
        except LiveBasisCarryContextError:
            reason = "source_timestamp_missing_or_invalid"
    if reason is None and raw.get("stale") is not False:
        reason = "source_leg_stale"
    if reason is None and (age is None or age > MAX_FRESHNESS_SECONDS):
        reason = "source_leg_freshness_exceeds_threshold"
    source_id = str(raw.get("source_id") or "").strip() or None
    secid = str(raw.get("secid") or "").strip() or None
    if reason is None and (source_id is None or secid is None):
        reason = "source_reference_missing"
    normalized = None if value is None else value / divisor
    if normalized is not None and (not math.isfinite(normalized) or normalized <= 0):
        reason = "normalized_rate_invalid"
        normalized = None

    return {
        **base,
        "status": "READY" if reason is None else "UNAVAILABLE",
        "unavailable_reason": reason,
        "raw_value": value,
        "normalized_rate": normalized,
        "timestamp": timestamp,
        "received_at_utc": raw.get("received_at_utc"),
        "age_seconds": age,
        "stale": raw.get("stale"),
        "source_id": source_id,
        "secid": secid,
        "expiry_date": _expiry(raw.get("expiry_date")),
        "expiry_metadata": raw.get("expiry_metadata"),
    }


def _sync(
    observations: Mapping[str, Mapping[str, object]],
    required: tuple[str, ...],
) -> dict[str, object]:
    timestamps = {key: observations[key].get("timestamp") for key in required}
    refs = {
        key: {
            "logical_id": key,
            "secid": observations[key].get("secid"),
            "source_id": observations[key].get("source_id"),
            "received_at_utc": observations[key].get("received_at_utc"),
            "expiry_date": observations[key].get("expiry_date"),
        }
        for key in required
    }
    ages = {key: observations[key].get("age_seconds") for key in required}

    def result(
        *,
        ok: bool,
        reason: str | None,
        data_as_of: str | None = None,
        skew: float | None = None,
        trade_date: str | None = None,
    ) -> dict[str, object]:
        return {
            "synchronized": ok,
            "status": "READY" if ok else "UNAVAILABLE",
            "unavailable_reason": reason,
            "data_as_of": data_as_of,
            "source_timestamps": timestamps,
            "source_refs": refs,
            "freshness": {
                "status": "FRESH" if all(observations[key].get("status") == "READY" for key in required)
                else "UNAVAILABLE",
                "threshold_seconds": MAX_FRESHNESS_SECONDS,
                "age_seconds_by_leg": ages,
            },
            "max_leg_skew_seconds": None if skew is None else round(skew, 3),
            "max_accepted_skew_seconds": MAX_SKEW_SECONDS,
            "source_trade_date": trade_date,
        }

    for key in required:
        if observations[key].get("status") != "READY":
            reason = observations[key].get("unavailable_reason") or "unknown"
            return result(ok=False, reason=f"leg_unavailable:{key}:{reason}")

    parsed = {key: _aware_utc(timestamps[key], f"{key}.timestamp") for key in required}
    dates = {value.astimezone(MOSCOW).date() for value in parsed.values()}
    if len(dates) != 1:
        return result(ok=False, reason="source_trade_dates_mismatch")
    oldest, newest = min(parsed.values()), max(parsed.values())
    skew = (newest - oldest).total_seconds()
    if skew > MAX_SKEW_SECONDS:
        return result(ok=False, reason="source_timestamp_skew_exceeds_threshold", skew=skew)
    return result(
        ok=True,
        reason=None,
        data_as_of=_iso(newest),
        skew=skew,
        trade_date=next(iter(dates)).isoformat(),
    )


def _metric_shell(
    spec: PairSpec,
    stage4_id: str,
    required: tuple[str, ...],
    sync: Mapping[str, object],
    *,
    units: str,
    formula: str,
    formula_ref: str,
) -> dict[str, object]:
    return {
        "metric_id": f"{spec.key}.{stage4_id}",
        "stage4_metric_id": stage4_id,
        "pair_id": spec.pair_id,
        "legs": list(required),
        "value": None,
        "units": units,
        "formula": formula,
        "formula_semantics_ref": formula_ref,
        "live_rate_field": "last",
        "data_as_of": sync.get("data_as_of"),
        "source_timestamps": sync.get("source_timestamps"),
        "source_refs": sync.get("source_refs"),
        "freshness": sync.get("freshness"),
        "synchronized": sync.get("synchronized") is True,
        "max_leg_skew_seconds": sync.get("max_leg_skew_seconds"),
        "max_accepted_skew_seconds": sync.get("max_accepted_skew_seconds"),
        "status": "UNAVAILABLE",
        "unavailable_reason": sync.get("unavailable_reason"),
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
    }


def _basis_metric(
    spec: PairSpec,
    observations: Mapping[str, Mapping[str, object]],
    stage4_id: str,
    comparison: str,
    reference: str,
    kind: str,
) -> dict[str, object]:
    sync = _sync(observations, (comparison, reference))
    if kind == "abs":
        formula = "comparison_rate - reference_rate"
        units = spec.unit
        formula_ref = f"{STAGE4_MATERIALIZER_REF}#build_basis_carry_frame:{stage4_id}"
    elif kind == "bps":
        formula = "((comparison_rate / reference_rate) - 1) * 10000"
        units = "basis_points"
        formula_ref = f"{STAGE4_CONTRACT_REF}#calculation_policy.basis_bps_formula"
    else:
        raise LiveBasisCarryContextError("unsupported basis kind")
    metric = _metric_shell(
        spec, stage4_id, (comparison, reference), sync,
        units=units, formula=formula, formula_ref=formula_ref,
    )
    if sync.get("synchronized") is not True:
        return metric
    comp = observations[comparison]["normalized_rate"]
    ref = observations[reference]["normalized_rate"]
    value = float(comp) - float(ref) if kind == "abs" else ((float(comp) / float(ref)) - 1.0) * 10000.0
    if not math.isfinite(value):
        metric["unavailable_reason"] = "derived_metric_nonfinite"
        return metric
    metric.update(value=value, status="READY", unavailable_reason=None)
    return metric


def _carry_metric(
    spec: PairSpec,
    observations: Mapping[str, Mapping[str, object]],
    stage4_id: str,
    comparison: str,
    reference: str,
    horizon: str,
) -> dict[str, object]:
    formulas = {
        "front_spot": (
            "((front_rate / spot_rate) - 1) * 365 / calendar_days_to_front_expiry",
            f"{STAGE4_CONTRACT_REF}#calculation_policy.front_spot_implied_carry_formula",
        ),
        "next_spot": (
            "((next_rate / spot_rate) - 1) * 365 / calendar_days_to_next_expiry",
            f"{STAGE4_CONTRACT_REF}#calculation_policy.next_spot_implied_carry_formula",
        ),
        "front_next": (
            "((next_rate / front_rate) - 1) * 365 / calendar_days_between_expiries",
            f"{STAGE4_CONTRACT_REF}#calculation_policy.front_next_term_carry_formula",
        ),
    }
    if horizon not in formulas:
        raise LiveBasisCarryContextError("unsupported carry horizon")
    sync = _sync(observations, (comparison, reference))
    formula, formula_ref = formulas[horizon]
    metric = _metric_shell(
        spec, stage4_id, (comparison, reference), sync,
        units="decimal_per_annum", formula=formula, formula_ref=formula_ref,
    )
    metric["annualization_basis_days"] = 365
    metric["carry_semantics"] = "market_implied_carry_proxy"
    if sync.get("synchronized") is not True:
        return metric

    try:
        trade_date = date.fromisoformat(str(sync.get("source_trade_date")))
        front_expiry = date.fromisoformat(str(observations[spec.front].get("expiry_date")))
        next_expiry = date.fromisoformat(str(observations[spec.next].get("expiry_date")))
    except ValueError:
        metric["unavailable_reason"] = "canonical_contract_expiry_metadata_missing"
        return metric

    days_front = (front_expiry - trade_date).days
    days_next = (next_expiry - trade_date).days
    days_term = (next_expiry - front_expiry).days
    metric["expiry_metadata"] = {
        "source_trade_date": trade_date.isoformat(),
        "source_trade_date_semantics": "observed_source_event_timestamp_Europe/Moscow",
        "front_expiry_date": front_expiry.isoformat(),
        "next_expiry_date": next_expiry.isoformat(),
        "calendar_days_to_front_expiry": days_front,
        "calendar_days_to_next_expiry": days_next,
        "calendar_days_between_expiries": days_term,
        "expiry_day_contract_allowed": False,
    }
    if days_front <= 0:
        metric["unavailable_reason"] = "front_contract_not_strictly_after_source_trade_date"
        return metric
    if days_next <= 0:
        metric["unavailable_reason"] = "next_contract_not_strictly_after_source_trade_date"
        return metric
    if days_term <= 0:
        metric["unavailable_reason"] = "next_expiry_not_after_front_expiry"
        return metric

    denominator_days = {
        "front_spot": days_front,
        "next_spot": days_next,
        "front_next": days_term,
    }[horizon]
    comp = float(observations[comparison]["normalized_rate"])
    ref = float(observations[reference]["normalized_rate"])
    value = ((comp / ref) - 1.0) * 365.0 / float(denominator_days)
    if not math.isfinite(value):
        metric["unavailable_reason"] = "derived_metric_nonfinite"
        return metric
    metric.update(value=value, status="READY", unavailable_reason=None)
    return metric


def _pair(spec: PairSpec, instruments: Mapping[str, object]) -> dict[str, object]:
    observations = {
        spec.spot: _leg(instruments, spec.spot, divisor=1.0, raw_unit=spec.spot_raw_unit, unit=spec.unit),
        spec.perpetual: _leg(
            instruments, spec.perpetual, divisor=1.0,
            raw_unit=spec.perpetual_raw_unit, unit=spec.unit,
        ),
        spec.front: _leg(
            instruments, spec.front, divisor=spec.front_divisor,
            raw_unit=spec.front_raw_unit, unit=spec.unit,
        ),
        spec.next: _leg(
            instruments, spec.next, divisor=spec.next_divisor,
            raw_unit=spec.next_raw_unit, unit=spec.unit,
        ),
    }
    metrics: list[dict[str, object]] = []
    for prefix, comparison, reference in (
        ("perpetual_spot_basis", spec.perpetual, spec.spot),
        ("front_spot_basis", spec.front, spec.spot),
        ("next_spot_basis", spec.next, spec.spot),
        ("front_perpetual_basis", spec.front, spec.perpetual),
        ("next_perpetual_basis", spec.next, spec.perpetual),
        ("front_next_spread", spec.next, spec.front),
    ):
        for suffix, kind in (("_abs", "abs"), ("_bps", "bps")):
            metrics.append(
                _basis_metric(
                    spec, observations, prefix + suffix,
                    comparison, reference, kind,
                )
            )
    metrics.extend(
        (
            _carry_metric(
                spec, observations, "front_spot_implied_carry_annualized",
                spec.front, spec.spot, "front_spot",
            ),
            _carry_metric(
                spec, observations, "next_spot_implied_carry_annualized",
                spec.next, spec.spot, "next_spot",
            ),
            _carry_metric(
                spec, observations, "front_next_term_carry_annualized",
                spec.next, spec.front, "front_next",
            ),
        )
    )
    ready = [item for item in metrics if item["status"] == "READY"]
    as_of = [
        _aware_utc(item["data_as_of"], "metric.data_as_of")
        for item in ready if item.get("data_as_of") is not None
    ]
    return {
        "pair_id": spec.pair_id,
        "status": "READY" if len(ready) == len(metrics) else "PARTIAL" if ready else "UNAVAILABLE",
        "normalized_unit": spec.unit,
        "live_spot_available": observations[spec.spot]["status"] == "READY",
        "legs": observations,
        "metrics": metrics,
        "ready_metric_count": len(ready),
        "unavailable_metric_ids": [item["metric_id"] for item in metrics if item["status"] != "READY"],
        "data_as_of": _iso(max(as_of)) if as_of else None,
    }


def build_context(live_snapshot: Mapping[str, object]) -> dict[str, object]:
    if live_snapshot.get("schema_version") != live_core.SCHEMA_VERSION:
        raise LiveBasisCarryContextError("synchronized live market/OI schema mismatch")
    instruments = live_snapshot.get("instruments")
    if not isinstance(instruments, Mapping):
        raise LiveBasisCarryContextError("synchronized live market/OI instruments are missing")

    pairs = {spec.key: _pair(spec, instruments) for spec in PAIR_SPECS}
    ready = sum(int(pair["ready_metric_count"]) for pair in pairs.values())
    total = sum(len(pair["metrics"]) for pair in pairs.values())
    as_of = [
        _aware_utc(pair["data_as_of"], "pair.data_as_of")
        for pair in pairs.values() if pair.get("data_as_of") is not None
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "READY" if ready == total else "PARTIAL" if ready else "UNAVAILABLE",
        "data_as_of": _iso(max(as_of)) if as_of else None,
        "source_component_ref": SOURCE_COMPONENT_REF,
        "source_component_schema_version": live_core.SCHEMA_VERSION,
        "source_snapshot_received_at_utc": live_snapshot.get("snapshot_received_at_utc"),
        "live_input_policy": {
            "rate_field": "last",
            "additional_live_fetch_performed": False,
            "stale_stage3_value_allowed_as_live": False,
            "missing_value_interpreted_as_zero": False,
        },
        "synchronization_policy": {
            "mode": "pairwise_exact_source_event_timestamps_with_bounded_skew",
            "maximum_accepted_skew_seconds": MAX_SKEW_SECONDS,
            "freshness_threshold_seconds": MAX_FRESHNESS_SECONDS,
            "same_observed_source_trade_date_required": True,
            "source_trade_date_semantics": "observed_source_event_timestamp_Europe/Moscow",
            "forward_fill_allowed": False,
            "backward_fill_allowed": False,
            "asof_join_allowed": False,
            "nearest_join_allowed": False,
            "calendar_inference_allowed": False,
            "stale_fresh_mix_allowed": False,
            "fail_closed": True,
        },
        "stage4_semantics": {
            "contract_ref": STAGE4_CONTRACT_REF,
            "config_ref": STAGE4_CONFIG_REF,
            "materializer_ref": STAGE4_MATERIALIZER_REF,
            "basis_bps_formula": "((comparison_rate / reference_rate) - 1) * 10000",
            "annualization_basis_days": 365,
            "front_spot_implied_carry_formula": (
                "((front_rate / spot_rate) - 1) * 365 / calendar_days_to_front_expiry"
            ),
            "next_spot_implied_carry_formula": (
                "((next_rate / spot_rate) - 1) * 365 / calendar_days_to_next_expiry"
            ),
            "front_next_term_carry_formula": (
                "((next_rate / front_rate) - 1) * 365 / calendar_days_between_expiries"
            ),
            "metric_semantics": "market_implied_carry_proxy",
            "fair_value_model_claimed": False,
            "expiry_day_contract_allowed_for_annualized_carry": False,
        },
        "live_leg_availability": {
            "available": [
                key for key in live_core.LOGICAL_ORDER
                if isinstance(instruments.get(key), Mapping)
            ],
            "unavailable": ["usd_tom"] if not isinstance(instruments.get("usd_tom"), Mapping) else [],
        },
        "pairs": pairs,
        "ready_metric_count": ready,
        "total_metric_count": total,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
    }

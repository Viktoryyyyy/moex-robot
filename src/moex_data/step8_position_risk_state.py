from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime, timezone
from decimal import (
    MAX_EMAX,
    MIN_EMIN,
    Clamped,
    Decimal,
    DecimalException,
    Inexact,
    InvalidOperation,
    Overflow,
    Underflow,
    localcontext,
)
from pathlib import Path
from typing import Any, Mapping, Sequence


INPUT_SCHEMA_VERSION = "step8_position_risk_input.v1"
OUTPUT_SCHEMA_VERSION = "step8_position_risk_state.v1"
SCENARIO_KEYS = (
    "usd_rub_minus_5",
    "usd_rub_minus_3",
    "usd_rub_minus_1",
    "usd_rub_plus_1",
    "usd_rub_plus_3",
    "usd_rub_plus_5",
)
SOURCE_MODES = {"manual", "read_only_broker_export"}
_SAFE_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*\Z")
_MAX_EXACT_AGGREGATE_PRECISION_DIGITS = 100_000


class Step8PositionRiskError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step8PositionRiskError(message)


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail(field + " must be object")
    return value


def _list(value: object, field: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(field + " must be array")
    return value


def _expect_keys(value: Mapping[str, Any], required: set[str], optional: set[str], field: str) -> None:
    keys = set(value)
    missing = required - keys
    unknown = keys - required - optional
    if missing:
        _fail(field + " missing keys: " + ",".join(sorted(missing)))
    if unknown:
        _fail(field + " unknown keys: " + ",".join(sorted(unknown)))


def _text(value: object, field: str) -> str:
    if not isinstance(value, str):
        _fail(field + " must be string")
    text = value.strip()
    if not text:
        _fail(field + " must be non-empty string")
    return text


def _safe_token(value: object, field: str) -> str:
    if not isinstance(value, str):
        _fail(field + " must be string")
    if not value:
        _fail(field + " must be non-empty string")
    if value != value.strip():
        _fail(field + " must not contain surrounding whitespace")
    if _SAFE_TOKEN_RE.fullmatch(value) is None:
        _fail(field + " must be explicit safe token")
    return value


def _int(value: object, field: str, *, minimum: int | None = None, nonzero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(field + " must be integer")
    if minimum is not None and value < minimum:
        _fail(field + " below minimum")
    if nonzero and value == 0:
        _fail(field + " must be non-zero")
    return value


def _decimal(value: object, field: str, *, minimum: Decimal | None = None, positive: bool = False) -> Decimal:
    if isinstance(value, (bool, float)) or value is None or not isinstance(value, (str, int, Decimal)):
        _fail(field + " must be finite decimal without binary float coercion")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise Step8PositionRiskError(field + " must be finite decimal") from exc
    if not result.is_finite():
        _fail(field + " must be finite decimal")
    if minimum is not None and result < minimum:
        _fail(field + " below minimum")
    if positive and result <= 0:
        _fail(field + " must be positive")
    return result


def _sum_exact(values: Sequence[Decimal]) -> Decimal:
    items = [item for item in values if item != 0]
    if not items:
        return Decimal("0")
    minimum_exponent = min(int(item.as_tuple().exponent) for item in items)
    maximum_adjusted = max(item.adjusted() for item in items)
    carry_digits = len(str(len(items))) + 1
    span_precision = maximum_adjusted - minimum_exponent + 1 + carry_digits
    subnormal_precision = max(1, MIN_EMIN - minimum_exponent + 1)
    precision = max(1, span_precision, subnormal_precision)
    if precision > _MAX_EXACT_AGGREGATE_PRECISION_DIGITS:
        _fail("decimal aggregate exceeds exact resource-safety precision bound")
    try:
        with localcontext() as context:
            context.prec = precision
            context.Emax = MAX_EMAX
            context.Emin = MIN_EMIN
            context.clamp = 0
            for signal in (Clamped, Inexact, Underflow, Overflow):
                context.traps[signal] = True
            total = Decimal("0")
            for item in items:
                total += item
            return total
    except (DecimalException, ValueError) as exc:
        raise Step8PositionRiskError(
            "decimal aggregate exceeds exact Decimal representability"
        ) from exc


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    tuple_value = value.as_tuple()
    exponent = int(tuple_value.exponent)
    digits = list(tuple_value.digits)
    while exponent < 0 and digits and digits[-1] == 0:
        digits.pop()
        exponent += 1
    canonical = Decimal((tuple_value.sign, tuple(digits), exponent))
    return str(canonical)


def _reject_json_constant(token: str) -> None:
    _fail("JSON numeric constant must be finite: " + token)


def _reject_duplicate_json_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail("duplicate JSON object member: " + key)
        result[key] = value
    return result


def _utc_timestamp(value: object, field: str) -> str:
    text = _text(value, field)
    if re.search(r"[+-]\d{2}:\d{2}\Z", text) and not text.endswith("+00:00"):
        _fail(field + " must be UTC timestamp")
    match = re.fullmatch(
        r"(?P<date>\d{4}-\d{2}-\d{2})T"
        r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
        r"(?P<fraction>\.\d+)?(?P<tz>Z|\+00:00)",
        text,
    )
    if match is None:
        _fail(field + " must be ISO-8601 UTC timestamp")
    try:
        datetime.fromisoformat(
            f"{match.group('date')}T{match.group('hour')}:{match.group('minute')}:{match.group('second')}+00:00"
        )
    except ValueError as exc:
        raise Step8PositionRiskError(field + " must be ISO-8601 UTC timestamp") from exc
    fraction = match.group("fraction") or ""
    return (
        f"{match.group('date')}T{match.group('hour')}:{match.group('minute')}:{match.group('second')}"
        f"{fraction}+00:00"
    )


def _iso_date_or_none(value: object, field: str) -> str | None:
    if value is None:
        return None
    text = _text(value, field)
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise Step8PositionRiskError(field + " must be YYYY-MM-DD or null") from exc


def _position(value: object, index: int) -> dict[str, Any]:
    field = f"positions[{index}]"
    raw = _mapping(value, field)
    _expect_keys(
        raw,
        {
            "position_id",
            "instrument_id",
            "expiry",
            "contracts",
            "average_price",
            "fills",
            "commission_total_rub",
            "realized_pnl_rub",
            "unrealized_pnl_rub",
            "horizon",
            "invalidation",
            "protective_stop",
            "tranches",
        },
        {"expiry_not_applicable_reason"},
        field,
    )
    expiry = _iso_date_or_none(raw["expiry"], field + ".expiry")
    expiry_reason = raw.get("expiry_not_applicable_reason")
    if expiry is None:
        expiry_reason = _text(expiry_reason, field + ".expiry_not_applicable_reason")
    elif expiry_reason not in (None, ""):
        _fail(field + ".expiry_not_applicable_reason must be null when expiry is set")

    contracts = _int(raw["contracts"], field + ".contracts", nonzero=True)
    average_price = _decimal(raw["average_price"], field + ".average_price", positive=True)

    fills_out: list[dict[str, Any]] = []
    fill_ids: set[str] = set()
    fill_contract_sum = 0
    fill_commissions: list[Decimal] = []
    for fill_index, fill_value in enumerate(_list(raw["fills"], field + ".fills")):
        fill_field = f"{field}.fills[{fill_index}]"
        fill = _mapping(fill_value, fill_field)
        _expect_keys(fill, {"fill_id", "ts_utc", "contracts", "price", "commission_rub"}, set(), fill_field)
        fill_id = _safe_token(fill["fill_id"], fill_field + ".fill_id")
        if fill_id in fill_ids:
            _fail(field + " duplicate fill_id")
        fill_ids.add(fill_id)
        fill_contracts = _int(fill["contracts"], fill_field + ".contracts", nonzero=True)
        commission = _decimal(fill["commission_rub"], fill_field + ".commission_rub", minimum=Decimal("0"))
        fill_contract_sum += fill_contracts
        fill_commissions.append(commission)
        fills_out.append(
            {
                "fill_id": fill_id,
                "ts_utc": _utc_timestamp(fill["ts_utc"], fill_field + ".ts_utc"),
                "contracts": fill_contracts,
                "price": _decimal_text(_decimal(fill["price"], fill_field + ".price", positive=True)),
                "commission_rub": _decimal_text(commission),
            }
        )

    fill_commission_sum = _sum_exact(fill_commissions)
    commission_total = _decimal(
        raw["commission_total_rub"],
        field + ".commission_total_rub",
        minimum=Decimal("0"),
    )
    if fill_contract_sum != contracts:
        _fail(field + ".fills contracts do not reconcile to position contracts")
    if fill_commission_sum != commission_total:
        _fail(field + ".fills commissions do not reconcile to commission_total_rub")

    invalidation_raw = _mapping(raw["invalidation"], field + ".invalidation")
    _expect_keys(invalidation_raw, {"level", "loss_rub"}, set(), field + ".invalidation")
    invalidation = {
        "level": _decimal_text(
            _decimal(invalidation_raw["level"], field + ".invalidation.level", positive=True)
        ),
        "loss_rub": _decimal_text(
            _decimal(
                invalidation_raw["loss_rub"],
                field + ".invalidation.loss_rub",
                minimum=Decimal("0"),
            )
        ),
    }

    stop_value = raw["protective_stop"]
    protective_stop: dict[str, str] | None
    if stop_value is None:
        protective_stop = None
    else:
        stop_raw = _mapping(stop_value, field + ".protective_stop")
        _expect_keys(stop_raw, {"level"}, set(), field + ".protective_stop")
        protective_stop = {
            "level": _decimal_text(
                _decimal(stop_raw["level"], field + ".protective_stop.level", positive=True)
            )
        }

    tranches_out: list[dict[str, Any]] = []
    for tranche_index, tranche_value in enumerate(_list(raw["tranches"], field + ".tranches")):
        tranche_field = f"{field}.tranches[{tranche_index}]"
        tranche = _mapping(tranche_value, tranche_field)
        _expect_keys(tranche, {"level", "contracts_delta"}, set(), tranche_field)
        tranches_out.append(
            {
                "level": _decimal_text(
                    _decimal(tranche["level"], tranche_field + ".level", positive=True)
                ),
                "contracts_delta": _int(
                    tranche["contracts_delta"],
                    tranche_field + ".contracts_delta",
                    nonzero=True,
                ),
            }
        )

    return {
        "position_id": _safe_token(raw["position_id"], field + ".position_id"),
        "instrument_id": _safe_token(raw["instrument_id"], field + ".instrument_id"),
        "expiry": expiry,
        "expiry_not_applicable_reason": expiry_reason if expiry is None else None,
        "contracts": contracts,
        "average_price": _decimal_text(average_price),
        "fills": fills_out,
        "fill_contract_sum": fill_contract_sum,
        "fill_commission_sum_rub": _decimal_text(fill_commission_sum),
        "commission_total_rub": _decimal_text(commission_total),
        "realized_pnl_rub": _decimal_text(
            _decimal(raw["realized_pnl_rub"], field + ".realized_pnl_rub")
        ),
        "unrealized_pnl_rub": _decimal_text(
            _decimal(raw["unrealized_pnl_rub"], field + ".unrealized_pnl_rub")
        ),
        "horizon": _text(raw["horizon"], field + ".horizon"),
        "invalidation": invalidation,
        "protective_stop": protective_stop,
        "tranches": tranches_out,
    }


def _scenario_grid(value: object) -> dict[str, Any]:
    raw = _mapping(value, "scenario_pnl_rub")
    _expect_keys(raw, set(SCENARIO_KEYS) | {"gap"}, set(), "scenario_pnl_rub")
    result = {
        key: _decimal_text(_decimal(raw[key], "scenario_pnl_rub." + key))
        for key in SCENARIO_KEYS
    }
    gap = _mapping(raw["gap"], "scenario_pnl_rub.gap")
    _expect_keys(gap, {"usd_rub_move", "pnl_rub"}, set(), "scenario_pnl_rub.gap")
    move = _decimal(gap["usd_rub_move"], "scenario_pnl_rub.gap.usd_rub_move")
    if move == 0:
        _fail("scenario_pnl_rub.gap.usd_rub_move must be non-zero")
    result["gap"] = {
        "usd_rub_move": _decimal_text(move),
        "pnl_rub": _decimal_text(_decimal(gap["pnl_rub"], "scenario_pnl_rub.gap.pnl_rub")),
    }
    return result


def build_position_risk_state(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(payload, "payload")
    _expect_keys(
        raw,
        {
            "schema_version",
            "snapshot_id",
            "as_of_ts_utc",
            "source",
            "account",
            "positions",
            "scenario_pnl_rub",
        },
        set(),
        "payload",
    )
    if raw["schema_version"] != INPUT_SCHEMA_VERSION:
        _fail("schema_version mismatch")

    source = _mapping(raw["source"], "source")
    _expect_keys(source, {"mode", "reference"}, set(), "source")
    source_mode = _text(source["mode"], "source.mode")
    if source_mode not in SOURCE_MODES:
        _fail("source.mode unsupported")

    account = _mapping(raw["account"], "account")
    _expect_keys(
        account,
        {
            "currency",
            "free_funds_rub",
            "current_initial_margin_rub",
            "variation_margin_rub",
            "liquidity_buffer_rub",
            "max_total_contracts",
            "max_allowed_loss_rub",
        },
        set(),
        "account",
    )
    if account["currency"] != "RUB":
        _fail("account.currency must be RUB")
    max_total_contracts = _int(
        account["max_total_contracts"],
        "account.max_total_contracts",
        minimum=0,
    )
    max_allowed_loss = _decimal(
        account["max_allowed_loss_rub"],
        "account.max_allowed_loss_rub",
        minimum=Decimal("0"),
    )
    liquidity_buffer = _decimal(account["liquidity_buffer_rub"], "account.liquidity_buffer_rub")

    positions_out: list[dict[str, Any]] = []
    position_ids: set[str] = set()
    position_keys: set[tuple[str, str | None]] = set()
    current_gross = 0
    conservative_additional_gross = 0
    invalidation_losses: list[Decimal] = []
    commissions: list[Decimal] = []
    realized_pnls: list[Decimal] = []
    unrealized_pnls: list[Decimal] = []
    for index, value in enumerate(_list(raw["positions"], "positions")):
        position = _position(value, index)
        if position["position_id"] in position_ids:
            _fail("duplicate position_id")
        position_ids.add(position["position_id"])
        position_key = (str(position["instrument_id"]), position["expiry"])
        if position_key in position_keys:
            _fail("duplicate instrument_id/expiry position")
        position_keys.add(position_key)
        current_gross += abs(int(position["contracts"]))
        conservative_additional_gross += sum(
            abs(int(item["contracts_delta"])) for item in position["tranches"]
        )
        invalidation_losses.append(Decimal(str(position["invalidation"]["loss_rub"])))
        commissions.append(Decimal(str(position["commission_total_rub"])))
        realized_pnls.append(Decimal(str(position["realized_pnl_rub"])))
        unrealized_pnls.append(Decimal(str(position["unrealized_pnl_rub"])))
        positions_out.append(position)

    total_invalidation_loss = _sum_exact(invalidation_losses)
    total_commission = _sum_exact(commissions)
    total_realized = _sum_exact(realized_pnls)
    total_unrealized = _sum_exact(unrealized_pnls)

    scenarios = _scenario_grid(raw["scenario_pnl_rub"])
    scenario_values = [Decimal(str(scenarios[key])) for key in SCENARIO_KEYS]
    scenario_values.append(Decimal(str(scenarios["gap"]["pnl_rub"])))
    worst_scenario_pnl = min(scenario_values)
    best_scenario_pnl = max(scenario_values)
    worst_scenario_loss = max(Decimal("0"), worst_scenario_pnl.copy_negate())

    planned_conservative_gross = current_gross + conservative_additional_gross
    current_headroom = max_total_contracts - current_gross
    planned_headroom = max_total_contracts - planned_conservative_gross
    invalidation_headroom = _sum_exact((max_allowed_loss, total_invalidation_loss.copy_negate()))
    scenario_headroom = _sum_exact((max_allowed_loss, worst_scenario_loss.copy_negate()))

    return {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "snapshot_id": _safe_token(raw["snapshot_id"], "snapshot_id"),
        "as_of_ts_utc": _utc_timestamp(raw["as_of_ts_utc"], "as_of_ts_utc"),
        "source": {
            "mode": source_mode,
            "reference": _text(source["reference"], "source.reference"),
        },
        "account": {
            "currency": "RUB",
            "free_funds_rub": _decimal_text(
                _decimal(account["free_funds_rub"], "account.free_funds_rub")
            ),
            "current_initial_margin_rub": _decimal_text(
                _decimal(
                    account["current_initial_margin_rub"],
                    "account.current_initial_margin_rub",
                    minimum=Decimal("0"),
                )
            ),
            "variation_margin_rub": _decimal_text(
                _decimal(account["variation_margin_rub"], "account.variation_margin_rub")
            ),
            "liquidity_buffer_rub": _decimal_text(liquidity_buffer),
            "max_total_contracts": max_total_contracts,
            "max_allowed_loss_rub": _decimal_text(max_allowed_loss),
        },
        "positions": positions_out,
        "scenario_pnl_rub": scenarios,
        "derived": {
            "current_gross_contracts": current_gross,
            "current_contract_headroom": current_headroom,
            "current_contract_limit_breach": current_headroom < 0,
            "planned_conservative_additional_gross_contracts": conservative_additional_gross,
            "planned_conservative_gross_contracts": planned_conservative_gross,
            "planned_conservative_contract_headroom": planned_headroom,
            "planned_conservative_contract_limit_breach": planned_headroom < 0,
            "total_invalidation_loss_rub": _decimal_text(total_invalidation_loss),
            "invalidation_loss_headroom_rub": _decimal_text(invalidation_headroom),
            "invalidation_loss_limit_breach": invalidation_headroom < 0,
            "worst_supplied_scenario_pnl_rub": _decimal_text(worst_scenario_pnl),
            "best_supplied_scenario_pnl_rub": _decimal_text(best_scenario_pnl),
            "worst_supplied_scenario_loss_rub": _decimal_text(worst_scenario_loss),
            "scenario_loss_headroom_rub": _decimal_text(scenario_headroom),
            "scenario_loss_limit_breach": scenario_headroom < 0,
            "liquidity_buffer_breach": liquidity_buffer < 0,
            "total_commission_rub": _decimal_text(total_commission),
            "total_realized_pnl_rub": _decimal_text(total_realized),
            "total_unrealized_pnl_rub": _decimal_text(total_unrealized),
        },
        "calculation_policy": {
            "broker_write_access_used": False,
            "automatic_order_placement_allowed": False,
            "automatic_position_sizing_allowed": False,
            "trade_recommendation_generated": False,
            "stop_or_invalidation_generation_allowed": False,
            "tranche_generation_allowed": False,
            "realized_pnl_recomputed": False,
            "unrealized_pnl_recomputed": False,
            "invalidation_loss_recomputed_from_price": False,
            "scenario_pnl_recomputed_from_market_move": False,
            "supplied_pnl_fields_are_external_evidence": True,
            "instrument_payout_mapping_required_before_pnl_recalculation": True,
        },
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and aggregate explicit Stage 8 position/risk state."
    )
    parser.add_argument("--input-json", required=True)
    parser.add_argument("--output-json")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = json.loads(
            Path(args.input_json).read_text(encoding="utf-8"),
            parse_float=Decimal,
            parse_constant=_reject_json_constant,
            object_pairs_hook=_reject_duplicate_json_members,
        )
        result = build_position_risk_state(payload)
        encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        if args.output_json:
            output = Path(args.output_json)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(encoded, encoding="utf-8")
        else:
            print(encoded, end="")
    except Exception as exc:
        print(
            json.dumps(
                {
                    "project": "MOEX_Bot",
                    "step": 8,
                    "status": "position_risk_failed",
                    "error": str(exc),
                },
                ensure_ascii=False,
            )
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
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
    text = str(value or "").strip()
    if not text:
        _fail(field + " must be non-empty string")
    return text


def _safe_token(value: object, field: str) -> str:
    text = _text(value, field)
    if text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be explicit safe token")
    return text


def _int(value: object, field: str, *, minimum: int | None = None, nonzero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(field + " must be integer")
    if minimum is not None and value < minimum:
        _fail(field + " below minimum")
    if nonzero and value == 0:
        _fail(field + " must be non-zero")
    return value


def _decimal(value: object, field: str, *, minimum: Decimal | None = None, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        _fail(field + " must be finite decimal")
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


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _utc_timestamp(value: object, field: str) -> str:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise Step8PositionRiskError(field + " must be ISO-8601 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        _fail(field + " must be UTC timestamp")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


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
            "position_id", "instrument_id", "expiry", "contracts", "average_price", "fills",
            "commission_total_rub", "realized_pnl_rub", "unrealized_pnl_rub", "horizon",
            "invalidation", "protective_stop", "tranches",
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
    fill_commission_sum = Decimal("0")
    for fill_index, fill_value in enumerate(_list(raw["fills"], field + ".fills")):
        fill_field = f"{field}.fills[{fill_index}]"
        fill = _mapping(fill_value, fill_field)
        _expect_keys(fill, {"fill_id", "ts_utc", "contracts", "price", "commission_rub"}, set(), fill_field)
        fill_id = _safe_token(fill["fill_id"], fill_field + ".fill_id")
        if fill_id in fill_ids:
            _fail(field + " duplicate fill_id")
        fill_ids.add(fill_id)
        commission = _decimal(fill["commission_rub"], fill_field + ".commission_rub", minimum=Decimal("0"))
        fill_commission_sum += commission
        fills_out.append({
            "fill_id": fill_id,
            "ts_utc": _utc_timestamp(fill["ts_utc"], fill_field + ".ts_utc"),
            "contracts": _int(fill["contracts"], fill_field + ".contracts", nonzero=True),
            "price": _decimal_text(_decimal(fill["price"], fill_field + ".price", positive=True)),
            "commission_rub": _decimal_text(commission),
        })

    invalidation_raw = _mapping(raw["invalidation"], field + ".invalidation")
    _expect_keys(invalidation_raw, {"level", "loss_rub"}, set(), field + ".invalidation")
    invalidation = {
        "level": _decimal_text(_decimal(invalidation_raw["level"], field + ".invalidation.level", positive=True)),
        "loss_rub": _decimal_text(_decimal(invalidation_raw["loss_rub"], field + ".invalidation.loss_rub", minimum=Decimal("0"))),
    }

    stop_value = raw["protective_stop"]
    protective_stop: dict[str, str] | None
    if stop_value is None:
        protective_stop = None
    else:
        stop_raw = _mapping(stop_value, field + ".protective_stop")
        _expect_keys(stop_raw, {"level"}, set(), field + ".protective_stop")
        protective_stop = {"level": _decimal_text(_decimal(stop_raw["level"], field + ".protective_stop.level", positive=True))}

    tranches_out: list[dict[str, Any]] = []
    for tranche_index, tranche_value in enumerate(_list(raw["tranches"], field + ".tranches")):
        tranche_field = f"{field}.tranches[{tranche_index}]"
        tranche = _mapping(tranche_value, tranche_field)
        _expect_keys(tranche, {"level", "contracts_delta"}, set(), tranche_field)
        tranches_out.append({
            "level": _decimal_text(_decimal(tranche["level"], tranche_field + ".level", positive=True)),
            "contracts_delta": _int(tranche["contracts_delta"], tranche_field + ".contracts_delta", nonzero=True),
        })

    return {
        "position_id": _safe_token(raw["position_id"], field + ".position_id"),
        "instrument_id": _safe_token(raw["instrument_id"], field + ".instrument_id"),
        "expiry": expiry,
        "expiry_not_applicable_reason": expiry_reason if expiry is None else None,
        "contracts": contracts,
        "average_price": _decimal_text(average_price),
        "fills": fills_out,
        "fill_commission_sum_rub": _decimal_text(fill_commission_sum),
        "commission_total_rub": _decimal_text(_decimal(raw["commission_total_rub"], field + ".commission_total_rub", minimum=Decimal("0"))),
        "realized_pnl_rub": _decimal_text(_decimal(raw["realized_pnl_rub"], field + ".realized_pnl_rub")),
        "unrealized_pnl_rub": _decimal_text(_decimal(raw["unrealized_pnl_rub"], field + ".unrealized_pnl_rub")),
        "horizon": _text(raw["horizon"], field + ".horizon"),
        "invalidation": invalidation,
        "protective_stop": protective_stop,
        "tranches": tranches_out,
    }


def _scenario_grid(value: object) -> dict[str, Any]:
    raw = _mapping(value, "scenario_pnl_rub")
    _expect_keys(raw, set(SCENARIO_KEYS) | {"gap"}, set(), "scenario_pnl_rub")
    result = {key: _decimal_text(_decimal(raw[key], "scenario_pnl_rub." + key)) for key in SCENARIO_KEYS}
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
    _expect_keys(raw, {"schema_version", "snapshot_id", "as_of_ts_utc", "source", "account", "positions", "scenario_pnl_rub"}, set(), "payload")
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
            "currency", "free_funds_rub", "current_initial_margin_rub", "variation_margin_rub",
            "liquidity_buffer_rub", "max_total_contracts", "max_allowed_loss_rub",
        },
        set(),
        "account",
    )
    if account["currency"] != "RUB":
        _fail("account.currency must be RUB")
    max_total_contracts = _int(account["max_total_contracts"], "account.max_total_contracts", minimum=0)
    max_allowed_loss = _decimal(account["max_allowed_loss_rub"], "account.max_allowed_loss_rub", minimum=Decimal("0"))
    liquidity_buffer = _decimal(account["liquidity_buffer_rub"], "account.liquidity_buffer_rub")

    positions_out: list[dict[str, Any]] = []
    position_ids: set[str] = set()
    position_keys: set[tuple[str, str | None]] = set()
    current_gross = 0
    conservative_additional_gross = 0
    total_invalidation_loss = Decimal("0")
    total_commission = Decimal("0")
    total_realized = Decimal("0")
    total_unrealized = Decimal("0")
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
        conservative_additional_gross += sum(abs(int(item["contracts_delta"])) for item in position["tranches"])
        total_invalidation_loss += Decimal(str(position["invalidation"]["loss_rub"]))
        total_commission += Decimal(str(position["commission_total_rub"]))
        total_realized += Decimal(str(position["realized_pnl_rub"]))
        total_unrealized += Decimal(str(position["unrealized_pnl_rub"]))
        positions_out.append(position)

    scenarios = _scenario_grid(raw["scenario_pnl_rub"])
    scenario_values = [Decimal(str(scenarios[key])) for key in SCENARIO_KEYS]
    scenario_values.append(Decimal(str(scenarios["gap"]["pnl_rub"])))
    worst_scenario_pnl = min(scenario_values) if scenario_values else Decimal("0")
    worst_scenario_loss = max(Decimal("0"), -worst_scenario_pnl)

    planned_conservative_gross = current_gross + conservative_additional_gross
    current_headroom = max_total_contracts - current_gross
    planned_headroom = max_total_contracts - planned_conservative_gross
    invalidation_headroom = max_allowed_loss - total_invalidation_loss
    scenario_headroom = max_allowed_loss - worst_scenario_loss

    return {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "snapshot_id": _safe_token(raw["snapshot_id"], "snapshot_id"),
        "as_of_ts_utc": _utc_timestamp(raw["as_of_ts_utc"], "as_of_ts_utc"),
        "source": {"mode": source_mode, "reference": _text(source["reference"], "source.reference")},
        "account": {
            "currency": "RUB",
            "free_funds_rub": _decimal_text(_decimal(account["free_funds_rub"], "account.free_funds_rub")),
            "current_initial_margin_rub": _decimal_text(_decimal(account["current_initial_margin_rub"], "account.current_initial_margin_rub", minimum=Decimal("0"))),
            "variation_margin_rub": _decimal_text(_decimal(account["variation_margin_rub"], "account.variation_margin_rub")),
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
            "realized_pnl_recomputed": False,
            "unrealized_pnl_recomputed": False,
            "invalidation_loss_recomputed_from_price": False,
            "scenario_pnl_recomputed_from_market_move": False,
            "supplied_pnl_fields_are_external_evidence": True,
            "instrument_payout_mapping_required_before_pnl_recalculation": True,
        },
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate and aggregate explicit Stage 8 position/risk state.")
    parser.add_argument("--input-json", required=True)
    parser.add_argument("--output-json")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
        result = build_position_risk_state(payload)
        encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        if args.output_json:
            output = Path(args.output_json)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(encoded, encoding="utf-8")
        else:
            print(encoded, end="")
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 8, "status": "position_risk_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

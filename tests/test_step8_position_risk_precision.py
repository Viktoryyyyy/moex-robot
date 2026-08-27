from __future__ import annotations

import json
from pathlib import Path

import pytest

from moex_data.step8_position_risk_state import build_position_risk_state, main


def _payload() -> dict[str, object]:
    return {
        "schema_version": "step8_position_risk_input.v1",
        "snapshot_id": "precision_regression_v1",
        "as_of_ts_utc": "2026-08-27T10:00:00Z",
        "source": {"mode": "manual", "reference": "precision_fixture"},
        "account": {
            "currency": "RUB",
            "free_funds_rub": "0",
            "current_initial_margin_rub": "0",
            "variation_margin_rub": "0",
            "liquidity_buffer_rub": "0",
            "max_total_contracts": 1,
            "max_allowed_loss_rub": "10000000000000000000000000000",
        },
        "positions": [
            {
                "position_id": "precision_position",
                "instrument_id": "precision_instrument",
                "expiry": None,
                "expiry_not_applicable_reason": "fixture",
                "contracts": 1,
                "average_price": "1",
                "fills": [
                    {
                        "fill_id": "precision_fill",
                        "ts_utc": "2026-08-27T09:00:00Z",
                        "contracts": 1,
                        "price": "1",
                        "commission_rub": "0",
                    }
                ],
                "commission_total_rub": "0",
                "realized_pnl_rub": "0",
                "unrealized_pnl_rub": "0",
                "horizon": "fixture",
                "invalidation": {
                    "level": "1",
                    "loss_rub": "9999999999999999999999999999.9",
                },
                "protective_stop": None,
                "tranches": [],
            }
        ],
        "scenario_pnl_rub": {
            "usd_rub_minus_5": "0",
            "usd_rub_minus_3": "0",
            "usd_rub_minus_1": "0",
            "usd_rub_plus_1": "0",
            "usd_rub_plus_3": "0",
            "usd_rub_plus_5": "0",
            "gap": {"usd_rub_move": "-1", "pnl_rub": "0"},
        },
    }


def test_high_precision_decimals_are_preserved_without_context_rounding() -> None:
    result = build_position_risk_state(_payload())
    assert result["derived"]["total_invalidation_loss_rub"] == "9999999999999999999999999999.9"
    assert result["derived"]["invalidation_loss_headroom_rub"] == "0.1"
    assert result["derived"]["invalidation_loss_limit_breach"] is False


def test_cli_parses_json_decimal_tokens_without_binary_float_rounding(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = _payload()
    raw = json.dumps(payload, ensure_ascii=False).replace(
        '"max_allowed_loss_rub": "10000000000000000000000000000"',
        '"max_allowed_loss_rub": 9007199254740993.1',
    )
    input_path = tmp_path / "input.json"
    input_path.write_text(raw, encoding="utf-8")

    assert main(["--input-json", str(input_path)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["account"]["max_allowed_loss_rub"] == "9007199254740993.1"

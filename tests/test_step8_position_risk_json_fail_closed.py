from __future__ import annotations

import json
from pathlib import Path

import pytest

from moex_data.step8_position_risk_state import Step8PositionRiskError, build_position_risk_state, main


def _payload() -> dict[str, object]:
    return {
        "schema_version": "step8_position_risk_input.v1",
        "snapshot_id": "json_edge_v1",
        "as_of_ts_utc": "2026-08-27T10:00:00.000000001Z",
        "source": {"mode": "manual", "reference": "json_edge_fixture"},
        "account": {
            "currency": "RUB",
            "free_funds_rub": "0",
            "current_initial_margin_rub": "0",
            "variation_margin_rub": "0",
            "liquidity_buffer_rub": "0",
            "max_total_contracts": 1,
            "max_allowed_loss_rub": "0",
        },
        "positions": [
            {
                "position_id": "json_edge_position",
                "instrument_id": "json_edge_instrument",
                "expiry": None,
                "expiry_not_applicable_reason": "fixture",
                "contracts": 1,
                "average_price": "1",
                "fills": [
                    {
                        "fill_id": "json_edge_fill",
                        "ts_utc": "2026-08-27T09:00:00.123456789Z",
                        "contracts": 1,
                        "price": "1",
                        "commission_rub": "0",
                    }
                ],
                "commission_total_rub": "0",
                "realized_pnl_rub": "0",
                "unrealized_pnl_rub": "0",
                "horizon": "fixture",
                "invalidation": {"level": "1", "loss_rub": "0"},
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


def test_sub_microsecond_timestamp_digits_are_preserved_exactly() -> None:
    result = build_position_risk_state(_payload())
    assert result["as_of_ts_utc"] == "2026-08-27T10:00:00.000000001+00:00"
    assert result["positions"][0]["fills"][0]["ts_utc"] == "2026-08-27T09:00:00.123456789+00:00"


def test_non_utc_offset_still_fails_closed() -> None:
    payload = _payload()
    payload["as_of_ts_utc"] = "2026-08-27T13:00:00+03:00"
    with pytest.raises(Step8PositionRiskError, match="must be UTC"):
        build_position_risk_state(payload)


def test_cli_rejects_duplicate_json_object_members(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    input_path = tmp_path / "duplicate.json"
    input_path.write_text(
        '{"schema_version":"step8_position_risk_input.v1","schema_version":"conflict"}',
        encoding="utf-8",
    )

    assert main(["--input-json", str(input_path)]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "position_risk_failed"
    assert output["error"] == "duplicate JSON object member: schema_version"

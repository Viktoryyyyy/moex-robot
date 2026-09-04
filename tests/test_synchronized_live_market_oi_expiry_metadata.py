from __future__ import annotations

from moex_data import synchronized_live_market_oi_context as core
from moex_data import synchronized_live_market_oi_context_apim as apim


def test_expiry_metadata_is_reused_from_same_rfud_securities_payload() -> None:
    snapshot = {
        "instruments": {
            "si_front": {"secid": "SiU6"},
            "si_next": {"secid": "SiZ6"},
            "cr_front": {"secid": "CRU6"},
            "cr_next": {"secid": "CRZ6"},
        },
        "provenance": {
            "forts": {
                "source_url": "https://apim.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities.json"
            }
        },
    }
    payload = {
        "securities": {
            "columns": list(core.FUTURES_SECURITY_COLUMNS),
            "data": [
                ["SiU6", "RFUD", "2026-09-17", 1.0, 1.0],
                ["SiZ6", "RFUD", "2026-12-17", 1.0, 1.0],
                ["CRU6", "RFUD", "2026-09-17", 0.001, 1.0],
                ["CRZ6", "RFUD", "2026-12-17", 0.001, 1.0],
            ],
        }
    }

    apim._attach_expiry_metadata(snapshot, payload)

    assert snapshot["instruments"]["si_front"]["expiry_date"] == "2026-09-17"
    assert snapshot["instruments"]["si_next"]["expiry_date"] == "2026-12-17"
    assert snapshot["instruments"]["cr_front"]["expiry_date"] == "2026-09-17"
    assert snapshot["instruments"]["cr_next"]["expiry_date"] == "2026-12-17"
    for logical_id in apim.EXPIRING_LOGICAL_IDS:
        metadata = snapshot["instruments"][logical_id]["expiry_metadata"]
        assert metadata["source_id"] == apim.CONTRACT_METADATA_SOURCE_ID
        assert metadata["source_field"] == "LASTTRADEDATE"
        assert metadata["same_rfud_response_as_live_binding"] is True
        assert metadata["front_next_minimum_days_to_expiry"] == 1
        assert metadata["expiry_day_contract_allowed"] is False

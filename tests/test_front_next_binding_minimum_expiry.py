from __future__ import annotations

import pandas as pd

from moex_data.futures.front_next_binding import bind_front_next


def _reference() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"SECID": "SiU6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-08-24"},
            {"SECID": "SiZ6", "BOARDID": "RFUD", "LASTTRADEDATE": "2026-12-17"},
            {"SECID": "SiH7", "BOARDID": "RFUD", "LASTTRADEDATE": "2027-03-18"},
        ]
    )


def test_default_binding_keeps_expiry_day_contract_and_payload_shape() -> None:
    result = bind_front_next(_reference(), root="Si", as_of_date="2026-08-24")
    assert [item["secid"] for item in result] == ["SiU6", "SiZ6"]
    assert all("minimum_days_to_expiry" not in item for item in result)


def test_stage4_binding_skips_expiry_day_contract() -> None:
    result = bind_front_next(
        _reference(),
        root="Si",
        as_of_date="2026-08-24",
        minimum_days_to_expiry=1,
    )
    assert [item["secid"] for item in result] == ["SiZ6", "SiH7"]
    assert all(item["minimum_days_to_expiry"] == "1" for item in result)
    assert all(item["last_trade_date"] > "2026-08-24" for item in result)

from __future__ import annotations

import pandas as pd

from moex_data import synchronized_live_market_oi_context as live


def _securities() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ["SiU6", "RFUD", "2026-09-17"],
            ["SiZ6", "RFUD", "2026-12-17"],
            ["SiH7", "RFUD", "2027-03-18"],
            ["CRU6", "RFUD", "2026-09-17"],
            ["CRZ6", "RFUD", "2026-12-17"],
            ["CRH7", "RFUD", "2027-03-18"],
        ],
        columns=["SECID", "BOARDID", "LASTTRADEDATE"],
    )


def test_expiry_day_si_contract_is_excluded_from_live_front_next() -> None:
    bindings = live._bindings_from_forts(
        _securities(),
        as_of_date="2026-09-17",
        availability_ts_utc="2026-09-17T10:00:00+00:00",
    )

    assert bindings["si_front"] == "SiZ6"
    assert bindings["si_next"] == "SiH7"
    assert bindings["si_front"] != "SiU6"


def test_expiry_day_cr_contract_is_excluded_from_live_front_next() -> None:
    bindings = live._bindings_from_forts(
        _securities(),
        as_of_date="2026-09-17",
        availability_ts_utc="2026-09-17T10:00:00+00:00",
    )

    assert bindings["cr_front"] == "CRZ6"
    assert bindings["cr_next"] == "CRH7"
    assert bindings["cr_front"] != "CRU6"

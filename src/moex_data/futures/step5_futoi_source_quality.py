from __future__ import annotations

from collections.abc import Mapping
from typing import Final

SOURCE_QUALITY_OMISSIONS: Final[dict[str, dict[str, str]]] = {
    "si_futures_family": {
        "2025-08-11": "no_complete_balanced_FIZ_YUR_snapshot",
    },
    "cr_futures_family": {},
}


def omissions_for_instrument(instrument_id: str) -> Mapping[str, str]:
    return SOURCE_QUALITY_OMISSIONS.get(instrument_id, {})


def omission_records(instrument_id: str) -> list[dict[str, str]]:
    return [
        {"trade_date": trade_date, "reason": reason}
        for trade_date, reason in sorted(omissions_for_instrument(instrument_id).items())
    ]


def expected_derived_rows(instrument_id: str, raw_partition_count: int) -> int:
    return int(raw_partition_count) - len(omissions_for_instrument(instrument_id))

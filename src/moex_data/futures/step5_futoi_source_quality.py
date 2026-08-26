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


def omission_records(
    instrument_id: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for trade_date, reason in sorted(omissions_for_instrument(instrument_id).items()):
        if start_date is not None and trade_date < start_date:
            continue
        if end_date is not None and trade_date > end_date:
            continue
        rows.append({"trade_date": trade_date, "reason": reason})
    return rows


def expected_derived_rows(
    instrument_id: str,
    raw_partition_count: int,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> int:
    return int(raw_partition_count) - len(
        omission_records(instrument_id, start_date=start_date, end_date=end_date)
    )

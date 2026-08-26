from __future__ import annotations

import pandas as pd
import pytest

from moex_data.step5_futoi_positioning_acceptance import _reconstruct_eod_row_with_source_tail_policy


def _row(ts: str, group: str, *, pos: int, long: int, short: int, seqnum: int) -> dict[str, object]:
    return {
        "instrument_id": "si_futures_family",
        "trade_date": "2026-08-17",
        "ts": ts,
        "systime": ts.replace("00", "10", 1) if False else ts,
        "sess_id": 1,
        "seqnum": seqnum,
        "clgroup": group,
        "pos": pos,
        "pos_long": long,
        "pos_short": short,
        "pos_long_num": 10,
        "pos_short_num": 10,
        "availability_ts_utc": "2026-08-17T17:10:00+00:00",
    }


def _raw() -> pd.DataFrame:
    rows = [
        _row("2026-08-17 20:00:00", "FIZ", pos=20, long=60, short=-40, seqnum=1),
        _row("2026-08-17 20:00:00", "YUR", pos=-20, long=40, short=-60, seqnum=1),
        _row("2026-08-17 20:05:00", "FIZ", pos=30, long=70, short=-40, seqnum=2),
        _row("2026-08-17 20:05:00", "YUR", pos=-29, long=41, short=-70, seqnum=2),
        _row("2026-08-17 20:10:00", "FIZ", pos=31, long=71, short=-40, seqnum=3),
    ]
    return pd.DataFrame(rows)


def _rebuild(frame: pd.DataFrame) -> dict[str, object]:
    return _reconstruct_eod_row_with_source_tail_policy(
        frame,
        instrument_id="si_futures_family",
        trade_date="2026-08-17",
        frozen_partition_ref="${MOEX_DATA_ROOT}/runs/frozen.parquet",
        canonical_source_ref="${MOEX_DATA_ROOT}/market/raw.parquet",
        frozen_sha256="a" * 64,
    )


def test_acceptance_oracle_walks_past_incomplete_and_unbalanced_tail() -> None:
    rebuilt = _rebuild(_raw())
    assert rebuilt["snapshot_ts_utc"] == "2026-08-17T17:00:00+00:00"
    assert rebuilt["phys_net"] == 20
    assert rebuilt["legal_net"] == -20
    assert rebuilt["source_row_count"] == 5


def test_acceptance_oracle_fails_closed_without_balanced_pair() -> None:
    frame = _raw()
    frame = frame.loc[frame["ts"] != "2026-08-17 20:00:00"].reset_index(drop=True)
    with pytest.raises(ValueError, match="no complete balanced FIZ/YUR snapshot"):
        _rebuild(frame)

from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures.materialize_futoi_eod import FutoiEodError, _single_eod_row


def _raw(*, ambiguous_session: bool = False, incomplete_final: bool = False) -> pd.DataFrame:
    rows = [
        {"instrument_id":"si_futures_family","trade_date":"2026-08-17","ts":"2026-08-17 10:00:00","systime":"2026-08-17 10:00:10","sess_id":1,"seqnum":1,"clgroup":"FIZ","pos":10,"pos_long":55,"pos_short":-45,"pos_long_num":10,"pos_short_num":9,"availability_ts_utc":"2026-08-17T07:01:00+00:00"},
        {"instrument_id":"si_futures_family","trade_date":"2026-08-17","ts":"2026-08-17 10:00:00","systime":"2026-08-17 10:00:10","sess_id":1,"seqnum":1,"clgroup":"YUR","pos":-10,"pos_long":45,"pos_short":-55,"pos_long_num":8,"pos_short_num":11,"availability_ts_utc":"2026-08-17T07:01:00+00:00"},
        {"instrument_id":"si_futures_family","trade_date":"2026-08-17","ts":"2026-08-17 10:05:00","systime":"2026-08-17 10:05:10","sess_id":1,"seqnum":2,"clgroup":"FIZ","pos":15,"pos_long":58,"pos_short":-43,"pos_long_num":10,"pos_short_num":9,"availability_ts_utc":"2026-08-17T07:06:00+00:00"},
        {"instrument_id":"si_futures_family","trade_date":"2026-08-17","ts":"2026-08-17 10:05:00","systime":"2026-08-17 10:05:15","sess_id":2 if ambiguous_session else 1,"seqnum":3,"clgroup":"FIZ","pos":20,"pos_long":60,"pos_short":-40,"pos_long_num":10,"pos_short_num":8,"availability_ts_utc":"2026-08-17T07:06:00+00:00"},
    ]
    if not incomplete_final:
        rows.append({"instrument_id":"si_futures_family","trade_date":"2026-08-17","ts":"2026-08-17 10:05:00","systime":"2026-08-17 10:05:12","sess_id":1,"seqnum":3,"clgroup":"YUR","pos":-20,"pos_long":40,"pos_short":-60,"pos_long_num":7,"pos_short_num":12,"availability_ts_utc":"2026-08-17T07:06:00+00:00"})
    return pd.DataFrame(rows)


def _row(frame: pd.DataFrame) -> dict[str, object]:
    return _single_eod_row(
        frame,
        instrument_id="si_futures_family",
        trade_date="2026-08-17",
        frozen_ref="${MOEX_DATA_ROOT}/runs/fixture/frozen.parquet",
        canonical_source_ref="${MOEX_DATA_ROOT}/market/fixture.parquet",
        frozen_sha256="a" * 64,
    )


def test_eod_resolves_same_session_revision_and_uses_paired_final_snapshot() -> None:
    row = _row(_raw())
    assert row["phys_seqnum"] == 3
    assert row["legal_seqnum"] == 3
    assert row["phys_net"] == 20
    assert row["legal_net"] == -20
    assert row["total_open_interest"] == 100
    assert row["total_short_abs"] == 100
    assert row["source_revision_rows_dropped"] == 1
    assert row["snapshot_ts_utc"] == "2026-08-17T07:05:00+00:00"
    assert row["phys_net_share_of_oi"] == pytest.approx(0.2)
    assert row["source_frozen_partition_sha256"] == "a" * 64


def test_eod_rejects_multi_session_revision_for_same_event() -> None:
    with pytest.raises(FutoiEodError, match="multi-session revision"):
        _row(_raw(ambiguous_session=True))


def test_eod_falls_back_from_incomplete_latest_snapshot() -> None:
    row = _row(_raw(incomplete_final=True))
    assert row["snapshot_ts_utc"] == "2026-08-17T07:00:00+00:00"
    assert row["phys_net"] == 10
    assert row["legal_net"] == -10
    assert row["source_row_count"] == 4
    assert row["source_revision_rows_dropped"] == 1


def test_eod_falls_back_from_complete_but_unbalanced_latest_snapshot() -> None:
    frame = _raw()
    mask = (frame["ts"] == "2026-08-17 10:05:00") & (frame["clgroup"] == "YUR")
    frame.loc[mask, "pos_long"] = 41
    frame.loc[mask, "pos"] = -19
    row = _row(frame)
    assert row["snapshot_ts_utc"] == "2026-08-17T07:00:00+00:00"
    assert row["total_open_interest"] == 100
    assert row["total_short_abs"] == 100


def test_eod_fails_closed_when_no_complete_balanced_snapshot_exists() -> None:
    frame = _raw()
    frame.loc[frame["clgroup"] == "YUR", "pos_long"] += 1
    frame.loc[frame["clgroup"] == "YUR", "pos"] += 1
    with pytest.raises(FutoiEodError, match="no complete balanced FIZ/YUR snapshot"):
        _row(frame)

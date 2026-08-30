from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures import futoi_live_factual_refresh as factual


def _row(ts: str, group: str, *, seqnum: int, long: int, short: int, net: int) -> dict[str, object]:
    return {
        "trade_date": "2026-08-28",
        "ts": ts,
        "systime": "2026-08-28 23:55:10",
        "availability_ts_utc": "2026-08-28T20:56:00+00:00",
        "ingest_ts": "2026-08-28T20:56:00+00:00",
        "sess_id": 1,
        "seqnum": seqnum,
        "clgroup": group,
        "pos": net,
        "pos_long": long,
        "pos_short": short,
        "pos_long_num": 10,
        "pos_short_num": 11,
        "source_id": factual.SOURCE_ID,
        "source_ticker": "si",
        "secid": "SiU6",
    }


def test_latest_aligned_fiz_yur_uses_latest_shared_timestamp_and_max_seqnum() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-08-28 23:45:00", "FIZ", seqnum=1, long=100, short=-80, net=20),
            _row("2026-08-28 23:45:00", "YUR", seqnum=1, long=80, short=-100, net=-20),
            _row("2026-08-28 23:50:00", "FIZ", seqnum=1, long=105, short=-80, net=25),
            _row("2026-08-28 23:50:00", "FIZ", seqnum=2, long=110, short=-80, net=30),
            _row("2026-08-28 23:50:00", "YUR", seqnum=2, long=80, short=-110, net=-30),
        ]
    )

    result = factual.latest_aligned_factual(frame, expected_trade_date="2026-08-28")

    assert result["snapshot_ts"].startswith("2026-08-28T23:50:00")
    assert result["fiz"] == {
        "long": 110,
        "short": 80,
        "net": 30,
        "long_participants": 10,
        "short_participants": 11,
    }
    assert result["yur"]["net"] == -30
    assert result["total_open_interest"] == 190
    assert result["short_semantics"] == "absolute_contract_count"


def test_latest_aligned_fiz_yur_fails_closed_on_unbalanced_net() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-08-28 23:50:00", "FIZ", seqnum=2, long=110, short=-80, net=30),
            _row("2026-08-28 23:50:00", "YUR", seqnum=2, long=80, short=-105, net=-25),
        ]
    )

    with pytest.raises(factual.FutoiLiveFactualRefreshError, match="balance to zero"):
        factual.latest_aligned_factual(frame, expected_trade_date="2026-08-28")


def test_latest_aligned_fiz_yur_fails_closed_without_exact_pair() -> None:
    frame = pd.DataFrame(
        [_row("2026-08-28 23:50:00", "FIZ", seqnum=2, long=110, short=-80, net=30)]
    )

    with pytest.raises(factual.FutoiLiveFactualRefreshError, match="no exact aligned FIZ/YUR"):
        factual.latest_aligned_factual(frame, expected_trade_date="2026-08-28")

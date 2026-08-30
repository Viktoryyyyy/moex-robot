from __future__ import annotations

import hashlib
from datetime import date, timedelta

import pandas as pd
import pytest

from moex_data.futures import futoi_live_factual_refresh as factual


def _row(
    ts: str,
    group: str,
    *,
    seqnum: int,
    long: int,
    short: int,
    net: int,
    sess_id: int = 1,
) -> dict[str, object]:
    return {
        "trade_date": "2026-08-28",
        "ts": ts,
        "systime": "2026-08-28 23:55:10",
        "availability_ts_utc": "2026-08-28T20:56:00+00:00",
        "ingest_ts": "2026-08-28T20:56:00+00:00",
        "sess_id": sess_id,
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


def test_latest_completed_trading_date_requires_complete_calendar_coverage(monkeypatch) -> None:
    end = date(2026, 8, 30)
    start = end - timedelta(days=factual.CALENDAR_LOOKBACK_DAYS)
    calendar = {
        start + timedelta(days=offset): False
        for offset in range(factual.CALENDAR_LOOKBACK_DAYS)
    }
    calendar[date(2026, 8, 28)] = True

    monkeypatch.setattr(
        factual.futures_calendar,
        "fetch_futures_calendar_rows",
        lambda *args, **kwargs: [{"date": "placeholder"}],
    )
    monkeypatch.setattr(
        factual.futures_calendar,
        "_calendar_map",
        lambda rows: calendar,
    )

    with pytest.raises(
        factual.FutoiLiveFactualRefreshError,
        match="calendar coverage incomplete: missing date 2026-08-30",
    ):
        factual._latest_completed_trading_date("2026-08-30", timeout=1.0)


def test_latest_completed_trading_date_accepts_complete_weekend_calendar(monkeypatch) -> None:
    end = date(2026, 8, 30)
    start = end - timedelta(days=factual.CALENDAR_LOOKBACK_DAYS)
    calendar = {
        start + timedelta(days=offset): False
        for offset in range(factual.CALENDAR_LOOKBACK_DAYS + 1)
    }
    calendar[date(2026, 8, 28)] = True

    monkeypatch.setattr(
        factual.futures_calendar,
        "fetch_futures_calendar_rows",
        lambda *args, **kwargs: [{"date": "placeholder"}],
    )
    monkeypatch.setattr(
        factual.futures_calendar,
        "_calendar_map",
        lambda rows: calendar,
    )

    assert factual._latest_completed_trading_date("2026-08-30", timeout=1.0) == "2026-08-28"


def test_latest_aligned_fiz_yur_uses_latest_shared_timestamp_session_and_max_seqnum() -> None:
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

    assert result["snapshot_ts"] == "2026-08-28T20:50:00+00:00"
    assert result["source_publication_time"] == "2026-08-28T20:55:10+00:00"
    assert result["sess_id"] == 1
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


def test_latest_aligned_fiz_yur_fails_closed_on_cross_session_pair() -> None:
    frame = pd.DataFrame(
        [
            _row(
                "2026-08-28 23:50:00",
                "FIZ",
                seqnum=2,
                long=110,
                short=-80,
                net=30,
                sess_id=1,
            ),
            _row(
                "2026-08-28 23:50:00",
                "YUR",
                seqnum=2,
                long=80,
                short=-110,
                net=-30,
                sess_id=2,
            ),
        ]
    )

    with pytest.raises(factual.FutoiLiveFactualRefreshError, match="share sess_id"):
        factual.latest_aligned_factual(frame, expected_trade_date="2026-08-28")


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


def test_load_current_historical_uses_content_attested_baseline(tmp_path, monkeypatch) -> None:
    marker = tmp_path / "marker.json"
    manifest = tmp_path / "manifest.json"
    partition = tmp_path / "part.parquet"
    marker.write_text("{}\n", encoding="utf-8")
    manifest.write_text("{}\n", encoding="utf-8")
    partition.write_bytes(b"accepted-futoi-partition")
    partition_sha = hashlib.sha256(partition.read_bytes()).hexdigest()

    monkeypatch.setattr(
        factual.historical_attestation,
        "resolve_content_attested_history",
        lambda **kwargs: {
            "requested_till": "2026-08-28",
            "marker_path": marker.as_posix(),
            "marker_sha256": "a" * 64,
            "manifest_path": manifest.as_posix(),
            "manifest_sha256": "b" * 64,
            "partition_content_set_sha256": "c" * 64,
            "records": (
                {
                    "trade_date": "2026-08-28",
                    "snapshot_path": partition.as_posix(),
                    "sha256": partition_sha,
                },
            ),
        },
    )

    path, provenance = factual._load_current_historical(
        tmp_path, expected_trade_date="2026-08-28"
    )

    assert path == partition
    assert provenance["accepted_state_kind"] == "historical_content_attested"
    assert provenance["accepted_partition_sha256"] == partition_sha
    assert provenance["historical_content_attestation_marker_ref"].endswith("marker.json")

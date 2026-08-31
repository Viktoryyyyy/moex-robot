from __future__ import annotations

import json

import pytest

from moex_data.futures import backfill_futoi_instrument as futoi


def test_emit_progress_writes_machine_readable_stderr(capsys) -> None:
    futoi._emit_progress(
        instrument_id="si_futures_family",
        secid="SiU6",
        futoi_ticker="si",
        processed_dates=25,
        total_dates=100,
        trade_date="2020-01-27",
        partition_count=17,
        failure_count=0,
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload == {
        "event": "progress",
        "failure_count": 0,
        "futoi_ticker": "si",
        "instrument_id": "si_futures_family",
        "partition_count": 17,
        "processed_dates": 25,
        "secid": "SiU6",
        "total_dates": 100,
        "trade_date": "2020-01-27",
    }


def test_cli_progress_default_and_validation() -> None:
    args = futoi.parse_args(
        [
            "--date-start",
            "2020-01-03",
            "--date-end",
            "2020-01-04",
            "--instrument-id",
            "si_futures_family",
            "--run-id",
            "stage2_futoi_test",
        ]
    )
    assert args.progress_every == 25

    with pytest.raises(futoi.FutoiBackfillError, match="progress_every must be >= 0"):
        futoi.backfill_range(
            date_start="2020-01-03",
            date_end="2020-01-04",
            instrument_id="si_futures_family",
            run_id="stage2_futoi_test",
            progress_every=-1,
        )

from __future__ import annotations

import json

import pytest

from moex_data.futures import backfill_stage2_forts_raw_5m_instrument as stage2


def test_emit_progress_writes_machine_readable_stderr(capsys) -> None:
    stage2._emit_progress(
        instrument_id="usdrubf_futures_family",
        secid="USDRUBF",
        processed_dates=25,
        total_dates=100,
        trade_date="2022-05-20",
        partition_count=18,
        skipped_count=7,
        failure_count=0,
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload == {
        "event": "progress",
        "failure_count": 0,
        "instrument_id": "usdrubf_futures_family",
        "partition_count": 18,
        "processed_dates": 25,
        "secid": "USDRUBF",
        "skipped_empty_source_dates": 7,
        "total_dates": 100,
        "trade_date": "2022-05-20",
    }


def test_cli_progress_default_and_validation() -> None:
    args = stage2.parse_args(
        [
            "--date-start",
            "2022-04-26",
            "--date-end",
            "2022-04-27",
            "--instrument-id",
            "usdrubf_futures_family",
            "--secid",
            "USDRUBF",
            "--artifact-version",
            "stage2_test",
        ]
    )
    assert args.progress_every == 25

    with pytest.raises(stage2.Stage2QuotesBackfillError, match="progress_every must be >= 0"):
        stage2.backfill_range(
            date_start="2022-04-26",
            date_end="2022-04-27",
            instrument_id="usdrubf_futures_family",
            secid="USDRUBF",
            artifact_version="stage2_test",
            progress_every=-1,
        )

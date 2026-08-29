from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import backfill_futoi_instrument as backfill
from moex_data.futures import futoi_raw_incremental_acceptance as acceptance
from moex_data.futures import materialize_futoi_instrument as materializer


def _write_json(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _root_ref(root: Path, path: Path) -> str:
    return "${MOEX_DATA_ROOT}/" + path.relative_to(root).as_posix()


def _historical_state(root: Path) -> None:
    instrument_id = "si_futures_family"
    manifest = root / "state" / "accepted_manifests" / "raw_history" / ("instrument_id=" + instrument_id) / "manifest.json"
    _write_json(
        manifest,
        {
            "target_dataset_id": materializer.DATASET_ID,
            "instrument_id": instrument_id,
            "source_id": materializer.SOURCE_ID,
            "acceptance_status": "pass",
            "requested_from": "2020-01-03",
            "requested_till": "2026-08-17",
            "partition_count": 1757,
            "row_count": 597650,
        },
    )
    pointer = root / "state" / "datasets" / ("dataset_id=" + materializer.DATASET_ID) / ("instrument_id=" + instrument_id) / "current_accepted_manifest.json"
    _write_json(
        pointer,
        {
            "dataset_id": materializer.DATASET_ID,
            "instrument_id": instrument_id,
            "manifest_ref": _root_ref(root, manifest),
            "quality_status": "pass",
            "acceptance_status": "pass",
        },
    )


def _canonical_partition(trade_date: str) -> Path:
    rows: list[dict[str, object]] = []
    for index, group in enumerate(("FIZ", "YUR"), start=1):
        moment = trade_date + " 10:00:00"
        rows.append(
            {
                "instrument_id": "si_futures_family",
                "trade_date": trade_date,
                "ts": moment,
                "moment": moment,
                "systime": trade_date + " 10:00:05",
                "sess_id": 1,
                "seqnum": index,
                "secid": "SiU6",
                "board": "RFUD",
                "market": "forts",
                "engine": "futures",
                "source_id": materializer.SOURCE_ID,
                "source_ticker": "SI",
                "clgroup": group,
                "pos": 20,
                "pos_long": 100 + index,
                "pos_short": -(80 + index),
                "pos_long_num": 10 + index,
                "pos_short_num": 9 + index,
                "availability_ts_utc": "2026-08-29T19:40:27+00:00",
                "ingest_ts": "2026-08-29T19:40:27+00:00",
            }
        )
    path = materializer._partition_path(trade_date, "si_futures_family", materializer.SOURCE_ID)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _backfill_evidence(root: Path, run_id: str) -> Path:
    trade_date = "2026-08-18"
    partition = _canonical_partition(trade_date)
    quality_path = backfill._aggregate_quality_path(trade_date, run_id)
    manifest_path = backfill._aggregate_manifest_path(trade_date, run_id)
    _write_json(
        quality_path,
        {
            "run_id": run_id,
            "dataset_id": materializer.DATASET_ID,
            "instrument_id": "si_futures_family",
            "source_id": materializer.SOURCE_ID,
            "quality_status": "pass",
            "row_count": 2,
            "duplicate_key_count": 0,
            "null_required_count": 0,
            "invalid_position_count": 0,
            "partition_count": 1,
            "skipped_empty_source_dates": [],
            "failed_dates": [],
        },
    )
    _write_json(
        manifest_path,
        {
            "run_id": run_id,
            "run_date": trade_date,
            "dataset_id": materializer.DATASET_ID,
            "instrument_scope": ["si_futures_family"],
            "source_scope": [materializer.SOURCE_ID],
            "requested_from": trade_date,
            "requested_till": trade_date,
            "partitions_written": [partition.as_posix()],
            "partitions_skipped": [],
            "quality_report_ref": quality_path.as_posix(),
            "refresh_status": "succeeded",
            "failed_dates": [],
            "latest_autodetect_used": False,
            "hardcoded_server_path_used": False,
            "producer": backfill.PRODUCER_ID,
            "stage2_controlled_backfill": True,
        },
    )
    return partition


@pytest.fixture
def data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())
    _historical_state(root)
    return root


def test_rejects_raw_ts_that_differs_from_moment(data_root: Path) -> None:
    partition = _backfill_evidence(data_root, "raw_ts_mismatch_v1")
    frame = pd.read_parquet(partition)
    frame.loc[0, "ts"] = "2026-08-18 10:00:01"
    frame.to_parquet(partition, index=False)

    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="raw ts must equal raw moment"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family",
            backfill_run_id="raw_ts_mismatch_v1",
            date_end="2026-08-18",
        )


def test_rejects_semantic_duplicate_after_identifier_canonicalization(data_root: Path) -> None:
    partition = _backfill_evidence(data_root, "canonical_duplicate_v1")
    frame = pd.read_parquet(partition)
    frame["sess_id"] = frame["sess_id"].astype(str)
    frame["seqnum"] = frame["seqnum"].astype(str)
    duplicate = frame.iloc[[0]].copy()
    duplicate.loc[:, "sess_id"] = "01"
    duplicate.loc[:, "seqnum"] = "01"
    pd.concat([frame, duplicate], ignore_index=True).to_parquet(partition, index=False)

    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="duplicate_key_count"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family",
            backfill_run_id="canonical_duplicate_v1",
            date_end="2026-08-18",
        )

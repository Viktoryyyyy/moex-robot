from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
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
    instrument_id = "si_futures_family"
    reference_secid = "SiU6"
    partition = _canonical_partition(trade_date)
    quality_path = backfill._aggregate_quality_path(trade_date, run_id)
    manifest_path = backfill._aggregate_manifest_path(trade_date, run_id)
    observed_path = backfill._observed_date_evidence_path(trade_date, run_id)
    observed_values = {
        "schema_version": backfill.OBSERVED_DATE_EVIDENCE_SCHEMA,
        "producer": backfill.PRODUCER_ID,
        "run_id": run_id,
        "dataset_id": materializer.DATASET_ID,
        "instrument_id": instrument_id,
        "date_source_artifact_id": backfill.DATE_SOURCE_ARTIFACT_ID,
        "date_source_id": backfill.DATE_SOURCE_ID,
        "date_source_endpoint": backfill.DATE_SOURCE_ENDPOINT,
        "date_selection_rule": backfill.DATE_SELECTION_RULE,
        "reference_secid": reference_secid,
        "requested_from": trade_date,
        "requested_till": trade_date,
        "observed_dates": [trade_date],
        "observed_date_count": 1,
    }
    _write_json(observed_path, observed_values)
    observed_sha = hashlib.sha256(observed_path.read_bytes()).hexdigest()
    date_source = {
        "date_source_artifact_id": backfill.DATE_SOURCE_ARTIFACT_ID,
        "date_source_id": backfill.DATE_SOURCE_ID,
        "date_source_endpoint": backfill.DATE_SOURCE_ENDPOINT,
        "date_selection_rule": backfill.DATE_SELECTION_RULE,
        "reference_secid": reference_secid,
        "observed_date_evidence_ref": observed_path.as_posix(),
        "observed_date_evidence_sha256": observed_sha,
        "observed_date_count": 1,
        "observed_trade_dates": [trade_date],
    }
    quality_values = {
        "run_id": run_id,
        "dataset_id": materializer.DATASET_ID,
        "instrument_id": instrument_id,
        "source_id": materializer.SOURCE_ID,
        "quality_status": "pass",
        "row_count": 2,
        "duplicate_key_count": 0,
        "null_required_count": 0,
        "invalid_position_count": 0,
        "partition_count": 1,
        "failed_dates": [],
        **date_source,
    }
    _write_json(quality_path, quality_values)
    manifest_values = {
        "run_id": run_id,
        "run_date": trade_date,
        "dataset_id": materializer.DATASET_ID,
        "instrument_scope": [instrument_id],
        "source_scope": [materializer.SOURCE_ID],
        "requested_from": trade_date,
        "requested_till": trade_date,
        "partitions_written": [partition.as_posix()],
        "partition_evidence": [
            {
                "trade_date": trade_date,
                "subrun_id": backfill._subrun_id(run_id, trade_date),
                "partition_path": partition.as_posix(),
                "sha256": hashlib.sha256(partition.read_bytes()).hexdigest(),
                "row_count": 2,
            }
        ],
        "quality_report_ref": quality_path.as_posix(),
        "refresh_status": "succeeded",
        "failed_dates": [],
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "producer": backfill.PRODUCER_ID,
        "stage2_controlled_backfill": True,
        **date_source,
    }
    _write_json(manifest_path, manifest_values)
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


def test_rejects_semantic_duplicate_after_secid_and_clgroup_canonicalization(data_root: Path) -> None:
    partition = _backfill_evidence(data_root, "source_key_text_duplicate_v1")
    frame = pd.read_parquet(partition)
    duplicate = frame.iloc[[0]].copy()
    duplicate.loc[:, "secid"] = "SIU6"
    duplicate.loc[:, "clgroup"] = " fiz "
    pd.concat([frame, duplicate], ignore_index=True).to_parquet(partition, index=False)

    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="duplicate_key_count"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family",
            backfill_run_id="source_key_text_duplicate_v1",
            date_end="2026-08-18",
        )


@pytest.mark.parametrize(
    ("artifact", "error_pattern"),
    (("manifest", "manifest run_id mismatch"), ("quality", "quality run_id mismatch")),
)
def test_rejects_embedded_backfill_run_id_mismatch(
    data_root: Path,
    artifact: str,
    error_pattern: str,
) -> None:
    run_id = "run_binding_" + artifact + "_v1"
    _backfill_evidence(data_root, run_id)
    trade_date = "2026-08-18"
    target = (
        backfill._aggregate_manifest_path(trade_date, run_id)
        if artifact == "manifest"
        else backfill._aggregate_quality_path(trade_date, run_id)
    )
    values = json.loads(target.read_text(encoding="utf-8"))
    values["run_id"] = "different_run"
    _write_json(target, values)

    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match=error_pattern):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family",
            backfill_run_id=run_id,
            date_end=trade_date,
        )


@pytest.mark.parametrize(
    ("field", "value", "error_pattern"),
    (
        ("pos", np.inf, "non-finite numeric value"),
        ("pos_long_num", 10.5, "participant counts must be non-negative integers"),
        ("pos", 21, "net position must equal pos_long plus pos_short"),
    ),
)
def test_rejects_complete_position_invariant_defects(
    data_root: Path,
    field: str,
    value: object,
    error_pattern: str,
) -> None:
    run_id = "position_invariant_" + field + "_v1"
    partition = _backfill_evidence(data_root, run_id)
    frame = pd.read_parquet(partition)
    frame[field] = frame[field].astype(float)
    frame.loc[0, field] = value
    frame.to_parquet(partition, index=False)

    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match=error_pattern):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family",
            backfill_run_id=run_id,
            date_end="2026-08-18",
        )


@pytest.mark.parametrize(
    ("field", "value", "error_pattern"),
    (
        ("availability_ts_utc", "2026-08-18T06:00:00+00:00", "availability timestamp precedes source publication timestamp"),
        ("ingest_ts", "2026-08-18T06:30:00+00:00", "ingest timestamp precedes availability timestamp"),
    ),
)
def test_rejects_invalid_provenance_chronology(
    data_root: Path,
    field: str,
    value: str,
    error_pattern: str,
) -> None:
    run_id = "chronology_" + field + "_v1"
    partition = _backfill_evidence(data_root, run_id)
    frame = pd.read_parquet(partition)
    if field == "ingest_ts":
        frame.loc[:, "availability_ts_utc"] = "2026-08-18T07:30:00+00:00"
    frame.loc[:, field] = value
    frame.to_parquet(partition, index=False)

    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match=error_pattern):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family",
            backfill_run_id=run_id,
            date_end="2026-08-18",
        )


def test_rejects_negative_canonical_seqnum(data_root: Path) -> None:
    run_id = "negative_seqnum_v1"
    partition = _backfill_evidence(data_root, run_id)
    frame = pd.read_parquet(partition)
    frame.loc[0, "seqnum"] = -1
    frame.to_parquet(partition, index=False)

    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="seqnum must be non-negative"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family",
            backfill_run_id=run_id,
            date_end="2026-08-18",
        )


def test_rejects_partition_bytes_overwritten_after_run_evidence(data_root: Path) -> None:
    run_id = "run_bytes_binding_v1"
    partition = _backfill_evidence(data_root, run_id)
    frame = pd.read_parquet(partition)
    frame.loc[:, "ingest_ts"] = "2026-08-29T19:40:28+00:00"
    frame.to_parquet(partition, index=False)

    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="bytes differ from run-bound evidence"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family",
            backfill_run_id=run_id,
            date_end="2026-08-18",
        )

from __future__ import annotations

import hashlib
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


def _historical_state(root: Path, instrument_id: str = "si_futures_family") -> Path:
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
    return pointer


def _partition(
    root: Path,
    trade_date: str,
    instrument_id: str = "si_futures_family",
    groups: tuple[str, ...] = ("FIZ", "YUR"),
    invalid_position: bool = False,
    secid: str = "SiU6",
    drop_column: str | None = None,
) -> Path:
    rows: list[dict[str, object]] = []
    for index, group in enumerate(groups, start=1):
        moment = trade_date + " 10:00:00"
        pos_long = -1 if invalid_position and index == 1 else 100 + index
        pos_short = -(80 + index)
        rows.append(
            {
                "instrument_id": instrument_id,
                "trade_date": trade_date,
                "ts": moment,
                "moment": moment,
                "systime": trade_date + " 10:00:05",
                "sess_id": 1,
                "seqnum": index,
                "secid": secid,
                "board": "RFUD",
                "market": "forts",
                "engine": "futures",
                "source_id": materializer.SOURCE_ID,
                "source_ticker": "SI",
                "clgroup": group,
                "pos": pos_long + pos_short,
                "pos_long": pos_long,
                "pos_short": pos_short,
                "pos_long_num": 10 + index,
                "pos_short_num": 9 + index,
                "availability_ts_utc": "2026-08-29T19:40:27+00:00",
                "ingest_ts": "2026-08-29T19:40:27+00:00",
            }
        )
    frame = pd.DataFrame(rows)
    if drop_column is not None:
        frame = frame.drop(columns=[drop_column])
    path = materializer._partition_path(trade_date, instrument_id, materializer.SOURCE_ID)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _backfill_evidence(
    root: Path,
    *,
    run_id: str,
    requested_from: str,
    requested_till: str,
    written_dates: tuple[str, ...],
    skipped_dates: tuple[str, ...] = (),
    groups: tuple[str, ...] = ("FIZ", "YUR"),
    aggregate_duplicate_key_count: int = 0,
    invalid_partition_position: bool = False,
    partition_secid: str = "SiU6",
    drop_partition_column: str | None = None,
    instrument_id: str = "si_futures_family",
) -> list[Path]:
    partitions = [
        _partition(
            root,
            value,
            instrument_id=instrument_id,
            groups=groups,
            invalid_position=invalid_partition_position,
            secid=partition_secid,
            drop_column=drop_partition_column,
        )
        for value in written_dates
    ]
    row_count = len(written_dates) * len(groups)
    quality_path = backfill._aggregate_quality_path(requested_till, run_id)
    manifest_path = backfill._aggregate_manifest_path(requested_till, run_id)
    quality = {
        "run_id": run_id,
        "dataset_id": materializer.DATASET_ID,
        "instrument_id": instrument_id,
        "source_id": materializer.SOURCE_ID,
        "quality_status": "pass",
        "row_count": row_count,
        "duplicate_key_count": aggregate_duplicate_key_count,
        "null_required_count": 0,
        "invalid_position_count": 0,
        "partition_count": len(partitions),
        "skipped_empty_source_dates": list(skipped_dates),
        "failed_dates": [],
    }
    manifest = {
        "run_id": run_id,
        "run_date": requested_till,
        "dataset_id": materializer.DATASET_ID,
        "instrument_scope": [instrument_id],
        "source_scope": [materializer.SOURCE_ID],
        "requested_from": requested_from,
        "requested_till": requested_till,
        "partitions_written": [value.as_posix() for value in partitions],
        "partition_evidence": [
            {
                "trade_date": trade_date,
                "subrun_id": backfill._subrun_id(run_id, trade_date),
                "partition_path": path.as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "row_count": len(groups),
            }
            for trade_date, path in zip(written_dates, partitions, strict=True)
        ],
        "partitions_skipped": list(skipped_dates),
        "quality_report_ref": quality_path.as_posix(),
        "refresh_status": "succeeded",
        "failed_dates": [],
        "latest_autodetect_used": False,
        "hardcoded_server_path_used": False,
        "producer": backfill.PRODUCER_ID,
        "stage2_controlled_backfill": True,
    }
    _write_json(quality_path, quality)
    _write_json(manifest_path, manifest)
    return partitions


@pytest.fixture
def data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())
    return root


def test_first_increment_accepts_without_mutating_historical_pointer(data_root: Path) -> None:
    historical_pointer = _historical_state(data_root)
    before = historical_pointer.read_bytes()
    canonical_partitions = _backfill_evidence(
        data_root,
        run_id="inc_v1",
        requested_from="2026-08-18",
        requested_till="2026-08-19",
        written_dates=("2026-08-18",),
        skipped_dates=("2026-08-19",),
    )
    canonical_bytes = canonical_partitions[0].read_bytes()

    result = acceptance.accept_incremental_backfill(
        instrument_id="si_futures_family",
        backfill_run_id="inc_v1",
        date_end="2026-08-19",
    )

    assert result["status"] == "accepted"
    assert result["incremental_partition_count"] == 1
    assert result["incremental_row_count"] == 2
    assert result["cumulative_partition_count"] == 1758
    assert result["cumulative_row_count"] == 597652
    assert result["accepted_partition_snapshots_immutable"] is True
    assert result["full_raw_contract_revalidated"] is True
    assert result["registry_binding_revalidated"] is True
    assert historical_pointer.read_bytes() == before
    pointer = json.loads(acceptance.incremental_pointer_path(data_root, "si_futures_family").read_text())
    manifest_path = data_root / pointer["manifest_ref"][len("${MOEX_DATA_ROOT}/") :]
    manifest = json.loads(manifest_path.read_text())
    assert manifest["parent_kind"] == "historical_stage2"
    assert manifest["partitions"][0]["clgroups"] == ["FIZ", "YUR"]
    assert manifest["partitions"][0]["secid"] == "SiU6"
    accepted_partition = data_root / manifest["partitions"][0]["accepted_partition_ref"][len("${MOEX_DATA_ROOT}/") :]
    assert accepted_partition.read_bytes() == canonical_bytes
    assert manifest["directional_signal_authority"] is False
    assert manifest["trading_action_authority"] is False


def test_accepted_snapshot_survives_later_canonical_replacement(data_root: Path) -> None:
    _historical_state(data_root)
    partitions = _backfill_evidence(
        data_root,
        run_id="snapshot_v1",
        requested_from="2026-08-18",
        requested_till="2026-08-18",
        written_dates=("2026-08-18",),
    )
    original = partitions[0].read_bytes()
    acceptance.accept_incremental_backfill(
        instrument_id="si_futures_family", backfill_run_id="snapshot_v1", date_end="2026-08-18"
    )
    pointer = json.loads(acceptance.incremental_pointer_path(data_root, "si_futures_family").read_text())
    manifest_path = data_root / pointer["manifest_ref"][len("${MOEX_DATA_ROOT}/") :]
    manifest = json.loads(manifest_path.read_text())
    accepted_partition = data_root / manifest["partitions"][0]["accepted_partition_ref"][len("${MOEX_DATA_ROOT}/") :]

    partitions[0].write_bytes(b"replacement")

    assert accepted_partition.read_bytes() == original
    assert accepted_partition.read_bytes() != partitions[0].read_bytes()


def test_second_increment_chains_from_previous_increment(data_root: Path) -> None:
    _historical_state(data_root)
    _backfill_evidence(
        data_root,
        run_id="inc_v1",
        requested_from="2026-08-18",
        requested_till="2026-08-18",
        written_dates=("2026-08-18",),
    )
    acceptance.accept_incremental_backfill(
        instrument_id="si_futures_family", backfill_run_id="inc_v1", date_end="2026-08-18"
    )
    _backfill_evidence(
        data_root,
        run_id="inc_v2",
        requested_from="2026-08-19",
        requested_till="2026-08-20",
        written_dates=("2026-08-20",),
        skipped_dates=("2026-08-19",),
    )

    result = acceptance.accept_incremental_backfill(
        instrument_id="si_futures_family", backfill_run_id="inc_v2", date_end="2026-08-20"
    )

    assert result["cumulative_till"] == "2026-08-20"
    assert result["cumulative_partition_count"] == 1759
    pointer = json.loads(acceptance.incremental_pointer_path(data_root, "si_futures_family").read_text())
    manifest_path = data_root / pointer["manifest_ref"][len("${MOEX_DATA_ROOT}/") :]
    manifest = json.loads(manifest_path.read_text())
    assert manifest["parent_kind"] == "incremental"
    assert manifest["parent_accepted_till"] == "2026-08-18"


def test_rejects_gap_from_parent_end(data_root: Path) -> None:
    _historical_state(data_root)
    _backfill_evidence(
        data_root,
        run_id="gap_v1",
        requested_from="2026-08-19",
        requested_till="2026-08-19",
        written_dates=("2026-08-19",),
    )
    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="parent end plus one"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family", backfill_run_id="gap_v1", date_end="2026-08-19"
        )
    assert not acceptance.incremental_pointer_path(data_root, "si_futures_family").exists()


def test_rejects_partition_without_both_groups(data_root: Path) -> None:
    _historical_state(data_root)
    _backfill_evidence(
        data_root,
        run_id="bad_groups_v1",
        requested_from="2026-08-18",
        requested_till="2026-08-18",
        written_dates=("2026-08-18",),
        groups=("FIZ",),
    )
    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="FIZ and YUR"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family", backfill_run_id="bad_groups_v1", date_end="2026-08-18"
        )
    assert not acceptance.incremental_pointer_path(data_root, "si_futures_family").exists()


def test_recomputes_partition_quality_instead_of_trusting_aggregate_report(data_root: Path) -> None:
    _historical_state(data_root)
    _backfill_evidence(
        data_root,
        run_id="invalid_position_v1",
        requested_from="2026-08-18",
        requested_till="2026-08-18",
        written_dates=("2026-08-18",),
        invalid_partition_position=True,
    )
    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="invalid_position_count"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family", backfill_run_id="invalid_position_v1", date_end="2026-08-18"
        )
    assert not acceptance.incremental_pointer_path(data_root, "si_futures_family").exists()


def test_rejects_missing_raw_contract_column(data_root: Path) -> None:
    _historical_state(data_root)
    _backfill_evidence(
        data_root,
        run_id="missing_contract_col_v1",
        requested_from="2026-08-18",
        requested_till="2026-08-18",
        written_dates=("2026-08-18",),
        drop_partition_column="availability_ts_utc",
    )
    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="raw-contract required columns"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family", backfill_run_id="missing_contract_col_v1", date_end="2026-08-18"
        )


def test_rejects_registry_secid_mismatch(data_root: Path) -> None:
    _historical_state(data_root)
    _backfill_evidence(
        data_root,
        run_id="wrong_secid_v1",
        requested_from="2026-08-18",
        requested_till="2026-08-18",
        written_dates=("2026-08-18",),
        partition_secid="WrongSecid",
    )
    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="registry binding mismatch: secid"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family", backfill_run_id="wrong_secid_v1", date_end="2026-08-18"
        )


def test_rejects_symlink_partition_before_resolution(data_root: Path) -> None:
    _historical_state(data_root)
    partitions = _backfill_evidence(
        data_root,
        run_id="symlink_v1",
        requested_from="2026-08-18",
        requested_till="2026-08-18",
        written_dates=("2026-08-18",),
    )
    canonical = partitions[0]
    target = data_root / "other.parquet"
    target.write_bytes(canonical.read_bytes())
    canonical.unlink()
    canonical.symlink_to(target)
    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="must not be a symlink"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family", backfill_run_id="symlink_v1", date_end="2026-08-18"
        )


def test_failed_second_increment_leaves_pointer_unchanged(data_root: Path) -> None:
    _historical_state(data_root)
    _backfill_evidence(
        data_root,
        run_id="inc_v1",
        requested_from="2026-08-18",
        requested_till="2026-08-18",
        written_dates=("2026-08-18",),
    )
    acceptance.accept_incremental_backfill(
        instrument_id="si_futures_family", backfill_run_id="inc_v1", date_end="2026-08-18"
    )
    pointer_path = acceptance.incremental_pointer_path(data_root, "si_futures_family")
    before = pointer_path.read_bytes()
    _backfill_evidence(
        data_root,
        run_id="inc_bad_v2",
        requested_from="2026-08-19",
        requested_till="2026-08-19",
        written_dates=("2026-08-19",),
        aggregate_duplicate_key_count=1,
    )
    with pytest.raises(acceptance.FutoiIncrementalAcceptanceError, match="quality defect"):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family", backfill_run_id="inc_bad_v2", date_end="2026-08-19"
        )
    assert pointer_path.read_bytes() == before
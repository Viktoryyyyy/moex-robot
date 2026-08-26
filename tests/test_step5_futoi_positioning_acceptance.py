from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data import step5_futoi_positioning_acceptance as acceptance
from moex_data.futures.freeze_accepted_futoi_history import freeze_accepted_history
from moex_data.futures.materialize_futoi_eod import FutoiEodError, materialize_eod_history, raw_partition_path
from moex_data.futures.materialize_futoi_positioning_features_d1 import materialize_features

ROOT_PREFIX = "${MOEX_DATA_ROOT}/"


def _identity(instrument_id: str) -> tuple[str, str]:
    if instrument_id == "si_futures_family":
        return "SiU6", "si"
    if instrument_id == "cr_futures_family":
        return "CRU6", "cr"
    raise AssertionError(instrument_id)


def _raw_day(instrument_id: str, trade_date: str, n: int) -> pd.DataFrame:
    secid, source_ticker = _identity(instrument_id)
    phys_net = 10 + n
    phys_long = 55 + n
    phys_short = -(phys_long - phys_net)
    legal_net = -phys_net
    legal_long = 45
    legal_short = -(legal_long - legal_net)
    common = {
        "instrument_id": instrument_id,
        "trade_date": trade_date,
        "ts": trade_date + " 20:00:00",
        "moment": trade_date + " 20:00:00",
        "sess_id": 1,
        "seqnum": n + 1,
        "secid": secid,
        "board": "RFUD",
        "market": "forts",
        "engine": "futures",
        "source_id": "moex_algopack_futoi",
        "source_ticker": source_ticker,
        "availability_ts_utc": trade_date + "T17:01:00+00:00",
        "ingest_ts": trade_date + "T17:02:00+00:00",
    }
    return pd.DataFrame([
        {**common,"systime":trade_date+" 20:00:10","clgroup":"FIZ","pos":phys_net,"pos_long":phys_long,"pos_short":phys_short,"pos_long_num":10,"pos_short_num":9},
        {**common,"systime":trade_date+" 20:00:12","clgroup":"YUR","pos":legal_net,"pos_long":legal_long,"pos_short":legal_short,"pos_long_num":8,"pos_short_num":11},
    ])


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _rooted(root: Path, path: Path) -> str:
    return ROOT_PREFIX + path.relative_to(root).as_posix()


def _date_digest(values: list[str]) -> str:
    payload = (("\n".join(values) + "\n") if values else "").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _install_accepted_raw_history(root: Path, instrument_id: str, dates: list[str]) -> None:
    acceptance_run_id = "raw_fixture_" + instrument_id
    manifest_dir = root / "state" / "accepted_manifests" / "target_dataset_id=futures_futoi_raw" / f"instrument_id={instrument_id}" / f"acceptance_run_id={acceptance_run_id}"
    snapshot = manifest_dir / "acceptance_report_snapshot.json"
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    snapshot.write_text('{"acceptance_status":"pass"}\n', encoding="utf-8")
    snapshot_sha = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    manifest = manifest_dir / "accepted_manifest.json"
    manifest_values = {
        "schema_version":"futures_raw_history_accepted_manifest.v1",
        "producer":"moex_data.futures.stage2_raw_history_promotion.v1",
        "dataset_id":"futures_raw_history_accepted_manifest",
        "target_dataset_id":"futures_futoi_raw",
        "instrument_id":instrument_id,
        "acceptance_run_id":acceptance_run_id,
        "source_id":"moex_algopack_futoi",
        "requested_from":dates[0],
        "requested_till":dates[-1],
        "partition_count":len(dates),
        "row_count":len(dates) * 2,
        "partition_dates_sha256":_date_digest(dates),
        "missing_partition_dates":[],
        "missing_dates_sha256":_date_digest([]),
        "calendar_missing_partition_count":0,
        "acceptance_status":"pass",
        "network_access_used":False,
        "historical_backfill_used":False,
        "acceptance_report_ref":_rooted(root, snapshot),
        "acceptance_report_sha256":snapshot_sha,
    }
    _write_json(manifest, manifest_values)
    pointer = root / "state" / "datasets" / "dataset_id=futures_futoi_raw" / f"instrument_id={instrument_id}" / "current_accepted_manifest.json"
    _write_json(pointer, {
        "dataset_id":"futures_futoi_raw",
        "instrument_id":instrument_id,
        "run_id":acceptance_run_id,
        "manifest_ref":_rooted(root, manifest),
        "quality_report_ref":_rooted(root, snapshot),
        "acceptance_report_ref":_rooted(root, snapshot),
        "quality_status":"pass",
        "acceptance_status":"pass",
        "promotion_basis":"raw_history_acceptance",
    })


def _materialize_fixture(root: Path, instrument_id: str, dates: list[str], run_root: Path, run_id: str) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    for n, trade_date in enumerate(dates):
        path = raw_partition_path(root, instrument_id, trade_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        _raw_day(instrument_id, trade_date, n).to_parquet(path, index=False)
    _install_accepted_raw_history(root, instrument_id, dates)
    frozen = freeze_accepted_history(
        data_root=root,
        output_root=run_root,
        repo_root=Path.cwd(),
        instrument_id=instrument_id,
        start_date=dates[0],
        end_date=dates[-1],
        run_id=f"{run_id}_{instrument_id}_raw_freeze",
    )
    eod = materialize_eod_history(
        data_root=root,
        output_root=run_root,
        frozen_input_manifest=frozen["manifest_path"],
        instrument_id=instrument_id,
        start_date=dates[0],
        end_date=dates[-1],
        run_id=f"{run_id}_{instrument_id}_eod",
    )
    features = materialize_features(
        eod_partition=eod["partition_path"],
        output_root=run_root,
        instrument_id=instrument_id,
        run_id=f"{run_id}_{instrument_id}_features",
    )
    return frozen, eod, features


def _pilot_evidence(
    run_id: str,
    run_root: Path,
    dates: list[str],
    frozen_outputs: list[dict[str, object]],
    eod_outputs: list[dict[str, object]],
    feature_outputs: list[dict[str, object]],
) -> dict[str, object]:
    histories = {
        instrument_id: {
            "start_date": dates[0],
            "end_date": dates[-1],
            "expected_raw_partitions": len(dates),
            "expected_eod_rows": len(dates),
            "source_quality_omissions": [],
        }
        for instrument_id in ("si_futures_family", "cr_futures_family")
    }
    return {
        "project":"MOEX_Bot","step":5,"status":"pilot_passed","artifact_version":run_id,"run_id":run_id,
        "run_root":run_root.as_posix(),"run_artifacts_immutable":True,"run_id_reuse_allowed":False,
        "raw_ingestion_changed":False,"network_calls_used":False,"latest_autodetect_used":False,
        "canonical_raw_partition_reads_after_freeze_used":False,
        "immutable_raw_input_freeze_used":True,
        "raw_input_freeze_mode":"create_only_hardlink_same_validated_inode",
        "root_aggregate_semantics":True,"front_next_split_claimed":False,"historical_pit_research_ready_claimed":False,
        "revision_policy":"same_analytical_key_single_sess_id_then_max_seqnum",
        "snapshot_policy":"latest_resolved_complete_balanced_FIZ_YUR_event_ts",
        "source_quality_omission_policy":"explicit_attested_date_only_fail_closed_otherwise",
        "counts":{
            "mandatory_instruments":2,
            "frozen_raw_inputs":len(frozen_outputs),
            "eod_outputs":len(eod_outputs),
            "feature_outputs":len(feature_outputs),
            "source_quality_omission_count":0,
            "expected_accepted_pointers":4,
        },
        "histories":histories,
        "frozen_inputs":frozen_outputs,"eod_outputs":eod_outputs,"feature_outputs":feature_outputs,
    }


def test_stage5_end_to_end_acceptance_promotes_four_validated_pointers(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    dates = pd.date_range("2026-08-01", periods=6, freq="D").strftime("%Y-%m-%d").tolist()
    run_id = "step5_fixture_v1"
    run_root = tmp_path / "runs" / "step5_futoi_positioning" / f"run_id={run_id}"
    frozen_outputs: list[dict[str, object]] = []
    eod_outputs: list[dict[str, object]] = []
    feature_outputs: list[dict[str, object]] = []

    for instrument_id in ("si_futures_family", "cr_futures_family"):
        monkeypatch.setitem(acceptance.EXPECTED_ROWS, instrument_id, 6)
        frozen, eod, features = _materialize_fixture(tmp_path, instrument_id, dates, run_root, run_id)
        frozen_outputs.append(frozen)
        eod_outputs.append(eod)
        feature_outputs.append(features)

    evidence = _pilot_evidence(run_id, run_root, dates, frozen_outputs, eod_outputs, feature_outputs)
    _write_json(tmp_path / "state" / "acceptance" / "step5_futoi_positioning" / f"run_id={run_id}" / "pilot_evidence.json", evidence)

    result = acceptance.promote(run_id=run_id)
    assert result["status"] == "accepted"
    assert result["accepted_pointer_count"] == 4
    assert result["immutable_frozen_raw_input_verified"] is True
    for dataset_id in ("futures_futoi_eod", "futures_futoi_positioning_features_d1"):
        for instrument_id in ("si_futures_family", "cr_futures_family"):
            pointer = tmp_path / "state" / "datasets" / f"dataset_id={dataset_id}" / f"instrument_id={instrument_id}" / "current_accepted_manifest.json"
            assert pointer.is_file()
            values = json.loads(pointer.read_text(encoding="utf-8"))
            assert values["acceptance_run_id"] == run_id
            assert values["quality_status"] == "pass"
            assert values["immutable_frozen_raw_input_verified"] is True
            assert values["historical_pit_research_ready_claimed"] is False


def test_freeze_refuses_physical_raw_partition_without_accepted_history(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    trade_date = "2026-08-17"
    path = raw_partition_path(tmp_path, "si_futures_family", trade_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    _raw_day("si_futures_family", trade_date, 0).to_parquet(path, index=False)
    with pytest.raises((FutoiEodError, ValueError), match="accepted raw pointer"):
        freeze_accepted_history(
            data_root=tmp_path,
            output_root=tmp_path / "run",
            repo_root=Path.cwd(),
            instrument_id="si_futures_family",
            start_date=trade_date,
            end_date=trade_date,
            run_id="unaccepted_raw_fixture",
        )


def test_canonical_raw_replacement_after_freeze_does_not_change_eod_input(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    instrument_id = "si_futures_family"
    dates = ["2026-08-17"]
    source_path = raw_partition_path(tmp_path, instrument_id, dates[0])
    source_path.parent.mkdir(parents=True, exist_ok=True)
    original = _raw_day(instrument_id, dates[0], 0)
    original.to_parquet(source_path, index=False)
    _install_accepted_raw_history(tmp_path, instrument_id, dates)
    run_root = tmp_path / "runs" / "step5_futoi_positioning" / "run_id=replacement"
    frozen = freeze_accepted_history(
        data_root=tmp_path,
        output_root=run_root,
        repo_root=Path.cwd(),
        instrument_id=instrument_id,
        start_date=dates[0],
        end_date=dates[-1],
        run_id="replacement_freeze",
    )
    replacement = _raw_day(instrument_id, dates[0], 99)
    temp = source_path.with_name("replacement.parquet")
    replacement.to_parquet(temp, index=False)
    temp.replace(source_path)

    eod = materialize_eod_history(
        data_root=tmp_path,
        output_root=run_root,
        frozen_input_manifest=frozen["manifest_path"],
        instrument_id=instrument_id,
        start_date=dates[0],
        end_date=dates[-1],
        run_id="replacement_eod",
    )
    frame = pd.read_parquet(eod["partition_path"])
    assert int(frame.iloc[0]["phys_net"]) == 10
    assert eod["canonical_raw_partition_reads_used"] is False


def test_acceptance_rejects_tampered_frozen_partition(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    dates = pd.date_range("2026-08-01", periods=6, freq="D").strftime("%Y-%m-%d").tolist()
    run_root = tmp_path / "runs" / "step5_futoi_positioning" / "run_id=frozen_corruption"
    frozen, eod, _ = _materialize_fixture(tmp_path, "si_futures_family", dates, run_root, "frozen_corruption")
    manifest = json.loads(Path(str(frozen["manifest_path"])).read_text(encoding="utf-8"))
    frozen_ref = str(manifest["records"][0]["frozen_partition_ref"])
    frozen_path = tmp_path / frozen_ref[len(ROOT_PREFIX):]
    frozen_path.write_bytes(frozen_path.read_bytes() + b"tamper")
    eod_manifest = json.loads(Path(str(eod["manifest_path"])).read_text(encoding="utf-8"))
    with pytest.raises(ValueError, match="frozen partition content SHA-256 mismatch"):
        acceptance._validate_frozen_input(eod_manifest, "si_futures_family", run_root, 6)


def test_acceptance_rejects_corrupted_eod_derived_metric(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    dates = pd.date_range("2026-08-01", periods=6, freq="D").strftime("%Y-%m-%d").tolist()
    run_root = tmp_path / "runs" / "step5_futoi_positioning" / "run_id=metric_corruption"
    _, eod, _ = _materialize_fixture(tmp_path, "si_futures_family", dates, run_root, "metric_corruption")
    path = Path(str(eod["partition_path"]))
    frame = pd.read_parquet(path)
    frame.loc[0, "phys_net_share_of_oi"] = float(frame.loc[0, "phys_net_share_of_oi"]) + 0.01
    frame.to_parquet(path, index=False)
    with pytest.raises(ValueError, match="phys_net_share_of_oi"):
        acceptance._validate_eod(path, "si_futures_family", 6)


def test_acceptance_rejects_feature_base_history_not_equal_to_eod(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    dates = pd.date_range("2026-08-01", periods=6, freq="D").strftime("%Y-%m-%d").tolist()
    run_root = tmp_path / "runs" / "step5_futoi_positioning" / "run_id=feature_corruption"
    _, eod, features = _materialize_fixture(tmp_path, "si_futures_family", dates, run_root, "feature_corruption")
    feature_path = Path(str(features["partition_path"]))
    frame = pd.read_parquet(feature_path)
    frame.loc[2, "phys_net_share_of_oi"] = float(frame.loc[2, "phys_net_share_of_oi"]) + 0.02
    frame.to_parquet(feature_path, index=False)
    with pytest.raises(ValueError, match="base-column mismatch"):
        acceptance._validate_feature_source_alignment(feature_path, Path(str(eod["partition_path"])))

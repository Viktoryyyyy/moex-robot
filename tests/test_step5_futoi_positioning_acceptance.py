from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from moex_data import step5_futoi_positioning_acceptance as acceptance
from moex_data.futures.materialize_futoi_eod import materialize_eod_history, raw_partition_path
from moex_data.futures.materialize_futoi_positioning_features_d1 import materialize_features


def _raw_day(instrument_id: str, trade_date: str, n: int) -> pd.DataFrame:
    phys_net = 10 + n
    phys_long = 55 + n
    phys_short = -(phys_long - phys_net)
    legal_net = -phys_net
    legal_long = 45
    legal_short = -(legal_long - legal_net)
    return pd.DataFrame([
        {"instrument_id":instrument_id,"trade_date":trade_date,"ts":trade_date+" 20:00:00","systime":trade_date+" 20:00:10","sess_id":1,"seqnum":n+1,"clgroup":"FIZ","pos":phys_net,"pos_long":phys_long,"pos_short":phys_short,"pos_long_num":10,"pos_short_num":9,"availability_ts_utc":trade_date+"T17:01:00+00:00"},
        {"instrument_id":instrument_id,"trade_date":trade_date,"ts":trade_date+" 20:00:00","systime":trade_date+" 20:00:12","sess_id":1,"seqnum":n+1,"clgroup":"YUR","pos":legal_net,"pos_long":legal_long,"pos_short":legal_short,"pos_long_num":8,"pos_short_num":11,"availability_ts_utc":trade_date+"T17:01:00+00:00"},
    ])


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_stage5_end_to_end_acceptance_promotes_four_validated_pointers(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", tmp_path.as_posix())
    dates = pd.date_range("2026-08-01", periods=6, freq="D").strftime("%Y-%m-%d").tolist()
    run_id = "step5_fixture_v1"
    run_root = tmp_path / "runs" / "step5_futoi_positioning" / f"run_id={run_id}"
    eod_outputs = []
    feature_outputs = []

    for instrument_id in ("si_futures_family", "cr_futures_family"):
        monkeypatch.setitem(acceptance.EXPECTED_ROWS, instrument_id, 6)
        for n, trade_date in enumerate(dates):
            path = raw_partition_path(tmp_path, instrument_id, trade_date)
            path.parent.mkdir(parents=True, exist_ok=True)
            _raw_day(instrument_id, trade_date, n).to_parquet(path, index=False)
        eod = materialize_eod_history(
            data_root=tmp_path,
            output_root=run_root,
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
        eod_outputs.append(eod)
        feature_outputs.append(features)

    evidence = {
        "project":"MOEX_Bot","step":5,"status":"pilot_passed","artifact_version":run_id,"run_id":run_id,
        "run_root":run_root.as_posix(),"run_artifacts_immutable":True,"run_id_reuse_allowed":False,
        "raw_ingestion_changed":False,"network_calls_used":False,"latest_autodetect_used":False,
        "root_aggregate_semantics":True,"front_next_split_claimed":False,"historical_pit_research_ready_claimed":False,
        "revision_policy":"same_analytical_key_single_sess_id_then_max_seqnum",
        "snapshot_policy":"max_resolved_ts_requires_FIZ_and_YUR",
        "eod_outputs":eod_outputs,"feature_outputs":feature_outputs,
    }
    _write_json(tmp_path / "state" / "acceptance" / "step5_futoi_positioning" / f"run_id={run_id}" / "pilot_evidence.json", evidence)

    result = acceptance.promote(run_id=run_id)
    assert result["status"] == "accepted"
    assert result["accepted_pointer_count"] == 4
    for dataset_id in ("futures_futoi_eod", "futures_futoi_positioning_features_d1"):
        for instrument_id in ("si_futures_family", "cr_futures_family"):
            pointer = tmp_path / "state" / "datasets" / f"dataset_id={dataset_id}" / f"instrument_id={instrument_id}" / "current_accepted_manifest.json"
            assert pointer.is_file()
            values = json.loads(pointer.read_text(encoding="utf-8"))
            assert values["acceptance_run_id"] == run_id
            assert values["quality_status"] == "pass"
            assert values["historical_pit_research_ready_claimed"] is False

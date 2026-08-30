from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from moex_data import step10_rub_refresh_scheduler as step10
from moex_data import step7_rub_native_d1_w1_materializer as step7


def _base_frame(instrument_id: str, trade_date: str = "2026-08-17") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_id": instrument_id,
                "trade_date": trade_date,
                "period_start_date": trade_date,
                "period_end_date": trade_date,
                "timeframe": "1D",
                "secid": "USDRUBF" if instrument_id.startswith("usd") else "CNYRUBF",
                "availability_ts_utc": "2026-08-18T03:00:00+00:00",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.0,
                "volume": 1.0,
                "value": 1.0,
                "num_trades": 1.0,
                "source_row_count": 1,
                "source_period_count": 1,
                "source_lineage_sha256": "a" * 64,
            }
        ]
    )


def test_parse_args_requires_explicit_date_and_run_id() -> None:
    with pytest.raises(SystemExit):
        step10.parse_args([])
    args = step10.parse_args(["--through-date", "2026-08-19", "--run-id", "step10_test"])
    assert args.through_date == "2026-08-19"
    assert args.run_id == "step10_test"


def test_through_date_must_be_completed_moscow_date(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    repo = tmp_path / "repo"
    (repo / "configs" / "datasets").mkdir(parents=True)
    (repo / "configs" / "datasets" / "step9_rub_analysis_bundle.v1.yaml").write_text("version: 1\n", encoding="utf-8")
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())
    with pytest.raises(step10.Step10RefreshError, match="completed Moscow calendar date"):
        step10.run_refresh(
            through_date="2026-08-20",
            run_id="step10_same_day",
            repo_root=repo,
            env_file=None,
            now_utc=datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc),
        )


def test_stage7_frozen_manifest_is_compatible_with_materializer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())
    run_root = root / "runs" / "step10_rub_daily_refresh" / "run_id=test"
    frozen = run_root / "inputs" / "stage7_raw" / "instrument_id=usdrubf_futures_family" / "trade_date=2026-08-18" / "part.parquet"
    frozen.parent.mkdir(parents=True)
    frame = pd.DataFrame(
        [
            {
                "instrument_id": "usdrubf_futures_family",
                "trade_date": "2026-08-18",
                "ts": "2026-08-18T10:00:00",
                "secid": "USDRUBF",
                "open": 84.0,
                "high": 84.2,
                "low": 83.9,
                "close": 84.1,
                "volume": 10,
                "value": 840,
                "num_trades": 2,
            },
            {
                "instrument_id": "usdrubf_futures_family",
                "trade_date": "2026-08-18",
                "ts": "2026-08-18T18:00:00",
                "secid": "USDRUBF",
                "open": 84.1,
                "high": 84.5,
                "low": 84.0,
                "close": 84.4,
                "volume": 20,
                "value": 1688,
                "num_trades": 3,
            },
        ]
    )
    frame.to_parquet(frozen, index=False)
    digest = hashlib.sha256(frozen.read_bytes()).hexdigest()
    records = [
        {
            "trade_date": "2026-08-18",
            "instrument_id": "usdrubf_futures_family",
            "sha256": digest,
            "frozen_ref": "${MOEX_DATA_ROOT}/" + frozen.relative_to(root).as_posix(),
            "canonical_ref": "${MOEX_DATA_ROOT}/market/raw/example.parquet",
            "independent_inode_exact_byte_copy": True,
        }
    ]
    manifest = step10._write_step7_frozen_manifest(
        root=root,
        run_root=run_root,
        instrument_id="usdrubf_futures_family",
        records=records,
    )
    d1 = step7.build_d1(
        data_root=root,
        frozen_manifest_path=manifest,
        instrument_id="usdrubf_futures_family",
        history_start="2026-08-18",
        history_end="2026-08-18",
    )
    assert len(d1) == 1
    assert d1.iloc[0]["open"] == pytest.approx(84.0)
    assert d1.iloc[0]["close"] == pytest.approx(84.4)
    assert d1.iloc[0]["source_row_count"] == 2
    assert d1.iloc[0]["source_lineage_sha256"] == digest


def test_stage5_output_support_identity_and_pointer_hashes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())
    run_root = root / "runs" / "step10_rub_daily_refresh" / "run_id=test"
    frame = pd.DataFrame(
        [
            {
                "instrument_id": "si_futures_family",
                "trade_date": "2026-08-18",
                "snapshot_ts_utc": "2026-08-18T20:50:00+00:00",
                "availability_ts_utc": "2026-08-19T00:10:00+00:00",
            }
        ]
    )
    output = step10._write_stage5_output(
        root=root,
        run_root=run_root,
        dataset_id="futures_futoi_eod",
        instrument_id="si_futures_family",
        producer_run_id="test_si_eod",
        frame=frame,
        source_refs=["${MOEX_DATA_ROOT}/inputs/raw.parquet"],
    )
    manifest = json.loads(Path(output["manifest_path"]).read_text(encoding="utf-8"))
    quality = json.loads(Path(output["quality_path"]).read_text(encoding="utf-8"))
    assert manifest["dataset_id"] == "futures_futoi_eod"
    assert manifest["instrument_id"] == "si_futures_family"
    assert manifest["run_id"] == "test_si_eod"
    assert manifest["quality_status"] == "pass"
    assert quality["quality_status"] == "pass"
    assert Path(output["partition_path"]).is_file()


def test_stage3_stage4_keep_completed_trade_date_and_live_reference_date(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, str]] = []

    def pilot3(**kwargs):
        calls.append(("stage3", kwargs["trade_date"], kwargs["as_of_date"]))
        return {"status": "pilot_passed"}

    def pilot4(**kwargs):
        calls.append(("stage4", kwargs["trade_date"], kwargs["as_of_date"]))
        return {"status": "pilot_passed"}

    monkeypatch.setattr(step10.step3_pilot, "run_pilot", pilot3)
    monkeypatch.setattr(step10.step4_pilot, "run_pilot", pilot4)
    monkeypatch.setattr(step10.step3_acceptance, "promote_step3_pilot", lambda **_kwargs: {"status": "accepted", "accepted_pointer_count": 10})
    monkeypatch.setattr(step10.step4_acceptance, "promote", lambda **_kwargs: {"status": "accepted", "accepted_pointer_count": 2})

    result = step10._run_stage3_stage4(
        latest_trade_date="2026-08-19",
        reference_date="2026-08-20",
        run_id="scheduler",
        env_file="/tmp/unused",
        timeout=1.0,
    )
    assert result["trade_date"] == "2026-08-19"
    assert calls == [
        ("stage3", "2026-08-19", "2026-08-20"),
        ("stage4", "2026-08-19", "2026-08-20"),
    ]


def test_pointer_preserves_stage_contract_and_records_scheduler_provenance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    pointer_path = root / "state" / "pointer.json"
    monkeypatch.setattr(step10, "_spec", lambda *_args: object())
    monkeypatch.setattr(step10, "_pointer_path", lambda _root, _spec: pointer_path)

    def build(dataset_id: str, instrument_id: str, timeframe: str | None) -> dict[str, object]:
        base = root / dataset_id
        base.mkdir()
        output: dict[str, object] = {
            "dataset_id": dataset_id,
            "instrument_id": instrument_id,
            "timeframe": timeframe,
            "run_id": "producer",
        }
        for key, name in (("partition_path", "part.parquet"), ("manifest_path", "manifest.json"), ("quality_path", "quality.json")):
            path = base / name
            path.write_bytes(b"x")
            output[key] = path
        return output

    _, stage5 = step10._pointer_from_output(root, build("futures_futoi_eod", "si_futures_family", None), "scheduler")
    _, stage7 = step10._pointer_from_output(root, build("rub_native_ohlcv_htf", "usdrubf_futures_family", "1D"), "scheduler")
    assert stage5["acceptance_contract_id"] == step10.STAGE5_ACCEPTANCE_CONTRACT_ID
    assert stage7["acceptance_contract_id"] == step10.STAGE7_ACCEPTANCE_CONTRACT_ID
    assert stage5["scheduler_contract_id"] == step10.CONTRACT_ID
    assert stage7["scheduler_contract_id"] == step10.CONTRACT_ID


def test_stage7_weekly_boundary_rebuild_emits_coherent_eight_with_exact_snapshot_lineage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    run_root = root / "runs" / "step10_rub_daily_refresh" / "run_id=sunday"
    base_frames = {name: _base_frame(name, "2026-08-21") for name in step10.STAGE7_INSTRUMENTS}
    writes: list[dict[str, object]] = []

    def fake_build_w1(frame, *, history_start, history_end):
        assert history_end == "2026-08-23"
        result = frame.copy()
        result["timeframe"] = "1W"
        return result

    monkeypatch.setattr(step10.step7_materializer, "build_w1", fake_build_w1)
    monkeypatch.setattr(step10.step7_materializer, "build_technical_features", lambda frame, **_kwargs: frame.copy())

    def fake_write_output(**kwargs):
        base = run_root / "fake" / str(kwargs["dataset_id"]) / str(kwargs["instrument_id"]) / str(kwargs["timeframe"])
        partition = base / "part.parquet"
        manifest = base / "manifest.json"
        quality = base / "quality.json"
        partition.parent.mkdir(parents=True, exist_ok=True)
        partition.write_bytes(b"partition")
        manifest.write_text("{}\n", encoding="utf-8")
        quality.write_text("{}\n", encoding="utf-8")
        writes.append(dict(kwargs))
        return {
            "dataset_id": kwargs["dataset_id"],
            "instrument_id": kwargs["instrument_id"],
            "timeframe": kwargs["timeframe"],
            "run_id": kwargs["producer_run_id"],
            "partition_path": partition,
            "manifest_path": manifest,
            "quality_report_path": quality,
            "row_count": len(kwargs["frame"].index),
        }

    monkeypatch.setattr(step10.step7_materializer, "_write_output", fake_write_output)
    outputs = step10._stage7_refresh(
        root=root,
        run_root=run_root,
        run_id="sunday",
        base_frames=base_frames,
        trading_dates=[],
        rebuild_weekly=True,
        weekly_boundary_end="2026-08-23",
        timeout=1.0,
    )
    assert len(outputs) == 8
    assert len(writes) == 8
    for instrument in step10.STAGE7_INSTRUMENTS:
        snapshot = run_root / "inputs" / "stage7_base_d1" / ("instrument_id=" + instrument) / "part.parquet"
        assert snapshot.is_file()
        instrument_writes = [item for item in writes if item["instrument_id"] == instrument]
        d1 = next(item for item in instrument_writes if item["dataset_id"] == "rub_native_ohlcv_htf" and item["timeframe"] == "1D")
        w1 = next(item for item in instrument_writes if item["dataset_id"] == "rub_native_ohlcv_htf" and item["timeframe"] == "1W")
        assert str(d1["source_ref"]).endswith("/inputs/stage7_base_d1/instrument_id=" + instrument + "/part.parquet")
        assert str(w1["source_ref"]).endswith("/fake/rub_native_ohlcv_htf/" + instrument + "/1D/part.parquet")


def test_capture_written_pointer_state_only_owns_exact_published_bytes(tmp_path: Path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first_values = {"id": "first"}
    second_values = {"id": "second"}
    first.write_bytes(step10._pointer_payload_bytes(first_values))
    second.write_bytes(b"concurrent publication\n")
    captured = step10._capture_written_pointer_state([(first, first_values), (second, second_values)])
    assert captured == {first: step10._pointer_payload_bytes(first_values)}


def test_restore_skips_pointer_replaced_by_concurrent_publisher(tmp_path: Path) -> None:
    pointer = tmp_path / "pointer.json"
    pointer.write_bytes(b"concurrent\n")
    step10._restore_pointer_snapshot(
        {pointer: b"old\n"},
        {pointer: b"stage10\n"},
    )
    assert pointer.read_bytes() == b"concurrent\n"


def test_run_refresh_orders_catchup_and_post_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    repo = tmp_path / "repo"
    (repo / "configs" / "datasets").mkdir(parents=True)
    (repo / "configs" / "datasets" / "step9_rub_analysis_bundle.v1.yaml").write_text("version: 1\n", encoding="utf-8")
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())

    pointer = root / "state" / "dummy.json"
    pointer.parent.mkdir(parents=True)
    pointer.write_bytes(b"old")
    calls: list[str] = []

    monkeypatch.setattr(step10, "_snapshot_pointers", lambda _root: {pointer: b"old"})
    monkeypatch.setattr(
        step10,
        "_load_stage5_base",
        lambda _root, _as_of: ("2026-08-17", {name: pd.DataFrame({"instrument_id": [name], "trade_date": ["2026-08-17"]}) for name in step10.STAGE5_INSTRUMENTS}),
    )
    monkeypatch.setattr(
        step10,
        "_load_stage7_base",
        lambda _root, _as_of: ("2026-08-17", {name: _base_frame(name) for name in step10.STAGE7_INSTRUMENTS}),
    )
    monkeypatch.setattr(step10, "_calendar_dates", lambda **_kwargs: ["2026-08-18", "2026-08-19"])

    def fake_stage5(**kwargs):
        calls.append("stage5")
        assert kwargs["trading_dates"] == ["2026-08-18", "2026-08-19"]
        return [{"id": i} for i in range(4)]

    def fake_stage7(**kwargs):
        calls.append("stage7")
        assert kwargs["trading_dates"] == ["2026-08-18", "2026-08-19"]
        return [{"id": i} for i in range(8)]

    monkeypatch.setattr(step10, "_stage5_refresh", fake_stage5)
    monkeypatch.setattr(step10, "_stage7_refresh", fake_stage7)
    monkeypatch.setattr(step10, "_latest_source_dates", lambda _root, _as_of: ("2026-08-17", "2026-08-17"))

    def fake_sources(**kwargs):
        calls.append("stage3_stage4")
        assert kwargs["latest_trade_date"] == "2026-08-19"
        assert kwargs["reference_date"] == "2026-08-20"
        return {"status": "refreshed", "trade_date": "2026-08-19"}

    monkeypatch.setattr(step10, "_run_stage3_stage4", fake_sources)
    monkeypatch.setattr(step10, "_pointer_from_output", lambda _root, output, _run: (root / "state" / ("p" + str(output["id"])), {"id": output["id"]}))

    def fake_promote(records):
        calls.append("promote")
        assert len(records) == 12

    monkeypatch.setattr(step10, "_transactional_pointer_replace", fake_promote)

    def fake_smoke(_as_of):
        calls.append("smoke")
        assert _as_of.tzinfo is not None
        assert _as_of > datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
        return {"status": "passed", "daily_block_count": 20, "weekly_block_count": 24}

    monkeypatch.setattr(step10, "_stage9_smoke", fake_smoke)
    result = step10.run_refresh(
        through_date="2026-08-19",
        run_id="step10_test_order",
        repo_root=repo,
        env_file=None,
        now_utc=datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc),
    )
    assert result["status"] == "succeeded"
    assert result["new_trading_dates"] == ["2026-08-18", "2026-08-19"]
    assert result["implicit_latest_used"] is False
    assert calls == ["stage5", "stage7", "stage3_stage4", "promote", "smoke"]


def test_run_refresh_sunday_rebuild_promotes_coherent_stage7_and_reports_refreshed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    repo = tmp_path / "repo"
    (repo / "configs" / "datasets").mkdir(parents=True)
    (repo / "configs" / "datasets" / "step9_rub_analysis_bundle.v1.yaml").write_text("version: 1\n", encoding="utf-8")
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())
    pointer = root / "state" / "dummy.json"
    pointer.parent.mkdir(parents=True)
    pointer.write_bytes(b"old")
    monkeypatch.setattr(step10, "_snapshot_pointers", lambda _root: {pointer: b"old"})
    monkeypatch.setattr(step10, "_load_stage5_base", lambda _root, _as_of: ("2026-08-21", {name: pd.DataFrame({"instrument_id": [name], "trade_date": ["2026-08-21"]}) for name in step10.STAGE5_INSTRUMENTS}))
    monkeypatch.setattr(step10, "_load_stage7_base", lambda _root, _as_of: ("2026-08-21", {name: _base_frame(name, "2026-08-21") for name in step10.STAGE7_INSTRUMENTS}))
    monkeypatch.setattr(step10, "_calendar_dates", lambda **_kwargs: ["2026-08-21"])
    monkeypatch.setattr(step10, "_stage5_refresh", lambda **_kwargs: [])

    def fake_stage7(**kwargs):
        assert kwargs["trading_dates"] == []
        assert kwargs["rebuild_weekly"] is True
        assert kwargs["weekly_boundary_end"] == "2026-08-23"
        return [{"id": i} for i in range(8)]

    monkeypatch.setattr(step10, "_stage7_refresh", fake_stage7)
    monkeypatch.setattr(step10, "_latest_source_dates", lambda _root, _as_of: ("2026-08-21", "2026-08-21"))
    monkeypatch.setattr(step10, "_pointer_from_output", lambda _root, output, _run: (root / "state" / ("w" + str(output["id"])), {"id": output["id"]}))
    promoted: list[int] = []
    monkeypatch.setattr(step10, "_transactional_pointer_replace", lambda records: promoted.append(len(records)))
    monkeypatch.setattr(step10, "_stage9_smoke", lambda _as_of: {"status": "passed", "daily_block_count": 20, "weekly_block_count": 24})
    result = step10.run_refresh(
        through_date="2026-08-23",
        run_id="step10_sunday",
        repo_root=repo,
        env_file=None,
        now_utc=datetime(2026, 8, 24, 1, 0, tzinfo=timezone.utc),
    )
    assert promoted == [8]
    assert result["new_trading_dates"] == []
    assert result["stage5"] == {"status": "no_op", "output_count": 0}
    assert result["stage7"] == {"status": "refreshed", "output_count": 8}


def test_failure_after_pointer_mutation_restores_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    repo = tmp_path / "repo"
    (repo / "configs" / "datasets").mkdir(parents=True)
    (repo / "configs" / "datasets" / "step9_rub_analysis_bundle.v1.yaml").write_text("version: 1\n", encoding="utf-8")
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())
    pointer = root / "state" / "dummy.json"
    pointer.parent.mkdir(parents=True)
    pointer.write_bytes(b"old")
    restored: list[bool] = []
    monkeypatch.setattr(step10, "_snapshot_pointers", lambda _root: {pointer: b"old"})
    monkeypatch.setattr(step10, "_load_stage5_base", lambda _root, _as_of: ("2026-08-17", {name: pd.DataFrame({"instrument_id": [name], "trade_date": ["2026-08-17"]}) for name in step10.STAGE5_INSTRUMENTS}))
    monkeypatch.setattr(step10, "_load_stage7_base", lambda _root, _as_of: ("2026-08-17", {name: _base_frame(name) for name in step10.STAGE7_INSTRUMENTS}))
    monkeypatch.setattr(step10, "_calendar_dates", lambda **_kwargs: ["2026-08-18"])
    monkeypatch.setattr(step10, "_stage5_refresh", lambda **_kwargs: [{"id": i} for i in range(4)])
    monkeypatch.setattr(step10, "_stage7_refresh", lambda **_kwargs: [{"id": i} for i in range(8)])
    monkeypatch.setattr(step10, "_latest_source_dates", lambda _root, _as_of: ("2026-08-18", "2026-08-18"))
    monkeypatch.setattr(step10, "_pointer_from_output", lambda _root, output, _run: (root / "state" / ("p" + str(output["id"])), {"id": output["id"]}))
    monkeypatch.setattr(step10, "_transactional_pointer_replace", lambda _records: None)
    monkeypatch.setattr(step10, "_stage9_smoke", lambda _as_of: (_ for _ in ()).throw(step10.Step10RefreshError("smoke failed")))

    def fake_restore(snapshot, expected_current):
        assert snapshot == {pointer: b"old"}
        assert expected_current == {pointer: b"old"}
        restored.append(True)

    monkeypatch.setattr(step10, "_restore_pointer_snapshot", fake_restore)
    with pytest.raises(step10.Step10RefreshError, match="smoke failed"):
        step10.run_refresh(
            through_date="2026-08-18",
            run_id="step10_test_rollback",
            repo_root=repo,
            env_file=None,
            now_utc=datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc),
        )
    assert restored == [True]

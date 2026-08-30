from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from moex_data import step10_rub_refresh_scheduler as step10


def _d1_row(instrument_id: str, trade_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_id": instrument_id,
                "trade_date": trade_date,
                "period_start_date": trade_date,
                "period_end_date": trade_date,
                "timeframe": "1D",
                "secid": "USDRUBF" if instrument_id.startswith("usd") else "CNYRUBF",
                "availability_ts_utc": "2026-08-24T03:00:00+00:00",
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


def test_sunday_catchup_with_friday_delta_uses_sunday_and_binds_base_plus_delta(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "data"
    root.mkdir()
    run_root = root / "runs" / "step10_rub_daily_refresh" / "run_id=sunday_catchup"
    base_frames = {name: _d1_row(name, "2026-08-20") for name in step10.STAGE7_INSTRUMENTS}
    canonical: dict[str, Path] = {}
    for instrument in step10.STAGE7_INSTRUMENTS:
        path = root / "market" / "raw" / instrument / "2026-08-21.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((instrument + "-raw").encode("utf-8"))
        canonical[instrument] = path

    def materialize_instrument_partition(**kwargs):
        instrument = kwargs["instrument_id"]
        return SimpleNamespace(
            payload={
                "quality_status": "pass",
                "row_count": 1,
                "storage_partition_path": canonical[instrument].as_posix(),
            }
        )

    monkeypatch.setattr(step10.forts_raw, "materialize_instrument_partition", materialize_instrument_partition)
    monkeypatch.setattr(
        step10.step7_materializer,
        "build_d1",
        lambda **kwargs: _d1_row(kwargs["instrument_id"], "2026-08-21"),
    )

    w1_history_ends: list[str] = []

    def build_w1(frame, *, history_start, history_end):
        w1_history_ends.append(history_end)
        out = frame.copy()
        out["timeframe"] = "1W"
        return out

    monkeypatch.setattr(step10.step7_materializer, "build_w1", build_w1)
    monkeypatch.setattr(step10.step7_materializer, "build_technical_features", lambda frame, **_kwargs: frame.copy())
    writes: list[dict[str, object]] = []

    def write_output(**kwargs):
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

    monkeypatch.setattr(step10.step7_materializer, "_write_output", write_output)

    outputs = step10._stage7_refresh(
        root=root,
        run_root=run_root,
        run_id="sunday_catchup",
        base_frames=base_frames,
        trading_dates=["2026-08-21"],
        rebuild_weekly=True,
        weekly_boundary_end="2026-08-23",
        timeout=1.0,
    )

    assert len(outputs) == 8
    assert w1_history_ends == ["2026-08-23", "2026-08-23"]
    for instrument in step10.STAGE7_INSTRUMENTS:
        instrument_writes = [item for item in writes if item["instrument_id"] == instrument]
        d1 = next(item for item in instrument_writes if item["dataset_id"] == "rub_native_ohlcv_htf" and item["timeframe"] == "1D")
        w1 = next(item for item in instrument_writes if item["dataset_id"] == "rub_native_ohlcv_htf" and item["timeframe"] == "1W")
        assert w1["history_end"] == "2026-08-23"
        assert str(d1["source_ref"]).endswith("/inputs/stage7_lineage/instrument_id=" + instrument + "/lineage.json")

        lineage_path = root / str(d1["source_ref"])[len(step10.ROOT_REF_PREFIX):]
        lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
        assert lineage["schema_version"] == "step10_stage7_rolling_lineage.v1"
        assert lineage["lineage_order"] == ["accepted_base_frame_snapshot", "frozen_delta_manifest"]
        assert lineage["exact_base_frame_snapshot_bound"] is True
        assert lineage["immutable_inputs_bound"] is True
        assert len(lineage["source_refs"]) == 2

        base_snapshot = root / lineage["base_snapshot_ref"][len(step10.ROOT_REF_PREFIX):]
        delta_manifest = root / lineage["delta_manifest_ref"][len(step10.ROOT_REF_PREFIX):]
        assert base_snapshot.is_file()
        assert delta_manifest.is_file()
        assert hashlib.sha256(base_snapshot.read_bytes()).hexdigest() == lineage["base_snapshot_sha256"]
        assert hashlib.sha256(delta_manifest.read_bytes()).hexdigest() == lineage["delta_manifest_sha256"]

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data.step7_rub_native_d1_w1_materializer import build_d1, build_technical_features, build_w1

ROOT_PREFIX = "${MOEX_DATA_ROOT}/"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _raw_day(instrument_id: str, secid: str, trade_date: str, base: float) -> pd.DataFrame:
    return pd.DataFrame([
        {"instrument_id": instrument_id, "trade_date": trade_date, "ts": trade_date + " 10:00:00", "secid": secid, "open": base, "high": base + 2, "low": base - 1, "close": base + 1, "volume": 10.0, "value": 100.0, "num_trades": 2.0},
        {"instrument_id": instrument_id, "trade_date": trade_date, "ts": trade_date + " 10:05:00", "secid": secid, "open": base + 1, "high": base + 3, "low": base, "close": base + 2, "volume": 20.0, "value": 200.0, "num_trades": 3.0},
    ])


def _frozen_manifest(root: Path, dates: list[str]) -> Path:
    instrument = "usdrubf_futures_family"
    records = []
    for n, trade_date in enumerate(dates):
        path = root / "runs" / "fixture" / "inputs" / "dataset_id=futures_raw_5m" / f"instrument_id={instrument}" / f"trade_date={trade_date}" / "part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        _raw_day(instrument, "USDRUBF", trade_date, 80.0 + n).to_parquet(path, index=False)
        records.append({
            "trade_date": trade_date,
            "instrument_id": instrument,
            "row_count": 2,
            "secids": ["USDRUBF"],
            "sha256": _sha(path),
            "frozen_ref": ROOT_PREFIX + path.relative_to(root).as_posix(),
        })
    digest = hashlib.sha256("".join(r["trade_date"] + "\t" + r["sha256"] + "\n" for r in records).encode()).hexdigest()
    manifest = root / "runs" / "fixture" / "state" / "frozen_inputs" / f"instrument_id={instrument}" / "frozen_raw_manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({
        "schema_version": "step7_frozen_raw_5m_manifest.v1",
        "dataset_id": "futures_raw_5m",
        "instrument_id": instrument,
        "source_id": "moex_algopack_fo_tradestats_5m",
        "requested_start_date": dates[0],
        "requested_end_date": dates[-1],
        "freeze_method": "validated_inode_create_only_hardlink",
        "mutable_canonical_raw_read_after_freeze_allowed": False,
        "partition_count": len(records),
        "frozen_content_sha256": digest,
        "partitions": records,
    }), encoding="utf-8")
    return manifest


def test_d1_aggregates_frozen_5m_and_sets_conservative_availability(tmp_path: Path) -> None:
    dates = ["2026-08-03", "2026-08-04"]
    manifest = _frozen_manifest(tmp_path, dates)
    d1 = build_d1(data_root=tmp_path, frozen_manifest_path=manifest, instrument_id="usdrubf_futures_family", history_start=dates[0], history_end=dates[-1])
    assert len(d1) == 2
    assert d1.loc[0, "open"] == pytest.approx(80.0)
    assert d1.loc[0, "high"] == pytest.approx(83.0)
    assert d1.loc[0, "low"] == pytest.approx(79.0)
    assert d1.loc[0, "close"] == pytest.approx(82.0)
    assert d1.loc[0, "volume"] == pytest.approx(30.0)
    assert d1.loc[0, "availability_ts_utc"] == "2026-08-04T03:00:00+00:00"


def test_w1_excludes_partial_first_and_last_iso_weeks() -> None:
    dates = pd.date_range("2026-04-28", "2026-05-11", freq="D")
    rows = []
    for n, ts in enumerate(dates):
        trade_date = ts.strftime("%Y-%m-%d")
        rows.append({
            "instrument_id":"usdrubf_futures_family","secid":"USDRUBF","timeframe":"1D",
            "period_start_date":trade_date,"period_end_date":trade_date,"trade_date":trade_date,
            "availability_ts_utc":"2026-01-01T00:00:00+00:00","open":80+n,"high":82+n,"low":79+n,"close":81+n,
            "volume":1.0,"value":2.0,"num_trades":1.0,"source_row_count":1,"source_period_count":1,"source_lineage_sha256":str(n).zfill(64),
        })
    d1 = pd.DataFrame(rows)
    w1 = build_w1(d1, history_start="2026-04-28", history_end="2026-05-11")
    assert w1["week_start_date"].tolist() == ["2026-05-04"]
    assert w1.loc[0, "week_end_date"] == "2026-05-10"
    assert w1.loc[0, "trading_day_count"] == 7


def test_technical_features_are_causal_and_wilder_atr_is_seeded_exactly() -> None:
    rows = []
    for n in range(25):
        trade_date = (pd.Timestamp("2026-01-01") + pd.Timedelta(days=n)).strftime("%Y-%m-%d")
        rows.append({
            "instrument_id":"usdrubf_futures_family","secid":"USDRUBF","timeframe":"1D",
            "period_start_date":trade_date,"period_end_date":trade_date,"trade_date":trade_date,"availability_ts_utc":"2026-01-01T00:00:00+00:00",
            "open":100.0+n,"high":102.0+n,"low":99.0+n,"close":101.0+n,
        })
    d1 = pd.DataFrame(rows)
    features = build_technical_features(d1, source_ohlcv_run_id="fixture_d1")
    assert pd.isna(features.loc[0, "return_1obs"])
    assert features.loc[1, "return_1obs"] == pytest.approx(102.0 / 101.0 - 1.0)
    assert features.loc[1, "gap_abs"] == pytest.approx(0.0)
    assert features.loc[1, "higher_high_vs_prev_bar"] == True
    assert features.loc[1, "higher_low_vs_prev_bar"] == True
    assert pd.isna(features.loc[12, "atr_14_wilder"])
    assert features.loc[13, "atr_14_wilder"] == pytest.approx(3.0)
    assert features.loc[19, "atr_20_wilder"] == pytest.approx(3.0)
    assert features.loc[24, "atr_14_wilder"] == pytest.approx(3.0)

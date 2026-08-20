from __future__ import annotations

from pathlib import Path

import pandas as pd

from moex_data.futures import stage2_raw_history_acceptance as acceptance


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    _write(
        root / "configs/datasets/futures_data_lake.v1.yaml",
        """stage2_forts_source_bindings:
  status: raw_historical_backfills_completed_acceptance_pending
  quote_source:
    source_id: moex_algopack_fo_tradestats_5m
    dataset_contract_ref: contracts/datasets/futures_raw_5m.v1.yaml
    historical_backfill_instrument_ids:
      - usdrubf_futures_family
      - cnyrubf_futures_family
    proven_coverage:
      usdrubf_futures_family:
        secid: USDRUBF
        first_available: "2026-08-17"
        last_available: "2026-08-17"
        partitions: 1
        rows: 2
        physical_quality_status: pass
        market_spotcheck_status: pass
      cnyrubf_futures_family:
        secid: CNYRUBF
        first_available: "2026-08-17"
        last_available: "2026-08-17"
        partitions: 1
        rows: 2
        physical_quality_status: pass
        market_spotcheck_status: pass
  futoi_source:
    source_id: moex_algopack_futoi
    dataset_contract_ref: contracts/datasets/futures_futoi_raw.v1.yaml
    public_iss_evidence_status: invalidated
    historical_priority_backfills:
      si_futures_family:
        ticker: si
        first_available: "2026-08-17"
        last_available: "2026-08-17"
        partitions: 1
        rows: 2
        skipped_empty_source_dates: 0
        bad_partitions: 0
        physical_quality_status: pass
      cr_futures_family:
        ticker: cr
        first_available: "2026-08-17"
        last_available: "2026-08-17"
        partitions: 1
        rows: 2
        skipped_empty_source_dates: 0
        bad_partitions: 0
        physical_quality_status: pass
  readiness_flags:
    historical_quotes_backfill_completed: true
    priority_futoi_raw_backfill_completed: true
    raw_physical_audit_completed: true
    accepted_pointer_ready: false
    scheduler_ready: false
    d1_materialization_ready: false
    research_ready: false
""",
    )
    _write(
        root / "contracts/datasets/futures_raw_5m.v1.yaml",
        """dataset_id: futures_raw_5m
path_pattern: "${MOEX_DATA_ROOT}/market/raw/timeframe=5m/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet"
""",
    )
    _write(
        root / "contracts/datasets/futures_futoi_raw.v1.yaml",
        """dataset_id: futures_futoi_raw
path_pattern: "${MOEX_DATA_ROOT}/market/supplementary/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet"
""",
    )
    _write(
        root / "contracts/datasets/futures_raw_history_acceptance.v1.yaml",
        """dataset_id: futures_raw_history_acceptance
path_pattern: "${MOEX_DATA_ROOT}/state/acceptance/target_dataset_id={TARGET_DATASET_ID}/instrument_id={INSTRUMENT_ID}/run_id={RUN_ID}/acceptance_report.json"
""",
    )
    return root


def _quote_frame(rows: int = 2) -> pd.DataFrame:
    timestamps = pd.to_datetime(["2026-08-17 10:00:00", "2026-08-17 10:05:00"])[:rows]
    return pd.DataFrame(
        {
            "instrument_id": ["usdrubf_futures_family"] * rows,
            "trade_date": ["2026-08-17"] * rows,
            "ts": timestamps,
            "session_date": ["2026-08-17"] * rows,
            "secid": ["USDRUBF"] * rows,
            "board": ["RFUD"] * rows,
            "market": ["FORTS"] * rows,
            "engine": ["futures"] * rows,
            "source_id": ["moex_algopack_fo_tradestats_5m"] * rows,
            "open": [80.0, 80.1][:rows],
            "high": [80.2, 80.3][:rows],
            "low": [79.9, 80.0][:rows],
            "close": [80.1, 80.2][:rows],
            "volume": [10.0, 12.0][:rows],
            "value": [800.0, 962.4][:rows],
            "num_trades": [5, 6][:rows],
            "source": ["MOEX_ALGOPACK_FO_TRADESTATS"] * rows,
            "ingest_ts": ["2026-08-18T00:00:00+00:00"] * rows,
        }
    )


def _futoi_frame() -> pd.DataFrame:
    ts = pd.to_datetime(["2026-08-17 10:00:00", "2026-08-17 10:00:00"])
    return pd.DataFrame(
        {
            "instrument_id": ["si_futures_family", "si_futures_family"],
            "trade_date": ["2026-08-17", "2026-08-17"],
            "ts": ts,
            "moment": ts,
            "systime": pd.to_datetime(["2026-08-17 10:01:00", "2026-08-17 10:01:00"]),
            "sess_id": [1, 1],
            "seqnum": [10, 11],
            "secid": ["SiU6", "SiU6"],
            "board": ["RFUD", "RFUD"],
            "market": ["forts", "forts"],
            "engine": ["futures", "futures"],
            "source_id": ["moex_algopack_futoi", "moex_algopack_futoi"],
            "source_ticker": ["si", "si"],
            "clgroup": ["FIZ", "YUR"],
            "pos": [10, -10],
            "pos_long": [20, 30],
            "pos_short": [-10, -40],
            "pos_long_num": [2, 3],
            "pos_short_num": [1, 4],
            "availability_ts_utc": pd.to_datetime(
                ["2026-08-18T00:00:00+00:00", "2026-08-18T00:00:00+00:00"]
            ),
            "ingest_ts": pd.to_datetime(
                ["2026-08-18T00:00:00+00:00", "2026-08-18T00:00:00+00:00"]
            ),
        }
    )


def test_quote_existing_history_passes_without_pointer_or_network(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    path = (
        data_root
        / "market/raw/timeframe=5m/instrument_id=usdrubf_futures_family"
        / "trade_date=2026-08-17/source=moex_algopack_fo_tradestats_5m/part.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _quote_frame().to_parquet(path, index=False)

    result = acceptance.audit_history(
        repo_root=repo,
        target_dataset_id="futures_raw_5m",
        instrument_id="usdrubf_futures_family",
        run_id="quote_acceptance_test",
    )

    assert result["acceptance_status"] == "pass"
    assert result["actual_partition_count"] == 1
    assert result["actual_row_count"] == 2
    assert result["accepted_pointer_written"] is False
    assert result["network_access_used"] is False
    assert result["historical_backfill_used"] is False
    assert Path(str(result["acceptance_report_reference"])).exists()
    assert not (data_root / "state/datasets").exists()


def test_futoi_existing_history_passes_source_record_key_and_timestamp_semantics(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    path = (
        data_root
        / "market/supplementary/dataset_id=futures_futoi_raw/instrument_id=si_futures_family"
        / "trade_date=2026-08-17/source=moex_algopack_futoi/part.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _futoi_frame().to_parquet(path, index=False)

    result = acceptance.audit_history(
        repo_root=repo,
        target_dataset_id="futures_futoi_raw",
        instrument_id="si_futures_family",
        run_id="futoi_acceptance_test",
    )

    assert result["acceptance_status"] == "pass"
    assert result["secid_scope"] == ["SiU6"]
    assert result["hard_check_failures"] == []
    assert result["network_access_used"] is False
    assert result["accepted_pointer_written"] is False


def test_row_count_mismatch_fails_closed_and_still_writes_evidence(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    path = (
        data_root
        / "market/raw/timeframe=5m/instrument_id=usdrubf_futures_family"
        / "trade_date=2026-08-17/source=moex_algopack_fo_tradestats_5m/part.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _quote_frame(rows=1).to_parquet(path, index=False)

    result = acceptance.audit_history(
        repo_root=repo,
        target_dataset_id="futures_raw_5m",
        instrument_id="usdrubf_futures_family",
        run_id="quote_acceptance_fail",
    )

    assert result["acceptance_status"] == "fail"
    assert "expected_row_count_mismatch" in result["hard_check_failures"]
    assert Path(str(result["acceptance_report_reference"])).exists()
    assert not (data_root / "state/datasets").exists()


def test_reference_expiry_quotes_are_outside_historical_acceptance_scope(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path / "data"))
    try:
        acceptance.audit_history(
            repo_root=repo,
            target_dataset_id="futures_raw_5m",
            instrument_id="si_futures_family",
            run_id="forbidden_reference_history",
        )
    except acceptance.RawHistoryAcceptanceError as exc:
        assert "not in Stage 2 historical quote acceptance scope" in str(exc)
    else:
        raise AssertionError("reference-only fixed expiry must not enter historical acceptance")

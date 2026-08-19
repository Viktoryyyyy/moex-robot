from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import materialize_futoi_eod as target


ZERO_SHA = "0" * 64


def _binding() -> dict[str, object]:
    return {
        "instrument_id": "si_futures_family",
        "canonical_symbol": "Si",
        "secid": "SiU6",
        "board": "RFUD",
        "market": "forts",
        "engine": "futures",
        "futoi.source_id": target.SOURCE_ID,
        "futoi.ticker": "si",
        "futoi.availability_status": "available",
        "futoi.probe_status": "completed",
        "futoi.enabled_for_materialization": False,
    }


def _row(clgroup: str, ts: str, seqnum: object, *, sess_id: object = 6263, pos: int | None = None) -> dict[str, object]:
    fiz = clgroup == "FIZ"
    position = pos if pos is not None else (52872 if fiz else -52872)
    return {
        "instrument_id": "si_futures_family",
        "trade_date": "2021-04-06",
        "ts": pd.Timestamp(ts),
        "moment": pd.Timestamp(ts),
        "systime": pd.Timestamp("2025-06-21 16:22:46"),
        "sess_id": sess_id,
        "seqnum": seqnum,
        "secid": "SiU6",
        "board": "RFUD",
        "market": "forts",
        "engine": "futures",
        "source_id": target.SOURCE_ID,
        "source_ticker": "SI",
        "clgroup": clgroup,
        "pos": position,
        "pos_long": 664057 if fiz else 815033,
        "pos_short": -611185 if fiz else -867905,
        "pos_long_num": 14423 if fiz else 249,
        "pos_short_num": 9666 if fiz else 146,
        "availability_ts_utc": "2026-08-19T13:00:00+00:00",
        "ingest_ts": "2026-08-19T13:00:00+00:00",
    }


def _raw_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _row("FIZ", "2021-04-06 18:30:00", 195, pos=53870),
            _row("YUR", "2021-04-06 18:30:00", 195, pos=-53870),
            _row("FIZ", "2021-04-06 18:30:00", 215, pos=53415),
            _row("YUR", "2021-04-06 18:30:00", 215, pos=-53415),
            _row("FIZ", "2021-04-06 18:44:59", 219),
            _row("YUR", "2021-04-06 18:44:59", 219),
            _row("FIZ", "2021-04-06 18:44:59", 220),
            _row("YUR", "2021-04-06 18:44:59", 220),
        ]
    )


def _derive(raw: pd.DataFrame) -> pd.DataFrame:
    validated = target._validate_raw_partition(raw, "2021-04-06", "si_futures_family", _binding())
    return target._derive_partition(
        validated,
        trade_date="2021-04-06",
        instrument_id="si_futures_family",
        raw_run_id="raw_run",
        raw_manifest_ref="${MOEX_DATA_ROOT}/state/raw_manifest.json",
        raw_content_pin_ref="${MOEX_DATA_ROOT}/state/raw_pin.json",
        raw_content_pin_sha256=ZERO_SHA,
        raw_partition_ref="${MOEX_DATA_ROOT}/market/raw_partition.parquet",
        raw_partition_sha256=ZERO_SHA,
        derived_ingest_ts="2026-08-19T14:00:00+00:00",
    )


def test_eod_selects_latest_ts_then_highest_seqnum_revision() -> None:
    result = _derive(_raw_rows())
    assert len(result) == 2
    assert set(result["clgroup"].tolist()) == {"FIZ", "YUR"}
    assert set(result["seqnum"].astype(int).tolist()) == {220}
    assert set(result["sess_id"].astype(int).tolist()) == {6263}
    assert set(result["ts"].tolist()) == {pd.Timestamp("2021-04-06 18:44:59")}
    assert set(result["max_ts_revision_count"].astype(int).tolist()) == {2}
    assert set(result["raw_source_record_count"].astype(int).tolist()) == {8}
    assert set(result["raw_content_pin_sha256"].tolist()) == {ZERO_SHA}
    assert not result.duplicated(subset=list(target.EOD_KEY_FIELDS)).any()


def test_eod_seqnum_selection_preserves_integer_precision_above_2_pow_53() -> None:
    result = _derive(
        pd.DataFrame(
            [
                _row("FIZ", "2021-04-06 18:44:59", "9007199254740992"),
                _row("YUR", "2021-04-06 18:44:59", "9007199254740992"),
                _row("FIZ", "2021-04-06 18:44:59", "9007199254740993"),
                _row("YUR", "2021-04-06 18:44:59", "9007199254740993"),
            ]
        )
    )
    assert set(result["seqnum"].tolist()) == {9007199254740993}


def test_eod_fails_when_max_ts_contains_multiple_sessions_for_group() -> None:
    raw = pd.concat(
        [_raw_rows(), pd.DataFrame([_row("FIZ", "2021-04-06 18:44:59", 1, sess_id=6264, pos=50000)])],
        ignore_index=True,
    )
    with pytest.raises(target.FutoiEodError, match="ambiguous EOD session"):
        _derive(raw)


def test_eod_fails_when_fiz_and_yur_latest_snapshot_is_not_coherent() -> None:
    raw = _raw_rows()
    mask = (raw["clgroup"] == "YUR") & (raw["seqnum"] == 220)
    raw.loc[mask, "seqnum"] = 221
    with pytest.raises(target.FutoiEodError, match="not the same source snapshot"):
        _derive(raw)


def test_raw_partition_rejects_source_record_duplicates() -> None:
    frame = pd.concat([_raw_rows(), _raw_rows().iloc[[0]]], ignore_index=True)
    with pytest.raises(target.FutoiEodError, match="duplicate source-record key"):
        target._validate_raw_partition(frame, "2021-04-06", "si_futures_family", _binding())


def _write_raw_state(root: Path) -> tuple[Path, Path]:
    raw_partition = (
        root
        / "market"
        / "supplementary"
        / "dataset_id=futures_futoi_raw"
        / "instrument_id=si_futures_family"
        / "trade_date=2021-04-06"
        / ("source=" + target.SOURCE_ID)
        / "part.parquet"
    )
    raw_partition.parent.mkdir(parents=True, exist_ok=True)
    _raw_rows().to_parquet(raw_partition, index=False)

    quality_path = (
        root
        / "state"
        / "quality"
        / "dataset_id=futures_futoi_raw"
        / "run_date=2021-04-06"
        / "run_id=raw_run"
        / "quality_report.json"
    )
    quality_path.parent.mkdir(parents=True, exist_ok=True)
    quality_path.write_text(
        json.dumps(
            {
                "run_id": "raw_run",
                "dataset_id": "futures_futoi_raw",
                "instrument_id": "si_futures_family",
                "source_id": target.SOURCE_ID,
                "requested_from": "2021-04-06",
                "requested_till": "2021-04-06",
                "quality_status": "pass",
                "row_count": 8,
                "partition_count": 1,
                "duplicate_key_count": 0,
                "null_required_count": 0,
                "invalid_position_count": 0,
                "skipped_empty_source_dates": [],
                "failed_dates": [],
            }
        ),
        encoding="utf-8",
    )

    manifest_path = (
        root
        / "state"
        / "refresh"
        / "dataset_id=futures_futoi_raw"
        / "run_date=2021-04-06"
        / "run_id=raw_run"
        / "manifest.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": "raw_run",
                "run_date": "2021-04-06",
                "dataset_id": "futures_futoi_raw",
                "instrument_scope": ["si_futures_family"],
                "source_scope": [target.SOURCE_ID],
                "requested_from": "2021-04-06",
                "requested_till": "2021-04-06",
                "partitions_written": [str(raw_partition)],
                "partitions_skipped": [],
                "failed_dates": [],
                "quality_report_ref": str(quality_path),
                "refresh_status": "succeeded",
            }
        ),
        encoding="utf-8",
    )
    return manifest_path, raw_partition


def _patch_root(monkeypatch, root: Path) -> None:
    monkeypatch.setattr(target, "_data_root", lambda: root.resolve())
    monkeypatch.setattr(target.raw_pin, "_data_root", lambda: root.resolve())
    monkeypatch.setattr(target.raw_materializer, "_registry_binding", lambda *_: _binding())
    monkeypatch.setattr(
        target.raw_materializer,
        "_fetch_exact",
        lambda *_: (_ for _ in ()).throw(AssertionError("EOD derivation must not refetch source")),
    )


def _make_pin(monkeypatch, tmp_path: Path) -> tuple[dict[str, object], Path, Path]:
    manifest_path, raw_partition = _write_raw_state(tmp_path)
    _patch_root(monkeypatch, tmp_path)
    pin = target.raw_pin.create_content_pin(
        instrument_id="si_futures_family",
        run_id="raw_pin_run",
        raw_manifest_path=manifest_path,
    )
    return pin, raw_partition, manifest_path


def _eod_output_path(root: Path, run_id: str = "eod_run") -> Path:
    return (
        root
        / "market"
        / "supplementary"
        / "dataset_id=futures_futoi_eod"
        / ("run_id=" + run_id)
        / "instrument_id=si_futures_family"
        / "trade_date=2021-04-06"
        / ("source=" + target.SOURCE_ID)
        / "part.parquet"
    )


def test_full_eod_materialization_requires_verified_content_pin(monkeypatch, tmp_path: Path) -> None:
    pin, _, _ = _make_pin(monkeypatch, tmp_path)
    result = target.materialize_futoi_eod(
        instrument_id="si_futures_family",
        run_id="eod_run",
        raw_pin_path=str(pin["content_pin_reference"]),
        raw_pin_sha256=str(pin["content_pin_sha256"]),
    )

    assert result["status"] == "succeeded"
    assert result["quality_status"] == "pass"
    assert result["raw_partition_count"] == 1
    assert result["partition_count"] == 1
    assert result["row_count"] == 2
    assert result["failure_count"] == 0
    assert result["ambiguity_count"] == 0
    assert result["raw_content_pin_sha256"] == pin["content_pin_sha256"]
    assert result["output_version_mode"] == "run_id"
    assert result["immutable_output_run_reserved"] is True
    assert "run_id=eod_run" in result["immutable_output_run_root"]
    assert result["dynamic_scan_used"] is False
    assert result["direct_source_refetch_used"] is False
    assert result["accepted_manifest_pointer_reference"] is None

    output = pd.read_parquet(_eod_output_path(tmp_path))
    assert len(output) == 2
    assert set(output["seqnum"].astype(int).tolist()) == {220}
    assert output["raw_manifest_reference"].str.startswith("${MOEX_DATA_ROOT}/").all()
    assert output["raw_content_pin_reference"].str.startswith("${MOEX_DATA_ROOT}/").all()
    assert output["raw_partition_reference"].str.startswith("${MOEX_DATA_ROOT}/").all()
    assert output["raw_content_pin_sha256"].eq(pin["content_pin_sha256"]).all()
    assert output["availability_ts_utc"].eq(output["derived_ingest_ts"]).all()
    assert output["raw_availability_ts_utc"].ne(output["availability_ts_utc"]).all()

    manifest = json.loads(Path(result["manifest_reference"]).read_text(encoding="utf-8"))
    assert manifest["output_version_mode"] == "run_id"
    assert manifest["immutable_output_run_reserved"] is True
    assert len(manifest["partitions_written"]) == 1
    assert "/run_id=eod_run/" in manifest["partitions_written"][0]


def test_eod_same_run_id_cannot_overwrite_prior_success(monkeypatch, tmp_path: Path) -> None:
    pin, _, _ = _make_pin(monkeypatch, tmp_path)
    kwargs = {
        "instrument_id": "si_futures_family",
        "run_id": "eod_run",
        "raw_pin_path": str(pin["content_pin_reference"]),
        "raw_pin_sha256": str(pin["content_pin_sha256"]),
    }
    target.materialize_futoi_eod(**kwargs)
    output_path = _eod_output_path(tmp_path)
    before = output_path.read_bytes()

    with pytest.raises(target.FutoiEodError, match="immutable output run already exists"):
        target.materialize_futoi_eod(**kwargs)

    assert output_path.read_bytes() == before


def test_raw_partition_mutation_after_pin_fails_closed(monkeypatch, tmp_path: Path) -> None:
    pin, raw_partition, _ = _make_pin(monkeypatch, tmp_path)
    mutated = _raw_rows()
    mutated.loc[mutated.index[-1], "pos"] = -999999
    mutated.to_parquet(raw_partition, index=False)

    with pytest.raises(target.raw_pin.FutoiRawContentPinError, match="content changed after pin creation"):
        target.materialize_futoi_eod(
            instrument_id="si_futures_family",
            run_id="eod_run_after_mutation",
            raw_pin_path=str(pin["content_pin_reference"]),
            raw_pin_sha256=str(pin["content_pin_sha256"]),
        )


def test_wrong_explicit_pin_digest_fails_closed(monkeypatch, tmp_path: Path) -> None:
    pin, _, _ = _make_pin(monkeypatch, tmp_path)
    with pytest.raises(target.raw_pin.FutoiRawContentPinError, match="content pin SHA-256 mismatch"):
        target.materialize_futoi_eod(
            instrument_id="si_futures_family",
            run_id="eod_run_wrong_pin_digest",
            raw_pin_path=str(pin["content_pin_reference"]),
            raw_pin_sha256=ZERO_SHA,
        )


def test_second_content_pin_same_run_id_cannot_overwrite(monkeypatch, tmp_path: Path) -> None:
    pin, _, manifest_path = _make_pin(monkeypatch, tmp_path)
    pin_path = Path(str(pin["content_pin_reference"]))
    before = pin_path.read_bytes()

    with pytest.raises(target.raw_pin.FutoiRawContentPinError, match="overwrite is forbidden"):
        target.raw_pin.create_content_pin(
            instrument_id="si_futures_family",
            run_id="raw_pin_run",
            raw_manifest_path=manifest_path,
        )

    assert pin_path.read_bytes() == before


def test_raw_manifest_rejects_skipped_date_outside_requested_range(monkeypatch, tmp_path: Path) -> None:
    manifest_path, _ = _write_raw_state(tmp_path)
    values = json.loads(manifest_path.read_text(encoding="utf-8"))
    values["partitions_skipped"] = ["2021-04-07"]
    manifest_path.write_text(json.dumps(values), encoding="utf-8")
    monkeypatch.setattr(target.raw_pin, "_data_root", lambda: tmp_path.resolve())

    with pytest.raises(target.raw_pin.FutoiRawContentPinError, match="outside requested range"):
        target.raw_pin.validate_raw_manifest(manifest_path, "si_futures_family")


def test_raw_manifest_content_identity_must_match_canonical_path(monkeypatch, tmp_path: Path) -> None:
    manifest_path, _ = _write_raw_state(tmp_path)
    values = json.loads(manifest_path.read_text(encoding="utf-8"))
    values["run_id"] = "different_run"
    manifest_path.write_text(json.dumps(values), encoding="utf-8")
    monkeypatch.setattr(target.raw_pin, "_data_root", lambda: tmp_path.resolve())
    with pytest.raises(target.raw_pin.FutoiRawContentPinError, match="path identity"):
        target.raw_pin.validate_raw_manifest(manifest_path, "si_futures_family")


def test_raw_manifest_outside_data_root_is_rejected(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(target.raw_pin, "_data_root", lambda: root.resolve())
    with pytest.raises(target.raw_pin.FutoiRawContentPinError, match="inside MOEX_DATA_ROOT"):
        target.raw_pin.validate_raw_manifest(outside, "si_futures_family")

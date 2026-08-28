from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from moex_data import step3_raw_acceptance as step3
from moex_data import step4_basis_carry_acceptance as step4
from moex_data import step5_futoi_positioning_acceptance_base as step5
from moex_data import step9_rub_analysis_bundle as step9


def _write(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_step3_pointer_values_publish_all_stage9_digests(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    manifest = _write(tmp_path / "runs" / "step3_canonical_raw" / "run_id=r" / "manifest.json", b"manifest")
    quality = _write(tmp_path / "runs" / "step3_canonical_raw" / "run_id=r" / "quality.json", b"quality")
    partition = _write(tmp_path / "runs" / "step3_canonical_raw" / "run_id=r" / "part.parquet", b"partition")
    spec = step3.PointerSpec(
        dataset_id="futures_raw_5m",
        instrument_id="si_front_contract",
        source_id="source",
        secid="SiU6",
        trade_date="2026-08-28",
        row_count=1,
        manifest_path=manifest,
        quality_path=quality,
        partition_path=partition,
        manifest_run_id="producer",
    )

    values = step3._pointer_values(spec, acceptance_run_id="acceptance")

    assert values["manifest_sha256"] == _sha(manifest)
    assert values["quality_report_sha256"] == _sha(quality)
    assert values["partition_sha256"] == _sha(partition)


def test_step4_promotion_publishes_all_stage9_digests(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence_dir = tmp_path / "state" / "acceptance" / "step4_rub_basis_carry" / "run_id=acceptance"
    evidence_dir.mkdir(parents=True)
    (evidence_dir / "pilot_evidence.json").write_text("{}", encoding="utf-8")

    outputs = []
    expected = ("usd_rub_basis_carry", "cny_rub_basis_carry")
    for index, instrument_id in enumerate(expected):
        base = tmp_path / "runs" / "step4_rub_basis_carry" / "run_id=acceptance" / instrument_id
        manifest = _write(base / "manifest.json", f"manifest-{index}".encode())
        quality = _write(base / "quality.json", f"quality-{index}".encode())
        partition = _write(base / "part.parquet", f"partition-{index}".encode())
        outputs.append(
            {
                "instrument_id": instrument_id,
                "manifest_run_id": "producer",
                "manifest": manifest,
                "quality": quality,
                "partition": partition,
                "physical_readback": {"physical_readback_passed": True},
            }
        )

    monkeypatch.setattr(step4, "validate_pilot", lambda values, run_id: outputs)
    captured = []
    monkeypatch.setattr(step4, "_transactional_replace", lambda records: captured.extend(records))

    result = step4.promote(run_id="acceptance")

    assert result["accepted_pointer_count"] == 2
    for record, output in zip(captured[:2], outputs):
        values = record[1]
        assert values["manifest_sha256"] == _sha(output["manifest"])
        assert values["quality_report_sha256"] == _sha(output["quality"])
        assert values["partition_sha256"] == _sha(output["partition"])


def test_step5_promotion_publishes_all_stage9_digests(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    evidence_dir = tmp_path / "state" / "acceptance" / "step5_futoi_positioning" / "run_id=acceptance"
    evidence_dir.mkdir(parents=True)
    (evidence_dir / "pilot_evidence.json").write_text("{}", encoding="utf-8")

    outputs = []
    for dataset_id in (step5.EOD_DATASET, step5.FEATURE_DATASET):
        for index, instrument_id in enumerate(("si_futures_family", "cr_futures_family")):
            base = tmp_path / "runs" / "step5_futoi_positioning" / "run_id=acceptance" / dataset_id / instrument_id
            manifest = _write(base / "manifest.json", f"manifest-{dataset_id}-{index}".encode())
            quality = _write(base / "quality.json", f"quality-{dataset_id}-{index}".encode())
            partition = _write(base / "part.parquet", f"partition-{dataset_id}-{index}".encode())
            outputs.append(
                {
                    "dataset_id": dataset_id,
                    "instrument_id": instrument_id,
                    "producer_run_id": "producer",
                    "manifest": manifest,
                    "quality": quality,
                    "partition": partition,
                    "physical_readback": {"physical_readback_passed": True},
                }
            )

    monkeypatch.setattr(step5, "validate_pilot", lambda values, run_id: outputs)
    captured = []
    monkeypatch.setattr(step5, "_transactional_replace", lambda records: captured.extend(records))

    result = step5.promote(run_id="acceptance")

    assert result["accepted_pointer_count"] == 4
    for record, output in zip(captured[:4], outputs):
        values = record[1]
        assert values["manifest_sha256"] == _sha(output["manifest"])
        assert values["quality_report_sha256"] == _sha(output["quality"])
        assert values["partition_sha256"] == _sha(output["partition"])


def test_stage9_support_identity_fields_are_mandatory():
    spec = step9.PointerSpec(
        block_id="stage7.technical.1D.usdrubf_futures_family",
        stage=7,
        dataset_id="rub_technical_features_htf",
        instrument_id="usdrubf_futures_family",
        causal_field="availability_ts_utc",
        timeframe="1D",
    )

    with pytest.raises(step9.Step9AnalysisBundleError, match="missing dataset_id"):
        step9._validate_support_identity({}, spec, "manifest", quality_required=True)
    with pytest.raises(step9.Step9AnalysisBundleError, match="missing instrument_id"):
        step9._validate_support_identity({"dataset_id": spec.dataset_id}, spec, "manifest", quality_required=True)
    with pytest.raises(step9.Step9AnalysisBundleError, match="missing timeframe"):
        step9._validate_support_identity(
            {"dataset_id": spec.dataset_id, "instrument_id": spec.instrument_id},
            spec,
            "manifest",
            quality_required=True,
        )
    with pytest.raises(step9.Step9AnalysisBundleError, match="missing quality_status"):
        step9._validate_support_identity(
            {"dataset_id": spec.dataset_id, "instrument_id": spec.instrument_id, "timeframe": "1D"},
            spec,
            "manifest",
            quality_required=True,
        )

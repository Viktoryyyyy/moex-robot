from __future__ import annotations

import hashlib
import json
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


@pytest.mark.parametrize(
    ("module", "dataset_id", "instrument_id"),
    [
        (step4, "rub_basis_carry_5m", "usd_rub_basis_carry"),
        (step5, "futures_futoi_eod", "si_futures_family"),
    ],
)
def test_stage4_stage5_promotions_publish_all_stage9_digests(tmp_path, monkeypatch, module, dataset_id, instrument_id):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    manifest = _write(tmp_path / "runs" / "test" / "manifest.json", b"manifest")
    quality = _write(tmp_path / "runs" / "test" / "quality.json", b"quality")
    partition = _write(tmp_path / "runs" / "test" / "part.parquet", b"partition")
    evidence_dir = tmp_path / "state" / "acceptance" / "test" / "run_id=acceptance"
    evidence_dir.mkdir(parents=True)
    (evidence_dir / "pilot_evidence.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(module, "_evidence_dir", lambda run_id: evidence_dir)
    monkeypatch.setattr(
        module,
        "validate_pilot",
        lambda values, run_id: [
            {
                "dataset_id": dataset_id,
                "instrument_id": instrument_id,
                "producer_run_id": "producer",
                "manifest_run_id": "producer",
                "manifest": manifest,
                "quality": quality,
                "partition": partition,
                "physical_readback": {"physical_readback_passed": True},
            }
        ],
    )
    captured = []
    monkeypatch.setattr(module, "_transactional_replace", lambda records: captured.extend(records))

    if module is step4:
        monkeypatch.setattr(module, "EXPECTED_INSTRUMENTS", {instrument_id})
        result = module.promote(run_id="acceptance")
        pointer_values = captured[0][1]
        assert result["accepted_pointer_count"] == 1
    else:
        monkeypatch.setattr(module, "EXPECTED_INSTRUMENTS", frozenset({instrument_id}))
        result = module.promote(run_id="acceptance")
        pointer_values = captured[0][1]
        assert result["accepted_pointer_count"] == 1

    assert pointer_values["manifest_sha256"] == _sha(manifest)
    assert pointer_values["quality_report_sha256"] == _sha(quality)
    assert pointer_values["partition_sha256"] == _sha(partition)


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
        step9._validate_support_identity(
            {"dataset_id": spec.dataset_id}, spec, "manifest", quality_required=True
        )
    with pytest.raises(step9.Step9AnalysisBundleError, match="missing timeframe"):
        step9._validate_support_identity(
            {"dataset_id": spec.dataset_id, "instrument_id": spec.instrument_id},
            spec,
            "manifest",
            quality_required=True,
        )
    with pytest.raises(step9.Step9AnalysisBundleError, match="missing quality_status"):
        step9._validate_support_identity(
            {
                "dataset_id": spec.dataset_id,
                "instrument_id": spec.instrument_id,
                "timeframe": "1D",
            },
            spec,
            "manifest",
            quality_required=True,
        )

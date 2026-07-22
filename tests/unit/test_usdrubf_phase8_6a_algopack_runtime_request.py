from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from moex_research.runners import (
    usdrubf_phase8_6a_algopack_cnyrub_source_validation as runner,
)


def _arguments(tmp_path: Path, *, run_id: str) -> argparse.Namespace:
    paths = {
        "modeling_dataset_path": tmp_path / "modeling_dataset.parquet",
        "dataset_manifest_path": tmp_path / "dataset_manifest.json",
        "feature_schema_path": tmp_path / "feature_schema.json",
        "m0_validation_predictions_path": tmp_path / "m0_validation.parquet",
        "phase8_3_aggregate_metrics_path": tmp_path / "phase83_metrics.json",
        "phase8_3_gate_results_path": tmp_path / "phase83_gates.json",
        "experiment_contract_path": tmp_path / "experiment_contract.json",
    }
    for path in paths.values():
        path.write_text("{}", encoding="utf-8")
    return argparse.Namespace(
        **{name: str(path) for name, path in paths.items()},
        output_dir=str(tmp_path / "runtime_output"),
        run_id=run_id,
        git_commit_sha="a" * 40,
    )


def test_algopack_runtime_run_id_is_accepted(tmp_path: Path) -> None:
    arguments = _arguments(
        tmp_path,
        run_id="phase8_6a_algopack_cnyrub_source_validation_20260722_v1",
    )

    request = runner.request_from_args(arguments)

    assert request.run_id == arguments.run_id
    assert request.output_dir == tmp_path / "runtime_output"


def test_legacy_public_iss_run_id_is_rejected(tmp_path: Path) -> None:
    arguments = _arguments(
        tmp_path,
        run_id="phase8_6a_moex_cnyrub_source_validation_20260722_v1",
    )

    with pytest.raises(
        runner.Phase86AAlgoPackSourceValidationError,
        match="immutable AlgoPack run id",
    ):
        runner.request_from_args(arguments)


def test_runtime_entrypoint_uses_algopack_validator(tmp_path: Path) -> None:
    arguments = _arguments(
        tmp_path,
        run_id="phase8_6a_moex_cnyrub_source_validation_20260722_v1",
    )
    request = runner.Phase86ARequest(
        modeling_dataset_path=Path(arguments.modeling_dataset_path),
        dataset_manifest_path=Path(arguments.dataset_manifest_path),
        feature_schema_path=Path(arguments.feature_schema_path),
        m0_validation_predictions_path=Path(
            arguments.m0_validation_predictions_path
        ),
        phase83_aggregate_metrics_path=Path(
            arguments.phase8_3_aggregate_metrics_path
        ),
        phase83_gate_results_path=Path(arguments.phase8_3_gate_results_path),
        experiment_contract_path=Path(arguments.experiment_contract_path),
        output_dir=Path(arguments.output_dir),
        run_id=arguments.run_id,
        git_commit_sha=arguments.git_commit_sha,
    )

    with pytest.raises(
        runner.Phase86AAlgoPackSourceValidationError,
        match="immutable AlgoPack run id",
    ):
        runner.run_source_validation(request)

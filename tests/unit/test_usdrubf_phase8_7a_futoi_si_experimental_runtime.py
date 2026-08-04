from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pandas as pd
import pytest

from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_experimental_runtime as experimental,
)
from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_runtime as base,
)


ARTIFACT_NAMES = (
    "input_identity_verification.json",
    "official_route_validation.json",
    "futoi_si_license_access_validation.json",
    "futoi_si_schema_profile.json",
    "futoi_si_daily_positioning.parquet",
    "futoi_si_pit_acceptance_matrix.parquet",
    "coverage_by_source.csv",
    "session_alignment_diagnostics.csv",
    "source_blocker_register.json",
    "gate_results.json",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _authority_path() -> Path:
    return _repo_root() / experimental.AUTHORITY_REPO_PATH


def _git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=_repo_root(),
        text=True,
    ).strip()


def _bind_test_data_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        experimental,
        "_data_root",
        lambda: tmp_path.resolve(),
    )


def _base_request(
    tmp_path: Path,
    *,
    run_id: str = experimental.AUTHORIZED_RUN_ID,
    output_dir: Path | None = None,
    git_commit_sha: str | None = None,
) -> base.RuntimeRequest:
    def json_file(name: str) -> Path:
        path = tmp_path / name
        path.write_text("{}", encoding="utf-8")
        return path

    return base.RuntimeRequest(
        modeling_dataset_path=tmp_path / "modeling.parquet",
        dataset_manifest_path=json_file("manifest.json"),
        feature_schema_path=json_file("schema.json"),
        m0_validation_predictions_path=tmp_path / "predictions.parquet",
        phase83_aggregate_metrics_path=json_file("phase83_metrics.json"),
        phase83_gate_results_path=json_file("phase83_gates.json"),
        experiment_contract_path=json_file("parent_contract.json"),
        license_access_evidence_path=json_file("license.json"),
        pit_semantics_evidence_path=json_file("pit.json"),
        output_dir=(
            output_dir
            if output_dir is not None
            else tmp_path / experimental.CANONICAL_OUTPUT_RELATIVE
        ),
        run_id=run_id,
        git_commit_sha=git_commit_sha or _git_head(),
    )


def _request(tmp_path: Path) -> experimental.ExperimentalRuntimeRequest:
    return experimental.ExperimentalRuntimeRequest(
        base_request=_base_request(tmp_path),
        authority_contract_path=_authority_path(),
    )


def test_data_root_binding_rejects_alternate_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "canonical"
    alternate = tmp_path / "alternate"
    canonical.mkdir()
    alternate.mkdir()
    monkeypatch.setattr(
        experimental,
        "AUTHORIZED_DATA_ROOT",
        canonical.resolve(),
    )

    monkeypatch.setenv("MOEX_DATA_ROOT", str(alternate))
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._data_root()
    assert raised.value.blocker == "provenance_not_sufficient"

    monkeypatch.setenv("MOEX_DATA_ROOT", str(canonical))
    assert experimental._data_root() == canonical.resolve()


def test_symlinked_authorized_descendant_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    (tmp_path / "research").symlink_to(outside, target_is_directory=True)

    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._authority_marker_path()
    assert raised.value.blocker == "provenance_not_sufficient"


def test_checked_in_authority_is_bound_to_current_git_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    request = _request(tmp_path)

    payload = experimental._verify_experimental_authority(request)

    assert payload["authority"]["mode"] == experimental.AUTHORITY_MODE
    assert payload["single_execution"]["authorized_run_id"] == (
        experimental.AUTHORIZED_RUN_ID
    )
    assert payload["single_execution"]["authorized_data_root"] == (
        experimental.AUTHORIZED_DATA_ROOT.as_posix()
    )
    assert payload["single_execution"]["authority_reuse_allowed"] is False
    assert payload["authority_blob_sha1"] == base._git_blob_sha1(
        _authority_path()
    )
    assert payload["authority"]["production_prediction_allowed"] is False
    assert payload["authority"]["model_fitting_allowed"] is False
    assert payload["authority"]["raw_payload_redistribution_allowed"] is False
    assert payload["authority"]["trading_action_allowed"] is False


def test_authority_rejects_wrong_run_id_and_output_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    wrong_run = experimental.ExperimentalRuntimeRequest(
        base_request=_base_request(
            tmp_path,
            run_id="phase8_7a_futoi_si_source_validation_20260804_v2",
        ),
        authority_contract_path=_authority_path(),
    )
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._verify_experimental_authority(wrong_run)
    assert raised.value.blocker == "provenance_not_sufficient"

    wrong_output = experimental.ExperimentalRuntimeRequest(
        base_request=_base_request(
            tmp_path,
            output_dir=tmp_path / "another-output",
        ),
        authority_contract_path=_authority_path(),
    )
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._verify_experimental_authority(wrong_output)
    assert raised.value.blocker == "provenance_not_sufficient"


def test_authority_consumption_is_atomic_and_rejects_reuse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    request = _request(tmp_path)

    marker = experimental._consume_authority(request)
    payload = json.loads(marker.read_text(encoding="utf-8"))

    assert payload["run_id"] == experimental.AUTHORIZED_RUN_ID
    assert payload["git_commit_sha"] == request.base_request.git_commit_sha
    assert marker == (
        tmp_path / experimental.CONSUMPTION_MARKER_RELATIVE
    )

    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._consume_authority(request)
    assert raised.value.blocker == "provenance_not_sufficient"


def _mock_runtime_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    *,
    eligible: pd.DataFrame,
    validation_ids: pd.DataFrame,
    gates: dict[str, dict[str, bool]],
    observed: dict[str, object],
) -> None:
    monkeypatch.setattr(
        experimental,
        "_verify_experimental_authority",
        lambda request: {
            "authority_blob_sha1": base._git_blob_sha1(
                request.authority_contract_path
            )
        },
    )
    monkeypatch.setattr(
        base,
        "_verify_frozen_inputs",
        lambda _request: {"modeling_dataset": "a" * 64},
    )
    monkeypatch.setattr(
        experimental.pd,
        "read_parquet",
        lambda _path: pd.DataFrame(),
    )
    monkeypatch.setattr(
        base,
        "_identity_frames",
        lambda _modeling, _predictions: (eligible, validation_ids),
    )
    monkeypatch.setattr(
        base,
        "_license_access_validation",
        lambda _evidence: (
            False,
            {
                "provider": "MOEX AlgoPack FUTOI",
                "approved": False,
            },
        ),
    )
    monkeypatch.setattr(base, "_pit_semantics_passed", lambda _evidence: False)
    monkeypatch.setattr(
        base.validation.algopack_http,
        "load_algopack_token",
        lambda: "token",
    )

    calls: list[str] = []

    def load_pair(source_date, *, bearer_token):
        assert bearer_token == "token"
        calls.append(source_date.isoformat())
        return object(), tuple(base.validation.RAW_REQUIRED_FIELDS)

    monkeypatch.setattr(base.validation, "load_futoi_daily_pair", load_pair)
    monkeypatch.setattr(
        base.validation,
        "build_futoi_pit_acceptance_matrix",
        lambda _eligible, _pairs: (
            pd.DataFrame({"accepted": [True] * len(eligible)}),
            pd.DataFrame({"diagnostic": ["ok"]}),
        ),
    )
    monkeypatch.setattr(
        base.validation,
        "coverage_by_source",
        lambda _matrix, _validation_ids: pd.DataFrame({"coverage": [1.0]}),
    )

    def evaluate_gates(**kwargs):
        observed["gate_kwargs"] = kwargs
        return gates

    monkeypatch.setattr(base.validation, "evaluate_gates", evaluate_gates)

    def write_artifacts(_output_dir, **kwargs):
        observed["artifact_kwargs"] = kwargs
        return ARTIFACT_NAMES

    monkeypatch.setattr(
        base.validation,
        "write_validation_artifacts",
        write_artifacts,
    )
    observed["calls"] = calls


def _gate_results(*, fail_g6: bool = False) -> dict[str, dict[str, bool]]:
    failed = {
        "G3_license_and_access",
        "G5_pit_publication_semantics",
        "G9_final_acceptance",
    }
    if fail_g6:
        failed.add("G6_exact_coverage")
    return {
        name: {"passed": name not in failed}
        for name in (
            "G1_immutable_inputs",
            "G2_exact_route_and_transport",
            "G3_license_and_access",
            "G4_schema_and_pairing",
            "G5_pit_publication_semantics",
            "G6_exact_coverage",
            "G7_numerical_and_chronology",
            "G8_provenance_and_no_leakage",
            "G9_final_acceptance",
        )
    }


def test_runtime_retrieves_once_and_preserves_unverified_gates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    request = _request(tmp_path)
    eligible = pd.DataFrame(
        {
            "target_trade_date": ["2026-07-30", "2026-07-31"],
            "target_instrument_id": [
                "forts.usdrubf",
                "forts.usdrubf",
            ],
            "prior_trade_date": ["2026-07-29", "2026-07-30"],
        }
    )
    validation_ids = eligible.loc[
        :, ["target_trade_date", "target_instrument_id"]
    ].copy()
    observed: dict[str, object] = {}
    _mock_runtime_dependencies(
        monkeypatch,
        eligible=eligible,
        validation_ids=validation_ids,
        gates=_gate_results(),
        observed=observed,
    )

    result = experimental.execute(request)

    assert observed["calls"] == ["2026-07-29", "2026-07-30"]
    assert result["artifact_count"] == 10
    assert result["technical_gates_passed"] is True
    assert result["final_status"] == experimental.EXPERIMENTAL_STATUS
    assert result["authority_consumed"] is True
    assert result["production_use_allowed"] is False
    assert experimental._authority_marker_path().exists()

    gate_kwargs = observed["gate_kwargs"]
    assert isinstance(gate_kwargs, dict)
    assert gate_kwargs["license_access_passed"] is False
    assert gate_kwargs["pit_semantics_verified"] is False

    artifact_kwargs = observed["artifact_kwargs"]
    assert isinstance(artifact_kwargs, dict)
    gate_artifact = artifact_kwargs["gates"]
    blocker_artifact = artifact_kwargs["blockers"]
    assert gate_artifact["gates"]["G3_license_and_access"]["passed"] is False
    assert (
        gate_artifact["gates"]["G5_pit_publication_semantics"]["passed"]
        is False
    )
    assert gate_artifact["gates"]["G9_final_acceptance"]["passed"] is False
    assert blocker_artifact["historical_model_use_status"] == "experimental_only"
    assert blocker_artifact["experimental_authority"]["promotion_allowed"] is False
    assert blocker_artifact["experimental_authority"]["trading_allowed"] is False


def test_technical_gate_failure_keeps_source_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    request = _request(tmp_path)
    eligible = pd.DataFrame(
        {
            "target_trade_date": ["2026-07-30"],
            "target_instrument_id": ["forts.usdrubf"],
            "prior_trade_date": ["2026-07-29"],
        }
    )
    validation_ids = eligible.loc[
        :, ["target_trade_date", "target_instrument_id"]
    ].copy()
    observed: dict[str, object] = {}
    _mock_runtime_dependencies(
        monkeypatch,
        eligible=eligible,
        validation_ids=validation_ids,
        gates=_gate_results(fail_g6=True),
        observed=observed,
    )

    result = experimental.execute(request)

    assert result["technical_gates_passed"] is False
    assert result["final_status"] == experimental.FAIL_STATUS

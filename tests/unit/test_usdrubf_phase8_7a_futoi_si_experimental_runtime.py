from __future__ import annotations

import json
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
    return (
        _repo_root()
        / "contracts"
        / "experiments"
        / "usdrubf_phase8_7a_futoi_si_experimental_runtime_authority_v1.json"
    )


def _base_request(tmp_path: Path) -> base.RuntimeRequest:
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
        output_dir=tmp_path / "output",
        run_id="phase8_7a_futoi_si_source_validation_20260803_v1",
        git_commit_sha="1" * 40,
    )


def test_checked_in_authority_is_exact_and_fail_closed() -> None:
    payload = experimental._verify_experimental_authority(_authority_path())
    assert payload["authority"]["mode"] == experimental.AUTHORITY_MODE
    assert payload["authority"]["historical_authenticated_retrieval_allowed"] is True
    assert payload["authority"]["production_prediction_allowed"] is False
    assert payload["authority"]["model_fitting_allowed"] is False
    assert payload["authority"]["raw_payload_redistribution_allowed"] is False
    assert payload["authority"]["trading_action_allowed"] is False


def test_authority_digest_rejects_tampering(tmp_path: Path) -> None:
    payload = json.loads(_authority_path().read_text(encoding="utf-8"))
    payload["authority"]["model_fitting_allowed"] = True
    tampered = tmp_path / "authority.json"
    tampered.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._verify_experimental_authority(tampered)
    assert raised.value.blocker == "provenance_not_sufficient"


def test_experimental_runtime_retrieves_with_unverified_license_and_keeps_gates_failed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = experimental.ExperimentalRuntimeRequest(
        base_request=_base_request(tmp_path),
        authority_contract_path=_authority_path(),
    )
    eligible = pd.DataFrame(
        {
            "target_trade_date": ["2026-07-30", "2026-07-31"],
            "target_instrument_id": ["forts.usdrubf", "forts.usdrubf"],
            "prior_trade_date": ["2026-07-29", "2026-07-30"],
        }
    )
    validation_ids = eligible.loc[
        :, ["target_trade_date", "target_instrument_id"]
    ].copy()
    observed: dict[str, object] = {}

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
            pd.DataFrame({"accepted": [True, True]}),
            pd.DataFrame({"diagnostic": ["ok"]}),
        ),
    )
    monkeypatch.setattr(
        base.validation,
        "coverage_by_source",
        lambda _matrix, _validation_ids: pd.DataFrame({"coverage": [1.0]}),
    )

    gates = {
        name: {"passed": name not in {"G3", "G5", "G9"}}
        for name in ("G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8", "G9")
    }

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

    result = experimental.execute(request)

    assert calls == ["2026-07-29", "2026-07-30"]
    assert result["artifact_count"] == 10
    assert result["technical_gates_passed"] is True
    assert result["final_status"] == experimental.EXPERIMENTAL_STATUS
    assert result["production_use_allowed"] is False

    gate_kwargs = observed["gate_kwargs"]
    assert isinstance(gate_kwargs, dict)
    assert gate_kwargs["license_access_passed"] is False
    assert gate_kwargs["pit_semantics_verified"] is False

    artifact_kwargs = observed["artifact_kwargs"]
    assert isinstance(artifact_kwargs, dict)
    gate_artifact = artifact_kwargs["gates"]
    blocker_artifact = artifact_kwargs["blockers"]
    assert gate_artifact["gates"]["G3"]["passed"] is False
    assert gate_artifact["gates"]["G5"]["passed"] is False
    assert gate_artifact["gates"]["G9"]["passed"] is False
    assert blocker_artifact["historical_model_use_status"] == "experimental_only"
    assert blocker_artifact["experimental_authority"]["promotion_allowed"] is False
    assert blocker_artifact["experimental_authority"]["trading_allowed"] is False


def test_technical_gate_failure_keeps_source_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = experimental.ExperimentalRuntimeRequest(
        base_request=_base_request(tmp_path),
        authority_contract_path=_authority_path(),
    )
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

    monkeypatch.setattr(base, "_verify_frozen_inputs", lambda _request: {})
    monkeypatch.setattr(experimental.pd, "read_parquet", lambda _path: pd.DataFrame())
    monkeypatch.setattr(
        base,
        "_identity_frames",
        lambda _modeling, _predictions: (eligible, validation_ids),
    )
    monkeypatch.setattr(
        base,
        "_license_access_validation",
        lambda _evidence: (False, {"approved": False}),
    )
    monkeypatch.setattr(base, "_pit_semantics_passed", lambda _evidence: False)
    monkeypatch.setattr(
        base.validation.algopack_http,
        "load_algopack_token",
        lambda: "token",
    )
    monkeypatch.setattr(
        base.validation,
        "load_futoi_daily_pair",
        lambda _date, *, bearer_token: (
            object(),
            tuple(base.validation.RAW_REQUIRED_FIELDS),
        ),
    )
    monkeypatch.setattr(
        base.validation,
        "build_futoi_pit_acceptance_matrix",
        lambda _eligible, _pairs: (pd.DataFrame(), pd.DataFrame()),
    )
    monkeypatch.setattr(
        base.validation,
        "coverage_by_source",
        lambda _matrix, _validation_ids: pd.DataFrame(),
    )
    gates = {
        name: {"passed": name not in {"G3", "G5", "G6", "G9"}}
        for name in ("G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8", "G9")
    }
    monkeypatch.setattr(
        base.validation,
        "evaluate_gates",
        lambda **_kwargs: gates,
    )
    monkeypatch.setattr(
        base.validation,
        "write_validation_artifacts",
        lambda _output_dir, **_kwargs: ARTIFACT_NAMES,
    )

    result = experimental.execute(request)

    assert result["technical_gates_passed"] is False
    assert result["final_status"] == experimental.FAIL_STATUS

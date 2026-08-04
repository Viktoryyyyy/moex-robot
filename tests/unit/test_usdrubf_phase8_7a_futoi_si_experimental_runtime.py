from __future__ import annotations

import json
import os
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
RUN_ID = "phase8_7a_futoi_si_source_validation_20260804_v1"
AUTHORIZATION_ID = "futoi-si-20260804-v1"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _policy_path() -> Path:
    return _repo_root() / experimental.POLICY_REPO_PATH


def _git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=_repo_root(),
        text=True,
    ).strip()


def _request_data_root(request: base.RuntimeRequest) -> Path:
    root = request.output_dir
    for _ in experimental._canonical_output_relative(request.run_id).parts:
        root = root.parent
    return root.resolve()


def _bind_test_data_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path.resolve()
    monkeypatch.setattr(experimental, "_data_root", lambda: root)

    def open_root(
        expected_device: int | None = None,
        expected_inode: int | None = None,
    ) -> int:
        descriptor = os.open(root, experimental.OPEN_DIRECTORY_FLAGS)
        metadata = os.fstat(descriptor)
        if (expected_device is None) != (expected_inode is None):
            os.close(descriptor)
            raise experimental._fail("canonical data root identity is incomplete")
        if expected_device is not None and (
            metadata.st_dev != expected_device
            or metadata.st_ino != expected_inode
        ):
            os.close(descriptor)
            raise experimental._fail(
                "canonical data root physical identity mismatch"
            )
        return descriptor

    monkeypatch.setattr(experimental, "_open_canonical_data_root", open_root)


def _bind_test_authority_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Path:
    trusted_root = tmp_path / "trusted-authorities"
    trusted_root.mkdir(mode=0o750, exist_ok=True)
    trusted_root.chmod(0o750)
    monkeypatch.setattr(
        experimental,
        "TRUSTED_AUTHORITY_ROOT",
        trusted_root.resolve(),
    )
    monkeypatch.setattr(
        experimental,
        "TRUSTED_AUTHORITY_OWNER_UID",
        os.getuid(),
    )
    monkeypatch.setattr(
        experimental,
        "_open_trusted_authority_root",
        lambda: os.open(trusted_root, experimental.OPEN_DIRECTORY_FLAGS),
    )
    return trusted_root.resolve()


def _bind_test_roots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Path:
    _bind_test_data_root(monkeypatch, tmp_path)
    return _bind_test_authority_root(monkeypatch, tmp_path)


def _base_request(
    tmp_path: Path,
    *,
    run_id: str = RUN_ID,
    output_dir: Path | None = None,
    git_commit_sha: str | None = None,
) -> base.RuntimeRequest:
    def json_file(name: str) -> Path:
        path = tmp_path / name
        path.write_text("{}", encoding="utf-8")
        return path

    expected_output = (
        tmp_path
        / experimental.OUTPUT_PARENT_RELATIVE
        / f"run_id={run_id}"
    )
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
        output_dir=output_dir or expected_output,
        run_id=run_id,
        git_commit_sha=git_commit_sha or _git_head(),
    )


def _authority_payload(request: base.RuntimeRequest) -> dict[str, object]:
    root_metadata = _request_data_root(request).stat()
    return {
        "project": "MOEX_Bot",
        "task_id": experimental.TASK_ID,
        "authorization_id": AUTHORIZATION_ID,
        "approved_by": "PM_L2_PHASE_OWNER",
        "mode": experimental.AUTHORITY_MODE,
        "git_commit_sha": request.git_commit_sha,
        "run_id": request.run_id,
        "data_root": experimental.AUTHORIZED_DATA_ROOT.as_posix(),
        "data_root_device": root_metadata.st_dev,
        "data_root_inode": root_metadata.st_ino,
        "output_dir": request.output_dir.absolute().as_posix(),
        "issued_at": "2026-08-04T08:00:00+03:00",
        "historical_authenticated_retrieval_allowed": True,
        "phase8_7a_source_validation_allowed": True,
        "phase8_7b_feature_computation_allowed": False,
        "model_fitting_allowed": False,
        "production_prediction_allowed": False,
        "model_or_strategy_promotion_allowed": False,
        "raw_payload_redistribution_allowed": False,
        "broker_action_allowed": False,
        "trading_action_allowed": False,
    }


def _authority_file(
    request: base.RuntimeRequest,
    *,
    mutate=None,
    filename: str | None = None,
) -> Path:
    payload = _authority_payload(request)
    if mutate is not None:
        mutate(payload)
    path = experimental.TRUSTED_AUTHORITY_ROOT / (
        filename or f"{payload['authorization_id']}.json"
    )
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o640)
    return path


def _request(
    tmp_path: Path,
    *,
    mutate_authority=None,
) -> experimental.ExperimentalRuntimeRequest:
    base_request = _base_request(tmp_path)
    return experimental.ExperimentalRuntimeRequest(
        base_request=base_request,
        policy_contract_path=_policy_path(),
        runtime_authority_evidence_path=_authority_file(
            base_request,
            mutate=mutate_authority,
        ),
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


def test_data_root_physical_identity_match_and_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    metadata = tmp_path.stat()

    experimental._assert_data_root_identity(metadata.st_dev, metadata.st_ino)
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._assert_data_root_identity(
            metadata.st_dev,
            metadata.st_ino + 1,
        )
    assert raised.value.blocker == "provenance_not_sufficient"


def test_data_root_symlinked_ancestor_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    physical = tmp_path / "physical"
    physical.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(physical, target_is_directory=True)
    monkeypatch.setattr(experimental, "AUTHORIZED_DATA_ROOT", linked)
    monkeypatch.setenv("MOEX_DATA_ROOT", str(linked))

    original_validator = experimental._validate_data_root_metadata

    def validate(metadata: os.stat_result, *, label: str) -> None:
        if label in {"filesystem root", "canonical data-root ancestor tmp"}:
            assert os.path.isdir("/")
            return
        original_validator(metadata, label=label)

    monkeypatch.setattr(experimental, "_validate_data_root_metadata", validate)
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._open_canonical_data_root()
    assert raised.value.blocker == "provenance_not_sufficient"


def test_data_root_writable_ancestor_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    writable = tmp_path / "writable-component"
    data_root = writable / "data"
    data_root.mkdir(parents=True)
    writable.chmod(0o770)
    monkeypatch.setattr(experimental, "AUTHORIZED_DATA_ROOT", data_root)
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))

    original_validator = experimental._validate_data_root_metadata

    def validate(metadata: os.stat_result, *, label: str) -> None:
        if label in {"filesystem root", "canonical data-root ancestor tmp"}:
            return
        original_validator(metadata, label=label)

    monkeypatch.setattr(experimental, "_validate_data_root_metadata", validate)
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._open_canonical_data_root()
    assert raised.value.blocker == "provenance_not_sufficient"


def test_dirty_worktree_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fake_run_git(
        _repo_root: Path,
        *args: str,
        allow_empty: bool = False,
    ) -> str:
        assert allow_empty is True
        assert args[:2] == ("status", "--porcelain=v1")
        return "?? untracked_runtime.py"

    monkeypatch.setattr(experimental, "_run_git", fake_run_git)
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._verify_clean_worktree(tmp_path)
    assert raised.value.blocker == "provenance_not_sufficient"


def test_trusted_authority_is_captured_from_one_descriptor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_authority_root(monkeypatch, tmp_path)
    request = _base_request(tmp_path)
    path = _authority_file(request)

    payload, raw, opened_path = experimental._read_trusted_authority_once(path)
    path.write_text('{"value":2}\n', encoding="utf-8")

    assert payload["authorization_id"] == AUTHORIZATION_ID
    assert json.loads(raw.decode("utf-8"))["authorization_id"] == AUTHORIZATION_ID
    assert opened_path == path
    assert experimental._sha256_bytes(raw) != experimental._sha256_bytes(
        path.read_bytes()
    )


def test_trusted_authority_rejects_filename_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_authority_root(monkeypatch, tmp_path)
    request = _base_request(tmp_path)
    path = _authority_file(request, filename="wrong-name.json")

    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._read_trusted_authority_once(path)
    assert raised.value.blocker == "provenance_not_sufficient"


def test_trusted_authority_rejects_group_writable_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_authority_root(monkeypatch, tmp_path)
    request = _base_request(tmp_path)
    path = _authority_file(request)
    path.chmod(0o660)

    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._read_trusted_authority_once(path)
    assert raised.value.blocker == "provenance_not_sufficient"


def test_symlinked_output_descendant_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    (tmp_path / "research").symlink_to(outside, target_is_directory=True)
    metadata = tmp_path.stat()

    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._create_output_directory(
            RUN_ID,
            data_root_device=metadata.st_dev,
            data_root_inode=metadata.st_ino,
        )
    assert raised.value.blocker == "provenance_not_sufficient"


def test_checked_in_policy_is_tracked_but_not_runtime_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    base_request = _base_request(tmp_path)
    request = experimental.ExperimentalRuntimeRequest(
        base_request=base_request,
        policy_contract_path=_policy_path(),
        runtime_authority_evidence_path=tmp_path / "unused-authority.json",
    )

    summary = experimental._verify_policy_contract(request)

    assert summary["contract_id"] == experimental.POLICY_CONTRACT_ID
    assert summary["contract_version"] == experimental.POLICY_CONTRACT_VERSION
    policy = json.loads(_policy_path().read_text(encoding="utf-8"))
    assert policy["policy_boundary"]["checked_in_policy_is_runtime_authority"] is False
    assert policy["policy_boundary"]["runtime_authority_must_bind_data_root_identity"] is True
    assert policy["policy_boundary"]["trusted_runtime_authority_root"] == (
        experimental.TRUSTED_AUTHORITY_ROOT.as_posix()
    )


def test_external_runtime_authority_binds_exact_invocation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_roots(monkeypatch, tmp_path)
    request = _request(tmp_path)

    summary = experimental._verify_runtime_authority(request)
    metadata = tmp_path.stat()

    assert summary["authorization_id"] == AUTHORIZATION_ID
    assert summary["git_commit_sha"] == request.base_request.git_commit_sha
    assert summary["run_id"] == RUN_ID
    assert summary["data_root_device"] == metadata.st_dev
    assert summary["data_root_inode"] == metadata.st_ino
    assert summary["output_dir"] == request.base_request.output_dir.as_posix()
    assert summary["evidence_path"] == (
        request.runtime_authority_evidence_path.as_posix()
    )
    assert summary["trusted_owner_uid"] == os.getuid()
    assert summary["global_single_use_claimed"] is False
    assert summary["production_use_allowed"] is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("git_commit_sha", "0" * 40),
        ("run_id", "phase8_7a_futoi_si_source_validation_20260804_v2"),
        ("data_root_device", 0),
        ("data_root_inode", 0),
        ("output_dir", "/tmp/not-authorized"),
        ("historical_authenticated_retrieval_allowed", False),
        ("model_fitting_allowed", True),
    ],
)
def test_runtime_authority_rejects_mismatched_invocation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    _bind_test_roots(monkeypatch, tmp_path)

    def mutate(payload: dict[str, object]) -> None:
        payload[field] = value

    request = _request(tmp_path, mutate_authority=mutate)
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._verify_runtime_authority(request)
    assert raised.value.blocker == "provenance_not_sufficient"


def test_runtime_authority_rejects_wrong_physical_data_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_roots(monkeypatch, tmp_path)

    def mutate(payload: dict[str, object]) -> None:
        payload["data_root_inode"] = int(payload["data_root_inode"]) + 1

    request = _request(tmp_path, mutate_authority=mutate)
    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._verify_runtime_authority(request)
    assert raised.value.blocker == "provenance_not_sufficient"


def test_runtime_authority_outside_trusted_root_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_roots(monkeypatch, tmp_path)
    request = _request(tmp_path)
    outside = tmp_path / f"{AUTHORIZATION_ID}.json"
    outside.write_bytes(request.runtime_authority_evidence_path.read_bytes())
    outside.chmod(0o640)
    request = experimental.ExperimentalRuntimeRequest(
        base_request=request.base_request,
        policy_contract_path=request.policy_contract_path,
        runtime_authority_evidence_path=outside,
    )

    with pytest.raises(base.validation.FutoiSiSourceValidationError) as raised:
        experimental._verify_runtime_authority(request)
    assert raised.value.blocker == "provenance_not_sufficient"


def test_secure_writer_creates_exact_artifact_inventory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_data_root(monkeypatch, tmp_path)
    output_dir = (
        tmp_path / experimental.OUTPUT_PARENT_RELATIVE / f"run_id={RUN_ID}"
    )
    metadata = tmp_path.stat()
    names = experimental._write_validation_artifacts_secure(
        output_dir,
        run_id=RUN_ID,
        data_root_device=metadata.st_dev,
        data_root_inode=metadata.st_ino,
        input_identity_verification={"project": "MOEX_Bot"},
        route_validation={"route_validated": True},
        license_validation={"status": "blocked"},
        schema_profile={"schema_stable": True},
        pairs=[],
        matrix=pd.DataFrame({"accepted": [True]}),
        coverage=pd.DataFrame({"coverage": [1.0]}),
        diagnostics=pd.DataFrame({"diagnostic": ["ok"]}),
        blockers={"blockers": []},
        gates={"gates": {}},
    )

    assert names == tuple(sorted(ARTIFACT_NAMES))
    assert tuple(sorted(path.name for path in output_dir.iterdir())) == names


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
        "_verify_policy_contract",
        lambda _request: {
            "contract_id": experimental.POLICY_CONTRACT_ID,
            "contract_version": experimental.POLICY_CONTRACT_VERSION,
            "sha256": "a" * 64,
        },
    )

    def authority(request):
        metadata = experimental._data_root().stat()
        return {
            "authorization_id": AUTHORIZATION_ID,
            "run_id": request.base_request.run_id,
            "data_root_device": metadata.st_dev,
            "data_root_inode": metadata.st_ino,
            "evidence_sha256": "b" * 64,
            "production_use_allowed": False,
            "promotion_allowed": False,
            "trading_allowed": False,
        }

    monkeypatch.setattr(experimental, "_verify_runtime_authority", authority)
    monkeypatch.setattr(
        base,
        "_verify_frozen_inputs",
        lambda _request: {"modeling_dataset": "c" * 64},
    )
    monkeypatch.setattr(experimental.pd, "read_parquet", lambda _path: pd.DataFrame())
    monkeypatch.setattr(
        base,
        "_identity_frames",
        lambda _modeling, _predictions: (eligible, validation_ids),
    )
    monkeypatch.setattr(
        base,
        "_license_access_validation",
        lambda _evidence: (False, {"provider": "MOEX AlgoPack FUTOI", "approved": False}),
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
        experimental,
        "_write_validation_artifacts_secure",
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


def test_runtime_retrieves_with_external_authority_and_preserves_gates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_roots(monkeypatch, tmp_path)
    request = _request(tmp_path)
    eligible = pd.DataFrame(
        {
            "target_trade_date": ["2026-07-30", "2026-07-31"],
            "target_instrument_id": ["forts.usdrubf", "forts.usdrubf"],
            "prior_trade_date": ["2026-07-29", "2026-07-30"],
        }
    )
    validation_ids = eligible.loc[:, ["target_trade_date", "target_instrument_id"]].copy()
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
    assert result["runtime_authorization_id"] == AUTHORIZATION_ID
    assert result["production_use_allowed"] is False

    gate_kwargs = observed["gate_kwargs"]
    assert isinstance(gate_kwargs, dict)
    assert gate_kwargs["license_access_passed"] is False
    assert gate_kwargs["pit_semantics_verified"] is False
    artifact_kwargs = observed["artifact_kwargs"]
    assert isinstance(artifact_kwargs, dict)
    assert artifact_kwargs["data_root_device"] == tmp_path.stat().st_dev
    assert artifact_kwargs["data_root_inode"] == tmp_path.stat().st_ino
    gates = artifact_kwargs["gates"]["gates"]
    assert gates["G3_license_and_access"]["passed"] is False
    assert gates["G5_pit_publication_semantics"]["passed"] is False
    assert gates["G9_final_acceptance"]["passed"] is False


def test_technical_gate_failure_keeps_source_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bind_test_roots(monkeypatch, tmp_path)
    request = _request(tmp_path)
    eligible = pd.DataFrame(
        {
            "target_trade_date": ["2026-07-30"],
            "target_instrument_id": ["forts.usdrubf"],
            "prior_trade_date": ["2026-07-29"],
        }
    )
    validation_ids = eligible.loc[:, ["target_trade_date", "target_instrument_id"]].copy()
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

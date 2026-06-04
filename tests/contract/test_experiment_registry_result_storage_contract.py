from __future__ import annotations

import json
from pathlib import Path

import pytest

from moex_research.registry.controlled_writer import (
    CONTROLLED_TEMPDIR,
    PRODUCTION_WRITE,
    dry_run_validate_result_storage,
    write_controlled_result_storage,
)
from moex_research.registry.experiment_registry_contracts import (
    PMReviewCloseout,
    validate_pm_review_closeout,
)
from moex_research.registry.result_storage_contracts import (
    ArtifactBundleManifest,
    ArtifactRef,
    ResultStorageBundle,
    ResultStorageValidationError,
    compute_deterministic_run_id,
)


REPO_COMMIT = "4a04019e0b1a69262ed1edeb45bb980521225c4e"


def _immutable_inputs(**overrides):
    values = {
        "strategy_package_ref": "src/strategies/reference_fixture_strategy",
        "strategy_config_ref": "configs/strategies/reference_fixture_strategy.process_proof.v1.yaml",
        "research_runner_ref": "src/moex_research/runners/request_contracts.py",
        "research_run_request_ref": "artifacts/research/requests/request.json",
        "canonical_backtest_engine_ref": "src/moex_backtest/engine/canonical.py",
        "dataset_version_or_hash": "dataset_hash_001",
        "source_snapshot_ref": "artifacts/data/source_snapshots/futures/source_snapshot.json",
        "data_refresh_manifest_ref": "artifacts/data/manifests/refresh_manifest.json",
        "materialized_partition_ref": "artifacts/data/materialized/futures/d1/partition.parquet",
        "quality_report_ref": "artifacts/data/quality/futures/d1_quality.json",
        "parameter_snapshot_ref": "artifacts/research/params/parameter_snapshot.json",
    }
    values.update(overrides)
    return values


def _artifact(artifact_id, role, locator=None, ref_class="repo_relative", artifact_format="json"):
    return ArtifactRef(
        artifact_id=artifact_id,
        role=role,
        ref_class=ref_class,
        locator=locator or "artifacts/research/registry/" + artifact_id + ".json",
        producer="moex_research.registry",
        consumer="pm_review",
        format=artifact_format,
        schema_version="v1",
    )


def _manifest(run_id, artifacts=None):
    artifacts = artifacts or (
        _artifact("artifact.parameter_snapshot.v1", "parameter_snapshot"),
        _artifact("artifact.canonical_backtest_result.v1", "canonical_backtest_result"),
        _artifact("artifact.research_run_request.v1", "research_run_request"),
        _artifact("artifact.research_runner.v1", "research_runner"),
        _artifact("artifact.strategy_package.v1", "strategy_package"),
        _artifact("artifact.strategy_config.v1", "strategy_config"),
        _artifact("artifact.dataset_manifest.v1", "dataset_manifest"),
        _artifact("artifact.pm_review_closeout.v1", "pm_review_closeout"),
    )
    return ArtifactBundleManifest(
        manifest_id="manifest." + run_id + ".v1",
        run_id=run_id,
        schema_version="artifact_bundle_manifest.v1",
        repo_commit=REPO_COMMIT,
        artifacts=tuple(artifacts),
    )


def _bundle(**overrides):
    immutable_inputs = overrides.pop("immutable_inputs", _immutable_inputs())
    run_id = overrides.pop("run_id", compute_deterministic_run_id(immutable_inputs))
    manifest = overrides.pop("artifact_manifest", _manifest(run_id))
    data_refs = {
        "dataset_version_or_hash": immutable_inputs["dataset_version_or_hash"],
        "source_snapshot_ref": immutable_inputs["source_snapshot_ref"],
        "data_refresh_manifest_ref": immutable_inputs["data_refresh_manifest_ref"],
        "materialized_partition_ref": immutable_inputs["materialized_partition_ref"],
        "quality_report_ref": immutable_inputs["quality_report_ref"],
    }
    result_refs = {
        "parameter_snapshot_ref": "artifact.parameter_snapshot.v1",
        "canonical_backtest_result_ref": "artifact.canonical_backtest_result.v1",
        "research_runner_ref": "artifact.research_runner.v1",
        "research_run_request_ref": "artifact.research_run_request.v1",
        "strategy_package_ref": "artifact.strategy_package.v1",
        "strategy_config_ref": "artifact.strategy_config.v1",
        "dataset_manifest_ref": "artifact.dataset_manifest.v1",
    }
    data_refs.update(overrides.pop("data_refs", {}))
    result_refs.update(overrides.pop("result_refs", {}))
    values = {
        "run_id": run_id,
        "schema_version": "result_storage_bundle.v1",
        "storage_mode": "dry_run",
        "immutable_inputs": immutable_inputs,
        "data_refs": data_refs,
        "result_refs": result_refs,
        "artifact_manifest": manifest,
        "pm_review_closeout_ref": "artifact.pm_review_closeout.v1",
        "finalized": True,
    }
    values.update(overrides)
    return ResultStorageBundle(**values)


def _closeout(run_id, status="pending_pm_review"):
    return PMReviewCloseout(
        review_id="artifact.pm_review_closeout.v1",
        run_id=run_id,
        status=status,
        reviewer_role="PM_L3_DELIVERY_VALIDATION_OWNER",
        reviewed_artifact_refs=("artifact.canonical_backtest_result.v1", "artifact.parameter_snapshot.v1"),
    )


def _registry_entry(bundle, **overrides):
    from moex_research.registry.experiment_registry_contracts import ExperimentRegistryEntry

    values = {
        "registry_entry_id": "registry." + bundle.run_id + ".v1",
        "run_id": bundle.run_id,
        "strategy_id": "reference_fixture_strategy",
        "strategy_version": "0.1.0",
        "run_status": "dry_run_validated",
        "result_status": "not_evaluated",
        "result_storage_bundle_ref": "bundle." + bundle.run_id + ".v1",
        "artifact_bundle_manifest_ref": bundle.artifact_manifest.manifest_id,
        "pm_review_closeout_ref": "artifact.pm_review_closeout.v1",
        "repo_commit": REPO_COMMIT,
        "data_refs": bundle.data_refs,
        "result_refs": bundle.result_refs,
        "immutable_inputs_hash": bundle.immutable_inputs_hash,
        "metadata": {"pm_review_status": "pending_pm_review"},
    }
    values.update(overrides)
    return ExperimentRegistryEntry(**values)


def _valid_triplet():
    bundle = _bundle()
    closeout = _closeout(bundle.run_id)
    entry = _registry_entry(bundle)
    return entry, bundle, closeout


def test_valid_dry_run_registry_entry_with_all_required_refs():
    entry, bundle, closeout = _valid_triplet()
    result = dry_run_validate_result_storage(entry, bundle, closeout)
    assert result.persisted is False
    assert result.registry_entry.run_id == bundle.run_id
    assert bundle.data_refs["dataset_version_or_hash"] == "dataset_hash_001"
    assert bundle.result_refs["canonical_backtest_result_ref"] == "artifact.canonical_backtest_result.v1"


def test_valid_controlled_tempdir_write_creates_complete_bundle_and_manifest(tmp_path):
    entry, bundle, closeout = _valid_triplet()
    result = write_controlled_result_storage(
        registry_entry=entry,
        bundle=bundle,
        pm_review_closeout=closeout,
        storage_mode=CONTROLLED_TEMPDIR,
        storage_root=tmp_path,
    )
    bundle_root = Path(result.bundle_root)
    assert result.persisted is True
    assert (bundle_root / "result_storage_bundle.json").is_file()
    assert (bundle_root / "artifact_bundle_manifest.json").is_file()
    assert (bundle_root / "experiment_registry_entry.json").is_file()
    assert (bundle_root / "pm_review_closeout.json").is_file()


def test_production_write_rejected_fail_closed(tmp_path):
    entry, bundle, closeout = _valid_triplet()
    with pytest.raises(ResultStorageValidationError):
        write_controlled_result_storage(
            registry_entry=entry,
            bundle=bundle,
            pm_review_closeout=closeout,
            storage_mode=PRODUCTION_WRITE,
            storage_root=tmp_path,
        )


def test_server_absolute_path_rejected_unless_external_pattern_contract():
    with pytest.raises(ResultStorageValidationError):
        _artifact("artifact.bad_path.v1", "parameter_snapshot", locator="/home/trader/moex_bot/result.json")
    allowed = _artifact(
        "artifact.external_pattern.v1",
        "dataset_manifest",
        locator="/home/trader/moex_bot/artifacts/{run_id}/manifest.json",
        ref_class="external_pattern",
    )
    assert allowed.ref_class == "external_pattern"
    assert allowed.locator.startswith("/home/trader/")


@pytest.mark.parametrize("bad_ref", ["artifacts/latest/result.json", "artifacts/current/result.json", "artifacts/autodetect/result.json"])
def test_latest_current_autodetect_refs_rejected(bad_ref):
    with pytest.raises(ResultStorageValidationError):
        _artifact("artifact.bad_marker.v1", "parameter_snapshot", locator=bad_ref)


def test_stdout_only_result_rejected():
    with pytest.raises(ResultStorageValidationError):
        _artifact("artifact.stdout.v1", "canonical_backtest_result", locator="stdout")
    with pytest.raises(ResultStorageValidationError):
        _artifact("artifact.stdout_format.v1", "canonical_backtest_result", artifact_format="stdout")


def test_dangling_artifact_refs_rejected():
    with pytest.raises(ResultStorageValidationError):
        _bundle(result_refs={"canonical_backtest_result_ref": "artifact.missing.v1"})


@pytest.mark.parametrize(
    "field_name",
    ["source_snapshot_ref", "data_refresh_manifest_ref", "materialized_partition_ref", "quality_report_ref", "dataset_version_or_hash"],
)
def test_missing_required_data_refs_rejected(field_name):
    with pytest.raises(ResultStorageValidationError):
        _bundle(data_refs={field_name: ""})


def test_parameter_snapshot_required_and_materialized_as_artifact_role():
    run_id = compute_deterministic_run_id(_immutable_inputs())
    artifacts = (
        _artifact("artifact.canonical_backtest_result.v1", "canonical_backtest_result"),
        _artifact("artifact.research_run_request.v1", "research_run_request"),
        _artifact("artifact.pm_review_closeout.v1", "pm_review_closeout"),
    )
    with pytest.raises(ResultStorageValidationError):
        _manifest(run_id, artifacts=artifacts)


def test_run_id_deterministic_for_same_immutable_inputs():
    first = compute_deterministic_run_id(_immutable_inputs())
    second = compute_deterministic_run_id(_immutable_inputs())
    assert first == second
    assert first.startswith("run_")


def test_run_id_collision_rejected_when_immutable_input_changes(tmp_path):
    entry, bundle, closeout = _valid_triplet()
    write_controlled_result_storage(
        registry_entry=entry,
        bundle=bundle,
        pm_review_closeout=closeout,
        storage_mode=CONTROLLED_TEMPDIR,
        storage_root=tmp_path,
    )
    changed_inputs = _immutable_inputs(strategy_version="changed")
    colliding_bundle = _bundle(immutable_inputs=changed_inputs, run_id=bundle.run_id, artifact_manifest=_manifest(bundle.run_id))
    colliding_entry = _registry_entry(colliding_bundle, immutable_inputs_hash=colliding_bundle.immutable_inputs_hash)
    with pytest.raises(ResultStorageValidationError):
        write_controlled_result_storage(
            registry_entry=colliding_entry,
            bundle=colliding_bundle,
            pm_review_closeout=_closeout(colliding_bundle.run_id),
            storage_mode=CONTROLLED_TEMPDIR,
            storage_root=tmp_path,
        )


def test_idempotent_rerun_accepted_for_byte_identical_inputs(tmp_path):
    entry, bundle, closeout = _valid_triplet()
    first = write_controlled_result_storage(
        registry_entry=entry,
        bundle=bundle,
        pm_review_closeout=closeout,
        storage_mode=CONTROLLED_TEMPDIR,
        storage_root=tmp_path,
    )
    second = write_controlled_result_storage(
        registry_entry=entry,
        bundle=bundle,
        pm_review_closeout=closeout,
        storage_mode=CONTROLLED_TEMPDIR,
        storage_root=tmp_path,
    )
    assert first.idempotent is False
    assert second.idempotent is True


@pytest.mark.parametrize(
    "status",
    ["pending_pm_review", "accepted_final", "accepted_as_executed", "conditional_pass", "rejected", "blocked", "invalidated"],
)
def test_pm_review_closeout_status_accepts_only_allowed_statuses(status):
    bundle = _bundle()
    assert validate_pm_review_closeout(_closeout(bundle.run_id, status=status)).status == status


@pytest.mark.parametrize("status", ["approved", "promoted", "live_ready", "runtime_ready", "production_ready", "market_supported"])
def test_promotion_status_values_rejected(status):
    bundle = _bundle()
    with pytest.raises(ResultStorageValidationError):
        _registry_entry(bundle, metadata={"readiness_status": status})
    with pytest.raises(ResultStorageValidationError):
        _closeout(bundle.run_id, status=status)


def test_canonical_backtest_result_ref_required():
    with pytest.raises(ResultStorageValidationError):
        _bundle(result_refs={"canonical_backtest_result_ref": ""})


def test_research_runner_ref_and_research_run_request_ref_required():
    with pytest.raises(ResultStorageValidationError):
        _bundle(result_refs={"research_runner_ref": ""})
    with pytest.raises(ResultStorageValidationError):
        _bundle(result_refs={"research_run_request_ref": ""})


def test_controlled_write_limited_to_fixture_tempdir_local_test_roots(tmp_path):
    entry, bundle, closeout = _valid_triplet()
    with pytest.raises(ResultStorageValidationError):
        write_controlled_result_storage(
            registry_entry=entry,
            bundle=bundle,
            pm_review_closeout=closeout,
            storage_mode="controlled_local_test",
            storage_root=tmp_path / "not_allowed",
        )
    ok_root = tmp_path / "local_test_results"
    result = write_controlled_result_storage(
        registry_entry=entry,
        bundle=bundle,
        pm_review_closeout=closeout,
        storage_mode="controlled_local_test",
        storage_root=ok_root,
    )
    assert result.persisted is True


def test_atomic_write_leaves_no_finalized_bundle_on_validation_failure(tmp_path):
    run_id = compute_deterministic_run_id(_immutable_inputs())
    bad_manifest = _manifest(
        run_id,
        artifacts=(
            _artifact("artifact.parameter_snapshot.v1", "parameter_snapshot"),
            _artifact("artifact.canonical_backtest_result.v1", "canonical_backtest_result"),
            _artifact("artifact.research_run_request.v1", "research_run_request"),
            _artifact("artifact.pm_review_closeout.v1", "pm_review_closeout"),
        ),
    )
    with pytest.raises(ResultStorageValidationError):
        bad_bundle = _bundle(artifact_manifest=bad_manifest, result_refs={"research_runner_ref": "artifact.missing_runner.v1"})
        write_controlled_result_storage(
            registry_entry=_registry_entry(bad_bundle),
            bundle=bad_bundle,
            pm_review_closeout=_closeout(bad_bundle.run_id),
            storage_mode=CONTROLLED_TEMPDIR,
            storage_root=tmp_path,
        )
    assert not any(tmp_path.iterdir())


def test_written_bundle_json_preserves_immutable_inputs_hash(tmp_path):
    entry, bundle, closeout = _valid_triplet()
    result = write_controlled_result_storage(
        registry_entry=entry,
        bundle=bundle,
        pm_review_closeout=closeout,
        storage_mode=CONTROLLED_TEMPDIR,
        storage_root=tmp_path,
    )
    payload = json.loads(Path(result.bundle_ref).read_text(encoding="utf-8"))
    assert payload["immutable_inputs_hash"] == bundle.immutable_inputs_hash

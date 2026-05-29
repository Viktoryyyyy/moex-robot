from pathlib import Path

import pytest

import moex_research.runners.planning_dry_run as planning_module
from moex_research.runners.artifact_manifest_draft import (
    ArtifactManifestDraft,
    ArtifactManifestDraftValidationError,
    validate_artifact_manifest_draft,
)
from moex_research.runners.execution_request import ExecutionArtifactPlan, StrategyTestingExecutionRequest
from moex_research.runners.planning_dry_run import (
    PLANNING_DRY_RUN_RESULT_FIELDS,
    PlanningDryRunValidationError,
    run_planning_dry_run,
    validate_planning_dry_run_inputs,
    validate_planning_dry_run_result,
)
from moex_research.runners.registry_entry_draft import (
    RegistryEntryDraft,
    RegistryEntryDraftValidationError,
    validate_registry_entry_draft,
)
from tests.fixtures.strategy_testing.ema_3_19_plan_only_execution_request import (
    EMA_3_19_PLAN_ONLY_ARTIFACT_MANIFEST_REF,
    EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN,
    EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATHS = (
    REPO_ROOT / "src" / "moex_research" / "runners" / "planning_dry_run.py",
    REPO_ROOT / "src" / "moex_research" / "runners" / "artifact_manifest_draft.py",
    REPO_ROOT / "src" / "moex_research" / "runners" / "registry_entry_draft.py",
)
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "dry_run_status",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "planned_execution_status",
        "planned_artifact_integration_status",
        "artifact_manifest_draft_id_or_none",
        "registry_entry_draft_id_or_none",
        "write_allowed",
        "registry_write_allowed",
        "promotion_" + "verdict_allowed",
        "error_message_or_none",
    }
)


def _decision_gate_field() -> str:
    return "promotion_" + "verdict_allowed"


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _legacy_strategy_marker() -> str:
    return "d1_" + "ts" + "mom"


def _legacy_strategy_short_marker() -> str:
    return "ts" + "mom"


def _market_access() -> str:
    return "li" + "ve"


def _scheduler_access() -> str:
    return "run" + "time"


def _external_actor() -> str:
    return "bro" + "ker"


def _intent_marker() -> str:
    return "or" + "der"


def _artifact_draft_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "artifact_manifest_draft_id": "artifact_manifest_draft.strategy_test.ema_3_19.plan_only.v1",
        "request_id": EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.request_id,
        "strategy_id": EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.strategy_id,
        "strategy_test_id": EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.strategy_test_id,
        "planned_artifacts": EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN.required_output_artifacts,
        "artifact_manifest_ref": EMA_3_19_PLAN_ONLY_ARTIFACT_MANIFEST_REF,
    }
    values.update(overrides)
    return values


def _registry_draft_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "registry_entry_draft_id": "registry_entry_draft.strategy_test.ema_3_19.plan_only.v1",
        "request_id": EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.request_id,
        "strategy_id": EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.strategy_id,
        "strategy_test_id": EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.strategy_test_id,
        "planned_registry_entry_ref": "registry_entry.strategy_test.ema_3_19.plan_only.v1",
        "artifact_manifest_ref": EMA_3_19_PLAN_ONLY_ARTIFACT_MANIFEST_REF,
        "metrics_artifact_ref_or_none": None,
        "report_artifact_ref_or_none": None,
    }
    values.update(overrides)
    return values


def _request(**overrides: object) -> StrategyTestingExecutionRequest:
    values = dict(EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.__dict__)
    values.update(overrides)
    return StrategyTestingExecutionRequest(**values)


def _artifact_plan(**overrides: object) -> ExecutionArtifactPlan:
    values = dict(EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN.__dict__)
    values.update(overrides)
    return ExecutionArtifactPlan(**values)


def test_full_ema_3_19_planning_dry_run_succeeds_in_memory():
    result = run_planning_dry_run()

    assert validate_planning_dry_run_result(result) is result
    assert result.dry_run_status == "planned"
    assert result.request_id == EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.request_id
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST.strategy_test_id
    assert result.planned_execution_status == "planned"
    assert result.planned_artifact_integration_status == "planned"
    assert result.artifact_manifest_draft_id_or_none is not None
    assert result.registry_entry_draft_id_or_none is not None
    assert result.write_allowed is False
    assert result.registry_write_allowed is False
    assert getattr(result, _decision_gate_field()) is False
    assert result.error_message_or_none is None


def test_planning_dry_run_imports_only_approved_ema_fixture_path():
    source = (REPO_ROOT / "src" / "moex_research" / "runners" / "planning_dry_run.py").read_text(
        encoding="utf-8"
    )

    assert "tests.fixtures.strategy_testing.ema_3_19_plan_only_execution_request" in source
    assert source.count("tests.fixtures.strategy_testing.") == 1
    assert "tests.fixtures.strategy_testing.ema_3_19_package" not in source


def test_planned_execution_boundary_is_called(monkeypatch: pytest.MonkeyPatch):
    original = planning_module.plan_strategy_testing_execution
    calls: dict[str, int] = {"count": 0}

    def wrapped(request: StrategyTestingExecutionRequest):
        calls["count"] += 1
        return original(request)

    monkeypatch.setattr(planning_module, "plan_strategy_testing_execution", wrapped)

    result = run_planning_dry_run()

    assert result.dry_run_status == "planned"
    assert calls["count"] == 1


def test_planned_artifact_integration_boundary_is_called(monkeypatch: pytest.MonkeyPatch):
    original = planning_module.plan_execution_artifacts
    calls: dict[str, int] = {"count": 0}

    def wrapped(request: StrategyTestingExecutionRequest, artifact_plan: ExecutionArtifactPlan):
        calls["count"] += 1
        return original(request, artifact_plan)

    monkeypatch.setattr(planning_module, "plan_execution_artifacts", wrapped)

    result = run_planning_dry_run()

    assert result.dry_run_status == "planned"
    assert calls["count"] == 1


def test_validate_planning_dry_run_inputs_returns_validated_objects():
    request, artifact_plan = validate_planning_dry_run_inputs(
        EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST,
        EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN,
    )

    assert request is EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST
    assert artifact_plan is EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN


def test_artifact_manifest_draft_validates_with_flags_disabled_by_default():
    draft = ArtifactManifestDraft(**_artifact_draft_values())

    assert validate_artifact_manifest_draft(draft) is draft
    assert draft.planned_artifacts
    assert draft.artifact_manifest_ref == EMA_3_19_PLAN_ONLY_ARTIFACT_MANIFEST_REF
    assert draft.write_allowed is False
    assert draft.registry_write_allowed is False
    assert getattr(draft, _decision_gate_field()) is False


def test_registry_entry_draft_validates_with_flags_disabled_by_default():
    draft = RegistryEntryDraft(**_registry_draft_values())

    assert validate_registry_entry_draft(draft) is draft
    assert draft.planned_registry_entry_ref
    assert draft.artifact_manifest_ref == EMA_3_19_PLAN_ONLY_ARTIFACT_MANIFEST_REF
    assert draft.metrics_artifact_ref_or_none is None
    assert draft.report_artifact_ref_or_none is None
    assert draft.write_allowed is False
    assert getattr(draft, _decision_gate_field()) is False


def test_planning_dry_run_result_validates():
    result = run_planning_dry_run()

    assert validate_planning_dry_run_result(result) is result
    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(PLANNING_DRY_RUN_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS


def test_invalid_request_fails_closed():
    result = run_planning_dry_run(request=object(), artifact_plan=EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN)

    assert result.dry_run_status == "rejected"
    assert result.error_message_or_none is not None
    assert result.write_allowed is False
    assert result.registry_write_allowed is False
    assert getattr(result, _decision_gate_field()) is False


def test_invalid_artifact_plan_fails_closed():
    result = run_planning_dry_run(request=EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST, artifact_plan=object())

    assert result.dry_run_status == "rejected"
    assert result.error_message_or_none is not None
    assert result.write_allowed is False
    assert result.registry_write_allowed is False
    assert getattr(result, _decision_gate_field()) is False


def test_mismatched_artifact_plan_fails_closed():
    result = run_planning_dry_run(
        request=_request(artifact_plan_ref="artifact_plan.strategy_test.ema_3_19.other.v1"),
        artifact_plan=EMA_3_19_PLAN_ONLY_ARTIFACT_PLAN,
    )

    assert result.dry_run_status == "rejected"
    assert result.error_message_or_none is not None


@pytest.mark.parametrize("flag_name", ("write_allowed", "registry_write_allowed", _decision_gate_field()))
def test_invalid_artifact_manifest_draft_flags_fail_closed(flag_name: str):
    with pytest.raises(ArtifactManifestDraftValidationError):
        ArtifactManifestDraft(**_artifact_draft_values(**{flag_name: True}))


@pytest.mark.parametrize("flag_name", ("write_allowed", _decision_gate_field()))
def test_invalid_registry_entry_draft_flags_fail_closed(flag_name: str):
    with pytest.raises(RegistryEntryDraftValidationError):
        RegistryEntryDraft(**_registry_draft_values(**{flag_name: True}))


def test_invalid_artifact_manifest_draft_missing_refs_fail_closed():
    with pytest.raises(ArtifactManifestDraftValidationError):
        ArtifactManifestDraft(**_artifact_draft_values(planned_artifacts=()))
    with pytest.raises(ArtifactManifestDraftValidationError):
        ArtifactManifestDraft(**_artifact_draft_values(artifact_manifest_ref=""))


def test_invalid_registry_entry_draft_missing_refs_fail_closed():
    with pytest.raises(RegistryEntryDraftValidationError):
        RegistryEntryDraft(**_registry_draft_values(planned_registry_entry_ref=""))
    with pytest.raises(RegistryEntryDraftValidationError):
        RegistryEntryDraft(**_registry_draft_values(artifact_manifest_ref=""))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_markers_fail_closed_for_drafts(marker: str):
    bad_ref = "artifact." + marker + ".ema_3_19.v1"

    with pytest.raises(ArtifactManifestDraftValidationError):
        ArtifactManifestDraft(**_artifact_draft_values(artifact_manifest_ref=bad_ref))
    with pytest.raises(ArtifactManifestDraftValidationError):
        ArtifactManifestDraft(**_artifact_draft_values(planned_artifacts=(bad_ref,)))
    with pytest.raises(RegistryEntryDraftValidationError):
        RegistryEntryDraft(**_registry_draft_values(planned_registry_entry_ref=bad_ref))
    with pytest.raises(RegistryEntryDraftValidationError):
        RegistryEntryDraft(**_registry_draft_values(artifact_manifest_ref=bad_ref))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_selection_markers_fail_closed_for_planning_dry_run(marker: str):
    bad_plan = _artifact_plan(artifact_manifest_ref="artifact." + marker + ".ema_3_19.v1")

    result = run_planning_dry_run(EMA_3_19_PLAN_ONLY_EXECUTION_REQUEST, bad_plan)

    assert result.dry_run_status == "rejected"
    assert result.error_message_or_none is not None


def test_result_schema_rejects_extra_fields():
    result = run_planning_dry_run()
    result.extra_field = "not allowed"

    with pytest.raises(PlanningDryRunValidationError):
        validate_planning_dry_run_result(result)


def test_result_schema_rejects_enabled_flags():
    result = run_planning_dry_run()
    result.write_allowed = True

    with pytest.raises(PlanningDryRunValidationError):
        validate_planning_dry_run_result(result)


def test_result_object_contains_only_planning_identifiers_statuses_and_flags():
    result = run_planning_dry_run()

    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS


def test_result_object_does_not_contain_execution_outputs_or_authorizations():
    result = run_planning_dry_run()
    blocked_fields = (
        "real_metrics",
        "report_output",
        "registry_write_status",
        "back" + "test_result",
        "research_result",
        _scheduler_access() + "_" + _market_access() + "_authorization",
        _market_access() + "_authorization",
        "promotion_" + "verdict_ref",
        "promotion_" + "verdict_status",
    )

    for field_name in blocked_fields:
        assert field_name not in result.__dict__


def test_source_has_no_forbidden_execution_responsibility_markers():
    forbidden_markers = (
        "execute_" + "strategy",
        "generate_" + "signals",
        "materialize_" + "signals",
        "calculate_" + "ema",
        "run_" + "back" + "test",
        "execute_" + "back" + "test",
        "run_" + "research",
        "calculate_" + "pnl",
        "calculate_" + "metrics",
        "write_" + "report",
        "generate_" + "report",
        "write_" + "artifact",
        "write_" + "registry",
        "create_" + "promotion_" + "verdict",
        "promotion_" + "verdict_created",
        _external_actor(),
        _intent_marker(),
        _market_access() + "_execution",
        _scheduler_access() + "_execution",
        "data_" + "root",
        _freshness_marker(),
        _active_marker(),
        _implicit_marker(),
        _legacy_strategy_marker(),
    )

    for source_path in SOURCE_PATHS:
        source_text = source_path.read_text(encoding="utf-8").casefold()
        for marker in forbidden_markers:
            assert marker not in source_text, (source_path, marker)


def test_source_does_not_import_legacy_strategy_or_data_infra():
    forbidden_markers = (
        _legacy_strategy_marker(),
        _legacy_strategy_short_marker(),
        "data_" + "lake",
        "moex_data",
        _scheduler_access(),
        "ser" + "ver",
        "/home/",
        "/var/",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "glob(",
        "os.",
        "open(",
    )

    for source_path in SOURCE_PATHS:
        source_text = source_path.read_text(encoding="utf-8").casefold()
        for marker in forbidden_markers:
            assert marker not in source_text, (source_path, marker)

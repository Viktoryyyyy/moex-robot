from pathlib import Path

import pytest

from moex_research.runners.execution_request import (
    ALLOWED_EXECUTION_INPUT_ARTIFACT_CLASSES,
    ALLOWED_EXECUTION_INPUT_REF_TYPES,
    ALLOWED_STRATEGY_TESTING_EXECUTION_MODES,
    ExecutionArtifactPlan,
    ExecutionArtifactPlanValidationError,
    ExecutionInputBinding,
    ExecutionInputBindingValidationError,
    StrategyTestingExecutionRequest,
    StrategyTestingExecutionRequestValidationError,
    validate_execution_artifact_plan,
    validate_execution_input_binding,
    validate_strategy_testing_execution_request,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "execution_request.py"


def _request_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "exec_request.strategy_test.minimal.v1",
        "strategy_id": "strategy_alpha",
        "strategy_version": "0.1.0",
        "strategy_test_id": "strategy_test.strategy_alpha.signal_only.v1",
        "test_type": "signal_only_research",
        "package_ref": "strategy_test_package.strategy_alpha.v1",
        "dataset_refs": ("dataset_ref.futures_derived_d1.v1",),
        "feature_refs": ("feature_ref.alpha_inputs.v1",),
        "label_refs": ("label_ref.primary_1d.v1",),
        "signal_refs": ("signal_ref.alpha_direction.v1",),
        "backtest_semantics_ref": "contract.strategy_testing.semantics.v1",
        "cost_slippage_ref": "contract.strategy_testing.cost_slippage.v1",
        "artifact_plan_ref": "artifact_plan.strategy_alpha.v1",
        "execution_mode": "plan_only",
    }
    values.update(overrides)
    return values


def _artifact_plan_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "artifact_plan_id": "artifact_plan.strategy_alpha.v1",
        "run_id_or_planned_run_id": "planned_run.strategy_alpha.v1",
        "required_output_artifacts": ("artifact_manifest.strategy_alpha.v1",),
        "artifact_manifest_ref": "artifact_manifest.strategy_alpha.v1",
        "metrics_artifact_ref_or_none": None,
        "report_artifact_ref_or_none": None,
    }
    values.update(overrides)
    return values


def _binding_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "binding_id": "binding.dataset.alpha.v1",
        "ref_id": "dataset_ref.futures_derived_d1.v1",
        "ref_type": "dataset",
        "artifact_class": "repo_relative",
        "artifact_ref": "contracts/datasets/futures_derived_d1.yaml",
        "schema_version": "futures_derived_d1.v1",
    }
    values.update(overrides)
    return values


def test_valid_strategy_testing_execution_request_passes():
    request = StrategyTestingExecutionRequest(**_request_values())

    assert validate_strategy_testing_execution_request(request) is request
    assert request.execution_mode == "plan_only"
    assert request.dataset_refs == ("dataset_ref.futures_derived_d1.v1",)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("request_id", ""),
        ("strategy_id", ""),
        ("strategy_version", ""),
        ("strategy_test_id", ""),
        ("test_type", "unsupported_test"),
        ("package_ref", ""),
        ("dataset_refs", ()),
        ("feature_refs", ()),
        ("label_refs", ()),
        ("signal_refs", ()),
        ("backtest_semantics_ref", ""),
        ("cost_slippage_ref", ""),
        ("artifact_plan_ref", ""),
        ("execution_mode", "unsupported_mode"),
    ],
)
def test_invalid_strategy_testing_execution_request_fails_closed(field_name, replacement):
    with pytest.raises(StrategyTestingExecutionRequestValidationError):
        StrategyTestingExecutionRequest(**_request_values(**{field_name: replacement}))


def test_request_requires_exact_field_set():
    values = _request_values(extra_field="not_allowed")

    with pytest.raises(StrategyTestingExecutionRequestValidationError):
        StrategyTestingExecutionRequest(**values)


def test_all_allowed_execution_mode_values_pass():
    assert ALLOWED_STRATEGY_TESTING_EXECUTION_MODES == frozenset(
        {
            "dry_validation_only",
            "plan_only",
            "non_live_research_planned",
            "non_live_backtest_planned",
        }
    )
    for mode in ALLOWED_STRATEGY_TESTING_EXECUTION_MODES:
        request = StrategyTestingExecutionRequest(**_request_values(execution_mode=mode))

        assert request.execution_mode == mode


@pytest.mark.parametrize(
    "mode",
    [
        "live",
        "runtime",
        "broker",
        "order",
        "production",
        "live_execution",
        "runtime_execution",
        "broker_order_execution",
        "production_execution",
    ],
)
def test_live_runtime_broker_order_production_execution_modes_fail(mode):
    with pytest.raises(StrategyTestingExecutionRequestValidationError):
        StrategyTestingExecutionRequest(**_request_values(execution_mode=mode))


def test_valid_execution_artifact_plan_passes_and_permission_flags_default_false():
    plan = ExecutionArtifactPlan(**_artifact_plan_values())

    assert validate_execution_artifact_plan(plan) is plan
    assert plan.registry_write_allowed is False
    assert plan.promotion_verdict_allowed is False
    assert plan.required_output_artifacts == ("artifact_manifest.strategy_alpha.v1",)


def test_registry_write_allowed_true_fails_closed():
    with pytest.raises(ExecutionArtifactPlanValidationError):
        ExecutionArtifactPlan(**_artifact_plan_values(registry_write_allowed=True))


def test_promotion_verdict_allowed_true_fails_closed():
    with pytest.raises(ExecutionArtifactPlanValidationError):
        ExecutionArtifactPlan(**_artifact_plan_values(promotion_verdict_allowed=True))


def test_empty_required_output_artifacts_fail_closed():
    with pytest.raises(ExecutionArtifactPlanValidationError):
        ExecutionArtifactPlan(**_artifact_plan_values(required_output_artifacts=()))


def test_artifact_manifest_ref_must_be_explicit():
    with pytest.raises(ExecutionArtifactPlanValidationError):
        ExecutionArtifactPlan(**_artifact_plan_values(artifact_manifest_ref=""))


def test_valid_execution_input_binding_passes():
    binding = ExecutionInputBinding(**_binding_values())

    assert validate_execution_input_binding(binding) is binding
    assert binding.ref_type == "dataset"
    assert binding.artifact_class == "repo_relative"


def test_all_allowed_input_binding_ref_types_pass():
    assert ALLOWED_EXECUTION_INPUT_REF_TYPES == frozenset(
        {
            "dataset",
            "feature",
            "label",
            "signal",
            "backtest_semantics",
            "cost_slippage",
            "artifact_contract",
        }
    )
    for ref_type in ALLOWED_EXECUTION_INPUT_REF_TYPES:
        binding = ExecutionInputBinding(**_binding_values(ref_type=ref_type))

        assert binding.ref_type == ref_type


def test_all_allowed_input_binding_artifact_classes_pass():
    assert ALLOWED_EXECUTION_INPUT_ARTIFACT_CLASSES == frozenset(
        {"repo_relative", "external_pattern", "cli_argument", "env_contract"}
    )
    for artifact_class in ALLOWED_EXECUTION_INPUT_ARTIFACT_CLASSES:
        binding = ExecutionInputBinding(**_binding_values(artifact_class=artifact_class))

        assert binding.artifact_class == artifact_class


def test_unsupported_ref_type_fails_closed():
    with pytest.raises(ExecutionInputBindingValidationError):
        ExecutionInputBinding(**_binding_values(ref_type="unknown_ref"))


def test_unsupported_artifact_class_fails_closed():
    with pytest.raises(ExecutionInputBindingValidationError):
        ExecutionInputBinding(**_binding_values(artifact_class="implicit_file"))


@pytest.mark.parametrize(
    "artifact_ref",
    [
        "contracts/datasets/latest.yaml",
        "contracts/datasets/current.yaml",
        "contracts/datasets/autodetect.yaml",
    ],
)
def test_latest_current_autodetect_artifact_refs_fail_closed(artifact_ref):
    with pytest.raises(ExecutionInputBindingValidationError):
        ExecutionInputBinding(**_binding_values(artifact_ref=artifact_ref))


def test_source_has_no_forbidden_execution_responsibility_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()
    forbidden_markers = (
        "run_backtest",
        "execute_backtest",
        "execute_strategy",
        "generate_signals",
        "calculate_pnl",
        "calculate_metrics",
        "write_report",
        "write_registry",
        "create_promotion_verdict",
        "broker",
        "order",
        "live_execution",
        "runtime_execution",
        "data_root",
        "server",
        "d1_tsmom",
    )

    for marker in forbidden_markers:
        assert marker not in source


def test_source_keeps_discovery_markers_guard_only():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    assert '"late" + "st"' in source
    assert '"cur" + "rent"' in source
    assert '"auto" + "detect"' in source
    assert "latest" not in source
    assert "current" not in source
    assert "autodetect" not in source


def test_no_server_data_lake_runtime_terms_are_introduced():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()
    forbidden_terms = (
        "data_lake",
        "moex_data",
        "runtime_live",
        "server_path",
        "/home/",
        "/var/",
        "subprocess",
        "requests",
        "urllib",
        "socket",
        "glob(",
        "pathlib",
        "os.",
        "open(",
    )

    for term in forbidden_terms:
        assert term not in source


def test_no_d1_tsmom_imports_or_references():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    assert "d1_tsmom" not in source
    assert "tsmom" not in source

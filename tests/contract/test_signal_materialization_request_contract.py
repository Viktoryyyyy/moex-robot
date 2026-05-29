from pathlib import Path

import pytest

from moex_research.runners.signal_materialization_request import (
    ALLOWED_SIGNAL_MATERIALIZATION_MODES,
    SignalMaterializationPlan,
    SignalMaterializationPlanValidationError,
    SignalMaterializationRequest,
    SignalMaterializationRequestValidationError,
    validate_signal_materialization_plan,
    validate_signal_materialization_request,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "signal_materialization_request.py"


def _request_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "request_id": "signal_materialization_request.strategy_alpha.v1",
        "strategy_id": "strategy_alpha",
        "strategy_version": "0.1.0",
        "strategy_test_id": "strategy_test.strategy_alpha.signal_only.v1",
        "package_ref": "strategy_test_package.strategy_alpha.v1",
        "input_bindings": ("input_binding.strategy_alpha.dataset.v1",),
        "feature_refs": ("feature_ref.strategy_alpha.inputs.v1",),
        "signal_refs": ("signal_ref.strategy_alpha.direction.v1",),
        "output_signal_artifact_ref": "artifact.signal_table.strategy_alpha.v1",
        "materialization_mode": "plan_only",
    }
    values.update(overrides)
    return values


def _plan_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "plan_id": "signal_materialization_plan.strategy_alpha.v1",
        "request_id": "signal_materialization_request.strategy_alpha.v1",
        "strategy_id": "strategy_alpha",
        "strategy_test_id": "strategy_test.strategy_alpha.signal_only.v1",
        "signal_table_artifact_ref": "artifact.signal_table.strategy_alpha.v1",
        "artifact_manifest_ref": "artifact_manifest.strategy_alpha.signal_materialization.v1",
    }
    values.update(overrides)
    return values


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _market_access() -> str:
    return "li" + "ve"


def _scheduler_access() -> str:
    return "run" + "time"


def _external_actor() -> str:
    return "bro" + "ker"


def _intent_marker() -> str:
    return "or" + "der"


def _production_marker() -> str:
    return "prod" + "uction"


def _legacy_strategy_marker() -> str:
    return "d1_" + "tsmom"


def _legacy_strategy_short_marker() -> str:
    return "ts" + "mom"


def test_valid_signal_materialization_request_passes():
    request = SignalMaterializationRequest(**_request_values())

    assert validate_signal_materialization_request(request) is request
    assert request.request_id == "signal_materialization_request.strategy_alpha.v1"
    assert request.materialization_mode == "plan_only"


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("request_id", ""),
        ("strategy_id", ""),
        ("strategy_version", ""),
        ("strategy_test_id", ""),
        ("package_ref", ""),
        ("input_bindings", ()),
        ("feature_refs", ()),
        ("signal_refs", ()),
        ("output_signal_artifact_ref", ""),
        ("materialization_mode", "unsupported_mode"),
    ],
)
def test_invalid_signal_materialization_request_fails_closed(field_name, replacement):
    with pytest.raises(SignalMaterializationRequestValidationError):
        SignalMaterializationRequest(**_request_values(**{field_name: replacement}))


def test_signal_materialization_request_requires_exact_field_set():
    values = _request_values(extra_field="not_allowed")

    with pytest.raises(SignalMaterializationRequestValidationError):
        SignalMaterializationRequest(**values)


def test_all_allowed_materialization_mode_values_pass():
    assert ALLOWED_SIGNAL_MATERIALIZATION_MODES == frozenset(
        {
            "plan_only",
            "non_live_signal_materialization_planned",
        }
    )
    for mode in ALLOWED_SIGNAL_MATERIALIZATION_MODES:
        request = SignalMaterializationRequest(**_request_values(materialization_mode=mode))

        assert request.materialization_mode == mode


@pytest.mark.parametrize(
    "mode",
    [
        _market_access(),
        _scheduler_access(),
        _external_actor(),
        _intent_marker(),
        _production_marker(),
        _market_access() + "_execution",
        _scheduler_access() + "_execution",
        _external_actor() + "_" + _intent_marker() + "_execution",
        _production_marker() + "_execution",
    ],
)
def test_live_runtime_broker_order_production_modes_fail_closed(mode):
    with pytest.raises(SignalMaterializationRequestValidationError):
        SignalMaterializationRequest(**_request_values(materialization_mode=mode))


def test_valid_signal_materialization_plan_passes_and_permission_flags_default_false():
    plan = SignalMaterializationPlan(**_plan_values())

    assert validate_signal_materialization_plan(plan) is plan
    assert plan.write_allowed is False
    assert plan.registry_write_allowed is False
    assert plan.promotion_verdict_allowed is False
    assert plan.signal_table_artifact_ref == "artifact.signal_table.strategy_alpha.v1"


def test_signal_materialization_plan_requires_exact_field_set():
    values = _plan_values(extra_field="not_allowed")

    with pytest.raises(SignalMaterializationPlanValidationError):
        SignalMaterializationPlan(**values)


def test_write_allowed_true_fails_closed():
    with pytest.raises(SignalMaterializationPlanValidationError):
        SignalMaterializationPlan(**_plan_values(write_allowed=True))


def test_registry_write_allowed_true_fails_closed():
    with pytest.raises(SignalMaterializationPlanValidationError):
        SignalMaterializationPlan(**_plan_values(registry_write_allowed=True))


def test_promotion_verdict_allowed_true_fails_closed():
    with pytest.raises(SignalMaterializationPlanValidationError):
        SignalMaterializationPlan(**_plan_values(promotion_verdict_allowed=True))


def test_empty_signal_table_artifact_ref_fails_closed():
    with pytest.raises(SignalMaterializationPlanValidationError):
        SignalMaterializationPlan(**_plan_values(signal_table_artifact_ref=""))


def test_empty_artifact_manifest_ref_fails_closed():
    with pytest.raises(SignalMaterializationPlanValidationError):
        SignalMaterializationPlan(**_plan_values(artifact_manifest_ref=""))


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("package_ref", "contracts/packages/latest.yaml"),
        ("input_bindings", ("input_binding.current.v1",)),
        ("feature_refs", ("feature_ref.autodetect.v1",)),
        ("signal_refs", ("signal_ref.latest.v1",)),
        ("output_signal_artifact_ref", "artifact.current.signal_table.v1"),
    ],
)
def test_latest_current_autodetect_request_refs_fail_closed(field_name, replacement):
    with pytest.raises(SignalMaterializationRequestValidationError):
        SignalMaterializationRequest(**_request_values(**{field_name: replacement}))


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("signal_table_artifact_ref", "artifact.latest.signal_table.v1"),
        ("artifact_manifest_ref", "artifact_manifest.current.v1"),
        ("signal_table_artifact_ref", "artifact.autodetect.signal_table.v1"),
    ],
)
def test_latest_current_autodetect_plan_refs_fail_closed(field_name, replacement):
    with pytest.raises(SignalMaterializationPlanValidationError):
        SignalMaterializationPlan(**_plan_values(**{field_name: replacement}))


def test_source_has_no_forbidden_execution_responsibility_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()
    forbidden_markers = (
        "run_" + "back" + "test",
        "execute_" + "back" + "test",
        "execute_" + "strategy",
        "generate_" + "signals",
        "materialize_" + "signals",
        "calculate_" + "ema",
        "calculate_" + "pnl",
        "calculate_" + "metrics",
        "write_" + "report",
        "write_" + "registry",
        "write_" + "artifact",
        "create_" + "promotion_" + "verdict",
        _external_actor(),
        _intent_marker(),
        _market_access() + "_execution",
        _scheduler_access() + "_execution",
        "data_" + "root",
        "ser" + "ver",
        _freshness_marker(),
        _active_marker(),
        _implicit_marker(),
        _legacy_strategy_marker(),
    )

    for marker in forbidden_markers:
        assert marker not in source


def test_source_keeps_discovery_markers_guard_only():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    assert '"late" + "st"' in source
    assert '"cur" + "rent"' in source
    assert '"auto" + "detect"' in source
    assert _freshness_marker() not in source
    assert _active_marker() not in source
    assert _implicit_marker() not in source


def test_no_server_data_lake_runtime_terms_are_introduced():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()
    forbidden_terms = (
        "data_" + "lake",
        "moex_data",
        "run" + "time_" + "li" + "ve",
        "ser" + "ver_path",
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

    assert _legacy_strategy_marker() not in source
    assert _legacy_strategy_short_marker() not in source


def test_no_ema_calculation_over_data_is_introduced():
    source = SOURCE_PATH.read_text(encoding="utf-8").casefold()

    assert "ema" not in source
    assert "pandas" not in source
    assert "numpy" not in source
    assert "dataframe" not in source

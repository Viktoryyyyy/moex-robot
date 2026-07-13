from __future__ import annotations

import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    REPO_ROOT
    / "contracts/validation/usdrubf_phase7_modeling_readiness_target_policy_v1.yaml"
)
POLICY_PATH = (
    REPO_ROOT
    / "docs/sot/strategies/ema_3_19_ai/phase7_modeling_readiness_target_policy_v1.md"
)

REQUIRED_SECTIONS = (
    "contract_identity",
    "target_policy",
    "supervised_row_eligibility",
    "unlabeled_handling",
    "chronological_walk_forward_validation",
    "baseline_metrics",
    "leakage_checks",
    "future_evaluation_artifacts",
    "forbidden_capabilities",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _assert_repo_yaml_subset_parses(path: Path) -> str:
    text = _read(path)
    assert text.strip(), f"{path} is empty"
    stack: list[int] = [-1]
    block_scalar_parent_indent: int | None = None

    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        assert "\t" not in line, f"{path}:{line_no} contains a tab"
        indent = len(line) - len(line.lstrip(" "))
        assert indent % 2 == 0, f"{path}:{line_no} has non-2-space indentation"

        if block_scalar_parent_indent is not None:
            if indent > block_scalar_parent_indent:
                continue
            block_scalar_parent_indent = None

        stripped = line.strip()
        while stack and indent <= stack[-1]:
            stack.pop()
        assert stack, f"{path}:{line_no} invalid indentation stack"

        if stripped.startswith("- "):
            item = stripped[2:].strip()
            assert item, f"{path}:{line_no} has an empty list item"
            continue

        assert ":" in stripped, f"{path}:{line_no} is not a key/value YAML line"
        key, value = stripped.split(":", 1)
        assert re.match(r"^[A-Za-z0-9_./${}-]+$", key), (
            f"{path}:{line_no} invalid key"
        )
        value = value.strip()
        if not value:
            stack.append(indent)
        elif value in {">", "|"}:
            stack.append(indent)
            block_scalar_parent_indent = indent
        elif value[0] in {'"', "'"}:
            assert value[-1] == value[0], (
                f"{path}:{line_no} has an unclosed quoted scalar"
            )

    return text


def _load_contract() -> tuple[dict[str, Any] | None, str]:
    text = _assert_repo_yaml_subset_parses(CONTRACT_PATH)
    try:
        import yaml  # type: ignore[import-not-found]
    except ImportError:
        return None, text

    payload = yaml.safe_load(text)
    assert isinstance(payload, dict), "Phase 7 contract must parse as a YAML mapping"
    return payload, text


def _assert_contains_all(text: str, markers: tuple[str, ...]) -> None:
    lowered = text.lower()
    for marker in markers:
        assert marker.lower() in lowered, marker


def test_phase7_contract_exists_parses_and_has_required_sections() -> None:
    assert CONTRACT_PATH.is_file()
    payload, text = _load_contract()

    for section in REQUIRED_SECTIONS:
        assert re.search(rf"^{re.escape(section)}:\s*$", text, flags=re.MULTILINE)
        if payload is not None:
            assert section in payload


def test_manual_label_only_target_and_supervised_classes_are_explicit() -> None:
    payload, text = _load_contract()
    _assert_contains_all(
        text,
        (
            "source_policy: manual_label_only",
            "allowed_target_source: manual_phase_labels_v1",
            "supervised_classes:",
            "- B",
            "- S",
            "- OUT",
            "out_is_valid_supervised_class: true",
            "unlabeled_is_supervised_class: false",
        ),
    )

    if payload is not None:
        target_policy = payload["target_policy"]
        assert target_policy["allowed_target_source"] == "manual_phase_labels_v1"
        assert target_policy["supervised_classes"] == ["B", "S", "OUT"]


def test_unlabeled_is_excluded_from_fitting_and_supervised_metrics() -> None:
    _, text = _load_contract()
    _assert_contains_all(
        text,
        (
            "sentinel: UNLABELED",
            "convert_to_out_allowed: false",
            "eligible_for_supervised_fitting: false",
            "included_in_supervised_metrics: false",
            "included_in_class_support: false",
            "included_in_confusion_matrix: false",
            "included_in_probability_metrics: false",
        ),
    )


def test_chronological_walk_forward_and_train_before_validation_are_explicit() -> None:
    payload, text = _load_contract()
    _assert_contains_all(
        text,
        (
            "method: chronological_walk_forward",
            "random_split_allowed: false",
            "shuffle_allowed: false",
            "training_must_precede_validation: true",
            "max_train_target_trade_date_strictly_less_than_min_validation_target_trade_date",
            "feature_selection",
            "imputation",
            "scaling",
            "calibration",
        ),
    )

    if payload is not None:
        validation = payload["chronological_walk_forward_validation"]
        assert validation["training_must_precede_validation"] is True
        assert validation["shuffle_allowed"] is False


def test_baseline_metrics_and_future_evaluation_artifacts_are_declared() -> None:
    _, text = _load_contract()
    _assert_contains_all(
        text,
        (
            "majority_class_train_only",
            "class_prior_train_only",
            "balanced_accuracy",
            "macro_f1",
            "per_class_precision",
            "multiclass_log_loss",
            "evaluation_manifest.json",
            "fold_boundaries.csv",
            "fold_metrics.csv",
            "aggregate_metrics.json",
            "per_class_metrics.csv",
            "confusion_matrix.csv",
            "baseline_metrics.json",
            "validation_predictions.parquet",
            "eligible_labeled_validation_rows_only: true",
        ),
    )


def test_leakage_checks_forbid_future_and_target_derived_features() -> None:
    _, text = _load_contract()
    _assert_contains_all(
        text,
        (
            "manual_label_only_target_source",
            "target_values_not_features",
            "no_manual_label_metadata_features",
            "no_future_derived_features",
            "point_in_time_availability",
            "train_only_preprocessing",
            "chronological_ordering",
            "no_validation_feedback",
            "future_returns",
            "future_volatility",
            "future_drawdown",
            "future_phase",
            "transition_outcomes",
            "boundary_distance",
        ),
    )


def test_forbidden_capabilities_exclude_runtime_fitting_prediction_and_trading() -> None:
    payload, text = _load_contract()
    _assert_contains_all(
        text,
        (
            "runtime_execution: true",
            "model_training: true",
            "model_fitting: true",
            "prediction_generation: true",
            "trading_signal_generation: true",
            "broker_or_order_actions: true",
            "external_data_ingestion: true",
            "dataset_materialization: true",
            "server_apply: true",
            "direct_main_write: true",
            "merge: true",
        ),
    )

    if payload is not None:
        forbidden = payload["forbidden_capabilities"]
        for capability in (
            "runtime_execution",
            "model_training",
            "model_fitting",
            "prediction_generation",
            "trading_signal_generation",
            "broker_or_order_actions",
        ):
            assert forbidden[capability] is True


def test_policy_document_matches_contract_and_keeps_phase7_repository_only() -> None:
    assert POLICY_PATH.is_file()
    policy = _read(POLICY_PATH)
    _assert_contains_all(
        policy,
        (
            "manual-label-only target policy",
            "B / S / OUT",
            "`UNLABELED` is not a fourth supervised class",
            "chronological walk-forward validation",
            "baseline metric contract",
            "leakage-prevention checklist",
            "future evaluation artifact contract",
            "does not fit a model",
            "do not authorize creation of the artifacts in Phase 7.0",
            "Merge authority and server-apply authority remain `PM_L2_ONLY`",
        ),
    )

from pathlib import Path

import pytest

from moex_research.runners.canonical_data_read import CanonicalDataReadRequest
from moex_research.runners.canonical_data_read_plan import (
    ALLOWED_ACCESS_MODES,
    ALLOWED_READ_PLAN_MODES,
    CANONICAL_DATA_ACCESS_POLICY_FIELDS,
    CANONICAL_DATA_READ_PLAN_FIELDS,
    CANONICAL_DATA_READ_PLAN_RESULT_FIELDS,
    CanonicalDataAccessPolicy,
    CanonicalDataReadPlan,
    CanonicalDataReadPlanResult,
    CanonicalDataReadPlanValidationError,
    dry_run_plan_canonical_data_read,
    plan_canonical_data_read,
    validate_canonical_data_access_policy,
    validate_canonical_data_read_plan,
    validate_canonical_data_read_plan_result,
)
from tests.fixtures.strategy_testing.approved_canonical_data_read_plan import (
    APPROVED_CANONICAL_DATA_DRY_RUN_POLICY,
    EMA_3_19_SI_D1_CANONICAL_READ_PLAN,
)
from tests.fixtures.strategy_testing.approved_canonical_data_refs import (
    APPROVED_SI_D1_CANONICAL_DATASET_REF,
    EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = REPO_ROOT / "src" / "moex_research" / "runners" / "canonical_data_read_plan.py"
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "strategy_testing" / "approved_canonical_data_read_plan.py"
EXPECTED_POLICY_FIELDS = frozenset(
    {
        "policy_id",
        "allowed_dataset_classes",
        "allowed_instruments",
        "allowed_timeframes",
        "allow_file_read",
        "allow_network",
        "allow_discovery",
        "access_mode",
    }
)
EXPECTED_PLAN_FIELDS = frozenset(
    {
        "plan_id",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "dataset_ref_id",
        "schema_ref",
        "access_policy_id",
        "planned_reader_id",
        "read_plan_mode",
    }
)
EXPECTED_RESULT_FIELDS = frozenset(
    {
        "plan_status",
        "plan_id",
        "request_id",
        "strategy_id",
        "strategy_test_id",
        "dataset_ref_id",
        "schema_ref",
        "access_policy_id",
        "planned_reader_id",
        "error_message_or_none",
    }
)


def _freshness_marker() -> str:
    return "late" + "st"


def _active_marker() -> str:
    return "cur" + "rent"


def _implicit_marker() -> str:
    return "auto" + "detect"


def _legacy_strategy_marker() -> str:
    return "d1_" + "ts" + "mom"


def _host_marker() -> str:
    return "ser" + "ver"


def _scheduler_marker() -> str:
    return "run" + "time"


def _market_access_marker() -> str:
    return "li" + "ve"


def _network_marker() -> str:
    return "net" + "work"


def _policy_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "policy_id": "canonical.data.access_policy.dry_run.test",
        "allowed_dataset_classes": ("canonical_bars",),
        "allowed_instruments": ("Si", "USDRUBF"),
        "allowed_timeframes": ("D1", "5m"),
        "allow_file_read": False,
        "allow_network": False,
        "allow_discovery": False,
        "access_mode": "dry_run_plan_only",
    }
    values.update(overrides)
    return values


def test_valid_canonical_access_policy_and_fixture_pass():
    policy = CanonicalDataAccessPolicy(**_policy_values())

    assert validate_canonical_data_access_policy(policy) is policy
    assert validate_canonical_data_access_policy(APPROVED_CANONICAL_DATA_DRY_RUN_POLICY) is APPROVED_CANONICAL_DATA_DRY_RUN_POLICY
    assert policy.allow_file_read is False
    assert policy.allow_network is False
    assert policy.allow_discovery is False
    assert policy.access_mode == "dry_run_plan_only"
    assert frozenset(policy.__dict__) == EXPECTED_POLICY_FIELDS
    assert frozenset(CANONICAL_DATA_ACCESS_POLICY_FIELDS) == EXPECTED_POLICY_FIELDS
    assert ALLOWED_ACCESS_MODES == frozenset({"dry_run_plan_only"})


@pytest.mark.parametrize("field_name", ("allow_file_read", "allow_network", "allow_discovery"))
def test_access_policy_requires_disabled_side_effects(field_name: str):
    with pytest.raises(CanonicalDataReadPlanValidationError):
        CanonicalDataAccessPolicy(**_policy_values(**{field_name: True}))


@pytest.mark.parametrize(
    "override",
    (
        {"allowed_dataset_classes": ("raw_bars",)},
        {"allowed_instruments": ("GAZP",)},
        {"allowed_timeframes": ("1m",)},
        {"access_mode": "production_read"},
    ),
)
def test_access_policy_unsupported_values_fail_closed(override: dict[str, object]):
    with pytest.raises(CanonicalDataReadPlanValidationError):
        CanonicalDataAccessPolicy(**_policy_values(**override))


@pytest.mark.parametrize("marker", (_freshness_marker(), _active_marker(), _implicit_marker()))
def test_access_policy_selection_markers_fail_closed(marker: str):
    with pytest.raises(CanonicalDataReadPlanValidationError):
        CanonicalDataAccessPolicy(**_policy_values(policy_id="policy." + marker + ".fixture"))


@pytest.mark.parametrize(
    "marker",
    (
        _host_marker(),
        _scheduler_marker(),
        _market_access_marker(),
        "data" + "lake",
        _network_marker(),
    ),
)
def test_access_policy_platform_markers_fail_closed(marker: str):
    with pytest.raises(CanonicalDataReadPlanValidationError):
        CanonicalDataAccessPolicy(**_policy_values(policy_id="policy." + marker + ".fixture"))


def test_plan_canonical_data_read_builds_plan_from_approved_request():
    plan = plan_canonical_data_read(EMA_3_19_SI_D1_CANONICAL_READ_REQUEST, APPROVED_CANONICAL_DATA_DRY_RUN_POLICY)

    assert validate_canonical_data_read_plan(plan) is plan
    assert validate_canonical_data_read_plan(EMA_3_19_SI_D1_CANONICAL_READ_PLAN) is EMA_3_19_SI_D1_CANONICAL_READ_PLAN
    assert plan.plan_id == EMA_3_19_SI_D1_CANONICAL_READ_REQUEST.request_id + ".plan"
    assert plan.dataset_ref_id == APPROVED_SI_D1_CANONICAL_DATASET_REF.dataset_ref_id
    assert plan.schema_ref == APPROVED_SI_D1_CANONICAL_DATASET_REF.schema_ref
    assert plan.access_policy_id == APPROVED_CANONICAL_DATA_DRY_RUN_POLICY.policy_id
    assert plan.planned_reader_id == "canonical_data_reader.dry_run_reference.v1"
    assert plan.read_plan_mode == "canonical_read_planned_only"
    assert frozenset(plan.__dict__) == EXPECTED_PLAN_FIELDS
    assert frozenset(CANONICAL_DATA_READ_PLAN_FIELDS) == EXPECTED_PLAN_FIELDS
    assert ALLOWED_READ_PLAN_MODES == frozenset({"canonical_read_planned_only"})


def test_plan_canonical_data_read_rejects_policy_mismatch():
    policy = CanonicalDataAccessPolicy(
        **_policy_values(allowed_instruments=("USDRUBF",)),
    )
    with pytest.raises(CanonicalDataReadPlanValidationError):
        plan_canonical_data_read(EMA_3_19_SI_D1_CANONICAL_READ_REQUEST, policy)


def test_dry_run_plan_canonical_data_read_returns_metadata_only_result():
    result = dry_run_plan_canonical_data_read(
        EMA_3_19_SI_D1_CANONICAL_READ_REQUEST,
        APPROVED_CANONICAL_DATA_DRY_RUN_POLICY,
    )

    assert result.plan_status == "planned"
    assert result.plan_id == EMA_3_19_SI_D1_CANONICAL_READ_REQUEST.request_id + ".plan"
    assert result.request_id == EMA_3_19_SI_D1_CANONICAL_READ_REQUEST.request_id
    assert result.strategy_id == "ema_3_19"
    assert result.strategy_test_id == "ema_3_19.strategy_test.canonical_read.v1"
    assert result.dataset_ref_id == "canonical.dataset.si.d1.v1"
    assert result.schema_ref == "canonical.schema.ohlcv.d1.v1"
    assert result.access_policy_id == APPROVED_CANONICAL_DATA_DRY_RUN_POLICY.policy_id
    assert result.planned_reader_id == "canonical_data_reader.dry_run_reference.v1"
    assert result.error_message_or_none is None
    assert validate_canonical_data_read_plan_result(result) is result
    assert frozenset(result.__dict__) == EXPECTED_RESULT_FIELDS
    assert frozenset(CANONICAL_DATA_READ_PLAN_RESULT_FIELDS) == EXPECTED_RESULT_FIELDS


def test_dry_run_plan_rejects_invalid_request_without_reading_data():
    result = dry_run_plan_canonical_data_read("not-request", APPROVED_CANONICAL_DATA_DRY_RUN_POLICY)

    assert result.plan_status == "rejected"
    assert result.error_message_or_none
    assert result.dataset_ref_id == "unavailable"


def test_plan_result_object_has_no_execution_or_data_fields():
    result = CanonicalDataReadPlanResult(
        plan_status="planned",
        plan_id="plan.fixture",
        request_id="request.fixture",
        strategy_id="ema_3_19",
        strategy_test_id="strategy_test.fixture",
        dataset_ref_id="canonical.dataset.si.d1.fixture",
        schema_ref="canonical.schema.fixture",
        access_policy_id="policy.fixture",
        planned_reader_id="reader.fixture",
        error_message_or_none=None,
    )
    forbidden = {
        "rows",
        "dataframe",
        "file_path",
        "signals",
        "metrics",
        "backtest_result",
        "registry_entry",
        "promotion_verdict",
        "runtime_authorization",
    }
    assert forbidden.isdisjoint(result.__dict__)


def test_invalid_plan_result_shape_fails_closed():
    with pytest.raises(CanonicalDataReadPlanValidationError):
        CanonicalDataReadPlanResult(
            plan_status="planned",
            plan_id="plan.fixture",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            dataset_ref_id="canonical.dataset.si.d1.fixture",
            schema_ref="canonical.schema.fixture",
            access_policy_id="policy.fixture",
            planned_reader_id="reader.fixture",
            error_message_or_none="bad",
        )
    with pytest.raises(CanonicalDataReadPlanValidationError):
        CanonicalDataReadPlanResult(
            plan_status="rejected",
            plan_id="plan.fixture",
            request_id="request.fixture",
            strategy_id="ema_3_19",
            strategy_test_id="strategy_test.fixture",
            dataset_ref_id="canonical.dataset.si.d1.fixture",
            schema_ref="canonical.schema.fixture",
            access_policy_id="policy.fixture",
            planned_reader_id="reader.fixture",
            error_message_or_none=None,
        )


def test_source_has_no_forbidden_execution_or_data_access_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8") + "\n" + FIXTURE_PATH.read_text(encoding="utf-8")
    forbidden = (
        "open(",
        "read_text(",
        "load_market_data",
        "calculate_ema",
        "generate_signals",
        "run_backtest",
        "execute_backtest",
        "run_research",
        "calculate_metrics",
        "write_registry",
        "create_promotion_verdict",
        "broker",
        "order",
        "live_execution",
        "runtime_execution",
        "server",
        "latest",
        "current",
        "autodetect",
        _legacy_strategy_marker(),
    )
    for marker in forbidden:
        assert marker not in source

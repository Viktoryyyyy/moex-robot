from pathlib import Path

import pytest

from moex_research.runners.real_read_design import (
    ALLOWED_DESIGN_MODES,
    ALLOWED_DESIGN_STATES,
    ALLOWED_SOURCE_SCOPES,
    DECISION_FIELDS,
    DESIGN_FIELDS,
    SOURCE_CONTRACT_FIELDS,
    RealReadDesign,
    RealReadDesignDecision,
    RealReadDesignError,
    RealReadSourceContract,
    evaluate_real_read_design,
    validate_real_read_design,
    validate_real_read_design_decision,
    validate_real_read_source_contract,
)
from moex_research.runners.real_read_review import RealReadReviewDecision

SOURCE_PATH = Path(__file__).resolve().parents[2] / "src" / "moex_research" / "runners" / "real_read_design.py"


def _review(status: str = "eligible_for_real_read_design") -> RealReadReviewDecision:
    return RealReadReviewDecision(
        decision_status=status,
        package_id="ema_3_19.real_read_review.package.test",
        gate_request_id="ema_3_19.real_read_gate.test",
        dataset_ref_id="canonical.dataset.si.d1.v1",
        instrument_id="Si",
        timeframe="D1",
        gate_status="eligible_for_separate_review",
        real_read_allowed=False,
        reason_or_none=None if status == "eligible_for_real_read_design" else "review blocked",
    )


def _source(**overrides: object) -> RealReadSourceContract:
    values: dict[str, object] = {
        "source_contract_id": "ema_3_19.real_read.source_contract.test",
        "review_decision": _review(),
        "dataset_contract_ref": "contracts.datasets.canonical_si_d1.v1",
        "calendar_contract_ref": "contracts.calendars.moex_futures.v1",
        "schema_contract_ref": "contracts.schemas.ohlcv_d1.v1",
        "read_scope": "single_dataset_single_instrument_single_timeframe",
        "source_scope": "canonical_dataset_contract_ref_only",
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadSourceContract(**values)


def _design(**overrides: object) -> RealReadDesign:
    values: dict[str, object] = {
        "design_id": "ema_3_19.real_read.design.test",
        "source_contract": _source(),
        "design_mode": "controlled_real_read_design_only",
        "requested_design_state": "eligible_for_controlled_real_read_execution_review",
        "output_contract_ref": "contracts.outputs.real_read_snapshot_metadata.v1",
        "quality_contract_ref": "contracts.quality.real_read_preflight.v1",
        "lineage_contract_ref": "contracts.lineage.real_read_design.v1",
        "allow_real_read": False,
        "allow_network": False,
        "allow_registry_write": False,
        "allow_runtime": False,
        "metadata_only": True,
    }
    values.update(overrides)
    return RealReadDesign(**values)


def test_valid_source_contract_passes_metadata_only():
    source = _source()

    assert validate_real_read_source_contract(source) is source
    assert frozenset(source.__dict__) == frozenset(SOURCE_CONTRACT_FIELDS)
    assert ALLOWED_SOURCE_SCOPES == frozenset({"canonical_dataset_contract_ref_only"})


def test_source_requires_eligible_review_decision():
    with pytest.raises(RealReadDesignError):
        _source(review_decision=_review("blocked"))


def test_valid_design_passes_without_authorizing_read():
    design = _design()

    assert validate_real_read_design(design) is design
    assert frozenset(design.__dict__) == frozenset(DESIGN_FIELDS)
    assert ALLOWED_DESIGN_MODES == frozenset({"controlled_real_read_design_only"})
    assert ALLOWED_DESIGN_STATES == frozenset(
        {"blocked", "eligible_for_controlled_real_read_execution_review", "rejected"}
    )


@pytest.mark.parametrize("field_name", ("allow_real_read", "allow_network", "allow_registry_write", "allow_runtime"))
def test_side_effect_flags_must_be_false(field_name: str):
    with pytest.raises(RealReadDesignError):
        _design(**{field_name: True})


def test_metadata_only_is_required_on_source_and_design():
    with pytest.raises(RealReadDesignError):
        _source(metadata_only=False)
    with pytest.raises(RealReadDesignError):
        _design(metadata_only=False)


def test_design_can_only_enable_execution_review_not_read():
    decision = evaluate_real_read_design(_design())

    assert decision.decision_status == "eligible_for_controlled_real_read_execution_review"
    assert decision.real_read_allowed is False
    assert decision.reason_or_none is None
    assert validate_real_read_design_decision(decision) is decision
    assert frozenset(decision.__dict__) == frozenset(DECISION_FIELDS)


def test_blocked_and_rejected_design_states_do_not_authorize_read():
    blocked = evaluate_real_read_design(_design(requested_design_state="blocked"))
    rejected = evaluate_real_read_design(_design(requested_design_state="rejected"))

    assert blocked.decision_status == "blocked"
    assert blocked.real_read_allowed is False
    assert blocked.reason_or_none == "design requested blocked decision"
    assert rejected.decision_status == "rejected"
    assert rejected.real_read_allowed is False
    assert rejected.reason_or_none == "design rejected"


def test_decision_cannot_authorize_direct_read():
    with pytest.raises(RealReadDesignError):
        RealReadDesignDecision(
            decision_status="eligible_for_controlled_real_read_execution_review",
            design_id="design.fixture",
            review_package_id="review.fixture",
            dataset_ref_id="dataset.fixture",
            instrument_id="Si",
            timeframe="D1",
            real_read_allowed=True,
            reason_or_none=None,
        )


def test_source_has_no_forbidden_execution_markers():
    source = SOURCE_PATH.read_text(encoding="utf-8")
    forbidden = (
        "load_market_data",
        "open(",
        "read_csv",
        "read_parquet",
        "requests.",
        "http",
        "write_registry",
        "broker",
        "order",
        "live_execution",
        "runtime_execution",
        "data_root",
        "latest",
        "current",
        "autodetect",
        "calculate_ema",
        "backtest_engine",
        "run_backtest",
    )
    for marker in forbidden:
        assert marker not in source

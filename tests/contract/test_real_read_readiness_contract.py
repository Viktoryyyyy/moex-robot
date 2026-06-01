from moex_research.runners import real_read_readiness as readiness


def test_readiness_exports_all_contract_layers():
    expected = {
        "RealReadGateRequest",
        "RealReadGateResult",
        "RealReadReviewEvidence",
        "RealReadReviewPackage",
        "RealReadReviewDecision",
        "RealReadSourceContract",
        "RealReadDesign",
        "RealReadDesignDecision",
        "RealReadExecutionPlan",
        "RealReadExecutionReview",
        "RealReadExecutionReviewDecision",
        "RealReadOwnerHandoff",
        "RealReadOwnerAck",
        "RealReadOwnerAckDecision",
        "RealReadManualResult",
        "RealReadManualResultIntake",
        "RealReadManualResultIntakeDecision",
        "RealReadPMReviewPackage",
        "RealReadPMReview",
        "RealReadPMReviewDecision",
        "RealReadChain",
        "RealReadChainCloseout",
        "RealReadChainCloseoutDecision",
        "RealReadPhaseStatusPackage",
        "RealReadPhaseStatus",
        "RealReadPhaseStatusDecision",
    }

    assert expected.issubset(set(readiness.__all__))
    for name in expected:
        assert getattr(readiness, name)


def test_readiness_exports_validators_and_evaluators():
    expected = {
        "evaluate_real_read_gate",
        "evaluate_real_read_review_package",
        "evaluate_real_read_design",
        "evaluate_real_read_execution_review",
        "evaluate_real_read_owner_ack",
        "evaluate_real_read_manual_result_intake",
        "evaluate_real_read_pm_review",
        "evaluate_real_read_chain_closeout",
        "evaluate_real_read_phase_status",
        "validate_real_read_gate_request",
        "validate_real_read_gate_result",
        "validate_real_read_review_package",
        "validate_real_read_review_decision",
        "validate_real_read_source_contract",
        "validate_real_read_design",
        "validate_real_read_design_decision",
        "validate_real_read_execution_plan",
        "validate_real_read_execution_review",
        "validate_real_read_execution_review_decision",
        "validate_real_read_owner_handoff",
        "validate_real_read_owner_ack",
        "validate_real_read_owner_ack_decision",
        "validate_real_read_manual_result",
        "validate_real_read_manual_result_intake",
        "validate_real_read_manual_result_intake_decision",
        "validate_real_read_pm_review_package",
        "validate_real_read_pm_review",
        "validate_real_read_pm_review_decision",
        "validate_real_read_chain",
        "validate_real_read_chain_closeout",
        "validate_real_read_chain_closeout_decision",
        "validate_real_read_phase_status_package",
        "validate_real_read_phase_status",
        "validate_real_read_phase_status_decision",
    }

    assert expected.issubset(set(readiness.__all__))
    for name in expected:
        assert callable(getattr(readiness, name))


def test_readiness_module_has_no_side_effect_flags():
    forbidden = {
        "allow_real_read",
        "allow_network",
        "allow_registry_write",
        "allow_runtime",
        "allow_promotion",
        "read_csv",
        "read_parquet",
        "requests",
        "broker",
        "order",
    }
    source = readiness.__loader__.get_source(readiness.__name__)
    assert source is not None
    for marker in forbidden:
        assert marker not in source

from __future__ import annotations

import json
from pathlib import Path

from moex_research.external_data import moex_algopack_http
from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_runtime as runtime,
)
from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_source_validation as source,
)


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    ROOT
    / "contracts/experiments/"
    "usdrubf_phase8_7a_futoi_si_source_and_feature_contract_v1.json"
)
GENERIC_PATH = (
    ROOT / "src/moex_research/external_data/moex_algopack_http.py"
)
CNYRUBF_PATH = (
    ROOT
    / "src/moex_research/external_data/"
    "moex_cnyrubf_algopack_history.py"
)
SOURCE_PATH = (
    ROOT
    / "src/moex_research/runners/"
    "usdrubf_phase8_7a_futoi_si_source_validation.py"
)
RUNTIME_PATH = (
    ROOT
    / "src/moex_research/runners/"
    "usdrubf_phase8_7a_futoi_si_runtime.py"
)


def _contract() -> dict[str, object]:
    value = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_implementation_matches_exact_contract_source_identity() -> None:
    contract = _contract()
    identity = contract["contract_identity"]
    approved = contract["owner_decision"]
    source_identity = contract["source_identity"]
    assert identity["project"] == "MOEX_Bot"
    assert identity["phase"] == "8.7A"
    assert identity["contract_version"] == "1.6"
    assert approved["approved_source_ticker"] == source.SOURCE_TICKER == "Si"
    assert source_identity["exact_path"] == source.FUTOI_PATH
    assert source_identity["target_security_id"] == source.TARGET_SECURITY_ID
    assert source.TARGET_SECURITY_ID == "USDRUBF"
    assert source.STORAGE_FAMILY_CODE == "USDRUBF"
    assert source.FUTOI_PATH.endswith("/si.json")
    assert "usdrubf.json" not in source.FUTOI_PATH


def test_required_implementation_files_and_runtime_artifacts_exist() -> None:
    contract = _contract()
    scope = contract["implementation_scope_next_pr"]
    required_new = set(scope["required_new_files"])
    required_modified = set(scope["required_modified_files"])
    expected_new = {
        "src/moex_research/external_data/moex_algopack_http.py",
        "src/moex_research/runners/usdrubf_phase8_7a_futoi_si_source_validation.py",
        "src/moex_research/runners/usdrubf_phase8_7a_futoi_si_runtime.py",
        "tests/unit/test_moex_algopack_http.py",
        "tests/unit/test_usdrubf_phase8_7a_futoi_si_source_validation.py",
        "tests/contract/test_usdrubf_phase8_7a_futoi_si_source_validation_contract.py",
    }
    assert required_new == expected_new
    assert required_modified == {
        "src/moex_research/external_data/moex_cnyrubf_algopack_history.py",
        "tests/unit/test_usdrubf_phase8_6a_algopack_cnyrubf_history.py",
    }
    for relative in required_new | required_modified:
        assert (ROOT / relative).is_file()
    assert set(source.REQUIRED_RUNTIME_ARTIFACTS) == set(
        contract["required_runtime_artifacts"]
    )


def test_generic_transport_uses_canonical_token_and_no_source_blocker() -> None:
    contract = _contract()
    policy = contract["generic_algopack_transport_policy"]
    text = GENERIC_PATH.read_text(encoding="utf-8")
    assert moex_algopack_http.ALGOPACK_TOKEN_ENV == "MOEX_ALGOPACK_TOKEN"
    assert policy["required_environment_variable"] == "MOEX_ALGOPACK_TOKEN"
    assert policy["moex_api_key_alias_allowed"] is False
    assert "MOEX_API_KEY" not in text
    assert "Authorization" in text
    assert "RejectAllRedirects" in text
    assert policy["generic_error_model"][
        "source_specific_blocker_assigned_by_generic_layer"
    ] is False
    assert not hasattr(moex_algopack_http.AlgoPackHttpError, "blocker")


def test_cnyrubf_is_adapter_over_generic_transport() -> None:
    text = CNYRUBF_PATH.read_text(encoding="utf-8")
    assert "moex_algopack_http" in text
    assert "fetch_algopack_bytes as generic_fetch_algopack_bytes" in text
    assert "RejectAllRedirects as _RejectAllRedirects" in text
    assert "MOEX_API_KEY" not in text


def test_pairing_and_pit_contract_are_frozen() -> None:
    contract = _contract()
    schema = contract["raw_schema_policy"]
    pairing = contract["daily_pairing_policy"]
    pit = contract["pit_and_revision_policy"]
    assert pairing["candidate_pair_key"] == [
        "trade_date",
        "moment",
        "sess_id",
    ]
    assert schema["cross_group_seqnum_equality_required"] is False
    assert pairing["seqnum_cross_group_join_allowed"] is False
    assert source.PARTICIPANT_GROUPS == ("FIZ", "YUR")
    assert pit["availability_field"] == "systime"
    assert pit["forecast_anchor_local_time"] == "06:00:00"
    assert pit["fail_closed_if_publication_time_not_provable"] is True
    assert source.FORECAST_ANCHOR.isoformat() == "06:00:00"


def test_license_gate_prevents_unapproved_network_runtime() -> None:
    contract = _contract()
    policy = contract["license_and_access_policy"]
    assert policy["phase8_7b_entry_allowed_before_pass"] is False
    assert policy["failure_blocker"] == (
        "provider_license_and_access_terms_not_documented"
    )
    passed, normalized = source.validate_license_access_evidence({})
    assert passed is False
    assert normalized["blocker"] == policy["failure_blocker"]
    runtime_text = RUNTIME_PATH.read_text(encoding="utf-8")
    assert "if license_passed:" in runtime_text
    assert "network retrieval not performed" in runtime_text


def test_no_feature_computation_or_model_evaluation_in_phase8_7a() -> None:
    contract = _contract()
    scope = contract["implementation_scope_next_pr"]
    source_text = SOURCE_PATH.read_text(encoding="utf-8")
    runtime_text = RUNTIME_PATH.read_text(encoding="utf-8")
    assert scope["feature_computation_requires_phase8_7a_pass"] is True
    assert scope["incremental_value_evaluation_requires_separate_phase8_7b"] is True
    forbidden = (
        "LogisticRegression",
        "RandomForest",
        "fit(",
        "predict_proba",
        "joblib.dump",
    )
    for marker in forbidden:
        assert marker not in source_text
        assert marker not in runtime_text


def test_runtime_cli_and_contract_digest_are_frozen() -> None:
    assert runtime.CONTRACT_GIT_BLOB_SHA1 == (
        "eeb64cb20e8a122bc1d962077d4ca748c19d5fad"
    )
    assert "--license-access-evidence-path" in runtime.REQUIRED_ARGS
    assert "--pit-semantics-evidence-path" in runtime.REQUIRED_ARGS
    parser = runtime.build_argument_parser()
    option_strings = {
        option
        for action in parser._actions
        for option in action.option_strings
    }
    assert set(runtime.REQUIRED_ARGS).issubset(option_strings)

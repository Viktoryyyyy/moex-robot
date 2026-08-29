from __future__ import annotations

import ast
from datetime import date, datetime, timezone
import json
from pathlib import Path

from src.moex_research.intelligence.usdrubf_live_shadow_bridge import load_futoi_context


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = REPO_ROOT / "contracts/intelligence/usdrubf_futoi_live_acceptance_governance_v1.json"
LICENSE_EVIDENCE_PATH = REPO_ROOT / "contracts/intelligence/futoi_si_license_access_validation.json"
SNAPSHOT_PATH = REPO_ROOT / "src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot.py"
LIVE_SHADOW_PATH = REPO_ROOT / "src/moex_research/runners/usdrubf_live_shadow_smoke.py"


REQUIRED_GATES = {
    "registered_source",
    "deterministic_adapter",
    "provenance",
    "historical_pit_semantics",
    "historical_quality_and_coverage",
    "license_access_and_derived_use",
    "canonical_live_smoke",
    "recurring_live_quality_and_freshness",
    "typed_integration",
    "snapshot_live_enable",
}

REQUIRED_LICENSE_EVIDENCE_FIELDS = {
    "provider",
    "product",
    "account_entitlement",
    "permitted_research_use",
    "permitted_local_raw_storage",
    "permitted_derived_feature_use",
    "redistribution_policy",
    "evidence_source",
    "verified_at",
}


def _contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _license_evidence() -> dict[str, object]:
    return json.loads(LICENSE_EVIDENCE_PATH.read_text(encoding="utf-8"))


def _function(path: Path, name: str) -> ast.FunctionDef:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    ]
    assert len(functions) == 1
    return functions[0]


def _futoi_loader_call(function: ast.FunctionDef) -> ast.Call:
    calls = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "load_futoi_context"
    ]
    assert len(calls) == 1
    return calls[0]


def _assert_loader_disabled(function: ast.FunctionDef) -> None:
    call = _futoi_loader_call(function)
    enabled_keywords = [keyword for keyword in call.keywords if keyword.arg == "enabled"]
    assert len(enabled_keywords) == 1
    assert isinstance(enabled_keywords[0].value, ast.Constant)
    assert enabled_keywords[0].value.value is False


def test_futoi_live_acceptance_is_explicitly_governed_blocked() -> None:
    contract = _contract()

    assert contract["project"] == "MOEX_Bot"
    assert contract["status"] == "FUTOI_GOVERNED_BLOCKED"
    assert contract["acceptance_rule"]["all_required_gates_must_pass_for_live_accepted"] is True
    assert contract["acceptance_rule"]["adapter_working_is_not_live_acceptance"] is True
    assert contract["acceptance_rule"]["successful_authenticated_request_is_not_license_acceptance"] is True
    assert contract["acceptance_rule"]["account_owner_attestation_may_satisfy_account_specific_internal_use_permission"] is True
    assert contract["source_of_truth"]["license_access_evidence_ref"] == "contracts/intelligence/futoi_si_license_access_validation.json"

    gates = {gate["gate_id"]: gate for gate in contract["gates"]}
    assert set(gates) == REQUIRED_GATES
    assert all(gate["required"] is True for gate in gates.values())
    assert all(gate["status"] in {"PASS", "BLOCKED"} for gate in gates.values())
    assert any(gate["status"] == "BLOCKED" for gate in gates.values())

    license_gate = gates["license_access_and_derived_use"]
    assert license_gate["status"] == "PASS"
    gate_evidence = license_gate["evidence"]
    assert gate_evidence["artifact_ref"] == "contracts/intelligence/futoi_si_license_access_validation.json"
    assert gate_evidence["permission_basis"] == "explicit_account_owner_attestation"
    assert gate_evidence["account_bound"] is True
    assert gate_evidence["internal_project_use_only"] is True
    assert gate_evidence["external_redistribution_permission_asserted"] is False
    assert gate_evidence["api_key_or_successful_fetch_treated_as_permission"] is False

    evidence = _license_evidence()
    assert REQUIRED_LICENSE_EVIDENCE_FIELDS <= set(evidence)
    assert evidence["project"] == "MOEX_Bot"
    assert evidence["status"] == "PASS_FOR_INTERNAL_PROJECT_USE"
    assert evidence["provider"] == "MOEX"
    assert evidence["product"] == "AlgoPack FUTOI"
    assert evidence["attestor_identity"]["github_login"] == "Viktoryyyyy"
    assert evidence["attestor_identity"]["role"] == "repository_owner_and_account_owner"
    assert evidence["account_entitlement"]["product_entitlement"] == "FUTOI"
    assert evidence["account_entitlement"]["credential_name_only"] == "MOEX_API_KEY"
    assert evidence["account_entitlement"]["credential_value_recorded"] is False
    assert evidence["permitted_research_use"] is True
    assert evidence["permitted_local_raw_storage"] is True
    assert evidence["permitted_derived_feature_use"] is True
    assert evidence["redistribution_policy"]["external_redistribution_permission_asserted"] is False
    assert evidence["evidence_source"]["type"] == "explicit_account_owner_attestation"
    assert evidence["evidence_source"]["owner_statement"] == "разрешено"
    verified_at = datetime.fromisoformat(evidence["verified_at"].replace("Z", "+00:00"))
    assert verified_at.tzinfo is not None

    assert gates["canonical_live_smoke"]["status"] == "BLOCKED"
    assert gates["recurring_live_quality_and_freshness"]["status"] == "BLOCKED"
    assert gates["snapshot_live_enable"]["status"] == "BLOCKED"
    assert contract["architecture_decision"]["legacy_phase8_transport_may_become_live_authority"] is False


def test_blocked_acceptance_has_no_hidden_factual_or_action_authority() -> None:
    authority = _contract()["authority"]

    assert authority["factual_live_authority"] is False
    assert authority["directional_authority"] is False
    assert authority["action_authority"] is False
    assert authority["buy_sell_authority"] is False
    assert authority["blocked_fallback_direction"] == "MIXED"
    assert authority["blocked_fallback_confidence"] == 0.0
    assert authority["blocked_quality_status"] == "BLOCKED"


def test_disabled_loader_returns_governed_blocked_fallback() -> None:
    contract = _contract()
    authority = contract["authority"]
    fallback_at = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)

    context = load_futoi_context(
        prior_trade_date=date(2026, 8, 28),
        current_trade_date=date(2026, 8, 29),
        fallback_available_at=fallback_at,
        enabled=False,
    )

    assert context.direction == authority["blocked_fallback_direction"] == "MIXED"
    assert context.confidence == authority["blocked_fallback_confidence"] == 0.0
    assert context.quality_status == authority["blocked_quality_status"] == "BLOCKED"
    assert context.available_at == fallback_at
    assert context.details["reason"] == "live_futoi_not_explicitly_enabled"


def test_snapshot_remains_fail_closed_while_acceptance_is_blocked() -> None:
    assert _contract()["status"] == "FUTOI_GOVERNED_BLOCKED"
    function = _function(SNAPSHOT_PATH, "_live_market_component")
    _assert_loader_disabled(function)

    data_assignments = [
        node
        for node in function.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "data" for target in node.targets)
        and isinstance(node.value, ast.Dict)
    ]
    assert len(data_assignments) == 1
    data_dict = data_assignments[0].value
    futoi_values = [
        value
        for key, value in zip(data_dict.keys, data_dict.values)
        if isinstance(key, ast.Constant) and key.value == "futoi"
    ]
    assert len(futoi_values) == 1
    assert isinstance(futoi_values[0], ast.Dict)
    futoi_dict = futoi_values[0]
    action_values = [
        value
        for key, value in zip(futoi_dict.keys, futoi_dict.values)
        if isinstance(key, ast.Constant) and key.value == "action_authority"
    ]
    assert len(action_values) == 1
    assert isinstance(action_values[0], ast.Constant)
    assert action_values[0].value is False


def test_live_shadow_runner_cannot_bypass_governed_block() -> None:
    assert _contract()["status"] == "FUTOI_GOVERNED_BLOCKED"
    source = LIVE_SHADOW_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    status_assignments = [
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "_FUTOI_LIVE_ACCEPTANCE_STATUS"
            for target in node.targets
        )
    ]
    assert len(status_assignments) == 1
    assert isinstance(status_assignments[0].value, ast.Constant)
    assert status_assignments[0].value.value == "FUTOI_GOVERNED_BLOCKED"

    function = _function(LIVE_SHADOW_PATH, "run_once")
    assert function.body
    first_statement = function.body[0]
    assert isinstance(first_statement, ast.If)
    assert "enable_futoi" in ast.dump(first_statement.test)
    assert first_statement.body
    assert isinstance(first_statement.body[0], ast.Raise)
    _assert_loader_disabled(function)

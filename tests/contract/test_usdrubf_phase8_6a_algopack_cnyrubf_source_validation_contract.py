from __future__ import annotations

import ast
import json
from pathlib import Path

from moex_research.runners import (
    usdrubf_phase8_6a_algopack_cnyrubf_source_validation as runner,
)


CONTRACT_PATH = Path(
    "contracts/experiments/"
    "usdrubf_phase8_6a_algopack_cnyrubf_fo_source_correction_v1.json"
)
SOURCE_PATH = Path(
    "src/moex_research/external_data/moex_cnyrubf_algopack_history.py"
)
RUNNER_PATH = Path(
    "src/moex_research/runners/"
    "usdrubf_phase8_6a_algopack_cnyrubf_source_validation.py"
)
RUNTIME_PATH = Path(
    "src/moex_research/runners/"
    "usdrubf_phase8_6a_algopack_cnyrubf_runtime.py"
)


def load_contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_contract_identity_and_source_are_exact() -> None:
    contract = load_contract()
    identity = contract["contract_identity"]
    source = contract["source_identity"]

    assert identity == {
        "contract_id": runner.CONTRACT_ID,
        "contract_version": runner.CONTRACT_VERSION,
        "project": "MOEX_Bot",
        "task_id": runner.CONTRACT_TASK_ID,
        "lane": "ema_3_19_ai",
        "phase": "8.6A",
        "execution_mode": "browser_controlled_github_route",
        "status": "source_correction_contract_pending_implementation",
    }
    assert source["source_id"] == "moex_algopack_cnyrubf_fo_tradestats_5m"
    assert source["security_id"] == "CNYRUBF"
    assert source["asset_code"] == "CNYRUBTOM"
    assert source["board_id"] == "RFUD"
    assert source["engine"] == "futures"
    assert source["market"] == "forts"
    assert source["algopack_market_code"] == "FO"
    assert source["contract_roll_mapping_required"] is False


def test_exact_implementation_file_inventory_exists() -> None:
    contract = load_contract()
    declared = tuple(
        contract["implementation_scope_next_pr"]["required_new_files"]
    )
    assert declared == runner.IMPLEMENTATION_FILES
    for path in declared:
        assert Path(path).is_file(), path


def test_implementation_does_not_import_spot_identity_modules() -> None:
    forbidden_modules = {
        "moex_research.external_data.moex_cnyrub_history",
        "moex_research.external_data.moex_cnyrub_algopack_history",
        "moex_research.external_data.moex_cnyrub_algopack_timestamp_policy",
        "moex_research.runners.usdrubf_phase8_6a_algopack_cnyrub_source_validation",
        "moex_research.runners.usdrubf_phase8_6a_moex_cnyrub_source_validation",
    }
    for path in (SOURCE_PATH, RUNNER_PATH, RUNTIME_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
        assert not imported & forbidden_modules, (path, imported & forbidden_modules)


def test_runtime_uses_direct_cnyrubf_parser_without_monkeypatch() -> None:
    text = RUNTIME_PATH.read_text(encoding="utf-8")
    assert "install_timestamp_policy" not in text
    assert "algopack_cnyrubf_source_validation" in text
    assert "load_dotenv" in text


def test_metadata_contract_separates_columns_from_logical_keys() -> None:
    policy = load_contract()["metadata_identity_policy"]
    assert policy["required_description_columns"] == ["name", "value"]
    assert policy["required_description_value_keys"] == ["SECID"]
    assert "SECID" not in policy["required_description_columns"]
    assert policy["secid_must_equal"] == "CNYRUBF"
    assert policy["boardid_must_equal"] == "RFUD"
    assert policy["engine_must_equal"] == "futures"
    assert policy["market_must_equal"] == "forts"


def test_acceptance_matrix_leakage_policy_is_enforced() -> None:
    contract = load_contract()
    policy = contract["acceptance_matrix_leakage_policy"]
    forbidden = frozenset(policy["forbidden_acceptance_matrix_fields"])

    assert forbidden == runner.FORBIDDEN_ACCEPTANCE_MATRIX_FIELDS
    assert not forbidden & set(runner.ACCEPTANCE_MATRIX_COLUMNS)
    assert policy["forbidden_fields_must_be_absent"] is True
    assert policy["failure_blocker"] == "target_derived_field_leakage"
    assert policy["phase8_6b_entry_allowed_on_failure"] is False
    assert "target-derived fields" in contract["gates"]["G8"]


def test_runtime_artifacts_and_authority_boundary_are_exact() -> None:
    contract = load_contract()
    implementation = contract["implementation_scope_next_pr"]

    assert tuple(contract["required_runtime_artifacts"]) == (
        runner.DECLARED_OUTPUT_ARTIFACTS
    )
    assert implementation["implementation_requires_separate_pr"] is True
    assert implementation["server_apply_allowed"] is False
    assert implementation["controlled_runtime_allowed"] is False
    assert (
        implementation["controlled_runtime_requires_separate_explicit_authority"]
        is True
    )
    assert "no server apply" in contract["non_authorizations"]
    assert "no controlled runtime" in contract["non_authorizations"]
    assert "no model fit" in contract["non_authorizations"]
    assert "no trading action" in contract["non_authorizations"]


def test_raw_schema_and_pit_semantics_match_live_probe() -> None:
    contract = load_contract()
    fields = contract["raw_tradestats_required_fields"]
    timestamp = contract["timestamp_policy"]
    probe = contract["live_probe_evidence"]

    assert "secid" in fields
    assert "asset_code" in fields
    assert "SYSTIME" in fields
    assert {"oi_open", "oi_high", "oi_low", "oi_close"}.issubset(fields)
    assert timestamp["tradetime_semantics"] == (
        "completed_five_minute_interval_end"
    )
    assert timestamp["bucket_begin"] == "bucket_end - 5 minutes"
    assert timestamp["source_available_at"] == "SYSTIME"
    assert timestamp["source_available_at_before_anchor_required"] is True
    assert probe["security_ids"] == ["CNYRUBF"]
    assert probe["asset_codes"] == ["CNYRUBTOM"]
    assert probe["rows_with_systime_before_tradetime"] == 0

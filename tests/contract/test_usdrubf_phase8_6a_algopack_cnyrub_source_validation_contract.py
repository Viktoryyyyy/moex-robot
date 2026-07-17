from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parents[2]
CONTRACT_PATH = (
    ROOT
    / "contracts/experiments/"
    "usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2.json"
)
CONTRACT = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

EXPECTED_SCOPE = [
    "contracts/experiments/"
    "usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2.json",
    "src/moex_research/external_data/moex_cnyrub_algopack_history.py",
    "src/moex_research/runners/"
    "usdrubf_phase8_6a_algopack_cnyrub_source_validation.py",
    "tests/unit/test_usdrubf_phase8_6a_algopack_cnyrub_history.py",
    "tests/unit/"
    "test_usdrubf_phase8_6a_algopack_cnyrub_source_validation.py",
    "tests/contract/"
    "test_usdrubf_phase8_6a_algopack_cnyrub_source_validation_contract.py",
]


def test_contract_identity_and_exact_create_only_scope() -> None:
    assert CONTRACT["contract_identity"] == {
        "contract_id": (
            "usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2"
        ),
        "contract_version": "2.0",
        "project": "MOEX Bot",
        "task_id": (
            "ema_3_19_ai_phase_8_6a_algopack_cnyrub_source_validation_v2"
        ),
        "lane": "ema_3_19_ai",
        "phase": "8.6A",
        "execution_mode": "browser_chatgpt_github_direct",
        "status": "source_validation_only",
    }
    assert CONTRACT["approved_branch"] == (
        "research/ema-3-19-ai/"
        "phase-8-6a-algopack-cnyrub-source-validation-v2"
    )
    scope = CONTRACT["approved_file_scope"]
    assert scope["create_only"] == EXPECTED_SCOPE
    assert scope["existing_files_to_modify"] == []
    assert scope["exact_file_count"] == 6
    assert scope["maximum_changed_file_count"] == 6
    assert scope["scope_widening_allowed"] is False


def test_exact_subscribed_algopack_source_and_no_fallback() -> None:
    source = CONTRACT["source_identity"]
    assert source["source_id"] == (
        "moex_algopack_cnyrub_tom_tradestats_5m"
    )
    assert source["official_service"] == "MOEX AlgoPack subscription"
    assert source["tradestats_route"] == (
        "https://apim.moex.com/iss/datashop/algopack/fx/tradestats/"
        "CNYRUB_TOM.json"
    )
    assert (
        source["security_id"],
        source["board_id"],
        source["engine"],
        source["market"],
        source["bucket_interval_minutes"],
    ) == ("CNYRUB_TOM", "CETS", "currency", "selt", 5)
    policy = CONTRACT["official_source_policy"]
    assert policy["third_party_market_data_allowed"] is False
    assert policy["bank_of_russia_rate_allowed"] is False
    assert policy["cnyrubf_allowed"] is False
    assert policy["synthetic_cross_allowed"] is False
    assert policy["substitute_security_or_board_allowed"] is False


def test_bearer_token_never_enters_cli_url_artifacts_or_repo() -> None:
    auth = CONTRACT["authorization_policy"]
    assert auth == {
        "scheme": "Bearer",
        "required_environment_variable": "MOEX_ALGOPACK_TOKEN",
        "token_in_cli_allowed": False,
        "token_in_url_allowed": False,
        "token_in_logs_allowed": False,
        "token_in_artifacts_allowed": False,
        "token_in_repository_allowed": False,
    }
    assert CONTRACT["required_environment_variables"] == [
        "MOEX_ALGOPACK_TOKEN"
    ]


def test_directional_volume_contract_is_explicit() -> None:
    aggregation = CONTRACT["aggregation_policy"]
    assert aggregation["daily_volume_buy"] == "sum vol_b"
    assert aggregation["daily_volume_sell"] == "sum vol_s"
    assert aggregation["daily_value_buy"] == "sum val_b"
    assert aggregation["daily_value_sell"] == "sum val_s"
    assert aggregation["volume_identity_required"] == (
        "volume == volume_buy + volume_sell"
    )
    required = set(CONTRACT["normalized_source_required_fields"])
    assert {
        "volume_buy",
        "volume_sell",
        "volume_imbalance",
        "value_buy",
        "value_sell",
        "trades_buy",
        "trades_sell",
    }.issubset(required)
    matrix = set(CONTRACT["acceptance_matrix_fields"])
    assert {
        "cnyrub_volume_buy",
        "cnyrub_volume_sell",
        "cnyrub_volume_imbalance",
        "cnyrub_value_buy",
        "cnyrub_value_sell",
        "cnyrub_trades_buy",
        "cnyrub_trades_sell",
    }.issubset(matrix)
    assert matrix.isdisjoint(
        CONTRACT["forbidden_acceptance_matrix_fields"]
    )


def test_exact_nine_artifacts_and_authority_boundary() -> None:
    assert len(CONTRACT["runtime_artifacts"]) == 9
    assert CONTRACT["artifact_policy"]["exact_count"] == 9
    assert CONTRACT["artifact_policy"]["model_file_allowed"] is False
    authority = CONTRACT["authority_boundary"]
    assert authority["direct_main_write_allowed"] is False
    assert authority["merge_allowed"] is False
    assert authority["server_apply_allowed"] is False
    assert authority["controlled_runtime_allowed"] is False
    assert authority["broker_action_allowed"] is False
    assert authority["trading_allowed"] is False

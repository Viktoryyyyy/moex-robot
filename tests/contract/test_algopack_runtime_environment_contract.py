from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).parents[2]
ENV_EXAMPLE = (ROOT / ".env.example").read_text(encoding="utf-8")
SERVER_LAYOUT = (ROOT / "docs/sot/runtime/server_layout.v1.md").read_text(encoding="utf-8")
SUBCHAT_EXECUTION_RULES = (
    ROOT / "docs/sot/runtime/subchat_server_execution_rules.v1.md"
).read_text(encoding="utf-8")
DAILY_REFRESH_SCHEDULER_CONTRACT = (
    ROOT / "contracts/datasets/futures_daily_refresh_scheduler_contract.md"
).read_text(encoding="utf-8")
ALGOPACK_CONTRACT = json.loads(
    (
        ROOT
        / "contracts/experiments/usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2.json"
    ).read_text(encoding="utf-8")
)
ALGOPACK_SOURCE = (
    ROOT / "src/moex_research/external_data/moex_cnyrub_algopack_history.py"
).read_text(encoding="utf-8")
CANONICAL_EXPLICIT_DOTENV_ENTRYPOINTS = {
    "src/moex_data/futures/universal_daily_refresh_runner.py": 4,
    "src/moex_research/runners/usdrubf_live_shadow_smoke.py": 4,
    "src/moex_research/runners/usdrubf_phase8_6a_algopack_cnyrub_runtime.py": 4,
    "src/moex_research/runners/usdrubf_phase8_6a_algopack_cnyrubf_runtime.py": 4,
}


def _load_dotenv_calls(text: str) -> list[ast.Call]:
    tree = ast.parse(text)
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "load_dotenv"
    ]


def test_project_env_path_is_explicit_and_repository_duplicate_must_be_absent() -> None:
    canonical_env = "/home/trader/moex_bot/.env"
    repo_env = "/home/trader/moex_bot/moex-robot/.env"
    assert f"Project environment file: `{canonical_env}`" in SERVER_LAYOUT
    assert f"The duplicate repository-local file `{repo_env}` must be absent" in SERVER_LAYOUT
    assert f'load_dotenv("{canonical_env}", override=False)' in SUBCHAT_EXECUTION_RULES
    assert 'load_dotenv(".env", override=False)' not in SUBCHAT_EXECUTION_RULES
    assert "load_dotenv('.env', override=False)" not in SUBCHAT_EXECUTION_RULES
    assert f"canonical_dotenv: {canonical_env}" in DAILY_REFRESH_SCHEDULER_CONTRACT
    assert "repository_local_dotenv: forbidden_duplicate_must_be_absent_on_applied_state" in (
        DAILY_REFRESH_SCHEDULER_CONTRACT
    )


def test_canonical_explicit_dotenv_entrypoints_use_parent_project_env() -> None:
    forbidden_absolute_repo_env = "/home/trader/moex_bot/moex-robot/.env"
    for path, parent_index in CANONICAL_EXPLICIT_DOTENV_ENTRYPOINTS.items():
        source = (ROOT / path).read_text(encoding="utf-8")
        expected_assignment = (
            "PROJECT_ENV_PATH = Path(__file__).resolve().parents["
            + str(parent_index)
            + "] / \".env\""
        )
        assert expected_assignment in source, path
        assert forbidden_absolute_repo_env not in source, path
        calls = _load_dotenv_calls(source)
        assert calls, path
        for call in calls:
            assert call.args, path
            first_arg = call.args[0]
            assert isinstance(first_arg, ast.Name) and first_arg.id == "PROJECT_ENV_PATH", path
            override = next((kw.value for kw in call.keywords if kw.arg == "override"), None)
            assert isinstance(override, ast.Constant) and override.value is False, path


def test_algopack_variable_is_canonical_across_example_contract_and_code() -> None:
    algopack_assignment = "MOEX_" + "ALGOPACK_TOKEN" + "="
    assert ENV_EXAMPLE.count(algopack_assignment) == 1
    assert ALGOPACK_CONTRACT["authorization_policy"]["required_environment_variable"] == (
        "MOEX_ALGOPACK_TOKEN"
    )
    assert ALGOPACK_CONTRACT["required_environment_variables"] == [
        "MOEX_ALGOPACK_TOKEN"
    ]
    assert "ALGOPACK_TOKEN_ENV: Final[str] = 'MOEX_ALGOPACK_TOKEN'" in ALGOPACK_SOURCE


def test_legacy_api_key_is_not_an_algopack_alias() -> None:
    api_key_assignment = "MOEX_" + "API_KEY" + "="
    assert ENV_EXAMPLE.count(api_key_assignment) == 1
    assert (
        "This variable is not accepted as a fallback for subscribed AlgoPack routes."
        in ENV_EXAMPLE
    )
    assert "`MOEX_API_KEY` is not an alias or fallback for `MOEX_ALGOPACK_TOKEN`." in (
        SERVER_LAYOUT
    )
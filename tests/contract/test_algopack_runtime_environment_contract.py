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
ALGOPACK_CONTRACT = json.loads(
    (
        ROOT
        / "contracts/experiments/usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2.json"
    ).read_text(encoding="utf-8")
)
ALGOPACK_SOURCE = (
    ROOT / "src/moex_research/external_data/moex_cnyrub_algopack_history.py"
).read_text(encoding="utf-8")
SRC_DIR = ROOT / "src"
DOTENV_SOURCE_TEXTS = {
    str(path.relative_to(ROOT)): text
    for path in sorted(SRC_DIR.rglob("*.py"))
    if "load_dotenv" in (text := path.read_text(encoding="utf-8"))
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


def _expected_project_env_assignment(path: str) -> str:
    source_path = ROOT / path
    relative_parent_parts = source_path.parent.relative_to(ROOT).parts
    parent_index = len(relative_parent_parts) + 1
    return (
        "PROJECT_ENV_PATH = Path(__file__).resolve().parents["
        + str(parent_index)
        + "] / \".env\""
    )


def test_project_env_path_is_explicit_and_repository_env_is_not_a_fallback() -> None:
    canonical_env = "/home/trader/moex_bot/.env"
    assert f"Project environment file: `{canonical_env}`" in SERVER_LAYOUT
    assert (
        "`/home/trader/moex_bot/moex-robot/.env` is not a canonical project runtime source"
        in SERVER_LAYOUT
    )
    assert f'load_dotenv("{canonical_env}", override=False)' in SUBCHAT_EXECUTION_RULES
    assert 'load_dotenv(".env", override=False)' not in SUBCHAT_EXECUTION_RULES
    assert "load_dotenv('.env', override=False)" not in SUBCHAT_EXECUTION_RULES


def test_source_dotenv_loaders_use_canonical_parent_project_env() -> None:
    assert DOTENV_SOURCE_TEXTS
    forbidden_absolute_repo_env = "/home/trader/moex_bot/moex-robot/.env"
    violations: list[str] = []
    for path, source in DOTENV_SOURCE_TEXTS.items():
        calls = _load_dotenv_calls(source)
        if not calls:
            continue
        if _expected_project_env_assignment(path) not in source:
            violations.append(path + ":missing_or_wrong_PROJECT_ENV_PATH")
        if forbidden_absolute_repo_env in source:
            violations.append(path + ":forbidden_repo_env_absolute_path")
        for index, call in enumerate(calls, start=1):
            if not call.args:
                violations.append(path + ":load_dotenv_call_" + str(index) + ":missing_path_argument")
            else:
                first_arg = call.args[0]
                if not (isinstance(first_arg, ast.Name) and first_arg.id == "PROJECT_ENV_PATH"):
                    violations.append(path + ":load_dotenv_call_" + str(index) + ":wrong_path_argument")
            override = next((kw.value for kw in call.keywords if kw.arg == "override"), None)
            if not (isinstance(override, ast.Constant) and override.value is False):
                violations.append(path + ":load_dotenv_call_" + str(index) + ":override_must_be_false")
    assert not violations, "\n".join(violations)


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
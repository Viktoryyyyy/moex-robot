from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parents[2]
ENV_EXAMPLE = (ROOT / ".env.example").read_text(encoding="utf-8")
SERVER_LAYOUT = (ROOT / "docs/sot/runtime/server_layout.v1.md").read_text(encoding="utf-8")
ALGOPACK_CONTRACT = json.loads(
    (
        ROOT
        / "contracts/experiments/usdrubf_phase8_6a_algopack_cnyrub_source_validation_v2.json"
    ).read_text(encoding="utf-8")
)
ALGOPACK_SOURCE = (
    ROOT / "src/moex_research/external_data/moex_cnyrub_algopack_history.py"
).read_text(encoding="utf-8")
RUNNERS_DIR = ROOT / "src/moex_research/runners"
DOTENV_RUNTIME_TEXTS = {
    path.name: text
    for path in sorted(RUNNERS_DIR.glob("*_runtime.py"))
    if "load_dotenv" in (text := path.read_text(encoding="utf-8"))
}


def test_project_env_path_is_explicit_and_repository_env_is_not_a_fallback() -> None:
    assert "Project environment file: `/home/trader/moex_bot/.env`" in SERVER_LAYOUT
    assert (
        "`/home/trader/moex_bot/moex-robot/.env` is not a canonical project runtime source"
        in SERVER_LAYOUT
    )
    assert DOTENV_RUNTIME_TEXTS
    expected_parent_env = "PROJECT_ENV_PATH = Path(__file__).resolve().parents[4] / \".env\""
    forbidden_repo_env = "parents[3] / \".env\""
    for runtime in DOTENV_RUNTIME_TEXTS.values():
        assert expected_parent_env in runtime
        assert forbidden_repo_env not in runtime


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

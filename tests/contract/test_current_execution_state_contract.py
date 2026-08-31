from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "docs" / "MOEX_BOT_CURRENT_EXECUTION_STATE_2026-08-27.md"
POINTER = ROOT / "docs" / "MOEX_BOT_CURRENT_EXECUTION_STATE.md"

IMPLEMENTATION_SHA = "21b57e54e993dd63f9f3a8b772bb39f39508db5e"
READINESS_SHA = "19fa3525b79973da75e1111e496358b6c3f68d95"

EXPECTED_APPLY_TEMPLATE = (
    'cd ~/moex_bot && source venv/bin/activate && cd moex-robot && '
    'test -z "$(git status --porcelain)" && '
    'test "$(git branch --show-current)" = "main" && '
    'git fetch origin main && '
    'test "$(git rev-parse origin/main)" = "<MERGED_SHA>" && '
    'git merge --ff-only origin/main && '
    'test "$(git rev-parse HEAD)" = "<MERGED_SHA>" && '
    'echo PROJECT=MOEX_Bot ACTION=server_apply STATUS=APPLIED APPLIED_SHA=$(git rev-parse HEAD)'
)

EXPECTED_STAGE7_PILOT = (
    'cd ~/moex_bot && source venv/bin/activate && cd moex-robot && '
    'test -z "$(git status --porcelain)" && '
    'test "$(git branch --show-current)" = "main" && '
    f'test "$(git rev-parse HEAD)" = "{IMPLEMENTATION_SHA}" && '
    'test ! -e /home/trader/moex_bot/data/runs/step7_rub_native_d1_w1/run_id=step7_pilot_20260827_v1 && '
    'test ! -e /home/trader/moex_bot/data/state/acceptance/step7_rub_native_d1_w1/run_id=step7_pilot_20260827_v1 && '
    'test ! -e /home/trader/moex_bot/step7_pilot_20260827_v1.log && '
    '(nohup env MOEX_DATA_ROOT=/home/trader/moex_bot/data PYTHONPATH=src '
    'python -m moex_data.step7_rub_native_d1_w1_pilot_runner '
    '--artifact-version step7_pilot_20260827_v1 '
    '--env-file /home/trader/moex_bot/.env '
    '> /home/trader/moex_bot/step7_pilot_20260827_v1.log 2>&1 < /dev/null '
    '& pid=$!; echo PROJECT=MOEX_Bot STATUS=STAGE7_V1_STARTED PID=$pid)'
)

EXPECTED_STAGE7_ACCEPTANCE = (
    'cd ~/moex_bot && source venv/bin/activate && cd moex-robot && '
    'test -z "$(git status --porcelain)" && '
    'test "$(git branch --show-current)" = "main" && '
    f'test "$(git rev-parse HEAD)" = "{IMPLEMENTATION_SHA}" && '
    'test ! -e /home/trader/moex_bot/data/state/acceptance/step7_rub_native_d1_w1/'
    'run_id=step7_pilot_20260827_v1/accepted_pointers.json && '
    'test ! -e /home/trader/moex_bot/step7_accept_20260827_v1.log && '
    '(nohup env MOEX_DATA_ROOT=/home/trader/moex_bot/data PYTHONPATH=src '
    'python -m moex_data.step7_rub_native_d1_w1_acceptance '
    '--run-id step7_pilot_20260827_v1 --repo-root . '
    '--env-file /home/trader/moex_bot/.env '
    '> /home/trader/moex_bot/step7_accept_20260827_v1.log 2>&1 < /dev/null '
    '& pid=$!; echo PROJECT=MOEX_Bot STATUS=STAGE7_ACCEPTANCE_STARTED PID=$pid)'
)


def _state_text() -> str:
    return STATE.read_text(encoding="utf-8")


def _exact_lines(text: str) -> set[str]:
    return {line.strip() for line in text.splitlines() if line.strip()}


def _section(text: str, start: str, end: str) -> str:
    return text.split(start, 1)[1].split(end, 1)[0]


def _bash_block(section: str) -> str:
    return section.split("```bash\n", 1)[1].split("\n```", 1)[0].strip()


def test_current_execution_state_uses_canonical_server_paths_only() -> None:
    text = _state_text()
    lines = _exact_lines(text)
    for line in (
        "repo_path=/home/trader/moex_bot/moex-robot",
        "shell_path=~/moex_bot/moex-robot",
        "command_prefix=cd ~/moex_bot && source venv/bin/activate && cd moex-robot",
    ):
        assert line in lines
    assert "moex_robot" not in text
    assert "Forbidden/deprecated paths:" not in text


def test_server_apply_template_is_exact_guarded_command() -> None:
    section = _section(
        _state_text(),
        "## 2. Canonical server apply command template\n",
        "\nThis command is the canonical apply pattern",
    )
    assert _bash_block(section) == EXPECTED_APPLY_TEMPLATE


def test_stage7_launch_commands_are_exact_guarded_commands() -> None:
    text = _state_text()
    pilot = _section(text, "### 5.2 Physical pilot start\n", "\nPilot result log:\n")
    acceptance = _section(text, "### 5.3 Acceptance start\n", "\nAcceptance result log:\n")
    assert _bash_block(pilot) == EXPECTED_STAGE7_PILOT
    assert _bash_block(acceptance) == EXPECTED_STAGE7_ACCEPTANCE


def test_stage7_closed_metadata_is_exact() -> None:
    lines = _exact_lines(_state_text())
    for line in (
        f"merged implementation SHA: {IMPLEMENTATION_SHA}",
        "run_id: step7_pilot_20260827_v1",
        "USDRUBF D1 OHLCV: 1100",
        "CNYRUBF D1 OHLCV: 1100",
        "USDRUBF completed W1 OHLCV: 224",
        "CNYRUBF completed W1 OHLCV: 224",
        "accepted_pointer_count: 8",
        "expected_pointer_count: 8",
        f"readiness merge SHA: {READINESS_SHA}",
        f"server applied SHA: {READINESS_SHA}",
    ):
        assert line in lines


def test_stage8_is_marked_wip_not_accepted_or_applied() -> None:
    lines = _exact_lines(_state_text())
    for line in (
        "branch: agent/step8-position-risk-state",
        "current WIP head: e77d9ea6ae35b87d34d797f86e1022186e978f56",
        "ahead_by: 4",
        "behind_by: 0",
        "PR: #393",
        "PR state at record time: open",
        "PR title: Implement Stage 8 bounded position risk state",
        "merged: false",
        "server_applied: false",
        "accepted: false",
    ):
        assert line in lines


def test_current_pointer_targets_dated_state() -> None:
    lines = _exact_lines(POINTER.read_text(encoding="utf-8"))
    assert "current_execution_state: `docs/MOEX_BOT_CURRENT_EXECUTION_STATE_2026-08-31.md`" in lines
    assert any("does not override the management canon" in line for line in lines)

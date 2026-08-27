from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "docs" / "MOEX_BOT_CURRENT_EXECUTION_STATE_2026-08-27.md"
POINTER = ROOT / "docs" / "MOEX_BOT_CURRENT_EXECUTION_STATE.md"


def _state_text() -> str:
    return STATE.read_text(encoding="utf-8")


def _exact_lines(text: str) -> set[str]:
    return {line.strip() for line in text.splitlines() if line.strip()}


def test_current_execution_state_uses_canonical_server_paths() -> None:
    text = _state_text()
    lines = _exact_lines(text)
    for line in (
        "repo_path=/home/trader/moex_bot/moex-robot",
        "shell_path=~/moex_bot/moex-robot",
        "command_prefix=cd ~/moex_bot && source venv/bin/activate && cd moex-robot",
    ):
        assert line in lines

    forbidden = text.split("Forbidden/deprecated paths:\n", 1)[1].split("\nRules:\n", 1)[0]
    forbidden_lines = _exact_lines(forbidden)
    assert forbidden_lines == {
        "```text",
        "```",
        "/home/trader/moex_bot/moex_robot",
        "~/moex_bot/moex_robot",
        "cd ~/moex_bot/moex_robot && source venv/bin/activate",
    }


def test_server_apply_template_is_exact_sha_guarded() -> None:
    text = _state_text()
    for token in (
        'test -z "$(git status --porcelain)"',
        'test "$(git branch --show-current)" = "main"',
        "git fetch origin main",
        'test "$(git rev-parse origin/main)" = "<MERGED_SHA>"',
        "git merge --ff-only origin/main",
        'test "$(git rev-parse HEAD)" = "<MERGED_SHA>"',
        "PROJECT=MOEX_Bot ACTION=server_apply STATUS=APPLIED",
    ):
        assert token in text


def test_stage7_closed_metadata_is_exact() -> None:
    lines = _exact_lines(_state_text())
    for line in (
        "merged implementation SHA: 21b57e54e993dd63f9f3a8b772bb39f39508db5e",
        "run_id: step7_pilot_20260827_v1",
        "USDRUBF D1 OHLCV: 1100",
        "CNYRUBF D1 OHLCV: 1100",
        "USDRUBF completed W1 OHLCV: 224",
        "CNYRUBF completed W1 OHLCV: 224",
        "accepted_pointer_count: 8",
        "expected_pointer_count: 8",
        "readiness merge SHA: 19fa3525b79973da75e1111e496358b6c3f68d95",
        "server applied SHA: 19fa3525b79973da75e1111e496358b6c3f68d95",
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
    assert "current_execution_state: `docs/MOEX_BOT_CURRENT_EXECUTION_STATE_2026-08-27.md`" in lines
    assert any("does not override the management canon" in line for line in lines)

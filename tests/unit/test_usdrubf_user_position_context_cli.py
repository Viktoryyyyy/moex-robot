from __future__ import annotations

from pathlib import Path

from src.moex_research.runners import usdrubf_user_position_context as position


def test_cli_shape_long_short_flat() -> None:
    long_args = position.parse_args(["--direction", "LONG", "--average-entry-price", "86.15"])
    short_args = position.parse_args(["--direction", "SHORT", "--average-entry-price", "85.95"])
    flat_args = position.parse_args(["--direction", "FLAT"])

    assert long_args.direction == "LONG"
    assert long_args.average_entry_price == 86.15
    assert short_args.direction == "SHORT"
    assert short_args.average_entry_price == 85.95
    assert flat_args.direction == "FLAT"
    assert flat_args.average_entry_price is None


def test_state_file_is_inside_existing_canonical_snapshot_state_dir(tmp_path: Path) -> None:
    path = position.user_position_context_path(tmp_path)

    assert path == (
        tmp_path
        / "state"
        / "rub_intelligence"
        / "chat_analysis_snapshot"
        / "user_position_context.json"
    )

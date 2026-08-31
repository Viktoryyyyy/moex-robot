from __future__ import annotations

from pathlib import Path


def test_active_runtime_has_no_legacy_calendar_dependency() -> None:
    repo = Path(__file__).resolve().parents[1]
    forbidden = (
        "/iss/" + "calendars.json",
        "moex_iss_" + "futures_calendar",
        "fetch_futures_" + "calendar_rows",
        "select_completed_" + "trading_dates",
    )
    occurrences: list[str] = []
    for path in sorted((repo / "src").rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                occurrences.append(path.relative_to(repo).as_posix() + ":" + token)
    assert occurrences == []

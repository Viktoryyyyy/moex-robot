from __future__ import annotations

from typing import Any, Dict

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as snapshot_runner


PROJECT = "MOEX_Bot"
MODE = "s7_3_chat_snapshot_mcp_reader"


def read_rub_analysis_snapshot_for_mcp() -> Dict[str, Any]:
    """Return the canonical persisted chat-analysis snapshot without refreshing sources."""

    snapshot, _path = snapshot_runner.read_current_snapshot()
    return dict(snapshot)


__all__ = ["MODE", "PROJECT", "read_rub_analysis_snapshot_for_mcp"]

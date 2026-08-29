#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from src.moex_research.consumers.usdrubf_chat_snapshot_consumer import (
    load_analysis_chat_snapshot,
)


mcp = FastMCP("moex-rub-analysis-snapshot")


@mcp.tool()
def rub_analysis_snapshot() -> dict[str, Any]:
    """Return only the canonical persisted RUB Intelligence analysis snapshot.

    This tool intentionally exposes no direct MOEX/news/macro fetch functions and
    performs no scenario, BUY/SELL/OUT, broker, or Telegram action.
    """

    return load_analysis_chat_snapshot()


if __name__ == "__main__":
    mcp.run(transport="stdio")

#!/usr/bin/env python3
from typing import Any, Dict

from mcp.server.fastmcp import FastMCP

from src.moex_research.runners.usdrubf_s7_3_chat_snapshot_mcp_adapter import (
    read_rub_analysis_snapshot_for_mcp,
)


mcp = FastMCP("moex-rub-analysis-snapshot")


@mcp.tool()
def ping() -> str:
    return "pong from moex_rub_analysis_snapshot_mcp"


@mcp.tool()
def rub_analysis_snapshot_current() -> Dict[str, Any]:
    """Read the canonical persisted RUB Intelligence snapshot; never refresh sources."""
    return read_rub_analysis_snapshot_for_mcp()


if __name__ == "__main__":
    mcp.run(transport="stdio")

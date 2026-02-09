#!/usr/bin/env python3
import asyncio
import os
from pathlib import Path

from agents import Agent, Runner
from agents.mcp import MCPServerStdio
from agents.model_settings import ModelSettings


async def main() -> None:
    root = Path(__file__).resolve().parent
    server_script = root / "mcp_moex_server.py"

    if not server_script.exists():
        raise FileNotFoundError(f"Не найден mcp_moex_server.py по пути {server_script}")

    # Поднимаем MCP-сервер MOEX по stdio
    async with MCPServerStdio(
        name="moex_mcp_stdio",
        params={
            "command": "python",
            "args": [str(server_script)],
        },
        cache_tools_list=True,
    ) as mcp_server:
        # Агент, который умеет пользоваться MCP-инструментами
        agent = Agent(
            name="MOEX_MCP_Agent",
            instructions=(
                "Ты — помощник для работы с MOEX.\n"
                "Всегда используй доступные MCP-инструменты (fo_5m_day, fo_marketdata), "
                "чтобы получить данные, затем кратко отвечай на запрос пользователя."
            ),
            mcp_servers=[mcp_server],
            model_settings=ModelSettings(
                tool_choice="auto",
            ),
        )

        # Тестовый запрос: вызвать fo_5m_day и посчитать количество баров
        query = (
            "Вызови инструмент fo_5m_day для тикера SiZ5 и даты 2025-11-13, "
            "посчитай количество 5-минутных баров и ответь только числом."
        )

        result = await Runner.run(agent, query)
        print("=== FINAL OUTPUT ===")
        print(result.final_output)


if __name__ == "__main__":
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY не задан в окружении")
    asyncio.run(main())

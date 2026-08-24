from __future__ import annotations

from typing import Any

from moex_data.futures import front_next_binding as binding


class _Response:
    url = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        rows = [[f"X{index}", "RFUD", "2026-12-17"] for index in range(490)]
        rows.extend(
            [
                ["SiU6", "RFUD", "2026-09-17"],
                ["SiZ6", "RFUD", "2026-12-17"],
                ["CRU6", "RFUD", "2026-09-17"],
                ["CRZ6", "RFUD", "2026-12-17"],
            ]
        )
        return {
            "securities": {
                "columns": ["SECID", "BOARDID", "LASTTRADEDATE"],
                "data": rows,
            }
        }


def test_reference_endpoint_is_consumed_as_one_complete_securities_block(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_get(url: str, *, params: dict[str, object], timeout: float, headers: dict[str, str]):
        calls.append(dict(params))
        return _Response()

    monkeypatch.setattr(binding, "current_moscow_date", lambda: "2026-08-24")
    monkeypatch.setattr(binding.requests, "get", fake_get)

    frame, _, _ = binding.fetch_reference_frame(as_of_date="2026-08-24")

    assert len(frame.index) == 494
    assert len(calls) == 1
    assert "start" not in calls[0]
    assert "securities.start" not in calls[0]

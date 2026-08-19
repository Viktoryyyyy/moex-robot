from __future__ import annotations

import pytest

from moex_data.futures import materialize_futoi_instrument as target


def test_fetch_exact_uses_authenticated_apim_full_intraday_futoi_block(monkeypatch) -> None:
    calls = []

    def fake_fetch(base_url, path, params, block, timeout, use_apim):
        calls.append((base_url, path, params, block, use_apim))
        return target.pd.DataFrame(
            [
                {
                    "sess_id": 1,
                    "seqnum": 100,
                    "tradedate": "2026-08-17",
                    "tradetime": "23:45:00",
                    "ticker": "USDRUBF",
                    "clgroup": "FIZ",
                    "pos": 9.0,
                    "pos_long": 99.0,
                    "pos_short": -90.0,
                    "pos_long_num": 10,
                    "pos_short_num": 9,
                    "systime": "2026-08-17 23:46:00",
                },
                {
                    "sess_id": 1,
                    "seqnum": 101,
                    "tradedate": "2026-08-17",
                    "tradetime": "23:50:00",
                    "ticker": "USDRUBF",
                    "clgroup": "FIZ",
                    "pos": 10.0,
                    "pos_long": 100.0,
                    "pos_short": -90.0,
                    "pos_long_num": 10,
                    "pos_short_num": 9,
                    "systime": "2026-08-17 23:51:00",
                },
            ]
        )

    monkeypatch.setenv("MOEX_API_KEY", "test-token")
    monkeypatch.setattr(target.availability, "fetch_paged_frame", fake_fetch)

    frame, source_url = target._fetch_exact(
        "usdrubf", "2026-08-17", 5.0, "https://apim.moex.com"
    )

    assert len(frame) == 2
    assert "tradedate" in frame.columns
    assert calls == [(
        "https://apim.moex.com",
        "/iss/analyticalproducts/futoi/securities/usdrubf.json",
        {"from": "2026-08-17", "till": "2026-08-17", "latest": 0},
        "futoi",
        True,
    )]
    assert source_url.startswith("https://apim.moex.com/")


def test_fetch_exact_fails_closed_without_apim_token(monkeypatch) -> None:
    monkeypatch.delenv("MOEX_API_KEY", raising=False)
    with pytest.raises(ValueError, match="MOEX_API_KEY is required for FUTOI APIM"):
        target._fetch_exact("usdrubf", "2026-08-17", 5.0, None)


def test_fetch_exact_rejects_error_message_payload(monkeypatch) -> None:
    monkeypatch.setenv("MOEX_API_KEY", "test-token")
    monkeypatch.setattr(
        target.availability,
        "fetch_paged_frame",
        lambda *args, **kwargs: target.pd.DataFrame([{"ERROR_MESSAGE": "not available"}]),
    )
    with pytest.raises(ValueError, match="ERROR_MESSAGE"):
        target._fetch_exact("usdrubf", "2026-08-17", 5.0, "https://apim.moex.com")

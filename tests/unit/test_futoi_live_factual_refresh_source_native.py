from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from moex_data.futures import futoi_live_factual_refresh_source_native as factual


def _raw_frame(trade_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sess_id": 1,
                "seqnum": 1,
                "tradedate": trade_date,
                "tradetime": "23:50:00",
                "ticker": "si",
                "clgroup": "FIZ",
                "pos": 20,
                "pos_long": 100,
                "pos_short": -80,
                "pos_long_num": 10,
                "pos_short_num": 11,
                "systime": trade_date + " 23:55:00",
            },
            {
                "sess_id": 1,
                "seqnum": 1,
                "tradedate": trade_date,
                "tradetime": "23:50:00",
                "ticker": "si",
                "clgroup": "YUR",
                "pos": -20,
                "pos_long": 80,
                "pos_short": -100,
                "pos_long_num": 12,
                "pos_short_num": 13,
                "systime": trade_date + " 23:55:00",
            },
        ]
    )


def _patch_binding(monkeypatch) -> None:
    monkeypatch.setattr(
        factual,
        "_binding",
        lambda: {"futoi.source_id": factual.SOURCE_ID, "futoi.ticker": "si"},
    )


def _patch_validation_passthrough(monkeypatch) -> None:
    monkeypatch.setattr(
        factual.materializer,
        "_validate_required_source_identifiers",
        lambda frame: frame,
    )
    monkeypatch.setattr(
        factual.materializer,
        "_validate_raw_source_rows",
        lambda frame, trade_date, ticker: frame,
    )


def test_source_native_freshness_uses_exact_through_date_when_data_exists(monkeypatch) -> None:
    _patch_binding(monkeypatch)
    _patch_validation_passthrough(monkeypatch)

    def fetch(ticker, trade_date, timeout, apim_base_url):
        del ticker, timeout, apim_base_url
        return _raw_frame(trade_date), "https://apim.example/futoi/si.json"

    monkeypatch.setattr(factual.materializer, "_fetch_exact", fetch)

    target, observations = factual.discover_latest_source_trade_date("2026-08-28", timeout=1.0)

    assert target == "2026-08-28"
    assert observations == [
        {
            "trade_date": "2026-08-28",
            "status": "DATA",
            "row_count": 2,
            "source_url": "https://apim.example/futoi/si.json",
        }
    ]


def test_source_native_freshness_skips_only_explicit_weekend_empty(monkeypatch) -> None:
    _patch_binding(monkeypatch)
    _patch_validation_passthrough(monkeypatch)

    def fetch(ticker, trade_date, timeout, apim_base_url):
        del ticker, timeout, apim_base_url
        if trade_date == "2026-08-29":
            raise factual.materializer.FutoiMaterializationError(factual.EXPLICIT_EMPTY_ERROR)
        return _raw_frame(trade_date), "https://apim.example/futoi/si.json"

    monkeypatch.setattr(factual.materializer, "_fetch_exact", fetch)

    target, observations = factual.discover_latest_source_trade_date("2026-08-29", timeout=1.0)

    assert target == "2026-08-28"
    assert observations[0] == {"trade_date": "2026-08-29", "status": "EMPTY_WEEKEND"}
    assert observations[1]["status"] == "DATA"


def test_source_native_freshness_fails_closed_on_weekday_empty(monkeypatch) -> None:
    _patch_binding(monkeypatch)

    def fetch(ticker, trade_date, timeout, apim_base_url):
        del ticker, trade_date, timeout, apim_base_url
        raise factual.materializer.FutoiMaterializationError(factual.EXPLICIT_EMPTY_ERROR)

    monkeypatch.setattr(factual.materializer, "_fetch_exact", fetch)

    with pytest.raises(factual.FutoiSourceNativeRefreshError, match="empty on weekday 2026-08-28"):
        factual.discover_latest_source_trade_date("2026-08-28", timeout=1.0)


def test_source_native_freshness_fails_closed_on_probe_error(monkeypatch) -> None:
    _patch_binding(monkeypatch)

    def fetch(ticker, trade_date, timeout, apim_base_url):
        del ticker, trade_date, timeout, apim_base_url
        raise RuntimeError("transport failure")

    monkeypatch.setattr(factual.materializer, "_fetch_exact", fetch)

    with pytest.raises(factual.FutoiSourceNativeRefreshError, match="transport failure"):
        factual.discover_latest_source_trade_date("2026-08-29", timeout=1.0)


def test_weekend_policy_is_deterministic() -> None:
    assert date.fromisoformat("2026-08-29").weekday() == 5
    assert date.fromisoformat("2026-08-30").weekday() == 6

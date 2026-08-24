from __future__ import annotations

import pandas as pd
import pytest

from moex_data.currency.materialize_cets_tom_raw_5m import CetsTomMaterializationError, normalize_to_5m


def _one_minute_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"open": 80.0, "high": 80.1, "low": 79.9, "close": 80.05, "volume": 10, "value": 800, "begin": "2026-08-21 10:00:00", "end": "2026-08-21 10:00:59"},
            {"open": 80.05, "high": 80.2, "low": 80.0, "close": 80.1, "volume": 20, "value": 1601, "begin": "2026-08-21 10:01:00", "end": "2026-08-21 10:01:59"},
            {"open": 80.1, "high": 80.3, "low": 80.0, "close": 80.2, "volume": 30, "value": 2404, "begin": "2026-08-21 10:02:00", "end": "2026-08-21 10:02:59"},
            {"open": 80.2, "high": 80.4, "low": 80.1, "close": 80.3, "volume": 40, "value": 3210, "begin": "2026-08-21 10:03:00", "end": "2026-08-21 10:03:59"},
            {"open": 80.3, "high": 80.5, "low": 80.2, "close": 80.4, "volume": 50, "value": 4020, "begin": "2026-08-21 10:04:00", "end": "2026-08-21 10:04:59"},
        ]
    )


def test_cets_tom_resamples_and_preserves_identity() -> None:
    result = normalize_to_5m(
        _one_minute_frame(),
        trade_date="2026-08-21",
        instrument_id="usd_tom",
        secid="USD000UTSTOM",
        source_url="https://iss.moex.com/example",
    )
    assert not result.empty
    assert set(result["instrument_id"]) == {"usd_tom"}
    assert set(result["secid"]) == {"USD000UTSTOM"}
    assert set(result["source_id"]) == {"moex_iss_cets_tom_1m"}
    assert result["ts"].is_monotonic_increasing
    assert float(result["volume"].sum()) == 150.0
    assert float(result["high"].max()) == 80.5
    assert float(result["low"].min()) == 79.9


def test_cets_tom_rejects_wrong_registry_secid() -> None:
    with pytest.raises(CetsTomMaterializationError, match="registry binding"):
        normalize_to_5m(
            _one_minute_frame(),
            trade_date="2026-08-21",
            instrument_id="usd_tom",
            secid="CNYRUB_TOM",
            source_url="https://iss.moex.com/example",
        )


def test_cets_tom_rejects_missing_candle_schema() -> None:
    with pytest.raises(CetsTomMaterializationError, match="schema missing"):
        normalize_to_5m(
            pd.DataFrame([{"open": 1.0}]),
            trade_date="2026-08-21",
            instrument_id="cny_tom",
            secid="CNYRUB_TOM",
            source_url="https://iss.moex.com/example",
        )

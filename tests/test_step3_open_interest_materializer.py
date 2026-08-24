from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures.materialize_open_interest_instrument import OpenInterestMaterializationError, normalize_open_interest


def _tradestats_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"SECID": "SiU6", "TRADEDATE": "2026-08-21", "TRADETIME": "10:00:00", "OI_OPEN": 1000, "OI_HIGH": 1020, "OI_LOW": 995, "OI_CLOSE": 1010},
            {"SECID": "SiU6", "TRADEDATE": "2026-08-21", "TRADETIME": "10:05:00", "OI_OPEN": 1010, "OI_HIGH": 1030, "OI_LOW": 1005, "OI_CLOSE": 1025},
        ]
    )


def test_open_interest_normalization_preserves_exact_source_semantics() -> None:
    result = normalize_open_interest(
        _tradestats_frame(),
        trade_date="2026-08-21",
        instrument_id="si_front_contract",
        secid="SiU6",
        source_url="https://apim.moex.com/example",
    )
    assert list(result["oi_open"]) == [1000, 1010]
    assert list(result["oi_close"]) == [1010, 1025]
    assert set(result["source_id"]) == {"moex_algopack_fo_open_interest_5m"}
    assert set(result["secid"]) == {"SiU6"}
    assert result["ts"].is_monotonic_increasing


def test_open_interest_rejects_invalid_range() -> None:
    frame = _tradestats_frame()
    frame.loc[0, "OI_HIGH"] = 990
    with pytest.raises(OpenInterestMaterializationError, match="OI high"):
        normalize_open_interest(
            frame,
            trade_date="2026-08-21",
            instrument_id="si_front_contract",
            secid="SiU6",
            source_url="https://apim.moex.com/example",
        )


def test_open_interest_rejects_missing_required_source_field() -> None:
    frame = _tradestats_frame().drop(columns=["OI_CLOSE"])
    with pytest.raises(OpenInterestMaterializationError, match="missing required column"):
        normalize_open_interest(
            frame,
            trade_date="2026-08-21",
            instrument_id="si_front_contract",
            secid="SiU6",
            source_url="https://apim.moex.com/example",
        )

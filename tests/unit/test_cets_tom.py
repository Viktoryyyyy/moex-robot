import pandas as pd
import pytest

from moex_data.iss.cets_tom import CetsTomError, normalize_1m_to_5m, validate_identity


def test_tom_identity_is_explicit():
    assert validate_identity("usd_tom", "USD000UTSTOM") == ("usd_tom", "USD000UTSTOM")
    assert validate_identity("cny_tom", "CNYRUB_TOM") == ("cny_tom", "CNYRUB_TOM")
    with pytest.raises(CetsTomError):
        validate_identity("usd_tom", "CNYRUB_TOM")


def test_tom_1m_resamples_to_5m_without_forward_fill():
    frame = pd.DataFrame(
        {
            "end": ["2026-08-21 10:01:00", "2026-08-21 10:02:00", "2026-08-21 10:05:00", "2026-08-21 10:06:00"],
            "open": [80.0, 80.1, 80.2, 80.4],
            "high": [80.2, 80.3, 80.5, 80.6],
            "low": [79.9, 80.0, 80.1, 80.3],
            "close": [80.1, 80.2, 80.4, 80.5],
            "volume": [1, 2, 3, 4],
            "value": [80.1, 160.4, 241.2, 322.0],
        }
    )
    result = normalize_1m_to_5m(frame, instrument_id="usd_tom", secid="USD000UTSTOM", trade_date="2026-08-21")
    assert len(result) == 2
    first = result.iloc[0]
    assert first["open"] == 80.0
    assert first["high"] == 80.5
    assert first["low"] == 79.9
    assert first["close"] == 80.4
    assert first["volume"] == 6
    assert result["source_id"].nunique() == 1

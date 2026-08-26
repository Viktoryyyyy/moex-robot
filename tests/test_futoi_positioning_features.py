from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures.materialize_futoi_positioning_features_d1 import BASE_FIELDS, build_features


def _eod(rows: int = 510) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=rows, freq="D")
    data = {
        "instrument_id": ["si_futures_family"] * rows,
        "trade_date": dates.strftime("%Y-%m-%d"),
        "snapshot_ts_utc": dates.tz_localize("UTC") + pd.Timedelta(hours=20),
        "availability_ts_utc": dates.tz_localize("UTC") + pd.Timedelta(hours=21),
    }
    for n, field in enumerate(BASE_FIELDS, start=1):
        data[field] = [float(i + n) for i in range(rows)]
    return pd.DataFrame(data)


def test_features_use_observation_lags_and_backward_rolling_windows() -> None:
    result = build_features(_eod(), instrument_id="si_futures_family")
    field = BASE_FIELDS[0]
    assert result.loc[0, f"{field}_chg_1obs"] != result.loc[0, f"{field}_chg_1obs"]
    assert result.loc[1, f"{field}_chg_1obs"] == pytest.approx(1.0)
    assert result.loc[5, f"{field}_chg_5obs"] == pytest.approx(5.0)
    assert pd.isna(result.loc[250, f"{field}_pctile_252obs"])
    assert result.loc[251, f"{field}_pctile_252obs"] == pytest.approx(1.0)
    assert pd.isna(result.loc[502, f"{field}_pctile_504obs"])
    assert result.loc[503, f"{field}_pctile_504obs"] == pytest.approx(1.0)
    assert pd.notna(result.loc[251, f"{field}_zscore_252obs"])
    assert pd.notna(result.loc[503, f"{field}_zscore_504obs"])


def test_feature_builder_rejects_wrong_instrument_identity() -> None:
    with pytest.raises(ValueError, match="instrument_id mismatch"):
        build_features(_eod(), instrument_id="cr_futures_family")

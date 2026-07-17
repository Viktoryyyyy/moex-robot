from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from moex_research.features import brent_incremental_features as features


def _modeling_dataset() -> pd.DataFrame:
    count = features.EXPECTED_ELIGIBLE_IDENTITIES
    dates = pd.date_range("2024-01-02", periods=count, freq="D")
    index = np.arange(count, dtype=float)
    frame = pd.DataFrame(
        {
            "target_trade_date": dates,
            "target_instrument_id": features.EXPECTED_INSTRUMENT,
            "prior_trade_date": dates - pd.Timedelta(days=1),
            "target_phase_label": np.resize(np.asarray(features.CLASS_ORDER), count),
            "target_is_labeled": True,
            "target_source": features.TARGET_SOURCE,
        }
    )
    for offset, column in enumerate(features.M0_NUMERIC_FEATURES, 1):
        frame[column] = index / 100.0 + offset
    frame[features.M0_CATEGORICAL_FEATURES[0]] = np.resize(
        np.asarray(["above", "below", "flat"]), count
    )
    return frame


def _brent_matrix() -> pd.DataFrame:
    dataset = _modeling_dataset()
    count = len(dataset)
    index = np.arange(count, dtype=float)
    codes = np.where(index < 160, "BRH4", np.where(index < 320, "BRM4", "BRU4"))
    changed = pd.Series(codes).ne(pd.Series(codes).shift())
    changed.iloc[0] = False
    previous = pd.Series(codes).shift()
    open_values = 75.0 + index / 200.0
    close_values = open_values + np.sin(index / 11.0) * 0.7
    high_values = np.maximum(open_values, close_values) + 1.0 + index / 10000.0
    low_values = np.minimum(open_values, close_values) - 1.0
    return pd.DataFrame(
        {
            "target_trade_date": dataset["target_trade_date"],
            "target_instrument_id": dataset["target_instrument_id"],
            "prior_trade_date": dataset["prior_trade_date"],
            "brent_contract_code": codes,
            "brent_contract_changed": changed,
            "brent_previous_contract_code": previous,
            "brent_trade_date": dataset["prior_trade_date"],
            "brent_open": open_values,
            "brent_high": high_values,
            "brent_low": low_values,
            "brent_close": close_values,
            "brent_volume": 1000.0 + index * 3.0,
            "brent_value": 100000.0 + index * 101.0,
            "brent_days_to_expiration": 30,
            "brent_retrieved_at_utc": "2026-07-16T00:00:00+00:00",
            "brent_candle_payload_sha256": "a" * 64,
            "brent_candle_route": "https://iss.moex.com/example",
        }
    )


def _build() -> features.BrentFeatureBuildResult:
    return features.build_brent_feature_matrices(_modeling_dataset(), _brent_matrix())


def test_exact_six_feature_inventory() -> None:
    assert features.BRENT_FEATURES == (
        "ext_brent_log_close",
        "ext_brent_intraday_return",
        "ext_brent_range_pct",
        "ext_brent_close_location",
        "ext_log1p_brent_volume",
        "ext_log1p_brent_value",
    )
    assert len(features.PRICE_ACTION_FEATURES) == 4
    assert len(features.ACTIVITY_FEATURES) == 2


def test_formulas_match_contract() -> None:
    source = _brent_matrix()
    built = features.build_brent_feature_matrices(_modeling_dataset(), source)
    row = built.brent_features.iloc[0]
    raw = source.iloc[0]
    assert row["ext_brent_log_close"] == pytest.approx(np.log(raw["brent_close"]))
    assert row["ext_brent_intraday_return"] == pytest.approx(
        raw["brent_close"] / raw["brent_open"] - 1.0
    )
    assert row["ext_brent_range_pct"] == pytest.approx(
        (raw["brent_high"] - raw["brent_low"]) / raw["brent_open"]
    )
    assert row["ext_brent_close_location"] == pytest.approx(
        (raw["brent_close"] - raw["brent_low"])
        / (raw["brent_high"] - raw["brent_low"])
    )
    assert row["ext_log1p_brent_volume"] == pytest.approx(
        np.log1p(raw["brent_volume"])
    )
    assert row["ext_log1p_brent_value"] == pytest.approx(
        np.log1p(raw["brent_value"])
    )


def test_zero_range_close_location_is_exactly_one_half() -> None:
    source = _brent_matrix()
    source.loc[0, ["brent_open", "brent_high", "brent_low", "brent_close"]] = 80.0
    built = features.build_brent_feature_matrices(_modeling_dataset(), source)
    assert built.brent_features.loc[0, "ext_brent_close_location"] == 0.5


def test_non_positive_open_fails_closed() -> None:
    source = _brent_matrix()
    source.loc[0, "brent_open"] = 0.0
    with pytest.raises(features.BrentIncrementalFeatureError, match="open"):
        features.build_brent_feature_matrices(_modeling_dataset(), source)


def test_non_positive_close_fails_closed() -> None:
    source = _brent_matrix()
    source.loc[0, "brent_close"] = 0.0
    with pytest.raises(features.BrentIncrementalFeatureError, match="close"):
        features.build_brent_feature_matrices(_modeling_dataset(), source)


def test_negative_volume_fails_closed() -> None:
    source = _brent_matrix()
    source.loc[0, "brent_volume"] = -1.0
    with pytest.raises(features.BrentIncrementalFeatureError, match="volume"):
        features.build_brent_feature_matrices(_modeling_dataset(), source)


def test_negative_value_fails_closed() -> None:
    source = _brent_matrix()
    source.loc[0, "brent_value"] = -1.0
    with pytest.raises(features.BrentIncrementalFeatureError, match="value"):
        features.build_brent_feature_matrices(_modeling_dataset(), source)


@pytest.mark.parametrize("value", [np.nan, np.inf, -np.inf])
def test_non_finite_source_values_fail_closed(value: float) -> None:
    source = _brent_matrix()
    source.loc[0, "brent_high"] = value
    with pytest.raises(features.BrentIncrementalFeatureError, match="finite"):
        features.build_brent_feature_matrices(_modeling_dataset(), source)


def test_identity_duplication_fails_closed() -> None:
    source = _brent_matrix()
    source.loc[1, list(features.IDENTITY_COLUMNS)] = source.loc[
        0, list(features.IDENTITY_COLUMNS)
    ].to_numpy()
    with pytest.raises(features.BrentIncrementalFeatureError, match="duplicate"):
        features.build_brent_feature_matrices(_modeling_dataset(), source)


def test_identity_mismatch_fails_closed() -> None:
    source = _brent_matrix()
    source.loc[0, "target_trade_date"] = "2030-01-01"
    with pytest.raises(features.BrentIncrementalFeatureError, match="identity or order"):
        features.build_brent_feature_matrices(_modeling_dataset(), source)


def test_target_day_candle_fails_closed() -> None:
    source = _brent_matrix()
    source.loc[0, "brent_trade_date"] = source.loc[0, "target_trade_date"]
    with pytest.raises(features.BrentIncrementalFeatureError, match="target-day"):
        features.build_brent_feature_matrices(_modeling_dataset(), source)


def test_wrong_prior_trade_date_fails_closed() -> None:
    source = _brent_matrix()
    source.loc[0, "brent_trade_date"] = "2023-12-01"
    with pytest.raises(features.BrentIncrementalFeatureError, match="prior_trade_date"):
        features.build_brent_feature_matrices(_modeling_dataset(), source)


def test_forbidden_audit_fields_cannot_enter_feature_matrices() -> None:
    built = _build()
    allowed = set(features.M0_NUMERIC_FEATURES) | set(
        features.M0_CATEGORICAL_FEATURES
    ) | set(features.BRENT_FEATURES)
    for matrix in built.matrices.values():
        assert set(matrix.columns) <= allowed
        assert not set(matrix.columns) & {
            "target_phase_label",
            "target_trade_date",
            "prior_trade_date",
            "brent_contract_code",
            "brent_retrieved_at_utc",
            "brent_candle_payload_sha256",
            "brent_candle_route",
        }


def test_roll_flag_is_audit_only_and_not_a_feature() -> None:
    assert all(
        "brent_contract_changed" not in columns
        for columns in features.MATRIX_NUMERIC_FEATURES.values()
    )


def test_no_cross_contract_return_feature_exists() -> None:
    inventory = {
        item for columns in features.MATRIX_NUMERIC_FEATURES.values() for item in columns
    }
    assert not any("cross_contract" in item or "back_adjusted" in item for item in inventory)

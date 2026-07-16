from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from moex_research.runners import usdrubf_phase8_3_external_factor_builder as builder


def _modeling_dataset() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=472)
    index = np.arange(472, dtype=float)
    frame = pd.DataFrame(
        {
            "target_trade_date": dates,
            "target_instrument_id": "forts.usdrubf",
            "target_phase_label": np.resize(np.asarray(["B", "S", "OUT"]), 472),
            "target_is_labeled": True,
            "target_source": "manual_phase_labels_v1",
        }
    )
    for offset, feature in enumerate(builder.M0_NUMERIC_FEATURES, 1):
        frame[feature] = index + offset + np.sin(index / (offset + 1))
    frame["lag1_ema_3_19_state"] = np.resize(
        np.asarray(["ema3_above_ema19", "ema3_below_ema19"]), 472
    )
    return frame


def _external_matrix() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=472)
    index = np.arange(472, dtype=float)
    minimum = 10.0 + index / 1000.0
    p25 = minimum + 0.10 + (index % 5) / 1000.0
    p75 = p25 + 0.20 + (index % 7) / 1000.0
    maximum = p75 + 0.30 + (index % 11) / 1000.0
    key_rate = 12.0 + index / 100.0
    ruonia = 11.0 + index / 120.0
    return pd.DataFrame(
        {
            "target_trade_date": dates,
            "target_instrument_id": "forts.usdrubf",
            "key_rate_pct": key_rate,
            "ruonia_minus_key_rate_pp": ruonia - key_rate,
            "ruonia_minimum_rate_pct": minimum,
            "ruonia_percentile_25_rate_pct": p25,
            "ruonia_percentile_75_rate_pct": p75,
            "ruonia_maximum_rate_pct": maximum,
            "key_rate_age_calendar_days": index % 180,
            "ruonia_transaction_volume_rub_bn": 100.0 + index,
            "ruonia_transaction_count": 1000.0 + index * 2,
            "ruonia_participant_count": 50.0 + index % 100,
        }
    )


def test_exact_472_identity_join_preserves_order() -> None:
    result = builder.build_external_feature_matrices(
        _modeling_dataset(), _external_matrix()
    )
    assert len(result.eligible) == 472
    assert result.external_features.loc[:, builder.IDENTITY_COLUMNS].equals(
        result.eligible.loc[:, builder.IDENTITY_COLUMNS]
    )
    assert tuple(result.matrices) == (
        "E1_M0_PLUS_EXTERNAL_FULL",
        "E2_M0_PLUS_POLICY_AND_MONEY_MARKET",
        "E3_M0_PLUS_RUONIA_ACTIVITY",
        "E4_EXTERNAL_ONLY",
    )


def test_duplicate_identity_fails() -> None:
    external = _external_matrix()
    external.loc[1, list(builder.IDENTITY_COLUMNS)] = external.loc[
        0, list(builder.IDENTITY_COLUMNS)
    ].to_numpy()
    with pytest.raises(builder.Phase83ExternalFactorBuilderError, match="duplicate"):
        builder.build_external_feature_matrices(_modeling_dataset(), external)


def test_missing_identity_fails() -> None:
    with pytest.raises(builder.Phase83ExternalFactorBuilderError, match="missing|extra"):
        builder.build_external_feature_matrices(
            _modeling_dataset(), _external_matrix().iloc[:-1].copy()
        )


def test_extra_identity_fails() -> None:
    external = _external_matrix()
    extra = external.iloc[[-1]].copy()
    extra["target_trade_date"] = pd.Timestamp("2030-01-01")
    external = pd.concat([external, extra], ignore_index=True)
    with pytest.raises(builder.Phase83ExternalFactorBuilderError, match="missing|extra"):
        builder.build_external_feature_matrices(_modeling_dataset(), external)


def test_nonfinite_external_value_fails() -> None:
    external = _external_matrix()
    external.loc[4, "key_rate_pct"] = np.inf
    with pytest.raises(builder.Phase83ExternalFactorBuilderError, match="finite"):
        builder.build_external_feature_matrices(_modeling_dataset(), external)


def test_negative_transaction_activity_fails() -> None:
    external = _external_matrix()
    external.loc[4, "ruonia_transaction_count"] = -1
    with pytest.raises(builder.Phase83ExternalFactorBuilderError, match="activity"):
        builder.build_external_feature_matrices(_modeling_dataset(), external)


def test_negative_key_rate_age_fails() -> None:
    external = _external_matrix()
    external.loc[4, "key_rate_age_calendar_days"] = -1
    with pytest.raises(builder.Phase83ExternalFactorBuilderError, match="key-rate age"):
        builder.build_external_feature_matrices(_modeling_dataset(), external)


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("ruonia_minimum_rate_pct", "ruonia_maximum_rate_pct"),
        ("ruonia_percentile_25_rate_pct", "ruonia_percentile_75_rate_pct"),
    ],
)
def test_negative_range_or_iqr_fails(left: str, right: str) -> None:
    external = _external_matrix()
    external.loc[4, right] = external.loc[4, left] - 0.01
    with pytest.raises(builder.Phase83ExternalFactorBuilderError, match="range or IQR"):
        builder.build_external_feature_matrices(_modeling_dataset(), external)


def test_all_eight_formulas_are_exact() -> None:
    external = _external_matrix()
    result = builder.build_external_feature_matrices(_modeling_dataset(), external)
    observed = result.external_features.loc[:, builder.EXTERNAL_FEATURES]
    assert np.allclose(observed["ext_key_rate_pct"], external["key_rate_pct"])
    assert np.allclose(
        observed["ext_ruonia_minus_key_rate_pp"],
        external["ruonia_minus_key_rate_pp"],
    )
    assert np.allclose(
        observed["ext_ruonia_rate_range_pp"],
        external["ruonia_maximum_rate_pct"]
        - external["ruonia_minimum_rate_pct"],
    )
    assert np.allclose(
        observed["ext_ruonia_rate_iqr_pp"],
        external["ruonia_percentile_75_rate_pct"]
        - external["ruonia_percentile_25_rate_pct"],
    )
    expected_logs = {
        "ext_log1p_key_rate_age_days": "key_rate_age_calendar_days",
        "ext_log1p_ruonia_transaction_volume_rub_bn": (
            "ruonia_transaction_volume_rub_bn"
        ),
        "ext_log1p_ruonia_transaction_count": "ruonia_transaction_count",
        "ext_log1p_ruonia_participant_count": "ruonia_participant_count",
    }
    for feature, source in expected_logs.items():
        assert np.allclose(observed[feature], np.log1p(external[source]))


def test_no_target_or_probability_field_enters_features() -> None:
    external = _external_matrix().assign(
        target_phase_label="S", probability_B=0.99, candidate_y_pred="S"
    )
    result = builder.build_external_feature_matrices(_modeling_dataset(), external)
    for matrix in result.matrices.values():
        assert not any(
            token in column.lower()
            for column in matrix.columns
            for token in ("target", "probability", "candidate_y_pred")
        )


def test_exact_E1_E2_E3_E4_inventories() -> None:
    result = builder.build_external_feature_matrices(
        _modeling_dataset(), _external_matrix()
    )
    for matrix_id, matrix in result.matrices.items():
        expected = (
            *builder.MATRIX_NUMERIC_FEATURES[matrix_id],
            *builder.MATRIX_CATEGORICAL_FEATURES[matrix_id],
        )
        assert tuple(matrix.columns) == expected
    assert tuple(
        feature
        for feature in result.matrices["E4_EXTERNAL_ONLY"].columns
    ) == builder.EXTERNAL_FEATURES


def test_E0_is_not_built_as_candidate_feature_matrix() -> None:
    result = builder.build_external_feature_matrices(
        _modeling_dataset(), _external_matrix()
    )
    assert "E0_FROZEN_PHASE7_2_CONTROL" not in result.matrices
    assert builder.MATRIX_ROLES["E0_FROZEN_PHASE7_2_CONTROL"] == (
        "immutable_historical_control"
    )

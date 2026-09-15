from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from moex_research.features.oil_fx_rub_v1_features import (
    LABEL_COLUMNS,
    OilFxRubFeatureError,
    build_feature_frame,
    build_forward_return_labels,
)


def _identities() -> pd.DataFrame:
    targets = pd.bdate_range("2024-08-05", periods=472)
    priors = pd.bdate_range("2024-08-02", periods=472)
    return pd.DataFrame(
        {
            "target_trade_date": targets.strftime("%Y-%m-%d"),
            "target_instrument_id": "forts.usdrubf",
            "prior_trade_date": priors.strftime("%Y-%m-%d"),
        }
    )


def _brent() -> pd.DataFrame:
    frame = _identities()
    x = np.arange(len(frame), dtype=float)
    frame["brent_contract_code"] = "BRX4"
    frame["brent_trade_date"] = frame["prior_trade_date"]
    frame["brent_open"] = 70.0 + x * 0.01
    frame["brent_high"] = frame["brent_open"] + 1.0
    frame["brent_low"] = frame["brent_open"] - 1.0
    frame["brent_close"] = frame["brent_open"] + 0.2
    frame["brent_volume"] = 1000.0 + x
    return frame


def _cnyrubf() -> pd.DataFrame:
    frame = _identities()
    x = np.arange(len(frame), dtype=float)
    frame["cnyrubf_security_id"] = "CNYRUBF"
    frame["cnyrubf_trade_date"] = frame["prior_trade_date"]
    frame["cnyrubf_open"] = 11.0 + x * 0.001
    frame["cnyrubf_high"] = frame["cnyrubf_open"] + 0.05
    frame["cnyrubf_low"] = frame["cnyrubf_open"] - 0.05
    frame["cnyrubf_close"] = frame["cnyrubf_open"] + 0.01
    frame["cnyrubf_volume"] = 5000.0 + x
    frame["cnyrubf_volume_imbalance"] = 0.1
    frame["cnyrubf_open_interest_close"] = 100000.0 + x
    return frame


def test_brent_roll_window_returns_are_structural_nulls() -> None:
    brent = _brent()
    brent.loc[10:, "brent_contract_code"] = "BRZ4"
    features = build_feature_frame(_identities(), brent, mode="brent_only")
    assert pd.isna(features.loc[10, "ext_brent_same_contract_close_return_1session"])
    assert pd.isna(features.loc[12, "ext_brent_same_contract_close_return_3session"])
    assert pd.isna(features.loc[14, "ext_brent_same_contract_close_return_5session"])
    assert np.isfinite(features.loc[15, "ext_brent_same_contract_close_return_5session"])


def test_full_mode_builds_cny_and_interaction_features() -> None:
    features = build_feature_frame(
        _identities(), _brent(), mode="oil_fx_full", cnyrubf_matrix=_cnyrubf()
    )
    assert "ext_cnyrubf_close_return_5session" in features
    assert "ext_brent_cnyrubf_intraday_interaction" in features
    assert features.loc[1:, "ext_cnyrubf_close_return_1session"].notna().all()


def test_wrong_cnyrubf_security_is_rejected() -> None:
    cny = _cnyrubf()
    cny.loc[0, "cnyrubf_security_id"] = "CNYRUB_TOM"
    with pytest.raises(OilFxRubFeatureError, match="security identity mismatch"):
        build_feature_frame(
            _identities(), _brent(), mode="oil_fx_full", cnyrubf_matrix=cny
        )


def test_brent_only_refuses_cnyrubf_input() -> None:
    with pytest.raises(OilFxRubFeatureError, match="forbidden in brent_only"):
        build_feature_frame(
            _identities(), _brent(), mode="brent_only", cnyrubf_matrix=_cnyrubf()
        )


def test_forward_labels_are_separate_session_based_artifact() -> None:
    identities = _identities()
    all_dates = pd.bdate_range("2024-08-05", periods=490)
    close = 80.0 + np.arange(len(all_dates), dtype=float)
    panel = pd.DataFrame({"trade_date": all_dates, "close": close})
    labels = build_forward_return_labels(identities, panel)
    assert tuple(column for column in labels if column not in ("target_trade_date", "target_instrument_id")) == LABEL_COLUMNS
    assert labels.loc[0, "fwd_usdrubf_close_return_1session"] == pytest.approx(81.0 / 80.0 - 1.0)
    assert labels.loc[0, "fwd_usdrubf_close_return_10session"] == pytest.approx(90.0 / 80.0 - 1.0)

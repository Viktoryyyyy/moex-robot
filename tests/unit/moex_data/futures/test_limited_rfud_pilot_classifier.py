import pandas as pd

from moex_data.futures.limited_rfud_pilot_classifier import REQUIRED_CONTINUOUS_V1
from moex_data.futures.limited_rfud_pilot_classifier import classify_pilot


CONFIG = {
    "pilot_families": ["CR", "GD", "GL"],
    "continuous_v1_policy": REQUIRED_CONTINUOUS_V1,
}


def _normalized():
    return pd.DataFrame([
        {"secid": "CRZ6", "short_code": "CRZ6", "family_code": "CR", "board": "rfud", "engine": "futures", "market": "forts", "instrument_type": "ordinary_expiring_future", "expiration_date": "2026-12-15"},
        {"secid": "GDZ6", "short_code": "GDZ6", "family_code": "GD", "board": "rfud", "engine": "futures", "market": "forts", "instrument_type": "ordinary_expiring_future", "expiration_date": "2026-12-15"},
        {"secid": "GLZ6", "short_code": "GLZ6", "family_code": "GL", "board": "rfud", "engine": "futures", "market": "forts", "instrument_type": "ordinary_expiring_future", "expiration_date": "2026-12-15"},
        {"secid": "BRZ6", "short_code": "BRZ6", "family_code": "BR", "board": "rfud", "engine": "futures", "market": "forts", "instrument_type": "ordinary_expiring_future", "expiration_date": "2026-12-15"},
    ])


def _pass_frame(secids, status_col, status_value):
    return pd.DataFrame([{"secid": secid, status_col: status_value, "family_code": secid[:2], "board": "rfud"} for secid in secids])


def test_classifier_includes_only_selected_pilot_families_when_all_checks_pass():
    secids = ["CRZ6", "GDZ6", "GLZ6", "BRZ6"]
    result = classify_pilot(
        _normalized(),
        _pass_frame(secids, "mapping_status", "pass"),
        _pass_frame(secids, "availability_status", "available"),
        _pass_frame(secids, "availability_status", "available"),
        _pass_frame(secids, "liquidity_status", "pass"),
        _pass_frame(secids, "history_depth_status", "pass"),
        "2026-05-05",
        CONFIG,
    )

    assert result["family_code"].tolist() == ["CR", "GD", "GL"]
    assert set(result["classification_status"].tolist()) == {"included"}
    assert set(result["roll_policy_id"].tolist()) == {"expiration_minus_1_trading_session_v1"}
    assert set(result["adjustment_policy_id"].tolist()) == {"unadjusted_v1"}
    assert set(result["adjustment_factor"].tolist()) == {1.0}


def test_missing_futoi_defers_instrument_without_silent_include():
    secids = ["CRZ6", "GDZ6", "GLZ6"]
    futoi = _pass_frame(["CRZ6", "GDZ6"], "availability_status", "available")
    result = classify_pilot(
        _normalized(),
        _pass_frame(secids, "mapping_status", "pass"),
        _pass_frame(secids, "availability_status", "available"),
        futoi,
        _pass_frame(secids, "liquidity_status", "pass"),
        _pass_frame(secids, "history_depth_status", "pass"),
        "2026-05-05",
        CONFIG,
    )

    gl = result.loc[result["secid"] == "GLZ6"].iloc[0]
    assert gl["classification_status"] == "deferred"
    assert gl["deferral_reason"] == "futoi_unavailable"


def test_missing_expiration_anchor_defers_continuous_eligibility():
    normalized = _normalized()
    normalized.loc[normalized["secid"] == "GDZ6", "expiration_date"] = None
    secids = ["CRZ6", "GDZ6", "GLZ6"]
    result = classify_pilot(
        normalized,
        _pass_frame(secids, "mapping_status", "pass"),
        _pass_frame(secids, "availability_status", "available"),
        _pass_frame(secids, "availability_status", "available"),
        _pass_frame(secids, "liquidity_status", "pass"),
        _pass_frame(secids, "history_depth_status", "pass"),
        "2026-05-05",
        CONFIG,
    )

    gd = result.loc[result["secid"] == "GDZ6"].iloc[0]
    assert gd["classification_status"] == "deferred"
    assert gd["deferral_reason"] == "expiration_anchor_missing"
    assert gd["continuous_eligibility_status"] == "fail"

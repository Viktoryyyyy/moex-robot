from __future__ import annotations

import pandas as pd
import pytest

from moex_data.analytics.materialize_rub_basis_carry_5m import BasisCarryMaterializationError, build_basis_carry_frame


def _frame(instrument_id: str, closes: list[float], timestamps: list[str] | None = None) -> pd.DataFrame:
    ts = timestamps or ["2026-08-24T07:00:00Z", "2026-08-24T07:05:00Z"]
    return pd.DataFrame({
        "instrument_id": [instrument_id] * len(closes),
        "trade_date": ["2026-08-24"] * len(closes),
        "ts": ts,
        "close": closes,
    })


def _binding(root: str, role: str, instrument_id: str, secid: str, expiry: str) -> dict[str, str]:
    return {
        "root": root,
        "role": role,
        "instrument_id": instrument_id,
        "secid": secid,
        "as_of_date": "2026-08-24",
        "last_trade_date": expiry,
    }


def test_usd_basis_carry_normalizes_si_per_1000_usd() -> None:
    result = build_basis_carry_frame(
        instrument_id="usd_rub_basis_carry",
        trade_date="2026-08-24",
        spot_frame=_frame("usd_tom", [80.0, 80.1]),
        perpetual_frame=_frame("usdrubf_futures_family", [80.2, 80.3]),
        front_frame=_frame("si_front_contract", [81000.0, 81100.0]),
        next_frame=_frame("si_next_contract", [83000.0, 83100.0]),
        front_binding=_binding("Si", "front", "si_front_contract", "SiU6", "2026-09-17"),
        next_binding=_binding("Si", "next", "si_next_contract", "SiZ6", "2026-12-17"),
        build_ts="2026-08-24T12:00:00+00:00",
    )
    assert result["front_rate"].tolist() == [81.0, 81.1]
    assert result["next_rate"].tolist() == [83.0, 83.1]
    assert result["front_spot_basis_abs"].iloc[0] == pytest.approx(1.0)
    assert result["perpetual_spot_basis_bps"].iloc[0] == pytest.approx(25.0)
    assert result["front_spot_implied_carry_annualized"].iloc[0] == pytest.approx((81.0 / 80.0 - 1.0) * 365.0 / 24.0)
    assert result["alignment_policy"].eq("exact_timestamp_inner_join").all()


def test_cny_basis_carry_does_not_apply_si_scale() -> None:
    result = build_basis_carry_frame(
        instrument_id="cny_rub_basis_carry",
        trade_date="2026-08-24",
        spot_frame=_frame("cny_tom", [12.0, 12.01]),
        perpetual_frame=_frame("cnyrubf_futures_family", [12.1, 12.11]),
        front_frame=_frame("cr_front_contract", [12.2, 12.21]),
        next_frame=_frame("cr_next_contract", [12.4, 12.41]),
        front_binding=_binding("CR", "front", "cr_front_contract", "CRU6", "2026-09-17"),
        next_binding=_binding("CR", "next", "cr_next_contract", "CRZ6", "2026-12-17"),
    )
    assert result["front_rate"].iloc[0] == pytest.approx(12.2)
    assert result["next_rate"].iloc[0] == pytest.approx(12.4)


def test_exact_timestamp_intersection_only() -> None:
    result = build_basis_carry_frame(
        instrument_id="cny_rub_basis_carry",
        trade_date="2026-08-24",
        spot_frame=_frame("cny_tom", [12.0, 12.1]),
        perpetual_frame=_frame("cnyrubf_futures_family", [12.0], ["2026-08-24T07:05:00Z"]),
        front_frame=_frame("cr_front_contract", [12.0, 12.1]),
        next_frame=_frame("cr_next_contract", [12.0, 12.1]),
        front_binding=_binding("CR", "front", "cr_front_contract", "CRU6", "2026-09-17"),
        next_binding=_binding("CR", "next", "cr_next_contract", "CRZ6", "2026-12-17"),
    )
    assert len(result) == 1
    assert result["ts"].iloc[0] == pd.Timestamp("2026-08-24T07:05:00Z")


def test_empty_exact_intersection_fails_closed() -> None:
    with pytest.raises(BasisCarryMaterializationError, match="intersection is empty"):
        build_basis_carry_frame(
            instrument_id="cny_rub_basis_carry",
            trade_date="2026-08-24",
            spot_frame=_frame("cny_tom", [12.0], ["2026-08-24T07:00:00Z"]),
            perpetual_frame=_frame("cnyrubf_futures_family", [12.0], ["2026-08-24T07:05:00Z"]),
            front_frame=_frame("cr_front_contract", [12.0], ["2026-08-24T07:10:00Z"]),
            next_frame=_frame("cr_next_contract", [12.0], ["2026-08-24T07:15:00Z"]),
            front_binding=_binding("CR", "front", "cr_front_contract", "CRU6", "2026-09-17"),
            next_binding=_binding("CR", "next", "cr_next_contract", "CRZ6", "2026-12-17"),
        )


def test_expiry_ordering_fails_closed() -> None:
    with pytest.raises(BasisCarryMaterializationError, match="next expiry must be after front expiry"):
        build_basis_carry_frame(
            instrument_id="usd_rub_basis_carry",
            trade_date="2026-08-24",
            spot_frame=_frame("usd_tom", [80.0, 80.1]),
            perpetual_frame=_frame("usdrubf_futures_family", [80.0, 80.1]),
            front_frame=_frame("si_front_contract", [80000.0, 80100.0]),
            next_frame=_frame("si_next_contract", [81000.0, 81100.0]),
            front_binding=_binding("Si", "front", "si_front_contract", "SiU6", "2026-12-17"),
            next_binding=_binding("Si", "next", "si_next_contract", "SiZ6", "2026-09-17"),
        )

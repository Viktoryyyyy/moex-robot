from __future__ import annotations

import pandas as pd
import pytest

from moex_research.runners import usdrubf_phase8_7a_futoi_si_runtime as runtime


def _modeling_with_full_history() -> pd.DataFrame:
    contract_dates = list(
        pd.date_range(
            runtime.FROZEN_ELIGIBLE_TARGET_DATE_FROM,
            "2026-06-10",
            periods=471,
        ).normalize()
    ) + [pd.Timestamp(runtime.FROZEN_ELIGIBLE_TARGET_DATE_TILL)]
    historical_dates = list(
        pd.date_range(end="2024-08-04", periods=576, freq="D")
    )
    target_dates = historical_dates + contract_dates
    return pd.DataFrame(
        {
            "target_trade_date": target_dates,
            "target_instrument_id": [runtime.validation.TARGET_INSTRUMENT_ID]
            * len(target_dates),
            "prior_trade_date": [value - pd.Timedelta(days=1) for value in target_dates],
        }
    )


def _validation_predictions(modeling: pd.DataFrame) -> pd.DataFrame:
    eligible = modeling.loc[
        pd.to_datetime(modeling.target_trade_date).between(
            runtime.FROZEN_ELIGIBLE_TARGET_DATE_FROM,
            runtime.FROZEN_ELIGIBLE_TARGET_DATE_TILL,
            inclusive="both",
        ),
        ["target_trade_date", "target_instrument_id"],
    ].drop_duplicates()
    return eligible.iloc[: runtime.validation.EXPECTED_VALIDATION_IDENTITIES].copy()


def test_identity_frames_apply_frozen_contract_window_before_count() -> None:
    modeling = _modeling_with_full_history()
    validation_predictions = _validation_predictions(modeling)

    assert len(modeling.drop_duplicates()) == 1048

    eligible, validation_ids = runtime._identity_frames(
        modeling,
        validation_predictions,
    )

    assert len(eligible) == runtime.validation.EXPECTED_ELIGIBLE_IDENTITIES == 472
    assert len(validation_ids) == runtime.validation.EXPECTED_VALIDATION_IDENTITIES == 320
    assert eligible.target_trade_date.min() == runtime.FROZEN_ELIGIBLE_TARGET_DATE_FROM
    assert eligible.target_trade_date.max() == runtime.FROZEN_ELIGIBLE_TARGET_DATE_TILL
    assert eligible.target_instrument_id.eq(runtime.validation.TARGET_INSTRUMENT_ID).all()


def test_identity_frames_fail_closed_when_frozen_window_boundary_is_missing() -> None:
    historical_dates = list(
        pd.date_range(end="2024-08-04", periods=576, freq="D")
    )
    malformed_contract_dates = list(
        pd.date_range(
            runtime.FROZEN_ELIGIBLE_TARGET_DATE_FROM,
            "2026-06-10",
            periods=472,
        ).normalize()
    )
    target_dates = historical_dates + malformed_contract_dates
    malformed = pd.DataFrame(
        {
            "target_trade_date": target_dates,
            "target_instrument_id": [runtime.validation.TARGET_INSTRUMENT_ID]
            * len(target_dates),
            "prior_trade_date": [value - pd.Timedelta(days=1) for value in target_dates],
        }
    )
    validation_predictions = _validation_predictions(malformed)

    with pytest.raises(
        runtime.validation.FutoiSiSourceValidationError,
        match="eligible identity date window mismatch",
    ):
        runtime._identity_frames(malformed, validation_predictions)

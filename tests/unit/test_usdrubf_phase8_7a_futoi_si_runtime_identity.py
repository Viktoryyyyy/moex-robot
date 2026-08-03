from __future__ import annotations

import pandas as pd

from moex_research.runners import usdrubf_phase8_7a_futoi_si_runtime as runtime
from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_source_validation as validation,
)


def test_identity_frames_use_only_frozen_supervised_manual_rows() -> None:
    eligible_dates = pd.date_range("2024-08-05", periods=472, freq="D")
    rows: list[dict[str, object]] = []
    phase_order = ("B", "S", "OUT")
    for index, target_date in enumerate(eligible_dates):
        rows.append(
            {
                "target_trade_date": target_date.strftime("%Y-%m-%d"),
                "target_instrument_id": validation.TARGET_INSTRUMENT_ID,
                "prior_trade_date": (target_date - pd.Timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                ),
                "target_phase_label": phase_order[index % len(phase_order)],
                "target_is_labeled": True,
                "target_source": "manual_phase_labels_v1",
            }
        )

    rows.extend(
        [
            {
                "target_trade_date": "2026-07-01",
                "target_instrument_id": validation.TARGET_INSTRUMENT_ID,
                "prior_trade_date": "2026-06-30",
                "target_phase_label": None,
                "target_is_labeled": False,
                "target_source": "manual_phase_labels_v1",
            },
            {
                "target_trade_date": "2026-07-02",
                "target_instrument_id": validation.TARGET_INSTRUMENT_ID,
                "prior_trade_date": "2026-07-01",
                "target_phase_label": "B",
                "target_is_labeled": True,
                "target_source": "other_label_source",
            },
        ]
    )
    modeling = pd.DataFrame(rows)
    validation_predictions = modeling.iloc[:320].loc[
        :, ["target_trade_date", "target_instrument_id"]
    ]

    eligible, validation_ids = runtime._identity_frames(
        modeling, validation_predictions
    )

    assert len(eligible.index) == 472
    assert len(validation_ids.index) == 320
    assert eligible["target_trade_date"].max() == eligible_dates[-1].strftime(
        "%Y-%m-%d"
    )
    assert eligible["target_instrument_id"].eq(
        validation.TARGET_INSTRUMENT_ID
    ).all()

from datetime import timedelta

import pandas as pd
import pytest

from moex_data.futures import futoi_raw_incremental_acceptance as acceptance


@pytest.mark.parametrize("offset_days", [0, 1])
def test_rejects_current_and_future_moscow_date_before_state_access(monkeypatch, offset_days):
    monkeypatch.delenv("MOEX_DATA_ROOT", raising=False)
    date_end = (
        pd.Timestamp.now(tz=acceptance.MOEX_SOURCE_TIMEZONE).date()
        + timedelta(days=offset_days)
    ).isoformat()

    with pytest.raises(
        acceptance.FutoiIncrementalAcceptanceError,
        match="date_end must be earlier than current Europe/Moscow calendar date",
    ):
        acceptance.accept_incremental_backfill(
            instrument_id="si_futures_family",
            backfill_run_id="completed_source_date_regression",
            date_end=date_end,
        )

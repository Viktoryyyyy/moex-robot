from __future__ import annotations

import pytest

from moex_data import synchronized_live_market_oi_context as live


@pytest.mark.parametrize("value", ["Infinity", "-Infinity", "NaN", "1e309"])
def test_number_rejects_non_finite_marketdata(value: str) -> None:
    with pytest.raises(live.SynchronizedLiveMarketOIError, match="non-finite"):
        live._number(value)

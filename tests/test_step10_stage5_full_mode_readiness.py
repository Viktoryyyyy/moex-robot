from __future__ import annotations

from moex_data import step10_rub_refresh_dispatcher as dispatcher


def test_stage5_full_mode_is_fail_closed_until_lineage_is_accepted() -> None:
    assert dispatcher.STAGE5_FULL_MODE_READY is False

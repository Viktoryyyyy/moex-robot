from __future__ import annotations

import pytest

from src.moex_research.intelligence.usdrubf_news_live_bls_dol import (
    RssAcquisitionError,
    _release_timestamp,
)


def test_release_timestamp_accepts_real_pypdf_usdl_token_for_employment() -> None:
    text = (
        "Transmission of material in this news release is embargoed until "
        "USDL-26-1291 8:30 a.m. (ET) Friday, August 7, 2026 "
        "Technical information: Household data"
    )

    actual = _release_timestamp(text)

    assert actual.isoformat() == "2026-08-07T08:30:00-04:00"


def test_release_timestamp_accepts_real_pypdf_usdl_token_for_cpi() -> None:
    text = (
        "Transmission of material in this release is embargoed until "
        "USDL-26-1378 8:30 a.m. (ET) Wednesday, August 12, 2026 "
        "Technical information"
    )

    actual = _release_timestamp(text)

    assert actual.isoformat() == "2026-08-12T08:30:00-04:00"


def test_release_timestamp_still_accepts_direct_time_without_usdl_token() -> None:
    text = (
        "Transmission of material in this release is embargoed until "
        "8:30 a.m. (ET) Wednesday, August 12, 2026"
    )

    actual = _release_timestamp(text)

    assert actual.isoformat() == "2026-08-12T08:30:00-04:00"


def test_release_timestamp_rejects_arbitrary_text_between_until_and_time() -> None:
    text = (
        "Transmission of material in this release is embargoed until "
        "UNPROVEN-TOKEN 8:30 a.m. (ET) Wednesday, August 12, 2026"
    )

    with pytest.raises(RssAcquisitionError) as exc_info:
        _release_timestamp(text)

    assert exc_info.value.code == "TIMESTAMP_UNPROVABLE"

from __future__ import annotations

from datetime import datetime

from src.moex_research.intelligence import usdrubf_news_live_bls_dol as dol


class _NoLocaleStrptimeDateTime(datetime):
    @classmethod
    def strptime(cls, date_string: str, format: str):
        raise AssertionError("release timestamp parsing must not depend on locale-sensitive strptime")


def test_bls_release_timestamp_parser_is_locale_independent(monkeypatch) -> None:
    monkeypatch.setattr(dol, "datetime", _NoLocaleStrptimeDateTime)

    result = dol._release_timestamp(
        "Transmission of material in this release is embargoed until "
        "8:30 a.m. (ET) Wednesday, August 12, 2026."
    )

    assert result.isoformat() == "2026-08-12T08:30:00-04:00"
    assert result.weekday() == 2

from __future__ import annotations

from copy import deepcopy

import pytest

from moex_data import synchronized_live_market_oi_context as live


FORTS_COLUMNS = [
    "SECID",
    "OPEN",
    "HIGH",
    "LOW",
    "LAST",
    "VOLTODAY",
    "VALTODAY",
    "NUMTRADES",
    "OPENPOSITION",
    "BID",
    "OFFER",
    "SYSTIME",
]
CETS_COLUMNS = [
    "SECID",
    "OPEN",
    "HIGH",
    "LOW",
    "LAST",
    "WAPRICE",
    "VOLTODAY",
    "NUMTRADES",
    "BID",
    "OFFER",
    "SYSTIME",
]


def _forts_row(
    secid: str,
    systime: str,
    *,
    oi: int | None = 1000,
    rub_per_quote_unit: float = 1.0,
) -> list[object]:
    volume = 100
    wap = 90.5
    value_rub = int(wap * volume * rub_per_quote_unit)
    return [secid, 90.0, 92.0, 89.0, 91.0, volume, value_rub, 321, oi, 90.9, 91.1, systime]


def _payloads() -> tuple[dict[str, object], dict[str, object]]:
    securities = {
        "columns": ["SECID", "BOARDID", "LASTTRADEDATE", "MINSTEP", "STEPPRICE"],
        "data": [
            ["USDRUBF", "RFUD", "2099-12-31", 0.01, 10.0],
            ["CNYRUBF", "RFUD", "2099-12-31", 0.001, 1.0],
            ["SiU6", "RFUD", "2026-09-17", 1.0, 1.0],
            ["SiZ6", "RFUD", "2026-12-17", 1.0, 1.0],
            ["SiH7", "RFUD", "2027-03-18", 1.0, 1.0],
            ["CRU6", "RFUD", "2026-09-17", 0.001, 1.0],
            ["CRZ6", "RFUD", "2026-12-17", 0.001, 1.0],
            ["CRH7", "RFUD", "2027-03-18", 0.001, 1.0],
        ],
    }
    marketdata = {
        "columns": FORTS_COLUMNS,
        "data": [
            _forts_row("USDRUBF", "2026-09-02 13:00:00", oi=50000, rub_per_quote_unit=1000),
            _forts_row("CNYRUBF", "2026-09-02 13:00:02", oi=60000, rub_per_quote_unit=1000),
            _forts_row("SiU6", "2026-09-02 13:00:04", oi=70000),
            _forts_row("SiZ6", "2026-09-02 13:00:06", oi=30000),
            _forts_row("CRU6", "2026-09-02 13:00:08", oi=40000, rub_per_quote_unit=1000),
            _forts_row("CRZ6", "2026-09-02 13:00:10", oi=20000, rub_per_quote_unit=1000),
        ],
    }
    cets = {
        "marketdata": {
            "columns": CETS_COLUMNS,
            "data": [
                [
                    "CNYRUB_TOM",
                    12.0,
                    12.2,
                    11.9,
                    12.1,
                    12.05,
                    999999,
                    111,
                    12.09,
                    12.11,
                    "2026-09-02 13:00:12",
                ]
            ],
        }
    }
    return {"securities": securities, "marketdata": marketdata}, cets


def _build(forts: dict[str, object], cets: dict[str, object]) -> dict[str, object]:
    return live.build_snapshot_from_payloads(
        forts_payload=forts,
        cets_payload=cets,
        forts_received_at_utc="2026-09-02T10:00:20+00:00",
        cets_received_at_utc="2026-09-02T10:00:22+00:00",
        forts_source_url="https://example.test/forts",
        cets_source_url="https://example.test/cets",
    )


def test_live_source_uses_canonical_authenticated_apim_contract() -> None:
    assert live.DEFAULT_BASE_URL == "https://apim.moex.com"
    assert live.API_URL_ENV == "MOEX_API_URL"
    headers = live._auth_headers({"MOEX_API_KEY": "secret-token"})
    assert headers["Authorization"] == "Bearer secret-token"
    with pytest.raises(live.SynchronizedLiveMarketOIError, match="MOEX_API_KEY is required"):
        live._auth_headers({})


def test_snapshot_maps_front_next_and_exposes_requested_fields() -> None:
    forts, cets = _payloads()
    snapshot = _build(forts, cets)

    assert snapshot["status"] == "READY"
    assert snapshot["synchronization"]["synchronized"] is True
    assert snapshot["bindings"] == {
        "usdrubf": "USDRUBF",
        "si_front": "SiU6",
        "si_next": "SiZ6",
        "cnyrubf": "CNYRUBF",
        "cr_front": "CRU6",
        "cr_next": "CRZ6",
        "cnyrub_tom": "CNYRUB_TOM",
    }

    requested = {
        "last",
        "open",
        "high",
        "low",
        "wap",
        "volume",
        "trades",
        "oi",
        "bid",
        "ask",
        "spread",
        "timestamp",
    }
    for item in snapshot["instruments"].values():
        assert requested <= set(item)

    assert snapshot["instruments"]["usdrubf"]["wap"] == pytest.approx(90.5)
    assert snapshot["instruments"]["si_front"]["wap"] == pytest.approx(90.5)
    assert snapshot["instruments"]["cr_front"]["wap"] == pytest.approx(90.5)
    assert snapshot["instruments"]["usdrubf"]["wap_method"] == "VALTODAY/VOLTODAY/(STEPPRICE/MINSTEP)"
    assert snapshot["instruments"]["si_front"]["oi"] == 70000
    assert snapshot["instruments"]["si_front"]["price_oi_same_source_row"] is True
    assert snapshot["instruments"]["si_front"]["price_oi_usable"] is True
    assert snapshot["instruments"]["cnyrub_tom"]["oi"] is None
    assert snapshot["instruments"]["cnyrub_tom"]["oi_status"] == "not_applicable"
    assert snapshot["instruments"]["cnyrub_tom"]["price_oi_usable"] is False


def test_excessive_cross_instrument_timestamp_skew_fails_closed() -> None:
    forts, cets = _payloads()
    altered = deepcopy(forts)
    columns = altered["marketdata"]["columns"]
    systime_index = columns.index("SYSTIME")
    secid_index = columns.index("SECID")
    for row in altered["marketdata"]["data"]:
        if row[secid_index] == "CRZ6":
            row[systime_index] = "2026-09-02 12:58:00"

    snapshot = _build(altered, cets)

    assert snapshot["status"] == "UNAVAILABLE"
    assert snapshot["synchronization"]["status"] == "FAIL"
    assert snapshot["synchronization"]["max_skew_seconds"] > 60
    assert snapshot["quality"]["analysis_usable"] is False
    assert not any(snapshot["quality"]["price_oi_usable_by_instrument"].values())


def test_stale_snapshot_fails_closed_even_when_rows_are_mutually_aligned() -> None:
    forts, cets = _payloads()
    altered_forts = deepcopy(forts)
    altered_cets = deepcopy(cets)
    for row in altered_forts["marketdata"]["data"]:
        row[FORTS_COLUMNS.index("SYSTIME")] = "2026-09-02 12:58:55"
    altered_cets["marketdata"]["data"][0][CETS_COLUMNS.index("SYSTIME")] = "2026-09-02 12:58:55"

    snapshot = _build(altered_forts, altered_cets)

    assert snapshot["synchronization"]["max_skew_seconds"] == 0
    assert snapshot["synchronization"]["all_instruments_fresh"] is False
    assert snapshot["synchronization"]["synchronized"] is False
    assert snapshot["quality"]["analysis_usable"] is False


def test_missing_futures_oi_blocks_price_oi_without_substituting_futoi() -> None:
    forts, cets = _payloads()
    altered = deepcopy(forts)
    for row in altered["marketdata"]["data"]:
        if row[FORTS_COLUMNS.index("SECID")] == "SiU6":
            row[FORTS_COLUMNS.index("OPENPOSITION")] = None

    snapshot = _build(altered, cets)

    assert snapshot["synchronization"]["synchronized"] is True
    assert snapshot["instruments"]["si_front"]["oi"] is None
    assert snapshot["instruments"]["si_front"]["oi_status"] == "missing"
    assert snapshot["instruments"]["si_front"]["price_oi_usable"] is False
    assert snapshot["quality"]["analysis_usable"] is False
    assert snapshot["quality"]["price_oi_usable_by_instrument"]["si_front"] is False

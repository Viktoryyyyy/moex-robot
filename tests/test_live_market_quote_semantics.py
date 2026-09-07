from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

import pytest

from moex_data import live_basis_carry_context as basis
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
    oi: int = 1000,
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
            "data": [[
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
            ]],
        }
    }
    return {"securities": securities, "marketdata": marketdata}, cets


def _set_quote(forts: dict[str, object], secid: str, bid: object, offer: object) -> None:
    columns = forts["marketdata"]["columns"]
    for row in forts["marketdata"]["data"]:
        if row[columns.index("SECID")] == secid:
            row[columns.index("BID")] = bid
            row[columns.index("OFFER")] = offer
            return
    raise AssertionError(f"missing fixture SECID {secid}")


def _build(forts: dict[str, object], cets: dict[str, object]) -> dict[str, object]:
    return live.build_snapshot_from_payloads(
        forts_payload=forts,
        cets_payload=cets,
        forts_received_at_utc="2026-09-02T10:00:20+00:00",
        cets_received_at_utc="2026-09-02T10:00:22+00:00",
    )


def test_normal_quote_is_usable_and_spread_is_derived_without_source_substitution() -> None:
    forts, cets = _payloads()
    snapshot = _build(forts, cets)
    item = snapshot["instruments"]["usdrubf"]

    assert item["bid"] == pytest.approx(90.9)
    assert item["ask"] == pytest.approx(91.1)
    assert item["spread"] == pytest.approx(0.2)
    assert item["quote_usable"] is True
    assert item["quote_status"] == "available"
    assert item["bid_source_value"] == pytest.approx(90.9)
    assert item["offer_source_value"] == pytest.approx(91.1)
    assert snapshot["quality"]["quote_required_for_analysis"] is False


def test_rfud_zero_offer_is_source_native_empty_side_not_a_crossed_price() -> None:
    forts, cets = _payloads()
    _set_quote(forts, "USDRUBF", 90.9, 0)
    snapshot = _build(forts, cets)
    item = snapshot["instruments"]["usdrubf"]

    assert snapshot["status"] == "READY"
    assert item["bid"] == pytest.approx(90.9)
    assert item["ask"] is None
    assert item["spread"] is None
    assert item["offer_source_value"] == 0
    assert item["quote_usable"] is False
    assert item["quote_status"] == "empty_offer_source_native"
    assert item["price_oi_usable"] is True


def test_missing_quote_side_does_not_poison_factual_price_oi() -> None:
    forts, cets = _payloads()
    _set_quote(forts, "USDRUBF", 90.9, None)
    snapshot = _build(forts, cets)
    item = snapshot["instruments"]["usdrubf"]

    assert snapshot["status"] == "READY"
    assert item["quote_status"] == "missing_offer"
    assert item["quote_usable"] is False
    assert item["spread"] is None
    assert item["price_oi_usable"] is True


def test_equal_positive_quote_is_preserved_but_not_declared_usable_without_source_contract() -> None:
    forts, cets = _payloads()
    _set_quote(forts, "USDRUBF", 91.0, 91.0)
    snapshot = _build(forts, cets)
    item = snapshot["instruments"]["usdrubf"]

    assert snapshot["status"] == "READY"
    assert item["bid"] == pytest.approx(91.0)
    assert item["ask"] == pytest.approx(91.0)
    assert item["spread"] is None
    assert item["quote_usable"] is False
    assert item["quote_status"] == "locked_quote_unverified"
    assert item["price_oi_usable"] is True


def test_positive_crossed_quote_is_preserved_without_swap_or_clamp_and_price_oi_remains_usable() -> None:
    forts, cets = _payloads()
    _set_quote(forts, "USDRUBF", 91.2, 91.1)
    snapshot = _build(forts, cets)
    item = snapshot["instruments"]["usdrubf"]

    assert snapshot["status"] == "READY"
    assert item["bid"] == pytest.approx(91.2)
    assert item["ask"] == pytest.approx(91.1)
    assert item["bid_source_value"] == pytest.approx(91.2)
    assert item["offer_source_value"] == pytest.approx(91.1)
    assert item["spread"] is None
    assert item["quote_usable"] is False
    assert item["quote_status"] == "crossed_quote_unusable"
    assert item["quote_temporal_coherence"] == "unproven_for_crossed_quote"
    assert item["price_oi_usable"] is True


@pytest.mark.parametrize(
    ("bid", "offer"),
    [
        ("bad", 91.1),
        (90.9, "bad"),
        (-1.0, 91.1),
        (90.9, float("inf")),
    ],
)
def test_malformed_quote_is_quote_local_failure_not_price_oi_failure(bid: object, offer: object) -> None:
    forts, cets = _payloads()
    _set_quote(forts, "USDRUBF", bid, offer)
    snapshot = _build(forts, cets)
    item = snapshot["instruments"]["usdrubf"]

    assert snapshot["status"] == "READY"
    assert item["quote_usable"] is False
    assert item["quote_status"] == "malformed_quote"
    assert item["spread"] is None
    assert item["price_oi_usable"] is True


def test_quote_freshness_is_not_given_an_invented_independent_timestamp() -> None:
    row = {
        "OPEN": 90.0,
        "HIGH": 92.0,
        "LOW": 89.0,
        "LAST": 91.0,
        "VOLTODAY": 100,
        "VALTODAY": 9050,
        "NUMTRADES": 321,
        "OPENPOSITION": 1000,
        "BID": 90.9,
        "OFFER": 91.1,
        "SYSTIME": "2026-09-02 12:58:00",
    }
    item = live._normalize_row(
        logical_id="usdrubf",
        secid="USDRUBF",
        row=row,
        source_id=live.FORTS_SOURCE_ID,
        received_at_utc=datetime(2026, 9, 2, 10, 0, 20, tzinfo=timezone.utc),
        freshness_reference_utc=datetime(2026, 9, 2, 10, 0, 22, tzinfo=timezone.utc),
        is_future=True,
        security_row={"MINSTEP": 1.0, "STEPPRICE": 1.0},
    )

    assert item["stale"] is True
    assert item["quote_stale"] is True
    assert item["quote_usable"] is False
    assert item["quote_status"] == "stale_quote"
    assert item["quote_freshness_basis"] == "marketdata_row_SYSTIME"
    assert item["quote_independent_timestamp_available"] is False


def test_live_basis_uses_last_and_survives_crossed_quote_quality_failure() -> None:
    forts, cets = _payloads()
    altered = deepcopy(forts)
    _set_quote(altered, "CNYRUBF", 91.2, 91.1)
    snapshot = _build(altered, cets)
    snapshot["instruments"]["cr_front"]["expiry_date"] = "2026-09-17"
    snapshot["instruments"]["cr_next"]["expiry_date"] = "2026-12-17"

    assert snapshot["instruments"]["cnyrubf"]["quote_usable"] is False
    assert snapshot["instruments"]["cnyrubf"]["price_oi_usable"] is True

    context = basis.build_context(snapshot)
    cny = context["pairs"]["cny_rub"]
    assert context["live_input_policy"]["rate_field"] == "last"
    assert context["live_input_policy"]["additional_live_fetch_performed"] is False
    assert cny["status"] == "READY"
    assert cny["ready_metric_count"] == len(cny["metrics"])
    assert all(metric["status"] == "READY" for metric in cny["metrics"])

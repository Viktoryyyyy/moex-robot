from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone

import pytest

from moex_data import live_basis_carry_context as basis
from moex_data import synchronized_live_market_oi_context as live
from moex_data import synchronized_live_market_oi_context_partial as partial


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
        "columns": list(FORTS_COLUMNS),
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
            "columns": list(CETS_COLUMNS),
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
    _zero_offer(forts, "USDRUBF")
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


def test_equal_positive_quote_is_usable_with_arithmetic_zero_spread() -> None:
    forts, cets = _payloads()
    _set_quote(forts, "USDRUBF", 91.0, 91.0)
    snapshot = _build(forts, cets)
    item = snapshot["instruments"]["usdrubf"]

    assert snapshot["status"] == "READY"
    assert item["bid"] == pytest.approx(91.0)
    assert item["ask"] == pytest.approx(91.0)
    assert item["spread"] == 0.0
    assert item["quote_usable"] is True
    assert item["quote_status"] == "available"
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
        (float("-inf"), 91.1),
        (True, 91.1),
        (90.9, [1, 2]),
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
    json.dumps(snapshot, allow_nan=False)
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


OFFER_EVIDENCE = ("OFFERDEPTH", "OFFERDEPTHT", "NUMOFFERS")


def _set_fields(payload: dict[str, object], secid: str, **values: object) -> None:
    block = payload["marketdata"]
    for field in values:
        if field not in block["columns"]:
            block["columns"].append(field)
            for row in block["data"]:
                row.append(None)
    for row in block["data"]:
        if row[block["columns"].index("SECID")] == secid:
            for field, value in values.items():
                row[block["columns"].index(field)] = value
            return
    raise AssertionError(f"missing fixture SECID {secid}")


def _drop_column(payload: dict[str, object], field: str) -> None:
    block = payload["marketdata"]
    index = block["columns"].index(field)
    block["columns"].pop(index)
    for row in block["data"]:
        row.pop(index)


def _zero_offer(payload: dict[str, object], secid: str) -> None:
    _set_fields(payload, secid, OFFER=0, **dict.fromkeys(OFFER_EVIDENCE, 0))


@pytest.mark.parametrize("present_mask", range(7))
def test_zero_offer_needs_all_three_evidence_columns(present_mask: int) -> None:
    forts, cets = _payloads()
    _zero_offer(forts, "USDRUBF")
    for index, field in enumerate(OFFER_EVIDENCE):
        if not present_mask & (1 << index):
            _drop_column(forts, field)
    item = _build(forts, cets)["instruments"]["usdrubf"]
    assert item["quote_status"] == "zero_quote_unproven"
    assert item["offer_source_value"] == 0
    assert item["ask"] is None and item["spread"] is None
    assert item["quote_usable"] is False
    assert item["price_oi_usable"] is True


@pytest.mark.parametrize("field", OFFER_EVIDENCE)
def test_null_evidence_does_not_prove_empty_offer(field: str) -> None:
    forts, cets = _payloads()
    _zero_offer(forts, "USDRUBF")
    _set_fields(forts, "USDRUBF", **{field: None})
    item = _build(forts, cets)["instruments"]["usdrubf"]
    assert item["quote_status"] == "zero_quote_unproven"
    assert item["quote_usable"] is False
    assert item["price_oi_usable"] is True


@pytest.mark.parametrize("field", OFFER_EVIDENCE)
@pytest.mark.parametrize("value", [1, -1, 0.5, False, "0", "bad", float("inf"), [0]])
def test_contradictory_or_invalid_evidence_fails_quote_closed(field: str, value: object) -> None:
    forts, cets = _payloads()
    _zero_offer(forts, "USDRUBF")
    _set_fields(forts, "USDRUBF", **{field: value})
    snapshot = _build(forts, cets)
    item = snapshot["instruments"]["usdrubf"]
    assert item["quote_status"] == "malformed_quote"
    assert item["quote_usable"] is False
    assert item["ask"] is None and item["spread"] is None
    assert item["price_oi_usable"] is True
    json.dumps(snapshot, allow_nan=False)


def test_evidence_is_not_borrowed_from_another_instrument() -> None:
    forts, cets = _payloads()
    _set_quote(forts, "USDRUBF", 90.9, 0)
    _zero_offer(forts, "CNYRUBF")
    snapshot = _build(forts, cets)
    assert snapshot["instruments"]["usdrubf"]["quote_status"] == "zero_quote_unproven"
    assert snapshot["instruments"]["cnyrubf"]["quote_status"] == "empty_offer_source_native"


@pytest.mark.parametrize("with_bid_evidence", [False, True])
def test_zero_bid_remains_unproven_even_with_symmetric_fields(with_bid_evidence: bool) -> None:
    forts, cets = _payloads()
    _set_quote(forts, "USDRUBF", 0, 91.1)
    if with_bid_evidence:
        _set_fields(forts, "USDRUBF", BIDDEPTH=0, BIDDEPTHT=0, NUMBIDS=0)
    item = _build(forts, cets)["instruments"]["usdrubf"]
    assert item["quote_status"] == "zero_quote_unproven"
    assert item["bid"] is None and item["ask"] == pytest.approx(91.1)
    assert item["bid_source_value"] == 0
    assert item["spread"] is None and item["quote_usable"] is False
    assert item["price_oi_usable"] is True


def test_offer_rule_is_not_applied_to_cets() -> None:
    forts, cets = _payloads()
    _zero_offer(cets, "CNYRUB_TOM")
    snapshot = _build(forts, cets)
    assert snapshot["instruments"]["cnyrub_tom"]["quote_status"] == "zero_quote_unproven"
    assert snapshot["quality"]["spot_price_usable"] is True


@pytest.mark.parametrize(
    ("fields", "status"),
    [(("BID",), "missing_bid"), (("OFFER",), "missing_offer"), (("BID", "OFFER"), "missing_book")],
)
def test_missing_quote_columns_are_local_to_quotes(fields: tuple[str, ...], status: str) -> None:
    forts, cets = _payloads()
    for payload in (forts, cets):
        for field in fields:
            _drop_column(payload, field)
    snapshot = _build(forts, cets)
    assert snapshot["status"] == "READY"
    assert snapshot["quality"]["price_oi_all_futures_usable"] is True
    for item in snapshot["instruments"].values():
        assert item["quote_status"] == status
        assert item["quote_usable"] is False
        assert item["spread"] is None


@pytest.mark.parametrize("field", ["LAST", "OPENPOSITION", "SYSTIME"])
def test_factual_columns_remain_required(field: str) -> None:
    forts, cets = _payloads()
    _zero_offer(forts, "USDRUBF")
    _drop_column(forts, field)
    with pytest.raises(live.SynchronizedLiveMarketOIError, match="missing required columns"):
        _build(forts, cets)


@pytest.mark.parametrize("field", ["LAST", "OPENPOSITION"])
def test_empty_offer_does_not_override_missing_factual_values(field: str) -> None:
    forts, cets = _payloads()
    _zero_offer(forts, "USDRUBF")
    _set_fields(forts, "USDRUBF", **{field: None})
    snapshot = _build(forts, cets)
    assert snapshot["instruments"]["usdrubf"]["price_oi_usable"] is False
    assert snapshot["quality"]["analysis_usable"] is False


def test_empty_offer_is_not_relabelled_stale_and_cannot_bypass_freshness() -> None:
    forts, cets = _payloads()
    _zero_offer(forts, "USDRUBF")
    fresh = _build(forts, cets)["instruments"]["usdrubf"]
    assert fresh["quote_stale"] is False and fresh["stale"] is False
    assert fresh["price_oi_usable"] is True

    _set_fields(forts, "USDRUBF", SYSTIME="2026-09-02 12:58:00")
    snapshot = _build(forts, cets)
    item = snapshot["instruments"]["usdrubf"]
    assert item["quote_status"] == "empty_offer_source_native"
    assert item["stale"] is True and item["quote_stale"] is True
    assert item["price_oi_usable"] is False
    assert snapshot["synchronization"]["synchronized"] is False


def test_partial_factual_policy_does_not_relax_cross_market_sync() -> None:
    forts, cets = _payloads()
    _zero_offer(forts, "USDRUBF")
    _set_fields(cets, "CNYRUB_TOM", SYSTIME="2026-09-02 12:58:00")
    snapshot = partial._reclassify(_build(forts, cets))
    assert snapshot["instruments"]["usdrubf"]["price_oi_usable"] is True
    assert snapshot["synchronization"]["synchronized"] is False
    assert snapshot["quality"]["analysis_usable"] is False
    assert snapshot["quality"]["full_cross_market_synchronization_required"] is True
    assert snapshot["status"] == "PARTIAL"


@pytest.mark.parametrize("secid", ["USDRUBF", "CNYRUBF"])
def test_basis_carry_is_unchanged_by_confirmed_empty_offer(secid: str) -> None:
    forts, cets = _payloads()
    baseline = _build(forts, cets)
    _zero_offer(forts, secid)
    altered = _build(forts, cets)
    for snapshot in (baseline, altered):
        for logical_id in ("si_front", "cr_front"):
            snapshot["instruments"][logical_id]["expiry_date"] = "2026-09-17"
        for logical_id in ("si_next", "cr_next"):
            snapshot["instruments"][logical_id]["expiry_date"] = "2026-12-17"
    expected = basis.build_context(baseline)
    actual = basis.build_context(altered)
    assert actual == expected
    assert actual["pairs"]["cny_rub"]["status"] == "READY"
    assert actual["live_input_policy"]["rate_field"] == "last"
    assert actual["live_input_policy"]["additional_live_fetch_performed"] is False


def test_sentinel_normalization_is_deterministic_and_keeps_input_unchanged() -> None:
    forts, cets = _payloads()
    _zero_offer(forts, "USDRUBF")
    before = deepcopy((forts, cets))
    first = _build(forts, cets)
    second = _build(forts, cets)
    assert json.dumps(first, sort_keys=True, allow_nan=False) == json.dumps(second, sort_keys=True, allow_nan=False)
    assert (forts, cets) == before
    assert not set(OFFER_EVIDENCE) & set(first["instruments"]["usdrubf"])
    assert first["instruments"]["usdrubf"]["offer_source_value"] == 0


def test_existing_apim_response_evidence_needs_no_additional_fetch() -> None:
    from moex_data import synchronized_live_market_oi_context_apim as apim

    def run(empty_offer: bool) -> tuple[dict[str, object], list[str]]:
        forts, cets = _payloads()
        present = {row[0] for row in forts["marketdata"]["data"]}
        forts["securities"]["data"] = [
            row for row in forts["securities"]["data"] if row[0] in present
        ]
        if empty_offer:
            _zero_offer(forts, "USDRUBF")
        calls = []

        class Response:
            status_code = 200

            def __init__(self, url: str, payload: dict[str, object]) -> None:
                self.url = url
                self.payload = payload

            def raise_for_status(self) -> None:
                pass

            def json(self) -> dict[str, object]:
                return deepcopy(self.payload)

        def get(url: str, **kwargs: object) -> Response:
            assert url in (live.DEFAULT_BASE_URL + live.FORTS_ENDPOINT, live.DEFAULT_BASE_URL + live.CETS_ENDPOINT)
            assert kwargs["allow_redirects"] is False
            calls.append(json.dumps([url, kwargs["params"]], sort_keys=True))
            return Response(url, forts if "/RFUD/" in url else cets)

        snapshot = partial.fetch_live_snapshot(
            http_get=get,
            now_fn=lambda: datetime(2026, 9, 2, 10, 0, 22, tzinfo=timezone.utc),
            env={"MOEX_API_KEY": "test-only"},
        )
        assert snapshot["provenance"]["forts"]["completeness"]["mode"] == apim.COMPLETENESS_MODE
        return snapshot, sorted(calls)

    baseline, normal_calls = run(False)
    sentinel, sentinel_calls = run(True)
    assert normal_calls == sentinel_calls
    assert len(sentinel_calls) == 3  # Existing RFUD + completeness probe + CETS.
    assert baseline["status"] == sentinel["status"] == "READY"
    item = sentinel["instruments"]["usdrubf"]
    assert item["quote_status"] == "empty_offer_source_native"
    assert item["price_oi_usable"] is True
    assert item["ask"] is None and item["spread"] is None

from __future__ import annotations

from moex_data import synchronized_live_market_oi_context as core
from moex_data import synchronized_live_market_oi_context_apim as apim


def _forts_row(secid: str, systime: str, *, oi: int) -> list[object]:
    return [secid, 90.0, 92.0, 89.0, 91.0, 100, 1000, 10, oi, 90.9, 91.1, systime]


def test_unproven_forts_wap_is_unavailable_without_blocking_price_oi() -> None:
    forts = {
        "securities": {
            "columns": list(core.FUTURES_SECURITY_COLUMNS),
            "data": [
                ["USDRUBF", "RFUD", "2099-12-31", 0.01, 10.0],
                ["CNYRUBF", "RFUD", "2099-12-31", 0.001, 1.0],
                ["SiU6", "RFUD", "2026-09-17", 1.0, 1.0],
                ["SiZ6", "RFUD", "2026-12-17", 1.0, 1.0],
                ["CRU6", "RFUD", "2026-09-17", 0.001, 1.0],
                ["CRZ6", "RFUD", "2026-12-17", 0.001, 1.0],
            ],
        },
        "marketdata": {
            "columns": list(core.FUTURES_MARKETDATA_COLUMNS),
            "data": [
                _forts_row("USDRUBF", "2026-09-02 13:00:00", oi=50000),
                _forts_row("CNYRUBF", "2026-09-02 13:00:02", oi=60000),
                _forts_row("SiU6", "2026-09-02 13:00:04", oi=70000),
                _forts_row("SiZ6", "2026-09-02 13:00:06", oi=30000),
                _forts_row("CRU6", "2026-09-02 13:00:08", oi=40000),
                _forts_row("CRZ6", "2026-09-02 13:00:10", oi=20000),
            ],
        },
        core.FORTS_ROW_RECEIPTS_KEY: {
            secid: "2026-09-02T10:00:20+00:00"
            for secid in ("USDRUBF", "CNYRUBF", "SiU6", "SiZ6", "CRU6", "CRZ6")
        },
    }
    cets = {
        "marketdata": {
            "columns": list(core.CETS_MARKETDATA_COLUMNS),
            "data": [[
                "CNYRUB_TOM",
                12.0,
                12.2,
                11.9,
                12.1,
                12.05,
                1000,
                20,
                12.09,
                12.11,
                "2026-09-02 13:00:12",
            ]],
        }
    }

    normalized = apim._without_unproven_forts_wap(forts)
    val_index = normalized["marketdata"]["columns"].index("VALTODAY")
    assert all(row[val_index] is None for row in normalized["marketdata"]["data"])
    assert all(row[val_index] == 1000 for row in forts["marketdata"]["data"])

    snapshot = core.build_snapshot_from_payloads(
        forts_payload=normalized,
        cets_payload=cets,
        forts_received_at_utc="2026-09-02T10:00:20+00:00",
        cets_received_at_utc="2026-09-02T10:00:22+00:00",
    )
    apim._mark_wap_semantics(snapshot)

    assert snapshot["status"] == "READY"
    assert snapshot["quality"]["price_oi_all_futures_usable"] is True
    for logical_id in core.FUTURES_LOGICAL_ORDER:
        item = snapshot["instruments"][logical_id]
        assert item["wap"] is None
        assert item["wap_method"] is None
        assert item["wap_status"] == "unavailable_source_native"
        assert item["price_oi_usable"] is True
    assert snapshot["instruments"]["cnyrub_tom"]["wap"] == 12.05
    assert snapshot["instruments"]["cnyrub_tom"]["wap_status"] == "available_source_native"
    assert snapshot["provenance"]["forts"]["wap_status"] == "unavailable_source_native"
